package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

type cachePutFailurePoint string

const (
	failureBeforeCacheRead cachePutFailurePoint = "before_read"
	failureAfterCacheRead  cachePutFailurePoint = "partial_read"
	failureAfterCacheEOF   cachePutFailurePoint = "after_eof"
)

type cachePutFailureResult struct {
	bytesRead int64
	readErr   error
}

type failingFirstStreamPutClient struct {
	cacheclient.CacheClient
	point       cachePutFailurePoint
	failure     error
	calls       atomic.Int32
	firstResult chan cachePutFailureResult
}

func (c *failingFirstStreamPutClient) PutStream(ctx context.Context, key string, r io.Reader, ttl int64) error {
	if c.calls.Add(1) != 1 {
		return c.CacheClient.PutStream(ctx, key, r, ttl)
	}

	var result cachePutFailureResult
	switch c.point {
	case failureBeforeCacheRead:
		// Return without touching the reader, as a cache route or stream setup
		// failure can do before the first body read.
	case failureAfterCacheRead:
		result.bytesRead, result.readErr = io.CopyN(io.Discard, r, 7)
	case failureAfterCacheEOF:
		result.bytesRead, result.readErr = io.Copy(io.Discard, r)
	}
	c.firstResult <- result
	return c.failure
}

type cacheWriteFailureLogWriter struct {
	errorText string
	returned  chan struct{}
	observed  chan struct{}
}

func (w *cacheWriteFailureLogWriter) Write(p []byte) (int, error) {
	entry := string(p)
	if strings.Contains(entry, w.errorText) {
		var signal chan struct{}
		switch {
		case strings.Contains(entry, "Cache write with metadata failed"):
			signal = w.returned
		case strings.Contains(entry, "Cache write failed"):
			signal = w.observed
		}
		if signal != nil {
			select {
			case signal <- struct{}{}:
			default:
			}
		}
	}
	return len(p), nil
}

func waitForCacheSlot(t *testing.T, svc *Service, weight int64) bool {
	t.Helper()

	deadline := time.NewTimer(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		if svc.acquireCacheSlot(context.Background(), weight, priorityReadMiss) {
			svc.releaseCacheSlot(weight)
			return true
		}
		select {
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
}

func TestCacheableFullGet_StoreFailureReleasesRelayAndReservation(t *testing.T) {
	const (
		bucket = "cache-failure-bucket"
		key    = "cache-failure-key"
	)
	body := bytes.Repeat([]byte("x"), 128<<10)
	bodyText := string(body)

	tests := []struct {
		point         cachePutFailurePoint
		wantReadBytes int64
	}{
		{point: failureBeforeCacheRead, wantReadBytes: 0},
		{point: failureAfterCacheRead, wantReadBytes: 7},
		{point: failureAfterCacheEOF, wantReadBytes: int64(len(body))},
	}
	for _, tt := range tests {
		t.Run(string(tt.point), func(t *testing.T) {
			injectedErr := errors.New("injected cache stream failure: " + string(tt.point))
			cacheClient := &failingFirstStreamPutClient{
				CacheClient: cacheclient.NewMemoryCache(),
				point:       tt.point,
				failure:     injectedErr,
				firstResult: make(chan cachePutFailureResult, 1),
			}
			cfg := config.NewDefault()
			cfg.Cache.MaxConcurrentWrites = 1
			cfg.Cache.MaxPopulateMemoryBytes = int64(len(body))
			originGets := atomic.Int32{}
			forwarder := &mockForwarder{
				doRequestFunc: func(_ context.Context, _ *http.Request, _ string, _ string) (*http.Response, error) {
					originGets.Add(1)
					return cacheableGetResponse(bodyText, `"cache-failure-etag"`), nil
				},
			}
			cacheStore := cache.NewCacheWithClient(cacheClient, &cfg.Cache)
			svc := NewService(forwarder, cacheStore, cfg)

			logs := &cacheWriteFailureLogWriter{
				errorText: injectedErr.Error(),
				returned:  make(chan struct{}, 1),
				observed:  make(chan struct{}, 1),
			}
			previousLogger := log.Logger
			log.Logger = zerolog.New(logs).Level(zerolog.DebugLevel)
			t.Cleanup(func() { log.Logger = previousLogger })

			handlerResults := make(chan error, 3)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handlerResults <- svc.HandleGetObject(w, r)
			}))
			defer server.Close()
			client := &http.Client{Timeout: 3 * time.Second}
			defer client.CloseIdleConnections()

			get := func(name string) (http.Header, string) {
				t.Helper()
				headers, body := readCompletionResponse(t, client, server.URL+"/"+bucket+"/"+key)
				waitForHandlerReturn(t, name, handlerResults)
				return headers, body
			}

			firstHeaders, firstBody := get("failed-populate GET")
			if got := firstHeaders.Get("X-Cache"); got != XCacheMiss {
				t.Errorf("failed-populate GET X-Cache = %q, want %q", got, XCacheMiss)
			}
			if got := firstHeaders.Get("ETag"); got != `"cache-failure-etag"` {
				t.Errorf("failed-populate GET ETag = %q, want origin ETag", got)
			}
			if got := firstHeaders.Get("Content-Type"); got != "text/plain" {
				t.Errorf("failed-populate GET Content-Type = %q, want origin Content-Type", got)
			}
			if got := firstHeaders.Get("Content-Length"); got != strconv.Itoa(len(body)) {
				t.Errorf("failed-populate GET Content-Length = %q, want %d", got, len(body))
			}
			if firstBody != bodyText {
				t.Errorf("failed-populate GET body length = %d, want %d", len(firstBody), len(body))
			}

			result := <-cacheClient.firstResult
			if result.readErr != nil {
				t.Errorf("injected store read failed: %v", result.readErr)
			}
			if result.bytesRead != tt.wantReadBytes {
				t.Errorf("store read %d bytes, want %d", result.bytesRead, tt.wantReadBytes)
			}
			select {
			case <-logs.returned:
			case <-time.After(time.Second):
				t.Errorf("cache store error %q was not returned", injectedErr)
			}
			select {
			case <-logs.observed:
			case <-time.After(time.Second):
				t.Errorf("cache failure %q was not observed after the GET completed", injectedErr)
			}

			if _, found, err := cacheStore.GetMeta(context.Background(), bucket, key); err != nil {
				t.Errorf("GetMeta after failed populate: %v", err)
			} else if found {
				t.Error("failed cache populate became visible")
			}

			weight := svc.populateWeight(int64(len(body)))
			if !waitForCacheSlot(t, svc, weight) {
				t.Errorf("cache reservation (%d bytes) was not available after relay completion", weight)
			}

			secondHeaders, secondBody := get("healthy retry GET")
			if secondBody != bodyText {
				t.Errorf("healthy retry body length = %d, want %d", len(secondBody), len(body))
			}
			if got := secondHeaders.Get("X-Cache"); got != XCacheMiss {
				t.Errorf("healthy retry X-Cache = %q, want %q", got, XCacheMiss)
			}
			waitForCacheMeta(t, cacheStore, bucket, key, time.Second)
			if !waitForCacheSlot(t, svc, weight) {
				t.Error("healthy cache populate did not release its reservation")
			}

			thirdHeaders, thirdBody := get("cached retry GET")
			if thirdBody != bodyText {
				t.Errorf("cached retry body length = %d, want %d", len(thirdBody), len(body))
			}
			if got := thirdHeaders.Get("X-Cache"); got != XCacheHit {
				t.Errorf("healthy populate was not served from cache: X-Cache = %q", got)
			}
			if got := originGets.Load(); got != 2 {
				t.Errorf("origin GETs = %d, want 2", got)
			}
		})
	}
}
