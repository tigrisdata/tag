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
	"testing/synctest"
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

// embeddedStorageInlineThreshold mirrors ocache v1.13.0's default inline threshold.
const embeddedStorageInlineThreshold = 64 << 10

// deadlinePausedReader models a local embedded store that does not use ctx to
// interrupt a non-seekable read. It gives the storage sniff read seven bytes,
// then waits for the real cache deadline before reading the remaining pipe.
type deadlinePausedReader struct {
	reader          io.Reader
	ctx             context.Context
	firstRead       chan cachePutFailureResult
	deadlineReached chan struct{}
	resume          <-chan struct{}
	first           bool
	resumed         bool
}

func (r *deadlinePausedReader) Read(p []byte) (int, error) {
	if !r.first {
		r.first = true
		n, err := io.ReadFull(r.reader, p[:7])
		r.firstRead <- cachePutFailureResult{bytesRead: int64(n), readErr: err}
		return n, err
	}
	if !r.resumed {
		<-r.ctx.Done()
		close(r.deadlineReached)
		<-r.resume
		r.resumed = true
	}
	return r.reader.Read(p)
}

// deadlinePausedCacheClient mirrors the local embedded storage path: it reads
// threshold+1 bytes, accepts EOF/ErrUnexpectedEOF for inline values, and ignores
// ctx for the local body and CAS metadata writes. MemoryCache backs stored values.
type deadlinePausedCacheClient struct {
	cacheclient.CacheClient
	calls           atomic.Int32
	firstRead       chan cachePutFailureResult
	finalRead       chan cachePutFailureResult
	deadlineReached chan struct{}
	resume          chan struct{}
}

func (c *deadlinePausedCacheClient) PutStream(ctx context.Context, key string, r io.Reader, ttl int64) error {
	if c.calls.Add(1) != 1 {
		return c.CacheClient.PutStream(ctx, key, r, ttl)
	}

	paused := &deadlinePausedReader{
		reader:          r,
		ctx:             ctx,
		firstRead:       c.firstRead,
		deadlineReached: c.deadlineReached,
		resume:          c.resume,
	}
	firstChunk := make([]byte, embeddedStorageInlineThreshold+1)
	n, err := io.ReadFull(paused, firstChunk)
	result := cachePutFailureResult{bytesRead: int64(n), readErr: err}
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		c.finalRead <- result
		return err
	}

	var body bytes.Buffer
	_, _ = body.Write(firstChunk[:n])
	if n > embeddedStorageInlineThreshold {
		more, copyErr := io.Copy(&body, paused)
		result.bytesRead += more
		result.readErr = copyErr
		c.finalRead <- result
		if copyErr != nil {
			return copyErr
		}
	} else {
		// Storage.Put accepts a short inline value when ReadFull returns EOF or
		// ErrUnexpectedEOF. The test verifies that a context error is not mistaken
		// for either and allowed to publish metadata.
		c.finalRead <- result
	}
	return c.CacheClient.PutStream(context.Background(), key, bytes.NewReader(body.Bytes()), ttl)
}

func (c *deadlinePausedCacheClient) PutIfVersion(_ context.Context, key string, data []byte, ttl int64, expected uint64) (uint64, error) {
	return c.CacheClient.PutIfVersion(context.Background(), key, data, ttl, expected)
}

func TestCacheableFullGet_DeadlineDoesNotPublishPartialBody(t *testing.T) {
	tests := []struct {
		name      string
		bodySize  int
		chunkSize int
	}{
		{name: "raw_file", bodySize: 128 << 10, chunkSize: 1024},
		{name: "inline", bodySize: 40 << 10, chunkSize: 512},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const (
					bucket        = "cache-deadline-bucket"
					key           = "cache-deadline-key"
					channelBuffer = 256
				)
				bodyText := strings.Repeat("x", tt.bodySize)
				cfg := config.NewDefault()
				cfg.Mode = config.ModeTransparent
				cfg.Cache.BlockSize = config.DefaultCacheBlockSize
				if cfg.IsTiered() || int64(tt.bodySize) >= cfg.Cache.BlockSize {
					t.Fatal("test must exercise a non-tiered whole-body populate")
				}
				cfg.Cache.SetLegacyCoordination(false)
				cfg.Cache.MaxConcurrentWrites = 1
				cfg.Cache.MaxPopulateMemoryBytes = int64(tt.bodySize)
				cfg.Broadcast.ChunkSize = tt.chunkSize
				cfg.Broadcast.ChannelBuffer = channelBuffer
				if tt.bodySize/tt.chunkSize <= channelBuffer/4+1 {
					t.Fatalf("%d chunks do not fill the 64-entry cache queue", tt.bodySize/tt.chunkSize)
				}

				deadlineErr := context.DeadlineExceeded
				cacheClient := &deadlinePausedCacheClient{
					CacheClient:     cacheclient.NewMemoryCache(),
					firstRead:       make(chan cachePutFailureResult, 1),
					finalRead:       make(chan cachePutFailureResult, 1),
					deadlineReached: make(chan struct{}),
					resume:          make(chan struct{}),
				}
				cacheStore := cache.NewCacheWithClient(cacheClient, &cfg.Cache)
				forwarder := &mockForwarder{
					doRequestFunc: func(_ context.Context, _ *http.Request, _ string, _ string) (*http.Response, error) {
						return cacheableGetResponse(bodyText, `"cache-deadline-etag"`), nil
					},
				}
				svc := NewService(forwarder, cacheStore, cfg)

				logs := &cacheWriteFailureLogWriter{
					errorText: deadlineErr.Error(),
					returned:  make(chan struct{}, 1),
					observed:  make(chan struct{}, 1),
				}
				previousLogger := log.Logger
				log.Logger = zerolog.New(logs).Level(zerolog.DebugLevel)
				defer func() { log.Logger = previousLogger }()

				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
				if err := svc.HandleGetObject(recorder, request); err != nil {
					t.Fatalf("HandleGetObject: %v", err)
				}
				response := recorder.Result()
				defer response.Body.Close()
				responseBody, err := io.ReadAll(response.Body)
				if err != nil {
					t.Fatalf("read GET response: %v", err)
				}
				if response.StatusCode != http.StatusOK || response.Header.Get("X-Cache") != XCacheMiss ||
					response.Header.Get("Content-Length") != strconv.Itoa(tt.bodySize) ||
					response.Header.Get("Content-Type") != "text/plain" ||
					response.Header.Get("ETag") != `"cache-deadline-etag"` || string(responseBody) != bodyText {
					t.Fatalf("GET response changed: status=%d headers=%v body-bytes=%d", response.StatusCode, response.Header, len(responseBody))
				}

				// The body has more than the 64-entry cache queue can hold. The store
				// consumes seven bytes then pauses, leaving the pipe write blocked and
				// the relay waiting to enqueue when the actual deadline fires.
				synctest.Wait()
				first := <-cacheClient.firstRead
				if first.bytesRead != 7 || first.readErr != nil {
					t.Fatalf("cache reader did not pause after seven bytes: %+v", first)
				}
				<-cacheClient.deadlineReached // synctest advances to the pending cache deadline
				synctest.Wait()
				close(cacheClient.resume)
				synctest.Wait()

				resumed := <-cacheClient.finalRead
				if resumed.bytesRead >= int64(tt.bodySize) {
					t.Fatalf("cache reader received %d bytes, want a truncated stream after deadline", resumed.bytesRead)
				}
				if !errors.Is(resumed.readErr, context.DeadlineExceeded) {
					t.Errorf("cache reader error = %v, want deadline error rather than clean EOF", resumed.readErr)
				}
				select {
				case <-logs.returned:
				default:
					t.Error("cache write did not report the deadline error")
				}
				select {
				case <-logs.observed:
				default:
					t.Error("cache deadline error was not observed after the GET completed")
				}

				if meta, found, err := cacheStore.GetMeta(context.Background(), bucket, key); err != nil {
					t.Errorf("GetMeta after timed-out populate: %v", err)
				} else if found {
					var cachedBody bytes.Buffer
					bodyErr := cacheStore.GetBodyStream(context.Background(), bucket, key, meta.ETag, &cachedBody)
					t.Errorf("timed-out partial body became visible: metadata length=%d, stored bytes=%d, body read error=%v", meta.ContentLength, cachedBody.Len(), bodyErr)
				}

				weight := svc.populateWeight(int64(tt.bodySize))
				if !svc.acquireCacheSlot(context.Background(), weight, priorityReadMiss) {
					t.Errorf("cache reservation (%d bytes) was not released after the cache reader exited", weight)
				} else {
					svc.releaseCacheSlot(weight)
				}
			})
		})
	}
}
