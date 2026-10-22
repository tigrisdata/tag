package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

func readCompletionResponse(t *testing.T, client *http.Client, url string) (http.Header, string) {
	t.Helper()

	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d", url, resp.StatusCode, http.StatusOK)
	}
	return resp.Header, string(body)
}

func waitForHandlerReturn(t *testing.T, requestName string, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handler %s: %v", requestName, err)
		}
	case <-time.After(time.Second):
		t.Fatalf("handler did not complete for %s", requestName)
	}
}

// TestCacheableFullGet_SubsequentRequestHitsCache verifies that a cacheable
// full-GET populate remains live after the first HTTP handler returns. The
// requests use independent HTTP connections and share a cache key, so the later
// request must be served from the published entry instead of upstream.
func TestCacheableFullGet_SubsequentRequestHitsCache(t *testing.T) {
	const (
		bucket = "completion-bucket"
		key    = "completion-key"
		body   = "cacheable completion body"
	)

	var originGets atomic.Int32
	forwarder := &mockForwarder{
		doRequestFunc: func(_ context.Context, _ *http.Request, _ string, _ string) (*http.Response, error) {
			originGets.Add(1)
			return cacheableGetResponse(body, `"completion-etag"`), nil
		},
	}
	cfg := config.NewDefault()
	cacheStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	svc := NewService(forwarder, cacheStore, cfg)
	completions := newHandlerCompletionTracker()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := svc.HandleGetObject(w, r)
		completions.complete(err)
	}))
	defer server.Close()

	// Separate transports give the two GETs independent connections without
	// closing the first connection before its cache listener has started.
	firstClient := &http.Client{
		Transport: &http.Transport{},
		Timeout:   time.Second,
	}
	defer firstClient.CloseIdleConnections()
	secondClient := &http.Client{
		Transport: &http.Transport{},
		Timeout:   time.Second,
	}
	defer secondClient.CloseIdleConnections()

	path := "/" + bucket + "/" + key
	firstHeaders, firstBody := readCompletionResponse(t, firstClient, server.URL+path)
	if firstBody != body {
		t.Errorf("first response body = %q, want %q", firstBody, body)
	}
	if got := firstHeaders.Get("X-Cache"); got != XCacheMiss {
		t.Errorf("first response X-Cache = %q, want %q", got, XCacheMiss)
	}

	// The body is complete, but cache publication is intentionally detached from
	// the request. Wait for its metadata commit, then use a separate connection to
	// verify that the completed populate serves the next request.
	waitForHandlerReturn(t, "first request", completions.done)
	waitForCacheMeta(t, cacheStore, bucket, key, time.Second)

	secondHeaders, secondBody := readCompletionResponse(t, secondClient, server.URL+path)
	if secondBody != body {
		t.Errorf("second response body = %q, want %q", secondBody, body)
	}
	if got := secondHeaders.Get("X-Cache"); got != XCacheHit {
		t.Errorf("second response X-Cache = %q, want %q", got, XCacheHit)
	}
	if got := originGets.Load(); got != 1 {
		t.Errorf("origin GETs = %d, want 1", got)
	}

	waitForHandlerReturn(t, "second request", completions.done)
}

// TestSetupCacheListener_PublishesWhenHeadersReadyAtRequestCancellation covers
// the scheduling boundary where streamFromUpstream has already published headers
// but the cache goroutine first runs after the HTTP handler returns. The request
// context is then canceled even though the cache listener has a complete response
// to consume, so headers must win over cancellation and the detached write must
// still publish the entry.
func TestSetupCacheListener_PublishesWhenHeadersReadyAtRequestCancellation(t *testing.T) {
	const (
		bucket = "header-cancel-bucket"
		key    = "header-cancel-key"
		body   = "cache listener survives request cancellation"
		etag   = `"header-cancel-etag"`
	)

	cfg := config.NewDefault()
	cfg.Cache.MaxConcurrentWrites = 1
	cacheStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	svc := NewService(&mockForwarder{}, cacheStore, cfg)
	broadcaster := broadcast.NewBroadcaster(cfg.Broadcast.ChannelBuffer)

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	broadcaster.SetHeaders(http.StatusOK, cacheableGetResponse(body, etag).Header)
	cancelRequest()

	_, cacheErrCh := svc.setupCacheListener(
		requestCtx,
		bucket,
		key,
		broadcaster,
		false,
		svc.populateWeight(int64(len(body))),
		cache.VersionAny,
	)
	if cacheErrCh == nil {
		t.Fatal("setupCacheListener did not create a cache listener")
	}

	broadcaster.Broadcast([]byte(body))
	broadcaster.Complete(nil)

	select {
	case err, ok := <-cacheErrCh:
		if !ok {
			t.Fatal("cache listener closed its result channel without a result")
		}
		if err != nil {
			t.Fatalf("cache listener: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cache listener did not finish")
	}

	meta, found, err := cacheStore.GetMeta(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if !found {
		t.Fatal("cache metadata was not published")
	}
	var cachedBody bytes.Buffer
	if err := cacheStore.GetBodyStream(context.Background(), bucket, key, meta.ETag, &cachedBody); err != nil {
		t.Fatalf("GetBodyStream: %v", err)
	}
	if got := cachedBody.String(); got != body {
		t.Errorf("cached body = %q, want %q", got, body)
	}

	if !svc.acquireCacheSlot(context.Background(), 1, priorityReadMiss) {
		t.Fatal("cache populate slot was not released")
	}
	svc.releaseCacheSlot(1)
}

// cancelingPipeStreamCache models a remote cache owner that stops consuming a
// streamed write as soon as its service context is canceled. The retained reader
// lets the test release the relay if Stop fails to close the pipe on that return.
type cancelingPipeStreamCache struct {
	cacheclient.CacheClient
	readerStarted chan struct{}
	reader        chan io.Reader
	writerExited  chan struct{}
}

func (c *cancelingPipeStreamCache) PutStream(ctx context.Context, _ string, r io.Reader, _ int64) error {
	var firstByte [1]byte
	n, err := r.Read(firstByte[:])
	if n == 0 {
		if err != nil {
			return err
		}
		return io.ErrNoProgress
	}

	c.reader <- r
	close(c.readerStarted)
	<-ctx.Done()
	close(c.writerExited)
	return ctx.Err()
}

// TestServiceStopUnblocksFullGetWhenCacheWriterStopsReading covers a full GET
// whose detached cache writer has begun a pipe read but returns on shutdown
// without consuming the rest of the body. The HTTP response must finish normally;
// Stop must then join the abandoned relay rather than waiting on its blocked write.
func TestServiceStopUnblocksFullGetWhenCacheWriterStopsReading(t *testing.T) {
	const body = "cache relay has more bytes than its reader consumes"

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Broadcast.ChunkSize = 8
	client := &cancelingPipeStreamCache{
		CacheClient:   cacheclient.NewMemoryCache(),
		readerStarted: make(chan struct{}),
		reader:        make(chan io.Reader, 1),
		writerExited:  make(chan struct{}),
	}
	cacheStore := cache.NewCacheWithClient(client, &cfg.Cache)
	forwarder := &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			return cacheableGetResponse(body, `"pipe-shutdown-etag"`), nil
		},
	}
	svc := NewService(forwarder, cacheStore, cfg)
	recorder := httptest.NewRecorder()
	requestDone := make(chan error, 1)
	go func() {
		requestDone <- svc.HandleGetObject(recorder, httptest.NewRequest(http.MethodGet, "/pipe-bucket/pipe-key", nil))
	}()

	select {
	case <-client.readerStarted:
	case <-time.After(time.Second):
		t.Fatal("full GET cache writer did not start reading the body")
	}
	select {
	case err := <-requestDone:
		if err != nil {
			t.Fatalf("HandleGetObject: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("full GET response did not finish while cache writer was held")
	}
	if recorder.Code != http.StatusOK || recorder.Body.String() != body {
		t.Fatalf("full GET response = (%d, %q), want (200, %q)", recorder.Code, recorder.Body.String(), body)
	}

	stopDone := make(chan struct{})
	go func() {
		svc.Stop()
		close(stopDone)
	}()
	select {
	case <-client.writerExited:
	case <-time.After(time.Second):
		t.Fatal("cache writer did not return after service cancellation")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		// The writer has returned, so no cache operation owns this reader any more.
		// Drain it only to unblock a failing implementation and leave no goroutine
		// behind after reporting the missing pipe-close handoff.
		if _, err := io.Copy(io.Discard, <-client.reader); err != nil {
			t.Errorf("drain abandoned test reader: %v", err)
		}
		select {
		case <-stopDone:
		case <-time.After(time.Second):
			t.Fatal("Service.Stop remained blocked after the abandoned reader was drained")
		}
		t.Fatal("Service.Stop did not join after the cache writer returned")
	}
}

// lateFailingStreamCache waits until the test has observed the client handler
// return, then fails the stream write. This forces the cache error to arrive
// after broadcaster completion rather than before the upstream EOF path returns.
type lateFailingStreamCache struct {
	cacheclient.CacheClient
	release <-chan struct{}
	err     error
}

func (c *lateFailingStreamCache) PutStream(ctx context.Context, _ string, r io.Reader, _ int64) error {
	if _, err := io.Copy(io.Discard, r); err != nil {
		return err
	}
	select {
	case <-c.release:
		return c.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type lateErrorLogWriter struct {
	errorText string
	logged    chan struct{}
}

func (w *lateErrorLogWriter) Write(p []byte) (int, error) {
	entry := string(p)
	if strings.Contains(entry, "Cache write failed") && strings.Contains(entry, w.errorText) {
		select {
		case w.logged <- struct{}{}:
		default:
		}
	}
	return len(p), nil
}

// TestStreamFromUpstream_LogsLateCacheWriteFailure ensures an asynchronous
// populate failure remains visible after the client handler is allowed to finish.
func TestStreamFromUpstream_LogsLateCacheWriteFailure(t *testing.T) {
	const body = "late cache write failure"
	injectedErr := errors.New("injected cache write failure")
	release := make(chan struct{})
	cacheClient := &lateFailingStreamCache{
		CacheClient: cacheclient.NewMemoryCache(),
		release:     release,
		err:         injectedErr,
	}
	cfg := config.NewDefault()
	cfg.Cache.MaxConcurrentWrites = 1
	svc := NewService(&mockForwarder{
		doRequestFunc: func(_ context.Context, _ *http.Request, _ string, _ string) (*http.Response, error) {
			return cacheableGetResponse(body, `"late-error-etag"`), nil
		},
	}, cache.NewCacheWithClient(cacheClient, &cfg.Cache), cfg)

	logs := &lateErrorLogWriter{
		errorText: injectedErr.Error(),
		logged:    make(chan struct{}, 1),
	}
	previousLogger := log.Logger
	log.Logger = zerolog.New(logs).Level(zerolog.WarnLevel)
	t.Cleanup(func() {
		log.Logger = previousLogger
	})

	handlerReturned := make(chan error, 1)
	writer := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/late-bucket/late-key", nil)
	go func() {
		handlerReturned <- svc.HandleGetObject(writer, request)
	}()

	select {
	case err := <-handlerReturned:
		if err != nil {
			t.Fatalf("HandleGetObject: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not return")
	}
	if got := writer.Body.String(); got != body {
		t.Fatalf("response body = %q, want %q", got, body)
	}

	close(release)
	select {
	case <-logs.logged:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for late cache write error")
	}

	// setupCacheListener owns the reserved slot even after the request handler
	// returns. Once its late error is observed, the slot must be available again.
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !svc.acquireCacheSlot(context.Background(), 1, priorityReadMiss) {
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for cache populate slot release")
		case <-ticker.C:
		}
	}
	svc.releaseCacheSlot(1)
}
