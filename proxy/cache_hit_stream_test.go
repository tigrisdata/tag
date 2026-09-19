package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
)

type testStreamCacheClient struct {
	cacheclient.CacheClient
	stream func(context.Context, string, io.Writer) error
}

func (c *testStreamCacheClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	return c.stream(ctx, key, w)
}

type testRangeStreamCacheClient struct {
	cacheclient.CacheClient
	rangeStream func(context.Context, string, int64, int64, io.Writer) error
}

func (c *testRangeStreamCacheClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	return c.rangeStream(ctx, key, start, end, w)
}

func newRangeStreamCacheService(t *testing.T, body []byte, stream func(context.Context, string, int64, int64, io.Writer) error) (*Service, *cache.Cache, *cache.CachedObjectMeta) {
	t.Helper()

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	base := cacheclient.NewMemoryCache()
	client := &testRangeStreamCacheClient{CacheClient: base, rangeStream: stream}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	meta := &cache.CachedObjectMeta{
		Bucket:        "bucket",
		Key:           "key",
		ETag:          `"etag"`,
		ContentType:   "text/plain",
		ContentLength: int64(len(body)),
		StatusCode:    http.StatusOK,
	}
	if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, body, 0); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	return NewService(&mockForwarder{}, store, cfg), store, meta
}

func newLargeStreamCacheService(t *testing.T, body []byte, stream func(context.Context, string, io.Writer) error) (*Service, *cache.Cache, *cache.CachedObjectMeta) {
	t.Helper()

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	base := cacheclient.NewMemoryCache()
	client := &testStreamCacheClient{CacheClient: base, stream: stream}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	meta := &cache.CachedObjectMeta{
		Bucket:        "bucket",
		Key:           "key",
		ETag:          `"etag"`,
		ContentType:   "text/plain",
		ContentLength: int64(len(body)),
		StatusCode:    http.StatusOK,
	}
	if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, body, 0); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	return NewService(&mockForwarder{}, store, cfg), store, meta
}

func TestServeFromCache_LargeObject(t *testing.T) {
	body := bytes.Repeat([]byte("cached body"), 8192)
	stream := func(_ context.Context, _ string, w io.Writer) error {
		if _, err := w.Write(nil); err != nil {
			return err
		}
		for _, chunk := range [][]byte{body[:1], body[1:32768], body[32768:]} {
			if _, err := w.Write(chunk); err != nil {
				return err
			}
		}
		return nil
	}
	svc, _, meta := newLargeStreamCacheService(t, body, stream)

	w := httptest.NewRecorder()
	if err := svc.serveFromCache(context.Background(), w, meta.Bucket, meta.Key, meta, time.Now()); err != nil {
		t.Fatalf("serveFromCache() error = %v", err)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if !bytes.Equal(w.Body.Bytes(), body) {
		t.Fatalf("body mismatch: got %d bytes, want %d", w.Body.Len(), len(body))
	}
}

func TestServeRangeFromCache_ErrorBeforeFirstByte(t *testing.T) {
	body := bytes.Repeat([]byte("cached body"), 8192)
	wantErr := errors.New("cache range stream failed before first byte")
	stream := func(_ context.Context, _ string, _, _ int64, _ io.Writer) error {
		return wantErr
	}
	svc, _, meta := newRangeStreamCacheService(t, body, stream)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	served, err := svc.serveRangeFromCache(context.Background(), w, r, meta.Bucket, meta.Key, meta, "bytes=0-32767", time.Now())
	if served {
		t.Fatal("range with no body must not be served")
	}
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("serveRangeFromCache() error = %v, want wrapped %v", err, wantErr)
	}
	if got := w.Header().Get(XCacheHeader); got != "" {
		t.Fatalf("%s = %q, want no cache status before first byte", XCacheHeader, got)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("body length = %d, want 0", w.Body.Len())
	}
}

func TestServeRangeFromCache_ErrorAfterFirstByteKeepsCommittedResponse(t *testing.T) {
	body := bytes.Repeat([]byte("cached body"), 8192)
	firstChunk := body[:1024]
	wantErr := errors.New("cache range stream failed after first byte")
	stream := func(_ context.Context, _ string, _, _ int64, w io.Writer) error {
		if _, err := w.Write(firstChunk); err != nil {
			return err
		}
		return wantErr
	}
	svc, _, meta := newRangeStreamCacheService(t, body, stream)

	beforeBytes := transferredBytes(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	served, err := svc.serveRangeFromCache(context.Background(), w, r, meta.Bucket, meta.Key, meta, "bytes=0-32767", time.Now())
	if !served {
		t.Fatal("range with a committed first chunk must be served")
	}
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("serveRangeFromCache() error = %v, want %v", err, wantErr)
	}
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusPartialContent)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if got := w.Header().Get("Content-Length"); got != "32768" {
		t.Fatalf("Content-Length = %q, want 32768", got)
	}
	if got := w.Header().Get("Content-Range"); got != "bytes 0-32767/90112" {
		t.Fatalf("Content-Range = %q, want bytes 0-32767/90112", got)
	}
	if !bytes.Equal(w.Body.Bytes(), firstChunk) {
		t.Fatalf("body mismatch: got %d bytes, want %d", w.Body.Len(), len(firstChunk))
	}
	if got := transferredBytes(t) - beforeBytes; got != float64(len(firstChunk)) {
		t.Fatalf("bytes transferred delta = %v, want %d", got, len(firstChunk))
	}
}

func TestHandleGetObject_RangeCacheHitPostCommitErrorDoesNotFallBack(t *testing.T) {
	body := bytes.Repeat([]byte("cached body"), 8192)
	firstChunk := body[:1024]
	streamErr := errors.New("cache range stream failed after first byte")
	stream := func(_ context.Context, _ string, _, _ int64, w io.Writer) error {
		if _, err := w.Write(firstChunk); err != nil {
			return err
		}
		return streamErr
	}
	svc, _, _ := newRangeStreamCacheService(t, body, stream)

	var upstreamCalls atomic.Int32
	svc.forwarder = &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return nil, errors.New("unexpected upstream request")
		},
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	r.Header.Set("Range", "bytes=0-32767")
	if err := svc.HandleGetObject(w, r); err == nil || !errors.Is(err, streamErr) {
		t.Fatalf("HandleGetObject() error = %v, want %v", err, streamErr)
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Fatalf("upstream calls = %d, want 0 after committed cache response", got)
	}
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusPartialContent)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if got := w.Header().Get("Content-Length"); got != "32768" {
		t.Fatalf("Content-Length = %q, want 32768", got)
	}
	if !bytes.Equal(w.Body.Bytes(), firstChunk) {
		t.Fatalf("body mismatch: got %d bytes, want %d", w.Body.Len(), len(firstChunk))
	}
}

func TestHandleGetObject_EmptyRangeCacheHitFallsBackBeforeHeaders(t *testing.T) {
	body := bytes.Repeat([]byte("cached body"), 8192)
	stream := func(_ context.Context, _ string, _, _ int64, _ io.Writer) error {
		return nil
	}
	svc, _, _ := newRangeStreamCacheService(t, body, stream)

	upstreamBody := []byte("upstream range")
	var upstreamCalls atomic.Int32
	svc.forwarder = &mockForwarder{
		validateFunc: func(*http.Request) (AuthResult, string, string, error) {
			return AuthValidated, "", "", nil
		},
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return &http.Response{
				StatusCode: http.StatusPartialContent,
				Header: http.Header{
					"Content-Length": []string{"14"},
					"Content-Range":  []string{"bytes 0-13/14"},
					"Content-Type":   []string{"text/plain"},
				},
				Body: io.NopCloser(bytes.NewReader(upstreamBody)),
			}, nil
		},
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	r.Header.Set("Range", "bytes=0-13")
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusPartialContent)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheMiss {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheMiss)
	}
	if !bytes.Equal(w.Body.Bytes(), upstreamBody) {
		t.Fatalf("body = %q, want %q", w.Body.Bytes(), upstreamBody)
	}
}

func TestServeFromCache_LargeObjectUncommittedBeforeFirstByte(t *testing.T) {
	body := bytes.Repeat([]byte("cached body"), 8192)
	wantErr := errors.New("cache stream failed before first byte")
	stream := func(_ context.Context, _ string, _ io.Writer) error {
		return wantErr
	}
	svc, _, meta := newLargeStreamCacheService(t, body, stream)

	w := httptest.NewRecorder()
	err := svc.serveFromCache(context.Background(), w, meta.Bucket, meta.Key, meta, time.Now())
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("serveFromCache() error = %v, want wrapped %v", err, wantErr)
	}
	if got := w.Header().Get(XCacheHeader); got != "" {
		t.Fatalf("%s = %q, want no cache status before first byte", XCacheHeader, got)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("body length = %d, want 0", w.Body.Len())
	}
}

func TestHandleGetObject_LargeEmptyCacheHitFallsBackBeforeHeaders(t *testing.T) {
	body := bytes.Repeat([]byte("cached body"), 8192)
	stream := func(_ context.Context, _ string, _ io.Writer) error {
		return nil
	}
	svc, _, _ := newLargeStreamCacheService(t, body, stream)

	var upstreamCalls atomic.Int32
	forwarder := &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header: http.Header{
					"Content-Length": []string{"14"},
					"Content-Type":   []string{"text/plain"},
					"ETag":           []string{`"upstream"`},
				},
				Body: io.NopCloser(bytes.NewReader([]byte("upstream body"))),
			}, nil
		},
	}
	svc.forwarder = forwarder

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheMiss {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheMiss)
	}
	if got := w.Body.String(); got != "upstream body" {
		t.Fatalf("body = %q, want %q", got, "upstream body")
	}
}

func TestHandleGetObject_LargeCacheHitPostCommitErrorDoesNotFallBack(t *testing.T) {
	body := bytes.Repeat([]byte("cached body"), 8192)
	beforeErrors := requestCount(t, "error")
	beforeSuccesses := requestCount(t, "success")
	firstChunk := body[:32768]
	streamErr := errors.New("cache stream failed after first byte")
	stream := func(_ context.Context, _ string, w io.Writer) error {
		if _, err := w.Write(firstChunk); err != nil {
			return err
		}
		return streamErr
	}
	svc, _, _ := newLargeStreamCacheService(t, body, stream)

	var upstreamCalls atomic.Int32
	svc.forwarder = &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return nil, errors.New("unexpected upstream request")
		},
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Fatalf("upstream calls = %d, want 0 after committed cache response", got)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if !bytes.Equal(w.Body.Bytes(), firstChunk) {
		t.Fatalf("body mismatch: got %d bytes, want %d", w.Body.Len(), len(firstChunk))
	}
	if got := requestCount(t, "error"); got != beforeErrors+1 {
		t.Fatalf("error request count = %v, want %v", got, beforeErrors+1)
	}
	if got := requestCount(t, "success"); got != beforeSuccesses {
		t.Fatalf("success request count = %v, want %v", got, beforeSuccesses)
	}
}

func transferredBytes(t *testing.T) float64 {
	t.Helper()

	var metric dto.Metric
	if err := metrics.BytesTransferred.WithLabelValues("out").Write(&metric); err != nil {
		t.Fatalf("read transferred bytes: %v", err)
	}
	return metric.GetCounter().GetValue()
}

func requestCount(t *testing.T, status string) float64 {
	t.Helper()

	var metric dto.Metric
	// mode defaults to "unknown" in tests (SetMode is a main.go concern); the
	// cache-hit paths this test exercises record source=local.
	if err := metrics.RequestsTotal.WithLabelValues("GetObject", status, "unknown", metrics.SourceLocal).Write(&metric); err != nil {
		t.Fatalf("read %s request count: %v", status, err)
	}
	return metric.GetCounter().GetValue()
}
