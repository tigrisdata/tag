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

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

type testStreamCacheClient struct {
	cacheclient.CacheClient
	stream func(context.Context, string, io.Writer) error
}

func (c *testStreamCacheClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	return c.stream(ctx, key, w)
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
}
