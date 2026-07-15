package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// mockForwarder implements RequestForwarder for revalidation unit tests.
type mockForwarder struct {
	conditionalResp *http.Response
	conditionalErr  error
	// Track calls for verification
	conditionalCalled bool
	conditionalETag   string
	// Optional Forward implementation for fallback tests
	forwardFunc func(ctx context.Context, w http.ResponseWriter, r *http.Request) error
}

func (m *mockForwarder) Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	if m.forwardFunc != nil {
		return m.forwardFunc(ctx, w, r)
	}
	return nil
}

func (m *mockForwarder) ForwardWithCapture(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
	return nil, nil
}

func (m *mockForwarder) ValidateAndGetCredentials(r *http.Request) (AuthResult, string, string, error) {
	return AuthValidated, "access", "secret", nil
}

func (m *mockForwarder) DoRequestWithCreds(ctx context.Context, r *http.Request, accessKey, secretKey string) (*http.Response, error) {
	return nil, nil
}

func (m *mockForwarder) DoFullObjectRequest(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error) {
	return nil, errors.New("mock: DoFullObjectRequest not implemented")
}

func (m *mockForwarder) DoConditionalGetRequest(ctx context.Context, bucket, key, accessKey, secretKey, etag string, lastModified int64, rangeHeader string) (*http.Response, error) {
	m.conditionalCalled = true
	m.conditionalETag = etag
	if m.conditionalErr != nil {
		return nil, m.conditionalErr
	}
	return m.conditionalResp, nil
}

func (m *mockForwarder) DoConditionalHeadRequest(ctx context.Context, bucket, key, accessKey, secretKey, etag string, lastModified int64) (*http.Response, error) {
	m.conditionalCalled = true
	m.conditionalETag = etag
	if m.conditionalErr != nil {
		return nil, m.conditionalErr
	}
	return m.conditionalResp, nil
}

// newTestService creates a Service with an in-memory cache for unit tests.
func newTestService(forwarder RequestForwarder, cacheEnabled bool) (*Service, *cache.Cache) {
	cfg := config.NewDefault()
	if !cacheEnabled {
		enabled := false
		cfg.Cache.Enabled = &enabled
	}

	var c *cache.Cache
	if cacheEnabled {
		memCache := cacheclient.NewMemoryCache()
		c = cache.NewCacheWithClient(memCache, &cfg.Cache)
	} else {
		c = cache.NewDisabledCache()
	}

	svc := NewService(forwarder, c, cfg)
	return svc, c
}

func TestRevalidation304_ServesCachedBody(t *testing.T) {
	// Setup: mock upstream returns 304 Not Modified
	mock := &mockForwarder{
		conditionalResp: &http.Response{
			StatusCode: http.StatusNotModified,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     http.Header{},
		},
	}

	svc, c := newTestService(mock, true)
	ctx := context.Background()
	bucket, key := "test-bucket", "test-key"

	// Pre-populate cache with metadata and body
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"abc123"`,
		ContentType:   "text/plain",
		ContentLength: 12,
		StatusCode:    http.StatusOK,
	}
	body := []byte("hello world!")
	err := c.PutWithMeta(ctx, bucket, key, meta, body, 0)
	if err != nil {
		t.Fatalf("PutWithMeta() error = %v", err)
	}

	// Execute revalidation
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	err = svc.revalidateAndServe(ctx, w, r, bucket, key, "access", "secret", meta, time.Now())
	if err != nil {
		t.Fatalf("revalidateAndServe() error = %v", err)
	}

	// Verify: conditional request was made
	if !mock.conditionalCalled {
		t.Error("expected conditional GET to be called")
	}
	if mock.conditionalETag != `"abc123"` {
		t.Errorf("conditional ETag = %q, want %q", mock.conditionalETag, `"abc123"`)
	}

	// Verify: response served from cache
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("X-Cache = %q, want %q", got, XCacheHit)
	}
	if w.Body.String() != "hello world!" {
		t.Errorf("body = %q, want %q", w.Body.String(), "hello world!")
	}
}

func TestRevalidation200_StreamsNewBody(t *testing.T) {
	// Setup: mock upstream returns 200 with new body
	newBody := "new content from upstream"
	mock := &mockForwarder{
		conditionalResp: &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(newBody)),
			Header: http.Header{
				"Content-Type":   []string{"text/plain"},
				"Content-Length": []string{"24"},
				"Etag":           []string{`"newetag"`},
			},
		},
	}

	svc, _ := newTestService(mock, true)
	ctx := context.Background()
	bucket, key := "test-bucket", "test-key"

	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"oldetag"`,
		ContentType:   "text/plain",
		ContentLength: 12,
		StatusCode:    http.StatusOK,
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	err := svc.revalidateAndServe(ctx, w, r, bucket, key, "access", "secret", meta, time.Now())
	if err != nil {
		t.Fatalf("revalidateAndServe() error = %v", err)
	}

	// Verify: new body streamed to client
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheRevalidated {
		t.Errorf("X-Cache = %q, want %q", got, XCacheRevalidated)
	}
	if w.Body.String() != newBody {
		t.Errorf("body = %q, want %q", w.Body.String(), newBody)
	}
}

func TestRevalidation200_UncacheableDeletesStale(t *testing.T) {
	// Setup: mock upstream returns 200 with Cache-Control: no-store (uncacheable)
	newBody := "uncacheable new content"
	mock := &mockForwarder{
		conditionalResp: &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(newBody)),
			Header: http.Header{
				"Content-Type":   []string{"text/plain"},
				"Content-Length": []string{"22"},
				"Etag":           []string{`"newetag"`},
				"Cache-Control":  []string{"no-store"},
			},
		},
	}

	svc, c := newTestService(mock, true)
	ctx := context.Background()
	bucket, key := "test-bucket", "test-key"

	// Pre-populate cache with stale entry
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"oldetag"`,
		ContentType:   "text/plain",
		ContentLength: 10,
		StatusCode:    http.StatusOK,
	}
	_ = c.PutWithMeta(ctx, bucket, key, meta, []byte("stale data"), 0)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	err := svc.revalidateAndServe(ctx, w, r, bucket, key, "access", "secret", meta, time.Now())
	if err != nil {
		t.Fatalf("revalidateAndServe() error = %v", err)
	}

	// Verify: new body streamed to client
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if w.Body.String() != newBody {
		t.Errorf("body = %q, want %q", w.Body.String(), newBody)
	}

	// Verify: stale cache entry was deleted even though new response is uncacheable
	_, found, _ := c.GetMeta(ctx, bucket, key)
	if found {
		t.Error("expected stale cache entry to be deleted when upstream returns uncacheable 200")
	}
}

func TestRevalidationError_ServesStale(t *testing.T) {
	// Setup: mock upstream returns error
	mock := &mockForwarder{
		conditionalErr: errors.New("upstream connection failed"),
	}

	svc, c := newTestService(mock, true)
	ctx := context.Background()
	bucket, key := "test-bucket", "test-key"

	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"abc123"`,
		ContentType:   "text/plain",
		ContentLength: 10,
		StatusCode:    http.StatusOK,
	}
	_ = c.PutWithMeta(ctx, bucket, key, meta, []byte("stale data"), 0)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	err := svc.revalidateAndServe(ctx, w, r, bucket, key, "access", "secret", meta, time.Now())
	if err != nil {
		t.Fatalf("revalidateAndServe() error = %v", err)
	}

	// Verify: stale data served
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("X-Cache = %q, want %q (stale fallback)", got, XCacheHit)
	}
	if w.Body.String() != "stale data" {
		t.Errorf("body = %q, want %q", w.Body.String(), "stale data")
	}
}

func TestRevalidationUnexpectedStatus_ServesStale(t *testing.T) {
	// Setup: mock upstream returns 500
	mock := &mockForwarder{
		conditionalResp: &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       io.NopCloser(strings.NewReader("server error")),
			Header:     http.Header{},
		},
	}

	svc, c := newTestService(mock, true)
	ctx := context.Background()
	bucket, key := "test-bucket", "test-key"

	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"abc123"`,
		ContentType:   "text/plain",
		ContentLength: 10,
		StatusCode:    http.StatusOK,
	}
	_ = c.PutWithMeta(ctx, bucket, key, meta, []byte("stale data"), 0)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	err := svc.revalidateAndServe(ctx, w, r, bucket, key, "access", "secret", meta, time.Now())
	if err != nil {
		t.Fatalf("revalidateAndServe() error = %v", err)
	}

	// Verify: stale data served on unexpected status
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("X-Cache = %q, want %q", got, XCacheHit)
	}
}

func TestRevalidateAndServeHead_304(t *testing.T) {
	mock := &mockForwarder{
		conditionalResp: &http.Response{
			StatusCode: http.StatusNotModified,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     http.Header{},
		},
	}

	svc, c := newTestService(mock, true)
	ctx := context.Background()
	bucket, key := "test-bucket", "test-key"

	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"abc123"`,
		ContentType:   "text/plain",
		ContentLength: 100,
		StatusCode:    http.StatusOK,
	}
	_ = c.PutWithMeta(ctx, bucket, key, meta, make([]byte, 100), 0)

	w := httptest.NewRecorder()
	err := svc.revalidateAndServeHead(ctx, w, bucket, key, "access", "secret", meta, time.Now())
	if err != nil {
		t.Fatalf("revalidateAndServeHead() error = %v", err)
	}

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("X-Cache = %q, want %q", got, XCacheHit)
	}
	// HEAD: no body
	if w.Body.Len() != 0 {
		t.Errorf("HEAD body length = %d, want 0", w.Body.Len())
	}
}

func TestRevalidateAndServeHead_200(t *testing.T) {
	mock := &mockForwarder{
		conditionalResp: &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Header: http.Header{
				"Content-Type":   []string{"application/json"},
				"Content-Length": []string{"200"},
				"Etag":           []string{`"newetag"`},
			},
		},
	}

	svc, c := newTestService(mock, true)
	ctx := context.Background()
	bucket, key := "test-bucket", "test-key"

	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"oldetag"`,
		ContentType:   "text/plain",
		ContentLength: 100,
		StatusCode:    http.StatusOK,
	}
	_ = c.PutWithMeta(ctx, bucket, key, meta, make([]byte, 100), 0)

	w := httptest.NewRecorder()
	err := svc.revalidateAndServeHead(ctx, w, bucket, key, "access", "secret", meta, time.Now())
	if err != nil {
		t.Fatalf("revalidateAndServeHead() error = %v", err)
	}

	// Verify: new headers from upstream
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheRevalidated {
		t.Errorf("X-Cache = %q, want %q", got, XCacheRevalidated)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want %q", got, "application/json")
	}

	// Verify: stale cache entry was invalidated
	_, found, _ := c.GetMeta(ctx, bucket, key)
	if found {
		t.Error("expected cache entry to be deleted after HEAD revalidation 200")
	}
}

func TestRevalidateAndServeHead_Error_ServesStale(t *testing.T) {
	mock := &mockForwarder{
		conditionalErr: errors.New("upstream timeout"),
	}

	svc, _ := newTestService(mock, true)
	ctx := context.Background()
	bucket, key := "test-bucket", "test-key"

	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"abc123"`,
		ContentType:   "text/plain",
		ContentLength: 100,
		StatusCode:    http.StatusOK,
	}

	w := httptest.NewRecorder()
	err := svc.revalidateAndServeHead(ctx, w, bucket, key, "access", "secret", meta, time.Now())
	if err != nil {
		t.Fatalf("revalidateAndServeHead() error = %v", err)
	}

	// Verify: stale headers served
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("X-Cache = %q, want %q (stale fallback)", got, XCacheHit)
	}
}

func TestServeFromCache_ZeroByteObject(t *testing.T) {
	mock := &mockForwarder{}
	svc, _ := newTestService(mock, true)

	meta := &cache.CachedObjectMeta{
		Bucket:        "b",
		Key:           "k",
		ContentType:   "text/plain",
		ContentLength: 0,
		StatusCode:    http.StatusOK,
	}

	w := httptest.NewRecorder()
	err := svc.serveFromCache(context.Background(), w, "b", "k", meta, time.Now())
	if err != nil {
		t.Fatalf("serveFromCache() error = %v", err)
	}

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("X-Cache = %q, want %q", got, XCacheHit)
	}
	if w.Body.Len() != 0 {
		t.Errorf("body length = %d, want 0", w.Body.Len())
	}
}

func TestServeFromCache_SmallObject(t *testing.T) {
	mock := &mockForwarder{}
	svc, c := newTestService(mock, true)
	ctx := context.Background()
	bucket, key := "b", "k"

	body := []byte("small cached body")
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ContentType:   "text/plain",
		ContentLength: int64(len(body)),
		StatusCode:    http.StatusOK,
	}
	_ = c.PutWithMeta(ctx, bucket, key, meta, body, 0)

	w := httptest.NewRecorder()
	err := svc.serveFromCache(ctx, w, bucket, key, meta, time.Now())
	if err != nil {
		t.Fatalf("serveFromCache() error = %v", err)
	}

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if w.Body.String() != "small cached body" {
		t.Errorf("body = %q, want %q", w.Body.String(), "small cached body")
	}
}

func TestRevalidationRange304_ServesCachedRange(t *testing.T) {
	// Setup: mock upstream returns 304 Not Modified (object unchanged)
	mock := &mockForwarder{
		conditionalResp: &http.Response{
			StatusCode: http.StatusNotModified,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     http.Header{},
		},
	}

	svc, c := newTestService(mock, true)
	ctx := context.Background()
	bucket, key := "test-bucket", "test-key"

	// Pre-populate cache with full object
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"abc123"`,
		ContentType:   "text/plain",
		ContentLength: 12,
		StatusCode:    http.StatusOK,
	}
	body := []byte("hello world!")
	err := c.PutWithMeta(ctx, bucket, key, meta, body, 0)
	if err != nil {
		t.Fatalf("PutWithMeta() error = %v", err)
	}

	// Request with Range header + revalidation
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	r.Header.Set("Range", "bytes=0-4")
	err = svc.revalidateAndServe(ctx, w, r, bucket, key, "access", "secret", meta, time.Now())
	if err != nil {
		t.Fatalf("revalidateAndServe() error = %v", err)
	}

	// Verify: conditional request was made
	if !mock.conditionalCalled {
		t.Error("expected conditional GET to be called")
	}

	// Verify: 206 Partial Content with correct range
	if w.Code != http.StatusPartialContent {
		t.Errorf("status = %d, want %d", w.Code, http.StatusPartialContent)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("X-Cache = %q, want %q", got, XCacheHit)
	}
	if w.Body.String() != "hello" {
		t.Errorf("body = %q, want %q", w.Body.String(), "hello")
	}
	if got := w.Header().Get("Content-Range"); got == "" {
		t.Error("expected Content-Range header to be set")
	}
}

func TestRevalidationRange206_StreamsRangeFromUpstream(t *testing.T) {
	// Setup: mock upstream returns 206 Partial Content (object changed, range returned)
	rangeBody := "new range!"
	mock := &mockForwarder{
		conditionalResp: &http.Response{
			StatusCode: http.StatusPartialContent,
			Body:       io.NopCloser(strings.NewReader(rangeBody)),
			Header: http.Header{
				"Content-Type":   []string{"text/plain"},
				"Content-Length": []string{"10"},
				"Content-Range":  []string{"bytes 0-9/100"},
				"Etag":           []string{`"newetag"`},
			},
		},
	}

	svc, c := newTestService(mock, true)
	ctx := context.Background()
	bucket, key := "test-bucket", "test-key"

	// Pre-populate cache with old object
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"oldetag"`,
		ContentType:   "text/plain",
		ContentLength: 50,
		StatusCode:    http.StatusOK,
	}
	_ = c.PutWithMeta(ctx, bucket, key, meta, []byte("old cached content that is now stale and outdated!"), 0)

	// Request with Range header + revalidation
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	r.Header.Set("Range", "bytes=0-9")
	err := svc.revalidateAndServe(ctx, w, r, bucket, key, "access", "secret", meta, time.Now())
	if err != nil {
		t.Fatalf("revalidateAndServe() error = %v", err)
	}

	// Verify: 206 with new range body from upstream
	if w.Code != http.StatusPartialContent {
		t.Errorf("status = %d, want %d", w.Code, http.StatusPartialContent)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheRevalidated {
		t.Errorf("X-Cache = %q, want %q", got, XCacheRevalidated)
	}
	if w.Body.String() != rangeBody {
		t.Errorf("body = %q, want %q", w.Body.String(), rangeBody)
	}

	// Verify: stale cache entry was deleted
	_, found, _ := c.GetMeta(ctx, bucket, key)
	if found {
		t.Error("expected stale cache entry to be deleted after 206 revalidation")
	}
}

func TestRevalidationRangeError_ServesStaleRange(t *testing.T) {
	// Setup: mock upstream returns error
	mock := &mockForwarder{
		conditionalErr: errors.New("upstream connection failed"),
	}

	svc, c := newTestService(mock, true)
	ctx := context.Background()
	bucket, key := "test-bucket", "test-key"

	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"abc123"`,
		ContentType:   "text/plain",
		ContentLength: 10,
		StatusCode:    http.StatusOK,
	}
	_ = c.PutWithMeta(ctx, bucket, key, meta, []byte("stale data"), 0)

	// Request with Range header + revalidation (upstream fails)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	r.Header.Set("Range", "bytes=0-4")
	err := svc.revalidateAndServe(ctx, w, r, bucket, key, "access", "secret", meta, time.Now())
	if err != nil {
		t.Fatalf("revalidateAndServe() error = %v", err)
	}

	// Verify: stale range served from cache
	if w.Code != http.StatusPartialContent {
		t.Errorf("status = %d, want %d", w.Code, http.StatusPartialContent)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("X-Cache = %q, want %q (stale fallback)", got, XCacheHit)
	}
	if w.Body.String() != "stale" {
		t.Errorf("body = %q, want %q", w.Body.String(), "stale")
	}
}

func TestRevalidation304_CacheBodyUnavailable_FallsThrough(t *testing.T) {
	// Setup: mock upstream returns 304, but cache body has been evicted.
	// Forward should be called as fallback to serve fresh content.
	freshBody := "fresh from upstream"
	mock := &mockForwarder{
		conditionalResp: &http.Response{
			StatusCode: http.StatusNotModified,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     http.Header{},
		},
		forwardFunc: func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(freshBody))
			return nil
		},
	}

	// Create cache with separate reference to underlying client for body deletion
	cfg := config.NewDefault()
	memCache := cacheclient.NewMemoryCache()
	c := cache.NewCacheWithClient(memCache, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	ctx := context.Background()
	bucket, key := "test-bucket", "test-key"

	// Pre-populate cache with metadata and body
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"abc123"`,
		ContentType:   "text/plain",
		ContentLength: 12,
		StatusCode:    http.StatusOK,
	}
	_ = c.PutWithMeta(ctx, bucket, key, meta, []byte("hello world!"), 0)

	// Delete only the body key to simulate cache body eviction
	_ = memCache.Delete(ctx, cache.MakeBodyKey(bucket, key, meta.ETag))

	// Execute revalidation — should fall through to Forward
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	err := svc.revalidateAndServe(ctx, w, r, bucket, key, "access", "secret", meta, time.Now())
	if err != nil {
		t.Fatalf("revalidateAndServe() error = %v (expected fallback to upstream)", err)
	}

	// Verify: fresh response from upstream (not InternalError)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheMiss {
		t.Errorf("X-Cache = %q, want %q (upstream fallback)", got, XCacheMiss)
	}
	if w.Body.String() != freshBody {
		t.Errorf("body = %q, want %q", w.Body.String(), freshBody)
	}
}
