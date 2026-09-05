// Copyright 2025 Tigris Data, Inc.

package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tigrisdata/tag/cache"
)

// forbidForward wires every upstream-touching mockForwarder hook to fail the
// test: with Cache-Control: only-if-cached the origin must never be contacted.
func forbidForward(t *testing.T) *mockForwarder {
	t.Helper()
	fail := func() { t.Error("origin contacted despite Cache-Control: only-if-cached") }
	return &mockForwarder{
		forwardFunc: func(context.Context, http.ResponseWriter, *http.Request) error {
			fail()
			return errors.New("forbidden forward")
		},
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			fail()
			return nil, errors.New("forbidden forward")
		},
	}
}

func putOnlyIfCachedObject(t *testing.T, c *cache.Cache, bucket, key, body string) *cache.CachedObjectMeta {
	t.Helper()
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"oic-etag"`,
		ContentType:   "text/plain",
		ContentLength: int64(len(body)),
		StatusCode:    http.StatusOK,
	}
	if err := c.PutWithMeta(context.Background(), bucket, key, meta, []byte(body), 0); err != nil {
		t.Fatalf("PutWithMeta() error = %v", err)
	}
	return meta
}

func TestOnlyIfCached_GetHitServesFromCache(t *testing.T) {
	mock := forbidForward(t)
	svc, c := newTestService(mock, true)
	putOnlyIfCachedObject(t, c, "b", "k", "hello world!")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	r.Header.Set("Cache-Control", "only-if-cached")
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("X-Cache = %q, want %q", got, XCacheHit)
	}
	if w.Body.String() != "hello world!" {
		t.Errorf("body = %q, want %q", w.Body.String(), "hello world!")
	}
	if mock.conditionalCalled {
		t.Error("conditional revalidation must not run under only-if-cached")
	}
}

func TestOnlyIfCached_GetMissAnswers504Locally(t *testing.T) {
	mock := forbidForward(t)
	svc, _ := newTestService(mock, true)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/b/absent", nil)
	r.Header.Set("Cache-Control", "only-if-cached")
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if w.Code != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", w.Code)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheMiss {
		t.Errorf("X-Cache = %q, want %q", got, XCacheMiss)
	}
}

func TestOnlyIfCached_OverridesNoCacheRevalidation(t *testing.T) {
	mock := forbidForward(t)
	svc, c := newTestService(mock, true)
	putOnlyIfCachedObject(t, c, "b", "k", "cached body")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	r.Header.Set("Cache-Control", "no-cache, only-if-cached")
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (stored response, no revalidation)", w.Code)
	}
	if mock.conditionalCalled {
		t.Error("no-cache must not force revalidation when only-if-cached forbids origin contact")
	}
}

func TestOnlyIfCached_NoStoreStillAnswers504(t *testing.T) {
	// no-store forbids using the cache and only-if-cached forbids the origin:
	// nothing can be served, locally and definitively.
	mock := forbidForward(t)
	svc, c := newTestService(mock, true)
	putOnlyIfCachedObject(t, c, "b", "k", "cached body")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	r.Header.Set("Cache-Control", "no-store, only-if-cached")
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if w.Code != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", w.Code)
	}
}

func TestOnlyIfCached_HeadHitServesMetadata(t *testing.T) {
	mock := forbidForward(t)
	svc, c := newTestService(mock, true)
	putOnlyIfCachedObject(t, c, "b", "k", "hello world!")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodHead, "/b/k", nil)
	r.Header.Set("Cache-Control", "only-if-cached")
	if err := svc.HandleHeadObject(w, r); err != nil {
		t.Fatalf("HandleHeadObject() error = %v", err)
	}
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("X-Cache = %q, want %q", got, XCacheHit)
	}
}

func TestOnlyIfCached_HeadMissAnswers504Locally(t *testing.T) {
	mock := forbidForward(t)
	svc, _ := newTestService(mock, true)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodHead, "/b/absent", nil)
	r.Header.Set("Cache-Control", "only-if-cached")
	if err := svc.HandleHeadObject(w, r); err != nil {
		t.Fatalf("HandleHeadObject() error = %v", err)
	}
	if w.Code != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", w.Code)
	}
}

func TestOnlyIfCached_CacheDisabledAnswers504(t *testing.T) {
	mock := forbidForward(t)
	svc, _ := newTestService(mock, false)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	r.Header.Set("Cache-Control", "only-if-cached")
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if w.Code != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", w.Code)
	}
}
