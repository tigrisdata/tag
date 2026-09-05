package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// newTieredTestService builds a Service in tiered mode with an in-memory cache
// and the given small/large tier boundary.
func newTieredTestService(forwarder RequestForwarder, threshold int64) (*Service, *cache.Cache) {
	cfg := config.NewDefault()
	cfg.Mode = config.ModeTiered
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SizeThreshold = threshold

	memCache := cacheclient.NewMemoryCache()
	c := cache.NewCacheWithClient(memCache, &cfg.Cache)
	svc := NewService(forwarder, c, cfg)
	return svc, c
}

// tieredMock wires a mockForwarder that counts upstream traffic and answers
// like a healthy upstream: PUT → 200 + ETag, GET → 200 + body, DELETE → 204.
func tieredMock() (*mockForwarder, *atomic.Int64, *atomic.Int64) {
	forwards := &atomic.Int64{}
	deletes := &atomic.Int64{}
	m := &mockForwarder{
		forwardFunc: func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
			forwards.Add(1)
			switch r.Method {
			case http.MethodPut:
				w.Header().Set("ETag", `"upstream-etag"`)
				w.WriteHeader(http.StatusOK)
			case http.MethodDelete:
				w.WriteHeader(http.StatusNoContent)
			default:
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("upstream body"))
			}
			return nil
		},
		doObjectDeleteFunc: func(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error) {
			deletes.Add(1)
			return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
		},
	}
	return m, forwards, deletes
}

func tieredDo(t *testing.T, svc *Service, method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body != "" || method == http.MethodPut {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()

	var err error
	switch method {
	case http.MethodGet, http.MethodHead:
		err = svc.HandleGetObject(w, req)
		if method == http.MethodHead {
			w = httptest.NewRecorder()
			req = httptest.NewRequest(method, path, nil)
			for k, v := range hdr {
				req.Header.Set(k, v)
			}
			err = svc.HandleHeadObject(w, req)
		}
	case http.MethodPut:
		err = svc.HandlePutObject(w, req)
	case http.MethodDelete:
		err = svc.HandleDeleteObject(w, req)
	}
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return w
}

// A small object's whole lifecycle — PUT, GET, HEAD, DELETE, GET-after-delete —
// must complete without a single upstream request.
func TestTieredSmallObjectLifecycleZeroUpstream(t *testing.T) {
	mock, forwards, deletes := tieredMock()
	svc, _ := newTieredTestService(mock, 1024)

	if w := tieredDo(t, svc, http.MethodPut, "/b/small", "hello tiered", nil); w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d", w.Code)
	}
	w := tieredDo(t, svc, http.MethodGet, "/b/small", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "hello tiered" {
		t.Fatalf("GET status = %d body %q", w.Code, w.Body.String())
	}
	if w := tieredDo(t, svc, http.MethodHead, "/b/small", "", nil); w.Code != http.StatusOK {
		t.Fatalf("HEAD status = %d", w.Code)
	}
	if w := tieredDo(t, svc, http.MethodDelete, "/b/small", "", nil); w.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d", w.Code)
	}
	if w := tieredDo(t, svc, http.MethodGet, "/b/small", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("GET after DELETE status = %d, want 404", w.Code)
	}

	if n := forwards.Load(); n != 0 {
		t.Fatalf("upstream forwards = %d, want 0", n)
	}
	if n := deletes.Load(); n != 0 {
		t.Fatalf("upstream deletes = %d, want 0", n)
	}
}

// A metadata miss is the authoritative answer: NoSuchKey, zero upstream.
func TestTieredMetaMissIsLocal404(t *testing.T) {
	mock, forwards, _ := tieredMock()
	svc, _ := newTieredTestService(mock, 1024)

	if w := tieredDo(t, svc, http.MethodGet, "/b/absent", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("GET status = %d, want 404", w.Code)
	}
	if w := tieredDo(t, svc, http.MethodHead, "/b/absent", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("HEAD status = %d, want 404", w.Code)
	}
	if n := forwards.Load(); n != 0 {
		t.Fatalf("upstream forwards = %d, want 0", n)
	}
}

// A large PUT passes through and stamps a BodyUpstream marker; HEAD then
// answers from the marker (no forward) while GET forwards for the body.
func TestTieredLargePutStampsMarker(t *testing.T) {
	mock, forwards, _ := tieredMock()
	svc, c := newTieredTestService(mock, 8)

	body := "sixteen byte body!"
	if w := tieredDo(t, svc, http.MethodPut, "/b/large", body, nil); w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d", w.Code)
	}
	if n := forwards.Load(); n != 1 {
		t.Fatalf("forwards after large PUT = %d, want 1", n)
	}

	meta, found, err := c.GetMeta(context.Background(), "b", "large")
	if err != nil || !found || meta == nil {
		t.Fatalf("marker meta not found: found=%v err=%v", found, err)
	}
	if !meta.BodyUpstream {
		t.Fatal("marker meta.BodyUpstream = false, want true")
	}
	if meta.ETag != `"upstream-etag"` {
		t.Fatalf("marker ETag = %q", meta.ETag)
	}
	if meta.ContentLength != int64(len(body)) {
		t.Fatalf("marker ContentLength = %d, want %d", meta.ContentLength, len(body))
	}

	if w := tieredDo(t, svc, http.MethodHead, "/b/large", "", nil); w.Code != http.StatusOK {
		t.Fatalf("HEAD status = %d", w.Code)
	}
	if n := forwards.Load(); n != 1 {
		t.Fatalf("forwards after HEAD = %d, want 1 (HEAD must serve from marker)", n)
	}

	w := tieredDo(t, svc, http.MethodGet, "/b/large", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "upstream body" {
		t.Fatalf("GET status = %d body %q", w.Code, w.Body.String())
	}
	if n := forwards.Load(); n != 2 {
		t.Fatalf("forwards after GET = %d, want 2 (GET body forwards)", n)
	}
}

// A small write over an upstream-tier object must delete the upstream copy.
func TestTieredSmallOverUpstreamCleansUp(t *testing.T) {
	mock, _, _ := tieredMock()
	deleted := make(chan string, 1)
	mock.doObjectDeleteFunc = func(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error) {
		deleted <- bucket + "/" + key
		return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
	}
	svc, c := newTieredTestService(mock, 8)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusOK {
		t.Fatalf("large PUT status = %d", w.Code)
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "tiny", nil); w.Code != http.StatusOK {
		t.Fatalf("small PUT status = %d", w.Code)
	}

	select {
	case got := <-deleted:
		if got != "b/obj" {
			t.Fatalf("cross-tier delete for %q, want b/obj", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cross-tier upstream delete never issued")
	}

	meta, found, err := c.GetMeta(context.Background(), "b", "obj")
	if err != nil || !found || meta == nil {
		t.Fatalf("meta not found after small overwrite: found=%v err=%v", found, err)
	}
	if meta.BodyUpstream {
		t.Fatal("meta still marked BodyUpstream after small overwrite")
	}
	if w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil); w.Body.String() != "tiny" {
		t.Fatalf("GET body = %q, want local copy", w.Body.String())
	}
}

// A large write over a local-tier object replaces it with an upstream marker.
func TestTieredLargeOverLocalReplacesMeta(t *testing.T) {
	mock, _, deletes := tieredMock()
	svc, c := newTieredTestService(mock, 8)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "tiny", nil); w.Code != http.StatusOK {
		t.Fatalf("small PUT status = %d", w.Code)
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusOK {
		t.Fatalf("large PUT status = %d", w.Code)
	}

	meta, found, err := c.GetMeta(context.Background(), "b", "obj")
	if err != nil || !found || meta == nil {
		t.Fatalf("meta not found after large overwrite: found=%v err=%v", found, err)
	}
	if !meta.BodyUpstream {
		t.Fatal("meta.BodyUpstream = false after large overwrite")
	}
	if n := deletes.Load(); n != 0 {
		t.Fatalf("upstream deletes = %d, want 0 (local displacement is free)", n)
	}
}

// Requests TAG cannot validate forward to upstream — the auth authority —
// even when the object is cached locally.
func TestTieredUnvalidatedForwards(t *testing.T) {
	mock, forwards, _ := tieredMock()
	svc, _ := newTieredTestService(mock, 1024)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "cached", nil); w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d", w.Code)
	}
	base := forwards.Load()

	mock.validateFunc = func(r *http.Request) (AuthResult, string, string, error) {
		return AuthNotValidated, "", "", nil
	}
	w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil)
	if w.Body.String() != "upstream body" {
		t.Fatalf("GET body = %q, want the forwarded upstream response", w.Body.String())
	}
	if n := forwards.Load(); n != base+1 {
		t.Fatalf("forwards = %d, want %d (unvalidated GET must forward)", n, base+1)
	}
}

// Deleting an upstream-tier object forwards the DELETE and drops the marker.
func TestTieredDeleteUpstreamTierForwards(t *testing.T) {
	mock, forwards, _ := tieredMock()
	svc, c := newTieredTestService(mock, 8)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusOK {
		t.Fatalf("large PUT status = %d", w.Code)
	}
	base := forwards.Load()

	if w := tieredDo(t, svc, http.MethodDelete, "/b/obj", "", nil); w.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d", w.Code)
	}
	if n := forwards.Load(); n != base+1 {
		t.Fatalf("forwards = %d, want %d (upstream-tier DELETE must forward)", n, base+1)
	}
	if _, found, _ := c.GetMeta(context.Background(), "b", "obj"); found {
		t.Fatal("marker meta still present after DELETE")
	}
}

// A conditional write against an upstream-tier prior forwards: only upstream
// can evaluate preconditions against the version it owns.
func TestTieredConditionalPutOverUpstreamForwards(t *testing.T) {
	mock, forwards, _ := tieredMock()
	svc, c := newTieredTestService(mock, 8)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusOK {
		t.Fatalf("large PUT status = %d", w.Code)
	}
	base := forwards.Load()

	w := tieredDo(t, svc, http.MethodPut, "/b/obj", "tiny", map[string]string{"If-Match": `"upstream-etag"`})
	if w.Code != http.StatusOK {
		t.Fatalf("conditional PUT status = %d", w.Code)
	}
	if n := forwards.Load(); n != base+1 {
		t.Fatalf("forwards = %d, want %d (conditional PUT over upstream prior must forward)", n, base+1)
	}
	meta, found, _ := c.GetMeta(context.Background(), "b", "obj")
	if !found || meta == nil || !meta.BodyUpstream {
		t.Fatal("expected a refreshed upstream marker after forwarded conditional PUT")
	}
}
