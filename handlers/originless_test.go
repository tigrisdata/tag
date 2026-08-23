package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

// newOriginlessServer builds the real stack in origin-less mode: config with no
// upstream, the origin-less forwarder, a live in-memory cache, and the server's
// actual route table. Tests drive the router directly, so what they prove is the
// property the design rests on — the proxying handlers are never entered.
func newOriginlessServer(t *testing.T) (*Server, *cache.Cache) {
	t.Helper()
	cfg := config.NewDefault()
	cfg.Upstream.Disabled = true
	cfg.Upstream.Endpoint = ""

	c := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	fwd := proxy.NewForwarder(nil, "", "auto", 10, nil, nil)
	svc := proxy.NewService(fwd, c, cfg)
	return NewServer(svc, "127.0.0.1", 0, false, 0), c
}

func seedObject(t *testing.T, c *cache.Cache, key, acl string, body []byte) {
	t.Helper()
	meta := &cache.CachedObjectMeta{
		Bucket: "b", Key: key, StatusCode: http.StatusOK,
		ETag: `"abc"`, ContentLength: int64(len(body)),
		ContentType: "text/plain", ACL: acl,
		CachedAt: time.Now().Unix(), LastModified: time.Now().Unix(),
	}
	if err := c.PutWithMeta(context.Background(), "b", key, meta, body, 60); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
}

func do(s *Server, method, target string, hdr map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, nil)
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, r)
	return w
}

func TestOriginlessRoutes_ReadsServeFromCacheAlone(t *testing.T) {
	s, c := newOriginlessServer(t)
	seedObject(t, c, "pub.txt", "public-read", []byte("hello world"))

	// Miss: the final answer, fast, NoSuchKey.
	if w := do(s, http.MethodGet, "/b/absent.txt", nil); w.Code != http.StatusNotFound || !strings.Contains(w.Body.String(), "NoSuchKey") {
		t.Fatalf("miss: code=%d body=%q", w.Code, w.Body.String())
	}

	// Hit: served with body and HIT header.
	w := do(s, http.MethodGet, "/b/pub.txt", nil)
	if w.Code != http.StatusOK || w.Body.String() != "hello world" {
		t.Fatalf("hit: code=%d body=%q", w.Code, w.Body.String())
	}
	if w.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("hit must be marked X-Cache: HIT, got %q", w.Header().Get("X-Cache"))
	}

	// HEAD: metadata only.
	if w := do(s, http.MethodHead, "/b/pub.txt", nil); w.Code != http.StatusOK || w.Header().Get("ETag") == "" {
		t.Fatalf("head: code=%d etag=%q", w.Code, w.Header().Get("ETag"))
	}

	// Range over a whole-cached object.
	w = do(s, http.MethodGet, "/b/pub.txt", map[string]string{"Range": "bytes=0-4"})
	if w.Code != http.StatusPartialContent || w.Body.String() != "hello" {
		t.Fatalf("range: code=%d body=%q", w.Code, w.Body.String())
	}

	// Conditional: If-None-Match on the cached ETag.
	if w := do(s, http.MethodGet, "/b/pub.txt", map[string]string{"If-None-Match": `"abc"`}); w.Code != http.StatusNotModified {
		t.Fatalf("if-none-match: code=%d", w.Code)
	}

	// Cache-Control from the client is not consulted: there is no upstream to
	// revalidate against, and the cached copy is the only copy. Both directives
	// used to be able to turn a healthy cached object into a 404.
	for _, cc := range []string{"no-cache", "no-store", "max-age=0"} {
		if w := do(s, http.MethodGet, "/b/pub.txt", map[string]string{"Cache-Control": cc}); w.Code != http.StatusOK {
			t.Fatalf("Cache-Control %q must be ignored, got %d", cc, w.Code)
		}
	}
}

func TestOriginlessRoutes_TrustModelIsAnonymousPublicReadOnly(t *testing.T) {
	s, c := newOriginlessServer(t)
	seedObject(t, c, "private.txt", "", []byte("secret"))

	// Anonymous read of a non-public object: NoSuchKey, indistinguishable from absent.
	if w := do(s, http.MethodGet, "/b/private.txt", nil); w.Code != http.StatusNotFound {
		t.Fatalf("anonymous private read: code=%d", w.Code)
	}
	// A signed request cannot be validated without an upstream: same answer.
	hdr := map[string]string{"Authorization": "AWS4-HMAC-SHA256 Credential=AKIA/20260823/auto/s3/aws4_request, SignedHeaders=host, Signature=deadbeef"}
	if w := do(s, http.MethodGet, "/b/private.txt", hdr); w.Code != http.StatusNotFound {
		t.Fatalf("signed private read: code=%d", w.Code)
	}
}

// The property the router dispatch exists for: mutations are rejected at the
// route table, so the proxying mutation handlers — which invalidate BEFORE
// forwarding — are never entered, and cached data cannot be destroyed by a
// rejected write. Under the guard-based design this failure needed five separate
// guards; here it is structural.
func TestOriginlessRoutes_MutationsNeverReachHandlersOrTouchCache(t *testing.T) {
	s, c := newOriginlessServer(t)
	seedObject(t, c, "k1.txt", "public-read", []byte("v1"))
	seedObject(t, c, "k2.txt", "public-read", []byte("v2"))

	mutations := []struct {
		name, method, target string
	}{
		{"put", http.MethodPut, "/b/k1.txt"},
		{"delete", http.MethodDelete, "/b/k1.txt"},
		{"bulk delete", http.MethodPost, "/b?delete"},
		{"copy", http.MethodPut, "/b/k1.txt"}, // copy is a PUT; header irrelevant, route rejects it
		{"initiate multipart", http.MethodPost, "/b/k1.txt?uploads"},
		{"complete multipart", http.MethodPost, "/b/k1.txt?uploadId=u1"},
		{"upload part", http.MethodPut, "/b/k1.txt?uploadId=u1&partNumber=1"},
	}
	for _, m := range mutations {
		if w := do(s, m.method, m.target, nil); w.Code != http.StatusNotImplemented {
			t.Fatalf("%s: code=%d, want 501", m.name, w.Code)
		}
	}

	// Listings and subresources are operations, not objects.
	for _, target := range []string{"/b?list-type=2", "/", "/b/k1.txt?tagging", "/b/k1.txt?acl"} {
		if w := do(s, http.MethodGet, target, nil); w.Code != http.StatusNotImplemented {
			t.Fatalf("GET %s: code=%d, want 501", target, w.Code)
		}
	}

	// The seeded entries must have survived every rejected request above.
	for _, key := range []string{"k1.txt", "k2.txt"} {
		if w := do(s, http.MethodGet, "/b/"+key, nil); w.Code != http.StatusOK {
			t.Fatalf("entry %q was damaged by a rejected mutation: code=%d", key, w.Code)
		}
	}
}
