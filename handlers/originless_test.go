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

	// The SDK operation tag must not defeat the plain-read rule: aws-sdk-go-v2
	// appends ?x-id=GetObject to every GetObject, and the tigris-os gateway is
	// exactly such a client.
	if w := do(s, http.MethodGet, "/b/pub.txt?x-id=GetObject", nil); w.Code != http.StatusOK {
		t.Fatalf("SDK-tagged GET: code=%d, want 200", w.Code)
	}
	// But x-id does not launder other parameters through.
	if w := do(s, http.MethodGet, "/b/pub.txt?x-id=GetObject&versionId=abc", nil); w.Code != http.StatusNotImplemented {
		t.Fatalf("x-id plus versionId: code=%d, want 501", w.Code)
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

	// Listings, subresources, and representation selectors are operations, not
	// plain object reads. versionId and partNumber matter most: serving the
	// current full object for them would be silently wrong data.
	for _, target := range []string{"/b?list-type=2", "/", "/b/k1.txt?tagging", "/b/k1.txt?acl", "/b/k1.txt?versionId=abc", "/b/k1.txt?partNumber=2", "/b/k1.txt?attributes"} {
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

// A zero-length object has no bytes to probe, and the storage backend cannot
// distinguish a present empty body from an absent one. It must stay visible —
// nothing can be missing from it.
func TestOriginlessRoutes_EmptyObjectIsServableNotAMiss(t *testing.T) {
	s, c := newOriginlessServer(t)
	seedObject(t, c, "empty.txt", "public-read", []byte{})

	if w := do(s, http.MethodGet, "/b/empty.txt", nil); w.Code != http.StatusOK || w.Body.Len() != 0 {
		t.Fatalf("GET empty: code=%d len=%d, want 200/0", w.Code, w.Body.Len())
	}
	if w := do(s, http.MethodHead, "/b/empty.txt", nil); w.Code != http.StatusOK {
		t.Fatalf("HEAD empty: code=%d, want 200", w.Code)
	}
	if w := do(s, http.MethodGet, "/b/empty.txt", map[string]string{"If-None-Match": `"abc"`}); w.Code != http.StatusNotModified {
		t.Fatalf("INM empty: code=%d, want 304", w.Code)
	}
}

// The truncation hazard: block-mode serves stream optimistically — headers first,
// missing blocks recovered from upstream mid-stream. With no origin that recovery
// fails AFTER the 200/206 is committed, handing the client a truncated body. The
// origin-less handler must therefore probe every covering block first and answer a
// clean NoSuchKey before any header is written.
func TestOriginlessRoutes_IncompleteBlockEntryIsACleanMissNotATruncatedServe(t *testing.T) {
	s, c := newOriginlessServer(t)
	ctx := context.Background()

	blockSize := int64(1024)
	// Two blocks; only block 0 is written. BlocksComplete deliberately lies — that
	// is exactly the state (blocks evicted after the meta was stamped) that makes
	// the shared helpers stream optimistically.
	meta := &cache.CachedObjectMeta{
		Bucket: "b", Key: "partial.bin", StatusCode: http.StatusOK,
		ETag: `"blk"`, ContentLength: 2 * blockSize, ContentType: "application/octet-stream",
		ACL: "public-read", CachedAt: time.Now().Unix(), LastModified: time.Now().Unix(),
		BlockSize: blockSize, BlocksComplete: true,
	}
	if err := c.PutBlock(ctx, "b", "partial.bin", meta.ETag, blockSize, 0, make([]byte, blockSize), 60); err != nil {
		t.Fatalf("seed block 0: %v", err)
	}
	if ok, err := c.PutMetaTombstoneAware(ctx, "b", "partial.bin", meta, 60, time.Now().UnixNano()); err != nil || !ok {
		t.Fatalf("seed meta: ok=%v err=%v", ok, err)
	}

	// Full GET: block 1 is absent, so this must be a clean 404 — never a committed
	// 200 with a short body.
	w := do(s, http.MethodGet, "/b/partial.bin", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("full GET over incomplete blocks: code=%d body_len=%d, want clean 404", w.Code, w.Body.Len())
	}

	// The contract: an incomplete entry is INVISIBLE — every request shape answers
	// exactly as if the object were absent. Anything else lets one path imply an
	// existence another path denies, and a HEAD/304-as-existence caller (the
	// tigris-os distribute worker skips re-population when IsObjectExists is true)
	// would then never heal an entry that can never be served.
	shapes := map[string]*httptest.ResponseRecorder{
		"range over present block": do(s, http.MethodGet, "/b/partial.bin", map[string]string{"Range": "bytes=0-99"}),
		"range over absent block":  do(s, http.MethodGet, "/b/partial.bin", map[string]string{"Range": "bytes=1000-1100"}),
		"HEAD":                     do(s, http.MethodHead, "/b/partial.bin", nil),
		"If-None-Match":            do(s, http.MethodGet, "/b/partial.bin", map[string]string{"If-None-Match": `"blk"`}),
	}
	for name, w := range shapes {
		if w.Code != http.StatusNotFound {
			t.Fatalf("%s on incomplete entry: code=%d, want 404 (entry must be invisible)", name, w.Code)
		}
	}
}

// A whole-object (non-block) entry whose body is gone is metadata orphaned by an
// eviction: it must be as invisible as the incomplete block entry above, on every
// request shape — a HEAD 200 or a 304 from bare metadata would tell an existence
// checker to skip the re-population that would make the entry servable.
func TestOriginlessRoutes_BodylessWholeObjectEntryIsInvisible(t *testing.T) {
	s, c := newOriginlessServer(t)
	ctx := context.Background()

	// PutMetaTombstoneAware writes metadata alone — exactly the orphaned state a
	// body eviction leaves behind.
	meta := &cache.CachedObjectMeta{
		Bucket: "b", Key: "orphan.txt", StatusCode: http.StatusOK,
		ETag: `"orph"`, ContentLength: 11, ContentType: "text/plain",
		ACL: "public-read", CachedAt: time.Now().Unix(), LastModified: time.Now().Unix(),
	}
	if ok, err := c.PutMetaTombstoneAware(ctx, "b", "orphan.txt", meta, 60, time.Now().UnixNano()); err != nil || !ok {
		t.Fatalf("seed meta: ok=%v err=%v", ok, err)
	}

	for name, w := range map[string]*httptest.ResponseRecorder{
		"GET":           do(s, http.MethodGet, "/b/orphan.txt", nil),
		"HEAD":          do(s, http.MethodHead, "/b/orphan.txt", nil),
		"If-None-Match": do(s, http.MethodGet, "/b/orphan.txt", map[string]string{"If-None-Match": `"orph"`}),
	} {
		if w.Code != http.StatusNotFound {
			t.Fatalf("%s on body-less entry: code=%d, want 404", name, w.Code)
		}
	}
}

// A bad range on a PRESENT object is not a miss: the object exists, the range is
// wrong, and the answer is 416 InvalidRange — the probe must not short-circuit
// that into NoSuchKey and make a present object look absent.
func TestOriginlessRoutes_BadRangeOnPresentObjectIs416NotMiss(t *testing.T) {
	s, c := newOriginlessServer(t)
	ctx := context.Background()

	blockSize := int64(1024)
	meta := &cache.CachedObjectMeta{
		Bucket: "b", Key: "obj.bin", StatusCode: http.StatusOK,
		ETag: `"blk"`, ContentLength: blockSize, ContentType: "application/octet-stream",
		ACL: "public-read", CachedAt: time.Now().Unix(), LastModified: time.Now().Unix(),
		BlockSize: blockSize, BlocksComplete: true,
	}
	if err := c.PutBlock(ctx, "b", "obj.bin", meta.ETag, blockSize, 0, make([]byte, blockSize), 60); err != nil {
		t.Fatalf("seed block: %v", err)
	}
	if ok, err := c.PutMetaTombstoneAware(ctx, "b", "obj.bin", meta, 60, time.Now().UnixNano()); err != nil || !ok {
		t.Fatalf("seed meta: ok=%v err=%v", ok, err)
	}

	// HEAD on a complete block-mode entry still answers 200.
	if w := do(s, http.MethodHead, "/b/obj.bin", nil); w.Code != http.StatusOK {
		t.Fatalf("HEAD on complete entry: code=%d, want 200", w.Code)
	}

	for name, rng := range map[string]string{
		"unsatisfiable": "bytes=99999-100000",
		"malformed":     "bytes=notarange",
		"multi-range":   "bytes=0-1,10-11",
	} {
		w := do(s, http.MethodGet, "/b/obj.bin", map[string]string{"Range": rng})
		if w.Code != http.StatusRequestedRangeNotSatisfiable {
			t.Fatalf("%s range: code=%d, want 416", name, w.Code)
		}
	}
}
