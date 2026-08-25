package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	// appends ?x-id=GetObject to every GetObject, and any stock-SDK caller is
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

// The network is the trust boundary: reads serve regardless of the cached ACL
// and regardless of authentication. A signed request's signature cannot be
// validated without an upstream, so it is ignored rather than evaluated — the
// callers' S3 clients sign every request by default, and rejecting signed
// reads would 404 them all.
func TestOriginlessRoutes_NetworkTrustServesRegardlessOfACLAndAuth(t *testing.T) {
	s, c := newOriginlessServer(t)
	seedObject(t, c, "private.txt", "", []byte("secret"))

	if w := do(s, http.MethodGet, "/b/private.txt", nil); w.Code != http.StatusOK || w.Body.String() != "secret" {
		t.Fatalf("anonymous read of non-public entry: code=%d body=%q", w.Code, w.Body.String())
	}
	hdr := map[string]string{"Authorization": "AWS4-HMAC-SHA256 Credential=AKIA/20260823/auto/s3/aws4_request, SignedHeaders=host, Signature=deadbeef"}
	if w := do(s, http.MethodGet, "/b/private.txt", hdr); w.Code != http.StatusOK {
		t.Fatalf("signed read: code=%d, want 200 (auth ignored, not evaluated)", w.Code)
	}
}

// The population path: PUT stores into the local cache, GET serves it back,
// DELETE removes it. This is the production loop — callers write to and read
// from this tier directly.
func TestOriginlessRoutes_WriteReadDeleteLoop(t *testing.T) {
	s, _ := newOriginlessServer(t)

	// PUT with a body (the SDK's x-id tag must not break writes either).
	req := httptest.NewRequest(http.MethodPut, "/b/loop.txt?x-id=PutObject", strings.NewReader("written directly"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Amz-Meta-Origin", "gateway")
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Header().Get("ETag") == "" {
		t.Fatalf("PUT: code=%d etag=%q", w.Code, w.Header().Get("ETag"))
	}
	etag := w.Header().Get("ETag")

	// Read back: bytes, headers, and user metadata round-trip.
	g := do(s, http.MethodGet, "/b/loop.txt", nil)
	if g.Code != http.StatusOK || g.Body.String() != "written directly" {
		t.Fatalf("GET after PUT: code=%d body=%q", g.Code, g.Body.String())
	}
	if g.Header().Get("ETag") != etag {
		t.Fatalf("ETag: put=%q get=%q", etag, g.Header().Get("ETag"))
	}
	// Raw-map read: user metadata is written with a lowercase wire name (S3's
	// convention), which Header.Get's canonicalizing lookup cannot see.
	if got := g.Header()["x-amz-meta-origin"]; len(got) != 1 || got[0] != "gateway" {
		t.Fatalf("user metadata lost: %v", got)
	}

	// Overwrite: new content serves, new ETag.
	req = httptest.NewRequest(http.MethodPut, "/b/loop.txt", strings.NewReader("rewritten"))
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Header().Get("ETag") == etag {
		t.Fatalf("overwrite: code=%d etag unchanged=%v", w.Code, w.Header().Get("ETag") == etag)
	}
	if g := do(s, http.MethodGet, "/b/loop.txt", nil); g.Body.String() != "rewritten" {
		t.Fatalf("GET after overwrite: %q", g.Body.String())
	}

	// DELETE, then the entry is gone on every shape.
	if w := do(s, http.MethodDelete, "/b/loop.txt", nil); w.Code != http.StatusNoContent {
		t.Fatalf("DELETE: code=%d", w.Code)
	}
	if g := do(s, http.MethodGet, "/b/loop.txt", nil); g.Code != http.StatusNotFound {
		t.Fatalf("GET after DELETE: code=%d", g.Code)
	}
	if h := do(s, http.MethodHead, "/b/loop.txt", nil); h.Code != http.StatusNotFound {
		t.Fatalf("HEAD after DELETE: code=%d", h.Code)
	}
}

// The write surface stays narrow: copies, multipart parts, and oversized bodies
// are refused without touching cached data.
func TestOriginlessRoutes_WriteSurfaceLimits(t *testing.T) {
	s, c := newOriginlessServer(t)
	seedObject(t, c, "kept.txt", "", []byte("kept"))

	// Server-side copy: 501, and the destination entry is untouched.
	req := httptest.NewRequest(http.MethodPut, "/b/kept.txt", nil)
	req.Header.Set("X-Amz-Copy-Source", "/b/other.txt")
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("copy: code=%d, want 501", w.Code)
	}
	if g := do(s, http.MethodGet, "/b/kept.txt", nil); g.Code != http.StatusOK {
		t.Fatalf("destination after rejected copy: code=%d", g.Code)
	}

	// UploadPart (PUT with uploadId): 501.
	req = httptest.NewRequest(http.MethodPut, "/b/kept.txt?uploadId=u1&partNumber=1", strings.NewReader("part"))
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("upload part: code=%d, want 501", w.Code)
	}

	// A body over the size threshold: EntityTooLarge, nothing stored.
	big := bytes.Repeat([]byte("x"), 128)
	cfgLimited := config.NewDefault()
	cfgLimited.Upstream.Disabled = true
	cfgLimited.Upstream.Endpoint = ""
	cfgLimited.Cache.SizeThreshold = 64
	cLimited := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfgLimited.Cache)
	svc := proxy.NewService(proxy.NewForwarder(nil, "", "auto", 10, nil, nil), cLimited, cfgLimited)
	srv := NewServer(svc, "127.0.0.1", 0, false, 0)
	req = httptest.NewRequest(http.MethodPut, "/b/big.txt", bytes.NewReader(big))
	w = httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized PUT: code=%d, want 400 EntityTooLarge", w.Code)
	}
	if g := doOn(srv, http.MethodGet, "/b/big.txt"); g.Code != http.StatusNotFound {
		t.Fatalf("oversized PUT must store nothing: code=%d", g.Code)
	}
}

func doOn(s *Server, method, target string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, httptest.NewRequest(method, target, nil))
	return w
}

// Unsupported operations answer 501 at the route table and cannot touch cached
// data — the proxying mutation handlers (which invalidate before forwarding) are
// never entered. PUT and DELETE are no longer in this list: they are the mode's
// own write path, covered by the write/read/delete loop test.
func TestOriginlessRoutes_MutationsNeverReachHandlersOrTouchCache(t *testing.T) {
	s, c := newOriginlessServer(t)
	seedObject(t, c, "k1.txt", "public-read", []byte("v1"))
	seedObject(t, c, "k2.txt", "public-read", []byte("v2"))

	mutations := []struct {
		name, method, target string
	}{
		{"initiate multipart", http.MethodPost, "/b/k1.txt?uploads"},
		{"complete multipart", http.MethodPost, "/b/k1.txt?uploadId=u1"},
		{"upload part", http.MethodPut, "/b/k1.txt?uploadId=u1&partNumber=1"},
		{"tagging write", http.MethodPut, "/b/k1.txt?tagging"},
		{"acl write", http.MethodPut, "/b/k1.txt?acl"},
	}
	for _, m := range mutations {
		if w := do(s, m.method, m.target, nil); w.Code != http.StatusNotImplemented {
			t.Fatalf("%s: code=%d, want 501", m.name, w.Code)
		}
	}

	// Listings, subresources, and representation selectors are operations, not
	// plain object reads. versionId and partNumber matter most: serving the
	// current full object for them would be silently wrong data.
	// Bucket listing now works and has its own test; the service-level listing,
	// subresources, and representation selectors remain 501.
	for _, target := range []string{"/", "/b/k1.txt?tagging", "/b/k1.txt?acl", "/b/k1.txt?versionId=abc", "/b/k1.txt?partNumber=2", "/b/k1.txt?attributes"} {
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
	// caller that skips re-population when a HEAD says the object exists)
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

// A streaming-signed SDK upload frames the body with aws-chunked encoding. The
// stored bytes — and the ETag — must be the decoded object, never the framing:
// framed bytes served with a 200 are corruption the caller cannot detect.
func TestOriginlessRoutes_AWSChunkedPutStoresDecodedBody(t *testing.T) {
	s, _ := newOriginlessServer(t)

	payload := "decoded payload bytes"
	framed := fmt.Sprintf("%x;chunk-signature=deadbeef\r\n%s\r\n0;chunk-signature=deadbeef\r\n\r\n", len(payload), payload)

	req := httptest.NewRequest(http.MethodPut, "/b/chunked.txt", strings.NewReader(framed))
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(len(payload)))
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("chunked PUT: code=%d", w.Code)
	}

	g := do(s, http.MethodGet, "/b/chunked.txt", nil)
	if g.Code != http.StatusOK || g.Body.String() != payload {
		t.Fatalf("GET after chunked PUT: code=%d body=%q, want the DECODED payload", g.Code, g.Body.String())
	}

	// The decoded length is what the threshold judges: a decoded size over the
	// limit is rejected even when the wire bytes have not been read yet.
	req = httptest.NewRequest(http.MethodPut, "/b/big-chunked.txt", strings.NewReader(framed))
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(int64(1)<<40))
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized decoded length: code=%d, want 400 EntityTooLarge", w.Code)
	}
}

// Content-Encoding describes the stored bytes, so it must survive the round
// trip — minus the aws-chunked token, whose framing PUT decodes away. Dropping
// it entirely would serve gzip bytes that clients read as the raw object;
// keeping aws-chunked would advertise framing the stored bytes no longer carry.
func TestOriginlessRoutes_PutPreservesContentEncoding(t *testing.T) {
	s, _ := newOriginlessServer(t)

	// Plain upload of pre-encoded bytes: header round-trips verbatim.
	req := httptest.NewRequest(http.MethodPut, "/b/enc.gz", strings.NewReader("gzip-encoded bytes"))
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT with Content-Encoding: code=%d", w.Code)
	}
	if g := do(s, http.MethodGet, "/b/enc.gz", nil); g.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("GET Content-Encoding=%q, want gzip", g.Header().Get("Content-Encoding"))
	}

	// Streaming upload with a combined value: aws-chunked is stripped (that
	// layer was decoded), the remaining encoding survives.
	payload := "still gzip bytes"
	framed := fmt.Sprintf("%x;chunk-signature=deadbeef\r\n%s\r\n0;chunk-signature=deadbeef\r\n\r\n", len(payload), payload)
	req = httptest.NewRequest(http.MethodPut, "/b/enc2.gz", strings.NewReader(framed))
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	// Mixed case: content-coding tokens are case-insensitive (RFC 9110), and
	// decoding keys off the streaming SHA-256 marker, not the header spelling.
	req.Header.Set("Content-Encoding", "AWS-Chunked,gzip")
	req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(len(payload)))
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("chunked PUT with combined encoding: code=%d", w.Code)
	}
	g := do(s, http.MethodGet, "/b/enc2.gz", nil)
	if g.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("GET Content-Encoding=%q, want gzip (aws-chunked stripped)", g.Header().Get("Content-Encoding"))
	}
	if g.Body.String() != payload {
		t.Fatalf("body=%q, want decoded payload", g.Body.String())
	}

	// aws-chunked alone: no residual header on the stored object.
	req = httptest.NewRequest(http.MethodPut, "/b/enc3.txt", strings.NewReader(framed))
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(len(payload)))
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("chunked PUT: code=%d", w.Code)
	}
	if g := do(s, http.MethodGet, "/b/enc3.txt", nil); g.Header().Get("Content-Encoding") != "" {
		t.Fatalf("GET Content-Encoding=%q, want empty", g.Header().Get("Content-Encoding"))
	}
}

// The budget reservation equals the limiter's bound, so the declared length is
// load-bearing: a body that does not match it is the client misdescribing the
// request, and is refused rather than stored under a wrong ETag.
func TestOriginlessRoutes_PutDeclaredLengthIsEnforced(t *testing.T) {
	s, _ := newOriginlessServer(t)

	// Shorter than declared: IncompleteBody.
	req := httptest.NewRequest(http.MethodPut, "/b/short.txt", strings.NewReader("ten bytes!"))
	req.ContentLength = 100
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "IncompleteBody") {
		t.Fatalf("short body: code=%d body=%q", w.Code, w.Body.String())
	}

	// Longer than declared: the limiter detects the overrun at declared+1 — the
	// buffer never grows past what was admitted against the budget.
	req = httptest.NewRequest(http.MethodPut, "/b/long.txt", strings.NewReader("twenty bytes of body"))
	req.ContentLength = 5
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("over-declared body: code=%d", w.Code)
	}

	// Unknown length: 411, as on real S3 — with no declared size there is nothing
	// truthful to reserve.
	req = httptest.NewRequest(http.MethodPut, "/b/unknown.txt", io.NopCloser(strings.NewReader("x")))
	req.ContentLength = -1
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusLengthRequired {
		t.Fatalf("unknown length: code=%d, want 411", w.Code)
	}

	// None of the refusals stored anything.
	for _, k := range []string{"short.txt", "long.txt", "unknown.txt"} {
		if g := do(s, http.MethodGet, "/b/"+k, nil); g.Code != http.StatusNotFound {
			t.Fatalf("%s: refused PUT stored something (code=%d)", k, g.Code)
		}
	}
}

// An empty streaming upload is the SDK's default for empty bodies: the decoded
// length header says 0 and the wire carries only framing. It must store an empty
// object, not be rejected as IncompleteBody by its framing size.
func TestOriginlessRoutes_EmptyStreamingPutStoresEmptyObject(t *testing.T) {
	s, _ := newOriginlessServer(t)

	framed := "0;chunk-signature=deadbeef\r\n\r\n"
	req := httptest.NewRequest(http.MethodPut, "/b/empty-stream.txt", strings.NewReader(framed))
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("X-Amz-Decoded-Content-Length", "0")
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("empty streaming PUT: code=%d body=%q", w.Code, w.Body.String())
	}
	if g := do(s, http.MethodGet, "/b/empty-stream.txt", nil); g.Code != http.StatusOK || g.Body.Len() != 0 {
		t.Fatalf("read-back: code=%d len=%d", g.Code, g.Body.Len())
	}

	// A streaming PUT without the decoded-length header has nothing truthful to
	// reserve: 411.
	req = httptest.NewRequest(http.MethodPut, "/b/no-dcl.txt", strings.NewReader(framed))
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusLengthRequired {
		t.Fatalf("streaming without decoded length: code=%d, want 411", w.Code)
	}
}

// A declared size that can never fit the configured budget answers
// EntityTooLarge immediately — never a blocking wait, never SlowDown-forever.
func TestOriginlessRoutes_PutLargerThanBudgetFailsFast(t *testing.T) {
	cfg := config.NewDefault()
	cfg.Upstream.Disabled = true
	cfg.Upstream.Endpoint = ""
	cfg.Cache.SizeThreshold = 1 << 30
	cfg.Cache.MaxPopulateMemoryBytes = 1 << 20 // 1 MiB budget, threshold far larger
	c := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	svc := proxy.NewService(proxy.NewForwarder(nil, "", "auto", 10, nil, nil), c, cfg)
	srv := NewServer(svc, "127.0.0.1", 0, false, 0)

	req := httptest.NewRequest(http.MethodPut, "/b/huge.txt", strings.NewReader("tiny"))
	req.ContentLength = 512 << 20 // declared 512 MiB against a 1 MiB budget
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { srv.router.ServeHTTP(w, req); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("over-budget PUT hung instead of failing fast")
	}
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "EntityTooLarge") {
		t.Fatalf("over-budget PUT: code=%d body=%q", w.Code, w.Body.String())
	}
}

// The SDK's x-id tag rides on the bucket ceremony too; and per RFC 7232 a
// present If-Match suppresses If-Unmodified-Since entirely.
func TestOriginlessRoutes_BucketXIDAndPreconditionPrecedence(t *testing.T) {
	s, c := newOriginlessServer(t)

	if w := do(s, http.MethodPut, "/newbucket?x-id=CreateBucket", nil); w.Code != http.StatusOK {
		t.Fatalf("CreateBucket with x-id: code=%d", w.Code)
	}

	seedObject(t, c, "pc.txt", "", []byte("data"))
	// Matching If-Match + stale If-Unmodified-Since: must serve (If-Match wins).
	w := do(s, http.MethodGet, "/b/pc.txt", map[string]string{
		"If-Match":            `"abc"`,
		"If-Unmodified-Since": "Mon, 01 Jan 2001 00:00:00 GMT",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("If-Match match + stale IUS: code=%d, want 200", w.Code)
	}
	// If-Match mismatch: 412 regardless.
	if w := do(s, http.MethodGet, "/b/pc.txt", map[string]string{"If-Match": `"nope"`}); w.Code != http.StatusPreconditionFailed {
		t.Fatalf("If-Match mismatch: code=%d, want 412", w.Code)
	}
	// IUS alone, stale: 412.
	if w := do(s, http.MethodGet, "/b/pc.txt", map[string]string{"If-Unmodified-Since": "Mon, 01 Jan 2001 00:00:00 GMT"}); w.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale IUS alone: code=%d, want 412", w.Code)
	}
}

// Bucket listing serves from cached metadata: ordering, prefix, delimiter
// rollup, pagination via V2 continuation tokens, and V1 markers.
func TestOriginlessRoutes_ListObjects(t *testing.T) {
	s, c := newOriginlessServer(t)
	for _, k := range []string{"a/1.txt", "a/2.txt", "b/1.txt", "top.txt", "zed.txt"} {
		seedObject(t, c, k, "", []byte("x"))
	}

	get := func(target string) string {
		w := do(s, http.MethodGet, target, nil)
		if w.Code != http.StatusOK {
			t.Fatalf("GET %s: code=%d body=%q", target, w.Code, w.Body.String())
		}
		return w.Body.String()
	}

	// V2 plain: all five keys, sorted, KeyCount=5.
	body := get("/b?list-type=2")
	for _, want := range []string{"<Key>a/1.txt</Key>", "<Key>zed.txt</Key>", "<KeyCount>5</KeyCount>", "<IsTruncated>false</IsTruncated>"} {
		if !strings.Contains(body, want) {
			t.Fatalf("V2 listing missing %s in %s", want, body)
		}
	}

	// Delimiter rollup: two common prefixes + two top-level keys.
	body = get("/b?list-type=2&delimiter=%2F")
	for _, want := range []string{"<Prefix>a/</Prefix>", "<Prefix>b/</Prefix>", "<Key>top.txt</Key>", "<KeyCount>4</KeyCount>"} {
		if !strings.Contains(body, want) {
			t.Fatalf("delimiter listing missing %s in %s", want, body)
		}
	}
	if strings.Contains(body, "<Key>a/1.txt</Key>") {
		t.Fatal("rolled-up member leaked into Contents")
	}

	// Prefix filter.
	body = get("/b?list-type=2&prefix=a%2F")
	if !strings.Contains(body, "<KeyCount>2</KeyCount>") || strings.Contains(body, "top.txt") {
		t.Fatalf("prefix listing wrong: %s", body)
	}

	// Pagination: max-keys=2 pages through all five without loss or repeats.
	seen := map[string]bool{}
	token := ""
	for i := 0; i < 5; i++ {
		target := "/b?list-type=2&max-keys=2"
		if token != "" {
			target += "&continuation-token=" + token
		}
		body := get(target)
		for _, line := range strings.Split(body, "<Key>") {
			if idx := strings.Index(line, "</Key>"); idx > 0 {
				k := line[:idx]
				if seen[k] {
					t.Fatalf("key %q repeated across pages", k)
				}
				seen[k] = true
			}
		}
		if !strings.Contains(body, "<IsTruncated>true</IsTruncated>") {
			break
		}
		start := strings.Index(body, "<NextContinuationToken>") + len("<NextContinuationToken>")
		end := strings.Index(body, "</NextContinuationToken>")
		token = body[start:end]
	}
	if len(seen) != 5 {
		t.Fatalf("pagination lost keys: saw %d of 5 (%v)", len(seen), seen)
	}

	// V1 with marker.
	body = get("/b?marker=b%2F1.txt")
	if strings.Contains(body, "<Key>a/1.txt</Key>") || !strings.Contains(body, "<Key>top.txt</Key>") {
		t.Fatalf("V1 marker wrong: %s", body)
	}

	// max-keys=0: empty, not truncated.
	body = get("/b?list-type=2&max-keys=0")
	if strings.Contains(body, "<Key>") || strings.Contains(body, "<IsTruncated>true") {
		t.Fatalf("max-keys=0 wrong: %s", body)
	}

	// Unknown listing parameter: 501, not a wrong listing.
	if w := do(s, http.MethodGet, "/b?list-type=2&versions", nil); w.Code != http.StatusNotImplemented {
		t.Fatalf("unknown param: code=%d, want 501", w.Code)
	}
}

// Multi-delete invalidates every named key; deleting an absent key succeeds
// (invalidation is idempotent); quiet mode omits confirmations.
func TestOriginlessRoutes_MultiDelete(t *testing.T) {
	s, c := newOriginlessServer(t)
	seedObject(t, c, "m1.txt", "", []byte("1"))
	seedObject(t, c, "m2.txt", "", []byte("2"))

	body := `<Delete><Object><Key>m1.txt</Key></Object><Object><Key>m2.txt</Key></Object><Object><Key>absent.txt</Key></Object></Delete>`
	req := httptest.NewRequest(http.MethodPost, "/b?delete", strings.NewReader(body))
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK || strings.Count(w.Body.String(), "<Deleted>") != 3 {
		t.Fatalf("multi-delete: code=%d body=%q", w.Code, w.Body.String())
	}
	for _, k := range []string{"m1.txt", "m2.txt"} {
		if g := do(s, http.MethodGet, "/b/"+k, nil); g.Code != http.StatusNotFound {
			t.Fatalf("%s survived multi-delete: %d", k, g.Code)
		}
	}
	// Quiet: no confirmations.
	req = httptest.NewRequest(http.MethodPost, "/b?delete", strings.NewReader(`<Delete><Quiet>true</Quiet><Object><Key>m1.txt</Key></Object></Delete>`))
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK || strings.Contains(w.Body.String(), "<Deleted>") {
		t.Fatalf("quiet multi-delete: code=%d body=%q", w.Code, w.Body.String())
	}
}

// Conditional writes: If-None-Match:* is put-if-absent; If-Match guards
// overwrites; If-Match on a missing object is NoSuchKey (ceph semantics).
func TestOriginlessRoutes_ConditionalPut(t *testing.T) {
	s, _ := newOriginlessServer(t)

	put := func(key, body string, hdr map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPut, "/b/"+key, strings.NewReader(body))
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		s.router.ServeHTTP(w, req)
		return w
	}

	// put-if-absent succeeds, then refuses.
	if w := put("c.txt", "v1", map[string]string{"If-None-Match": "*"}); w.Code != http.StatusOK {
		t.Fatalf("INM* create: %d", w.Code)
	}
	etag := put("probe.txt", "x", nil).Header().Get("ETag")
	_ = etag
	if w := put("c.txt", "v2", map[string]string{"If-None-Match": "*"}); w.Code != http.StatusPreconditionFailed {
		t.Fatalf("INM* over existing: %d, want 412", w.Code)
	}
	// If-Match wrong etag: 412; right etag: 200; missing object: 404.
	if w := put("c.txt", "v2", map[string]string{"If-Match": `"wrong"`}); w.Code != http.StatusPreconditionFailed {
		t.Fatalf("If-Match wrong: %d", w.Code)
	}
	g := do(s, http.MethodGet, "/b/c.txt", nil)
	if w := put("c.txt", "v2", map[string]string{"If-Match": g.Header().Get("ETag")}); w.Code != http.StatusOK {
		t.Fatalf("If-Match right: %d", w.Code)
	}
	if w := put("ghost.txt", "v", map[string]string{"If-Match": "*"}); w.Code != http.StatusNotFound {
		t.Fatalf("If-Match on missing: %d, want 404", w.Code)
	}
}

// Error bodies echo the response's x-amz-request-id, so clients can correlate.
func TestOriginlessRoutes_RequestIDEcho(t *testing.T) {
	s, _ := newOriginlessServer(t)
	w := do(s, http.MethodGet, "/b/nope.txt", nil)
	id := w.Header().Get("x-amz-request-id")
	if id == "" {
		t.Fatal("no x-amz-request-id header")
	}
	if !strings.Contains(w.Body.String(), "<RequestId>"+id+"</RequestId>") {
		t.Fatalf("body does not echo request id %q: %s", id, w.Body.String())
	}
}

// Conditional PUT existence is SERVABLE existence: If-None-Match:* over an
// orphaned meta (body gone) must store — that is the healing put-if-absent —
// and If-Match against it answers NoSuchKey, matching every read shape.
func TestOriginlessRoutes_ConditionalPutHonorsVisibilityContract(t *testing.T) {
	s, c := newOriginlessServer(t)
	ctx := context.Background()
	meta := &cache.CachedObjectMeta{
		Bucket: "b", Key: "orphan2.txt", StatusCode: http.StatusOK,
		ETag: `"gone"`, ContentLength: 5, ACL: "public-read",
		CachedAt: time.Now().Unix(), LastModified: time.Now().Unix(),
	}
	if ok, err := c.PutMetaTombstoneAware(ctx, "b", "orphan2.txt", meta, 60, time.Now().UnixNano()); err != nil || !ok {
		t.Fatalf("seed orphan meta: ok=%v err=%v", ok, err)
	}

	req := httptest.NewRequest(http.MethodPut, "/b/orphan2.txt", strings.NewReader("healed"))
	req.Header.Set("If-None-Match", "*")
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("INM* over orphaned meta must heal, got %d", w.Code)
	}
	if g := do(s, http.MethodGet, "/b/orphan2.txt", nil); g.Code != http.StatusOK || g.Body.String() != "healed" {
		t.Fatalf("healed entry: code=%d body=%q", g.Code, g.Body.String())
	}
}
