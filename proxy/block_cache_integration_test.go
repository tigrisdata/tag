package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigrisdata/tag/cache"
)

// blockMockForwarder serves range GETs from a backing object, for both the initial
// client-range forward (DoRequestWithCreds) and per-block fetches (DoConditionalGetRequest).
type blockMockForwarder struct {
	*mockForwarder
	object       []byte
	etag         string
	blockGetETag string       // ETag returned by per-block fetches (defaults to etag)
	blockGets    atomic.Int32 // count of per-block DoConditionalGetRequest calls
}

func newBlockMock(object []byte, etag string) *blockMockForwarder {
	m := &blockMockForwarder{mockForwarder: &mockForwarder{}, object: object, etag: etag, blockGetETag: etag}
	// The initial client-range forward serves the requested range too.
	m.mockForwarder.doRequestFunc = func(_ context.Context, r *http.Request, _, _ string) (*http.Response, error) {
		return m.serveRange(r.Header.Get("Range"), m.etag), nil
	}
	return m
}

func (m *blockMockForwarder) serveRange(rangeHeader, etag string) *http.Response {
	total := int64(len(m.object))
	var s, e int64
	fmt.Sscanf(strings.TrimPrefix(rangeHeader, "bytes="), "%d-%d", &s, &e)
	if e >= total {
		e = total - 1
	}
	h := http.Header{}
	h.Set("ETag", etag)
	h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, e, total))
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", fmt.Sprintf("%d", e-s+1))
	return &http.Response{
		StatusCode: http.StatusPartialContent,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(string(m.object[s : e+1]))),
	}
}

func (m *blockMockForwarder) DoConditionalGetRequest(_ context.Context, _, _, _, _, _ string, _ int64, rangeHeader string) (*http.Response, error) {
	m.blockGets.Add(1)
	return m.serveRange(rangeHeader, m.blockGetETag), nil
}

func blockGet(bucket, key, rangeHeader string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request, Signature=deadbeef")
	r.Header.Set("Range", rangeHeader)
	return r
}

func newBlockService(t *testing.T, mock *blockMockForwarder) (*Service, *cache.Cache) {
	t.Helper()
	svc, c := newTestService(mock, true)
	svc.config.Cache.BlockCachingEnabled = true
	svc.config.Cache.BlockCacheMinSize = 8 // the 10-byte test object is block-mode
	svc.config.Cache.BlockSize = 4
	svc.config.Cache.SizeThreshold = 1 << 20
	return svc, c
}

// Cold range miss forwards from upstream, then block-populates in the background; a
// subsequent range read within a populated block is served from cache with no re-fetch.
func TestBlockCache_ColdMissPopulatesThenServesFromBlocks(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`) // 10 bytes → blocks [0..3][4..7][8..9]
	svc, c := newBlockService(t, mock)

	// Cold miss on block 0.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "ABCD" {
		t.Fatalf("cold miss: code=%d body=%q, want 206 ABCD", w.Code, w.Body.String())
	}

	// Background block-mode populate writes the block(s) then the meta (visibility gate).
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated after cold miss")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if meta.BlockSize != 4 || meta.ContentLength != 10 {
		t.Fatalf("meta.BlockSize=%d ContentLength=%d, want 4 and 10", meta.BlockSize, meta.ContentLength)
	}

	// A sub-range of the already-populated block 0 is served from cache — no new block fetch.
	before := mock.blockGets.Load()
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, blockGet(wowBucket, wowKey, "bytes=1-2")); err != nil {
		t.Fatalf("cache hit: %v", err)
	}
	if w2.Code != http.StatusPartialContent || w2.Body.String() != "BC" {
		t.Fatalf("cache hit: code=%d body=%q, want 206 BC", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("X-Cache=%q, want %q", got, XCacheHit)
	}
	if after := mock.blockGets.Load(); after != before {
		t.Errorf("block re-fetched on a hit: blockGets %d -> %d", before, after)
	}
}

// A range spanning a not-yet-cached block is a partial hit: the missing block is fetched
// on demand, served, and cached for next time.
func TestBlockCache_PartialHitFetchesMissingBlock(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Populate block 0 via a cold miss.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}
	if c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 1) {
		t.Fatal("block 1 unexpectedly present before it was requested")
	}

	// Request block 1 (bytes 4-7): a partial hit that fetches and serves the missing block.
	before := mock.blockGets.Load()
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, blockGet(wowBucket, wowKey, "bytes=4-7")); err != nil {
		t.Fatalf("partial hit: %v", err)
	}
	if w2.Code != http.StatusPartialContent || w2.Body.String() != "EFGH" {
		t.Fatalf("partial hit: code=%d body=%q, want 206 EFGH", w2.Code, w2.Body.String())
	}
	if after := mock.blockGets.Load(); after != before+1 {
		t.Errorf("expected exactly one block fetch, got blockGets %d -> %d", before, after)
	}
	if !c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 1) {
		t.Error("block 1 not cached after the partial hit")
	}
}

// If a block fetched during populate reports a different ETag than the object we forwarded
// (a concurrent overwrite), it must not be cached, and the block-mode meta is not written.
func TestBlockCache_ETagMismatchDoesNotCache(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	mock.blockGetETag = `"v2"` // block fetches report a newer version than the forward
	svc, c := newBlockService(t, mock)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	// The client is still served from the upstream forward.
	if w.Code != http.StatusPartialContent || w.Body.String() != "ABCD" {
		t.Fatalf("cold miss: code=%d body=%q, want 206 ABCD", w.Code, w.Body.String())
	}
	// But the block-mode meta must never be written (the mismatched block was rejected).
	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("block-mode meta written despite an ETag mismatch on block fetch")
	}
}
