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

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// blockMockForwarder serves range GETs from a backing object, for both the initial
// client-range forward (DoRequestWithCreds) and per-block fetches (DoConditionalGetRequest).
type blockMockForwarder struct {
	*mockForwarder
	object              []byte
	etag                string
	blockGetETag        string       // ETag returned by per-block fetches (defaults to etag)
	blockGetWhole2xx    bool         // if set, per-block fetches return 200 with the WHOLE object
	blockGetUnknownLen  bool         // if set, per-block 206 fetches report ContentLength -1
	blockGetWrongOffset bool         // if set, per-block 206 Content-Range has wrong (same-length) bounds
	blockGet404         bool         // if set, per-block fetches return 404 (object gone)
	blockGets           atomic.Int32 // count of per-block DoConditionalGetRequest calls
}

func newBlockMock(object []byte, etag string) *blockMockForwarder {
	m := &blockMockForwarder{mockForwarder: &mockForwarder{}, object: object, etag: etag, blockGetETag: etag}
	// The client forward serves the requested range, or the whole object for a full GET. A
	// deleted object (blockGet404) 404s the forward too, not only per-block fetches.
	m.mockForwarder.doRequestFunc = func(_ context.Context, r *http.Request, _, _ string) (*http.Response, error) {
		if m.blockGet404 {
			return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
		}
		if rng := r.Header.Get("Range"); rng != "" {
			return m.serveRange(rng, m.etag), nil
		}
		return m.wholeObject(m.etag), nil
	}
	return m
}

// wholeObject returns the whole object as a 200 with the given ETag.
func (m *blockMockForwarder) wholeObject(etag string) *http.Response {
	h := http.Header{}
	h.Set("ETag", etag)
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", fmt.Sprintf("%d", len(m.object)))
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        h,
		Body:          io.NopCloser(strings.NewReader(string(m.object))),
		ContentLength: int64(len(m.object)),
	}
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
		StatusCode:    http.StatusPartialContent,
		Header:        h,
		Body:          io.NopCloser(strings.NewReader(string(m.object[s : e+1]))),
		ContentLength: e - s + 1, // real net/http populates this from the header
	}
}

func (m *blockMockForwarder) DoConditionalGetRequest(_ context.Context, _, _, _, _, _ string, _ int64, rangeHeader string) (*http.Response, error) {
	m.blockGets.Add(1)
	if m.blockGet404 {
		return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
	}
	if m.blockGetWhole2xx {
		return m.wholeObject(m.blockGetETag), nil
	}
	resp := m.serveRange(rangeHeader, m.blockGetETag)
	if m.blockGetUnknownLen {
		resp.ContentLength = -1 // chunked/unknown length: can't be validated
	}
	if m.blockGetWrongOffset {
		// Same length as the requested block but a different offset (non-conformant upstream).
		n := resp.ContentLength
		resp.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", 100, 100+n-1, len(m.object)))
	}
	return resp, nil
}

func blockGet(bucket, key, rangeHeader string) *http.Request {
	r := fullGet(bucket, key)
	r.Header.Set("Range", rangeHeader)
	return r
}

func fullGet(bucket, key string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request, Signature=deadbeef")
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

// newBlockServiceWithBudget builds a block-mode service with a specific populate byte budget,
// set at construction so the byteBudget reflects it (the budget is built in NewService).
func newBlockServiceWithBudget(t *testing.T, mock *blockMockForwarder, blockSize, budget int64) (*Service, *cache.Cache) {
	t.Helper()
	cfg := config.NewDefault()
	cfg.Cache.BlockCachingEnabled = true
	cfg.Cache.BlockCacheMinSize = 8
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = 1 << 20
	cfg.Cache.MaxPopulateMemoryBytes = budget
	c := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	return NewService(mock, c, cfg), c
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
	if c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, 1) {
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
	if !c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, 1) {
		t.Error("block 1 not cached after the partial hit")
	}
}

// A full-object GET of a block-mode entry assembles the whole object from its blocks,
// fetching any that are missing, and serves 200 with the complete body.
func TestBlockCache_FullGetAssemblesAllBlocks(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish a block-mode entry (meta + block 0) via a cold range miss.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	// Full GET (no Range): assemble all three blocks (0 present, 1 and 2 fetched), serve whole.
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("full GET: %v", err)
	}
	if w2.Code != http.StatusOK || w2.Body.String() != "ABCDEFGHIJ" {
		t.Fatalf("full GET: code=%d body=%q, want 200 ABCDEFGHIJ", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("X-Cache=%q, want %q", got, XCacheHit)
	}
	// Assembly populated the remaining blocks for next time.
	for i := int64(1); i <= 2; i++ {
		if !c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, i) {
			t.Errorf("block %d not cached after full-object assembly", i)
		}
	}
}

// Warm-on-write is a no-op for objects at or above the block-mode boundary: they are cached
// per-block on read (RFC 0001), never warmed whole. The whole-object fetch aborts after
// headers (no read-back), so no whole-body entry is created.
func TestBlockCache_WarmOnWriteNoopAboveBoundary(t *testing.T) {
	var warmFetches atomic.Int32
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusOK)
			return nil
		},
		doFullObjectFunc: warmObjectResponder(&warmFetches, "ABCDEFGHIJ"), // 10 bytes >= boundary
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true
	svc.config.Cache.BlockCachingEnabled = true
	svc.config.Cache.BlockCacheMinSize = 8
	svc.config.Cache.SizeThreshold = 1 << 20

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, "ABCDEFGHIJ")); err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("warm-on-write cached a block-mode-sized object whole (want no-op above the boundary)")
	}
}

// A 206 whose declared length matches the block but whose Content-Range identifies different
// byte bounds (a non-conformant upstream) must not be stored under the requested block index.
func TestBlockCache_WrongContentRangeBoundsNotStored(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	mock.blockGetWrongOffset = true // 206 has the right length but wrong Content-Range bounds
	svc, c := newBlockService(t, mock)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("block-mode meta written from a 206 with mismatched Content-Range bounds")
	}
	if c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, 0) {
		t.Error("body stored under block 0 despite wrong Content-Range bounds")
	}
}

// A 206 with an unknown/chunked ContentLength can't be validated against the requested block
// length, so it must not be stored (a short/long body would be served at fixed block offsets).
func TestBlockCache_UnknownLengthBlockNotStored(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	mock.blockGetUnknownLen = true // per-block 206 fetches report ContentLength -1
	svc, c := newBlockService(t, mock)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("block-mode meta written from an unknown-length block fetch")
	}
	if c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, 0) {
		t.Error("unknown-length body stored as block 0")
	}
}

// A full-object GET of a block-eligible object with no cached entry is WHOLE-cached (Option A):
// the whole object was fetched to satisfy the GET, so there is no range-miss amplification to
// avoid. It becomes a whole-mode entry (BlockSize==0); block mode is established only on the
// range-read path. A later range read serves from the whole body.
func TestBlockCache_FullGetMissWholeCaches(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Full GET on a cold object: served whole from upstream and whole-cached.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("full GET: %v", err)
	}
	if w.Code != http.StatusOK || w.Body.String() != "ABCDEFGHIJ" {
		t.Fatalf("full GET: code=%d body=%q, want 200 ABCDEFGHIJ", w.Code, w.Body.String())
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("object not cached after a full-GET miss")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if meta.BlockSize != 0 {
		t.Fatalf("meta.BlockSize=%d, want 0 (whole-mode from full-GET)", meta.BlockSize)
	}
	// A subsequent range read is served from the whole body (whole-mode dispatch).
	before := mock.blockGets.Load()
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, blockGet(wowBucket, wowKey, "bytes=4-7")); err != nil {
		t.Fatalf("range read: %v", err)
	}
	if w2.Code != http.StatusPartialContent || w2.Body.String() != "EFGH" {
		t.Fatalf("range read: code=%d body=%q, want 206 EFGH", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("X-Cache=%q, want %q (served from whole body)", got, XCacheHit)
	}
	if after := mock.blockGets.Load(); after != before {
		t.Errorf("whole-cached object triggered a block fetch: %d -> %d", before, after)
	}
}

// An out-of-band delete (a block fetch returns 404) invalidates the stale block-mode entry
// rather than retrying failed fetches on every read until TTL.
func TestBlockCache_Block404InvalidatesStaleMeta(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish a block-mode entry (meta + block 0).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	// The object is deleted out of band: block fetches now 404.
	mock.blockGet404 = true

	// A full GET assembles blocks; a missing block 404s → the stale entry is invalidated.
	w2 := httptest.NewRecorder()
	_ = svc.HandleGetObject(w2, fullGet(wowBucket, wowKey))
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stale block-mode meta not invalidated after a 404 block fetch")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// An overwrite that changes both the ETag and a touched block's length must still be treated as
// a version mismatch (invalidate), not masked by the length guard. The ETag guard runs before
// the length/bounds guards precisely so a stale entry self-heals instead of retrying until TTL.
func TestBlockCache_OverwriteWithLengthChangeInvalidates(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish a block-mode entry at v1.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	// Overwritten out of band to v2, and the block fetch also reports an unvalidatable length
	// (as if the block's length changed). With the ETag guard checked first this is an ETag
	// mismatch (invalidate); if the length guard ran first it would be a plain error (no heal).
	mock.etag = `"v2"`
	mock.blockGetETag = `"v2"`
	mock.blockGetUnknownLen = true

	w2 := httptest.NewRecorder()
	_ = svc.HandleGetObject(w2, fullGet(wowBucket, wowKey))
	deadline := time.Now().Add(2 * time.Second)
	for {
		meta, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
		if !found || meta.ETag == `"v2"` {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale v1 entry not invalidated on a version+length-changing overwrite (etag=%q)", meta.ETag)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A client-forced revalidation (Cache-Control: no-cache) of a block-mode entry must invalidate
// it — otherwise it serves fresh once but leaves the stale entry for later normal reads (whose
// already-cached blocks never re-check upstream). After revalidation the entry is re-established
// at the current version.
func TestBlockCache_ForceRevalidateInvalidatesBlockEntry(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish a block-mode entry at v1.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	// The object is overwritten out of band to v2.
	mock.etag = `"v2"`
	mock.blockGetETag = `"v2"`

	// A forced revalidation for the already-cached range (block 0) must not silently keep v1.
	req := blockGet(wowBucket, wowKey, "bytes=0-3")
	req.Header.Set("Cache-Control", "no-cache")
	w2 := httptest.NewRecorder()
	_ = svc.HandleGetObject(w2, req)

	// The stale v1 entry is invalidated, then re-established fresh (v2) by the fall-through.
	deadline := time.Now().Add(2 * time.Second)
	for {
		meta, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
		if !found || meta.ETag == `"v2"` {
			break // invalidated, or already refreshed to the current version
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale v1 block-mode entry not invalidated by forced revalidation (etag=%q)", meta.ETag)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A config that enables block caching but leaves block_size at 0 (e.g. a Config built directly,
// bypassing Load's normalization) must not divide by zero: it behaves as block-caching-off and
// the large object falls back to whole-object caching.
func TestBlockCache_ZeroBlockSizeNoPanic(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, _ := newBlockService(t, mock)
	svc.config.Cache.BlockSize = 0 // programmatic misconfiguration

	// A range read on a block-eligible-sized object must complete without panicking.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("range GET: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "ABCD" {
		t.Fatalf("range GET: code=%d body=%q, want 206 ABCD", w.Code, w.Body.String())
	}
}

// A block larger than the whole populate budget must still populate: fetchOneBlock reserves
// via populateWeight (clamped to the budget), not the raw block length, so a small
// max_populate_memory_bytes doesn't shed every block fetch and silently disable block caching.
func TestBlockCache_BlockLargerThanBudgetStillPopulates(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`) // blocks of 4 bytes
	svc, c := newBlockServiceWithBudget(t, mock, 4 /*block*/, 2 /*budget < block*/)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block not populated when block size exceeds the populate budget (weight not clamped)")
	}
}

// If upstream ignores the Range and returns 200 with the whole object, that body must not be
// stored as a block (its byte 0 is the object start, not the block offset). The block is
// rejected and the block-mode meta is not written.
func TestBlockCache_WholeObject200NotStoredAsBlock(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	mock.blockGetWhole2xx = true // per-block fetches return 200 with the whole object
	svc, c := newBlockService(t, mock)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "ABCD" {
		t.Fatalf("cold miss: code=%d body=%q, want 206 ABCD", w.Code, w.Body.String())
	}
	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("block-mode meta written despite a 200 (whole-object) block fetch")
	}
	if c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, 0) {
		t.Error("whole-object 200 body was stored as block 0")
	}
}

// When a block-mode entry is stale (the object was overwritten out of band, so a block fetch
// reports a newer ETag), the stale meta must be invalidated so later requests don't repeat the
// mismatch until TTL. Exercised via a full GET, whose miss path never re-populates block mode.
func TestBlockCache_ETagMismatchInvalidatesStaleMeta(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish a block-mode entry at ETag v1 (meta + block 0).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	// The object is overwritten out of band: upstream now serves v2.
	mock.etag = `"v2"`
	mock.blockGetETag = `"v2"`

	// A full GET assembles all blocks; a missing block reports v2 != cached v1, so the stale v1
	// entry is invalidated and then re-established from the fall-through as the current version
	// (whole-cached v2 under Option A). Either way the stale v1 entry must be gone.
	w2 := httptest.NewRecorder()
	_ = svc.HandleGetObject(w2, fullGet(wowBucket, wowKey))
	deadline := time.Now().Add(2 * time.Second)
	for {
		meta, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
		if !found || meta.ETag == `"v2"` {
			break // invalidated, or refreshed to the current version
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale v1 block-mode entry not invalidated after ETag mismatch (etag=%q)", meta.ETag)
		}
		time.Sleep(5 * time.Millisecond)
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
