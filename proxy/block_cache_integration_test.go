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
	blockGet403         bool         // if set, per-block fetches return 403 (access denied)
	blockGetTransient   bool         // if set, per-block fetches return a transient (non-stale) error
	blockGetNoETag      bool         // if set, per-block 206 fetches omit the ETag header
	blockGetShortBody   bool         // if set, per-block 206 body is shorter than its Content-Length
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
	if m.blockGetTransient {
		return nil, fmt.Errorf("simulated upstream blip")
	}
	if m.blockGet404 {
		return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
	}
	if m.blockGet403 {
		return &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
	}
	if m.blockGetWhole2xx {
		return m.wholeObject(m.blockGetETag), nil
	}
	resp := m.serveRange(rangeHeader, m.blockGetETag)
	if m.blockGetNoETag {
		resp.Header.Del("ETag") // upstream omitted the ETag: version can't be verified
	}
	if m.blockGetShortBody {
		// Keep the Content-Length header/field (so the length guard passes) but deliver a body
		// that ends early — a truncated response.
		resp.Body = io.NopCloser(strings.NewReader("x"))
	}
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
	svc.config.Cache.BlockSize = 4 // boundary: the 10-byte test object (>= 4) is block-mode
	svc.config.Cache.SizeThreshold = 1 << 20
	return svc, c
}

// newBlockServiceWithBudget builds a block-mode service with a specific populate byte budget,
// set at construction so the byteBudget reflects it (the budget is built in NewService).
func newBlockServiceWithBudget(t *testing.T, mock *blockMockForwarder, blockSize, budget int64) (*Service, *cache.Cache) {
	t.Helper()
	cfg := config.NewDefault()
	cfg.Cache.BlockCachingEnabled = true
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

	// Establish a block-mode entry with blocks 0 and 1 present (a cold miss over bytes 0-7), so
	// the object is mostly cached (2 of 3 blocks) and a full GET assembles the rest rather than
	// falling through to a single upstream GET (the mostly-missing amplification guard).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-7")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	// Full GET (no Range): assemble all three blocks (0,1 present, 2 fetched), serve whole.
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
	// Assembly populated the remaining block for next time.
	if !c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, 2) {
		t.Error("block 2 not cached after full-object assembly")
	}
}

// A full GET on a mostly-missing block-mode entry (e.g. only a footer block was ever cached)
// must NOT fan out into a per-block fetch of every missing block — that is a large upstream
// amplification. It falls through to the miss path (a single streaming upstream GET), which
// re-streams the object through the block splitter: all blocks are populated in one fetch, with
// no per-block round-trips, and the entry stays block-mode.
func TestBlockCache_FullGetMostlyMissingFallsThroughAndBlockSplits(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`) // 3 blocks; establish only block 0
	svc, c := newBlockService(t, mock)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	before := mock.blockGets.Load()
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("full GET: %v", err)
	}
	// Correct bytes are still served (via the upstream fall-through, not block assembly).
	if w2.Code != http.StatusOK || w2.Body.String() != "ABCDEFGHIJ" {
		t.Fatalf("full GET: code=%d body=%q, want 200 ABCDEFGHIJ", w2.Code, w2.Body.String())
	}
	// The amplify bail invalidates the mostly-missing entry, then the fall-through re-stream
	// block-splits the whole object and writes the block-mode meta last (the visibility gate).
	// Poll on the meta as the completion signal, then assert the full block set is present.
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode entry not re-established by the fall-through block-split")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if meta.BlockSize != 4 {
		t.Fatalf("entry not block-mode after fall-through: BlockSize=%d", meta.BlockSize)
	}
	for i := int64(0); i <= 2; i++ {
		if !c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, i) {
			t.Errorf("block %d not populated by fall-through block-split", i)
		}
	}
	// No per-block fetch amplification: the fall-through streamed once via the forward, not one
	// aligned range GET per missing block.
	if after := mock.blockGets.Load(); after != before {
		t.Errorf("full GET fanned out into per-block fetches: blockGets %d -> %d", before, after)
	}
}

// Warm-on-write stores a block-eligible object (>= block_size with block caching on) as blocks:
// the write-path read-back streams through the same block-splitting cache writer as a read miss,
// so the whole-vs-block boundary is size, not access pattern (RFC 0001).
func TestBlockCache_WarmOnWriteBlockSplitsBlockEligible(t *testing.T) {
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusOK)
			return nil
		},
		// A full-object read-back with a real Content-Length header (10 bytes >= block_size), so
		// the shared cache writer sees a block-eligible size and splits it.
		doFullObjectFunc: func(_ context.Context, _, _, _, _ string) (*http.Response, error) {
			h := http.Header{}
			h.Set("ETag", `"warm-etag"`)
			h.Set("Content-Type", "application/octet-stream")
			h.Set("Content-Length", "10")
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        h,
				Body:          io.NopCloser(strings.NewReader("ABCDEFGHIJ")),
				ContentLength: 10,
			}, nil
		},
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true
	svc.config.Cache.BlockCachingEnabled = true
	svc.config.Cache.BlockSize = 4 // the 10-byte object is block-eligible (>= 4)
	svc.config.Cache.SizeThreshold = 1 << 20

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, "ABCDEFGHIJ")); err != nil {
		t.Fatalf("PUT: %v", err)
	}
	// The read-back warm populates a block-mode entry (blocks first, meta last as the gate).
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("warm-on-write did not cache the block-eligible object")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if meta.BlockSize != 4 {
		t.Fatalf("meta.BlockSize=%d, want 4 (block-mode warm)", meta.BlockSize)
	}
	for i := int64(0); i <= 2; i++ {
		if !c.BlockExists(context.Background(), wowBucket, wowKey, meta.ETag, 4, i) {
			t.Errorf("block %d not populated by warm-on-write block-split", i)
		}
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

// A full-object GET of a block-eligible object with no cached entry is stored as BLOCKS: the
// shared cache writer splits the streamed body into fixed-size blocks (RFC 0001 — the
// whole-vs-block boundary is size, not access pattern). A later range read is served from the
// blocks with no upstream fetch.
func TestBlockCache_FullGetMissBlockSplits(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Full GET on a cold object: served whole from upstream, cached as blocks.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("full GET: %v", err)
	}
	if w.Code != http.StatusOK || w.Body.String() != "ABCDEFGHIJ" {
		t.Fatalf("full GET: code=%d body=%q, want 200 ABCDEFGHIJ", w.Code, w.Body.String())
	}
	// meta is written last (the visibility gate), so its presence implies all blocks landed.
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("object not cached after a full-GET miss")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if meta.BlockSize != 4 {
		t.Fatalf("meta.BlockSize=%d, want 4 (block-mode from full-GET split)", meta.BlockSize)
	}
	for i := int64(0); i <= 2; i++ {
		if !c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, i) {
			t.Errorf("block %d not populated by full-GET split", i)
		}
	}
	// A subsequent range read is served from the blocks with no upstream fetch.
	before := mock.blockGets.Load()
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, blockGet(wowBucket, wowKey, "bytes=4-7")); err != nil {
		t.Fatalf("range read: %v", err)
	}
	if w2.Code != http.StatusPartialContent || w2.Body.String() != "EFGH" {
		t.Fatalf("range read: code=%d body=%q, want 206 EFGH", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("X-Cache=%q, want %q (served from blocks)", got, XCacheHit)
	}
	if after := mock.blockGets.Load(); after != before {
		t.Errorf("range read of a fully-split object triggered a block fetch: %d -> %d", before, after)
	}
}

// An out-of-band delete (a block fetch returns 404) invalidates the stale block-mode entry
// rather than retrying failed fetches on every read until TTL.
func TestBlockCache_Block404InvalidatesStaleMeta(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish a block-mode entry with blocks 0 and 1 present (mostly cached, so a full GET
	// assembles the remaining block rather than falling through the amplification guard).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-7")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	// The object is deleted out of band: block fetches now 404.
	mock.blockGet404 = true

	// A full GET assembles blocks; the missing block 404s → the stale entry is invalidated.
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

	// Establish a block-mode entry at v1 with blocks 0 and 1 present (mostly cached, so a full
	// GET assembles the remaining block rather than falling through the amplification guard).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-7")); err != nil {
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

	// Establish a block-mode entry at ETag v1 with blocks 0 and 1 present (mostly cached, so a
	// full GET assembles the remaining block rather than falling through the amplification guard).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-7")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	// The object is overwritten out of band: upstream now serves v2.
	mock.etag = `"v2"`
	mock.blockGetETag = `"v2"`

	// A full GET assembles all blocks; the missing block reports v2 != cached v1, so the stale v1
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

// A full GET on a mostly-cached block entry that hits a *transient* per-block fetch failure
// during assembly falls through to the miss path, which re-streams the object through the block
// splitter. The entry stays block-mode (never demoted to whole) and remains servable.
func TestBlockCache_TransientBlockFailureFallsThroughToBlockSplit(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`) // 10 bytes → blocks [0..3][4..7][8..9]
	svc, c := newBlockService(t, mock)

	// Establish blocks 0 and 1 (mostly cached: a full GET assembles rather than short-circuiting
	// via the mostly-missing guard).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-7")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated after cold miss")
	}

	// Assembling the remaining block fails transiently → fall through to upstream.
	mock.blockGetTransient = true
	wf := httptest.NewRecorder()
	if err := svc.HandleGetObject(wf, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("full GET: %v", err)
	}
	// The client is still served the whole object from the upstream fall-through.
	if wf.Code != http.StatusOK || wf.Body.String() != "ABCDEFGHIJ" {
		t.Fatalf("full GET: code=%d body=%q, want 200 ABCDEFGHIJ", wf.Code, wf.Body.String())
	}
	// The entry stays block-mode (never whole-cached over).
	meta, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if !found || meta.BlockSize != 4 {
		t.Fatalf("entry not block-mode after transient fall-through: found=%v BlockSize=%d", found, meta.BlockSize)
	}
}

// triggerBlockModePopulate re-checks for a concurrently-established entry before stamping meta,
// so a stale scheduled populate (its schedule-time !found gate gone stale while it was fetching)
// does not overwrite an entry that landed in the meantime.
func TestBlockCache_StalePopulateDoesNotOverwriteExistingEntry(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish a block-mode entry.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}
	seeded, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)

	// Drive a stale populate carrying the same meta (as a racing range miss whose schedule-time
	// !found gate is now stale). It targets block 1 (not yet cached) so the fetch actually runs;
	// its re-check then finds the entry present and skips the meta write. Wait for the populate to
	// finish its work — block 1 written — as a deterministic signal (no fixed sleep), then confirm
	// it left the existing block-mode entry intact.
	svc.triggerBlockModePopulate(wowBucket, wowKey, "ak", "sk", seeded, []int64{1})
	deadline := time.Now().Add(2 * time.Second)
	for !c.BlockExists(context.Background(), wowBucket, wowKey, seeded.ETag, seeded.BlockSize, 1) {
		if time.Now().After(deadline) {
			t.Fatal("block-mode populate never wrote block 1")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if meta, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found || meta.BlockSize != 4 || meta.ETag != seeded.ETag {
		t.Fatalf("existing entry disturbed by a background populate: found=%v", found)
	}
}

// An anonymous range miss to a public-bucket object must store the block-mode entry as
// public-read (mirroring the whole-object path); otherwise every later anonymous read is
// turned away by the IsPublicRead() gate and the block cache is useless for anonymous traffic.
func TestBlockCache_AnonymousRangeMissMarksPublicRead(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)
	// Model transparent mode: an anonymous request validates as AuthNotValidated but still
	// receives TAG's own creds for background fetches (see ValidateAndGetCredentials).
	mock.validateFunc = func(r *http.Request) (AuthResult, string, string, error) {
		if hasNoAuthCredentials(r) {
			return AuthNotValidated, "access", "secret", nil
		}
		return AuthValidated, "access", "secret", nil
	}

	// Anonymous cold range miss (no Authorization header). Tigris omits X-Amz-Acl for a
	// bucket-inherited object, so the entry would be non-public without the fix.
	anon := blockGet(wowBucket, wowKey, "bytes=0-3")
	anon.Header.Del("Authorization")
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, anon); err != nil {
		t.Fatalf("anon cold miss: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "ABCD" {
		t.Fatalf("anon cold miss: code=%d body=%q, want 206 ABCD", w.Code, w.Body.String())
	}
	// The public-read ACL is inferred for the CACHE only; the origin's 206 had no X-Amz-Acl, so the
	// client's response must not carry a synthetic one.
	if got := w.Header().Get("X-Amz-Acl"); got != "" {
		t.Errorf("client 206 leaked a synthetic X-Amz-Acl=%q (origin sent none)", got)
	}

	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated after anonymous cold miss")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if !meta.IsPublicRead() {
		t.Fatalf("anonymous block-mode entry not public-read (ACL=%q); anonymous reads would skip it", meta.ACL)
	}

	// A second anonymous read within the populated block is served from cache, not turned away.
	before := mock.blockGets.Load()
	anon2 := blockGet(wowBucket, wowKey, "bytes=1-2")
	anon2.Header.Del("Authorization")
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, anon2); err != nil {
		t.Fatalf("anon cache read: %v", err)
	}
	if w2.Code != http.StatusPartialContent || w2.Body.String() != "BC" {
		t.Fatalf("anon cache read: code=%d body=%q, want 206 BC", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("anon read X-Cache=%q, want %q (public-read block entry should serve anonymously)", got, XCacheHit)
	}
	if after := mock.blockGets.Load(); after != before {
		t.Errorf("block re-fetched on an anonymous hit: blockGets %d -> %d", before, after)
	}
}

// A per-block fetch whose 206 omits the ETag can't be version-verified: a same-size overwrite
// would otherwise be stored under the old meta.ETag, mixing versions across blocks. Such a
// fetch must not be cached (and, being unconfirmed rather than a definitive mismatch, must not
// invalidate an existing entry).
func TestBlockCache_MissingETagBlockNotStored(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	mock.blockGetNoETag = true // per-block 206 fetches omit the ETag header
	svc, c := newBlockService(t, mock)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	// The client is still served from the upstream forward (which carries the ETag).
	if w.Code != http.StatusPartialContent || w.Body.String() != "ABCD" {
		t.Fatalf("cold miss: code=%d body=%q, want 206 ABCD", w.Code, w.Body.String())
	}
	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("block-mode meta written from an ETag-less block fetch")
	}
	if c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, 0) {
		t.Error("ETag-less body stored as block 0 (version unverifiable)")
	}
}

// Missing-ETag is a transient (unconfirmed) failure, not a definitive mismatch: it must leave
// an already-established block-mode entry intact rather than invalidating it.
func TestBlockCache_MissingETagDoesNotInvalidateEntry(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish a block-mode entry (block 0 + meta) via a normal cold miss.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated after cold miss")
	}

	// A range touching a not-yet-cached block now fetches it, but upstream omits the ETag.
	mock.blockGetNoETag = true
	wf := httptest.NewRecorder()
	if err := svc.HandleGetObject(wf, blockGet(wowBucket, wowKey, "bytes=4-7")); err != nil {
		t.Fatalf("partial-hit range: %v", err)
	}
	// The still-valid entry must survive (no invalidation on an unconfirmed fetch), and the
	// ETag-less block must not be cached.
	if meta, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found || meta.BlockSize != 4 {
		t.Fatalf("block-mode entry invalidated by an ETag-less fetch: found=%v", found)
	}
	if c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, 1) {
		t.Error("ETag-less body stored as block 1 (version unverifiable)")
	}
}

// errAfter reads n good bytes across calls, then fails — to exercise a mid-stream body error.
type errAfterReader struct {
	data []byte
	pos  int
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("simulated mid-stream read error")
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// putBlocksFromStream writes an exact-multiple object as whole blocks with no phantom trailing
// block, and gates visibility on the meta written last.
func TestPutBlocksFromStream_ExactMultipleNoPhantomBlock(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGH"), `"v1"`) // 8 bytes = exactly two 4-byte blocks
	svc, c := newBlockService(t, mock)

	h := http.Header{}
	h.Set("ETag", `"v1"`)
	h.Set("Content-Length", "8")
	meta := cache.MetaFromHTTPHeaders(wowBucket, wowKey, http.StatusOK, h)
	meta.BlockSize = 4

	if err := svc.putBlocksFromStream(context.Background(), wowBucket, wowKey, meta, strings.NewReader("ABCDEFGH"), 60, time.Now().UnixNano()); err != nil {
		t.Fatalf("putBlocksFromStream: %v", err)
	}
	for i := int64(0); i <= 1; i++ {
		if !c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, i) {
			t.Errorf("block %d not written", i)
		}
	}
	if c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, 2) {
		t.Error("phantom block 2 written for an exact-multiple object")
	}
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found {
		t.Error("block meta not written after a successful split")
	}
}

// A mid-stream body error aborts the split and leaves the meta unwritten, so a partially written
// block set is never made visible.
func TestPutBlocksFromStream_MidStreamErrorLeavesMetaUnwritten(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGH"), `"v1"`)
	svc, c := newBlockService(t, mock)

	h := http.Header{}
	h.Set("ETag", `"v1"`)
	h.Set("Content-Length", "8")
	meta := cache.MetaFromHTTPHeaders(wowBucket, wowKey, http.StatusOK, h)
	meta.BlockSize = 4

	// One full block, then a read error before the second.
	r := &errAfterReader{data: []byte("ABCD")}
	if err := svc.putBlocksFromStream(context.Background(), wowBucket, wowKey, meta, r, 60, time.Now().UnixNano()); err == nil {
		t.Fatal("expected an error from the mid-stream read failure")
	}
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); found {
		t.Error("block meta written despite a mid-stream error (partial entry made visible)")
	}
}

// A cleanly-truncated body (ends before Content-Length with no error signal) must not write the
// meta OR the short non-final block: BlockExists can't tell a short block from a full one, so a
// stored short block would serve truncated bytes under the committed length and poison later
// range-path populates that trust existing blocks.
func TestPutBlocksFromStream_TruncatedStreamLeavesNoShortBlockOrMeta(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGH"), `"v1"`)
	svc, c := newBlockService(t, mock)

	h := http.Header{}
	h.Set("ETag", `"v1"`)
	h.Set("Content-Length", "8") // claims 8 bytes (two 4-byte blocks)...
	meta := cache.MetaFromHTTPHeaders(wowBucket, wowKey, http.StatusOK, h)
	meta.BlockSize = 4

	// ...but the body carries only 6 bytes, ending cleanly (no read error).
	if err := svc.putBlocksFromStream(context.Background(), wowBucket, wowKey, meta, strings.NewReader("ABCDEF"), 60, time.Now().UnixNano()); err == nil {
		t.Fatal("expected an error from the truncated body")
	}
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); found {
		t.Error("block meta written for a truncated body (entry made visible)")
	}
	// The short second block must not have been stored (it would look present forever).
	if c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, 1) {
		t.Error("short non-final block 1 stored from a truncated body")
	}
}

// A body longer than Content-Length is rejected before the meta is committed, so the entry can
// never claim a length its blocks overrun.
func TestPutBlocksFromStream_OversizedStreamLeavesMetaUnwritten(t *testing.T) {
	mock := newBlockMock([]byte("ABCD"), `"v1"`)
	svc, c := newBlockService(t, mock)

	h := http.Header{}
	h.Set("ETag", `"v1"`)
	h.Set("Content-Length", "4") // claims 4 bytes (one block)...
	meta := cache.MetaFromHTTPHeaders(wowBucket, wowKey, http.StatusOK, h)
	meta.BlockSize = 4

	// ...but the body carries 6 bytes.
	if err := svc.putBlocksFromStream(context.Background(), wowBucket, wowKey, meta, strings.NewReader("ABCDEF"), 60, time.Now().UnixNano()); err == nil {
		t.Fatal("expected an error from the oversized body")
	}
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); found {
		t.Error("block meta written for an oversized body (entry made visible)")
	}
}

// invalidateStaleBlockMeta must delete only the version the caller observed as stale — never a
// newer entry another request re-established after an overwrite (finding: stale delete races
// newer entry).
func TestBlockCache_InvalidateStaleBlockMetaOnlyMatchingETag(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v2"`)
	svc, c := newBlockService(t, mock)

	// Seed a current v2 block-mode entry (cold range miss populates block 0 + meta at v2).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("v2 entry not populated")
	}

	// A lagging request that saw v1 as stale must NOT wipe the newer v2 entry.
	svc.invalidateStaleBlockMeta(wowBucket, wowKey, `"v1"`)
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found {
		t.Error("v2 entry wrongly deleted by a stale-v1 invalidation")
	}

	// Invalidating the matching (current) version does delete it.
	svc.invalidateStaleBlockMeta(wowBucket, wowKey, `"v2"`)
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); found {
		t.Error("v2 entry not deleted by a matching-version invalidation")
	}
}

// A full GET on a mostly-missing entry whose object was deleted out of band must not leave the
// stale entry in place (its cached blocks would keep serving deleted bytes on later range reads).
// The amplify bail invalidates it and the 404 fall-through leaves it gone.
func TestBlockCache_AmplifyBailInvalidatesDeletedObject(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish a mostly-missing entry (only block 0 cached).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("entry not populated")
	}

	// The object is deleted out of band: forward + block fetches now 404.
	mock.blockGet404 = true
	wf := httptest.NewRecorder()
	_ = svc.HandleGetObject(wf, fullGet(wowBucket, wowKey)) // amplify-bail -> invalidate -> 404 fall-through

	// The stale entry must be gone (not left serving deleted bytes until TTL).
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stale block-mode entry not invalidated after amplify-bail on a deleted object")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A per-block fetch whose 206 body ends short of its Content-Length must not be stored: a short
// block is indistinguishable from a complete one via BlockExists and would later serve truncated
// bytes. fetchOneBlock reads the full block before storing, so a short body errors out uncached.
func TestBlockCache_ShortBlockBodyNotStored(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	mock.blockGetShortBody = true // 206 header says a full block, body delivers 1 byte
	svc, c := newBlockService(t, mock)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("block-mode meta written from a short-body block fetch")
	}
	if c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, 0) {
		t.Error("short block body stored as block 0")
	}
}

// A 403 on a per-block fetch is principal/permission-level, not object-level, so it must NOT
// invalidate the block-mode entry shared across all principals (unlike a 404). The fetch fails
// and the request falls through to upstream, but the entry stays put for other principals.
func TestBlockCache_Block403DoesNotInvalidateSharedEntry(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish a block-mode entry (block 0 + meta).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("entry not populated")
	}
	seeded, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)

	// A range needing an uncached block now gets 403 on the block fetch. The forward still serves
	// (only per-block fetches are forbidden), so the client is served, but the entry must survive.
	mock.blockGet403 = true
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, blockGet(wowBucket, wowKey, "bytes=4-7")); err != nil {
		t.Fatalf("range read: %v", err)
	}
	if w2.Code != http.StatusPartialContent || w2.Body.String() != "EFGH" {
		t.Fatalf("range read: code=%d body=%q, want 206 EFGH (served via forward fall-through)", w2.Code, w2.Body.String())
	}
	// The block fetch (and any invalidation it would wrongly trigger) is synchronous within
	// HandleGetObject, so the entry state is settled on return — assert directly, no wait needed.
	if meta, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found || meta.ETag != seeded.ETag {
		t.Fatalf("block-mode entry wiped by a 403 block fetch: found=%v (a 403 is principal-level, not object-gone)", found)
	}
}

// A pathologically large client range whose covering blocks are mostly uncached must NOT fan out
// into one aligned GET per block (a request storm). serveRangeFromBlockCache bails to a single
// upstream range GET, without touching the still-valid entry.
func TestBlockCache_LargeRangeServeBailsInsteadOfFanningOut(t *testing.T) {
	obj := make([]byte, 200) // block_size 4 -> 50 covering blocks, > maxRangeBlockFanout (32)
	for i := range obj {
		obj[i] = byte('A' + i%26)
	}
	mock := newBlockMock(obj, `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish the block-mode entry with a small read (block 0 + meta).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("small read: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("entry not established")
	}
	before := mock.blockGets.Load()

	// A range spanning all 50 blocks (~49 missing > cap) must bail to one upstream range GET.
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, blockGet(wowBucket, wowKey, "bytes=0-199")); err != nil {
		t.Fatalf("large range: %v", err)
	}
	if w2.Code != http.StatusPartialContent || w2.Body.Len() != 200 {
		t.Fatalf("large range: code=%d len=%d, want 206 with 200 bytes", w2.Code, w2.Body.Len())
	}
	if after := mock.blockGets.Load(); after != before {
		t.Errorf("large range fanned out into per-block fetches: blockGets %d -> %d", before, after)
	}
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found {
		t.Error("range amplify-bail wrongly invalidated the entry")
	}
}

// A cold single range read that touches more than maxRangeBlockFanout blocks must skip the
// background block populate (serving from upstream instead of a per-block fetch storm), so no meta
// is written and no per-block fetches are issued.
func TestBlockCache_ColdLargeRangeSkipsPopulate(t *testing.T) {
	obj := make([]byte, 200) // 50 blocks > maxRangeBlockFanout (32)
	for i := range obj {
		obj[i] = byte('A' + i%26)
	}
	mock := newBlockMock(obj, `"v1"`)
	svc, c := newBlockService(t, mock)

	before := mock.blockGets.Load()
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-199")); err != nil {
		t.Fatalf("cold large range: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.Len() != 200 {
		t.Fatalf("cold large range: code=%d len=%d", w.Code, w.Body.Len())
	}
	// Populate skipped: no meta appears within the window, and no per-block fetches were issued.
	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("block-mode meta written for an over-large cold range (populate should be skipped)")
	}
	if after := mock.blockGets.Load(); after != before {
		t.Errorf("cold large range fanned out into per-block fetches: %d -> %d", before, after)
	}
}
