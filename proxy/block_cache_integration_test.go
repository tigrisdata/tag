package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
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
	forwards            atomic.Int32 // count of client-range forwards (DoRequestWithCreds)
}

func newBlockMock(object []byte, etag string) *blockMockForwarder {
	m := &blockMockForwarder{mockForwarder: &mockForwarder{}, object: object, etag: etag, blockGetETag: etag}
	// The client forward serves the requested range, or the whole object for a full GET. A
	// deleted object (blockGet404) 404s the forward too, not only per-block fetches.
	m.mockForwarder.doRequestFunc = func(_ context.Context, r *http.Request, _, _ string) (*http.Response, error) {
		m.forwards.Add(1)
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

// countingBlockClient separates the byte-zero presence probes from payload reads. The
// production cache wrapper turns BlockExistsErr into the backend's [0,1] read (the cache
// layer widens [0,0] for backends that cannot return a single byte), while a block serve
// asks for a non-empty block-local range.
type countingBlockClient struct {
	cacheclient.CacheClient
	presenceReads atomic.Int64
	payloadReads  atomic.Int64
}

func (c *countingBlockClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if start == 0 && end == 1 {
		c.presenceReads.Add(1)
	} else {
		c.payloadReads.Add(1)
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
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
	svc.config.Cache.SetBlockCachingEnabled(true)
	svc.config.Cache.BlockSize = 4 // boundary: the 10-byte test object (>= 4) is block-mode
	svc.config.Cache.SizeThreshold = 1 << 20
	return svc, c
}

// newBlockServiceWithBudget builds a block-mode service with a specific populate byte budget,
// set at construction so the byteBudget reflects it (the budget is built in NewService).
func newBlockServiceWithBudget(t *testing.T, mock *blockMockForwarder, blockSize, budget int64) (*Service, *cache.Cache) {
	t.Helper()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = 1 << 20
	cfg.Cache.MaxPopulateMemoryBytes = budget
	c := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	return NewService(mock, c, cfg), c
}

func newCountingBlockService(t *testing.T, mock *blockMockForwarder, blockSize int64) (*Service, *cache.Cache, *countingBlockClient) {
	t.Helper()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = 1 << 20
	client := &countingBlockClient{CacheClient: cacheclient.NewMemoryCache()}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	return NewService(mock, store, cfg), store, client
}

func newCountingBlockServiceWithBudget(t *testing.T, mock *blockMockForwarder, blockSize, budget int64) (*Service, *cache.Cache, *countingBlockClient) {
	t.Helper()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = 1 << 20
	cfg.Cache.MaxPopulateMemoryBytes = budget
	client := &countingBlockClient{CacheClient: cacheclient.NewMemoryCache()}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	return NewService(mock, store, cfg), store, client
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

// A range-created entry records its initial contiguous span only after the blocks are durable.
// Later same-span and contained multi-block requests read payloads directly without paying the
// byte-zero presence preflight, while the response remains byte-exact.
func TestBlockCache_RangeCertificatePayloadReadsAndBudgetFallback(t *testing.T) {
	for _, blocks := range []int{2, 4, 8, 32} {
		t.Run(fmt.Sprintf("blocks-%d", blocks), func(t *testing.T) {
			body := make([]byte, blocks*4)
			for i := range body {
				body[i] = byte('A' + i%26)
			}
			mock := newBlockMock(body, `"v1"`)
			svc, c, client := newCountingBlockService(t, mock, 4)
			fullRange := fmt.Sprintf("bytes=0-%d", len(body)-1)

			cold := httptest.NewRecorder()
			if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, fullRange)); err != nil {
				t.Fatalf("cold range miss: %v", err)
			}
			if cold.Code != http.StatusPartialContent || cold.Body.String() != string(body) {
				t.Fatalf("cold response = (%d, %d bytes), want (206, %d exact bytes)", cold.Code, cold.Body.Len(), len(body))
			}
			if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
				t.Fatal("range-created metadata was not published")
			}
			meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
			if meta.BlocksComplete {
				t.Fatal("range-created entry was incorrectly marked BlocksComplete")
			}
			if meta.RangeBlockSpan == nil || meta.RangeBlockSpan.First != 0 || meta.RangeBlockSpan.Last != int64(blocks-1) {
				t.Fatalf("RangeBlockSpan = %#v, want [0,%d]", meta.RangeBlockSpan, blocks-1)
			}

			defaultStagingCap := svc.populateBudget.stagingCap
			for _, tc := range []struct {
				rangeHeader   string
				want          string
				payloadReads  int64
				presenceReads int64
				stagingCap    int64
			}{
				{rangeHeader: fullRange, want: string(body), payloadReads: int64(blocks)},
				{rangeHeader: "bytes=1-6", want: string(body[1:7]), payloadReads: 2},
				// The requested bytes do not fit this deliberately small stage cap, so the
				// probe-first fallback checks both covering blocks before the 206.
				{rangeHeader: "bytes=2-6", want: string(body[2:7]), payloadReads: 2, presenceReads: 2, stagingCap: 3},
			} {
				svc.populateBudget.stagingCap = defaultStagingCap
				if tc.stagingCap > 0 {
					svc.populateBudget.stagingCap = tc.stagingCap
				}
				client.presenceReads.Store(0)
				client.payloadReads.Store(0)
				warm := httptest.NewRecorder()
				if err := svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, tc.rangeHeader)); err != nil {
					t.Fatalf("warm range %s: %v", tc.rangeHeader, err)
				}
				if warm.Code != http.StatusPartialContent || warm.Body.String() != tc.want {
					t.Fatalf("warm response %s = (%d, %q), want (206, %q)", tc.rangeHeader, warm.Code, warm.Body.String(), tc.want)
				}
				if got := client.presenceReads.Load(); got != tc.presenceReads {
					t.Errorf("presence probes for certified range %s = %d, want %d", tc.rangeHeader, got, tc.presenceReads)
				}
				if got := client.payloadReads.Load(); got != tc.payloadReads {
					t.Errorf("payload reads for certified range %s = %d, want %d", tc.rangeHeader, got, tc.payloadReads)
				}
			}
		})
	}
}

// A request that extends beyond the recorded span retains probe-first behavior, including
// fetching the newly covered edge blocks before commitment.
func TestBlockCache_RangeCertificateOutsideSpanUsesProbePath(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJKLMNOPQRST"), `"v1"`) // five 4-byte blocks
	svc, c, client := newCountingBlockService(t, mock, 4)

	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=4-11")); err != nil {
		t.Fatalf("cold interior range miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("range-created metadata was not published")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if meta.RangeBlockSpan == nil || meta.RangeBlockSpan.First != 1 || meta.RangeBlockSpan.Last != 2 {
		t.Fatalf("RangeBlockSpan = %#v, want [1,2]", meta.RangeBlockSpan)
	}

	client.presenceReads.Store(0)
	warm := httptest.NewRecorder()
	if err := svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("range outside certificate: %v", err)
	}
	if warm.Code != http.StatusPartialContent || warm.Body.String() != "ABCDEFGHIJKLMNOP" {
		t.Fatalf("outside-span response = (%d, %q), want (206, full range)", warm.Code, warm.Body.String())
	}
	if got := client.presenceReads.Load(); got != 6 {
		t.Fatalf("outside-span presence probes = %d, want 6 (four covering probes plus two fetch rechecks)", got)
	}
}

// Older metadata has no span certificate; a complete hit must retain the probe-first behavior.
func TestBlockCache_LegacyMetadataKeepsProbePath(t *testing.T) {
	body := []byte("ABCDEFGHIJKLMNOP")
	mock := newBlockMock(body, `"v1"`)
	svc, c, client := newCountingBlockService(t, mock, 4)
	meta := &cache.CachedObjectMeta{
		Bucket:        wowBucket,
		Key:           wowKey,
		ETag:          `"v1"`,
		ContentLength: int64(len(body)),
		StatusCode:    http.StatusOK,
		BlockSize:     4,
	}
	if wrote, err := c.PutMetaIfVersion(context.Background(), wowBucket, wowKey, meta, 60, cache.VersionAny); err != nil || !wrote {
		t.Fatalf("legacy-style metadata write = (wrote=%t, err=%v)", wrote, err)
	}
	for block := int64(0); block < 4; block++ {
		start := block * 4
		blockKey := cache.MakeBlockKey(wowBucket, wowKey, meta.ETag, meta.BlockSize, block)
		if err := client.CacheClient.Put(context.Background(), blockKey, body[start:start+4], 0); err != nil {
			t.Fatalf("seed legacy block %d: %v", block, err)
		}
	}

	warm := httptest.NewRecorder()
	if err := svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("legacy metadata range: %v", err)
	}
	if warm.Code != http.StatusPartialContent || warm.Body.String() != string(body) || warm.Header().Get("X-Cache") != XCacheHit {
		t.Fatalf("legacy metadata response = (%d, %q, cache=%q), want exact 206/HIT", warm.Code, warm.Body.String(), warm.Header().Get("X-Cache"))
	}
	if got := client.presenceReads.Load(); got != 4 {
		t.Fatalf("legacy metadata presence probes = %d, want 4", got)
	}
	if got := client.payloadReads.Load(); got != 4 {
		t.Fatalf("legacy metadata payload reads = %d, want 4", got)
	}
	if got := mock.forwards.Load(); got != 0 {
		t.Fatalf("legacy metadata origin forwards = %d, want 0", got)
	}
}

// A certified-range staging decline retains the old probe-first serve instead of changing
// response behavior or forcing an upstream round trip.
func TestBlockCache_CertifiedRangeStagingDeclineUsesProbePath(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJKLMNOP"), `"v1"`)
	svc, c, client := newCountingBlockServiceWithBudget(t, mock, 4, 16)

	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("cold range miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("range-created metadata was not published")
	}
	beforeFetches := mock.blockGets.Load()
	client.presenceReads.Store(0)
	svc.populateBudget.stagingCap = 3 // smaller than the certified requested range

	warm := httptest.NewRecorder()
	if err := svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("certified range with staging decline: %v", err)
	}
	if warm.Code != http.StatusPartialContent || warm.Body.String() != "ABCDEFGHIJKLMNOP" {
		t.Fatalf("staging-declined response = (%d, %q), want (206, full range)", warm.Code, warm.Body.String())
	}
	if got := mock.blockGets.Load(); got != beforeFetches {
		t.Fatalf("staging-declined warm range fetched %d new blocks, want 0", got-beforeFetches)
	}
	if got := client.presenceReads.Load(); got != 4 {
		t.Fatalf("staging-declined presence probes = %d, want 4", got)
	}
}

// A certificate is advisory. If its first requested block has evicted and recovery fails,
// the cache path must leave the response untouched so the handler can use its upstream fallback.
func TestBlockCache_RangeCertificateFirstBlockFailureFallsThroughCleanly(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJKLMNOP"), `"v1"`)
	svc, c, client := newCountingBlockService(t, mock, 4)

	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("cold range miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("range-created metadata was not published")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if err := client.CacheClient.Delete(context.Background(), cache.MakeBlockKey(wowBucket, wowKey, meta.ETag, meta.BlockSize, 0)); err != nil {
		t.Fatalf("evict first block: %v", err)
	}
	mock.blockGetTransient = true

	beforeForwards := mock.forwards.Load()
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("first-block failure fallback: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "ABCDEFGHIJKLMNOP" {
		t.Fatalf("first-block failure fallback = (%d, %q), want upstream 206 body", w.Code, w.Body.String())
	}
	if got := mock.forwards.Load() - beforeForwards; got != 1 {
		t.Fatalf("first-block failure upstream forwards = %d, want 1", got)
	}
}

// A later certified block that is absent when its payload is read is recovered before the
// full-range response commits; the certificate does not turn into a presence invariant.
func TestBlockCache_RangeCertificateLaterBlockEvictionRecovers(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJKLMNOP"), `"v1"`)
	svc, c, client := newCountingBlockService(t, mock, 4)

	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("cold range miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("range-created metadata was not published")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if err := client.CacheClient.Delete(context.Background(), cache.MakeBlockKey(wowBucket, wowKey, meta.ETag, meta.BlockSize, 2)); err != nil {
		t.Fatalf("evict middle block: %v", err)
	}
	beforeFetches := mock.blockGets.Load()
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("certified range after later-block eviction: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "ABCDEFGHIJKLMNOP" {
		t.Fatalf("later-block recovery = (%d, %q), want (206, full range)", w.Code, w.Body.String())
	}
	if got := mock.blockGets.Load() - beforeFetches; got != 1 {
		t.Fatalf("later-block recovery upstream fetches = %d, want 1", got)
	}
}

type blockRangeGateClient struct {
	cacheclient.CacheClient
	key     string
	armed   atomic.Bool
	entered chan struct{}
	release <-chan struct{}
}

func (c *blockRangeGateClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if key == c.key && start == 0 && end > 1 && c.armed.CompareAndSwap(true, false) {
		select {
		case c.entered <- struct{}{}:
		default:
		}
		select {
		case <-c.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

// An eviction between the start of parallel staging and one later block's read remains a
// pre-commit miss: the response waits for the block, then recovers it before writing headers.
func TestBlockCache_RangeCertificateEvictionDuringPrecommitRecoversBeforeHeaders(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJKLMNOP"), `"v1"`)
	backing := cacheclient.NewMemoryCache()
	release := make(chan struct{})
	var released atomic.Bool
	unblock := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	defer unblock()
	gate := &blockRangeGateClient{
		CacheClient: backing,
		entered:     make(chan struct{}, 1),
		release:     release,
	}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(gate, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("cold range miss: %v", err)
	}
	metaDeadline := time.NewTimer(2 * time.Second)
	defer metaDeadline.Stop()
	metaPoll := time.NewTicker(5 * time.Millisecond)
	defer metaPoll.Stop()
	var meta *cache.CachedObjectMeta
	var metaErr error
	for {
		var found bool
		meta, found, metaErr = c.GetMeta(context.Background(), wowBucket, wowKey)
		if metaErr == nil && found && meta != nil && meta.RangeBlockSpan != nil {
			break
		}
		select {
		case <-metaPoll.C:
		case <-metaDeadline.C:
			t.Fatalf("range metadata = (found=%t, meta=%#v, err=%v), want certified entry", found, meta, metaErr)
		}
	}
	gate.key = cache.MakeBlockKey(wowBucket, wowKey, meta.ETag, meta.BlockSize, 2)
	gate.armed.Store(true)
	beforeFetches := mock.blockGets.Load()
	warm := httptest.NewRecorder()
	done := make(chan error, 1)
	go func() { done <- svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=0-15")) }()

	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("certified read did not reach the gated later block")
	}
	if warm.Body.Len() != 0 || warm.Header().Get("X-Cache") != "" {
		t.Fatalf("response committed before later block read: status=%d cache=%q body=%q", warm.Code, warm.Header().Get("X-Cache"), warm.Body.String())
	}
	if err := backing.Delete(context.Background(), gate.key); err != nil {
		t.Fatalf("evict later block during pre-commit read: %v", err)
	}
	unblock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("range after gated eviction: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("range did not finish after releasing the later-block read")
	}
	if warm.Code != http.StatusPartialContent || warm.Body.String() != "ABCDEFGHIJKLMNOP" || warm.Header().Get("X-Cache") != XCacheHit {
		t.Fatalf("range after in-flight eviction = (%d, %q, cache=%q), want complete 206/HIT", warm.Code, warm.Body.String(), warm.Header().Get("X-Cache"))
	}
	if got := mock.blockGets.Load() - beforeFetches; got != 1 {
		t.Fatalf("in-flight eviction recovery upstream fetches = %d, want 1", got)
	}
}

// If a certified block was already evicted and upstream has moved on, the complete
// range must fall through before writing old-version headers or a prefix. Cover the first
// requested block and a later block because they take different speculative-read paths.
func TestBlockCache_RangeCertificateStaleFallsThroughBeforeCommit(t *testing.T) {
	tests := []struct {
		name       string
		evictBlock int64
		configure  func(*blockMockForwarder)
		wantStatus int
		wantBody   string
		wantETag   string
	}{
		{
			name:       "first block version replacement",
			evictBlock: 0,
			configure: func(mock *blockMockForwarder) {
				mock.etag = `"v2"`
				mock.blockGetETag = `"v2"`
				mock.object = []byte("QRSTUVWXYZabcdef")
			},
			wantStatus: http.StatusPartialContent,
			wantBody:   "QRSTUVWXYZabcdef",
			wantETag:   `"v2"`,
		},
		{
			name:       "first block tombstone",
			evictBlock: 0,
			configure: func(mock *blockMockForwarder) {
				mock.blockGet404 = true
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "later block version replacement",
			evictBlock: 2,
			configure: func(mock *blockMockForwarder) {
				mock.etag = `"v2"`
				mock.blockGetETag = `"v2"`
				mock.object = []byte("QRSTUVWXYZabcdef")
			},
			wantStatus: http.StatusPartialContent,
			wantBody:   "QRSTUVWXYZabcdef",
			wantETag:   `"v2"`,
		},
		{
			name:       "later block tombstone",
			evictBlock: 2,
			configure: func(mock *blockMockForwarder) {
				mock.blockGet404 = true
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := newBlockMock([]byte("ABCDEFGHIJKLMNOP"), `"v1"`)
			svc, c, client := newCountingBlockService(t, mock, 4)
			cold := httptest.NewRecorder()
			if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
				t.Fatalf("cold range miss: %v", err)
			}
			if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
				t.Fatal("range-created metadata was not published")
			}
			meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
			key := cache.MakeBlockKey(wowBucket, wowKey, meta.ETag, meta.BlockSize, tc.evictBlock)
			if err := client.CacheClient.Delete(context.Background(), key); err != nil {
				t.Fatalf("evict block %d: %v", tc.evictBlock, err)
			}
			tc.configure(mock)

			warm := httptest.NewRecorder()
			if err := svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
				t.Fatalf("stale range fallback: %v", err)
			}
			if warm.Code != tc.wantStatus || warm.Body.String() != tc.wantBody {
				t.Fatalf("stale response = (%d, %q), want (%d, %q)", warm.Code, warm.Body.String(), tc.wantStatus, tc.wantBody)
			}
			if tc.wantStatus == http.StatusPartialContent && warm.Header().Get("Content-Range") != "bytes 0-15/16" {
				t.Fatalf("stale Content-Range = %q, want bytes 0-15/16", warm.Header().Get("Content-Range"))
			}
			if got := warm.Header().Get("X-Cache"); got != XCacheMiss {
				t.Fatalf("stale X-Cache = %q, want %q", got, XCacheMiss)
			}
			if got := warm.Header().Get("ETag"); got != tc.wantETag {
				t.Fatalf("stale ETag = %q, want %q", got, tc.wantETag)
			}
			if current, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); found && current.ETag == `"v1"` {
				t.Fatalf("stale v1 metadata survived fallback: %#v", current)
			}
		})
	}
}

// A certified span may begin after block zero; full-range staging still returns its exact
// requested object bytes without probing or shifting block-local offsets.
func TestBlockCache_RangeCertificateInteriorSpanServesExactBytes(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJKLMNOPQRST"), `"v1"`)
	svc, c, client := newCountingBlockService(t, mock, 4)
	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=4-15")); err != nil {
		t.Fatalf("cold interior range: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("range-created metadata was not published")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if meta == nil || meta.RangeBlockSpan == nil || meta.RangeBlockSpan.First != 1 || meta.RangeBlockSpan.Last != 3 {
		t.Fatalf("interior metadata = %#v, want certificate blocks 1-3", meta)
	}
	client.presenceReads.Store(0)

	warm := httptest.NewRecorder()
	if err := svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=4-15")); err != nil {
		t.Fatalf("warm interior range: %v", err)
	}
	if warm.Code != http.StatusPartialContent || warm.Body.String() != "EFGHIJKLMNOP" {
		t.Fatalf("warm interior response = (%d, %q), want exact 206 body", warm.Code, warm.Body.String())
	}
	if got := client.presenceReads.Load(); got != 0 {
		t.Fatalf("warm interior presence probes = %d, want 0", got)
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
	svc.config.Cache.SetBlockCachingEnabled(true)
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

	if _, err := svc.putBlocksFromStream(context.Background(), wowBucket, wowKey, meta, strings.NewReader("ABCDEFGH"), 60, cache.VersionAny); err != nil {
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
	if _, err := svc.putBlocksFromStream(context.Background(), wowBucket, wowKey, meta, r, 60, cache.VersionAny); err == nil {
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
	if _, err := svc.putBlocksFromStream(context.Background(), wowBucket, wowKey, meta, strings.NewReader("ABCDEF"), 60, cache.VersionAny); err == nil {
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
	if _, err := svc.putBlocksFromStream(context.Background(), wowBucket, wowKey, meta, strings.NewReader("ABCDEF"), 60, cache.VersionAny); err == nil {
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
	svc.invalidateStaleMeta(wowBucket, wowKey, `"v1"`)
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found {
		t.Error("v2 entry wrongly deleted by a stale-v1 invalidation")
	}

	// Invalidating the matching (current) version does delete it.
	svc.invalidateStaleMeta(wowBucket, wowKey, `"v2"`)
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

type rangeTestUpstream struct {
	requests   atomic.Int32
	rangeCalls atomic.Int32
	lastRange  atomic.Value
	invalid    atomic.Bool
}

func newRangeTestUpstream(t *testing.T, bucket, key string, object []byte, etag string) (*httptest.Server, *rangeTestUpstream) {
	t.Helper()
	state := &rangeTestUpstream{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state.requests.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/"+bucket+"/"+key {
			state.invalid.Store(true)
			http.Error(w, "unexpected upstream request", http.StatusBadRequest)
			return
		}
		if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
			state.rangeCalls.Add(1)
			state.lastRange.Store(rangeHeader)
			var start, end int64
			if count, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil || count != 2 || start < 0 || end < start || end >= int64(len(object)) {
				state.invalid.Store(true)
				http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			body := object[start : end+1]
			w.Header().Set("ETag", etag)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(object)))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
			w.WriteHeader(http.StatusPartialContent)
			if _, err := w.Write(body); err != nil {
				state.invalid.Store(true)
			}
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(object)))
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(object); err != nil {
			state.invalid.Store(true)
		}
	}))
	t.Cleanup(server.Close)
	return server, state
}

func newHTTPRangeService(t *testing.T, endpoint string, blockCaching bool) (*Service, *cache.Cache) {
	t.Helper()
	cfg := config.NewDefault()
	cfg.Upstream.Endpoint = endpoint
	cfg.Cache.SetBlockCachingEnabled(blockCaching)
	if blockCaching {
		cfg.Cache.BlockSize = 4
		cfg.Cache.SizeThreshold = 1 << 20
	}
	store := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	forwarder := NewForwarder(nil, endpoint, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost, auth.NewProxySigner("test-proxy-access", "test-proxy-secret"), nil)
	return NewService(forwarder, store, cfg), store
}

func seedTwoBlockCompleteEntry(t *testing.T, c *cache.Cache, blocks [][]byte) {
	t.Helper()
	if len(blocks) != 2 {
		t.Fatalf("seed blocks = %d, want 2", len(blocks))
	}
	ctx := context.Background()
	const blockSize = int64(4)
	for idx, data := range blocks {
		if err := c.PutBlockStream(ctx, wowBucket, wowKey, `"v1"`, blockSize, int64(idx), bytes.NewReader(data), 60); err != nil {
			t.Fatalf("seed block %d: %v", idx, err)
		}
	}
	meta := &cache.CachedObjectMeta{
		Bucket:         wowBucket,
		Key:            wowKey,
		ETag:           `"v1"`,
		ContentLength:  8,
		StatusCode:     http.StatusOK,
		BlockSize:      blockSize,
		BlocksComplete: true,
	}
	if wrote, err := c.PutMetaIfVersion(ctx, wowBucket, wowKey, meta, 60, cache.VersionAny); err != nil || !wrote {
		t.Fatalf("seed BlocksComplete metadata = (wrote=%t, err=%v)", wrote, err)
	}
}

func seedWholeObjectRangeEntry(t *testing.T, c *cache.Cache, body []byte, contentLength int64) {
	t.Helper()
	ctx := context.Background()
	meta := &cache.CachedObjectMeta{
		Bucket:        wowBucket,
		Key:           wowKey,
		ETag:          `"v1"`,
		ContentLength: contentLength,
		StatusCode:    http.StatusOK,
	}
	if err := c.PutBodyStream(ctx, wowBucket, wowKey, meta.ETag, bytes.NewReader(body), 60); err != nil {
		t.Fatalf("seed whole-object body: %v", err)
	}
	if wrote, err := c.PutMetaIfVersion(ctx, wowBucket, wowKey, meta, 60, cache.VersionAny); err != nil || !wrote {
		t.Fatalf("seed whole-object metadata = (wrote=%t, err=%v)", wrote, err)
	}
}

// A short positive block read after the first block commits must recover only the
// exact unwritten suffix from an upstream response with the cached ETag.
func TestBlockCache_ShortPositiveReadRecoversSameETagRemainder(t *testing.T) {
	object := []byte("ABCDEFGH")
	server, upstream := newRangeTestUpstream(t, wowBucket, wowKey, object, `"v1"`)
	svc, c := newHTTPRangeService(t, server.URL, true)
	seedTwoBlockCompleteEntry(t, c, [][]byte{[]byte("ABCD"), []byte("EF")})

	beforeSuccesses := requestCount(t, "success")
	beforeErrors := requestCount(t, "error")
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("HandleGetObject() error = %v, want same-ETag suffix recovery", err)
	}
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), object) {
		t.Errorf("response = (status=%d, body=%q), want (200, %q)", w.Code, w.Body.Bytes(), object)
	}
	if got := w.Header().Get("Content-Length"); got != "8" {
		t.Errorf("Content-Length = %q, want 8", got)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if got := upstream.rangeCalls.Load(); got != 1 {
		t.Errorf("same-ETag suffix requests = %d, want exactly 1", got)
	} else if got := upstream.lastRange.Load(); got != "bytes=6-7" {
		t.Errorf("same-ETag upstream Range = %v, want bytes=6-7", got)
	}
	if got := upstream.requests.Load(); got != 1 || upstream.invalid.Load() {
		t.Errorf("upstream requests=%d invalid=%t, want one valid suffix request", got, upstream.invalid.Load())
	}
	if got := requestCount(t, "success"); got != beforeSuccesses+1 {
		t.Errorf("success requests = %v, want %v", got, beforeSuccesses+1)
	}
	if got := requestCount(t, "error"); got != beforeErrors {
		t.Errorf("error requests = %v, want %v", got, beforeErrors)
	}
}

// A short first block is still handled before headers commit, so the full GET
// falls through to its ordinary upstream path rather than exposing a prefix.
func TestBlockCache_ShortFirstBlockFallsThroughBeforeCommit(t *testing.T) {
	object := []byte("ABCDEFGH")
	server, upstream := newRangeTestUpstream(t, wowBucket, wowKey, object, `"v1"`)
	svc, c := newHTTPRangeService(t, server.URL, true)
	seedTwoBlockCompleteEntry(t, c, [][]byte{[]byte("AB"), []byte("EFGH")})

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("HandleGetObject() error = %v, want pre-commit upstream fall-through", err)
	}
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), object) {
		t.Errorf("response = (status=%d, body=%q), want complete upstream 200", w.Code, w.Body.Bytes())
	}
	if got := w.Header().Get("Content-Length"); got != "8" {
		t.Errorf("Content-Length = %q, want 8", got)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheMiss {
		t.Errorf("%s = %q, want %q", XCacheHeader, got, XCacheMiss)
	}
	if got := upstream.requests.Load(); got != 1 || upstream.rangeCalls.Load() != 0 || upstream.invalid.Load() {
		t.Errorf("upstream requests=%d ranges=%d invalid=%t, want one full GET", got, upstream.rangeCalls.Load(), upstream.invalid.Load())
	}
}

// A changed upstream object must not be appended to the already-committed
// cached prefix, and the request must be counted as an error rather than success.
func TestBlockCache_ShortPositiveReadRejectsChangedETagRemainder(t *testing.T) {
	server, upstream := newRangeTestUpstream(t, wowBucket, wowKey, []byte("abcdefgh"), `"v2"`)
	svc, c := newHTTPRangeService(t, server.URL, true)
	seedTwoBlockCompleteEntry(t, c, [][]byte{[]byte("ABCD"), []byte("EF")})

	beforeSuccesses := requestCount(t, "success")
	beforeErrors := requestCount(t, "error")
	w := httptest.NewRecorder()
	err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey))
	if !errors.Is(err, errBlockETagMismatch) {
		t.Errorf("HandleGetObject() error = %v, want changed-ETag error", err)
	}
	if w.Code != http.StatusOK || w.Body.String() != "ABCDEF" {
		t.Errorf("response = (status=%d, body=%q), want only cached prefix ABCDEF", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Length"); got != "8" {
		t.Errorf("Content-Length = %q, want 8", got)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if got := upstream.rangeCalls.Load(); got != 1 {
		t.Errorf("changed-ETag suffix requests = %d, want exactly 1", got)
	} else if got := upstream.lastRange.Load(); got != "bytes=6-7" {
		t.Errorf("changed-ETag upstream Range = %v, want bytes=6-7", got)
	}
	if got := upstream.requests.Load(); got != 1 || upstream.invalid.Load() {
		t.Errorf("upstream requests=%d invalid=%t, want one valid suffix request", got, upstream.invalid.Load())
	}
	if got := requestCount(t, "success"); got != beforeSuccesses {
		t.Errorf("success requests = %v, want unchanged %v", got, beforeSuccesses)
	}
	if got := requestCount(t, "error"); got != beforeErrors+1 {
		t.Errorf("error requests = %v, want %v", got, beforeErrors+1)
	}
}

// An exact whole-object cached Range GET must continue to deliver every requested
// byte and record success after the shared range-length check is applied.
func TestWholeBodyRange_ExactCachedReadSucceeds(t *testing.T) {
	object := []byte("ABCDEFGH")
	server, upstream := newRangeTestUpstream(t, wowBucket, wowKey, object, `"v1"`)
	svc, c := newHTTPRangeService(t, server.URL, false)
	seedWholeObjectRangeEntry(t, c, object, int64(len(object)))

	beforeSuccesses := requestCount(t, "success")
	beforeErrors := requestCount(t, "error")
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-7")); err != nil {
		t.Fatalf("HandleGetObject() error = %v, want exact cached range", err)
	}
	if w.Code != http.StatusPartialContent || !bytes.Equal(w.Body.Bytes(), object) {
		t.Errorf("response = (status=%d, body=%q), want exact 206 body %q", w.Code, w.Body.Bytes(), object)
	}
	if got := w.Header().Get("Content-Length"); got != "8" {
		t.Errorf("Content-Length = %q, want 8", got)
	}
	if got := w.Header().Get("Content-Range"); got != "bytes 0-7/8" {
		t.Errorf("Content-Range = %q, want bytes 0-7/8", got)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if got := upstream.requests.Load(); got != 0 {
		t.Errorf("exact cached range made %d upstream requests, want 0", got)
	}
	if got := requestCount(t, "success"); got != beforeSuccesses+1 {
		t.Errorf("success requests = %v, want %v", got, beforeSuccesses+1)
	}
	if got := requestCount(t, "error"); got != beforeErrors {
		t.Errorf("error requests = %v, want %v", got, beforeErrors)
	}
}

// A positive short whole-object read commits 206 on its first chunk. It must
// return an error rather than a successful cache hit or an upstream restart.
func TestWholeBodyRange_ShortPositiveReadReturnsError(t *testing.T) {
	server, upstream := newRangeTestUpstream(t, wowBucket, wowKey, []byte("ABCDEFGH"), `"v1"`)
	svc, c := newHTTPRangeService(t, server.URL, false)
	seedWholeObjectRangeEntry(t, c, []byte("ABCD"), 8)

	beforeSuccesses := requestCount(t, "success")
	beforeErrors := requestCount(t, "error")
	w := httptest.NewRecorder()
	err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-7"))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("HandleGetObject() error = %v, want short-range error", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "ABCD" {
		t.Errorf("response = (status=%d, body=%q), want committed 206 with prefix ABCD", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Length"); got != "8" {
		t.Errorf("Content-Length = %q, want 8", got)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if got := upstream.requests.Load(); got != 0 {
		t.Errorf("upstream HTTP requests = %d, want 0 after response commitment", got)
	}
	if got := requestCount(t, "success"); got != beforeSuccesses {
		t.Errorf("success requests = %v, want unchanged %v", got, beforeSuccesses)
	}
	if got := requestCount(t, "error"); got != beforeErrors+1 {
		t.Errorf("error requests = %v, want %v", got, beforeErrors+1)
	}
}

var errInjectedClientWrite = errors.New("injected client write failure")

type failFirstResponseWrite struct {
	http.ResponseWriter
	writes int
}

func (w *failFirstResponseWrite) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == 1 {
		return 0, errInjectedClientWrite
	}
	return w.ResponseWriter.Write(p)
}

// A failed client write after the cached 206 commits must return the writer error,
// record an error, and stop without another response write or upstream request.
func TestWholeBodyRange_FirstClientWriteFailureIsError(t *testing.T) {
	body := []byte("ABCDEFGH")
	server, upstream := newRangeTestUpstream(t, wowBucket, wowKey, body, `"v1"`)
	svc, c := newHTTPRangeService(t, server.URL, false)
	seedWholeObjectRangeEntry(t, c, body, int64(len(body)))

	var cached bytes.Buffer
	if err := c.GetRangeStream(context.Background(), wowBucket, wowKey, `"v1"`, 0, int64(len(body))-1, &cached); err != nil {
		t.Fatalf("verify exact cached range before injected write failure: %v", err)
	}
	if !bytes.Equal(cached.Bytes(), body) {
		t.Fatalf("cached range = %q, want %q", cached.Bytes(), body)
	}

	beforeSuccesses := requestCount(t, "success")
	beforeErrors := requestCount(t, "error")
	recorder := httptest.NewRecorder()
	w := &failFirstResponseWrite{ResponseWriter: recorder}
	err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-7"))
	if !errors.Is(err, errInjectedClientWrite) {
		t.Errorf("HandleGetObject() error = %v, want injected client write failure", err)
	}
	if w.writes != 1 || recorder.Code != http.StatusPartialContent || recorder.Body.Len() != 0 {
		t.Errorf("response writes=%d status=%d body=%q, want one failed write after committing an empty 206", w.writes, recorder.Code, recorder.Body.Bytes())
	}
	if got := recorder.Header().Get("Content-Length"); got != "8" {
		t.Errorf("Content-Length = %q, want 8", got)
	}
	if got := recorder.Header().Get("Content-Range"); got != "bytes 0-7/8" {
		t.Errorf("Content-Range = %q, want bytes 0-7/8", got)
	}
	if got := recorder.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if got := upstream.requests.Load(); got != 0 {
		t.Errorf("client write failure triggered %d upstream HTTP requests, want 0 after commitment", got)
	}
	if got := requestCount(t, "success"); got != beforeSuccesses {
		t.Errorf("success requests = %v, want unchanged %v", got, beforeSuccesses)
	}
	if got := requestCount(t, "error"); got != beforeErrors+1 {
		t.Errorf("error requests = %v, want %v", got, beforeErrors+1)
	}
}

// blockReadFaultClient wraps a CacheClient and, when armed, fails every block-key range read
// with a transient error — simulating a cluster gRPC blip during a warm serve's block reads.
type blockReadFaultClient struct {
	cacheclient.CacheClient
	fail atomic.Bool
}

func (p *blockReadFaultClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if p.fail.Load() && strings.HasPrefix(key, "blk|") {
		return fmt.Errorf("simulated block read blip")
	}
	return p.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

// countingBlockReadClient distinguishes block payload reads from metadata operations. A complete
// range must issue exactly one payload read for each covering block; the old probe-first path
// issued a second byte-zero read for every one of them.
type countingBlockReadClient struct {
	cacheclient.CacheClient
	reads atomic.Int32
}

func (c *countingBlockReadClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if strings.HasPrefix(key, "blk|") {
		c.reads.Add(1)
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

// blockReadSelectiveFaultClient injects a transient failure for one block's payload read while
// allowing its byte-zero presence probe to succeed, so the base reaches its post-commit remainder path.
type blockReadSelectiveFaultClient struct {
	cacheclient.CacheClient
	fail atomic.Bool
}

func (p *blockReadSelectiveFaultClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if p.fail.Load() && strings.HasPrefix(key, "blk|") && strings.HasSuffix(key, "|2") && !(start == 0 && end == 1) {
		return fmt.Errorf("simulated block 2 payload read blip")
	}
	return p.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

type blockReadBlockOneFaultClient struct {
	cacheclient.CacheClient
	fail atomic.Bool
}

func (p *blockReadBlockOneFaultClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if p.fail.Load() && strings.HasPrefix(key, "blk|") && strings.HasSuffix(key, "|1") {
		return fmt.Errorf("simulated block 1 read blip")
	}
	return p.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

type blockFirstTransientLaterStallClient struct {
	cacheclient.CacheClient
	firstKey      string
	laterKey      string
	armed         atomic.Bool
	laterSignaled atomic.Bool
	laterStarted  chan struct{}
}

func (c *blockFirstTransientLaterStallClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if c.armed.Load() {
		switch key {
		case c.firstKey:
			// Hold the first transient until a later cache read has entered its wait, making the
			// cancellation schedule deterministic instead of depending on goroutine scheduling.
			select {
			case <-c.laterStarted:
				return fmt.Errorf("simulated first-block cache read failure")
			case <-ctx.Done():
				return ctx.Err()
			}
		case c.laterKey:
			if c.laterSignaled.CompareAndSwap(false, true) {
				close(c.laterStarted)
			}
			<-ctx.Done()
			return ctx.Err()
		}
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

// A transient failure in the commitment block must cancel unrelated outstanding payload reads
// before the request deadline, so the handler can perform the clean upstream fallback.
func TestBlockCache_CertifiedFirstTransientCancelsLaterReadBeforeDeadline(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGH"), `"v1"`)
	faulty := &blockFirstTransientLaterStallClient{
		CacheClient:  cacheclient.NewMemoryCache(),
		laterStarted: make(chan struct{}),
	}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	store := cache.NewCacheWithClient(faulty, &cfg.Cache)
	svc := NewService(mock, store, cfg)

	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=0-7")); err != nil {
		t.Fatalf("cold range miss: %v", err)
	}
	if !metaCached(store, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("range-created metadata was not published")
	}
	meta, found, err := store.GetMeta(context.Background(), wowBucket, wowKey)
	if err != nil || !found || meta == nil || meta.RangeBlockSpan == nil || meta.BlocksComplete {
		t.Fatalf("range metadata = (found=%t, meta=%#v, err=%v), want non-BlocksComplete certificate", found, meta, err)
	}
	faulty.firstKey = cache.MakeBlockKey(wowBucket, wowKey, meta.ETag, meta.BlockSize, 0)
	faulty.laterKey = cache.MakeBlockKey(wowBucket, wowKey, meta.ETag, meta.BlockSize, 1)
	faulty.armed.Store(true)

	originalForward := mock.mockForwarder.doRequestFunc
	var fallbackSawLiveContext atomic.Bool
	mock.mockForwarder.doRequestFunc = func(ctx context.Context, r *http.Request, accessKey, secretKey string) (*http.Response, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fallbackSawLiveContext.Store(true)
		return originalForward(ctx, r, accessKey, secretKey)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	warm := httptest.NewRecorder()
	requestDone := make(chan error, 1)
	go func() {
		requestDone <- svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=0-7").WithContext(ctx))
	}()
	select {
	case <-faulty.laterStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("later cache read did not start")
	}
	select {
	case err := <-requestDone:
		if err != nil {
			t.Fatalf("certified range fallback: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("certified range waited for the later read instead of cancelling it")
	}
	if ctx.Err() != nil || !fallbackSawLiveContext.Load() {
		t.Fatalf("upstream fallback used expired request context: ctxErr=%v", ctx.Err())
	}
	if warm.Code != http.StatusPartialContent || warm.Body.String() != "ABCDEFGH" || warm.Header().Get("X-Cache") != XCacheMiss {
		t.Fatalf("first-block transient response = (%d, %q, cache=%q), want exact 206/MISS", warm.Code, warm.Body.String(), warm.Header().Get("X-Cache"))
	}
}

type certifiedRangeStallLaterTransientClient struct {
	cacheclient.CacheClient
	stallSuffix string
	failSuffix  string
	armed       atomic.Bool
	started     chan struct{}
	stopped     chan struct{}
	startOnce   atomic.Bool
	stopOnce    atomic.Bool
}

func (c *certifiedRangeStallLaterTransientClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if c.armed.Load() && strings.HasPrefix(key, "blk|") {
		if c.failSuffix != "" && strings.HasSuffix(key, c.failSuffix) {
			return errors.New("simulated later block transient")
		}
		if strings.HasSuffix(key, c.stallSuffix) && end-start+1 > 2 {
			if c.startOnce.CompareAndSwap(false, true) {
				close(c.started)
			}
			<-ctx.Done()
			if c.stopOnce.CompareAndSwap(false, true) {
				close(c.stopped)
			}
			return ctx.Err()
		}
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

func seedCompleteBlockEntry(tb testing.TB, c *cache.Cache, base cacheclient.CacheClient, bucket, key, etag string, object []byte, blockSize int64) *cache.CachedObjectMeta {
	tb.Helper()
	meta := &cache.CachedObjectMeta{
		Bucket:         bucket,
		Key:            key,
		ETag:           etag,
		ContentLength:  int64(len(object)),
		StatusCode:     http.StatusOK,
		BlockSize:      blockSize,
		BlocksComplete: true,
		CachedAt:       time.Now().Unix(),
	}
	for idx := int64(0); idx*blockSize < int64(len(object)); idx++ {
		start := idx * blockSize
		end := start + blockSize
		if end > int64(len(object)) {
			end = int64(len(object))
		}
		if err := base.Put(context.Background(), cache.MakeBlockKey(bucket, key, etag, blockSize, idx), object[start:end], 60); err != nil {
			tb.Fatalf("seed block %d: %v", idx, err)
		}
	}
	if wrote, err := c.PutMetaIfVersion(context.Background(), bucket, key, meta, 60, cache.VersionAny); err != nil || !wrote {
		tb.Fatalf("seed complete metadata: wrote=%t err=%v", wrote, err)
	}
	return meta
}

// A later transient must cancel any earlier payload read that is still waiting. Otherwise the
// certified path can wait for a request deadline and lose the upstream fallback the old probe gate
// reached immediately. Cover both the commitment slice and an earlier tail slice.
func TestBlockCache_CertifiedLaterTransientCancelsEarlierStalledRead(t *testing.T) {
	for _, stallSuffix := range []string{"|0", "|1"} {
		t.Run(strings.TrimPrefix(stallSuffix, "|"), func(t *testing.T) {
			mock := newBlockMock([]byte("ABCDEFGHIJKL"), `"v1"`)
			faulty := &certifiedRangeStallLaterTransientClient{
				CacheClient: cacheclient.NewMemoryCache(),
				stallSuffix: stallSuffix,
				failSuffix:  "|2",
				started:     make(chan struct{}),
				stopped:     make(chan struct{}),
			}
			cfg := config.NewDefault()
			cfg.Cache.SetBlockCachingEnabled(true)
			cfg.Cache.BlockSize = 4
			cfg.Cache.SizeThreshold = 1 << 20
			store := cache.NewCacheWithClient(faulty, &cfg.Cache)
			svc := NewService(mock, store, cfg)

			cold := httptest.NewRecorder()
			if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=0-11")); err != nil {
				t.Fatalf("cold range miss: %v", err)
			}
			if !metaCached(store, wowBucket, wowKey, 2*time.Second) {
				t.Fatal("range-created metadata was not published")
			}
			meta, found, err := store.GetMeta(context.Background(), wowBucket, wowKey)
			if err != nil || !found || meta == nil || meta.RangeBlockSpan == nil || meta.BlocksComplete {
				t.Fatalf("range metadata = (found=%t, meta=%#v, err=%v), want non-BlocksComplete certificate", found, meta, err)
			}
			if meta.RangeBlockSpan.First != 0 || meta.RangeBlockSpan.Last != 2 {
				t.Fatalf("range span = %#v, want [0,2]", meta.RangeBlockSpan)
			}

			originalForward := mock.mockForwarder.doRequestFunc
			var fallbackSawLiveContext atomic.Bool
			mock.mockForwarder.doRequestFunc = func(ctx context.Context, r *http.Request, accessKey, secretKey string) (*http.Response, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				fallbackSawLiveContext.Store(true)
				return originalForward(ctx, r, accessKey, secretKey)
			}
			faulty.armed.Store(true)
			beforeForwards := mock.forwards.Load()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			warm := httptest.NewRecorder()
			requestDone := make(chan error, 1)
			go func() {
				requestDone <- svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=0-11").WithContext(ctx))
			}()
			select {
			case <-faulty.started:
			case <-time.After(2 * time.Second):
				t.Fatal("certified payload read did not enter the stall")
			}
			select {
			case err := <-requestDone:
				if err != nil {
					t.Fatalf("fallback after later transient: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("later transient waited for the earlier read until the request deadline")
			}
			select {
			case <-faulty.stopped:
			default:
				t.Fatal("fallback completed before the stalled cache read was canceled and joined")
			}
			if ctx.Err() != nil || !fallbackSawLiveContext.Load() {
				t.Fatalf("upstream fallback did not retain a live request context: ctxErr=%v", ctx.Err())
			}
			if warm.Code != http.StatusPartialContent || warm.Body.String() != "ABCDEFGHIJKL" || warm.Header().Get("X-Cache") != XCacheMiss {
				t.Fatalf("fallback response = (%d, %q, cache=%q), want exact 206/MISS", warm.Code, warm.Body.String(), warm.Header().Get("X-Cache"))
			}
			if got := mock.forwards.Load() - beforeForwards; got != 1 {
				t.Fatalf("upstream fallbacks = %d, want 1", got)
			}
		})
	}
}

// A slow certified payload read has its own request-derived bound. It must fall back to the
// original Range while the client request still has time remaining, not wait for the client deadline.
func TestBlockCache_CertifiedRangeReadTimeoutFallsBackBeforeRequestDeadline(t *testing.T) {
	body := []byte("ABCDEFGH")
	mock := newBlockMock(body, `"v1"`)
	faulty := &certifiedRangeStallLaterTransientClient{
		CacheClient: cacheclient.NewMemoryCache(),
		stallSuffix: "|0",
		started:     make(chan struct{}),
		stopped:     make(chan struct{}),
	}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = int64(len(body))
	store := cache.NewCacheWithClient(faulty, &cfg.Cache)
	svc := NewService(mock, store, cfg)

	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=0-7")); err != nil {
		t.Fatalf("cold range miss: %v", err)
	}
	if !metaCached(store, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("range-created metadata was not published")
	}
	meta, found, err := store.GetMeta(context.Background(), wowBucket, wowKey)
	if err != nil || !found || meta == nil || meta.RangeBlockSpan == nil || meta.BlocksComplete {
		t.Fatalf("range metadata = (found=%t, meta=%#v, err=%v), want non-BlocksComplete certificate", found, meta, err)
	}
	initialRemaining := svc.populateBudget.remaining

	originalForward := mock.mockForwarder.doRequestFunc
	var fallbackSawLiveContext atomic.Bool
	mock.mockForwarder.doRequestFunc = func(ctx context.Context, r *http.Request, accessKey, secretKey string) (*http.Response, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fallbackSawLiveContext.Store(true)
		return originalForward(ctx, r, accessKey, secretKey)
	}
	faulty.armed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 2*certifiedRangeReadTimeout)
	defer cancel()
	warm := httptest.NewRecorder()
	requestDone := make(chan error, 1)
	go func() {
		requestDone <- svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=0-7").WithContext(ctx))
	}()
	select {
	case <-faulty.started:
	case <-time.After(2 * time.Second):
		t.Fatal("certified payload read did not reach the stalled owner")
	}
	select {
	case err := <-requestDone:
		if err != nil {
			t.Fatalf("bounded certified fallback: %v", err)
		}
	case <-time.After(certifiedRangeReadTimeout + time.Second):
		t.Fatal("certified range read exceeded its shorter recovery deadline")
	}
	select {
	case <-faulty.stopped:
	default:
		t.Fatal("fallback completed before the stalled cache read was canceled and joined")
	}
	if ctx.Err() != nil || !fallbackSawLiveContext.Load() {
		t.Fatalf("upstream fallback did not retain a live request context: ctxErr=%v", ctx.Err())
	}
	if warm.Code != http.StatusPartialContent || warm.Body.String() != string(body) || warm.Header().Get("X-Cache") != XCacheMiss {
		t.Fatalf("timed-out certified response = (%d,%q,cache=%q), want exact 206/MISS", warm.Code, warm.Body.String(), warm.Header().Get("X-Cache"))
	}
	if remaining, inUse := svc.populateBudget.remaining, svc.populateBudget.stagingInUse; remaining != initialRemaining || inUse != 0 {
		t.Fatalf("staging budget after timeout fallback = (remaining=%d,in_use=%d), want (%d,0)", remaining, inUse, initialRemaining)
	}
}

// A warm range that spans a block boundary is assembled byte-exact from both blocks with no
// upstream fetch (the probe-free assembled path).
func TestBlockCache_AssembledRangeSpansBlockBoundary(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`) // 10 bytes → blocks [0..3][4..7][8..9]
	svc, c := newBlockService(t, mock)

	// Establish blocks 0 and 1 via a cold miss.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-7")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated after cold miss")
	}

	// bytes=2-5 (len 4 = one block) covers blocks 0 and 1: assembled across the boundary.
	before := mock.blockGets.Load()
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, blockGet(wowBucket, wowKey, "bytes=2-5")); err != nil {
		t.Fatalf("warm boundary range: %v", err)
	}
	if w2.Code != http.StatusPartialContent || w2.Body.String() != "CDEF" {
		t.Fatalf("warm boundary range: code=%d body=%q, want 206 CDEF", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("X-Cache=%q, want %q", got, XCacheHit)
	}
	if after := mock.blockGets.Load(); after != before {
		t.Errorf("warm boundary range fetched blocks upstream: %d -> %d", before, after)
	}
	if cr := w2.Header().Get("Content-Range"); cr != "bytes 2-5/10" {
		t.Errorf("Content-Range=%q, want bytes 2-5/10", cr)
	}
}

// A transient fetch failure for a block discovered missing during assembly falls through to
// upstream BEFORE anything is committed: the client gets a clean 206 from the forward and the
// still-valid entry is not invalidated.
func TestBlockCache_AssembledRangeTransientFetchFallsThrough(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish only block 0.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	// Block 1 is missing and its fetch fails transiently → clean upstream fall-through.
	mock.blockGetTransient = true
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, blockGet(wowBucket, wowKey, "bytes=4-7")); err != nil {
		t.Fatalf("range with failing block fetch: %v", err)
	}
	if w2.Code != http.StatusPartialContent || w2.Body.String() != "EFGH" {
		t.Fatalf("fall-through: code=%d body=%q, want 206 EFGH", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got != XCacheMiss {
		t.Errorf("X-Cache=%q, want %q (upstream fall-through)", got, XCacheMiss)
	}
	// The entry survives: a transient failure never invalidates.
	if m, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found || m.BlockSize != 4 {
		t.Fatalf("entry lost after transient fetch failure: found=%v", found)
	}
}

// A transient CACHE read failure during assembly (e.g. a cluster gRPC blip) aborts with
// nothing committed — the client is served via the upstream fall-through and the still-valid
// entry is not invalidated. Pins that a read error is treated as transient, never as a
// missing block.
func TestBlockCache_AssemblyCacheReadFailureFallsThroughWithoutInvalidating(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	faulty := &blockReadFaultClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(faulty, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	// Establish a block-mode entry with block 0 present.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	// Arm block-read failures: the warm serve's assembly read fails → upstream fall-through.
	faulty.fail.Store(true)
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("range with failing cache reads: %v", err)
	}
	if w2.Code != http.StatusPartialContent || w2.Body.String() != "ABCD" {
		t.Fatalf("fall-through: code=%d body=%q, want 206 ABCD", w2.Code, w2.Body.String())
	}
	faulty.fail.Store(false)
	if m, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found || m.BlockSize != 4 {
		t.Fatalf("entry lost after transient cache read failure: found=%v", found)
	}
}

// A certified multi-block read whose first block cannot be read keeps the response uncommitted,
// so a transient failure falls through as an upstream MISS rather than being relabeled a HIT.
func TestBlockCache_CertifiedRangeCacheReadFailureFallsThroughWithoutInvalidating(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJKLMNOP"), `"v1"`)
	faulty := &blockReadFaultClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(faulty, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("cold range miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("range-created metadata not populated")
	}
	if meta, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found || meta.RangeBlockSpan == nil {
		t.Fatalf("range certificate missing: found=%v meta=%#v", found, meta)
	}

	faulty.fail.Store(true)
	warm := httptest.NewRecorder()
	if err := svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("certified range with transient cache reads: %v", err)
	}
	if warm.Code != http.StatusPartialContent || warm.Body.String() != "ABCDEFGHIJKLMNOP" {
		t.Fatalf("certified fallback = (%d, %q), want upstream 206 body", warm.Code, warm.Body.String())
	}
	if got := warm.Header().Get("X-Cache"); got != XCacheMiss {
		t.Fatalf("certified fallback X-Cache = %q, want %q", got, XCacheMiss)
	}
	faulty.fail.Store(false)
	if meta, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found || meta.BlockSize != 4 || meta.RangeBlockSpan == nil {
		t.Fatalf("entry after certified transient failure = (found=%v meta=%#v), want unchanged certificate", found, meta)
	}
}

// A later transient payload failure returns to the probe/fetch gate and ordered stream, preserving
// the existing inline recovery, validated remainder, and complete HIT behavior.
func TestBlockCache_CertifiedRangeLaterTransientRecoversRemainder(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJKLMNOP"), `"v1"`)
	faulty := &blockReadSelectiveFaultClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(faulty, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("cold range miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("range-created metadata not populated")
	}

	faulty.fail.Store(true)
	beforeBlockGets := mock.blockGets.Load()
	warm := httptest.NewRecorder()
	if err := svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("certified later transient: %v", err)
	}
	if warm.Code != http.StatusPartialContent || warm.Body.String() != "ABCDEFGHIJKLMNOP" {
		t.Fatalf("later transient response = (%d, %q), want cache-hit 206 body", warm.Code, warm.Body.String())
	}
	if got := warm.Header().Get("X-Cache"); got != XCacheHit {
		t.Fatalf("later transient X-Cache = %q, want %q", got, XCacheHit)
	}
	if got := mock.blockGets.Load() - beforeBlockGets; got != 1 {
		t.Fatalf("later transient remainder fetches = %d, want 1", got)
	}
}

// A later payload transient must rerun the probe/fetch gate before commitment so an earlier
// eviction plus a tombstone still invalidates the stale span.
func TestBlockCache_CertifiedLaterTransientStillFindsTombstone(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJKLMNOP"), `"v1"`)
	faulty := &blockReadSelectiveFaultClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(faulty, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("cold range miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("range-created metadata was not populated")
	}
	meta, found, err := c.GetMeta(context.Background(), wowBucket, wowKey)
	if err != nil || !found || meta == nil || meta.RangeBlockSpan == nil {
		t.Fatalf("range metadata = (found=%t, meta=%#v, err=%v), want certificate", found, meta, err)
	}
	if err := faulty.CacheClient.Delete(context.Background(), cache.MakeBlockKey(wowBucket, wowKey, meta.ETag, meta.BlockSize, 0)); err != nil {
		t.Fatalf("evict first block: %v", err)
	}
	faulty.fail.Store(true) // block 2 payload fails; byte-zero presence probes still succeed
	mock.blockGet404 = true
	beforeForwards := mock.forwards.Load()

	warm := httptest.NewRecorder()
	if err := svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("later transient with tombstone: %v", err)
	}
	if warm.Code != http.StatusNotFound || warm.Body.Len() != 0 || warm.Header().Get("X-Cache") != XCacheMiss {
		t.Fatalf("later transient tombstone = (%d, %q, cache=%q), want empty 404/MISS", warm.Code, warm.Body.String(), warm.Header().Get("X-Cache"))
	}
	if got := mock.forwards.Load() - beforeForwards; got != 1 {
		t.Fatalf("tombstone range forwards = %d, want 1", got)
	}
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); found {
		t.Fatal("stale range metadata survived transient plus tombstone")
	}
}

// A persistent transient on a later block read must be surfaced by the restored probe gate. The
// handler then forwards while the request context is live instead of starting a suffix request.
func TestBlockCache_CertifiedLaterProbeTransientFallsBackBeforeRemainder(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJKLMNOP"), `"v1"`)
	faulty := &blockReadBlockOneFaultClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(faulty, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, blockGet(wowBucket, wowKey, "bytes=0-15")); err != nil {
		t.Fatalf("cold range miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("range-created metadata was not populated")
	}
	faulty.fail.Store(true)
	mock.blockGetTransient = true
	beforeForwards := mock.forwards.Load()
	beforeBlockGets := mock.blockGets.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	warm := httptest.NewRecorder()
	if err := svc.HandleGetObject(warm, blockGet(wowBucket, wowKey, "bytes=0-15").WithContext(ctx)); err != nil {
		t.Fatalf("later probe failure fallback: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("upstream fallback waited for the request deadline: %v", ctx.Err())
	}
	if warm.Code != http.StatusPartialContent || warm.Body.String() != "ABCDEFGHIJKLMNOP" || warm.Header().Get("X-Cache") != XCacheMiss {
		t.Fatalf("later probe failure = (%d, %q, cache=%q), want full 206/MISS", warm.Code, warm.Body.String(), warm.Header().Get("X-Cache"))
	}
	if got := mock.forwards.Load() - beforeForwards; got != 1 {
		t.Fatalf("later probe failure forwards = %d, want 1", got)
	}
	if got := mock.blockGets.Load() - beforeBlockGets; got != 0 {
		t.Fatalf("later probe failure suffix requests = %d, want 0", got)
	}
}

// The probe-first path (full-object serves, oversized ranges) must surface a transient probe
// failure as an error from ensureBlocksCached — never as "blocks missing". If failed probes
// were counted as absent, the full-object caller's mostly-missing bail would fire and DELETE
// a still-valid entry on a network blip.
func TestBlockCache_ProbePathTransientFailureAbortsWithoutInvalidating(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	faulty := &blockReadFaultClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(faulty, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	// Establish a fully-populated block-mode entry (all three blocks + meta).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-9")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated after cold miss")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)

	faulty.fail.Store(true)
	err := svc.ensureBlocksCached(context.Background(), wowBucket, wowKey, "ak", "sk", meta, 0, 2, true, 0)
	if err == nil {
		t.Fatal("ensureBlocksCached returned nil despite probe failures")
	}
	if errors.Is(err, errBlockAssemblyWouldAmplify) {
		t.Fatal("transient probe failure misreported as amplify-bail (would delete a valid entry)")
	}
	if !strings.Contains(err.Error(), "simulated block read blip") {
		t.Fatalf("ensureBlocksCached error = %v, want the injected read error", err)
	}
	if m, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found || m.BlockSize != 4 {
		t.Fatalf("entry lost after transient probe failure: found=%v", found)
	}
}

// A warm full GET over many blocks still serves byte-exact from cache via the probe-first
// path (full-object serves are unchanged by the assembled-range path).
func TestBlockCache_WarmFullGetManyBlocksServesFromCache(t *testing.T) {
	object := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$") // 40 bytes
	mock := newBlockMock(object, `"v1"`)
	svc, c := newBlockServiceWithBudget(t, mock, 2, 1<<20) // 2-byte blocks → 20 blocks

	// Cold full GET: the miss path streams from upstream and block-splits into cache.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("cold full GET: %v", err)
	}
	if w.Code != http.StatusOK || w.Body.String() != string(object) {
		t.Fatalf("cold full GET: code=%d body=%q", w.Code, w.Body.String())
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated after cold full GET")
	}

	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("warm full GET: %v", err)
	}
	if w2.Code != http.StatusOK || w2.Body.String() != string(object) {
		t.Fatalf("warm full GET: code=%d body=%q, want cache-assembled object", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("X-Cache=%q, want %q", got, XCacheHit)
	}
	if got := mock.blockGets.Load(); got != 0 {
		t.Errorf("warm serve fetched %d blocks from upstream, want 0", got)
	}
}

// With the populate budget saturated by other work, an assembled range whose missing-block
// fetch declines falls through to a clean upstream forward — no truncation, no invalidation —
// and serves from cache again once the budget frees. (The staging and populate budgets are
// independent pools, so a decline means real global populate pressure; retrying via the probe
// path would draw on the same budget and just repeat it.)
func TestBlockCache_AssemblyFetchDeclineFallsThroughToUpstream(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockServiceWithBudget(t, mock, 4, 100)

	// Establish only block 0.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-3")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	// Saturate the populate budget so block 1's fetch declines.
	if !svc.populateBudget.tryAcquireReadMiss(100) {
		t.Fatal("could not saturate populate budget")
	}

	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, blockGet(wowBucket, wowKey, "bytes=4-7")); err != nil {
		t.Fatalf("range under populate saturation: %v", err)
	}
	if w2.Code != http.StatusPartialContent || w2.Body.String() != "EFGH" {
		t.Fatalf("range under saturation: code=%d body=%q, want 206 EFGH (upstream forward)", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got == XCacheHit {
		t.Errorf("X-Cache=%q, want a non-HIT upstream forward under populate saturation", got)
	}
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found {
		t.Fatal("still-valid entry was invalidated by a budget decline")
	}

	// Budget freed: the fetch fits again and the range serves from cache.
	svc.populateBudget.release(100)
	w3 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w3, blockGet(wowBucket, wowKey, "bytes=4-7")); err != nil {
		t.Fatalf("range after budget freed: %v", err)
	}
	if w3.Code != http.StatusPartialContent || w3.Body.String() != "EFGH" || w3.Header().Get("X-Cache") != XCacheHit {
		t.Fatalf("after budget freed: code=%d body=%q x-cache=%q, want 206 EFGH HIT", w3.Code, w3.Body.String(), w3.Header().Get("X-Cache"))
	}
}

// A full-stream split marks the entry BlocksComplete; a block evicted afterwards is recovered
// by an inline fetch mid-serve — the client still gets the complete, byte-exact object.
func TestBlockCache_CompleteEntryInlineFetchRecoversEvictedBlock(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`) // blocks [0..3][4..7][8..9]
	memCache := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(memCache, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	// Cold full GET → block split → complete entry.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("cold full GET: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("meta not populated")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if !meta.BlocksComplete {
		t.Fatal("entry from full-stream split not marked BlocksComplete")
	}

	// Evict block 1 out from under the complete entry.
	if err := memCache.Delete(context.Background(), cache.MakeBlockKey(wowBucket, wowKey, `"v1"`, 4, 1)); err != nil {
		t.Fatalf("delete block 1: %v", err)
	}
	before := mock.blockGets.Load()
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("warm full GET with evicted block: %v", err)
	}
	if w2.Code != http.StatusOK || w2.Body.String() != "ABCDEFGHIJ" {
		t.Fatalf("inline-fetch serve: code=%d body=%q, want 200 ABCDEFGHIJ", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("X-Cache=%q, want %q", got, XCacheHit)
	}
	if after := mock.blockGets.Load(); after != before+1 {
		t.Errorf("inline fetches = %d, want exactly 1 (the evicted block)", after-before)
	}
}

// A complete entry whose FIRST block is gone and unrecoverable (upstream failing) must fall
// through pre-commit: the client gets a clean 200 from the miss path, never a truncated body.
func TestBlockCache_CompleteEntryFirstBlockGoneFallsThroughCleanly(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	memCache := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(memCache, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("cold full GET: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("meta not populated")
	}

	if err := memCache.Delete(context.Background(), cache.MakeBlockKey(wowBucket, wowKey, `"v1"`, 4, 0)); err != nil {
		t.Fatalf("delete block 0: %v", err)
	}
	mock.blockGetTransient = true // the pre-commit recovery fetch fails
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("full GET with gone first block: %v", err)
	}
	// Served whole from the miss-path forward — a complete body, not a truncated one.
	if w2.Code != http.StatusOK || w2.Body.String() != "ABCDEFGHIJ" {
		t.Fatalf("fall-through: code=%d body=%q, want 200 ABCDEFGHIJ", w2.Code, w2.Body.String())
	}
}

// An entry established from a range (not complete) is promoted to BlocksComplete after its
// first successful full assembly, so later full GETs skip the probe pass.
func TestBlockCache_PartialEntryPromotedAfterFullAssembly(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, c := newBlockService(t, mock)

	// Establish blocks 0 and 1 via a cold range miss (block 2 missing → not complete).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-7")); err != nil {
		t.Fatalf("cold range miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("meta not populated")
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if meta.BlocksComplete {
		t.Fatal("range-established entry unexpectedly marked complete")
	}

	// Full GET assembles (fetching block 2) and serves; the promotion lands asynchronously.
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("full GET: %v", err)
	}
	if w2.Code != http.StatusOK || w2.Body.String() != "ABCDEFGHIJ" {
		t.Fatalf("full GET: code=%d body=%q", w2.Code, w2.Body.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		m, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
		if found && m.BlocksComplete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("entry not promoted to BlocksComplete after successful full assembly")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A definitive stale signal (upstream 404) surfacing from a mid-serve inline fetch must
// invalidate the BlocksComplete entry even though headers are already committed — otherwise
// every later full GET commits and truncates the same way until the meta TTL expires.
func TestBlockCache_CompleteEntryMidServeStaleSignalInvalidates(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	memCache := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(memCache, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	// Complete entry via a cold full GET.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("cold full GET: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("meta not populated")
	}

	// Evict block 1, then delete the object out of band: the mid-serve inline fetch 404s.
	if err := memCache.Delete(context.Background(), cache.MakeBlockKey(wowBucket, wowKey, `"v1"`, 4, 1)); err != nil {
		t.Fatalf("delete block 1: %v", err)
	}
	mock.blockGet404 = true

	w2 := httptest.NewRecorder()
	err := svc.HandleGetObject(w2, fullGet(wowBucket, wowKey))
	if err == nil {
		t.Fatal("mid-serve stale fetch: want a (truncation) error, got nil")
	}
	// The stale entry must be gone so the next GET re-resolves upstream instead of
	// repeating the truncation.
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); found {
		t.Fatal("stale BlocksComplete entry survived a definitive mid-serve stale signal")
	}
}

// A populate-budget decline for a block AFTER headers are committed must not truncate the
// response: the serve salvages the remaining bytes with one uncached upstream range GET and
// the still-valid entry survives (a transient decline is not a degenerate entry).
func TestBlockCache_CompleteEntryFetchDeclineSalvagesViaRemainder(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	memCache := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	cfg.Cache.MaxPopulateMemoryBytes = 100
	c := cache.NewCacheWithClient(memCache, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	// Complete entry via a cold full GET.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("cold full GET: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("meta not populated")
	}

	// Evict a MID-stream block and saturate the populate budget: the inline recovery fetch
	// declines only after the 200 is committed.
	if err := memCache.Delete(context.Background(), cache.MakeBlockKey(wowBucket, wowKey, `"v1"`, 4, 1)); err != nil {
		t.Fatalf("delete block 1: %v", err)
	}
	if !svc.populateBudget.tryAcquireReadMiss(100) {
		t.Fatal("could not saturate populate budget")
	}
	defer svc.populateBudget.release(100)

	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("full GET under populate saturation: %v", err)
	}
	if w2.Code != http.StatusOK || w2.Body.String() != "ABCDEFGHIJ" {
		t.Fatalf("degraded serve: code=%d body=%q, want the COMPLETE object (no truncation)", w2.Code, w2.Body.String())
	}
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); !found {
		t.Fatal("entry invalidated by a transient budget decline")
	}
}

// A BlocksComplete meta surviving mass block eviction must not turn one full GET into N
// serial upstream fetches: after maxInlineFetchesPerServe recoveries the serve switches to a
// single uncached upstream remainder stream, and the degenerate entry is invalidated so the
// next GET re-establishes it with one streaming re-split.
func TestBlockCache_CompleteEntryMassEvictionBoundsUpstreamFanout(t *testing.T) {
	object := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$") // 40 bytes -> 20 2-byte blocks
	mock := newBlockMock(object, `"v1"`)
	memCache := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 2
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(memCache, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("cold full GET: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("meta not populated")
	}

	// Mass-evict blocks 3..19 out from under the complete meta.
	for i := int64(3); i <= 19; i++ {
		if err := memCache.Delete(context.Background(), cache.MakeBlockKey(wowBucket, wowKey, `"v1"`, 2, i)); err != nil {
			t.Fatalf("delete block %d: %v", i, err)
		}
	}

	before := mock.blockGets.Load()
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("degraded full GET: %v", err)
	}
	if w2.Code != http.StatusOK || w2.Body.String() != string(object) {
		t.Fatalf("degraded serve: code=%d body=%q, want the exact object", w2.Code, w2.Body.String())
	}
	// Bounded fan-out: at most the inline-fetch cap in aligned GETs plus ONE remainder GET —
	// never one upstream round trip per missing block (17 here).
	if got := mock.blockGets.Load() - before; int(got) > maxInlineFetchesPerServe+1 {
		t.Errorf("degraded serve made %d upstream requests, want <= %d", got, maxInlineFetchesPerServe+1)
	}
	// The degenerate entry is invalidated so the next GET re-splits in one streaming fetch.
	if _, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); found {
		t.Fatal("degenerate mass-evicted entry survived the degraded serve")
	}
}

// Promotion must never extend an entry's lifetime: the rewritten meta carries only the
// REMAINING TTL computed from CachedAt, and entries with unknown age (or already expired)
// are not rewritten at all.
func TestRemainingMetaTTL(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	svc, _ := newBlockService(t, mock)
	svc.config.Cache.TTL = time.Hour
	now := time.Now().Unix()
	cases := []struct {
		name     string
		cachedAt int64
		min, max int
	}{
		{"fresh entry keeps ~full TTL", now, 3590, 3600},
		{"aged entry keeps only the remainder", now - 3500, 90, 110},
		{"expired entry is not rewritten", now - 4000, 0, 0},
		{"unknown age is not rewritten", 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := svc.remainingMetaTTL(&cache.CachedObjectMeta{CachedAt: tc.cachedAt})
			if got < tc.min || got > tc.max {
				t.Errorf("remainingMetaTTL(age=%ds) = %d, want in [%d,%d]", now-tc.cachedAt, got, tc.min, tc.max)
			}
		})
	}
}

// A warm multi-block serve whose pipeline buffer reservation the budget declines degrades to
// the direct sequential stream and still serves byte-exact from cache — budget pressure slows
// a warm serve down, it never surrenders it to upstream. Budget of 5: the complete path's
// first-block reservation (2) leaves 3, so the smallest two-buffer pipeline reservation (4)
// declines.
func TestBlockCache_PipelineBudgetDeclineDegradesToSequential(t *testing.T) {
	object := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$") // 40 bytes → 20 2-byte blocks
	mock := newBlockMock(object, `"v1"`)
	svc, c := newBlockServiceWithBudget(t, mock, 2, 5)

	// Cold full GET: the populate reserves min(40, budget)=5 and releases before the meta lands.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("cold full GET: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated after cold full GET")
	}

	before := mock.blockGets.Load()
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("warm full GET under budget pressure: %v", err)
	}
	if w2.Code != http.StatusOK || w2.Body.String() != string(object) {
		t.Fatalf("warm full GET: code=%d body=%q, want the exact object", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("X-Cache=%q, want %q", got, XCacheHit)
	}
	if after := mock.blockGets.Load(); after != before {
		t.Errorf("warm sequential serve fetched %d blocks from upstream, want 0", after-before)
	}
}

// A warm range wider than one block takes the probe-first path and streams its covering blocks
// through the pipeline; a block the probe found missing is fetched first, and the assembled 206
// is byte-exact across all block boundaries.
func TestBlockCache_WarmMultiBlockRangeServesPipelined(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`) // blocks [0..3][4..7][8..9]
	svc, c := newBlockService(t, mock)

	// Cold miss on bytes=0-7 populates blocks 0 and 1 (and the meta) in the background.
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-7")); err != nil {
		t.Fatalf("cold miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated")
	}

	// bytes=0-9 (10 bytes > BlockSize 4) skips the assembled-range path: probe finds block 2
	// missing, fetches it, then the three blocks stream pipelined.
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, blockGet(wowBucket, wowKey, "bytes=0-9")); err != nil {
		t.Fatalf("warm multi-block range: %v", err)
	}
	if w2.Code != http.StatusPartialContent || w2.Body.String() != "ABCDEFGHIJ" {
		t.Fatalf("multi-block range: code=%d body=%q, want 206 ABCDEFGHIJ", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("X-Cache=%q, want %q", got, XCacheHit)
	}
	if !c.BlockExists(context.Background(), wowBucket, wowKey, `"v1"`, 4, 2) {
		t.Error("block 2 not cached after the probe-path fetch")
	}
}

// A BlocksComplete entry serves an unaligned multi-block range with one payload read per
// covering block. The first requested slice is staged before the 206, while the remainder uses
// the ordered streamer without repeating byte-zero presence probes.
func TestBlockCache_CompleteEntryWarmMultiBlockRangeReadsEachBlockOnce(t *testing.T) {
	const (
		bucket = "complete-range-bucket"
		key    = "complete-range"
		etag   = `"v1"`
	)
	object := []byte("abcdefghijkl") // three 4-byte blocks
	base := cacheclient.NewMemoryCache()
	client := &countingBlockReadClient{CacheClient: base}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(client, &cfg.Cache)
	svc := NewService(newBlockMock(object, etag), c, cfg)
	seedCompleteBlockEntry(t, c, base, bucket, key, etag, object, 4)
	client.reads.Store(0)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(bucket, key, "bytes=1-10")); err != nil {
		t.Fatalf("complete multi-block range: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "bcdefghijk" {
		t.Fatalf("complete multi-block range: code=%d body=%q, want 206 bcdefghijk", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Range"); got != "bytes 1-10/12" {
		t.Errorf("Content-Range=%q, want bytes 1-10/12", got)
	}
	if got := w.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("X-Cache=%q, want %q", got, XCacheHit)
	}
	if got := client.reads.Load(); got != 3 {
		t.Errorf("warm complete range block reads = %d, want exactly 3 payload reads (no probes)", got)
	}
}

// The first requested block may be a later, partial block. It is recovered before headers are
// committed, so an eviction does not turn the range response into a truncated prefix.
func TestBlockCache_CompleteRangeRecoversEvictedFirstSlice(t *testing.T) {
	const (
		bucket = "first-slice-bucket"
		key    = "first-slice"
		etag   = `"v1"`
	)
	object := []byte("ABCDEFGHIJ")
	mock := newBlockMock(object, etag)
	base := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(base, &cfg.Cache)
	svc := NewService(mock, c, cfg)
	seedCompleteBlockEntry(t, c, base, bucket, key, etag, object, 4)
	if err := base.Delete(context.Background(), cache.MakeBlockKey(bucket, key, etag, 4, 1)); err != nil {
		t.Fatalf("evict first requested block: %v", err)
	}

	before := mock.blockGets.Load()
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(bucket, key, "bytes=5-9")); err != nil {
		t.Fatalf("range with evicted first slice: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "FGHIJ" {
		t.Fatalf("range with evicted first slice: code=%d body=%q, want 206 FGHIJ", w.Code, w.Body.String())
	}
	if got := w.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("X-Cache=%q, want %q", got, XCacheHit)
	}
	if got := mock.blockGets.Load() - before; got != 1 {
		t.Errorf("first-slice recovery upstream fetches = %d, want 1", got)
	}
}

// If the staged first slice cannot be recovered, no range headers or prefix are committed; the
// request uses the ordinary upstream range fallback instead of returning a partial cache body.
func TestBlockCache_CompleteRangeFirstSliceFailureFallsThrough(t *testing.T) {
	const (
		bucket = "first-failure-bucket"
		key    = "first-failure"
		etag   = `"v1"`
	)
	object := []byte("ABCDEFGHIJ")
	mock := newBlockMock(object, etag)
	base := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(base, &cfg.Cache)
	svc := NewService(mock, c, cfg)
	seedCompleteBlockEntry(t, c, base, bucket, key, etag, object, 4)
	if err := base.Delete(context.Background(), cache.MakeBlockKey(bucket, key, etag, 4, 1)); err != nil {
		t.Fatalf("evict first requested block: %v", err)
	}
	mock.blockGetTransient = true

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(bucket, key, "bytes=5-9")); err != nil {
		t.Fatalf("range with failed first slice: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "FGHIJ" {
		t.Fatalf("first-slice fallback: code=%d body=%q, want 206 FGHIJ", w.Code, w.Body.String())
	}
	if got := w.Header().Get("X-Cache"); got != XCacheMiss {
		t.Errorf("X-Cache=%q, want %q after clean upstream fallback", got, XCacheMiss)
	}
}

// A budget decline after the first block has been staged is salvaged from one uncached remainder
// range. The range remains byte-exact and the still-valid complete metadata is retained.
func TestBlockCache_CompleteRangeMidStreamDeclineSalvagesRemainder(t *testing.T) {
	const (
		bucket = "midstream-budget-bucket"
		key    = "midstream-budget"
		etag   = `"v1"`
	)
	object := []byte("ABCDEFGHIJ")
	mock := newBlockMock(object, etag)
	base := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	cfg.Cache.MaxPopulateMemoryBytes = 6 // first 3-byte slice fits; a 4-byte recovery does not
	c := cache.NewCacheWithClient(base, &cfg.Cache)
	svc := NewService(mock, c, cfg)
	seedCompleteBlockEntry(t, c, base, bucket, key, etag, object, 4)
	if err := base.Delete(context.Background(), cache.MakeBlockKey(bucket, key, etag, 4, 2)); err != nil {
		t.Fatalf("evict mid-stream block: %v", err)
	}

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(bucket, key, "bytes=1-9")); err != nil {
		t.Fatalf("range with mid-stream budget decline: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "BCDEFGHIJ" {
		t.Fatalf("mid-stream salvage: code=%d body=%q, want 206 BCDEFGHIJ", w.Code, w.Body.String())
	}
	if got, found, _ := c.GetMeta(context.Background(), bucket, key); !found || got.ETag != etag {
		t.Fatalf("complete metadata after transient decline: found=%t meta=%+v", found, got)
	}
}

// A changed ETag discovered after the 206 is committed must not be salvaged with a different
// version. The committed prefix remains, but the stale metadata is invalidated and no v2 bytes
// enter the response.
func TestBlockCache_CompleteRangeMidStreamETagMismatchDoesNotMixVersions(t *testing.T) {
	const (
		bucket = "midstream-etag-bucket"
		key    = "midstream-etag"
		etag   = `"v1"`
	)
	object := []byte("ABCDEFGHIJ")
	mock := newBlockMock(object, etag)
	base := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(base, &cfg.Cache)
	svc := NewService(mock, c, cfg)
	seedCompleteBlockEntry(t, c, base, bucket, key, etag, object, 4)
	if err := base.Delete(context.Background(), cache.MakeBlockKey(bucket, key, etag, 4, 2)); err != nil {
		t.Fatalf("evict mid-stream block: %v", err)
	}
	mock.blockGetETag = `"v2"`

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(bucket, key, "bytes=1-9")); err == nil {
		t.Fatal("mid-stream ETag mismatch: want an error, got nil")
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "BCDEFGH" {
		t.Fatalf("mid-stream ETag mismatch: code=%d body=%q, want committed v1 prefix", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "I") || strings.Contains(w.Body.String(), "J") {
		t.Fatalf("mid-stream ETag mismatch mixed a new-version suffix: body=%q", w.Body.String())
	}
	if _, found, _ := c.GetMeta(context.Background(), bucket, key); found {
		t.Fatal("stale complete metadata survived a mid-stream ETag mismatch")
	}
}

// A complete-range first-slice staging decline deliberately keeps the old conservative path:
// every covering block is probed before the 206, then streamed. This is a safety fallback, not
// the probe-free optimization.
func TestBlockCache_CompleteRangeStagingDeclineUsesProbePath(t *testing.T) {
	const (
		bucket = "range-budget-bucket"
		key    = "range-budget"
		etag   = `"v1"`
	)
	object := []byte("ABCDEFGH")
	base := cacheclient.NewMemoryCache()
	client := &countingBlockReadClient{CacheClient: base}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	cfg.Cache.MaxPopulateMemoryBytes = 6 // staging cap is 3, less than the 4-byte first slice
	c := cache.NewCacheWithClient(client, &cfg.Cache)
	svc := NewService(&mockForwarder{}, c, cfg)
	seedCompleteBlockEntry(t, c, base, bucket, key, etag, object, 4)
	client.reads.Store(0)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(bucket, key, "bytes=0-7")); err != nil {
		t.Fatalf("complete range under staging decline: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != string(object) {
		t.Fatalf("staging-decline range: code=%d body=%q, want 206 %q", w.Code, w.Body.String(), object)
	}
	if got := client.reads.Load(); got != 4 {
		t.Errorf("staging-decline block reads = %d, want two probes plus two payload reads", got)
	}
}

// failAfterWriter fails every Write once `remaining` bytes have been accepted, simulating a
// client that disconnects mid-body.
type failAfterWriter struct {
	http.ResponseWriter
	remaining int
}

func (f *failAfterWriter) Write(p []byte) (int, error) {
	if f.remaining <= 0 {
		return 0, errors.New("client gone")
	}
	if len(p) > f.remaining {
		p = p[:f.remaining]
	}
	n, err := f.ResponseWriter.Write(p)
	f.remaining -= n
	if err != nil {
		return n, err
	}
	if f.remaining == 0 {
		return n, errors.New("client gone")
	}
	return n, nil
}

// A client write failure mid-pipeline must cancel outstanding cache-read workers promptly and
// return their buffers instead of leaving them blocked. The serve returns without hanging; -race
// would flag an unsynchronized unwind.
func TestBlockCache_PipelineClientWriteFailureUnwindsCleanly(t *testing.T) {
	object := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$") // 40 bytes → 20 2-byte blocks
	mock := newBlockMock(object, `"v1"`)
	svc, c := newBlockServiceWithBudget(t, mock, 2, 1<<20)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("cold full GET: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("block-mode meta not populated after cold full GET")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		fw := &failAfterWriter{ResponseWriter: httptest.NewRecorder(), remaining: 9}
		_ = svc.HandleGetObject(fw, fullGet(wowBucket, wowKey))
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after a mid-body client write failure (prefetch workers stuck?)")
	}
}

// A client write failure on the SEQUENTIAL stream path (where cache bytes write straight
// into the client connection) must not trigger the upstream remainder salvage: the client is
// broken, not the cache, so retrying from upstream would waste a round trip on a dead
// connection. Pins the clientWriteTracker classification.
func TestBlockCache_SequentialClientWriteFailureDoesNotFetchRemainder(t *testing.T) {
	object := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$") // 40 bytes → 20 2-byte blocks
	mock := newBlockMock(object, `"v1"`)
	// Budget 5: the complete path's first-block reservation (2) leaves 3, so the smallest
	// two-buffer pipeline reservation (4) declines and the stream takes the sequential path.
	svc, c := newBlockServiceWithBudget(t, mock, 2, 5)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("cold full GET: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("meta not populated")
	}

	before := mock.blockGets.Load()
	fw := &failAfterWriter{ResponseWriter: httptest.NewRecorder(), remaining: 9}
	_ = svc.HandleGetObject(fw, fullGet(wowBucket, wowKey))
	if got := mock.blockGets.Load() - before; got != 0 {
		t.Errorf("client write failure triggered %d upstream requests, want 0 (no remainder salvage)", got)
	}
}

// clientForwards reports how many client-range forwards have hit upstream.
func (m *blockMockForwarder) clientForwards() int32 { return m.forwards.Load() }

// blockPutHookClient fires hook once, synchronously, on the first PutStream of hookKey —
// letting a test interleave an invalidation with a block assembly in progress.
type blockPutHookClient struct {
	cacheclient.CacheClient
	hookKey string
	fired   atomic.Bool
	hook    func()
}

func (h *blockPutHookClient) PutStream(ctx context.Context, key string, r io.Reader, ttl int64) error {
	if key == h.hookKey && h.fired.CompareAndSwap(false, true) {
		h.hook()
	}
	return h.CacheClient.PutStream(ctx, key, r, ttl)
}

type blockMetaTokenErrorClient struct {
	cacheclient.CacheClient
	metaKey string
	failed  atomic.Bool
}

func (c *blockMetaTokenErrorClient) failMetaTokenRead(key string) bool {
	return key == c.metaKey && c.failed.CompareAndSwap(false, true)
}

func (c *blockMetaTokenErrorClient) Get(ctx context.Context, key string) ([]byte, error) {
	if c.failMetaTokenRead(key) {
		return nil, errors.New("simulated metadata token read failure")
	}
	return c.CacheClient.Get(ctx, key)
}

func (c *blockMetaTokenErrorClient) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	if c.failMetaTokenRead(key) {
		return nil, 0, false, errors.New("simulated metadata token read failure")
	}
	return c.CacheClient.GetWithVersion(ctx, key)
}

// A failed decision-time token read must stop range population before fetching blocks or
// publishing a range certificate, under both supported metadata coordinators.
func TestBlockCache_RangePopulateSkipsWhenTokenReadFails(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
	}{
		{name: "legacy", legacy: true},
		{name: "cas", legacy: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := newBlockMock([]byte("ABCDEFGHIJKLMNOP"), `"v1"`)
			cfg := config.NewDefault()
			cfg.Cache.SetBlockCachingEnabled(true)
			cfg.Cache.SetLegacyCoordination(tc.legacy)
			cfg.Cache.BlockSize = 4
			cfg.Cache.SizeThreshold = 1 << 20
			client := &blockMetaTokenErrorClient{
				CacheClient: cacheclient.NewMemoryCache(),
				metaKey:     cache.MakeMetaKey(wowBucket, wowKey),
			}
			c := cache.NewCacheWithClient(client, &cfg.Cache)
			svc := NewService(mock, c, cfg)
			meta := &cache.CachedObjectMeta{
				Bucket:        wowBucket,
				Key:           wowKey,
				ETag:          `"v1"`,
				ContentLength: 16,
				StatusCode:    http.StatusOK,
				BlockSize:     4,
			}

			svc.populateBlockMode(context.Background(), wowBucket, wowKey, "ak", "sk", meta, []int64{0, 1})
			if !client.failed.Load() {
				t.Fatal("metadata token read was not attempted")
			}
			if got := mock.blockGets.Load(); got != 0 {
				t.Fatalf("block fetches after token read failure = %d, want 0", got)
			}
			if _, found, err := c.GetMeta(context.Background(), wowBucket, wowKey); err != nil || found {
				t.Fatalf("metadata after token read failure = (found=%t, err=%v), want absent", found, err)
			}
		})
	}
}

// An invalidation after the decision-time token but during the block fetch must reject the
// metadata commit, so no range certificate can resurrect a deleted entry.
func TestBlockCache_RangePopulateFenceRejectsMidFetchInvalidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
	}{
		{name: "legacy", legacy: true},
		{name: "cas", legacy: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := newBlockMock([]byte("ABCDEFGHIJKLMNOP"), `"v1"`)
			cfg := config.NewDefault()
			cfg.Cache.SetBlockCachingEnabled(true)
			cfg.Cache.SetLegacyCoordination(tc.legacy)
			cfg.Cache.BlockSize = 4
			cfg.Cache.SizeThreshold = 1 << 20
			hooked := &blockPutHookClient{
				CacheClient: cacheclient.NewMemoryCache(),
				hookKey:     cache.MakeBlockKey(wowBucket, wowKey, `"v1"`, 4, 0),
			}
			c := cache.NewCacheWithClient(hooked, &cfg.Cache)
			svc := NewService(mock, c, cfg)
			hooked.hook = func() {
				if err := c.DeleteWithMeta(context.Background(), wowBucket, wowKey); err != nil {
					t.Errorf("mid-fetch invalidation: %v", err)
				}
			}
			meta := &cache.CachedObjectMeta{
				Bucket:        wowBucket,
				Key:           wowKey,
				ETag:          `"v1"`,
				ContentLength: 16,
				StatusCode:    http.StatusOK,
				BlockSize:     4,
			}
			svc.populateBlockMode(context.Background(), wowBucket, wowKey, "ak", "sk", meta, []int64{0, 1})
			if !hooked.fired.Load() {
				t.Fatal("invalidation hook did not run during block fetch")
			}
			if got := mock.blockGets.Load(); got != 2 {
				t.Fatalf("blocks fetched before range metadata commit = %d, want 2", got)
			}
			if _, found, err := c.GetMeta(context.Background(), wowBucket, wowKey); err != nil || found {
				t.Fatalf("metadata after mid-fetch invalidation = (found=%t, err=%v), want absent", found, err)
			}
		})
	}
}

// An invalidation landing DURING the full-GET assembly must refuse the
// blocks-complete promotion: the promotion's token is read before assembly, so
// the mid-assembly delete + same-ETag re-establishment (whose blocks the
// promotion knows nothing about) stays !BlocksComplete. Regression test for
// the post-assembly token read, under the default legacy coordination.
func TestBlockCache_PromotionRefusedWhenInvalidationRacesAssembly(t *testing.T) {
	mock := newBlockMock([]byte("ABCDEFGHIJ"), `"v1"`)
	hooked := &blockPutHookClient{
		CacheClient: cacheclient.NewMemoryCache(),
		hookKey:     cache.MakeBlockKey(wowBucket, wowKey, `"v1"`, 4, 2),
	}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	c := cache.NewCacheWithClient(hooked, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	// The moment assembly writes the missing block: invalidate, then
	// re-establish the same-ETag entry (as a fresh partial populate would).
	hooked.hook = func() {
		ctx := context.Background()
		prior, found, _ := c.GetMeta(ctx, wowBucket, wowKey)
		if !found {
			t.Error("hook: no meta to displace")
			return
		}
		if err := c.DeleteWithMeta(ctx, wowBucket, wowKey); err != nil {
			t.Errorf("hook: invalidation: %v", err)
			return
		}
		_, tok, _, _ := c.GetMetaWithVersion(ctx, wowBucket, wowKey)
		fresh := *prior
		fresh.BlocksComplete = false
		if wrote, err := c.PutMetaIfVersion(ctx, wowBucket, wowKey, &fresh, 60, tok); err != nil || !wrote {
			t.Errorf("hook: re-establish: wrote=%v err=%v", wrote, err)
		}
	}

	// Partial entry via a cold range miss (blocks 0,1 of 3).
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(wowBucket, wowKey, "bytes=0-7")); err != nil {
		t.Fatalf("cold range miss: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("meta not populated")
	}

	// The refused promotion is observable as a precondition_lost meta_put — the
	// only such refusal in this flow (the hook's own re-establishment is checked
	// to succeed), so waiting on it synchronizes with the fire-and-forget
	// promotion goroutine instead of racing it with a fixed sleep.
	lostBefore := testutil.ToFloat64(metrics.CacheOperations.WithLabelValues("meta_put", "precondition_lost"))

	// Full GET assembles block 2 (firing the hook mid-assembly) and serves.
	w2 := httptest.NewRecorder()
	if err := svc.HandleGetObject(w2, fullGet(wowBucket, wowKey)); err != nil {
		t.Fatalf("full GET: %v", err)
	}
	if w2.Code != http.StatusOK || w2.Body.String() != "ABCDEFGHIJ" {
		t.Fatalf("full GET: code=%d body=%q", w2.Code, w2.Body.String())
	}
	if !hooked.fired.Load() {
		t.Fatal("hook never fired: assembly did not write the missing block")
	}

	// The displaced assembly's promotion must be attempted AND refused; the
	// re-established entry stays !BlocksComplete throughout.
	deadline := time.Now().Add(5 * time.Second)
	for {
		m, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
		if found && m.BlocksComplete {
			t.Fatal("promotion committed over a mid-assembly invalidation")
		}
		if testutil.ToFloat64(metrics.CacheOperations.WithLabelValues("meta_put", "precondition_lost")) > lostBefore {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("promotion refusal never observed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
