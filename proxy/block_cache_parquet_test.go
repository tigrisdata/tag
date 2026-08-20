package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// parquetTailBlock builds an object tail that a parquet reader would recognise:
// footerLen bytes of metadata, then the 4-byte length, then the magic.
func parquetTailBlock(t *testing.T, blockSize, footerLen int64) (content []byte, contentLength int64) {
	t.Helper()
	// One full block of padding before the metadata region keeps the trailer
	// inside the tail block while forcing the metadata to start earlier.
	contentLength = blockSize + footerLen + parquetTrailerSize
	content = make([]byte, contentLength)
	binary.LittleEndian.PutUint32(content[contentLength-parquetTrailerSize:], uint32(footerLen))
	copy(content[contentLength-4:], parquetMagic)
	return content, contentLength
}

func TestIsParquetKey(t *testing.T) {
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{"metrics/date=2026-08-19/hour=01/minute=40/ingestor.data.01M0.parquet", true},
		{"UPPER.PARQUET", true},
		{"manifest.json", false},
		{"parquet", false},
		{"a.parquet.tmp", false},
	} {
		if got := isParquetKey(tc.key); got != tc.want {
			t.Errorf("isParquetKey(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

// The trigger must fire only for a completed read that reached the object's
// trailer, and only when the optimization is enabled. Everything else is a
// read that tells us nothing about a reader opening the file.
func TestMaybePrefetchParquetFooter_TriggerConditions(t *testing.T) {
	const blockSize = 1024
	meta := &cache.CachedObjectMeta{
		ETag:          `"v1"`,
		BlockSize:     blockSize,
		ContentLength: 4096,
	}
	tail := meta.ContentLength - 1
	trailer := meta.ContentLength - parquetTrailerSize

	for _, tc := range []struct {
		name        string
		enabled     bool
		key         string
		start, end  int64
		wantTrigger bool
	}{
		{"tail read on parquet fires", true, "a.parquet", trailer, tail, true},
		{"whole object read fires", true, "a.parquet", 0, tail, true},
		{"disabled does not fire", false, "a.parquet", trailer, tail, false},
		{"non-parquet key does not fire", true, "a.json", trailer, tail, false},
		{"read short of the trailer does not fire", true, "a.parquet", 0, trailer - 1, false},
		{"read stopping before the last byte does not fire", true, "a.parquet", trailer, tail - 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.NewDefault()
			cfg.Cache.ParquetOptimization = tc.enabled
			s := &Service{config: cfg}

			// With no cache wired, a fired trigger panics in the goroutine it
			// starts, so assert on the decision itself rather than the effect.
			got := s.parquetFooterPrefetchWanted(tc.key, meta, tc.start, tc.end)
			if got != tc.wantTrigger {
				t.Errorf("trigger = %v, want %v", got, tc.wantTrigger)
			}
		})
	}
}

// The declared metadata length is read from the object's own trailer. A value
// that is not a parquet trailer, or that cannot describe this object, must be
// rejected rather than turned into a prefetch range.
func TestReadParquetFooterLength(t *testing.T) {
	const blockSize = 1024

	t.Run("reads the declared length", func(t *testing.T) {
		content, contentLength := parquetTailBlock(t, blockSize, 300)
		s := newParquetTestService(t, content, blockSize)
		meta := &cache.CachedObjectMeta{ETag: `"v1"`, BlockSize: blockSize, ContentLength: contentLength}

		got, ok := s.readParquetFooterLength(context.Background(), "b", "a.parquet", meta)
		if !ok || got != 300 {
			t.Fatalf("footer length = %d (ok=%v), want 300", got, ok)
		}
	})

	t.Run("rejects a missing magic", func(t *testing.T) {
		content, contentLength := parquetTailBlock(t, blockSize, 300)
		copy(content[contentLength-4:], "XXXX")
		s := newParquetTestService(t, content, blockSize)
		meta := &cache.CachedObjectMeta{ETag: `"v1"`, BlockSize: blockSize, ContentLength: contentLength}

		if _, ok := s.readParquetFooterLength(context.Background(), "b", "a.parquet", meta); ok {
			t.Fatal("accepted an object whose trailer is not a parquet trailer")
		}
	})

	t.Run("rejects a length larger than the object", func(t *testing.T) {
		content, contentLength := parquetTailBlock(t, blockSize, 300)
		binary.LittleEndian.PutUint32(content[contentLength-parquetTrailerSize:], uint32(contentLength))
		s := newParquetTestService(t, content, blockSize)
		meta := &cache.CachedObjectMeta{ETag: `"v1"`, BlockSize: blockSize, ContentLength: contentLength}

		if _, ok := s.readParquetFooterLength(context.Background(), "b", "a.parquet", meta); ok {
			t.Fatal("accepted a metadata length that cannot fit in the object")
		}
	})
}

// Precision accounting must count blocks, not reads: a prefetched block served
// repeatedly is one useful prefetch, not many.
func TestPrefetchAttribution_CountsEachBlockOnce(t *testing.T) {
	s := NewService(nil, nil, config.NewDefault())
	const blockSize = 1024

	s.notePrefetchedBlock("b", "a.parquet", `"v1"`, blockSize, 7)
	key := cache.MakeBlockKey("b", "a.parquet", `"v1"`, blockSize, 7)
	if _, ok := s.prefetchedBlocks.Get(key); !ok {
		t.Fatal("prefetched block was not recorded")
	}

	s.notePrefetchHit("b", "a.parquet", `"v1"`, blockSize, 7)
	if _, ok := s.prefetchedBlocks.Get(key); ok {
		t.Fatal("attribution left the block recorded, so a re-read would count twice")
	}
	// A second hit on the same block must be a no-op rather than a panic or a
	// second attribution.
	s.notePrefetchHit("b", "a.parquet", `"v1"`, blockSize, 7)
}

// parquetFooterForwarder serves ranged reads out of a full object body and
// records what was asked for, which is what the prefetch is judged on.
type parquetFooterForwarder struct {
	mockForwarder
	body []byte
	etag string

	mu     sync.Mutex
	ranges []string
	served chan struct{}
}

func (f *parquetFooterForwarder) DoConditionalGetRequest(_ context.Context, _, _, _, _, _ string, _ int64, rangeHeader string) (*http.Response, error) {
	var start, end int64
	if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
		return nil, fmt.Errorf("unparsable range %q: %w", rangeHeader, err)
	}
	f.mu.Lock()
	f.ranges = append(f.ranges, rangeHeader)
	f.mu.Unlock()
	if f.served != nil {
		f.served <- struct{}{}
	}

	header := make(http.Header)
	header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(f.body)))
	header.Set("ETag", f.etag)
	chunk := f.body[start : end+1]
	return &http.Response{
		StatusCode:    http.StatusPartialContent,
		ContentLength: int64(len(chunk)),
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(chunk)),
	}, nil
}

func (f *parquetFooterForwarder) requestedRanges() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ranges)
}

// The whole point of the optimization: metadata that spans more blocks than the
// tail must pull exactly the blocks it spans — no more, and not the tail block
// the triggering read already cached.
func TestPrefetchParquetFooterBlocks_FetchesTheSpannedBlocks(t *testing.T) {
	const (
		blockSize = 1024
		blocks    = 6
		bucket    = "bucket"
		key       = "a.parquet"
		etag      = `"v1"`
	)
	contentLength := int64(blockSize * blocks)
	// Place the metadata start inside block 3, so blocks 3 and 4 must be
	// prefetched and block 5 (the tail) must not.
	metaStart := int64(3500)
	footerLen := contentLength - parquetTrailerSize - metaStart

	body := make([]byte, contentLength)
	for i := range body {
		body[i] = byte(i)
	}
	binary.LittleEndian.PutUint32(body[contentLength-parquetTrailerSize:], uint32(footerLen))
	copy(body[contentLength-4:], parquetMagic)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.ParquetOptimization = true
	store := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	fwd := &parquetFooterForwarder{body: body, etag: etag}
	s := NewService(fwd, store, cfg)

	meta := &cache.CachedObjectMeta{ETag: etag, BlockSize: blockSize, ContentLength: contentLength}
	// Seed only the tail block, as the triggering read would have.
	tail := int64(blocks - 1)
	if err := store.PutBlock(context.Background(), bucket, key, etag, blockSize, tail, body[tail*blockSize:], 3600); err != nil {
		t.Fatalf("seeding tail block: %v", err)
	}

	s.prefetchParquetFooterBlocks(bucket, key, "access", "secret", meta, nil)

	want := []string{"bytes=3072-4095", "bytes=4096-5119"}
	if got := fwd.requestedRanges(); !slices.Equal(got, want) {
		t.Fatalf("upstream ranges = %v, want %v", got, want)
	}
	for _, idx := range []int64{3, 4} {
		if !store.BlockExists(context.Background(), bucket, key, etag, blockSize, idx) {
			t.Errorf("block %d was not cached by the prefetch", idx)
		}
		if _, ok := s.prefetchedBlocks.Get(cache.MakeBlockKey(bucket, key, etag, blockSize, idx)); !ok {
			t.Errorf("block %d was not recorded for attribution", idx)
		}
	}
}

// Metadata that fits inside the tail block is already cached by the read that
// triggered the prefetch, so speculating further would be pure waste.
func TestPrefetchParquetFooterBlocks_NoFetchWhenFooterFitsTailBlock(t *testing.T) {
	const blockSize = 1024
	content, contentLength := parquetTailBlock(t, blockSize, 300)
	s := newParquetTestService(t, content, blockSize)
	fwd := &parquetFooterForwarder{body: content, etag: `"v1"`}
	s.forwarder = fwd

	meta := &cache.CachedObjectMeta{ETag: `"v1"`, BlockSize: blockSize, ContentLength: contentLength}
	s.prefetchParquetFooterBlocks("b", "a.parquet", "access", "secret", meta, nil)

	if got := fwd.requestedRanges(); len(got) != 0 {
		t.Fatalf("prefetched %v for metadata that fits the tail block", got)
	}
}

// A hot object is tail-read repeatedly. Each such read must not start its own
// prefetch goroutine while one is already running for that object version.
func TestMaybePrefetchParquetFooter_CoalescesConcurrentTriggers(t *testing.T) {
	const blockSize = 1024
	content, contentLength := parquetTailBlock(t, blockSize, 300)
	s := newParquetTestService(t, content, blockSize)
	meta := &cache.CachedObjectMeta{ETag: `"v1"`, BlockSize: blockSize, ContentLength: contentLength}

	// Stand in for a prefetch already running for this object version.
	dedupKey := "pq:b/a.parquet/" + meta.ETag
	s.activeBackgroundFetches.Store(dedupKey, struct{}{})

	s.maybePrefetchParquetFooter("b", "a.parquet", "access", "secret", meta, contentLength-parquetTrailerSize, contentLength-1, nil)

	// A coalesced trigger must leave the in-flight marker for its owner to clear.
	if _, ok := s.activeBackgroundFetches.Load(dedupKey); !ok {
		t.Fatal("coalesced trigger deleted the in-flight marker owned by another prefetch")
	}
}

// Regression test for the wiring, not the mechanism. A parquet reader's trailer
// probe is a few bytes, so it is served by the assembled-range path and never
// reaches streamBlockRange. Triggering the prefetch only from the streaming path
// meant the signal that matters most never fired in production shapes.
func TestServeRangeFromBlockCache_TrailerProbeTriggersFooterPrefetch(t *testing.T) {
	const (
		blockSize = 1024
		blocks    = 6
		bucket    = "bucket"
		key       = "a.parquet"
		etag      = `"v1"`
	)
	contentLength := int64(blockSize * blocks)
	metaStart := int64(3500) // inside block 3, so blocks 3 and 4 must be prefetched
	footerLen := contentLength - parquetTrailerSize - metaStart

	body := make([]byte, contentLength)
	binary.LittleEndian.PutUint32(body[contentLength-parquetTrailerSize:], uint32(footerLen))
	copy(body[contentLength-4:], parquetMagic)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.ParquetOptimization = true
	store := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	served := make(chan struct{}, blocks)
	fwd := &parquetFooterForwarder{body: body, etag: etag, served: served}
	s := NewService(fwd, store, cfg)

	meta := &cache.CachedObjectMeta{ETag: etag, BlockSize: blockSize, ContentLength: contentLength}
	tail := int64(blocks - 1)
	if err := store.PutBlock(context.Background(), bucket, key, etag, blockSize, tail, body[tail*blockSize:], 3600); err != nil {
		t.Fatalf("seeding tail block: %v", err)
	}

	// The read a parquet reader actually issues first: the 8-byte trailer.
	rangeHeader := fmt.Sprintf("bytes=%d-%d", contentLength-parquetTrailerSize, contentLength-1)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	req.Header.Set("Range", rangeHeader)

	ok, err := s.serveRangeFromBlockCache(context.Background(), rec, req, bucket, key, "access", "secret", meta, rangeHeader, time.Now())
	if err != nil || !ok {
		t.Fatalf("serveRangeFromBlockCache(trailer) = (%v, %v), want (true, nil)", ok, err)
	}
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", rec.Code)
	}

	// The prefetch is detached; wait for the two blocks it must fetch.
	for i := 0; i < 2; i++ {
		select {
		case <-served:
		case <-time.After(5 * time.Second):
			t.Fatalf("trailer probe did not trigger the footer prefetch (ranges so far: %v)", fwd.requestedRanges())
		}
	}

	want := []string{"bytes=3072-4095", "bytes=4096-5119"}
	if got := fwd.requestedRanges(); !slices.Equal(got, want) {
		t.Fatalf("prefetched ranges = %v, want %v", got, want)
	}
}

// Regression test for the cold-open race. The assembled serve path returns a
// freshly fetched tail block from an in-memory lease while its cache write is
// still detached, so the trailer is NOT yet readable from cache when the
// prefetch fires. Reading it back from cache would skip the prefetch on a
// reader's first open, which is precisely what this feature targets -- and it
// would also bias tag_cache_parquet_footer_bytes toward warm re-opens only.
func TestPrefetchParquetFooterBlocks_UsesServedTrailerWhenTailNotYetCached(t *testing.T) {
	const (
		blockSize = 1024
		blocks    = 6
		bucket    = "bucket"
		key       = "a.parquet"
		etag      = `"v1"`
	)
	contentLength := int64(blockSize * blocks)
	metaStart := int64(3500)
	footerLen := contentLength - parquetTrailerSize - metaStart

	body := make([]byte, contentLength)
	binary.LittleEndian.PutUint32(body[contentLength-parquetTrailerSize:], uint32(footerLen))
	copy(body[contentLength-4:], parquetMagic)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.ParquetOptimization = true
	store := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	fwd := &parquetFooterForwarder{body: body, etag: etag}
	s := NewService(fwd, store, cfg)

	meta := &cache.CachedObjectMeta{ETag: etag, BlockSize: blockSize, ContentLength: contentLength}

	// Deliberately seed NOTHING: this is the cold open, with the tail block's
	// cache write still in flight.
	trailer := body[contentLength-parquetTrailerSize:]
	s.prefetchParquetFooterBlocks(bucket, key, "access", "secret", meta, trailer)

	want := []string{"bytes=3072-4095", "bytes=4096-5119"}
	if got := fwd.requestedRanges(); !slices.Equal(got, want) {
		t.Fatalf("prefetched ranges = %v, want %v (served trailer was ignored)", got, want)
	}
}

// Without the served bytes and without a cached tail, there is nothing to read
// the length from, so the prefetch must skip rather than guess.
func TestPrefetchParquetFooterBlocks_SkipsWhenTrailerUnavailable(t *testing.T) {
	const blockSize = 1024
	content, contentLength := parquetTailBlock(t, blockSize, 300)
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.ParquetOptimization = true
	store := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	fwd := &parquetFooterForwarder{body: content, etag: `"v1"`}
	s := NewService(fwd, store, cfg)

	meta := &cache.CachedObjectMeta{ETag: `"v1"`, BlockSize: blockSize, ContentLength: contentLength}
	s.prefetchParquetFooterBlocks("b", "a.parquet", "access", "secret", meta, nil)

	if got := fwd.requestedRanges(); len(got) != 0 {
		t.Fatalf("prefetched %v with no readable trailer", got)
	}
}

// newParquetTestService wires a Service whose cache serves the given object
// content at block granularity, which is all the footer reader needs.
func newParquetTestService(t *testing.T, content []byte, blockSize int64) *Service {
	t.Helper()
	cfg := config.NewDefault()
	c := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	s := NewService(nil, c, cfg)
	s.config.Cache.ParquetOptimization = true

	contentLength := int64(len(content))
	blocks := (contentLength + blockSize - 1) / blockSize
	for i := int64(0); i < blocks; i++ {
		start := i * blockSize
		end := min(start+blockSize, contentLength)
		if err := c.PutBlock(context.Background(), "b", "a.parquet", `"v1"`, blockSize, i, content[start:end], 3600); err != nil {
			t.Fatalf("seeding block %d: %v", i, err)
		}
	}
	return s
}
