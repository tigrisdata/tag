package main

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/embedded"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// cacheRangeBenchmarkSink accepts the caller's bytes without adding a ReaderFrom
// fast path that the normal HTTP range writer does not provide.
type cacheRangeBenchmarkSink struct{}

func (cacheRangeBenchmarkSink) Write(p []byte) (int, error) { return len(p), nil }

var cacheRangeBenchmarkWriter io.Writer = cacheRangeBenchmarkSink{}

type embeddedCacheRangeBenchmarkFixture struct {
	store     *cache.Cache
	body      []byte
	bucket    string
	key       string
	etag      string
	rangeSize int64
	offsets   []int64
	ctx       context.Context
}

func newEmbeddedCacheRangeBenchmarkFixture(tb testing.TB, bodySize, rangeSize int) *embeddedCacheRangeBenchmarkFixture {
	tb.Helper()
	if bodySize <= 0 || rangeSize <= 0 || rangeSize > bodySize {
		tb.Fatalf("invalid body/range sizes: body=%d range=%d", bodySize, rangeSize)
	}

	cfg := config.NewDefault()
	cfg.Cache.DiskPath = tb.TempDir()
	cfg.Cache.SizeThreshold = int64(bodySize)
	cfg.Cache.SetBlockCachingEnabled(false)

	client, err := embedded.New(&embedded.Config{DiskPath: cfg.Cache.DiskPath, TTL: cfg.Cache.TTL})
	if err != nil {
		tb.Fatalf("create embedded cache: %v", err)
	}
	tb.Cleanup(func() { _ = client.Close() })

	store := cache.NewCacheWithClient(newEmbeddedBlockCacheClient(client), &cfg.Cache)
	body := make([]byte, bodySize)
	for i := range body {
		body[i] = byte(i*31 + i/257)
	}
	bucket, key, etag := "benchmark", "range-cache", `"range-v1"`
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          etag,
		ContentType:   "application/octet-stream",
		ContentLength: int64(len(body)),
		StatusCode:    http.StatusOK,
	}
	if err := store.PutWithMeta(context.Background(), bucket, key, meta, body, 0); err != nil {
		tb.Fatalf("seed embedded range cache: %v", err)
	}

	offsetCount := bodySize / rangeSize
	rng := rand.New(rand.NewSource(1))
	offsets := make([]int64, offsetCount)
	maxStart := int64(bodySize - rangeSize)
	for i := range offsets {
		offsets[i] = rng.Int63n(maxStart + 1)
	}
	return &embeddedCacheRangeBenchmarkFixture{
		store:     store,
		body:      body,
		bucket:    bucket,
		key:       key,
		etag:      etag,
		rangeSize: int64(rangeSize),
		offsets:   offsets,
		ctx:       context.Background(),
	}
}

func (f *embeddedCacheRangeBenchmarkFixture) readAt(start int64, w io.Writer) error {
	return f.store.GetRangeStream(f.ctx, f.bucket, f.key, f.etag, start, start+f.rangeSize-1, w)
}

func (f *embeddedCacheRangeBenchmarkFixture) verifyWarm(tb testing.TB) {
	tb.Helper()

	var got bytes.Buffer
	for _, start := range f.offsets {
		got.Reset()
		if err := f.readAt(start, &got); err != nil {
			tb.Fatalf("warm range read: %v", err)
		}
		if !bytes.Equal(got.Bytes(), f.body[start:start+f.rangeSize]) {
			tb.Fatalf("range at %d returned %d bytes, want %d exact bytes", start, got.Len(), f.rangeSize)
		}
	}
	f.body = nil // The stored object, not its population input, is the warmed state.
	runtime.GC()
}

// BenchmarkEmbeddedCacheRangeStream measures warmed, valid non-block hits for the
// documented large-object GET RANGE shape through the embedded-client wrapper.
// Run with -cpu=4 to model the repository's 64-call range benchmark concurrency.
func BenchmarkEmbeddedCacheRangeStream(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	b.Run("body-256MiB/range-64KiB", func(b *testing.B) {
		fixture := newEmbeddedCacheRangeBenchmarkFixture(b, 256<<20, 64<<10)
		fixture.verifyWarm(b)
		b.ReportAllocs()
		b.ResetTimer()
		b.SetParallelism(16)
		var nextWorker atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			offsetIndex := int(nextWorker.Add(1)-1) % len(fixture.offsets)
			for pb.Next() {
				start := fixture.offsets[offsetIndex]
				offsetIndex++
				if offsetIndex == len(fixture.offsets) {
					offsetIndex = 0
				}
				if err := fixture.readAt(start, cacheRangeBenchmarkWriter); err != nil {
					b.Errorf("warmed range read at %d: %v", start, err)
					return
				}
			}
		})
	})
}
