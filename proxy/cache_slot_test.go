package proxy

import (
	"context"
	"testing"

	"github.com/tigrisdata/tag/config"
)

// TestService_CacheSlotCountLimit verifies the count ceiling admits up to its
// capacity, rejects beyond it (so callers skip caching instead of spawning
// unbounded populate work), and frees slots on release.
func TestService_CacheSlotCountLimit(t *testing.T) {
	s := &Service{cacheSemaphore: make(chan struct{}, 2)}

	if !s.acquireCacheSlot(context.Background(), 1, priorityReadMiss) {
		t.Fatal("1st acquire should succeed")
	}
	if !s.acquireCacheSlot(context.Background(), 1, priorityReadMiss) {
		t.Fatal("2nd acquire should succeed")
	}
	if s.acquireCacheSlot(context.Background(), 1, priorityReadMiss) {
		t.Fatal("3rd acquire should fail when the count limit (2) is reached")
	}

	s.releaseCacheSlot(1)
	if !s.acquireCacheSlot(context.Background(), 1, priorityReadMiss) {
		t.Fatal("acquire after release should succeed")
	}
}

// TestService_CacheSlotUnlimited verifies nil limiters disable both caps.
func TestService_CacheSlotUnlimited(t *testing.T) {
	s := &Service{} // nil cacheSemaphore and nil populateBudget
	for range 100 {
		if !s.acquireCacheSlot(context.Background(), 1<<30, priorityReadMiss) {
			t.Fatal("nil limiters must always admit")
		}
	}
	s.releaseCacheSlot(1 << 30) // must be a no-op, not panic
}

// TestService_CacheSlotByteBudget verifies the byte budget admits until the
// aggregate reserved bytes would exceed it, independent of the count, and that
// releasing bytes frees capacity.
func TestService_CacheSlotByteBudget(t *testing.T) {
	// Count effectively unlimited (large), budget = 100 bytes.
	s := &Service{
		cacheSemaphore: make(chan struct{}, 1000),
		populateBudget: newByteBudget(100, 0, 1),
	}

	if !s.acquireCacheSlot(context.Background(), 60, priorityReadMiss) {
		t.Fatal("reserve 60/100 should succeed")
	}
	if !s.acquireCacheSlot(context.Background(), 40, priorityReadMiss) {
		t.Fatal("reserve 40 more (100/100) should succeed")
	}
	if s.acquireCacheSlot(context.Background(), 1, priorityReadMiss) {
		t.Fatal("reserve beyond the byte budget should fail")
	}
	// A rejected acquire must not leak the count slot it briefly took.
	s.releaseCacheSlot(40) // free 40 bytes
	if !s.acquireCacheSlot(context.Background(), 40, priorityReadMiss) {
		t.Fatal("acquire after releasing bytes should succeed")
	}
}

// TestService_CacheSlotByteBudgetReleasesCountOnByteReject verifies that when the
// byte budget rejects, the count slot taken first is handed back (no leak).
func TestService_CacheSlotByteBudgetReleasesCountOnByteReject(t *testing.T) {
	s := &Service{
		cacheSemaphore: make(chan struct{}, 1),
		populateBudget: newByteBudget(10, 0, 1),
	}
	// Byte budget too small — acquire must fail and free the single count slot.
	if s.acquireCacheSlot(context.Background(), 1000, priorityReadMiss) {
		t.Fatal("acquire should fail when weight exceeds the byte budget")
	}
	// The count slot must be available again.
	if !s.acquireCacheSlot(context.Background(), 5, priorityReadMiss) {
		t.Fatal("count slot leaked after byte-budget rejection")
	}
}

// TestPopulateWeight verifies the reserved weight is the object size capped at the
// per-populate buffer ceiling, with unknown sizes reserving the ceiling and a
// budget clamp so an over-large object still populates one-at-a-time.
func TestPopulateWeight(t *testing.T) {
	s := &Service{
		perPopulateCap: 80 << 20, // 80MB ceiling
		config:         &config.Config{},
	}
	s.config.Cache.MaxPopulateMemoryBytes = 1 << 30 // 1 GiB budget

	tests := []struct {
		name          string
		contentLength int64
		want          int64
	}{
		{"small object reserves its size", 4096, 4096},
		{"unknown size reserves the ceiling", -1, 80 << 20},
		{"zero size reserves the ceiling", 0, 80 << 20},
		{"large object capped at ceiling", 500 << 20, 80 << 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := s.populateWeight(tt.contentLength); got != tt.want {
				t.Errorf("populateWeight(%d) = %d, want %d", tt.contentLength, got, tt.want)
			}
		})
	}

	// Budget smaller than the ceiling: an object bigger than the whole budget is
	// clamped to the budget so it can still populate one at a time.
	s.config.Cache.MaxPopulateMemoryBytes = 1 << 20 // 1 MiB budget < 80MB ceiling
	if got := s.populateWeight(-1); got != 1<<20 {
		t.Errorf("populateWeight(-1) with 1MiB budget = %d, want %d", got, 1<<20)
	}
}

// TestService_BackgroundReservationRejectsWhenBudgetTooSmall guards against the
// background path under-reserving its direct writer: when the budget is smaller
// than the direct-populate ceiling, admission rejects the populate rather than
// pretending that a smaller reservation can bound its buffers.
func TestService_BackgroundReservationRejectsWhenBudgetTooSmall(t *testing.T) {
	const budget = 8 << 20 // 8MB budget, below the 80MB test ceiling
	s := &Service{
		cacheSemaphore:              make(chan struct{}, 256),
		populateBudget:              newByteBudget(budget, 0, 1),
		backgroundPopulateWriterCap: 80 << 20,
		config:                      &config.Config{},
	}
	s.config.Cache.MaxPopulateMemoryBytes = budget

	w := s.backgroundPopulateWeight() // what fetchFullObjectToCache reserves
	if w != 80<<20 {
		t.Fatalf("backgroundPopulateWeight() = %d, want %d (full direct reservation)", w, 80<<20)
	}
	if s.acquireCacheSlot(context.Background(), w, priorityReadMiss) {
		t.Fatal("background populate should be rejected when its buffers exceed the budget")
	}
	if s.acquireCacheSlot(context.Background(), w, priorityWarmWrite) {
		t.Fatal("warm background populate should be rejected when its buffers exceed the budget")
	}
	// A rejected read-miss must return the count slot it briefly took.
	if !s.acquireCacheSlot(context.Background(), 1, priorityReadMiss) {
		t.Fatal("count or byte budget leaked after oversized background rejection")
	}
	s.releaseCacheSlot(1)
}

// TestBackgroundPopulateBufferBytes verifies the direct background reservation
// includes the clustered sender and destination buffers, direct storage buffers and,
// in block mode, one pooled block scratch buffer rather than the foreground relay.
func TestBackgroundPopulateBufferBytes(t *testing.T) {
	cfg := &config.Config{}
	cfg.Cache.BlockSize = 1 << 20
	scratch := int64(cfg.Cache.BlockSize)
	writer := backgroundCacheClientStreamBufferBytes + backgroundCacheServerChunkBufferBytes +
		backgroundStorageFileBufferBytes + backgroundStorageProbeBufferBytes
	if got, want := backgroundPopulateBufferBytes(cfg), writer+scratch; got != want {
		t.Errorf("backgroundPopulateBufferBytes(block mode) = %d, want %d", got, want)
	}

	cfg.Cache.SetBlockCachingEnabled(false)
	if got, want := backgroundPopulateBufferBytes(cfg), writer; got != want {
		t.Errorf("backgroundPopulateBufferBytes(whole mode) = %d, want %d", got, want)
	}

	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 0
	if got, want := backgroundPopulateBufferBytes(cfg), writer; got != want {
		t.Errorf("backgroundPopulateBufferBytes(zero block size) = %d, want %d", got, want)
	}
}

// TestBackgroundReservationIncludesClusteredStreamBuffers verifies that the aggregate
// budget rejects a second direct writer when two clustered writers would need more than it.
func TestBackgroundReservationIncludesClusteredStreamBuffers(t *testing.T) {
	cfg := &config.Config{}
	cfg.Cache.SetBlockCachingEnabled(false)
	weight := backgroundPopulateBufferBytes(cfg)
	want := backgroundCacheClientStreamBufferBytes + backgroundCacheServerChunkBufferBytes +
		backgroundStorageFileBufferBytes + backgroundStorageProbeBufferBytes
	if weight != want {
		t.Fatalf("whole-object background reservation = %d, want %d", weight, want)
	}

	const budget = 7 << 20
	s := &Service{
		cacheSemaphore:              make(chan struct{}, 2),
		populateBudget:              newByteBudget(budget, 0, weight),
		backgroundPopulateWriterCap: weight,
		config:                      cfg,
	}
	if !s.acquireCacheSlot(context.Background(), weight, priorityReadMiss) {
		t.Fatal("first whole-object populate should fit")
	}
	if s.acquireCacheSlot(context.Background(), weight, priorityReadMiss) {
		t.Fatal("second whole-object populate should exceed the 7 MiB budget")
	}
	s.releaseCacheSlot(weight)
}

// TestBackgroundReservationAddsBlockScratchOnlyAfterSelection verifies that a small
// whole-object warm can fit even when a block representation would not fit the budget.
func TestBackgroundReservationAddsBlockScratchOnlyAfterSelection(t *testing.T) {
	cfg := &config.Config{}
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4 << 20
	writer := backgroundPopulateWriterBufferBytes()
	scratch := int64(cfg.Cache.BlockSize)
	budget := writer + scratch - 1
	s := &Service{
		cacheSemaphore:              make(chan struct{}, 1),
		populateBudget:              newByteBudget(budget, 0, backgroundPopulateBufferBytes(cfg)),
		backgroundPopulateWriterCap: writer,
		config:                      cfg,
	}

	if !s.acquireCacheSlot(context.Background(), writer, priorityReadMiss) {
		t.Fatal("whole-object writer reservation should fit")
	}
	if s.tryAcquireCacheBytes(scratch, priorityWarmWrite) {
		t.Fatal("block scratch reservation should exceed the remaining budget")
	}
	s.releaseCacheSlot(writer)

	if !s.acquireCacheSlot(context.Background(), writer, priorityReadMiss) {
		t.Fatal("whole-object warm should remain admissible without block scratch")
	}
	s.releaseCacheSlot(writer)
}

// TestGetExactBlockBuf avoids reusing an oversized pooled buffer for the direct block writer.
func TestGetExactBlockBuf(t *testing.T) {
	const blockSize = 1 << 20
	large := make([]byte, 16<<20)
	blockBufPool.Put(&large)

	bufp := getExactBlockBuf(blockSize)
	if got := cap(*bufp); got != blockSize {
		t.Fatalf("getExactBlockBuf capacity = %d, want %d", got, blockSize)
	}
	putBlockBuf(bufp)
}

// TestPerPopulateBufferBytes verifies the foreground relay ceiling accounts for
// the broadcast listener channel plus the cache-write queue's 64-chunk floor.
func TestPerPopulateBufferBytes(t *testing.T) {
	mk := func(chunk, channelBuf int) *config.Config {
		c := &config.Config{}
		c.Broadcast.ChunkSize = chunk
		c.Broadcast.ChannelBuffer = channelBuf
		return c
	}

	// Defaults: (1024 + 256) * 64KB.
	if got, want := perPopulateBufferBytes(mk(64*1024, 1024)), int64(1024+256)*64*1024; got != want {
		t.Errorf("perPopulateBufferBytes(default) = %d, want %d", got, want)
	}
	// Small channel_buffer: queue is floored at 64, not channelBuf/4=4.
	if got, want := perPopulateBufferBytes(mk(64*1024, 16)), int64(16+64)*64*1024; got != want {
		t.Errorf("perPopulateBufferBytes(channelBuf=16) = %d, want %d (64-chunk floor)", got, want)
	}
	// chunk_size below the pool size is charged at DefaultChunkSize: queued chunks
	// retain pooled 64KB backing arrays regardless of the configured chunk_size.
	if got, want := perPopulateBufferBytes(mk(16*1024, 1024)), int64(1024+256)*64*1024; got != want {
		t.Errorf("perPopulateBufferBytes(chunk=16KB) = %d, want %d (pool floor, not 16KB)", got, want)
	}
	// Zero broadcast values fall back to defaults.
	if got, want := perPopulateBufferBytes(mk(0, 0)), int64(1024+256)*64*1024; got != want {
		t.Errorf("perPopulateBufferBytes(zero) = %d, want %d (defaults)", got, want)
	}
}
