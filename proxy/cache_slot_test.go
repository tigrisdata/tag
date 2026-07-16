package proxy

import (
	"testing"

	"github.com/tigrisdata/tag/config"
)

// TestService_CacheSlotCountLimit verifies the count ceiling admits up to its
// capacity, rejects beyond it (so callers skip caching instead of spawning
// unbounded populate work), and frees slots on release.
func TestService_CacheSlotCountLimit(t *testing.T) {
	s := &Service{cacheSemaphore: make(chan struct{}, 2)}

	if !s.acquireCacheSlot(1) {
		t.Fatal("1st acquire should succeed")
	}
	if !s.acquireCacheSlot(1) {
		t.Fatal("2nd acquire should succeed")
	}
	if s.acquireCacheSlot(1) {
		t.Fatal("3rd acquire should fail when the count limit (2) is reached")
	}

	s.releaseCacheSlot(1)
	if !s.acquireCacheSlot(1) {
		t.Fatal("acquire after release should succeed")
	}
}

// TestService_CacheSlotUnlimited verifies nil limiters disable both caps.
func TestService_CacheSlotUnlimited(t *testing.T) {
	s := &Service{} // nil cacheSemaphore and nil populateBudget
	for range 100 {
		if !s.acquireCacheSlot(1 << 30) {
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
		populateBudget: newByteBudget(100),
	}

	if !s.acquireCacheSlot(60) {
		t.Fatal("reserve 60/100 should succeed")
	}
	if !s.acquireCacheSlot(40) {
		t.Fatal("reserve 40 more (100/100) should succeed")
	}
	if s.acquireCacheSlot(1) {
		t.Fatal("reserve beyond the byte budget should fail")
	}
	// A rejected acquire must not leak the count slot it briefly took.
	s.releaseCacheSlot(40) // free 40 bytes
	if !s.acquireCacheSlot(40) {
		t.Fatal("acquire after releasing bytes should succeed")
	}
}

// TestService_CacheSlotByteBudgetReleasesCountOnByteReject verifies that when the
// byte budget rejects, the count slot taken first is handed back (no leak).
func TestService_CacheSlotByteBudgetReleasesCountOnByteReject(t *testing.T) {
	s := &Service{
		cacheSemaphore: make(chan struct{}, 1),
		populateBudget: newByteBudget(10),
	}
	// Byte budget too small — acquire must fail and free the single count slot.
	if s.acquireCacheSlot(1000) {
		t.Fatal("acquire should fail when weight exceeds the byte budget")
	}
	// The count slot must be available again.
	if !s.acquireCacheSlot(5) {
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

// TestService_BackgroundReservationClampedToBudget guards against the background
// path reserving the raw ceiling: when the budget is smaller than the per-populate
// ceiling, a background populate (unknown size) must still admit — one at a time —
// via populateWeight's budget clamp, not fail admission entirely.
func TestService_BackgroundReservationClampedToBudget(t *testing.T) {
	const budget = 8 << 20 // 8MB budget, below the 80MB ceiling
	s := &Service{
		cacheSemaphore: make(chan struct{}, 256),
		populateBudget: newByteBudget(budget),
		perPopulateCap: 80 << 20,
		config:         &config.Config{},
	}
	s.config.Cache.MaxPopulateMemoryBytes = budget

	w := s.populateWeight(-1) // what fetchFullObjectToCache reserves
	if w != budget {
		t.Fatalf("populateWeight(-1) = %d, want %d (clamped to budget)", w, budget)
	}
	if !s.acquireCacheSlot(w) {
		t.Fatal("background populate should admit one-at-a-time when budget < ceiling")
	}
	if s.acquireCacheSlot(s.populateWeight(-1)) {
		t.Fatal("second concurrent background populate should be throttled by the full budget")
	}
	s.releaseCacheSlot(w)
	if !s.acquireCacheSlot(s.populateWeight(-1)) {
		t.Fatal("after release the next background populate should admit")
	}
}

// TestPerPopulateBufferBytes verifies the per-populate ceiling accounts for the
// broadcast listener channel plus the cache-write queue's 64-chunk floor.
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
