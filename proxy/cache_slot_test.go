package proxy

import (
	"testing"

	"github.com/tigrisdata/tag/config"
)

// TestEffectiveCacheWriteLimit verifies the cache-populate concurrency is capped
// by the memory budget, not just the raw count — so a byte-unaware count can't pin
// gigabytes of populate buffers under large-object fan-out.
func TestEffectiveCacheWriteLimit(t *testing.T) {
	// per-populate buffer with defaults = (1024 + 256) * 64KB ≈ 80MB.
	const chunk = 64 * 1024
	const channelBuf = 1024
	newCfg := func(count int, budget int64) *config.Config {
		c := &config.Config{}
		c.Cache.MaxConcurrentWrites = count
		c.Cache.MaxPopulateMemoryBytes = budget
		c.Broadcast.ChunkSize = chunk
		c.Broadcast.ChannelBuffer = channelBuf
		return c
	}

	tests := []struct {
		name   string
		count  int
		budget int64
		want   int
	}{
		{"memory budget reduces count", 256, 1 << 30, 12}, // 1GiB / ~80MB = 12
		{"count is the binding cap", 4, 1 << 30, 4},       // budget allows more, count wins
		{"tiny budget still allows one", 256, 1, 1},       // never zero
		{"negative budget disables memory cap", 256, -1, 256},
		{"negative count disables limiter", -1, 1 << 30, 0}, // unbounded
		{"zero count disables limiter", 0, 1 << 30, 0},      // unbounded (finalize would set default first)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectiveCacheWriteLimit(newCfg(tt.count, tt.budget)); got != tt.want {
				t.Errorf("effectiveCacheWriteLimit(count=%d, budget=%d) = %d, want %d", tt.count, tt.budget, got, tt.want)
			}
		})
	}
}

// TestEffectiveCacheWriteLimit_ZeroBroadcastFallsBackToDefaults verifies the
// per-populate estimate falls back to broadcast defaults when unset, so the
// memory cap still applies.
func TestEffectiveCacheWriteLimit_ZeroBroadcastFallsBackToDefaults(t *testing.T) {
	c := &config.Config{}
	c.Cache.MaxConcurrentWrites = 256
	c.Cache.MaxPopulateMemoryBytes = 1 << 30
	// Broadcast fields left at zero — should use DefaultChunkSize/DefaultChannelBuffer.
	if got := effectiveCacheWriteLimit(c); got != 12 {
		t.Errorf("effectiveCacheWriteLimit with zero broadcast = %d, want 12 (defaults)", got)
	}
}

// TestService_CacheSlotLimit verifies the concurrent-cache-write limiter admits
// up to its capacity, rejects beyond it (so callers skip caching instead of
// spawning unbounded populate work), and frees slots on release.
func TestService_CacheSlotLimit(t *testing.T) {
	s := &Service{cacheSemaphore: make(chan struct{}, 2)}

	if !s.acquireCacheSlot() {
		t.Fatal("1st acquire should succeed")
	}
	if !s.acquireCacheSlot() {
		t.Fatal("2nd acquire should succeed")
	}
	if s.acquireCacheSlot() {
		t.Fatal("3rd acquire should fail when the limit (2) is reached")
	}

	s.releaseCacheSlot()
	if !s.acquireCacheSlot() {
		t.Fatal("acquire after release should succeed")
	}
}

// TestService_CacheSlotUnlimited verifies a nil semaphore disables the limit.
func TestService_CacheSlotUnlimited(t *testing.T) {
	s := &Service{cacheSemaphore: nil}
	for i := 0; i < 100; i++ {
		if !s.acquireCacheSlot() {
			t.Fatal("nil semaphore must always admit")
		}
	}
	s.releaseCacheSlot() // must be a no-op, not panic
}
