package proxy

import "testing"

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
