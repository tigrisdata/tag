package proxy

import (
	"context"
	"math"
	"testing"
	"time"
)

// readMiss uses the whole budget when no warm-on-write is pending (elastic — the
// reservation costs nothing when unused).
func TestByteBudget_ReadMissUsesFullBudgetWhenNoWarmPending(t *testing.T) {
	b := newByteBudget(100, 0.5, 1) // reserveCap = 50, but nothing pending
	if !b.tryAcquireReadMiss(100) {
		t.Fatal("read-miss should use the full budget when no warm-on-write is pending")
	}
	if b.tryAcquireReadMiss(1) {
		t.Fatal("budget is now exhausted; further read-miss must be declined")
	}
}

// readMiss backs off by exactly the pending warm demand (capped at reserveCap),
// leaving that headroom protected for warm-on-write.
func TestByteBudget_ReadMissYieldsPendingWarm(t *testing.T) {
	b := newByteBudget(100, 0.5, 1) // cap 50
	b.pendingWarm = 30              // a warm-on-write of 30 is waiting

	// read-miss may take up to 100 - min(30,50) = 70.
	if !b.tryAcquireReadMiss(70) {
		t.Fatal("read-miss should be admitted up to budget minus pending warm reserve")
	}
	if b.tryAcquireReadMiss(1) {
		t.Fatal("read-miss must not consume the reserve held for pending warm-on-write")
	}
}

// The reserve is capped at reserveCap: even huge pending warm demand can't make
// read-miss yield more than the configured fraction.
func TestByteBudget_ReserveCapBoundsYield(t *testing.T) {
	b := newByteBudget(100, 0.5, 1) // cap 50
	b.pendingWarm = 90              // more than the cap

	if !b.tryAcquireReadMiss(50) {
		t.Fatal("read-miss should still get (total - cap) even under large warm demand")
	}
	if b.tryAcquireReadMiss(1) {
		t.Fatal("read-miss must not dip below the capped reserve")
	}
}

// reserveFraction 0 disables the reservation: read-miss uses the whole budget even
// with warm-on-write pending (equal competition).
func TestByteBudget_ReservationDisabled(t *testing.T) {
	b := newByteBudget(100, 0, 1) // cap 0
	b.pendingWarm = 40
	if !b.tryAcquireReadMiss(100) {
		t.Fatal("with the reservation disabled, read-miss uses the full budget")
	}
}

// Fix for the small-budget starvation: when the configured fraction of the budget is
// smaller than a single populate's byte weight, reserveCap is floored at
// perPopulateCap so one warm-on-write can still fit. Otherwise a read-miss flood
// would starve every warm on a small budget (fraction*total < one populate).
func TestByteBudget_ReserveCapFlooredAtPerPopulateCap(t *testing.T) {
	const perPopulateCap = 40
	// 0.1 * 100 = 10, well below a single populate (40); the floor lifts it to 40.
	b := newByteBudget(100, 0.1, perPopulateCap)
	if b.reserveCap != perPopulateCap {
		t.Fatalf("reserveCap should be floored at perPopulateCap %d, got %d", perPopulateCap, b.reserveCap)
	}

	// A warm-on-write of one populate is pending; read-miss must yield the full 40.
	b.pendingWarm = perPopulateCap
	if !b.tryAcquireReadMiss(60) {
		t.Fatal("read-miss should take total minus the floored reserve (100-40=60)")
	}
	if b.tryAcquireReadMiss(1) {
		t.Fatal("read-miss must not consume the floored reserve held for the pending warm")
	}
}

// A non-finite fraction (NaN) is unordered and slips past the </> clamps; it must
// disable the reserve rather than produce a garbage int64 cap from total*NaN.
func TestByteBudget_NaNFractionDisablesReserve(t *testing.T) {
	b := newByteBudget(100, math.NaN(), 40)
	if b.reserveCap != 0 {
		t.Fatalf("NaN fraction must disable the reserve (cap 0), got %d", b.reserveCap)
	}
	// Reserve disabled: read-miss uses the whole budget even under warm demand.
	b.pendingWarm = 40
	if !b.tryAcquireReadMiss(100) {
		t.Fatal("NaN fraction should leave the full budget available to read-miss")
	}
}

// The count slot is shared with read-miss, so a warm holding its reserved bytes must
// still wait (bounded) for a count slot rather than being shed the instant a
// read-miss flood has filled every slot.
func TestService_AcquireCacheSlot_WarmWaitsForCountSlot(t *testing.T) {
	s := &Service{
		cacheSemaphore: make(chan struct{}, 1), // a single slot, held by the read-miss below
		populateBudget: newByteBudget(1000, 0.5, 1),
	}
	if !s.acquireCacheSlot(context.Background(), 10, priorityReadMiss) {
		t.Fatal("setup: read-miss should take the only count slot")
	}

	done := make(chan bool, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done <- s.acquireCacheSlot(ctx, 10, priorityWarmWrite)
	}()

	// Warm has budget headroom but no free count slot; it must wait, not shed.
	select {
	case <-done:
		t.Fatal("warm should wait for a count slot, not be shed immediately")
	case <-time.After(50 * time.Millisecond):
	}

	s.releaseCacheSlot(10) // free the slot; warm should now win it
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("warm should acquire once a count slot frees")
		}
	case <-time.After(time.Second):
		t.Fatal("warm timed out waiting for the count slot")
	}
}

// When no count slot frees before the deadline, warm is shed — and it must hand back
// the reserved budget it was holding while it waited, or that budget leaks.
func TestService_AcquireCacheSlot_WarmCountTimeoutReleasesBudget(t *testing.T) {
	s := &Service{
		cacheSemaphore: make(chan struct{}, 1),
		populateBudget: newByteBudget(1000, 0.5, 1),
	}
	if !s.acquireCacheSlot(context.Background(), 10, priorityReadMiss) {
		t.Fatal("setup: read-miss should take the only count slot")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if s.acquireCacheSlot(ctx, 10, priorityWarmWrite) {
		t.Fatal("warm should be shed when no count slot frees before the deadline")
	}
	// Read-miss holds 10 of 1000; if warm released the 10 it briefly took, 990 remain.
	if !s.populateBudget.tryAcquireReadMiss(990) {
		t.Fatal("warm must release its held budget when it times out on the count slot")
	}
}

// The regression this feature exists for: a read-miss flood fills the budget, yet a
// warm-on-write still acquires — read-miss yields freed budget to the pending warm.
func TestByteBudget_WarmWinsAgainstReadMissFlood(t *testing.T) {
	b := newByteBudget(100, 0.5, 1) // cap 50

	// Read-miss fills the entire budget.
	if !b.tryAcquireReadMiss(100) {
		t.Fatal("setup: read-miss should fill the budget")
	}

	// A warm-on-write asks for 40 and waits (budget is full).
	done := make(chan bool, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done <- b.acquireWarm(ctx, 40)
	}()

	// Let the warm register its pending demand.
	time.Sleep(20 * time.Millisecond)

	// Read-miss releases 40, then immediately tries to re-grab it. It must be
	// declined — the freed budget is reserved for the pending warm.
	b.release(40)
	if b.tryAcquireReadMiss(40) {
		t.Fatal("read-miss must yield freed budget to the pending warm-on-write")
	}

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("warm-on-write should acquire once read-miss frees budget")
		}
	case <-time.After(time.Second):
		t.Fatal("warm-on-write timed out waiting for its reserved budget")
	}
}

// warm-on-write gives up (does not block forever) when budget never frees, so it's
// shed like any other populate rather than pinning a goroutine.
func TestByteBudget_WarmTimesOutWhenBudgetNeverFrees(t *testing.T) {
	b := newByteBudget(100, 0.5, 1)
	if !b.tryAcquireReadMiss(100) {
		t.Fatal("setup: fill the budget")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if b.acquireWarm(ctx, 40) {
		t.Fatal("warm-on-write should time out when budget never frees")
	}
}

// Priority routes through acquireCacheSlot: with the budget full, a read-miss slot
// is declined (non-blocking) while a warm-on-write slot is admitted once freed.
func TestService_AcquireCacheSlot_WarmPriority(t *testing.T) {
	s := &Service{
		cacheSemaphore: make(chan struct{}, 1000),
		populateBudget: newByteBudget(100, 0.5, 1),
	}
	if !s.acquireCacheSlot(context.Background(), 100, priorityReadMiss) {
		t.Fatal("setup: fill the budget with a read-miss")
	}
	// read-miss is declined immediately when full.
	if s.acquireCacheSlot(context.Background(), 40, priorityReadMiss) {
		t.Fatal("read-miss must be declined when the budget is full")
	}
	// warm-on-write waits and wins once the read-miss releases.
	done := make(chan bool, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done <- s.acquireCacheSlot(ctx, 40, priorityWarmWrite)
	}()
	time.Sleep(20 * time.Millisecond)
	s.releaseCacheSlot(60) // free 60 of the 100
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("warm-on-write should acquire once budget frees")
		}
	case <-time.After(time.Second):
		t.Fatal("warm-on-write timed out")
	}
}
