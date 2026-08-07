package proxy

import "testing"

// Serve-staging draws from the SAME budget as populates (one honest total,
// max_populate_memory_bytes) but is capped at a share so it can never starve populates —
// the #141 isolation, now enforced within a single pool instead of a second same-size pool.

// Staging cannot exceed its cap even when the rest of the budget is free.
func TestByteBudget_StagingCappedAtShare(t *testing.T) {
	b := newByteBudget(100, 0, 1) // no warm reserve
	b.stagingCap = 50

	if !b.tryAcquireStaging(50) {
		t.Fatal("staging 50 should fit under cap 50")
	}
	if b.tryAcquireStaging(1) {
		t.Fatal("staging past its cap must be refused even though 50 bytes of budget remain")
	}
}

// The staging cap guarantees populates a floor of (total - stagingCap): a maxed-out staging
// load can never push the budget below that floor, so cold-miss populates always fit.
func TestByteBudget_StagingCannotStarvePopulate(t *testing.T) {
	b := newByteBudget(100, 0, 1)
	b.stagingCap = 50

	if !b.tryAcquireStaging(50) { // staging holds its whole cap
		t.Fatal("staging should acquire up to its cap")
	}
	if !b.tryAcquireReadMiss(50) {
		t.Fatal("populate floor (total-stagingCap=50) must remain available under max staging")
	}
	if b.tryAcquireReadMiss(1) {
		t.Fatal("budget is now fully committed; further populate must decline")
	}
}

// releaseStaging returns bytes to the pool AND clears stagingInUse — using plain release()
// would leak stagingInUse and permanently shrink the cap.
func TestByteBudget_StagingReleaseRestoresCap(t *testing.T) {
	b := newByteBudget(100, 0, 1)
	b.stagingCap = 50

	if !b.tryAcquireStaging(50) {
		t.Fatal("initial staging acquire should succeed")
	}
	b.releaseStaging(50)
	if b.stagingInUse != 0 {
		t.Fatalf("stagingInUse = %d after release, want 0", b.stagingInUse)
	}
	if b.remaining != 100 {
		t.Fatalf("remaining = %d after release, want 100", b.remaining)
	}
	if !b.tryAcquireStaging(50) {
		t.Fatal("after release the full staging cap must be acquirable again")
	}
}

// When populates are idle, staging may use the whole budget if its cap allows (the default
// stagingCap == total, so non-block budgets are unrestricted).
func TestByteBudget_StagingUsesWholeBudgetWhenCapIsTotal(t *testing.T) {
	b := newByteBudget(100, 0, 1) // stagingCap defaults to total (100)
	if b.stagingCap != 100 {
		t.Fatalf("default stagingCap = %d, want total 100", b.stagingCap)
	}
	if !b.tryAcquireStaging(100) {
		t.Fatal("with cap == total, staging may use the whole budget when it is free")
	}
	if b.tryAcquireStaging(1) {
		t.Fatal("budget exhausted")
	}
}

// Symmetrically, populate may use the whole budget when nothing is staging (the cap bounds
// staging, it does not reserve bytes away from populate).
func TestByteBudget_PopulateUsesWholeBudgetWhenStagingIdle(t *testing.T) {
	b := newByteBudget(100, 0, 1)
	b.stagingCap = 50 // staging idle
	if !b.tryAcquireReadMiss(100) {
		t.Fatal("populate must be able to use the entire budget while staging is idle")
	}
}

// Staging leaves room for pending warm-on-write, like read-miss, so warm is never starved
// regardless of how the staging cap and warm reserve are configured.
func TestByteBudget_StagingRespectsWarmReserve(t *testing.T) {
	b := newByteBudget(100, 0.5, 1) // reserveCap = 50
	b.stagingCap = 100              // cap is not the binding constraint here
	b.pendingWarm = 40              // reserve = min(pendingWarm, reserveCap) = 40

	if b.tryAcquireStaging(70) {
		t.Fatal("staging must not consume the 40 bytes reserved for pending warm-on-write")
	}
	if !b.tryAcquireStaging(60) {
		t.Fatal("staging up to (total - reserve) = 60 should succeed")
	}
}
