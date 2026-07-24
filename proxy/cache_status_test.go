package proxy

import (
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/tigrisdata/tag/metrics"
)

// writeCacheStatus is the single point that sets the X-Cache header and the
// hit/miss counters, so they cannot drift. This pins the mapping: HIT -> hits,
// MISS -> misses, and REVALIDATED/BYPASS/DISABLED -> neither (they set the header
// only).
func TestWriteCacheStatus_HeaderAndCounterInLockstep(t *testing.T) {
	read := func(c prometheus.Counter) float64 {
		t.Helper()
		var m dto.Metric
		if err := c.Write(&m); err != nil {
			t.Fatalf("read counter: %v", err)
		}
		return m.GetCounter().GetValue()
	}

	cases := []struct {
		status        string
		wantHitDelta  float64
		wantMissDelta float64
	}{
		{XCacheHit, 1, 0},
		{XCacheMiss, 0, 1},
		{XCacheRevalidated, 0, 0},
		{XCacheBypass, 0, 0},
		{XCacheDisabled, 0, 0},
	}

	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			hit0, miss0 := read(metrics.CacheHits), read(metrics.CacheMisses)

			w := httptest.NewRecorder()
			writeCacheStatus(w, tc.status)

			if got := w.Header().Get(XCacheHeader); got != tc.status {
				t.Errorf("X-Cache header = %q, want %q", got, tc.status)
			}
			if d := read(metrics.CacheHits) - hit0; d != tc.wantHitDelta {
				t.Errorf("%s: cache-hits delta = %v, want %v", tc.status, d, tc.wantHitDelta)
			}
			if d := read(metrics.CacheMisses) - miss0; d != tc.wantMissDelta {
				t.Errorf("%s: cache-misses delta = %v, want %v", tc.status, d, tc.wantMissDelta)
			}
		})
	}
}
