package metrics

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// gaugeValue reads the current value of a Prometheus gauge without pulling in the
// testutil package (which would add an indirect module dependency just for tests).
func gaugeValue(t *testing.T) float64 {
	t.Helper()
	var m dto.Metric
	if err := CacheSizeBytes.Write(&m); err != nil {
		t.Fatalf("read gauge: %v", err)
	}
	return m.GetGauge().GetValue()
}

// SampleCacheSize must publish an initial value immediately (before any tick) and
// stop cleanly when the context is cancelled.
func TestSampleCacheSize(t *testing.T) {
	CacheSizeBytes.Set(0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		// A long interval guarantees the value observed below comes from the
		// immediate initial sample, not a tick.
		SampleCacheSize(ctx, time.Hour, func() int64 { return 4096 })
		close(done)
	}()

	// Poll for the initial sample rather than sleeping a fixed duration.
	deadline := time.Now().Add(2 * time.Second)
	for gaugeValue(t) != 4096 {
		if time.Now().After(deadline) {
			t.Fatalf("tag_cache_size_bytes = %v, want 4096 (initial sample not published)", gaugeValue(t))
		}
		time.Sleep(2 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SampleCacheSize did not return after context cancellation")
	}
}
