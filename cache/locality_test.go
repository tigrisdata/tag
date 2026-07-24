package cache

import (
	"bytes"
	"context"
	"testing"

	dto "github.com/prometheus/client_model/go"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
)

// localityMockClient wraps a real in-memory client and reports a fixed
// ownership answer, standing in for the embedded ocache cluster client's
// IsLocal without needing an actual cluster.
type localityMockClient struct {
	cacheclient.CacheClient
	local bool
}

func (m *localityMockClient) IsLocal(string) bool { return m.local }

func readLocality(t *testing.T, locality string) float64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.CacheServeLocality.WithLabelValues(locality).Write(&m); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

// serveLocalityDeltas runs fn and returns how much the local/remote counters
// moved, so tests are independent of accumulated global counter state.
func serveLocalityDeltas(t *testing.T, fn func()) (localDelta, remoteDelta float64) {
	t.Helper()
	l0 := readLocality(t, metrics.LocalityLocal)
	r0 := readLocality(t, metrics.LocalityRemote)
	fn()
	return readLocality(t, metrics.LocalityLocal) - l0, readLocality(t, metrics.LocalityRemote) - r0
}

func newLocalityCache(t *testing.T, local bool) (*Cache, cacheclient.CacheClient) {
	t.Helper()
	mem := cacheclient.NewMemoryCache()
	mock := &localityMockClient{CacheClient: mem, local: local}
	cfg := config.NewDefault()
	return NewCacheWithClient(mock, &cfg.Cache), mem
}

// A body read on a locally-owned key records locality=local; on a peer-owned
// key it records locality=remote. This is the signal that makes cross-node
// serving visible (issue #122).
func TestServeLocality_ByOwnership(t *testing.T) {
	ctx := context.Background()
	bucket, key, etag := "b", "k", `"v1"`
	body := []byte("hello-world-body")

	cases := []struct {
		name       string
		local      bool
		wantLocal  float64
		wantRemote float64
	}{
		{"local owner", true, 1, 0},
		{"remote owner", false, 0, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, mem := newLocalityCache(t, tc.local)
			if err := mem.Put(ctx, MakeBodyKey(bucket, key, etag), body, 60); err != nil {
				t.Fatalf("seed body: %v", err)
			}

			var buf bytes.Buffer
			ld, rd := serveLocalityDeltas(t, func() {
				if err := c.GetBodyStream(ctx, bucket, key, etag, &buf); err != nil {
					t.Fatalf("GetBodyStream: %v", err)
				}
			})
			if ld != tc.wantLocal || rd != tc.wantRemote {
				t.Errorf("GetBodyStream locality delta = (local %v, remote %v), want (%v, %v)",
					ld, rd, tc.wantLocal, tc.wantRemote)
			}
		})
	}
}

// Both range paths — the normal inclusive range and the position-0 special
// case — record serve locality exactly once.
func TestServeLocality_RangeStream(t *testing.T) {
	ctx := context.Background()
	bucket, key, etag := "b", "k", `"v1"`
	body := []byte("hello-world-body")

	t.Run("normal range records remote", func(t *testing.T) {
		c, mem := newLocalityCache(t, false)
		if err := mem.Put(ctx, MakeBodyKey(bucket, key, etag), body, 60); err != nil {
			t.Fatalf("seed body: %v", err)
		}
		var buf bytes.Buffer
		ld, rd := serveLocalityDeltas(t, func() {
			if err := c.GetRangeStream(ctx, bucket, key, etag, 1, 4, &buf); err != nil {
				t.Fatalf("GetRangeStream: %v", err)
			}
		})
		if ld != 0 || rd != 1 {
			t.Errorf("range locality delta = (local %v, remote %v), want (0, 1)", ld, rd)
		}
	})

	t.Run("position-0 special case records local", func(t *testing.T) {
		c, mem := newLocalityCache(t, true)
		if err := mem.Put(ctx, MakeBodyKey(bucket, key, etag), body, 60); err != nil {
			t.Fatalf("seed body: %v", err)
		}
		var buf bytes.Buffer
		ld, rd := serveLocalityDeltas(t, func() {
			if err := c.GetRangeStream(ctx, bucket, key, etag, 0, 0, &buf); err != nil {
				t.Fatalf("GetRangeStream(0,0): %v", err)
			}
		})
		if ld != 1 || rd != 0 {
			t.Errorf("position-0 locality delta = (local %v, remote %v), want (1, 0)", ld, rd)
		}
	})
}

// A miss must not record any locality — only actual serves are counted.
func TestServeLocality_MissRecordsNothing(t *testing.T) {
	ctx := context.Background()
	c, _ := newLocalityCache(t, true)

	var buf bytes.Buffer
	ld, rd := serveLocalityDeltas(t, func() {
		// Nothing seeded → ErrNotFound, no serve.
		if err := c.GetBodyStream(ctx, "b", "missing", `"v1"`, &buf); err != ErrNotFound {
			t.Fatalf("GetBodyStream on miss = %v, want ErrNotFound", err)
		}
	})
	if ld != 0 || rd != 0 {
		t.Errorf("miss recorded locality (local %v, remote %v), want (0, 0)", ld, rd)
	}
}

// A client that cannot report ownership (no IsLocal) leaves the metric
// untouched rather than guessing — keeps the ratio honest until an
// IsLocal-capable ocache is deployed.
func TestServeLocality_NoCapabilityRecordsNothing(t *testing.T) {
	ctx := context.Background()
	bucket, key, etag := "b", "k", `"v1"`

	mem := cacheclient.NewMemoryCache() // plain client: no IsLocal
	cfg := config.NewDefault()
	c := NewCacheWithClient(mem, &cfg.Cache)
	if err := mem.Put(ctx, MakeBodyKey(bucket, key, etag), []byte("body"), 60); err != nil {
		t.Fatalf("seed body: %v", err)
	}

	var buf bytes.Buffer
	ld, rd := serveLocalityDeltas(t, func() {
		if err := c.GetBodyStream(ctx, bucket, key, etag, &buf); err != nil {
			t.Fatalf("GetBodyStream: %v", err)
		}
	})
	if ld != 0 || rd != 0 {
		t.Errorf("non-cluster client recorded locality (local %v, remote %v), want (0, 0)", ld, rd)
	}
}
