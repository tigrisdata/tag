package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type blockServeMetricValues struct {
	hits, misses, full, partial float64
}

type localityMetricValues struct {
	local, remote float64
}

func snapshotBlockServeMetrics(t *testing.T) blockServeMetricValues {
	t.Helper()
	return blockServeMetricValues{
		hits:    testutil.ToFloat64(metrics.CacheBlockHits),
		misses:  testutil.ToFloat64(metrics.CacheBlockMisses),
		full:    testutil.ToFloat64(metrics.CacheBlockRangeServed.WithLabelValues("full_hit")),
		partial: testutil.ToFloat64(metrics.CacheBlockRangeServed.WithLabelValues("partial_hit")),
	}
}

func snapshotLocalityMetrics(t *testing.T) localityMetricValues {
	t.Helper()
	return localityMetricValues{
		local:  testutil.ToFloat64(metrics.CacheServeLocality.WithLabelValues(metrics.LocalityLocal)),
		remote: testutil.ToFloat64(metrics.CacheServeLocality.WithLabelValues(metrics.LocalityRemote)),
	}
}

func assertRangeFallback(t *testing.T, fixture *rangeFanoutFixture) {
	t.Helper()
	end := len(fixture.body) - 1
	w := httptest.NewRecorder()
	if err := fixture.service.HandleGetObject(w, blockGet(fixture.bucket, fixture.key, fmt.Sprintf("bytes=0-%d", end))); err != nil {
		t.Fatalf("HandleGetObject: %v", err)
	}
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	if !bytes.Equal(w.Body.Bytes(), fixture.body) {
		t.Fatalf("fallback body differs from the requested range: got %d bytes, want %d", w.Body.Len(), len(fixture.body))
	}
	if got, want := w.Header().Get("Content-Range"), fmt.Sprintf("bytes 0-%d/%d", end, len(fixture.body)); got != want {
		t.Fatalf("Content-Range = %q, want %q", got, want)
	}
	if got := fixture.forwarder.upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstream requests = %d, want one range fallback", got)
	}
	if got := fixture.forwarder.blockGets.Load(); got != 0 {
		t.Fatalf("per-block origin requests = %d, want none", got)
	}
	meta, found, err := fixture.store.GetMeta(context.Background(), fixture.bucket, fixture.key)
	if err != nil || !found {
		t.Fatalf("metadata after fallback: found=%v err=%v", found, err)
	}
	if meta.ETag != fixture.etag || meta.BlockSize != fixture.blockSize || meta.ContentLength != int64(len(fixture.body)) {
		t.Fatalf("metadata changed after fallback: etag=%q blockSize=%d length=%d", meta.ETag, meta.BlockSize, meta.ContentLength)
	}
}

func expectedRangeProbeKeys(fixture *rangeFanoutFixture, count int) []string {
	keys := make([]string, count)
	for index := range keys {
		keys[index] = cache.MakeBlockKey(fixture.bucket, fixture.key, fixture.etag, fixture.blockSize, int64(index))
	}
	return keys
}

func assertRecordedRangeProbes(t *testing.T, fixture *rangeFanoutFixture, want []string) {
	t.Helper()
	if got := fixture.client.recordedProbes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("CacheClient probe keys = %v, want %v", got, want)
	}
	if got := fixture.owner.recordedProbes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("remote-owner probe keys = %v, want %v", got, want)
	}
}

func assertRecordedRangeProbePrefix(t *testing.T, fixture *rangeFanoutFixture, want []string) {
	t.Helper()
	for name, got := range map[string][]string{
		"CacheClient":  fixture.client.recordedProbes(),
		"remote owner": fixture.owner.recordedProbes(),
	} {
		if len(got) < len(want) || !reflect.DeepEqual(got[:len(want)], want) {
			t.Fatalf("%s initial probe keys = %v, want prefix %v", name, got, want)
		}
	}
}

func TestBlockCache_RangeFanoutStopsAfterCapPlusOneMisses(t *testing.T) {
	tests := []struct {
		name               string
		blocks             int
		cached             []int
		wantProbes         int
		wantRemoteLocality float64
	}{
		{name: "33 all missing", blocks: 33, wantProbes: 33},
		{name: "50 early crossing with cached tail", blocks: 50, cached: []int{0, 40}, wantProbes: int(maxRangeBlockFanout) + 2, wantRemoteLocality: 1},
		{name: "50 late crossing", blocks: 50, cached: rangeFanoutPrefix(17), wantProbes: 50, wantRemoteLocality: 17},
		{name: "128 early crossing with cached tail", blocks: 128, cached: []int{0, 80}, wantProbes: int(maxRangeBlockFanout) + 2, wantRemoteLocality: 1},
		{name: "128 late crossing", blocks: 128, cached: rangeFanoutPrefix(95), wantProbes: 128, wantRemoteLocality: 95},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newRangeFanoutFixture(t, 4, tc.blocks, tc.cached, 0)
			if err := fixture.warmRemoteConnections(); err != nil {
				t.Fatalf("warm remote owner: %v", err)
			}
			fixture.startProbeTracking()

			serveBefore := snapshotBlockServeMetrics(t)
			localityBefore := snapshotLocalityMetrics(t)
			assertRangeFallback(t, fixture)

			wantProbes := expectedRangeProbeKeys(fixture, tc.wantProbes)
			assertRecordedRangeProbes(t, fixture, wantProbes)
			if after := snapshotBlockServeMetrics(t); after != serveBefore {
				t.Errorf("fallback changed committed block-serve metrics: before=%+v after=%+v", serveBefore, after)
			}
			localityAfter := snapshotLocalityMetrics(t)
			if got := localityAfter.local - localityBefore.local; got != 0 {
				t.Errorf("locality samples for remote-owned blocks = %v, want 0 local samples", got)
			}
			if got := localityAfter.remote - localityBefore.remote; got != tc.wantRemoteLocality {
				t.Errorf("remote locality samples = %v, want %v successful probes only", got, tc.wantRemoteLocality)
			}
		})
	}
}

func TestBlockCache_RangeFanoutAtLimitStillAssembles(t *testing.T) {
	fixture := newRangeFanoutFixture(t, 4, 33, []int{0}, 0)
	if err := fixture.warmRemoteConnections(); err != nil {
		t.Fatalf("warm remote owner: %v", err)
	}
	fixture.startProbeTracking()

	w := httptest.NewRecorder()
	if err := fixture.service.HandleGetObject(w, blockGet(fixture.bucket, fixture.key, "bytes=0-131")); err != nil {
		t.Fatalf("HandleGetObject: %v", err)
	}
	if w.Code != http.StatusPartialContent || !bytes.Equal(w.Body.Bytes(), fixture.body) {
		t.Fatalf("response = %d/%d bytes, want 206 with %d exact bytes", w.Code, w.Body.Len(), len(fixture.body))
	}
	if got := w.Header().Get("X-Cache"); got != XCacheHit {
		t.Fatalf("X-Cache = %q, want %q", got, XCacheHit)
	}
	assertRecordedRangeProbePrefix(t, fixture, expectedRangeProbeKeys(fixture, 33))
	if got := fixture.forwarder.blockGets.Load(); got != 32 {
		t.Fatalf("per-block origin requests = %d, want 32 misses assembled at the cap", got)
	}
	if got := fixture.forwarder.upstreamCalls.Load(); got != 0 {
		t.Fatalf("upstream range fallbacks = %d, want none", got)
	}
}

func TestBlockCache_RangeFanoutTransientProbeErrorIsNotAMiss(t *testing.T) {
	fixture := newRangeFanoutFixture(t, 4, 50, nil, 0)
	if err := fixture.warmRemoteConnections(); err != nil {
		t.Fatalf("warm remote owner: %v", err)
	}
	fixture.startProbeTracking()
	fixture.owner.failKey = cache.MakeBlockKey(fixture.bucket, fixture.key, fixture.etag, fixture.blockSize, 32)

	err := fixture.service.ensureBlocksCached(context.Background(), fixture.bucket, fixture.key, "access", "secret", fixture.meta, 0, 49, false, maxRangeBlockFanout)
	if status.Code(err) != codes.Unavailable || errors.Is(err, errBlockAssemblyWouldAmplify) {
		t.Fatalf("ensureBlocksCached error = %v, want the transient owner error (not amplification)", err)
	}
	assertRecordedRangeProbes(t, fixture, expectedRangeProbeKeys(fixture, 33))
	if meta, found, err := fixture.store.GetMeta(context.Background(), fixture.bucket, fixture.key); err != nil || !found || meta.ETag != fixture.etag {
		t.Fatalf("metadata changed after transient probe error: meta=%+v found=%v err=%v", meta, found, err)
	}
}

func TestBlockCache_RangeFanoutCancellationAfterCutoffIsPreserved(t *testing.T) {
	fixture := newRangeFanoutFixture(t, 4, 50, []int{0, 40}, 0)
	if err := fixture.warmRemoteConnections(); err != nil {
		t.Fatalf("warm remote owner: %v", err)
	}
	fixture.startProbeTracking()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var missingProbes int
	fixture.client.afterProbe = func(_ string, err error) {
		if err != nil {
			missingProbes++
			if missingProbes == int(maxRangeBlockFanout)+1 {
				cancel()
			}
		}
	}

	w := httptest.NewRecorder()
	err := fixture.service.HandleGetObject(w, blockGet(fixture.bucket, fixture.key, "bytes=0-199").WithContext(ctx))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("HandleGetObject error = %v, want context.Canceled", err)
	}
	assertRecordedRangeProbes(t, fixture, expectedRangeProbeKeys(fixture, int(maxRangeBlockFanout)+2))
	if w.Body.Len() != 0 {
		t.Fatalf("canceled fallback wrote %d response bytes, want none", w.Body.Len())
	}
	if got := fixture.forwarder.blockGets.Load(); got != 0 {
		t.Fatalf("per-block origin requests = %d, want none", got)
	}
}

func TestBlockCache_FullObjectMajorityScanRemainsExhaustive(t *testing.T) {
	fixture := newRangeFanoutFixture(t, 4, 50, nil, 0)
	if err := fixture.warmRemoteConnections(); err != nil {
		t.Fatalf("warm remote owner: %v", err)
	}
	fixture.startProbeTracking()

	w := httptest.NewRecorder()
	if err := fixture.service.HandleGetObject(w, fullGet(fixture.bucket, fixture.key)); err != nil {
		t.Fatalf("HandleGetObject: %v", err)
	}
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), fixture.body) {
		t.Fatalf("full response = %d/%d bytes, want 200 with %d exact bytes", w.Code, w.Body.Len(), len(fixture.body))
	}
	assertRecordedRangeProbes(t, fixture, expectedRangeProbeKeys(fixture, 50))
	if got := fixture.forwarder.upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstream requests = %d, want one full-object fallback", got)
	}
	if got := fixture.forwarder.blockGets.Load(); got != 0 {
		t.Fatalf("per-block origin requests = %d, want none", got)
	}
}

func TestBlockCache_RangeFanoutProbeFailureAtTheLimitDoesNotBail(t *testing.T) {
	fixture := newRangeFanoutFixture(t, 4, 50, nil, 0)
	if err := fixture.warmRemoteConnections(); err != nil {
		t.Fatalf("warm remote owner: %v", err)
	}
	fixture.startProbeTracking()
	fixture.owner.failKey = cache.MakeBlockKey(fixture.bucket, fixture.key, fixture.etag, fixture.blockSize, int64(maxRangeBlockFanout))

	w := httptest.NewRecorder()
	if err := fixture.service.HandleGetObject(w, blockGet(fixture.bucket, fixture.key, "bytes=0-199")); err != nil {
		t.Fatalf("HandleGetObject: %v", err)
	}
	if w.Code != http.StatusPartialContent || !bytes.Equal(w.Body.Bytes(), fixture.body) {
		t.Fatalf("fallback response = %d/%d bytes, want 206 with %d exact bytes", w.Code, w.Body.Len(), len(fixture.body))
	}
	assertRecordedRangeProbes(t, fixture, expectedRangeProbeKeys(fixture, int(maxRangeBlockFanout)+1))
	if got := fixture.forwarder.upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstream requests = %d, want one range fallback", got)
	}
	if got := fixture.forwarder.blockGets.Load(); got != 0 {
		t.Fatalf("per-block origin requests = %d, want none", got)
	}
}
