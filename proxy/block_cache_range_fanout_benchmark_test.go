package proxy

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/config"
)

// BenchmarkBlockCacheRangeFanout measures the ordinary GET handler on sparse
// block-mode ranges that fall through to one upstream 206. The in-process cache
// owner adds a fixed 1 ms delay per gRPC read; it is a latency model, not a claim
// about production peer latency. The controlled forwarder returns the exact
// requested bytes without origin network time. The standard ns/op includes the
// full response copy, and prefallback_ns/op stops at upstream dispatch.
func BenchmarkBlockCacheRangeFanout(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	cases := []struct {
		name   string
		blocks int
		cached []int
	}{
		{name: "blocks33_all_missing", blocks: 33},
		{name: "blocks50_early_one_hit", blocks: 50, cached: []int{0}},
		{name: "blocks50_late_crossing", blocks: 50, cached: rangeFanoutPrefix(17)},
		{name: "blocks128_early_one_hit", blocks: 128, cached: []int{0}},
		{name: "blocks128_late_crossing", blocks: 128, cached: rangeFanoutPrefix(95)},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkBlockCacheRangeFanout(b, tc.blocks, tc.cached)
		})
	}
}

func benchmarkBlockCacheRangeFanout(b *testing.B, blockCount int, cached []int) {
	b.Helper()

	fixture := newRangeFanoutFixture(b, config.DefaultCacheBlockSize, blockCount, cached, time.Millisecond)
	if err := fixture.warmRemoteConnections(); err != nil {
		b.Fatalf("warm cache-owner RPCs: %v", err)
	}

	end := len(fixture.body) - 1
	request := blockGet(fixture.bucket, fixture.key, fmt.Sprintf("bytes=0-%d", end))
	writer := newRangeFanoutSink(len(fixture.body))
	started := time.Time{}
	var prefallback time.Duration
	fixture.forwarder.onDispatch = func() {
		prefallback += time.Since(started)
	}

	b.ResetTimer()
	for b.Loop() {
		writer.reset()
		started = time.Now()
		if err := fixture.service.HandleGetObject(writer, request); err != nil {
			b.Fatalf("HandleGetObject: %v", err)
		}
		if writer.statusCode != http.StatusPartialContent || writer.written != len(fixture.body) {
			b.Fatalf("fallback response = %d/%d bytes, want 206/%d", writer.statusCode, writer.written, len(fixture.body))
		}
	}
	if got := fixture.forwarder.upstreamCalls.Load(); got != int64(b.N) {
		b.Fatalf("upstream range calls = %d, want %d", got, b.N)
	}
	b.ReportMetric(float64(prefallback.Nanoseconds())/float64(b.N), "prefallback_ns/op")
}
