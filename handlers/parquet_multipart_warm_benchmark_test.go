package handlers

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// BenchmarkParquetFirstOpenAfterMultipartWriteWarm drives the registered S3
// completion and GET routes with a document-shaped object. The local origin
// returns exact range responses and adds a fixed 5 ms per ranged GET; this is a
// controlled latency model, not a production estimate. Only the first-open
// client operation is timed. Background read-prefetch runs normally but its
// upstream requests are counted separately from foreground requests. The test
// forwarder accepts a fixed signed-request fixture and returns a successful 200
// completion with ETag "v1"; its local origin serves a sparse document-shaped
// object with a valid PAR1 trailer. Neither production auth work nor the live
// origin-latency distribution is modeled.
func BenchmarkParquetFirstOpenAfterMultipartWriteWarm(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	const (
		blockSize  = int64(1 << 20)
		objectSize = int64(300)*blockSize + blockSize/4
		footerLen  = int64(3_500_000)
	)

	var foregroundGets int64
	var foregroundBytes int64
	b.ResetTimer()
	for iteration := 0; b.Loop(); iteration++ {
		b.StopTimer()
		var gets int64
		var bytes int64
		func() {
			origin := newHandlerParquetOrigin(objectSize, blockSize, footerLen, 5*time.Millisecond)
			originHTTP := httptest.NewServer(origin)
			replay := newHandlerParquetReplayWithOrigin(b, origin, originHTTP, true)
			defer replay.close()

			replay.completeAndPauseFooter(b, fmt.Sprintf("benchmark-upload-%d", iteration))
			replay.finishFooterWarm(b)
			replay.resetMeasurements()
			replay.observeReadPrefetch()

			b.StartTimer()
			responses := replay.firstOpen(b)
			b.StopTimer()
			replay.verifyFirstOpen(b, responses)
			gets = int64(replay.foregroundGets())
			bytes = replay.forwarder.foregroundBytes()
			if unexpected := replay.unexpectedRanges(); len(unexpected) != 0 {
				b.Fatalf("unexpected origin ranges during first open: %v", unexpected)
			}
			replay.waitReadPrefetch(b)
		}()
		foregroundGets += gets
		foregroundBytes += bytes
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(foregroundGets)/float64(b.N), "foreground_origin_gets/op")
	b.ReportMetric(float64(foregroundBytes)/float64(b.N), "foreground_origin_bytes/op")
}
