package cache

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func benchmarkBlockExistsErr(b *testing.B, writes [][]byte, backendErr error, wantPresent bool) {
	b.Helper()
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	client := &rangeProbeClient{writes: writes, err: backendErr}
	c := newRangeProbeCache(b, client)
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		present, err := c.BlockExistsErr(ctx, "bucket", "key", `"etag"`, 4, 0)
		if present != wantPresent || err != nil {
			b.Fatalf("BlockExistsErr = (present=%v, err=%v), want (present=%v, err=nil)", present, err, wantPresent)
		}
	}
}

// BenchmarkBlockExistsErrPresent measures the ordinary generic block-fetch leader
// probe when the cache backend returns the requested two-byte [0,1] range.
func BenchmarkBlockExistsErrPresent(b *testing.B) {
	benchmarkBlockExistsErr(b, [][]byte{{0, 1}}, nil, true)
}

// BenchmarkBlockExistsErrZeroByte measures the embedded-backend miss shape in
// which a successful [0,1] range request returns no bytes.
func BenchmarkBlockExistsErrZeroByte(b *testing.B) {
	benchmarkBlockExistsErr(b, nil, nil, false)
}

// BenchmarkBlockExistsErrNotFound measures a backend that reports a conventional
// not-found error before delivering any range bytes.
func BenchmarkBlockExistsErrNotFound(b *testing.B) {
	benchmarkBlockExistsErr(b, nil, errors.New("key not found"), false)
}
