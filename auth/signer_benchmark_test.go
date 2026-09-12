package auth

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
)

const signerBenchmarkPath = "/benchmark-bucket/object%2Fwith space+and%25?prefix=one%2Ftwo&marker=a%20b&x=100%25"

var signerBenchmarkHeaders = http.Header{
	"Range":                 {"bytes=0-1048575"},
	"X-Amz-Meta-Request-Id": {"benchmark-request"},
}

func newSignerBenchmark() *RequestSigner {
	return NewRequestSigner("https://upstream.example.com", "us-east-1")
}

func BenchmarkRequestValidatorBuildCanonicalQueryStringReservedBytes(b *testing.B) {
	validator := NewRequestValidator(NewCredentialStore())
	query := newReservedCanonicalQueryValues()
	if got := validator.buildCanonicalQueryString(newReservedCanonicalQueryValues()); got != reservedCanonicalQueryString {
		b.Fatalf("buildCanonicalQueryString() = %q, want %q", got, reservedCanonicalQueryString)
	}

	b.ReportAllocs()
	for b.Loop() {
		validator.buildCanonicalQueryString(query)
	}
}

func BenchmarkRequestSignerSignRequest(b *testing.B) {
	signer := newSignerBenchmark()
	ctx := context.Background()
	headers := signerBenchmarkHeaders.Clone()
	b.ReportAllocs()

	for b.Loop() {
		if _, err := signer.SignRequest(
			ctx,
			http.MethodGet,
			signerBenchmarkPath,
			nil,
			"",
			requestSignerTestAccessKey,
			requestSignerTestSecretKey,
			headers,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRequestSignerSignRequestParallel(b *testing.B) {
	signer := newSignerBenchmark()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		headers := signerBenchmarkHeaders.Clone()
		for pb.Next() {
			if _, err := signer.SignRequest(
				ctx,
				http.MethodGet,
				signerBenchmarkPath,
				nil,
				"",
				requestSignerTestAccessKey,
				requestSignerTestSecretKey,
				headers,
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}

const (
	signerConditionalBenchmarkBucket = "benchmark-bucket"
	signerConditionalBenchmarkKey    = "object/with space+and%25/key"
	signerConditionalBenchmarkRange  = "bytes=0-1048575"
)

// benchmarkConditionalFast signs the range-only synthetic request through
// the dedicated conditional object signer.
func benchmarkConditionalFast(signer *RequestSigner, ctx context.Context) error {
	_, err := signer.SignConditionalObjectRequest(
		ctx,
		http.MethodGet,
		signerConditionalBenchmarkBucket,
		signerConditionalBenchmarkKey,
		requestSignerTestAccessKey,
		requestSignerTestSecretKey,
		"",
		0,
		signerConditionalBenchmarkRange,
	)
	return err
}

// runFourWorkerBenchmark runs exactly four benchmark workers, independent of
// GOMAXPROCS. The callback receives the worker index, worker count, and the
// aggregate iteration count so it can keep one logical operation per iteration.
func runFourWorkerBenchmark(b *testing.B, work func(worker, workers, iterations int) error) {
	b.Helper()

	const workers = 4
	b.StopTimer()
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	var firstErr error
	var errOnce sync.Once

	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer done.Done()
			ready.Done()
			<-start
			if err := work(worker, workers, b.N); err != nil {
				errOnce.Do(func() { firstErr = err })
			}
		}(worker)
	}

	ready.Wait()
	b.ResetTimer()
	b.StartTimer()
	close(start)
	done.Wait()
	b.StopTimer()
	if firstErr != nil {
		b.Fatal(firstErr)
	}
}

// BenchmarkRequestSignerConditional measures the range-only conditional
// workload at the block fan-out sizes used by the cache path.
func BenchmarkRequestSignerConditional(b *testing.B) {
	for _, count := range []int{1, 2, 4, 32} {
		count := count
		b.Run("Serial/"+strconv.Itoa(count), func(b *testing.B) {
			signer := newSignerBenchmark()
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				for range count {
					if err := benchmarkConditionalFast(signer, ctx); err != nil {
						b.Fatal(err)
					}
				}
			}
		})

		b.Run("Parallel/"+strconv.Itoa(count), func(b *testing.B) {
			signer := newSignerBenchmark()
			ctx := context.Background()
			b.ReportAllocs()
			runFourWorkerBenchmark(b, func(worker, workers, iterations int) error {
				for i := worker; i < iterations; i += workers {
					for range count {
						if err := benchmarkConditionalFast(signer, ctx); err != nil {
							return err
						}
					}
				}
				return nil
			})
		})
	}
}
