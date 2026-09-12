package proxy

import (
	"context"
	"net/http"
	"sync"
	"testing"
)

const (
	conditionalSigningBenchmarkAccessKey = "AKIAIOSFODNN7EXAMPLE"
	conditionalSigningBenchmarkSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

type conditionalSigningBenchmarkTransport struct{}

func (conditionalSigningBenchmarkTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusPartialContent,
		Header:     make(http.Header),
		Body:       http.NoBody,
	}, nil
}

func newConditionalSigningBenchmarkForwarder() *baseForwarder {
	forwarder := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
	forwarder.httpClient = &http.Client{Transport: conditionalSigningBenchmarkTransport{}}
	return &forwarder
}

// BenchmarkBaseForwarderConditionalRequestCompletion measures the controlled
// completion of one synthetic range request through the production forwarder
// entry point. The transport returns immediately, so the result includes
// request construction, signing, and client/transport bookkeeping but no
// upstream latency or response body work.
func BenchmarkBaseForwarderConditionalRequestCompletion(b *testing.B) {
	forwarder := newConditionalSigningBenchmarkForwarder()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		resp, err := forwarder.DoConditionalGetRequest(
			ctx,
			"benchmark-bucket",
			"object/with space+and%25/key",
			conditionalSigningBenchmarkAccessKey,
			conditionalSigningBenchmarkSecretKey,
			"",
			0,
			"bytes=0-1048575",
		)
		if err != nil {
			b.Fatal(err)
		}
		if err := resp.Body.Close(); err != nil {
			b.Fatal(err)
		}
	}
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

// BenchmarkBaseForwarderConditionalRequestCompletionParallel measures the same
// controlled range request with exactly four workers.
func BenchmarkBaseForwarderConditionalRequestCompletionParallel(b *testing.B) {
	forwarder := newConditionalSigningBenchmarkForwarder()
	ctx := context.Background()
	b.ReportAllocs()
	runFourWorkerBenchmark(b, func(worker, workers, iterations int) error {
		for i := worker; i < iterations; i += workers {
			resp, err := forwarder.DoConditionalGetRequest(
				ctx,
				"benchmark-bucket",
				"object/with space+and%25/key",
				conditionalSigningBenchmarkAccessKey,
				conditionalSigningBenchmarkSecretKey,
				"",
				0,
				"bytes=0-1048575",
			)
			if err != nil {
				return err
			}
			if err := resp.Body.Close(); err != nil {
				return err
			}
		}
		return nil
	})
}
