package handlers

import (
	"fmt"
	"testing"
	"time"
)

const handlerPrefetchWindow = 32

type handlerPrefetchResponse struct {
	status      int
	cacheStatus string
	body        []byte
	err         error
}

func waitForHandlerPrefetchStarts(t *testing.T, started <-chan struct{}, want int) {
	t.Helper()
	for i := 0; i < want; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for cache read %d of %d", i+1, want)
		}
	}
}

// The normal Router path must start all bounded remote reads before the first
// client write releases the gate. This enters handleObject, HandleGetObject,
// metadata lookup, range probing, and streamBlockRange rather than calling the
// stream helper directly.
func TestHandleGetObjectBlockPrefetchesRemoteReads(t *testing.T) {
	fixture := newHandlerPrefetchBenchmarkFixture(t, true, handlerPrefetchWindow+1)
	started := make(chan struct{}, handlerPrefetchWindow)
	gate := make(chan struct{})
	fixture.trace.reset()
	fixture.controller.configure(0, gate, started)
	fixture.pacer.configure(0)

	result := make(chan handlerPrefetchResponse, 1)
	go func() {
		status, cacheStatus, body, err := fixture.getRange()
		result <- handlerPrefetchResponse{status: status, cacheStatus: cacheStatus, body: body, err: err}
	}()

	waitForHandlerPrefetchStarts(t, started, handlerPrefetchWindow)
	if got := fixture.trace.peak(); got != handlerPrefetchWindow {
		t.Fatalf("concurrent remote GetRangeStream calls = %d, want %d", got, handlerPrefetchWindow)
	}
	close(gate)

	select {
	case response := <-result:
		fixture.requireResponse(t, response.status, response.cacheStatus, response.body, response.err)
		t.Logf("handler-path remote GetRangeStream spans: %s", fixture.trace)
	case <-time.After(2 * time.Second):
		t.Fatal("handler-path prefetch serve did not complete")
	}
}

// All benchmark dimensions must continue to reach the ordinary cached range
// path. Gating each full-block read makes the bounded overlap deterministic;
// paced response writes still receive the same ordered bytes.
func TestHandleGetObjectBlockPrefetchMatrix(t *testing.T) {
	for _, locality := range []struct {
		name   string
		remote bool
	}{
		{name: "local", remote: false},
		{name: "remote", remote: true},
	} {
		t.Run(locality.name, func(t *testing.T) {
			for _, blocks := range []int{2, 4, 8, 16, 32} {
				t.Run(fmt.Sprintf("blocks=%d", blocks), func(t *testing.T) {
					for _, writer := range []struct {
						name  string
						delay time.Duration
					}{
						{name: "fast_client"},
						{name: "slow_client", delay: handlerPrefetchBenchmarkReadDelay},
					} {
						t.Run(writer.name, func(t *testing.T) {
							fixture := newHandlerPrefetchBenchmarkFixture(t, locality.remote, blocks)
							want := blocks
							if want > handlerPrefetchWindow {
								want = handlerPrefetchWindow
							}
							started := make(chan struct{}, want)
							gate := make(chan struct{})
							fixture.trace.reset()
							fixture.controller.configure(0, gate, started)
							fixture.pacer.configure(writer.delay)

							results := make(chan handlerPrefetchResponse, 1)
							go func() {
								status, cacheStatus, body, err := fixture.getRange()
								results <- handlerPrefetchResponse{status: status, cacheStatus: cacheStatus, body: body, err: err}
							}()
							waitForHandlerPrefetchStarts(t, started, want)
							if got := fixture.trace.peak(); got != want {
								t.Fatalf("concurrent cache reads = %d, want %d", got, want)
							}
							close(gate)
							select {
							case response := <-results:
								fixture.requireResponse(t, response.status, response.cacheStatus, response.body, response.err)
							case <-time.After(2 * time.Second):
								t.Fatal("handler-path prefetch serve did not complete")
							}
						})
					}
				})
			}
		})
	}
}
