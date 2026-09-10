package cache

import (
	"context"
	"io"
	"sync"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
)

// bodyReadProgressWriter records non-empty chunks handed to the destination
// without changing the writer's return values. The timestamp is relative to one stream so the
// watchdog does not depend on wall-clock adjustments. The watchdog pauses while a non-empty
// destination write is in flight: time blocked on a slow client is not cache-read idleness.
type bodyReadProgressWriter struct {
	writer    io.Writer
	idleSince time.Time

	mu        sync.Mutex
	inFlight  int
	writeDone chan struct{}
}

func newBodyReadProgressWriter(writer io.Writer) *bodyReadProgressWriter {
	return &bodyReadProgressWriter{
		writer:    writer,
		idleSince: time.Now(),
		writeDone: make(chan struct{}, 1),
	}
}

func (w *bodyReadProgressWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return w.writer.Write(p)
	}

	w.mu.Lock()
	w.inFlight++
	w.mu.Unlock()
	defer w.finishWrite()

	return w.writer.Write(p)
}

func (w *bodyReadProgressWriter) finishWrite() {
	w.mu.Lock()
	w.inFlight--
	if w.inFlight == 0 {
		// Start the next idle interval after the destination has accepted the
		// cache chunk. The time spent inside Write was cache progress, not a
		// stalled cache read.
		w.idleSince = time.Now()
	}
	w.mu.Unlock()

	// Wake a watchdog that is waiting for an in-flight write to finish. The
	// state is authoritative; a buffered, coalesced notification avoids making
	// every cache chunk wait for the watchdog goroutine.
	select {
	case w.writeDone <- struct{}{}:
	default:
	}
}

// idleState returns the current watchdog state. When a write is in flight, the
// idle duration is intentionally not computed because the watchdog must wait for
// that write to finish before starting a new cache-read interval.
func (w *bodyReadProgressWriter) idleState(now time.Time, timeout time.Duration, cancel func()) (inFlight bool, remaining time.Duration, expired bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.inFlight > 0 {
		return true, 0, false
	}

	elapsed := now.Sub(w.idleSince)
	if elapsed >= timeout {
		// Serialize expiry with Write's inFlight transition. If a non-empty
		// Write entered first, the check above observes it and does not cancel.
		cancel()
		return false, 0, true
	}
	return false, timeout - elapsed, false
}

// streamWithIdleTimeout gives one cache read a child context and cancels it when
// the cache has not produced a non-empty write to the destination within timeout.
// The request context remains the parent cancellation signal.
func streamWithIdleTimeout(
	ctx context.Context,
	writer io.Writer,
	timeout time.Duration,
	stream func(context.Context, io.Writer) error,
) (err error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	progressWriter := newBodyReadProgressWriter(writer)
	stopWatchdog := make(chan struct{})
	watchdogDone := make(chan struct{})

	go func() {
		defer close(watchdogDone)

		timer := time.NewTimer(timeout)
		defer timer.Stop()

		for {
			select {
			case <-stopWatchdog:
				return
			case <-streamCtx.Done():
				return
			case <-timer.C:
				for {
					inFlight, remaining, expired := progressWriter.idleState(time.Now(), timeout, cancel)
					if expired {
						return
					}
					if inFlight {
						// A destination write is evidence that the cache already
						// produced data. Wait for it rather than canceling a healthy
						// stream because the client is slow.
						select {
						case <-progressWriter.writeDone:
							continue
						case <-stopWatchdog:
							return
						case <-streamCtx.Done():
							return
						}
					}
					if remaining <= 0 {
						continue
					}
					timer.Reset(remaining)
					break
				}
			}
		}
	}()

	// Stop and join the watchdog even if a cache client returns through a panic.
	// The named error still returns the cache client's exact result on the normal path.
	defer func() {
		close(stopWatchdog)
		cancel()
		<-watchdogDone
	}()
	return stream(streamCtx, progressWriter)
}

func streamBodyWithIdleTimeout(
	ctx context.Context,
	client cacheclient.CacheClient,
	key string,
	writer io.Writer,
	timeout time.Duration,
) error {
	return streamWithIdleTimeout(ctx, writer, timeout, func(streamCtx context.Context, streamWriter io.Writer) error {
		return client.GetStream(streamCtx, key, streamWriter)
	})
}

func streamRangeWithIdleTimeout(
	ctx context.Context,
	client cacheclient.CacheClient,
	key string,
	start, end int64,
	writer io.Writer,
	timeout time.Duration,
) error {
	return streamWithIdleTimeout(ctx, writer, timeout, func(streamCtx context.Context, streamWriter io.Writer) error {
		return client.GetRangeStream(streamCtx, key, start, end, streamWriter)
	})
}
