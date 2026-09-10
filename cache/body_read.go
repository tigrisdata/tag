package cache

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
)

// bodyReadProgressWriter records non-empty chunks handed to the destination
// without changing the writer's return values. The timestamp is relative to one stream so the
// watchdog does not depend on wall-clock adjustments. The watchdog pauses while a non-empty
// destination write is in flight: time blocked on a slow client is not cache-read idleness.
type bodyReadProgressWriter struct {
	writer  io.Writer
	started time.Time

	lastProgress atomic.Int64
	// inFlight is the number of non-empty destination writes in progress. -1 is
	// reserved for a watchdog that has won the idle-expiry race, so a new write
	// cannot enter after cancellation is committed.
	inFlight        atomic.Int32
	waitingForWrite atomic.Bool
	writeDone       chan struct{}
}

func newBodyReadProgressWriter(writer io.Writer) *bodyReadProgressWriter {
	return &bodyReadProgressWriter{
		writer:    writer,
		started:   time.Now(),
		writeDone: make(chan struct{}, 1),
	}
}

// beginWrite linearizes a non-empty destination write against watchdog expiry.
func (w *bodyReadProgressWriter) beginWrite() bool {
	for {
		state := w.inFlight.Load()
		if state < 0 {
			return false
		}
		if w.inFlight.CompareAndSwap(state, state+1) {
			return true
		}
	}
}

func (w *bodyReadProgressWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return w.writer.Write(p)
	}

	if !w.beginWrite() {
		// The watchdog already canceled the stream. Preserve the destination
		// writer's behavior for any late cache-client callback without reviving
		// the progress state.
		return w.writer.Write(p)
	}

	n, err := w.writer.Write(p)
	w.finishWrite()
	return n, err
}

func (w *bodyReadProgressWriter) finishWrite() {
	remaining := w.inFlight.Add(-1)
	if remaining != 0 {
		return
	}

	// Start the next idle interval after the destination has accepted the
	// cache chunk. The time spent inside Write was cache progress, not a
	// stalled cache read.
	w.lastProgress.Store(time.Since(w.started).Nanoseconds())

	// Wake a watchdog that is waiting for an in-flight write to finish. The
	// state is authoritative; a buffered, coalesced notification avoids making
	// every cache chunk wait for the watchdog goroutine.
	if w.waitingForWrite.Load() {
		select {
		case w.writeDone <- struct{}{}:
		default:
		}
	}
}

// idleState returns the current watchdog state. When a write is in flight, the
// idle duration is intentionally not computed because the watchdog must wait for
// that write to finish before starting a new cache-read interval.
func (w *bodyReadProgressWriter) idleState(now time.Time, timeout time.Duration, cancel func()) (inFlight bool, remaining time.Duration, expired bool) {
	state := w.inFlight.Load()
	if state > 0 {
		return true, 0, false
	}
	if state < 0 {
		return false, 0, true
	}

	elapsed := now.Sub(w.started) - time.Duration(w.lastProgress.Load())
	if elapsed < timeout {
		return false, timeout - elapsed, false
	}

	// Serialize expiry with beginWrite. If a non-empty Write entered first,
	// the compare-and-swap fails and the timer will observe that write instead
	// of canceling a healthy stream.
	if !w.inFlight.CompareAndSwap(0, -1) {
		if w.inFlight.Load() > 0 {
			return true, 0, false
		}
		return false, 0, true
	}
	cancel()
	return false, 0, true
}

// waitForWrite blocks a watchdog that found an active destination write until
// that write (or the stream) finishes. The state recheck closes the race where
// the write completes just before the wait begins and therefore has no need to
// send a notification.
func (w *bodyReadProgressWriter) waitForWrite(stop <-chan struct{}, ctx context.Context) bool {
	w.waitingForWrite.Store(true)
	state := w.inFlight.Load()
	if state <= 0 {
		w.waitingForWrite.Store(false)
		return state == 0
	}

	select {
	case <-w.writeDone:
	case <-stop:
		w.waitingForWrite.Store(false)
		return false
	case <-ctx.Done():
		w.waitingForWrite.Store(false)
		return false
	}
	w.waitingForWrite.Store(false)
	return true
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
						if !progressWriter.waitForWrite(stopWatchdog, streamCtx) {
							return
						}
						continue
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
