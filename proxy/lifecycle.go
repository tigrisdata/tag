package proxy

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// stopJoinWarnAfter bounds how long Stop joins admitted work silently. The join
// itself has no deadline: it matches main's HTTP drain timeout so a shutdown
// that outlives both is logged once rather than appearing hung.
const stopJoinWarnAfter = 30 * time.Second

// serviceWorkers owns work that may outlive an HTTP handler. The mutex serializes
// admission with Stop: after stopping is set, no caller can add to wg while Stop
// waits on it.
type serviceWorkers struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	stopping bool
	wg       sync.WaitGroup
	active   atomic.Int64  // admitted tasks not yet done; reported when Stop's join runs long
	stopped  chan struct{} // closed once the first stop call has joined every task
}

func newServiceWorkers() *serviceWorkers {
	ctx, cancel := context.WithCancel(context.Background())
	return &serviceWorkers{ctx: ctx, cancel: cancel, stopped: make(chan struct{})}
}

// begin admits one unit of service-owned work. parent may add an operation's
// own cancellation (for example an HTTP request); the service lifetime always
// cancels the returned context as well. The caller must call done exactly once.
func (w *serviceWorkers) begin(parent context.Context) (ctx context.Context, done func(), ok bool) {
	if parent == nil {
		parent = w.ctx
	}

	w.mu.Lock()
	if w.stopping {
		w.mu.Unlock()
		return nil, nil, false
	}
	w.wg.Add(1)
	w.active.Add(1)
	w.mu.Unlock()

	ctx, cancel := context.WithCancel(parent)
	stopServiceCancel := context.AfterFunc(w.ctx, cancel)
	done = func() {
		stopServiceCancel()
		cancel()
		w.active.Add(-1)
		w.wg.Done()
	}
	return ctx, done, true
}

// goTask admits and starts one service-owned task. Descendant work that can
// outlive this function must be admitted separately; joined children may remain
// under this task's ownership.
func (w *serviceWorkers) goTask(parent context.Context, work func(context.Context)) bool {
	ctx, done, ok := w.begin(parent)
	if !ok {
		return false
	}
	go func() {
		defer done()
		work(ctx)
	}()
	return true
}

// stop seals admission before canceling and waiting. Every concurrent caller
// observes the same cancellation and waits for the same admitted work to exit;
// the caller that sealed admission performs the join, so its deadline log is
// emitted once. Later callers wait on a channel rather than a mutex so a
// synctest bubble sees them as durably blocked.
func (w *serviceWorkers) stop() {
	w.mu.Lock()
	first := !w.stopping
	w.stopping = true
	w.mu.Unlock()

	if first {
		w.cancel()
		w.join()
		close(w.stopped)
	}
	<-w.stopped
}

// join waits for every admitted task without a bound, logging once when the
// wait passes stopJoinWarnAfter and once more when it then completes, so an
// operator can tell a slow cache writer from a hung shutdown.
func (w *serviceWorkers) join() {
	joined := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(joined)
	}()

	deadline := time.NewTimer(stopJoinWarnAfter)
	defer deadline.Stop()
	select {
	case <-joined:
		return
	case <-deadline.C:
	}
	joinStart := w.warnJoinPastDeadline()
	<-joined
	log.Info().Dur("waited", time.Since(joinStart)).Msg("Detached cache work finished after the join deadline")
}

// warnJoinPastDeadline logs once when admitted tasks are still running past
// stopJoinWarnAfter, since stop then waits for them without a bound. It returns
// when the unbounded wait began.
func (w *serviceWorkers) warnJoinPastDeadline() time.Time {
	log.Warn().
		Dur("deadline", stopJoinWarnAfter).
		Int64("active_tasks", w.active.Load()).
		Msg("Service worker join deadline passed; shutdown is waiting past the deadline for detached cache work to finish")
	return time.Now()
}

// Stop cancels the service lifetime and waits until every admitted task has
// exited. Call it after HTTP request draining and before closing the cache
// client. It has no deadline: closing storage while an uncooperative cache call
// is still active would violate the service's resource-ownership boundary. A
// join that outlives stopJoinWarnAfter is logged once with the outstanding
// task count, and again when it completes.
func (s *Service) Stop() {
	if s.workers != nil {
		s.workers.stop()
	}
}

func (s *Service) startBackgroundTask(parent context.Context, work func(context.Context)) bool {
	return s.workers != nil && s.workers.goTask(parent, work)
}

func (s *Service) beginBackgroundTask(parent context.Context) (context.Context, func(), bool) {
	if s.workers == nil {
		return nil, nil, false
	}
	return s.workers.begin(parent)
}
