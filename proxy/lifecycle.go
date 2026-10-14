package proxy

import (
	"context"
	"sync"
)

// serviceWorkers owns work that may outlive an HTTP handler. The mutex serializes
// admission with Stop: after stopping is set, no caller can add to wg while Stop
// waits on it.
type serviceWorkers struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	stopping bool
	wg       sync.WaitGroup
}

func newServiceWorkers() *serviceWorkers {
	ctx, cancel := context.WithCancel(context.Background())
	return &serviceWorkers{ctx: ctx, cancel: cancel}
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
	w.mu.Unlock()

	ctx, cancel := context.WithCancel(parent)
	stopServiceCancel := context.AfterFunc(w.ctx, cancel)
	done = func() {
		stopServiceCancel()
		cancel()
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
// observes the same cancellation and waits for the same admitted work to exit.
func (w *serviceWorkers) stop() {
	w.mu.Lock()
	w.stopping = true
	w.mu.Unlock()

	w.cancel()
	w.wg.Wait()
}

// Stop cancels the service lifetime and waits until every admitted task has
// exited. Call it after HTTP request draining and before closing the cache
// client. It has no deadline: closing storage while an uncooperative cache call
// is still active would violate the service's resource-ownership boundary.
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
