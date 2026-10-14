package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

var errServiceWorkerOwnership = errors.New("service worker ownership violated")

type heldCacheWriteClient struct {
	cacheclient.CacheClient
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	exited   chan struct{}
}

func (c *heldCacheWriteClient) PutStream(ctx context.Context, _ string, _ io.Reader, _ int64) error {
	close(c.entered)
	<-ctx.Done()
	close(c.canceled)
	<-c.release
	close(c.exited)
	return ctx.Err()
}

func TestStopWaitsForWriterAfterFetchSupervisorTimesOut(t *testing.T) {
	if err := runStopWaitsForWriterAfterFetchSupervisorTimesOut(t); err != nil {
		t.Fatal(err)
	}
}

func runStopWaitsForWriterAfterFetchSupervisorTimesOut(t *testing.T) error {
	var proofErr error
	synctest.Test(t, func(t *testing.T) {
		const (
			bucket = "lifecycle-bucket"
			key    = "lifecycle-key"
			body   = "small cached body"
		)

		cfg := config.NewDefault()
		cfg.Cache.SetBlockCachingEnabled(false)
		cfg.Cache.MaxConcurrentWrites = 1
		cfg.Cache.MaxPopulateMemoryBytes = 8 << 20

		client := &heldCacheWriteClient{
			CacheClient: cacheclient.NewMemoryCache(),
			entered:     make(chan struct{}),
			canceled:    make(chan struct{}),
			release:     make(chan struct{}),
			exited:      make(chan struct{}),
		}
		store := cache.NewCacheWithClient(client, &cfg.Cache)
		forwarder := &mockForwarder{
			doFullObjectFunc: func(context.Context, string, string, string, string) (*http.Response, error) {
				return cacheableGetResponse(body, `"lifecycle-etag"`), nil
			},
		}
		svc := NewService(forwarder, store, cfg)
		var releaseOnce sync.Once
		releaseWriter := func() { releaseOnce.Do(func() { close(client.release) }) }
		defer svc.Stop()
		defer releaseWriter()

		svc.triggerBackgroundCacheFetch(bucket, key, "access", "secret", false, priorityReadMiss, 0)
		select {
		case <-client.entered:
			t.Log("worker admission observed at embedded PutStream")
		case <-time.After(time.Second):
			proofErr = errors.New("background populate did not reach the controlled cache writer")
			return
		}

		// The direct cache writer has a source-defined 60-second minimum while
		// the supervisor waits on that context. Synctest advances virtual time,
		// so the supervisor can time out while the held client remains active.
		time.Sleep(cacheWriteTimeout + time.Second)
		synctest.Wait()
		if _, active := svc.activeBackgroundFetches.Load(backgroundFetchKey(bucket, key, 0)); active {
			proofErr = errors.New("background-fetch supervisor did not leave after its writer timeout")
			return
		}
		select {
		case <-client.canceled:
		case <-time.After(time.Second):
			proofErr = errors.New("cache writer did not observe its source-defined timeout")
			return
		}
		t.Log("held PutStream observed context cancellation from its timeout")
		t.Logf("fetch supervisor timed out after %s while the admitted PutStream remained active", cacheWriteTimeout)
		if got := len(svc.cacheSemaphore); got == 0 {
			proofErr = fmt.Errorf("%w: cache-populate slot released while writer is held", errServiceWorkerOwnership)
			return
		} else if got != 1 {
			proofErr = fmt.Errorf("cache-populate slots while writer is held = %d, want 1", got)
			return
		}
		svc.populateBudget.mu.Lock()
		remaining := svc.populateBudget.remaining
		svc.populateBudget.mu.Unlock()
		wantRemaining := cfg.Cache.MaxPopulateMemoryBytes - backgroundPopulateWriterBufferBytes()
		if remaining > wantRemaining {
			proofErr = fmt.Errorf("%w: populate budget reservation was released while writer is held", errServiceWorkerOwnership)
			return
		} else if remaining < wantRemaining {
			proofErr = fmt.Errorf("populate budget remaining = %d, want %d while writer is held", remaining, wantRemaining)
			return
		}
		t.Logf("while PutStream is held: cache slots=%d, populate bytes reserved=%d", len(svc.cacheSemaphore), cfg.Cache.MaxPopulateMemoryBytes-remaining)

		stopDone := make(chan struct{}, 2)
		go func() {
			svc.Stop()
			stopDone <- struct{}{}
		}()
		go func() {
			svc.Stop()
			stopDone <- struct{}{}
		}()
		select {
		case <-svc.workers.ctx.Done():
		case <-time.After(time.Second):
			proofErr = fmt.Errorf("%w: Stop did not cancel the service lifetime", errServiceWorkerOwnership)
			return
		}
		synctest.Wait()

		if svc.startBackgroundTask(nil, func(context.Context) {}) {
			proofErr = fmt.Errorf("%w: service admitted detached work after Stop began", errServiceWorkerOwnership)
			return
		}
		t.Log("Stop canceled the service lifetime and rejected a later task admission")
		select {
		case <-stopDone:
			proofErr = fmt.Errorf("%w: Stop returned while the cache writer still owned its reservation", errServiceWorkerOwnership)
			return
		default:
		}
		t.Log("both concurrent Stop calls remain blocked while PutStream is held")

		releaseWriter()
		select {
		case <-client.exited:
		case <-time.After(time.Second):
			proofErr = errors.New("held cache writer did not exit after release")
			return
		}
		t.Log("inner PutStream exited after its controlled release")
		<-stopDone
		<-stopDone
		svc.populateBudget.mu.Lock()
		remaining = svc.populateBudget.remaining
		svc.populateBudget.mu.Unlock()
		if remaining != cfg.Cache.MaxPopulateMemoryBytes {
			proofErr = fmt.Errorf("%w: populate budget remaining after Stop = %d, want %d", errServiceWorkerOwnership, remaining, cfg.Cache.MaxPopulateMemoryBytes)
			return
		}
		if got := len(svc.cacheSemaphore); got != 0 {
			proofErr = fmt.Errorf("%w: cache-populate slots after Stop = %d, want 0", errServiceWorkerOwnership, got)
			return
		}
		t.Logf("both Stop calls returned after writer exit; cache slots=%d, populate bytes reserved=%d", len(svc.cacheSemaphore), cfg.Cache.MaxPopulateMemoryBytes-remaining)
	})
	return proofErr
}
