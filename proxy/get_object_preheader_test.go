package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

type preHeaderFailureTransport struct {
	started chan struct{}
	release chan struct{}
	err     error
	calls   atomic.Int32
}

func (t *preHeaderFailureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.calls.Add(1)
	t.started <- struct{}{}
	select {
	case <-t.release:
		return nil, t.err
	case <-r.Context().Done():
		return nil, r.Context().Err()
	}
}

type preHeaderWriteCounter struct {
	cacheclient.CacheClient
	writes atomic.Int32
}

func (c *preHeaderWriteCounter) Put(ctx context.Context, key string, data []byte, ttl int64) error {
	c.writes.Add(1)
	return c.CacheClient.Put(ctx, key, data, ttl)
}

func (c *preHeaderWriteCounter) PutIfVersion(ctx context.Context, key string, data []byte, ttl int64, expected uint64) (uint64, error) {
	c.writes.Add(1)
	return c.CacheClient.PutIfVersion(ctx, key, data, ttl, expected)
}

func (c *preHeaderWriteCounter) PutStream(ctx context.Context, key string, r io.Reader, ttl int64) error {
	c.writes.Add(1)
	return c.CacheClient.PutStream(ctx, key, r, ttl)
}

func (c *preHeaderWriteCounter) PutStreamIfVersion(ctx context.Context, key string, r io.Reader, ttl int64, expected uint64) (uint64, error) {
	c.writes.Add(1)
	return c.CacheClient.PutStreamIfVersion(ctx, key, r, ttl, expected)
}

func waitForBroadcastListeners(t *testing.T, b *broadcast.Broadcaster, count int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if got := b.ListenerCount(); got == count {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("broadcast listener count = %d, want %d", b.ListenerCount(), count)
		case <-ticker.C:
		}
	}
}

func TestPreHeaderTransportErrorUnblocksCoalescedGetters(t *testing.T) {
	const (
		bucket = "preheader-bucket"
		key    = "preheader-key"
	)

	transportErr := errors.New("injected upstream transport failure")
	transport := &preHeaderFailureTransport{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
		err:     transportErr,
	}
	var releaseOnce sync.Once
	releaseTransport := func() { releaseOnce.Do(func() { close(transport.release) }) }
	defer releaseTransport()

	cfg := config.NewDefault()
	client := &preHeaderWriteCounter{CacheClient: cacheclient.NewMemoryCache()}
	cacheStore := cache.NewCacheWithClient(client, &cfg.Cache)
	forwarder := newTransparentForwarderForTest(transport)
	service := NewService(forwarder, cacheStore, cfg)

	type result struct {
		err error
		ctx context.Context
	}
	results := make(chan result, 2)
	cancels := make([]context.CancelFunc, 2)
	for i := range cancels {
		ctx, cancel := context.WithCancel(context.Background())
		cancels[i] = cancel
		request := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil).WithContext(ctx)
		go func(ctx context.Context, request *http.Request) {
			err := service.HandleGetObject(httptest.NewRecorder(), request)
			results <- result{err: err, ctx: ctx}
		}(ctx, request)
	}
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("first full GET did not reach the upstream transport")
	}

	broadcaster, isFirst := service.broadcastManager.GetOrCreate(makeBroadcastKey(bucket, key, ""))
	if isFirst {
		t.Fatal("full GET did not register its broadcast before the upstream request")
	}
	waitForBroadcastListeners(t, broadcaster, 2)
	if transport.calls.Load() != 1 {
		t.Fatalf("upstream transport calls before release = %d, want 1", transport.calls.Load())
	}

	releaseTransport()

	var firstErr error
	for i := 0; i < 2; i++ {
		select {
		case got := <-results:
			if got.ctx.Err() != nil {
				t.Errorf("GET %d context ended before its service call returned: %v", i, got.ctx.Err())
			}
			if !errors.Is(got.err, transportErr) {
				t.Errorf("GET %d error = %v, want original transport failure %v", i, got.err, transportErr)
			}
			if i == 0 {
				firstErr = got.err
			} else if got.err != firstErr {
				t.Errorf("coalesced GET error = %v, want the same broadcast error %v", got.err, firstErr)
			}
		case <-time.After(time.Second):
			t.Errorf("GET %d did not return after the upstream transport failed while its context remained live", i)
			releaseTransport()
			for _, cancel := range cancels {
				cancel()
			}
			for j := i; j < 2; j++ {
				<-results
			}
			return
		}
	}

	if got := transport.calls.Load(); got != 1 {
		t.Errorf("coalesced full GETs made %d upstream transport calls, want 1", got)
	}
	if got := service.broadcastManager.ActiveCount(); got != 0 {
		t.Errorf("active broadcasts after the initiating GET returned = %d, want 0", got)
	}
	if got := client.writes.Load(); got != 0 {
		t.Errorf("cache writes after a pre-header transport failure = %d, want 0", got)
	}
	if _, found, err := cacheStore.GetMeta(context.Background(), bucket, key); err != nil || found {
		t.Errorf("cache metadata after a pre-header transport failure: found=%v err=%v, want no entry", found, err)
	}
}
