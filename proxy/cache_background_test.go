package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

func newBackgroundCacheService(t *testing.T, cfg *config.Config, response func() *http.Response) (*Service, *cache.Cache) {
	t.Helper()

	cacheStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	forwarder := &mockForwarder{
		doFullObjectFunc: func(_ context.Context, _, _, _, _ string) (*http.Response, error) {
			return response(), nil
		},
	}
	return NewService(forwarder, cacheStore, cfg), cacheStore
}

type countingReadCloser struct {
	io.Reader
	readBytes int
	closed    bool
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.readBytes += n
	return n, err
}

func (r *countingReadCloser) Close() error {
	r.closed = true
	return nil
}

type partialFailStreamClient struct {
	cacheclient.CacheClient
	readBytes int
}

func (c *partialFailStreamClient) PutStream(_ context.Context, _ string, r io.Reader, _ int64) error {
	buf := make([]byte, c.readBytes)
	_, _ = io.ReadFull(r, buf)
	return errors.New("injected stream write failure")
}

func TestFetchFullObjectToCache_DrainsUncacheableBody(t *testing.T) {
	const body = "body without an etag"
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	var source *countingReadCloser
	svc, _ := newBackgroundCacheService(t, cfg, func() *http.Response {
		resp := cacheableGetResponse(body, `"ignored-etag"`)
		resp.Header.Del("ETag")
		source = &countingReadCloser{Reader: resp.Body}
		resp.Body = source
		return resp
	})

	if err := svc.fetchFullObjectToCache(context.Background(), "background-bucket", "no-etag", "access", "secret", false, priorityReadMiss); err != nil {
		t.Fatalf("fetchFullObjectToCache: %v", err)
	}
	if source == nil {
		t.Fatal("background response was not created")
	}
	if source.readBytes != len(body) {
		t.Fatalf("uncacheable body read %d bytes, want %d", source.readBytes, len(body))
	}
	if !source.closed {
		t.Fatal("uncacheable response body was not closed")
	}
}

func TestFetchFullObjectToCache_DrainsBodyAfterCacheWriteFailure(t *testing.T) {
	const body = "body after a partial cache write failure"
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	baseClient := cacheclient.NewMemoryCache()
	cacheStore := cache.NewCacheWithClient(&partialFailStreamClient{CacheClient: baseClient, readBytes: 7}, &cfg.Cache)
	var source *countingReadCloser
	forwarder := &mockForwarder{
		doFullObjectFunc: func(_ context.Context, _, _, _, _ string) (*http.Response, error) {
			resp := cacheableGetResponse(body, `"partial-failure-etag"`)
			source = &countingReadCloser{Reader: resp.Body}
			resp.Body = source
			return resp, nil
		},
	}
	svc := NewService(forwarder, cacheStore, cfg)

	if err := svc.fetchFullObjectToCache(context.Background(), "background-bucket", "partial-failure", "access", "secret", false, priorityReadMiss); err == nil {
		t.Fatal("partial cache write failure was swallowed")
	}
	if source == nil {
		t.Fatal("background response was not created")
	}
	if source.readBytes != len(body) {
		t.Fatalf("body after cache failure read %d bytes, want %d", source.readBytes, len(body))
	}
	if !source.closed {
		t.Fatal("failed response body was not closed")
	}
}

func TestFetchFullObjectToCache_DrainsBodyWhenBlockScratchUnavailable(t *testing.T) {
	const bodySize = 2 << 20
	cfg := config.NewDefault()
	cfg.Cache.SizeThreshold = 8 << 20
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 1 << 20
	cfg.Cache.MaxPopulateMemoryBytes = backgroundPopulateWriterBufferBytes() + int64(cfg.Cache.BlockSize) - 1
	body := strings.Repeat("x", bodySize)
	var source *countingReadCloser
	svc, _ := newBackgroundCacheService(t, cfg, func() *http.Response {
		resp := cacheableGetResponse(body, `"block-scratch-etag"`)
		source = &countingReadCloser{Reader: resp.Body}
		resp.Body = source
		return resp
	})

	err := svc.fetchFullObjectToCache(context.Background(), "background-bucket", "block-scratch-unavailable", "access", "secret", false, priorityReadMiss)
	if !errors.Is(err, errCachePopulateDeclined) {
		t.Fatalf("fetchFullObjectToCache error = %v, want errCachePopulateDeclined", err)
	}
	if source == nil {
		t.Fatal("background response was not created")
	}
	if source.readBytes != len(body) {
		t.Fatalf("body with unavailable block scratch read %d bytes, want %d", source.readBytes, len(body))
	}
	if !source.closed {
		t.Fatal("response body with unavailable block scratch was not closed")
	}
}

func TestFetchFullObjectToCache_WritesWholeBodyDirectly(t *testing.T) {
	const (
		bucket = "background-bucket"
		key    = "whole-key"
		body   = "background whole body"
		etag   = `"background-whole-etag"`
	)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	svc, cacheStore := newBackgroundCacheService(t, cfg, func() *http.Response {
		return cacheableGetResponse(body, etag)
	})

	if err := svc.fetchFullObjectToCache(context.Background(), bucket, key, "access", "secret", false, priorityReadMiss); err != nil {
		t.Fatalf("fetchFullObjectToCache: %v", err)
	}

	meta, found, err := cacheStore.GetMeta(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if !found {
		t.Fatal("whole-object metadata was not written")
	}
	if meta.BlockSize != 0 {
		t.Fatalf("whole-object metadata BlockSize = %d, want 0", meta.BlockSize)
	}

	var got bytes.Buffer
	if err := cacheStore.GetBodyStream(context.Background(), bucket, key, meta.ETag, &got); err != nil {
		t.Fatalf("GetBodyStream: %v", err)
	}
	if got.String() != body {
		t.Errorf("cached body = %q, want %q", got.String(), body)
	}
}

func TestFetchFullObjectToCache_SmallWholeObjectFitsWithoutBlockScratch(t *testing.T) {
	const (
		bucket = "background-bucket"
		key    = "small-whole-under-block-budget"
		body   = "small whole body"
	)

	cfg := config.NewDefault()
	cfg.Cache.SizeThreshold = 1 << 20
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 32 << 20
	cfg.Cache.MaxPopulateMemoryBytes = 5 << 20
	previousPoolCap := maxPooledBlockBufBytes.Load()
	t.Cleanup(func() { maxPooledBlockBufBytes.Store(previousPoolCap) })
	svc, cacheStore := newBackgroundCacheService(t, cfg, func() *http.Response {
		return cacheableGetResponse(body, `"small-whole-etag"`)
	})

	if err := svc.fetchFullObjectToCache(context.Background(), bucket, key, "access", "secret", false, priorityReadMiss); err != nil {
		t.Fatalf("fetchFullObjectToCache: %v", err)
	}
	meta, found, err := cacheStore.GetMeta(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if !found {
		t.Fatal("small whole-object warm was rejected despite fitting the writer reservation")
	}
	if meta.BlockSize != 0 {
		t.Fatalf("small whole-object metadata BlockSize = %d, want 0", meta.BlockSize)
	}
}

func TestFetchFullObjectToCache_WarmWaitsForBlockScratchWithoutHoldingCountSlot(t *testing.T) {
	const (
		bucket = "background-bucket"
		key    = "warm-block-waits-for-scratch"
	)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 2 << 20
	cfg.Cache.MaxConcurrentWrites = 2
	cfg.Cache.MaxPopulateMemoryBytes = 20 << 20
	body := strings.Repeat("x", 2<<20)
	started := make(chan struct{})
	svc, cacheStore := newBackgroundCacheService(t, cfg, func() *http.Response {
		close(started)
		return cacheableGetResponse(body, `"warm-block-etag"`)
	})

	readMissWeight := int64(15 << 20)
	if !svc.acquireCacheSlot(context.Background(), readMissWeight, priorityReadMiss) {
		t.Fatal("setup: read-miss should reserve the pressure portion of the budget")
	}
	readMissOwned := true
	defer func() {
		if readMissOwned {
			svc.releaseCacheSlot(readMissWeight)
		}
	}()

	warmDone := make(chan error, 1)
	go func() {
		warmDone <- svc.fetchFullObjectToCache(context.Background(), bucket, key, "access", "secret", false, priorityWarmWrite)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("warm did not reach the origin")
	}

	// The warm has selected block mode and is waiting for its complete reservation.
	// Its initial count slot must be free while the read-miss holds the bytes.
	deadline := time.Now().Add(time.Second)
	for {
		svc.populateBudget.mu.Lock()
		pendingWarm := svc.populateBudget.pendingWarm
		svc.populateBudget.mu.Unlock()
		if pendingWarm >= int64(6<<20) {
			select {
			case svc.cacheSemaphore <- struct{}{}:
				<-svc.cacheSemaphore
			default:
				t.Fatal("warm scratch wait retained its count slot")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("warm did not wait for its combined block reservation")
		}
		time.Sleep(time.Millisecond)
	}

	// Let the pressure reservation go. The warm's pending combined reservation should
	// acquire and publish the complete block representation.
	svc.releaseCacheSlot(readMissWeight)
	readMissOwned = false
	select {
	case err := <-warmDone:
		if err != nil {
			t.Fatalf("warm fetch: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("warm did not complete after scratch became available")
	}
	if _, found, err := cacheStore.GetMeta(context.Background(), bucket, key); err != nil || !found {
		t.Fatalf("warm block metadata missing: found=%v err=%v", found, err)
	}
}

func TestFetchFullObjectToCache_WritesBlockBodyDirectly(t *testing.T) {
	const (
		bucket = "background-bucket"
		key    = "block-key"
		body   = "ABCDEFGH"
		etag   = `"background-block-etag"`
	)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	svc, cacheStore := newBackgroundCacheService(t, cfg, func() *http.Response {
		return cacheableGetResponse(body, etag)
	})

	if err := svc.fetchFullObjectToCache(context.Background(), bucket, key, "access", "secret", false, priorityReadMiss); err != nil {
		t.Fatalf("fetchFullObjectToCache: %v", err)
	}

	meta, found, err := cacheStore.GetMeta(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if !found {
		t.Fatal("block-mode metadata was not written")
	}
	if meta.BlockSize != 4 || !meta.BlocksComplete {
		t.Fatalf("block metadata = BlockSize %d, BlocksComplete %t; want 4, true", meta.BlockSize, meta.BlocksComplete)
	}

	var got bytes.Buffer
	for idx := int64(0); idx < 2; idx++ {
		var block bytes.Buffer
		if err := cacheStore.GetBlockRangeStream(context.Background(), bucket, key, meta.ETag, meta.BlockSize, idx, 0, 3, &block); err != nil {
			t.Fatalf("GetBlockRangeStream block %d: %v", idx, err)
		}
		_, _ = io.Copy(&got, &block)
	}
	if got.String() != body {
		t.Errorf("cached blocks = %q, want %q", got.String(), body)
	}
}

func TestFetchFullObjectToCache_DetachedWriteSurvivesFetchCancellation(t *testing.T) {
	const (
		bucket = "background-bucket"
		key    = "canceled-key"
		body   = "body after fetch cancellation"
	)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	var cancel context.CancelFunc
	svc, cacheStore := newBackgroundCacheService(t, cfg, func() *http.Response {
		cancel()
		return cacheableGetResponse(body, `"canceled-etag"`)
	})

	ctx, cancelContext := context.WithCancel(context.Background())
	cancel = cancelContext
	if err := svc.fetchFullObjectToCache(ctx, bucket, key, "access", "secret", false, priorityReadMiss); err != nil {
		t.Fatalf("fetchFullObjectToCache: %v", err)
	}
	if _, found, err := cacheStore.GetMeta(context.Background(), bucket, key); err != nil || !found {
		t.Fatalf("detached cache write did not publish metadata: found=%v err=%v", found, err)
	}
}

func TestFetchFullObjectToCache_TombstoneBlocksDirectWrite(t *testing.T) {
	const (
		bucket = "background-bucket"
		key    = "tombstone-key"
		body   = "stale background body"
	)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	var cacheStore *cache.Cache
	svc, cacheStore := newBackgroundCacheService(t, cfg, func() *http.Response {
		if err := cacheStore.WriteTombstone(context.Background(), bucket, key); err != nil {
			t.Fatalf("WriteTombstone: %v", err)
		}
		return cacheableGetResponse(body, `"stale-etag"`)
	})

	if err := svc.fetchFullObjectToCache(context.Background(), bucket, key, "access", "secret", false, priorityReadMiss); err != nil {
		t.Fatalf("fetchFullObjectToCache: %v", err)
	}
	if _, found, err := cacheStore.GetMeta(context.Background(), bucket, key); err != nil {
		t.Fatalf("GetMeta: %v", err)
	} else if found {
		t.Fatal("direct cache write bypassed a newer tombstone")
	}
}
