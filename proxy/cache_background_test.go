package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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

type blockingTombstoneGetClient struct {
	cacheclient.CacheClient
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingTombstoneGetClient) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "tomb|") {
		c.once.Do(func() { close(c.started) })
		select {
		case <-c.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return c.CacheClient.Get(ctx, key)
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

func TestTriggerBackgroundWarm_DoesNotWaitForTombstoneRead(t *testing.T) {
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	client := &blockingTombstoneGetClient{
		CacheClient: cacheclient.NewMemoryCache(),
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	cacheStore := cache.NewCacheWithClient(client, &cfg.Cache)
	fetchDone := make(chan struct{})
	forwarder := &mockForwarder{
		doFullObjectFunc: func(_ context.Context, _, _, _, _ string) (*http.Response, error) {
			close(fetchDone)
			return cacheableGetResponse("body", `"etag"`), nil
		},
	}
	svc := NewService(forwarder, cacheStore, cfg)

	triggerDone := make(chan struct{})
	go func() {
		svc.triggerBackgroundCacheFetchAfterInvalidation(
			"background-bucket", "blocked-tombstone-read", "access", "secret", false,
			priorityWarmWrite, invalidationEpoch{at: 1, order: 1},
		)
		close(triggerDone)
	}()

	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("detached warm did not reach the tombstone read")
	}
	select {
	case <-triggerDone:
	case <-time.After(time.Second):
		t.Fatal("warm trigger waited for the tombstone read")
	}
	close(client.release)
	select {
	case <-fetchDone:
	case <-time.After(time.Second):
		t.Fatal("detached warm did not continue after the tombstone read")
	}
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
		<-time.After(time.Millisecond)
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

// gatedBackgroundBody holds the first background response in the cache writer so a
// successful write can invalidate it before its tombstone-aware metadata commit.
type gatedBackgroundBody struct {
	io.Reader
	release  <-chan struct{}
	started  chan<- struct{}
	startOne sync.Once
	closeOne sync.Once
	onClose  func()
}

func (b *gatedBackgroundBody) Read(p []byte) (int, error) {
	b.startOne.Do(func() { close(b.started) })
	<-b.release
	return b.Reader.Read(p)
}

func (b *gatedBackgroundBody) Close() error {
	b.closeOne.Do(b.onClose)
	return nil
}

// A read-miss background fetch that began before a successful PUT must not consume
// the PUT's tee fallback. The old fetch is tombstone-skipped, then exactly one
// latest warm runs serially and publishes the current ETag/body with its credentials.
func TestBackgroundFetch_QueuesLatestWarmAfterWriteInvalidation(t *testing.T) {
	const (
		bucket  = "background-race-bucket"
		key     = "background-race-key"
		oldBody = "old-body"
		newBody = "new-body"
	)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	var (
		calls         atomic.Int32
		originGets    atomic.Int32
		concurrent    atomic.Int32
		maxConcurrent atomic.Int32
		puts          atomic.Int32
		firstRead     = make(chan struct{})
		releaseOld    = make(chan struct{})
		replacement   = make(chan struct{})
		headDone      = make(chan struct{})
		headOnce      sync.Once
	)
	var (
		credsMu       sync.Mutex
		replacementAK string
		replacementSK string
	)
	updateMax := func(now int32) {
		for {
			old := maxConcurrent.Load()
			if now <= old || maxConcurrent.CompareAndSwap(old, now) {
				return
			}
		}
	}

	mock := &teeMockForwarder{
		mockForwarder: &mockForwarder{
			conditionalResp: headResp(`"head-etag"`, "text/plain", int64(len("put-body"))),
			doRequestFunc: func(_ context.Context, _ *http.Request, _, _ string) (*http.Response, error) {
				originGets.Add(1)
				return cacheableGetResponse(newBody, `"new-etag"`), nil
			},
			doFullObjectFunc: func(_ context.Context, _, _, accessKey, secretKey string) (*http.Response, error) {
				call := calls.Add(1)
				now := concurrent.Add(1)
				updateMax(now)
				onClose := func() { concurrent.Add(-1) }
				if call == 1 {
					resp := cacheableGetResponse(oldBody, `"old-etag"`)
					resp.Body = &gatedBackgroundBody{
						Reader:  resp.Body,
						release: releaseOld,
						started: firstRead,
						onClose: onClose,
					}
					return resp, nil
				}
				if call == 2 {
					close(replacement)
					credsMu.Lock()
					replacementAK, replacementSK = accessKey, secretKey
					credsMu.Unlock()
					resp := cacheableGetResponse(newBody, `"new-etag"`)
					resp.Body = &gatedBackgroundBody{
						Reader:  resp.Body,
						release: closedSignal(),
						started: make(chan struct{}),
						onClose: onClose,
					}
					return resp, nil
				}
				return nil, errors.New("unexpected replacement fetch")
			},
		},
		teeFunc:  teeUpstream(&puts, `"put-etag"`),
		headHook: func() { headOnce.Do(func() { close(headDone) }) },
	}
	cacheStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	svc := NewService(mock, cacheStore, cfg)
	svc.config.Cache.WarmOnWrite = true
	svc.config.Cache.SizeThreshold = 1 << 20
	svc.config.Cache.BlockSize = 1 << 20

	svc.triggerBackgroundCacheFetch(bucket, key, "old-access", "old-secret", false, priorityReadMiss)
	select {
	case <-firstRead:
	case <-time.After(time.Second):
		t.Fatal("old background fetch did not reach the cache body")
	}

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(bucket, key, "put-body")); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}
	select {
	case <-headDone:
	case <-time.After(time.Second):
		t.Fatal("tee fallback did not issue its HEAD")
	}

	bcastKey := "bg:" + bucket + "/" + key
	var state *backgroundFetchState
	deadline := time.Now().Add(time.Second)
	for {
		actual, loaded := svc.activeBackgroundFetches.Load(bcastKey)
		if loaded {
			if candidate, ok := actual.(*backgroundFetchState); ok {
				candidate.mu.Lock()
				pending := candidate.pending != nil
				candidate.mu.Unlock()
				if pending {
					state = candidate
					break
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("write fallback did not become the pending background warm")
		}
		<-time.After(time.Millisecond)
	}
	if state == nil {
		t.Fatal("pending background warm state was not retained")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("background fetches before releasing stale owner = %d, want 1", got)
	}

	// The newest write can reach the detached trigger before an older write whose
	// fallback goroutine was delayed. The pending slot follows invalidation order,
	// not trigger arrival order, so the older credentials cannot replace it.
	state.mu.Lock()
	pendingInvalidatedAt := state.pending.invalidatedAt
	state.mu.Unlock()
	newestInvalidatedAt := pendingInvalidatedAt + 2
	olderInvalidatedAt := pendingInvalidatedAt + 1
	svc.triggerBackgroundCacheFetchAfterInvalidation(
		bucket, key, "latest-access", "latest-secret", false, priorityWarmWrite, invalidationEpoch{at: newestInvalidatedAt},
	)
	svc.triggerBackgroundCacheFetchAfterInvalidation(
		bucket, key, "older-access", "older-secret", false, priorityWarmWrite, invalidationEpoch{at: olderInvalidatedAt},
	)
	select {
	case <-replacement:
		t.Fatal("replacement warm started while the stale owner was still active")
	default:
	}
	close(releaseOld)

	select {
	case <-replacement:
	case <-time.After(time.Second):
		t.Fatal("latest replacement warm did not start")
	}
	if !metaCached(cacheStore, bucket, key, time.Second) {
		t.Fatal("replacement warm did not publish metadata")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("background fetches = %d, want one stale owner plus one replacement", got)
	}
	if got := maxConcurrent.Load(); got != 1 {
		t.Fatalf("concurrent background fetches = %d, want 1", got)
	}
	credsMu.Lock()
	gotAK, gotSK := replacementAK, replacementSK
	credsMu.Unlock()
	if gotAK != "latest-access" || gotSK != "latest-secret" {
		t.Fatalf("replacement credentials = %q/%q, want latest-access/latest-secret", gotAK, gotSK)
	}

	meta, found, err := cacheStore.GetMeta(context.Background(), bucket, key)
	if err != nil || !found {
		t.Fatalf("final metadata found=%v err=%v", found, err)
	}
	if meta.ETag != `"new-etag"` {
		t.Fatalf("final ETag = %q, want %q", meta.ETag, `"new-etag"`)
	}
	var body bytes.Buffer
	if err := cacheStore.GetBodyStream(context.Background(), bucket, key, meta.ETag, &body); err != nil {
		t.Fatalf("final body: %v", err)
	}
	if body.String() != newBody {
		t.Fatalf("final body = %q, want %q", body.String(), newBody)
	}

	readReq := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	readReq.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request, Signature=deadbeef")
	readW := httptest.NewRecorder()
	if err := svc.HandleGetObject(readW, readReq); err != nil {
		t.Fatalf("post-write HandleGetObject: %v", err)
	}
	if got := readW.Header().Get(XCacheHeader); got != XCacheHit {
		t.Fatalf("post-write X-Cache = %q, want %q", got, XCacheHit)
	}
	if got := readW.Body.String(); got != newBody {
		t.Fatalf("post-write GET body = %q, want %q", got, newBody)
	}
	if got := originGets.Load(); got != 0 {
		t.Fatalf("post-write origin GETs = %d, want 0", got)
	}
}

func TestDeleteInvalidationPublishesWarmOrderOnlyAfterSuccess(t *testing.T) {
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	svc, cacheStore := newBackgroundCacheService(t, cfg, func() *http.Response {
		return cacheableGetResponse("body", `"etag"`)
	})
	ctx := context.Background()
	const bucket, key = "delete-order-bucket", "delete-order-key"

	write := svc.invalidateObjectBeforeWrite(ctx, bucket, key)
	write = svc.invalidateObjectWithOrder(ctx, bucket, key, write.order)
	warm := backgroundFetchRequest{invalidatedAt: write.at, invalidationOrder: write.order}

	deleteOrder := svc.invalidateObjectBeforeWrite(ctx, bucket, key)
	if svc.backgroundWarmSuperseded(bucket, key, warm) {
		t.Fatal("pre-successful delete superseded the prior warm")
	}
	if got := cacheStore.GetTombstoneOrder(ctx, bucket, key); got != write.order {
		t.Fatalf("pre-successful delete tombstone order = %d, want %d", got, write.order)
	}

	svc.invalidateObjectWithOrder(ctx, bucket, key, deleteOrder.order)
	if !svc.backgroundWarmSuperseded(bucket, key, warm) {
		t.Fatal("successful delete did not supersede the prior warm")
	}
}

func TestBackgroundFetch_DelayedOlderWarmBlockedByTombstoneOrder(t *testing.T) {
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	var calls atomic.Int32
	svc, cacheStore := newBackgroundCacheService(t, cfg, func() *http.Response {
		calls.Add(1)
		return cacheableGetResponse("body", `"etag"`)
	})
	ctx := context.Background()
	const bucket, key = "background-order-bucket", "background-order-key"
	if err := cacheStore.WriteTombstoneWithOrder(ctx, bucket, key, 2); err != nil {
		t.Fatalf("WriteTombstoneWithOrder: %v", err)
	}

	// Model a warm retained by an active owner before a newer write invalidated the
	// key. The handoff must recheck the durable order; checking only metadata would
	// start this old-credential request after the newer write.
	bcastKey := "bg:" + bucket + "/" + key
	state := &backgroundFetchState{
		pending: &backgroundFetchRequest{
			accessKey:         "old-access",
			secretKey:         "old-secret",
			prio:              priorityWarmWrite,
			invalidatedAt:     time.Now().UnixNano(),
			invalidationOrder: 1,
		},
	}
	svc.activeBackgroundFetches.Store(bcastKey, state)
	request, ok := svc.nextBackgroundFetch(bcastKey, bucket, key, state)
	if ok {
		t.Fatalf("stale pending warm was promoted with credentials %q/%q", request.accessKey, request.secretKey)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("delayed older warm started %d fetches, want 0", got)
	}
	if _, loaded := svc.activeBackgroundFetches.Load(bcastKey); loaded {
		t.Fatal("delayed older warm left an active marker")
	}
}

func TestNextBackgroundFetch_AnonymousWarmRequiresPublicMetadata(t *testing.T) {
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	const (
		bucket = "background-bucket"
		key    = "anonymous-visibility"
	)

	svc, cacheStore := newBackgroundCacheService(t, cfg, func() *http.Response {
		return cacheableGetResponse("replacement", `"replacement-etag"`)
	})
	if err := cacheStore.PutWithMeta(
		context.Background(),
		bucket,
		key,
		&cache.CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"signed-etag"`, ACL: "private"},
		[]byte("signed-body"),
		0,
	); err != nil {
		t.Fatalf("seed private metadata: %v", err)
	}

	bcastKey := "bg:" + bucket + "/" + key
	state := &backgroundFetchState{
		pending: &backgroundFetchRequest{
			anonymous:     true,
			prio:          priorityWarmWrite,
			invalidatedAt: 1,
		},
	}
	svc.activeBackgroundFetches.Store(bcastKey, state)
	request, ok := svc.nextBackgroundFetch(bcastKey, bucket, key, state)
	if !ok {
		t.Fatal("anonymous warm was suppressed by private metadata")
	}
	if !request.anonymous {
		t.Fatal("replacement request lost anonymous authorization")
	}
	state.mu.Lock()
	state.closed = true
	state.mu.Unlock()
	svc.activeBackgroundFetches.CompareAndDelete(bcastKey, state)
}

func closedSignal() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
