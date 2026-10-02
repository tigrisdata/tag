package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// remoteMissTrace records the cross-node waterfall without timing-based
// synchronization. Tests use its ordering as a regression guard and log the
// completed trace for a readable miss-path timeline.
type remoteMissTrace struct {
	mu     sync.Mutex
	events []string
}

func (t *remoteMissTrace) add(event string) {
	t.mu.Lock()
	t.events = append(t.events, event)
	t.mu.Unlock()
}

func (t *remoteMissTrace) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.events...)
}

func (t *remoteMissTrace) requireOrder(tb testing.TB, want ...string) {
	tb.Helper()
	events := t.snapshot()
	next := 0
	for _, event := range events {
		if next < len(want) && event == want[next] {
			next++
		}
	}
	if next != len(want) {
		tb.Fatalf("waterfall = %s, missing ordered sequence %s", strings.Join(events, " -> "), strings.Join(want, " -> "))
	}
}

// gatedOwnerBlockClient models either a local or a remote embedded owner. It
// holds PutBlockBytes until the test releases it, retaining the caller's slice
// meanwhile so buffer ownership and write detachment can be observed directly.
type gatedOwnerBlockClient struct {
	cacheclient.CacheClient
	local bool
	trace *remoteMissTrace

	getCalls  atomic.Int64
	blockPuts atomic.Int32

	putStarted  chan struct{}
	allowPut    chan struct{}
	putFinished chan struct{}

	mu             sync.Mutex
	received       []byte
	putErr         error
	presenceReads  map[string]int
	afterProbeRead func(context.Context, string, error)
}

func newGatedOwnerBlockClient(base cacheclient.CacheClient, local bool, trace *remoteMissTrace) *gatedOwnerBlockClient {
	return &gatedOwnerBlockClient{
		CacheClient:   base,
		local:         local,
		trace:         trace,
		putStarted:    make(chan struct{}, 1),
		allowPut:      make(chan struct{}),
		putFinished:   make(chan struct{}, 1),
		presenceReads: make(map[string]int),
	}
}

func (c *gatedOwnerBlockClient) IsLocal(string) bool { return c.local }

func (c *gatedOwnerBlockClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if c.getCalls.Add(1) == 1 {
		c.trace.add("cache-miss-probe")
	} else {
		c.trace.add("cache-read")
	}
	err := c.CacheClient.GetRangeStream(ctx, key, start, end, w)
	if start == 0 && end == 1 {
		c.mu.Lock()
		c.presenceReads[key]++
		afterProbeRead := c.afterProbeRead
		c.mu.Unlock()
		if afterProbeRead != nil {
			afterProbeRead(ctx, key, err)
		}
	}
	return err
}

func (c *gatedOwnerBlockClient) PutBlockBytes(ctx context.Context, key string, data []byte, ttlSeconds int64) (bool, error) {
	c.blockPuts.Add(1)
	c.trace.add("remote-cache-put-start")
	select {
	case c.putStarted <- struct{}{}:
	default:
	}
	select {
	case <-c.allowPut:
	case <-ctx.Done():
		return true, ctx.Err()
	}

	c.mu.Lock()
	putErr := c.putErr
	if putErr == nil {
		c.received = append(c.received[:0], data...)
	}
	c.mu.Unlock()
	if putErr != nil {
		c.trace.add("remote-cache-put-finish")
		select {
		case c.putFinished <- struct{}{}:
		default:
		}
		return true, putErr
	}
	err := c.CacheClient.Put(ctx, key, data, ttlSeconds)
	c.trace.add("remote-cache-put-finish")
	select {
	case c.putFinished <- struct{}{}:
	default:
	}
	return true, err
}

func (c *gatedOwnerBlockClient) receivedBlock() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.received...)
}

func (c *gatedOwnerBlockClient) setAfterProbeRead(fn func(context.Context, string, error)) {
	c.mu.Lock()
	c.afterProbeRead = fn
	c.mu.Unlock()
}

func (c *gatedOwnerBlockClient) setPutError(err error) {
	c.mu.Lock()
	c.putErr = err
	c.mu.Unlock()
}

func (c *gatedOwnerBlockClient) presenceReadCount(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.presenceReads[key]
}

// tracedBlockForwarder adds the upstream range fetch to the test waterfall
// while reusing the normal block-cache response validation fixture.
type tracedBlockForwarder struct {
	*blockMockForwarder
	trace *remoteMissTrace
}

func (f *tracedBlockForwarder) DoConditionalGetRequest(ctx context.Context, bucket, key, accessKey, secretKey, etag string, modifiedSince int64, rangeHeader string) (*http.Response, error) {
	f.trace.add("upstream-fetch")
	return f.blockMockForwarder.DoConditionalGetRequest(ctx, bucket, key, accessKey, secretKey, etag, modifiedSince, rangeHeader)
}

func newGatedAssembledRangeService(t *testing.T, local bool) (*Service, *cache.Cache, *gatedOwnerBlockClient, *blockMockForwarder, *cache.CachedObjectMeta, *remoteMissTrace) {
	t.Helper()
	const (
		bucket = "remote-bucket"
		key    = "remote-key"
	)

	trace := &remoteMissTrace{}
	mock := newBlockMock([]byte("ABCDEFGH"), `"v1"`)
	forwarder := &tracedBlockForwarder{blockMockForwarder: mock, trace: trace}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	cfg.Cache.MaxConcurrentWrites = 1
	cfg.Cache.MaxPopulateMemoryBytes = -1 // isolate the count-bound detached writer in this fixture
	client := newGatedOwnerBlockClient(cacheclient.NewMemoryCache(), local, trace)
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	svc := NewService(forwarder, store, cfg)
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"v1"`,
		ContentLength: 8,
		StatusCode:    http.StatusOK,
		BlockSize:     4,
	}
	if wrote, err := store.PutMetaIfVersion(context.Background(), bucket, key, meta, 60, cache.VersionAny); err != nil || !wrote {
		t.Fatalf("seed block meta = (wrote=%t, err=%v)", wrote, err)
	}
	return svc, store, client, mock, meta, trace
}

type assembledRangeResult struct {
	w      *httptest.ResponseRecorder
	served bool
	err    error
}

func startGatedAssembledRange(svc *Service, meta *cache.CachedObjectMeta, trace *remoteMissTrace) <-chan assembledRangeResult {
	results := make(chan assembledRangeResult, 1)
	go func() {
		w := httptest.NewRecorder()
		served, err := svc.serveAssembledRange(context.Background(), w, meta.Bucket, meta.Key, "access", "secret", meta, byteRange{start: 0, end: 3}, time.Now())
		trace.add("response-committed")
		results <- assembledRangeResult{w: w, served: served, err: err}
	}()
	return results
}

func waitBlockSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitAssembledRange(t *testing.T, results <-chan assembledRangeResult) assembledRangeResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for assembled range response")
		return assembledRangeResult{}
	}
}

func activeBlockFetch(svc *Service, blockKey string) *blockFetchState {
	svc.blockFetchMu.Lock()
	defer svc.blockFetchMu.Unlock()
	return svc.blockFetches[blockKey]
}

type probeFirstRangeRequestIDKey struct{}

func startGatedProbeFirstRange(svc *Service, meta *cache.CachedObjectMeta) <-chan assembledRangeResult {
	return startGatedProbeFirstRangeWithContext(svc, meta, context.Background())
}

func startGatedProbeFirstRangeWithContext(svc *Service, meta *cache.CachedObjectMeta, ctx context.Context) <-chan assembledRangeResult {
	rangeHeader := fmt.Sprintf("bytes=0-%d", meta.ContentLength-1)
	return startGatedProbeFirstRequestWithContext(svc, meta, rangeHeader, ctx)
}

func startGatedProbeFirstRequestWithContext(svc *Service, meta *cache.CachedObjectMeta, rangeHeader string, ctx context.Context) <-chan assembledRangeResult {
	results := make(chan assembledRangeResult, 1)
	go func() {
		w := httptest.NewRecorder()
		req := fullGet(meta.Bucket, meta.Key).WithContext(ctx)
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}
		err := svc.HandleGetObject(w, req)
		results <- assembledRangeResult{w: w, served: w.Code == http.StatusPartialContent, err: err}
	}()
	return results
}

type gatedBlockFetchFailureForwarder struct {
	*tracedBlockForwarder
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *gatedBlockFetchFailureForwarder) DoConditionalGetRequest(ctx context.Context, _, _, _, _, _ string, _ int64, _ string) (*http.Response, error) {
	f.tracedBlockForwarder.blockMockForwarder.blockGets.Add(1)
	f.once.Do(func() { close(f.started) })
	select {
	case <-f.release:
		return nil, errors.New("simulated upstream block fetch failure")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func requireProbeFirstRange(t testing.TB, result assembledRangeResult) {
	t.Helper()
	if result.err != nil || !result.served {
		t.Fatalf("HandleGetObject range = (served=%t, err=%v)", result.served, result.err)
	}
	if result.w.Code != http.StatusPartialContent || result.w.Body.String() != "ABCDEFGH" {
		t.Fatalf("response = (%d, %q), want (206, ABCDEFGH)", result.w.Code, result.w.Body.String())
	}
	if got := result.w.Header().Get("X-Cache"); got != XCacheHit {
		t.Errorf("X-Cache=%q, want HIT", got)
	}
	if got := result.w.Header().Get("ETag"); got != `"v1"` {
		t.Errorf("ETag=%q, want %q", got, `"v1"`)
	}
	if got := result.w.Header().Get("Content-Range"); got != "bytes 0-7/8" {
		t.Errorf("Content-Range=%q, want %q", got, "bytes 0-7/8")
	}
}

func waitForBlockFetchConsumers(t testing.TB, svc *Service, blockKey string, want int) {
	t.Helper()
	timeout := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if state := activeBlockFetch(svc, blockKey); state != nil {
			state.mu.Lock()
			consumers := state.consumers
			state.mu.Unlock()
			if consumers >= want {
				return
			}
		}
		select {
		case <-ticker.C:
		case <-timeout:
			t.Fatalf("block fetch %q did not reach %d consumers", blockKey, want)
		}
	}
}

// A remote-owner miss must return as soon as the aligned upstream block has
// been validated. The trace proves that no post-put cache reread sits between
// the upstream fetch and the committed 206, while the blocked writer proves the
// client does not wait for remote persistence.
func TestServeAssembledRange_RemoteMissServesValidatedBytesBeforeCacheWrite(t *testing.T) {
	svc, store, client, mock, meta, trace := newGatedAssembledRangeService(t, false)
	results := startGatedAssembledRange(svc, meta, trace)

	// Do not wait for the writer to be scheduled first: it is intentionally a
	// separate goroutine and may start before or after this response completes.
	// The blocked PutBlockBytes call is the durability boundary the response must
	// not wait on.
	result := waitAssembledRange(t, results)
	if result.err != nil || !result.served {
		t.Fatalf("serveAssembledRange = (served=%t, err=%v)", result.served, result.err)
	}
	if result.w.Code != http.StatusPartialContent || result.w.Body.String() != "ABCD" {
		t.Fatalf("response = (%d, %q), want (206, ABCD)", result.w.Code, result.w.Body.String())
	}
	if got := client.getCalls.Load(); got != 1 {
		t.Fatalf("remote cache reads before response = %d, want 1 (the initial miss probe only)", got)
	}
	if got := mock.blockGets.Load(); got != 1 {
		t.Fatalf("upstream block fetches = %d, want 1", got)
	}
	waitBlockSignal(t, client.putStarted, "remote cache put start")

	blockKey := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0)
	state := activeBlockFetch(svc, blockKey)
	if state == nil {
		t.Fatal("detached remote write was removed before it completed")
	}
	state.mu.Lock()
	bufferRetained := state.bufp != nil && !state.cacheFinished && state.consumers == 0
	state.mu.Unlock()
	if !bufferRetained {
		t.Fatal("validated block buffer was not retained by the detached writer after the response")
	}

	close(client.allowPut)
	waitBlockSignal(t, client.putFinished, "remote cache put finish")
	select {
	case <-state.cacheDone:
	case <-time.After(2 * time.Second):
		t.Fatal("detached remote write did not release its fetch state")
	}
	state.mu.Lock()
	bufferReleased := state.bufp == nil
	state.mu.Unlock()
	if !bufferReleased {
		t.Fatal("pooled block buffer remained owned after both writer and assembler completed")
	}
	trace.requireOrder(t, "cache-miss-probe", "upstream-fetch", "response-committed", "remote-cache-put-finish")
	t.Logf("remote range-miss waterfall: %s", strings.Join(trace.snapshot(), " -> "))

	if !store.BlockExists(context.Background(), meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0) {
		t.Fatal("remote owner was not populated after the detached write completed")
	}
	if got := client.receivedBlock(); !bytes.Equal(got, []byte("ABCD")) {
		t.Fatalf("remote writer received %q, want ABCD", got)
	}
}

// Overlapping assembled requests keep joining the validated state while its
// remote write is pending, rather than refetching bytes the first request has
// already validated but not yet made durable at the owner.
func TestServeAssembledRange_RemoteMissCoalescesUntilWriteFinishes(t *testing.T) {
	svc, _, client, mock, meta, _ := newGatedAssembledRangeService(t, false)
	first := startGatedAssembledRange(svc, meta, client.trace)

	waitBlockSignal(t, client.putStarted, "first remote cache put start")
	if result := waitAssembledRange(t, first); result.err != nil || !result.served {
		t.Fatalf("first serveAssembledRange = (served=%t, err=%v)", result.served, result.err)
	}
	second := startGatedAssembledRange(svc, meta, client.trace)
	if result := waitAssembledRange(t, second); result.err != nil || !result.served {
		t.Fatalf("second serveAssembledRange = (served=%t, err=%v)", result.served, result.err)
	}
	if got := mock.blockGets.Load(); got != 1 {
		t.Fatalf("upstream block fetches for overlapping remote misses = %d, want 1", got)
	}
	if got := client.getCalls.Load(); got != 2 {
		t.Fatalf("remote cache reads for overlapping misses = %d, want 2 (one initial probe per request)", got)
	}
	select {
	case <-client.putStarted:
		t.Fatal("overlapping request started a second remote cache write")
	default:
	}

	close(client.allowPut)
	waitBlockSignal(t, client.putFinished, "remote cache put finish")
}

// A detached writer owns the cache-populate slot until its remote put completes,
// so a stalled peer cannot turn a burst of assembled misses into unbounded work.
func TestServeAssembledRange_RemoteWriterHoldsPopulateSlot(t *testing.T) {
	svc, _, client, mock, meta, _ := newGatedAssembledRangeService(t, false)
	results := startGatedAssembledRange(svc, meta, client.trace)

	waitBlockSignal(t, client.putStarted, "remote cache put start")
	if err := svc.fetchOneBlock(context.Background(), meta.Bucket, meta.Key, "access", "secret", meta, 1); !errors.Is(err, errCachePopulateDeclined) {
		t.Fatalf("second block fetch while remote writer is stalled = %v, want %v", err, errCachePopulateDeclined)
	}
	if got := mock.blockGets.Load(); got != 1 {
		t.Fatalf("bounded writer allowed a second upstream fetch: got %d, want 1", got)
	}

	close(client.allowPut)
	waitBlockSignal(t, client.putFinished, "remote cache put finish")
	result := waitAssembledRange(t, results)
	if result.err != nil || !result.served {
		t.Fatalf("serveAssembledRange = (served=%t, err=%v)", result.served, result.err)
	}
}

// Local ownership intentionally retains the old durability boundary: the 206
// cannot commit until the local cache write finishes, even though it uses the
// staged bytes rather than doing a post-write reread.
func TestServeAssembledRange_LocalOwnerWaitsForCacheWrite(t *testing.T) {
	svc, store, client, _, meta, _ := newGatedAssembledRangeService(t, true)
	results := startGatedAssembledRange(svc, meta, client.trace)

	waitBlockSignal(t, client.putStarted, "local cache put start")
	select {
	case result := <-results:
		t.Fatalf("local response completed before its cache write: served=%t err=%v", result.served, result.err)
	default:
	}

	close(client.allowPut)
	result := waitAssembledRange(t, results)
	if result.err != nil || !result.served || result.w.Body.String() != "ABCD" {
		t.Fatalf("local assembled response = (served=%t, err=%v, body=%q), want successful ABCD", result.served, result.err, result.w.Body.String())
	}
	if !store.BlockExists(context.Background(), meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0) {
		t.Fatal("local block was not durable before the response completed")
	}
}

// A delete that lands while a remote writer is detached may leave an unreachable
// versioned block, but it must never restore metadata past the tombstone.
func TestServeAssembledRange_RemoteWriteRespectsTombstoneVisibility(t *testing.T) {
	svc, store, client, _, meta, _ := newGatedAssembledRangeService(t, false)
	results := startGatedAssembledRange(svc, meta, client.trace)

	waitBlockSignal(t, client.putStarted, "remote cache put start")
	result := waitAssembledRange(t, results)
	if result.err != nil || !result.served {
		t.Fatalf("serveAssembledRange = (served=%t, err=%v)", result.served, result.err)
	}
	if err := store.Delete(context.Background(), meta.Bucket, meta.Key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	close(client.allowPut)
	waitBlockSignal(t, client.putFinished, "remote cache put finish")

	if _, found, err := store.GetMeta(context.Background(), meta.Bucket, meta.Key); err != nil || found {
		t.Fatalf("metadata after fenced detached write = (found=%t, err=%v), want absent", found, err)
	}
	// The invalidation left a fence: a writer holding pre-delete state cannot
	// recreate the entry (the ordering the tombstone timestamp used to assert).
	preDeleteToken := meta // any stale identity; commit with expected=0-era token must lose
	_ = preDeleteToken
	staleMeta := *meta
	if wrote, err := store.PutMetaIfVersion(context.Background(), meta.Bucket, meta.Key, &staleMeta, 60, 1); err != nil {
		t.Fatalf("PutMetaIfVersion: %v", err)
	} else if wrote {
		t.Fatal("stale pre-delete write recreated the entry over the fence")
	}
}

// Validation remains before both handoff and persistence: a short 206 body
// cannot be served from memory or left behind as a block after a remote miss.
func TestServeAssembledRange_RemoteMissRejectsShortValidatedBlock(t *testing.T) {
	svc, store, client, mock, meta, _ := newGatedAssembledRangeService(t, false)
	mock.blockGetShortBody = true

	w := httptest.NewRecorder()
	served, err := svc.serveAssembledRange(context.Background(), w, meta.Bucket, meta.Key, "access", "secret", meta, byteRange{start: 0, end: 3}, time.Now())
	if served || err == nil {
		t.Fatalf("short body serve = (served=%t, err=%v), want pre-commit failure", served, err)
	}
	select {
	case <-client.putStarted:
		t.Fatal("short upstream body reached the remote cache writer")
	default:
	}
	if store.BlockExists(context.Background(), meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0) {
		t.Fatal("short upstream body was persisted as a complete block")
	}
}

func releaseGatedBlockPut(client *gatedOwnerBlockClient) {
	select {
	case <-client.allowPut:
	default:
		close(client.allowPut)
	}
}

func TestProbeFirstRangeWaitsForLocalAndRemoteCacheWrites(t *testing.T) {
	for _, owner := range []struct {
		name  string
		local bool
	}{
		{name: "local", local: true},
		{name: "remote", local: false},
	} {
		t.Run(owner.name, func(t *testing.T) {
			svc, _, client, mock, meta, _ := newGatedAssembledRangeService(t, owner.local)
			t.Cleanup(func() { releaseGatedBlockPut(client) })
			block1Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 1)
			if err := client.CacheClient.Put(context.Background(), block1Key, []byte("EFGH"), 60); err != nil {
				t.Fatalf("seed second block: %v", err)
			}

			results := startGatedProbeFirstRange(svc, meta)
			waitBlockSignal(t, client.putStarted, "probe-first range cache put")
			if got := mock.blockGets.Load(); got != 1 {
				t.Fatalf("aligned upstream block GETs while put is gated = %d, want 1", got)
			}
			if got := client.blockPuts.Load(); got != 1 {
				t.Fatalf("block puts while waiting = %d, want 1", got)
			}
			select {
			case result := <-results:
				t.Fatalf("probe-first response completed before its cache write: served=%t err=%v", result.served, result.err)
			default:
			}

			block0Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0)
			if got := client.presenceReadCount(block0Key); got != 1 {
				t.Fatalf("byte-zero probes for missing block while put is gated = %d, want 1", got)
			}
			if got := client.presenceReadCount(block1Key); got != 1 {
				t.Fatalf("byte-zero probes for present block = %d, want 1", got)
			}

			releaseGatedBlockPut(client)
			waitBlockSignal(t, client.putFinished, "probe-first range cache put finish")
			result := waitAssembledRange(t, results)
			requireProbeFirstRange(t, result)
			if got := client.receivedBlock(); !bytes.Equal(got, []byte("ABCD")) {
				t.Fatalf("cache writer received %q, want ABCD", got)
			}
			if got := mock.forwards.Load(); got != 0 {
				t.Fatalf("client-range origin forwards = %d, want 0", got)
			}
		})
	}
}

func installLateSameETagBlockFill(t *testing.T, client *gatedOwnerBlockClient, meta *cache.CachedObjectMeta) (string, string, *atomic.Int32) {
	t.Helper()
	block0Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0)
	block1Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 1)
	if err := client.CacheClient.Put(context.Background(), block1Key, []byte("EFGH"), 60); err != nil {
		t.Fatalf("seed second block: %v", err)
	}

	var fills atomic.Int32
	fillDone := make(chan error, 1)
	var once sync.Once
	client.setAfterProbeRead(func(_ context.Context, key string, readErr error) {
		if key != block1Key {
			return
		}
		once.Do(func() {
			if readErr != nil {
				t.Errorf("second block presence read = %v, want a cache hit", readErr)
			}
			if got := client.presenceReadCount(block0Key); got != 1 {
				t.Errorf("target block reads before the racing fill = %d, want its one absent probe", got)
			}
			go func() {
				fills.Add(1)
				fillDone <- client.CacheClient.Put(context.Background(), block0Key, []byte("ABCD"), 60)
			}()
			select {
			case err := <-fillDone:
				if err != nil {
					t.Errorf("concurrent same-ETag block fill: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Error("concurrent block fill did not complete before fetch leadership")
			}
		})
	})
	return block0Key, block1Key, &fills
}

func TestProbeFirstRangeRacingSameETagFillUsesIdempotentWrite(t *testing.T) {
	svc, _, client, mock, meta, _ := newGatedAssembledRangeService(t, true)
	releaseGatedBlockPut(client)
	block0Key, block1Key, fills := installLateSameETagBlockFill(t, client, meta)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(meta.Bucket, meta.Key, "bytes=0-7")); err != nil {
		t.Fatalf("probe-first range after concurrent fill: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "ABCDEFGH" || w.Header().Get("X-Cache") != XCacheHit {
		t.Fatalf("racing fill response = (%d, cache=%q, %q), want (206, HIT, ABCDEFGH)", w.Code, w.Header().Get("X-Cache"), w.Body.String())
	}
	if got := w.Header().Get("Content-Range"); got != "bytes 0-7/8" {
		t.Errorf("Content-Range=%q, want bytes 0-7/8", got)
	}
	if got := w.Header().Get("ETag"); got != `"v1"` {
		t.Errorf("ETag=%q, want %q", got, `"v1"`)
	}
	if got := fills.Load(); got != 1 {
		t.Errorf("concurrent fills = %d, want 1", got)
	}
	if got := client.presenceReadCount(block0Key); got != 1 {
		t.Errorf("byte-zero probes for raced block = %d, want 1", got)
	}
	if got := client.presenceReadCount(block1Key); got != 1 {
		t.Errorf("byte-zero probes for present block = %d, want 1", got)
	}
	if got := mock.blockGets.Load(); got != 1 {
		t.Errorf("aligned origin block GETs after a concurrent fill = %d, want the one accepted idempotent fetch", got)
	}
	if got := client.blockPuts.Load(); got != 1 {
		t.Errorf("same-ETag cache puts after a concurrent fill = %d, want 1", got)
	}
	if got := client.receivedBlock(); !bytes.Equal(got, []byte("ABCD")) {
		t.Errorf("idempotent put bytes = %q, want ABCD", got)
	}
	if got := mock.forwards.Load(); got != 0 {
		t.Errorf("client-range origin forwards = %d, want 0", got)
	}
}

// Older scans may each elect one idempotent fetch after a same-ETag fill completes; active
// leaders still coalesce, and each confirmed-missing block remains bounded to one GET per leader.
func TestProbeFirstRangeOlderScansUseIdempotentSameETagPuts(t *testing.T) {
	for _, owner := range []struct {
		name  string
		local bool
	}{
		{name: "local", local: true},
		{name: "remote", local: false},
	} {
		for _, evict := range []bool{false, true} {
			eviction := "present"
			if evict {
				eviction = "evicted"
			}
			t.Run(owner.name+"/"+eviction, func(t *testing.T) {
				svc, _, client, mock, meta, _ := newGatedAssembledRangeService(t, owner.local)
				const staleScans = 3
				resume := make(chan struct{}, staleScans)
				t.Cleanup(func() {
					for i := 0; i < staleScans; i++ {
						select {
						case resume <- struct{}{}:
						default:
						}
					}
					releaseGatedBlockPut(client)
				})

				block0Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0)
				block1Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 1)
				if err := client.CacheClient.Put(context.Background(), block1Key, []byte("EFGH"), 60); err != nil {
					t.Fatalf("seed second block: %v", err)
				}

				paused := make(chan struct{}, staleScans)
				var pauseCount atomic.Int32
				client.setAfterProbeRead(func(_ context.Context, key string, _ error) {
					if key == block1Key && pauseCount.Add(1) <= staleScans {
						paused <- struct{}{}
						<-resume
					}
				})
				scanResults := make(chan assembledRangeResult, staleScans)
				for i := 0; i < staleScans; i++ {
					go func() {
						w := httptest.NewRecorder()
						err := svc.HandleGetObject(w, blockGet(meta.Bucket, meta.Key, "bytes=0-7"))
						scanResults <- assembledRangeResult{w: w, served: w.Code == http.StatusPartialContent, err: err}
					}()
				}
				for i := 0; i < staleScans; i++ {
					waitBlockSignal(t, paused, "scan to classify block 0 and pause on block 1")
				}
				if got := client.presenceReadCount(block0Key); got != staleScans {
					t.Fatalf("block 0 scans before the fill = %d, want %d", got, staleScans)
				}

				// The paused requests have each observed block 0 absent. Let a separate
				// ordinary request fill it and finish its local/remote cache write first.
				client.setAfterProbeRead(nil)
				filler := startGatedProbeFirstRange(svc, meta)
				waitBlockSignal(t, client.putStarted, "filler cache put")
				releaseGatedBlockPut(client)
				waitBlockSignal(t, client.putFinished, "filler cache put completion")
				requireProbeFirstRange(t, waitAssembledRange(t, filler))

				if evict {
					if err := client.CacheClient.Delete(context.Background(), block0Key); err != nil {
						t.Fatalf("evict filled block: %v", err)
					}
				}

				// Resume older scans serially. Each still acts on its confirmed absence and
				// may make one aligned GET; identical ETag-scoped bytes make each cache put idempotent.
				for i := 0; i < staleScans; i++ {
					resume <- struct{}{}
					if evict && i == 0 {
						waitBlockSignal(t, client.putStarted, "replacement cache put after eviction")
						waitBlockSignal(t, client.putFinished, "replacement cache put completion")
					}
					select {
					case result := <-scanResults:
						requireProbeFirstRange(t, result)
					case <-time.After(2 * time.Second):
						t.Fatal("resumed stale scan did not finish")
					}
				}

				wantGets := int32(staleScans + 1) // one filler plus one leader per older scan
				wantPuts := int32(staleScans + 1)
				if got := mock.blockGets.Load(); got != wantGets {
					t.Errorf("aligned origin GETs = %d, want %d (one per elected leader)", got, wantGets)
				}
				if got := client.blockPuts.Load(); got != wantPuts {
					t.Errorf("same-ETag cache puts = %d, want %d idempotent writes", got, wantPuts)
				}
				if got := client.receivedBlock(); !bytes.Equal(got, []byte("ABCD")) {
					t.Errorf("last idempotent put bytes = %q, want ABCD", got)
				}
				if got := client.presenceReadCount(block0Key); got != staleScans+1 {
					t.Errorf("block 0 presence reads = %d, want %d outer scans only", got, staleScans+1)
				}
				if got := client.presenceReadCount(block1Key); got != staleScans+1 {
					t.Errorf("block 1 presence reads = %d, want %d outer scans", got, staleScans+1)
				}
				if got := mock.forwards.Load(); got != 0 {
					t.Errorf("client Range origin forwards = %d, want 0", got)
				}
				svc.blockFetchMu.Lock()
				fetchStates := len(svc.blockFetches)
				svc.blockFetchMu.Unlock()
				if fetchStates != 0 {
					t.Errorf("fetch states after stale scans = %d, want 0", fetchStates)
				}
			})
		}
	}
}

func TestProbeFirstRangeLateFillSurvivesNonStalePopulateFailures(t *testing.T) {
	for _, test := range []struct {
		name        string
		local       bool
		failure     string
		alignedGets int32
		blockPuts   int32
	}{
		{name: "transient_fetch", local: true, failure: "fetch", alignedGets: 1},
		{name: "populate_slot_declined", local: true, failure: "slot"},
		{name: "local_put_failed", local: true, failure: "put", alignedGets: 1, blockPuts: 1},
		{name: "remote_put_failed", local: false, failure: "put", alignedGets: 1, blockPuts: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			svc, _, client, mock, meta, _ := newGatedAssembledRangeService(t, test.local)
			t.Cleanup(func() { releaseGatedBlockPut(client) })
			releaseGatedBlockPut(client)
			block0Key, block1Key, fills := installLateSameETagBlockFill(t, client, meta)
			mock.mockForwarder.doRequestFunc = func(context.Context, *http.Request, string, string) (*http.Response, error) {
				mock.forwards.Add(1)
				return nil, errors.New("original Range origin unavailable")
			}

			switch test.failure {
			case "fetch":
				mock.blockGetTransient = true
			case "slot":
				svc.cacheSemaphore <- struct{}{}
			case "put":
				client.setPutError(errors.New("cache write unavailable"))
			default:
				t.Fatalf("unknown failure mode %q", test.failure)
			}

			w := httptest.NewRecorder()
			err := svc.HandleGetObject(w, blockGet(meta.Bucket, meta.Key, "bytes=0-7"))
			if err != nil {
				t.Fatalf("probe-first range with a late same-ETag fill: %v", err)
			}
			requireProbeFirstRange(t, assembledRangeResult{w: w, served: w.Code == http.StatusPartialContent})
			if got := fills.Load(); got != 1 {
				t.Errorf("late same-ETag fills=%d, want 1", got)
			}
			if got := client.presenceReadCount(block0Key); got != 2 {
				t.Errorf("target presence reads=%d, want initial scan and one error-only recovery probe", got)
			}
			if got := client.presenceReadCount(block1Key); got != 1 {
				t.Errorf("present-block probes=%d, want one outer scan", got)
			}
			if got := mock.blockGets.Load(); got != test.alignedGets {
				t.Errorf("aligned origin GETs=%d, want %d", got, test.alignedGets)
			}
			if got := client.blockPuts.Load(); got != test.blockPuts {
				t.Errorf("block puts=%d, want %d", got, test.blockPuts)
			}
			if got := mock.forwards.Load(); got != 0 {
				t.Errorf("original Range origin forwards=%d, want 0", got)
			}
		})
	}
}

func TestProbeFirstRangeLateFillSurvivesStagingDeclinedSmallRange(t *testing.T) {
	svc, _, client, mock, meta, _ := newGatedAssembledRangeService(t, true)
	releaseGatedBlockPut(client)
	block0Key, _, fills := installLateSameETagBlockFill(t, client, meta)
	mock.blockGetTransient = true
	mock.mockForwarder.doRequestFunc = func(context.Context, *http.Request, string, string) (*http.Response, error) {
		mock.forwards.Add(1)
		return nil, errors.New("original Range origin unavailable")
	}
	svc.populateBudget = newByteBudget(4, 0, 4)
	svc.populateBudget.stagingCap = 1 // decline the two-byte assembly buffer, but admit one block fetch

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(meta.Bucket, meta.Key, "bytes=3-4")); err != nil {
		t.Fatalf("staging-declined small Range: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.String() != "DE" || w.Header().Get("X-Cache") != XCacheHit {
		t.Fatalf("small range response = (%d, cache=%q, body=%q), want (206, HIT, DE)", w.Code, w.Header().Get("X-Cache"), w.Body.String())
	}
	if got := w.Header().Get("ETag"); got != `"v1"` {
		t.Errorf("ETag=%q, want %q", got, `"v1"`)
	}
	if got := w.Header().Get("Content-Range"); got != "bytes 3-4/8" {
		t.Errorf("Content-Range=%q, want bytes 3-4/8", got)
	}
	if got := fills.Load(); got != 1 {
		t.Errorf("late fills=%d, want 1", got)
	}
	if got := client.presenceReadCount(block0Key); got != 2 {
		t.Errorf("target presence reads=%d, want scan and failed-fetch recovery", got)
	}
	if got := mock.blockGets.Load(); got != 1 {
		t.Errorf("aligned origin GETs=%d, want 1", got)
	}
	if got := mock.forwards.Load(); got != 0 {
		t.Errorf("original Range origin forwards=%d, want 0", got)
	}
}

func TestProbeFirstRangeLateFillSurvivesIncompleteFullGetFailure(t *testing.T) {
	svc, _, client, mock, meta, _ := newGatedAssembledRangeService(t, true)
	releaseGatedBlockPut(client)
	block0Key, _, fills := installLateSameETagBlockFill(t, client, meta)
	mock.blockGetTransient = true
	mock.mockForwarder.doRequestFunc = func(context.Context, *http.Request, string, string) (*http.Response, error) {
		mock.forwards.Add(1)
		return nil, errors.New("full GET origin unavailable")
	}

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, fullGet(meta.Bucket, meta.Key)); err != nil {
		t.Fatalf("incomplete full-object GET: %v", err)
	}
	if w.Code != http.StatusOK || w.Body.String() != "ABCDEFGH" || w.Header().Get("X-Cache") != XCacheHit {
		t.Fatalf("full response = (%d, cache=%q, body=%q), want (200, HIT, ABCDEFGH)", w.Code, w.Header().Get("X-Cache"), w.Body.String())
	}
	if got := w.Header().Get("ETag"); got != `"v1"` {
		t.Errorf("ETag=%q, want %q", got, `"v1"`)
	}
	if got := fills.Load(); got != 1 {
		t.Errorf("late fills=%d, want 1", got)
	}
	if got := client.presenceReadCount(block0Key); got != 2 {
		t.Errorf("target presence reads=%d, want scan and failed-fetch recovery", got)
	}
	if got := mock.blockGets.Load(); got != 1 {
		t.Errorf("aligned origin GETs=%d, want 1", got)
	}
	if got := mock.forwards.Load(); got != 0 {
		t.Errorf("full-object origin forwards=%d, want 0", got)
	}
}

func TestProbeFirstRangeLateFillDoesNotSuppressStaleETagMismatch(t *testing.T) {
	svc, store, client, mock, meta, _ := newGatedAssembledRangeService(t, true)
	releaseGatedBlockPut(client)
	block0Key, block1Key, fills := installLateSameETagBlockFill(t, client, meta)
	mock.blockGetETag = `"v2"`
	mock.mockForwarder.doRequestFunc = func(context.Context, *http.Request, string, string) (*http.Response, error) {
		mock.forwards.Add(1)
		return nil, errors.New("original Range origin unavailable")
	}

	w := httptest.NewRecorder()
	err := svc.HandleGetObject(w, blockGet(meta.Bucket, meta.Key, "bytes=0-7"))
	if err == nil {
		t.Fatal("ETag mismatch was hidden by the late same-ETag block fill")
	}
	if got := fills.Load(); got != 1 {
		t.Errorf("late same-ETag fills=%d, want 1", got)
	}
	if got := client.presenceReadCount(block0Key); got != 1 {
		t.Errorf("stale block presence reads=%d, want only its initial absent scan", got)
	}
	if got := client.presenceReadCount(block1Key); got != 1 {
		t.Errorf("present block probes=%d, want 1", got)
	}
	if got := mock.blockGets.Load(); got != 1 {
		t.Errorf("aligned origin GETs=%d, want 1", got)
	}
	if got := client.blockPuts.Load(); got != 0 {
		t.Errorf("cache puts after an ETag mismatch=%d, want 0", got)
	}
	if got := mock.forwards.Load(); got != 1 {
		t.Errorf("client Range forwards after stale invalidation=%d, want 1", got)
	}
	if _, found, getErr := store.GetMeta(context.Background(), meta.Bucket, meta.Key); getErr != nil || found {
		t.Errorf("metadata after ETag mismatch = (found=%t, err=%v), want absent", found, getErr)
	}
}

func TestProbeFirstRangeFollowerRecoversAfterCanceledLeader(t *testing.T) {
	for _, test := range []struct {
		name             string
		rangeHeader      string
		stagingDeclined  bool
		wantStatus       int
		wantBody         string
		wantContentRange string
		wantBlock0Reads  int
		wantBlock1Reads  int
	}{
		{name: "multi_block_range", rangeHeader: "bytes=0-7", wantStatus: http.StatusPartialContent, wantBody: "ABCDEFGH", wantContentRange: "bytes 0-7/8", wantBlock0Reads: 3, wantBlock1Reads: 2},
		{name: "staging_declined_small_range", rangeHeader: "bytes=0-1", stagingDeclined: true, wantStatus: http.StatusPartialContent, wantBody: "AB", wantContentRange: "bytes 0-1/8", wantBlock0Reads: 4},
		{name: "probe_first_full_object", wantStatus: http.StatusOK, wantBody: "ABCDEFGH", wantBlock0Reads: 3, wantBlock1Reads: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			svc, _, client, mock, meta, trace := newGatedAssembledRangeService(t, false)
			t.Cleanup(func() { releaseGatedBlockPut(client) })
			if test.stagingDeclined {
				svc.populateBudget = newByteBudget(4, 0, 4)
				svc.populateBudget.stagingCap = 1
			}
			block0Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0)
			block1Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 1)
			if err := client.CacheClient.Put(context.Background(), block1Key, []byte("EFGH"), 60); err != nil {
				t.Fatalf("seed second block: %v", err)
			}

			leaderScanRelease := make(chan struct{})
			followerScanRelease := make(chan struct{})
			var leaderScanOnce, followerScanOnce sync.Once
			releaseLeaderScan := func() { leaderScanOnce.Do(func() { close(leaderScanRelease) }) }
			releaseFollowerScan := func() { followerScanOnce.Do(func() { close(followerScanRelease) }) }
			t.Cleanup(releaseLeaderScan)
			t.Cleanup(releaseFollowerScan)
			pausedScans := make(chan string, 2)
			var pausedMu sync.Mutex
			paused := make(map[string]bool)
			client.setAfterProbeRead(func(ctx context.Context, key string, readErr error) {
				if key != block0Key {
					return
				}
				id, _ := ctx.Value(probeFirstRangeRequestIDKey{}).(string)
				if id != "leader" && id != "follower" {
					return
				}
				pausedMu.Lock()
				firstRead := !paused[id]
				paused[id] = true
				pausedMu.Unlock()
				if !firstRead {
					return
				}
				if readErr == nil {
					t.Errorf("initial block %s presence read unexpectedly succeeded", id)
				}
				pausedScans <- id
				if id == "leader" {
					<-leaderScanRelease
				} else {
					<-followerScanRelease
				}
			})

			fetchGate := &gatedBlockFetchFailureForwarder{
				tracedBlockForwarder: &tracedBlockForwarder{blockMockForwarder: mock, trace: trace},
				started:              make(chan struct{}),
				release:              make(chan struct{}),
			}
			var releaseFetchOnce sync.Once
			releaseFetch := func() { releaseFetchOnce.Do(func() { close(fetchGate.release) }) }
			t.Cleanup(releaseFetch)
			svc.forwarder = fetchGate
			mock.mockForwarder.doRequestFunc = func(context.Context, *http.Request, string, string) (*http.Response, error) {
				mock.forwards.Add(1)
				return nil, errors.New("original Range origin unavailable")
			}

			waitPaused := func(want string) {
				t.Helper()
				select {
				case got := <-pausedScans:
					if got != want {
						t.Fatalf("paused scan = %q, want %q", got, want)
					}
				case <-time.After(2 * time.Second):
					t.Fatalf("%s did not reach its initial missing-block scan", want)
				}
			}
			leaderCtx, cancelLeader := context.WithCancel(context.WithValue(context.Background(), probeFirstRangeRequestIDKey{}, "leader"))
			defer cancelLeader()
			followerCtx := context.WithValue(context.Background(), probeFirstRangeRequestIDKey{}, "follower")
			leader := startGatedProbeFirstRequestWithContext(svc, meta, test.rangeHeader, leaderCtx)
			waitPaused("leader")
			follower := startGatedProbeFirstRequestWithContext(svc, meta, test.rangeHeader, followerCtx)
			waitPaused("follower")

			if err := client.CacheClient.Put(context.Background(), block0Key, []byte("ABCD"), 60); err != nil {
				t.Fatalf("late same-ETag block fill: %v", err)
			}
			releaseLeaderScan()
			waitBlockSignal(t, fetchGate.started, "leader aligned fetch")
			releaseFollowerScan()
			waitForBlockFetchConsumers(t, svc, block0Key, 2)

			cancelLeader()
			select {
			case <-leader:
			case <-time.After(2 * time.Second):
				t.Fatal("canceled leader did not stop waiting for the shared fetch")
			}
			releaseFetch()
			result := waitAssembledRange(t, follower)
			if result.err != nil || result.w.Code != test.wantStatus || result.w.Body.String() != test.wantBody {
				t.Fatalf("live follower response = (%d, %q, err=%v), want (%d, %q)", result.w.Code, result.w.Body.String(), result.err, test.wantStatus, test.wantBody)
			}
			if got := result.w.Header().Get("X-Cache"); got != XCacheHit {
				t.Errorf("live follower X-Cache=%q, want HIT", got)
			}
			if got := result.w.Header().Get("ETag"); got != `"v1"` {
				t.Errorf("live follower ETag=%q, want %q", got, `"v1"`)
			}
			if test.wantContentRange != "" {
				if got := result.w.Header().Get("Content-Range"); got != test.wantContentRange {
					t.Errorf("live follower Content-Range=%q, want %q", got, test.wantContentRange)
				}
			}

			if got := client.presenceReadCount(block0Key); got != test.wantBlock0Reads {
				t.Errorf("block 0 reads=%d, want %d (outer scans, live follower recovery, and any matching body read)", got, test.wantBlock0Reads)
			}
			if got := client.presenceReadCount(block1Key); got != test.wantBlock1Reads {
				t.Errorf("block 1 presence reads=%d, want %d", got, test.wantBlock1Reads)
			}
			if got := mock.blockGets.Load(); got != 1 {
				t.Errorf("aligned upstream GETs=%d, want one coalesced fetch", got)
			}
			if got := mock.forwards.Load(); got > 1 {
				t.Errorf("original origin fallbacks=%d, want no follower fallback (at most the canceled leader)", got)
			}
			if activeBlockFetch(svc, block0Key) != nil {
				t.Error("shared block-fetch state remained after the follower was served")
			}
		})
	}
}
func TestProbeFirstRangeOlderScanRecoversLateFillAfterLeaderFailure(t *testing.T) {
	for _, test := range []struct {
		name             string
		rangeHeader      string
		stagingDeclined  bool
		wantStatus       int
		wantBody         string
		wantContentRange string
		wantBlock0Reads  int
		wantBlock1Reads  int
	}{
		{name: "multi_block_range", rangeHeader: "bytes=0-7", wantStatus: http.StatusPartialContent, wantBody: "ABCDEFGH", wantContentRange: "bytes 0-7/8", wantBlock0Reads: 4, wantBlock1Reads: 2},
		{name: "staging_declined_small_range", rangeHeader: "bytes=0-1", stagingDeclined: true, wantStatus: http.StatusPartialContent, wantBody: "AB", wantContentRange: "bytes 0-1/8", wantBlock0Reads: 6},
		{name: "probe_first_full_object", wantStatus: http.StatusOK, wantBody: "ABCDEFGH", wantBlock0Reads: 4, wantBlock1Reads: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			svc, _, client, mock, meta, trace := newGatedAssembledRangeService(t, false)
			t.Cleanup(func() { releaseGatedBlockPut(client) })
			if test.stagingDeclined {
				svc.populateBudget = newByteBudget(4, 0, 4)
				svc.populateBudget.stagingCap = 1
			}
			block0Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0)
			block1Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 1)
			if err := client.CacheClient.Put(context.Background(), block1Key, []byte("EFGH"), 60); err != nil {
				t.Fatalf("seed second block: %v", err)
			}

			olderScanPaused := make(chan struct{})
			releaseOlderScan := make(chan struct{})
			var pauseOnce, releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(releaseOlderScan) }) })
			client.setAfterProbeRead(func(ctx context.Context, key string, readErr error) {
				id, _ := ctx.Value(probeFirstRangeRequestIDKey{}).(string)
				if key != block0Key || id != "older" {
					return
				}
				pauseOnce.Do(func() {
					if readErr == nil {
						t.Error("older scan unexpectedly found block 0 before the late fill")
					}
					close(olderScanPaused)
					<-releaseOlderScan
				})
			})

			fetchGate := &gatedBlockFetchFailureForwarder{
				tracedBlockForwarder: &tracedBlockForwarder{blockMockForwarder: mock, trace: trace},
				started:              make(chan struct{}),
				release:              make(chan struct{}),
			}
			var releaseFetchOnce sync.Once
			releaseFetch := func() { releaseFetchOnce.Do(func() { close(fetchGate.release) }) }
			t.Cleanup(releaseFetch)
			svc.forwarder = fetchGate
			mock.mockForwarder.doRequestFunc = func(context.Context, *http.Request, string, string) (*http.Response, error) {
				mock.forwards.Add(1)
				return nil, errors.New("original Range origin unavailable")
			}

			olderCtx := context.WithValue(context.Background(), probeFirstRangeRequestIDKey{}, "older")
			older := startGatedProbeFirstRequestWithContext(svc, meta, test.rangeHeader, olderCtx)
			select {
			case <-olderScanPaused:
			case <-time.After(2 * time.Second):
				t.Fatal("older scan did not pause after classifying block 0 absent")
			}
			leaderCtx := context.WithValue(context.Background(), probeFirstRangeRequestIDKey{}, "leader")
			leader := startGatedProbeFirstRequestWithContext(svc, meta, test.rangeHeader, leaderCtx)
			waitBlockSignal(t, fetchGate.started, "leader aligned fetch")
			if err := client.CacheClient.Put(context.Background(), block0Key, []byte("ABCD"), 60); err != nil {
				t.Fatalf("late same-ETag block fill: %v", err)
			}
			releaseFetch()
			leaderResult := waitAssembledRange(t, leader)
			if leaderResult.err != nil || leaderResult.w.Code != test.wantStatus || leaderResult.w.Body.String() != test.wantBody || leaderResult.w.Header().Get("X-Cache") != XCacheHit {
				t.Fatalf("leader response = (%d, cache=%q, body=%q, err=%v), want (%d, HIT, %q)", leaderResult.w.Code, leaderResult.w.Header().Get("X-Cache"), leaderResult.w.Body.String(), leaderResult.err, test.wantStatus, test.wantBody)
			}
			if test.wantContentRange != "" && leaderResult.w.Header().Get("Content-Range") != test.wantContentRange {
				t.Errorf("leader Content-Range=%q, want %q", leaderResult.w.Header().Get("Content-Range"), test.wantContentRange)
			}

			releaseOnce.Do(func() { close(releaseOlderScan) })
			olderResult := waitAssembledRange(t, older)
			if olderResult.err != nil || olderResult.w.Code != test.wantStatus || olderResult.w.Body.String() != test.wantBody || olderResult.w.Header().Get("X-Cache") != XCacheHit {
				t.Fatalf("older-scan response = (%d, cache=%q, body=%q, err=%v), want (%d, HIT, %q)", olderResult.w.Code, olderResult.w.Header().Get("X-Cache"), olderResult.w.Body.String(), olderResult.err, test.wantStatus, test.wantBody)
			}
			if test.wantContentRange != "" && olderResult.w.Header().Get("Content-Range") != test.wantContentRange {
				t.Errorf("older-scan Content-Range=%q, want %q", olderResult.w.Header().Get("Content-Range"), test.wantContentRange)
			}
			if got := client.presenceReadCount(block0Key); got != test.wantBlock0Reads {
				t.Errorf("block 0 reads=%d, want %d (both scans, recovery, recheck, and any body read)", got, test.wantBlock0Reads)
			}
			if got := client.presenceReadCount(block1Key); got != test.wantBlock1Reads {
				t.Errorf("block 1 presence reads=%d, want %d", got, test.wantBlock1Reads)
			}
			if got := mock.blockGets.Load(); got != 2 {
				t.Errorf("aligned origin GETs=%d, want one for each elected leader before late-fill recovery", got)
			}
			if got := mock.forwards.Load(); got != 0 {
				t.Errorf("original Range fallbacks=%d, want 0", got)
			}
		})
	}
}

func TestProbeFirstRangeRecoveryUsesRequestContext(t *testing.T) {
	svc, _, client, mock, meta, _ := newGatedAssembledRangeService(t, true)
	releaseGatedBlockPut(client)
	block0Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0)
	block1Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 1)
	if err := client.CacheClient.Put(context.Background(), block1Key, []byte("EFGH"), 60); err != nil {
		t.Fatalf("seed second block: %v", err)
	}
	mock.blockGetTransient = true

	recoveryStarted := make(chan context.Context, 1)
	client.setAfterProbeRead(func(ctx context.Context, key string, _ error) {
		if key != block0Key || client.presenceReadCount(block0Key) != 2 {
			return
		}
		recoveryStarted <- ctx
		<-ctx.Done()
	})

	requestCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	results := make(chan assembledRangeResult, 1)
	go func() {
		w := httptest.NewRecorder()
		req := blockGet(meta.Bucket, meta.Key, "bytes=0-7").WithContext(requestCtx)
		err := svc.HandleGetObject(w, req)
		results <- assembledRangeResult{w: w, served: w.Code == http.StatusPartialContent, err: err}
	}()

	var recoveryCtx context.Context
	select {
	case recoveryCtx = <-recoveryStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("probe-first leader did not start its failure recovery read")
	}
	deadline, ok := recoveryCtx.Deadline()
	if !ok {
		t.Fatal("recovery presence read has no deadline")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > blockFailureRecoveryTimeout {
		t.Fatalf("recovery deadline has %s remaining, want at most %s", remaining, blockFailureRecoveryTimeout)
	}

	cancel()
	select {
	case <-recoveryCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("request cancellation did not cancel the recovery presence read")
	}
	select {
	case <-results:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not finish after recovery context cancellation")
	}
}

func TestProbeFirstRangeCoalescedFailureRecoveryIsPerConsumer(t *testing.T) {
	svc, _, client, mock, meta, _ := newGatedAssembledRangeService(t, false)
	block0Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0)
	block1Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 1)
	if err := client.CacheClient.Put(context.Background(), block1Key, []byte("EFGH"), 60); err != nil {
		t.Fatalf("seed second block: %v", err)
	}
	client.setPutError(errors.New("remote cache write unavailable"))
	svc.populateBudget = newByteBudget(4, 0, 4)
	svc.populateBudget.stagingCap = 2

	recoveryStarted := make(chan struct{})
	allowRecovery := make(chan struct{})
	var recoveryMu sync.Mutex
	blockedRecovery := false
	var recoveryRelease sync.Once
	releaseRecovery := func() { recoveryRelease.Do(func() { close(allowRecovery) }) }
	t.Cleanup(func() {
		releaseGatedBlockPut(client)
		releaseRecovery()
	})
	client.setAfterProbeRead(func(_ context.Context, key string, readErr error) {
		if key != block0Key || client.presenceReadCount(block0Key) <= 2 {
			return
		}
		recoveryMu.Lock()
		blockThisRead := !blockedRecovery
		blockedRecovery = true
		recoveryMu.Unlock()
		if !blockThisRead {
			return
		}
		if readErr != nil {
			t.Errorf("recovery presence read = %v, want the late-filled block", readErr)
		}
		close(recoveryStarted)
		select {
		case <-allowRecovery:
		case <-time.After(2 * time.Second):
			t.Error("recovery probe remained blocked")
		}
	})

	first := startGatedProbeFirstRange(svc, meta)
	waitBlockSignal(t, client.putStarted, "first remote cache put")
	second := startGatedProbeFirstRange(svc, meta)
	waitForBlockFetchConsumers(t, svc, block0Key, 2)
	fillDone := make(chan error, 1)
	go func() { fillDone <- client.CacheClient.Put(context.Background(), block0Key, []byte("ABCD"), 60) }()
	select {
	case err := <-fillDone:
		if err != nil {
			t.Fatalf("late same-ETag fill: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late block fill did not complete")
	}
	releaseGatedBlockPut(client)
	waitBlockSignal(t, client.putFinished, "failed remote cache put")
	select {
	case <-recoveryStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("a live consumer did not start its failure-only presence recovery")
	}

	if activeBlockFetch(svc, block0Key) != nil {
		t.Fatal("completed block-fetch state remained active during per-consumer recovery")
	}
	svc.populateBudget.mu.Lock()
	remaining := svc.populateBudget.remaining
	svc.populateBudget.mu.Unlock()
	if remaining != 4 || len(svc.cacheSemaphore) != 0 {
		t.Fatalf("remote reservation during recovery = (bytes=%d slots=%d), want (4, 0)", remaining, len(svc.cacheSemaphore))
	}
	if got := mock.blockGets.Load(); got != 1 || client.blockPuts.Load() != 1 {
		t.Fatalf("failed shared populate = (alignedGets=%d, puts=%d), want (1, 1)", got, client.blockPuts.Load())
	}

	var firstDone, secondDone bool
	select {
	case result := <-first:
		requireProbeFirstRange(t, result)
		firstDone = true
	case result := <-second:
		requireProbeFirstRange(t, result)
		secondDone = true
	case <-time.After(2 * time.Second):
		t.Fatal("unblocked consumer did not recover the late-filled block")
	}
	if got := client.presenceReadCount(block0Key); got != 4 {
		t.Fatalf("coalesced target reads=%d, want two outer scans and two consumer recovery reads", got)
	}
	if got := mock.forwards.Load(); got != 0 {
		t.Errorf("client Range fallbacks=%d, want 0", got)
	}
	select {
	case result := <-first:
		if firstDone {
			t.Fatalf("first response completed while its recovery probe was gated: %v", result.err)
		}
		requireProbeFirstRange(t, result)
		firstDone = true
	case result := <-second:
		if secondDone {
			t.Fatalf("second response completed while its recovery probe was gated: %v", result.err)
		}
		requireProbeFirstRange(t, result)
		secondDone = true
	default:
	}
	if firstDone == secondDone {
		t.Fatalf("response completion while one recovery is gated = (first=%t second=%t), want exactly one", firstDone, secondDone)
	}

	releaseRecovery()
	if !firstDone {
		requireProbeFirstRange(t, waitAssembledRange(t, first))
	}
	if !secondDone {
		requireProbeFirstRange(t, waitAssembledRange(t, second))
	}
	if got := client.presenceReadCount(block1Key); got != 2 {
		t.Errorf("present-block probes=%d, want one outer scan per request", got)
	}
}
func TestProbeFirstRangeCoalescesRemoteMissUntilCacheWriteFinishes(t *testing.T) {
	svc, _, client, mock, meta, _ := newGatedAssembledRangeService(t, false)
	t.Cleanup(func() { releaseGatedBlockPut(client) })
	block0Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0)
	block1Key := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 1)
	if err := client.CacheClient.Put(context.Background(), block1Key, []byte("EFGH"), 60); err != nil {
		t.Fatalf("seed second block: %v", err)
	}

	first := startGatedProbeFirstRange(svc, meta)
	waitBlockSignal(t, client.putStarted, "first probe-first cache put")
	second := startGatedProbeFirstRange(svc, meta)
	waitForBlockFetchConsumers(t, svc, block0Key, 2)
	if got := mock.blockGets.Load(); got != 1 {
		t.Fatalf("overlapping range requests caused %d aligned upstream GETs, want 1", got)
	}
	if got := client.blockPuts.Load(); got != 1 {
		t.Fatalf("overlapping range requests caused %d cache puts, want 1", got)
	}
	if got := client.presenceReadCount(block0Key); got != 2 {
		t.Fatalf("byte-zero probes across overlapping requests = %d, want one outer probe per request", got)
	}
	select {
	case result := <-first:
		t.Fatalf("first range completed before remote cache write: served=%t err=%v", result.served, result.err)
	default:
	}
	select {
	case result := <-second:
		t.Fatalf("second range completed before remote cache write: served=%t err=%v", result.served, result.err)
	default:
	}

	releaseGatedBlockPut(client)
	waitBlockSignal(t, client.putFinished, "shared probe-first cache put finish")
	requireProbeFirstRange(t, waitAssembledRange(t, first))
	requireProbeFirstRange(t, waitAssembledRange(t, second))
	if got := mock.blockGets.Load(); got != 1 {
		t.Fatalf("overlapping range requests fetched %d upstream blocks, want 1", got)
	}
}
