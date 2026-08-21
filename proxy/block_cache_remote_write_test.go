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

	getCalls atomic.Int64

	putStarted  chan struct{}
	allowPut    chan struct{}
	putFinished chan struct{}

	mu       sync.Mutex
	received []byte
}

func newGatedOwnerBlockClient(base cacheclient.CacheClient, local bool, trace *remoteMissTrace) *gatedOwnerBlockClient {
	return &gatedOwnerBlockClient{
		CacheClient: base,
		local:       local,
		trace:       trace,
		putStarted:  make(chan struct{}, 1),
		allowPut:    make(chan struct{}),
		putFinished: make(chan struct{}, 1),
	}
}

func (c *gatedOwnerBlockClient) IsLocal(string) bool { return c.local }

func (c *gatedOwnerBlockClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if c.getCalls.Add(1) == 1 {
		c.trace.add("cache-miss-probe")
	} else {
		c.trace.add("cache-read")
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

func (c *gatedOwnerBlockClient) PutBlockBytes(ctx context.Context, key string, data []byte, ttlSeconds int64) (bool, error) {
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
	c.received = append(c.received[:0], data...)
	c.mu.Unlock()
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
	if wrote, err := store.PutMetaTombstoneAware(context.Background(), bucket, key, meta, 60, time.Now().UnixNano()); err != nil || !wrote {
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
		t.Fatalf("metadata after tombstoned detached write = (found=%t, err=%v), want absent", found, err)
	}
	if tombstone := store.GetTombstoneTimestamp(context.Background(), meta.Bucket, meta.Key); tombstone == 0 {
		t.Fatal("delete tombstone disappeared while detached block write completed")
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
