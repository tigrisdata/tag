package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const prefetchTestBlockSize = 64 << 10

// blockReadTrace records cache-read spans for the warm block-serve test fixture.
// A local fixture records spans at its CacheClient wrapper; the remote fixture
// records them at a real gRPC cache-owner server, after the RPC has arrived.
type blockReadSpan struct {
	key      string
	started  time.Duration
	finished time.Duration
}

type blockReadTrace struct {
	mu sync.Mutex

	started     time.Time
	inFlight    int
	maxInFlight int
	spans       []blockReadSpan
}

func (t *blockReadTrace) reset() {
	t.mu.Lock()
	t.started = time.Now()
	t.inFlight = 0
	t.maxInFlight = 0
	t.spans = t.spans[:0]
	t.mu.Unlock()
}

func (t *blockReadTrace) begin(key string) func() {
	t.mu.Lock()
	span := len(t.spans)
	t.spans = append(t.spans, blockReadSpan{key: key, started: time.Since(t.started)})
	t.inFlight++
	if t.inFlight > t.maxInFlight {
		t.maxInFlight = t.inFlight
	}
	t.mu.Unlock()

	return func() {
		t.mu.Lock()
		t.spans[span].finished = time.Since(t.started)
		t.inFlight--
		t.mu.Unlock()
	}
}

func (t *blockReadTrace) peak() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.maxInFlight
}

func (t *blockReadTrace) String() string {
	t.mu.Lock()
	spans := append([]blockReadSpan(nil), t.spans...)
	t.mu.Unlock()

	parts := make([]string, 0, len(spans))
	for _, span := range spans {
		parts = append(parts, fmt.Sprintf("block %s@%s-%s", blockReadKeyIndex(span.key), span.started, span.finished))
	}
	return strings.Join(parts, " -> ")
}

func blockReadKeyIndex(key string) string {
	if at := strings.LastIndexByte(key, '|'); at >= 0 {
		return key[at+1:]
	}
	return key
}

// blockReadController holds cache reads at a test-controlled gate and records
// the resulting trace.
type blockReadController struct {
	mu sync.Mutex

	gate    <-chan struct{}
	started chan<- struct{}
	trace   *blockReadTrace
}

func (c *blockReadController) configure(gate <-chan struct{}, started chan<- struct{}) {
	c.mu.Lock()
	c.gate = gate
	c.started = started
	c.mu.Unlock()
}

func (c *blockReadController) begin(ctx context.Context, key string) (func(), error) {
	finish := c.trace.begin(key)

	c.mu.Lock()
	gate, started := c.gate, c.started
	c.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return finish, ctx.Err()
		}
	}
	return finish, nil
}

// localBlockReadClient models a local cache owner. IsLocal keeps cache
// locality accounting honest while the wrapped memory cache supplies the bytes.
type localBlockReadClient struct {
	cacheclient.CacheClient
	controller *blockReadController
}

func (*localBlockReadClient) IsLocal(string) bool { return true }

func (c *localBlockReadClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	finish, err := c.controller.begin(ctx, key)
	defer finish()
	if err != nil {
		return err
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

// remoteBlockReadClient marks the cache keys as remote. Its embedded simple
// client performs actual gRPC Get streams against prefetchBlockReadServer.
type remoteBlockReadClient struct {
	cacheclient.CacheClient
}

func (*remoteBlockReadClient) IsLocal(string) bool { return false }

// prefetchBlockReadServer is a small immutable remote cache owner. Fixture
// setup writes its block map before serving starts, so concurrent Get handlers
// only read immutable byte slices.
type prefetchBlockReadServer struct {
	pb.UnimplementedCacheServiceServer

	blocks     map[string][]byte
	controller *blockReadController
}

func (s *prefetchBlockReadServer) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	finish, err := s.controller.begin(stream.Context(), req.Key)
	defer finish()
	if err != nil {
		return err
	}

	data, ok := s.blocks[req.Key]
	if !ok {
		return status.Error(codes.NotFound, "block not found")
	}
	start, end := req.Start, req.End
	if end <= 0 {
		end = int64(len(data)) - 1
	}
	if start < 0 || start > end || end >= int64(len(data)) {
		return status.Error(codes.InvalidArgument, "invalid block range")
	}
	return stream.Send(&pb.GetResponse{Data: data[start : end+1]})
}

type blockPrefetchFixture struct {
	service    *Service
	meta       *cache.CachedObjectMeta
	bucket     string
	key        string
	body       []byte
	bodyBytes  int64
	blockCount int
	trace      *blockReadTrace
	controller *blockReadController
}

func newBlockPrefetchFixture(tb testing.TB, remote bool, blockCount int) *blockPrefetchFixture {
	tb.Helper()

	trace := &blockReadTrace{}
	controller := &blockReadController{trace: trace}
	body := make([]byte, blockCount*prefetchTestBlockSize)
	for block := 0; block < blockCount; block++ {
		for i := range body[block*prefetchTestBlockSize : (block+1)*prefetchTestBlockSize] {
			body[block*prefetchTestBlockSize+i] = byte(block)
		}
	}

	const (
		bucket = "benchmark"
		key    = "prefetch"
		etag   = `"prefetch"`
	)
	blockSize := int64(prefetchTestBlockSize)
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          etag,
		ContentLength: int64(len(body)),
		BlockSize:     blockSize,
	}

	var client cacheclient.CacheClient
	if remote {
		blocks := make(map[string][]byte, blockCount)
		for i := 0; i < blockCount; i++ {
			blockKey := cache.MakeBlockKey(bucket, key, etag, blockSize, int64(i))
			blocks[blockKey] = body[i*prefetchTestBlockSize : (i+1)*prefetchTestBlockSize]
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatal(err)
		}
		server := grpc.NewServer(grpc.MaxRecvMsgSize(cacheclient.MaxMessageSize))
		pb.RegisterCacheServiceServer(server, &prefetchBlockReadServer{blocks: blocks, controller: controller})
		go func() { _ = server.Serve(listener) }()
		tb.Cleanup(server.Stop)
		tb.Cleanup(func() { _ = listener.Close() })

		remoteClient, err := cacheclient.NewSimpleClient(&cacheclient.ClientConfig{
			Addrs:              []string{listener.Addr().String()},
			Mode:               cacheclient.ModeSimple,
			ConnectionPoolSize: 1,
		})
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() { _ = remoteClient.Close() })
		client = &remoteBlockReadClient{CacheClient: remoteClient}
	} else {
		localClient := cacheclient.NewMemoryCache()
		for i := 0; i < blockCount; i++ {
			blockKey := cache.MakeBlockKey(bucket, key, etag, blockSize, int64(i))
			start := i * prefetchTestBlockSize
			if err := localClient.Put(context.Background(), blockKey, body[start:start+prefetchTestBlockSize], 0); err != nil {
				tb.Fatalf("seed local block %d: %v", i, err)
			}
		}
		client = &localBlockReadClient{CacheClient: localClient, controller: controller}
	}

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = blockSize
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	return &blockPrefetchFixture{
		service:    NewService(&mockForwarder{}, store, cfg),
		meta:       meta,
		bucket:     bucket,
		key:        key,
		body:       body,
		bodyBytes:  int64(len(body)),
		blockCount: blockCount,
		trace:      trace,
		controller: controller,
	}
}

func (f *blockPrefetchFixture) serve(w http.ResponseWriter) (streamOutcome, error) {
	return f.service.streamBlockRange(context.Background(), w, f.bucket, f.key, "access", "secret", f.meta, 0, f.bodyBytes-1)
}

func (f *blockPrefetchFixture) newWriter() *pacedBlockWriter {
	return &pacedBlockWriter{
		expected: f.body,
		markers:  make([]blockWriteMarker, 0, f.blockCount),
	}
}

// pacedBlockWriter records constant-size markers without retaining the response
// body, then validates byte order against the fixture.
type pacedBlockWriter struct {
	n        int64
	expected []byte
	markers  []blockWriteMarker
	header   http.Header
}

type blockWriteMarker struct {
	offset int64
	n      int
	first  byte
	last   byte
}

func (w *pacedBlockWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (*pacedBlockWriter) WriteHeader(int) {}

func (w *pacedBlockWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		w.markers = append(w.markers, blockWriteMarker{
			offset: w.n,
			n:      len(p),
			first:  p[0],
			last:   p[len(p)-1],
		})
	}
	w.n += int64(len(p))
	return len(p), nil
}

func (w *pacedBlockWriter) matches() bool {
	if w.n != int64(len(w.expected)) {
		return false
	}
	for _, marker := range w.markers {
		end := marker.offset + int64(marker.n)
		if marker.offset < 0 || end > int64(len(w.expected)) || marker.first != w.expected[marker.offset] || marker.last != w.expected[end-1] {
			return false
		}
	}
	return true
}

func waitForBlockReadStarts(t *testing.T, started <-chan struct{}, want int) {
	t.Helper()
	for i := 0; i < want; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for cache read %d of %d", i+1, want)
		}
	}
}

// Warm blocks owned by different peers must begin their cache reads before the
// first client write drains the window. The real gRPC fixture and gate make the
// overlap deterministic; streamBlockRange still reports only fully ordered
// cache hits after all bytes reach the writer.
func TestBlockCache_PipelinePrefetchesRemoteReads(t *testing.T) {
	const blocks = maxBlockServePrefetch + 1
	fixture := newBlockPrefetchFixture(t, true, blocks)
	started := make(chan struct{}, maxBlockServePrefetch)
	gate := make(chan struct{})
	fixture.trace.reset()
	fixture.controller.configure(gate, started)

	type result struct {
		out streamOutcome
		err error
		w   *pacedBlockWriter
	}
	results := make(chan result, 1)
	go func() {
		w := fixture.newWriter()
		out, err := fixture.serve(w)
		results <- result{out: out, err: err, w: w}
	}()

	waitForBlockReadStarts(t, started, maxBlockServePrefetch)
	if got := fixture.trace.peak(); got != maxBlockServePrefetch {
		t.Fatalf("concurrent remote cache reads = %d, want %d", got, maxBlockServePrefetch)
	}
	close(gate)

	select {
	case got := <-results:
		if got.err != nil || got.out.fetched != 0 || got.out.fromCache != blocks || !got.w.matches() {
			t.Fatalf("prefetched remote serve = (out=%+v, bytes=%d, ordered=%t, err=%v)", got.out, got.w.n, got.w.matches(), got.err)
		}
		t.Logf("remote GetRangeStream spans: %s", fixture.trace)
	case <-time.After(2 * time.Second):
		t.Fatal("prefetched remote serve did not complete")
	}
}

// The prefetch window may use only staging bytes it has reserved. With a cap
// for three blocks, the 32-block request must stop at three concurrent reads
// and return every reservation after the ordered write loop completes.
func TestBlockCache_PipelinePrefetchWindowRespectsStagingBudget(t *testing.T) {
	const blocks = maxBlockServePrefetch
	fixture := newBlockPrefetchFixture(t, false, blocks)
	blockSize := int64(prefetchTestBlockSize)
	total := 6 * blockSize // NewService's half staging cap is three block buffers.
	fixture.service.config.Cache.MaxPopulateMemoryBytes = total
	fixture.service.populateBudget = newByteBudget(total, 0, fixture.service.perPopulateCap)
	fixture.service.populateBudget.stagingCap = total / 2

	started := make(chan struct{}, blocks)
	gate := make(chan struct{})
	fixture.trace.reset()
	fixture.controller.configure(gate, started)
	results := make(chan error, 1)
	go func() {
		_, err := fixture.serve(fixture.newWriter())
		results <- err
	}()

	const wantWindow = 3
	waitForBlockReadStarts(t, started, wantWindow)
	if got := fixture.trace.peak(); got != wantWindow {
		t.Fatalf("staging-limited cache reads = %d, want %d", got, wantWindow)
	}
	fixture.service.populateBudget.mu.Lock()
	inUse := fixture.service.populateBudget.stagingInUse
	fixture.service.populateBudget.mu.Unlock()
	if want := int64(wantWindow) * blockSize; inUse != want {
		t.Fatalf("staging bytes in use = %d, want %d", inUse, want)
	}
	close(gate)

	select {
	case err := <-results:
		if err != nil {
			t.Fatalf("staging-limited serve = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("staging-limited serve did not complete")
	}
	fixture.service.populateBudget.mu.Lock()
	remaining := fixture.service.populateBudget.remaining
	inUse = fixture.service.populateBudget.stagingInUse
	fixture.service.populateBudget.mu.Unlock()
	if remaining != total || inUse != 0 {
		t.Fatalf("staging budget after serve = (remaining=%d, in_use=%d), want (%d, 0)", remaining, inUse, total)
	}
}

// speculativeMissClient makes each selected block's first range read report a
// controlled miss. Later reads delegate to the underlying cache, letting a
// concurrent writer fill selected blocks before the ordered consumer retries.
type speculativeMissClient struct {
	cacheclient.CacheClient

	mu      sync.Mutex
	initial map[string]bool
	started chan<- struct{}
	release <-chan struct{}
}

func (c *speculativeMissClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	c.mu.Lock()
	initial := c.initial[key]
	delete(c.initial, key)
	c.mu.Unlock()
	if !initial {
		return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
	}
	c.started <- struct{}{}
	select {
	case <-c.release:
		return cache.ErrNotFound
	case <-ctx.Done():
		return ctx.Err()
	}
}

// prefetchFillForwarder supplies the one genuinely absent block after two
// speculative misses become cache hits. Its embedded mock supplies the other
// RequestForwarder methods used by the service.
type prefetchFillForwarder struct {
	mockForwarder
	body []byte
	etag string
}

func (f *prefetchFillForwarder) DoConditionalGetRequest(_ context.Context, _, _, _, _, _ string, _ int64, rangeHeader string) (*http.Response, error) {
	if rangeHeader != "bytes=4-5" {
		return nil, fmt.Errorf("unexpected upstream range %q", rangeHeader)
	}
	header := make(http.Header)
	header.Set("Content-Range", "bytes 4-5/6")
	header.Set("ETag", f.etag)
	return &http.Response{
		StatusCode:    http.StatusPartialContent,
		ContentLength: int64(len(f.body)),
		Header:        header,
		Body:          io.NopCloser(strings.NewReader(string(f.body))),
	}, nil
}

// A wider pipeline can observe several cache misses before its ordered consumer
// reaches the first. If another request fills two of them, those stale misses
// must not consume this serve's two-fetch allowance and force a third, genuine
// miss into remainder fallback (which would wrongly invalidate valid metadata).
func TestBlockCache_PipelineConcurrentFillDoesNotExhaustInlineFetchCap(t *testing.T) {
	const (
		blockSize  = 2
		blockCount = 3
		bucket     = "bucket"
		key        = "key"
		etag       = `"v1"`
	)
	body := []byte("ABCDEF")
	base := cacheclient.NewMemoryCache()
	started := make(chan struct{}, blockCount)
	release := make(chan struct{})
	initial := make(map[string]bool, blockCount)
	for i := int64(0); i < blockCount; i++ {
		initial[cache.MakeBlockKey(bucket, key, etag, blockSize, i)] = true
	}
	client := &speculativeMissClient{
		CacheClient: base,
		initial:     initial,
		started:     started,
		release:     release,
	}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = blockSize
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	forwarder := &prefetchFillForwarder{body: body[4:], etag: etag}
	service := NewService(forwarder, store, cfg)
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          etag,
		ContentLength: int64(len(body)),
		StatusCode:    http.StatusOK,
		BlockSize:     blockSize,
	}
	if wrote, err := store.PutMetaTombstoneAware(context.Background(), bucket, key, meta, 60, time.Now().UnixNano()); err != nil || !wrote {
		t.Fatalf("seed block metadata = (wrote=%t, err=%v)", wrote, err)
	}

	type result struct {
		out streamOutcome
		err error
		w   *pacedBlockWriter
	}
	results := make(chan result, 1)
	go func() {
		w := &pacedBlockWriter{expected: body}
		out, err := service.streamBlockRange(context.Background(), w, bucket, key, "access", "secret", meta, 0, int64(len(body)-1))
		results <- result{out: out, err: err, w: w}
	}()

	waitForBlockReadStarts(t, started, blockCount)
	for i := int64(0); i < 2; i++ {
		blockKey := cache.MakeBlockKey(bucket, key, etag, blockSize, i)
		start := i * blockSize
		if err := base.Put(context.Background(), blockKey, body[start:start+blockSize], 0); err != nil {
			t.Fatalf("concurrent fill block %d: %v", i, err)
		}
	}
	close(release)

	select {
	case got := <-results:
		if got.err != nil || got.out.fromCache != 2 || got.out.fetched != 1 || got.out.remainder || !got.w.matches() {
			t.Fatalf("concurrent-fill serve = (out=%+v, bytes=%d, ordered=%t, err=%v)", got.out, got.w.n, got.w.matches(), got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent-fill serve did not complete")
	}
	if _, found, err := store.GetMeta(context.Background(), bucket, key); err != nil || !found {
		t.Fatalf("metadata after recovered real miss = (found=%t, err=%v), want valid entry", found, err)
	}
}

// orderedReadErrorClient lets a later cache read fail while block zero remains
// in flight. A bounded prefetch may observe the later failure early, but it
// must surface the first block's failure at the same response point as the
// sequential stream and must not write either block.
type orderedReadErrorClient struct {
	cacheclient.CacheClient

	firstStarted chan<- struct{}
	laterStarted chan<- struct{}
	allowFirst   <-chan struct{}
	firstErr     error
	laterErr     error
}

func (*orderedReadErrorClient) IsLocal(string) bool { return true }

func (c *orderedReadErrorClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	switch blockReadKeyIndex(key) {
	case "0":
		c.firstStarted <- struct{}{}
		select {
		case <-c.allowFirst:
			return c.firstErr
		case <-ctx.Done():
			return ctx.Err()
		}
	case "1":
		c.laterStarted <- struct{}{}
		return c.laterErr
	default:
		return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
	}
}

func TestBlockCache_PipelinePreservesFirstReadErrorOrder(t *testing.T) {
	firstErr := errors.New("first block read failed")
	laterErr := errors.New("later block read failed")
	firstStarted := make(chan struct{}, 1)
	laterStarted := make(chan struct{}, 1)
	allowFirst := make(chan struct{})
	client := &orderedReadErrorClient{
		CacheClient:  cacheclient.NewMemoryCache(),
		firstStarted: firstStarted,
		laterStarted: laterStarted,
		allowFirst:   allowFirst,
		firstErr:     firstErr,
		laterErr:     laterErr,
	}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 2
	cfg.Cache.SizeThreshold = 2
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	service := NewService(&mockForwarder{}, store, cfg)
	meta := &cache.CachedObjectMeta{ETag: `"v1"`, ContentLength: 4, BlockSize: 2}

	type result struct {
		out streamOutcome
		err error
		w   *pacedBlockWriter
	}
	results := make(chan result, 1)
	go func() {
		w := &pacedBlockWriter{}
		out, err := service.streamBlockRangePipelined(context.Background(), &countingWriter{w: w}, "bucket", "key", "access", "secret", meta, 0, 3, 2)
		results <- result{out: out, err: err, w: w}
	}()

	waitForBlockReadStarts(t, firstStarted, 1)
	waitForBlockReadStarts(t, laterStarted, 1)
	close(allowFirst)
	select {
	case got := <-results:
		if !errors.Is(got.err, firstErr) || errors.Is(got.err, laterErr) {
			t.Fatalf("ordered pipeline error = %v, want first error only", got.err)
		}
		if got.out.fromCache != 0 || got.out.fetched != 0 || got.w.n != 0 {
			t.Fatalf("ordered pipeline wrote after first error: out=%+v bytes=%d", got.out, got.w.n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not return the first read error")
	}
}

// cancellablePrefetchClient holds the reads after block zero until the pipeline
// cancels its child context. It verifies a client-write failure does not leave
// a wider window's buffers or staging reservation behind.
type cancellablePrefetchClient struct {
	cacheclient.CacheClient

	firstStarted chan<- struct{}
	laterStarted chan<- struct{}
	cancelled    chan<- struct{}
	allowFirst   <-chan struct{}
}

func (*cancellablePrefetchClient) IsLocal(string) bool { return true }

func (c *cancellablePrefetchClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if blockReadKeyIndex(key) == "0" {
		c.firstStarted <- struct{}{}
		select {
		case <-c.allowFirst:
			return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.laterStarted <- struct{}{}
	<-ctx.Done()
	c.cancelled <- struct{}{}
	return ctx.Err()
}

func TestBlockCache_PipelineClientWriteFailureCancelsPrefetches(t *testing.T) {
	const (
		blockSize  = 2
		blockCount = maxBlockServePrefetch
		total      = 128 // staging cap is 64 bytes: exactly 32 block buffers.
	)
	firstStarted := make(chan struct{}, 1)
	laterStarted := make(chan struct{}, blockCount-1)
	cancelled := make(chan struct{}, blockCount-1)
	allowFirst := make(chan struct{})
	base := cacheclient.NewMemoryCache()
	client := &cancellablePrefetchClient{
		CacheClient:  base,
		firstStarted: firstStarted,
		laterStarted: laterStarted,
		cancelled:    cancelled,
		allowFirst:   allowFirst,
	}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = blockSize
	cfg.Cache.MaxPopulateMemoryBytes = total
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	service := NewService(&mockForwarder{}, store, cfg)
	meta := &cache.CachedObjectMeta{ETag: `"v1"`, ContentLength: blockSize * blockCount, BlockSize: blockSize}
	if err := base.Put(context.Background(), cache.MakeBlockKey("bucket", "key", meta.ETag, meta.BlockSize, 0), []byte("AB"), 0); err != nil {
		t.Fatalf("seed first block: %v", err)
	}

	results := make(chan error, 1)
	go func() {
		writer := &failAfterWriter{ResponseWriter: httptest.NewRecorder(), remaining: 0}
		_, err := service.streamBlockRange(context.Background(), writer, "bucket", "key", "access", "secret", meta, 0, meta.ContentLength-1)
		results <- err
	}()

	waitForBlockReadStarts(t, firstStarted, 1)
	waitForBlockReadStarts(t, laterStarted, blockCount-1)
	close(allowFirst)
	select {
	case err := <-results:
		if err == nil {
			t.Fatal("client write failure did not reach the caller")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client write failure left prefetch workers blocked")
	}
	waitForBlockReadStarts(t, cancelled, blockCount-1)
	service.populateBudget.mu.Lock()
	remaining, inUse := service.populateBudget.remaining, service.populateBudget.stagingInUse
	service.populateBudget.mu.Unlock()
	if remaining != total || inUse != 0 {
		t.Fatalf("staging budget after cancelled prefetch = (remaining=%d, in_use=%d), want (%d, 0)", remaining, inUse, total)
	}
}
