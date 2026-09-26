package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
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

const rangeFanoutETag = `"range-fanout-v1"`

// rangeFanoutFixture keeps object metadata in a local memory cache while
// selected blocks are owned by a real in-process ocache gRPC server. Missing
// blocks also reach that owner, as they do when a remote cache client resolves
// them through a peer.
type rangeFanoutFixture struct {
	service   *Service
	store     *cache.Cache
	client    *rangeFanoutCacheClient
	remote    cacheclient.CacheClient
	owner     *rangeFanoutOwner
	forwarder *rangeFanoutForwarder
	meta      *cache.CachedObjectMeta
	body      []byte
	bucket    string
	key       string
	etag      string
	blockSize int64
}

func newRangeFanoutFixture(tb testing.TB, blockSize int64, blockCount int, cached []int, delay time.Duration) *rangeFanoutFixture {
	tb.Helper()

	const (
		bucket = "range-fanout"
		key    = "object"
		etag   = rangeFanoutETag
	)
	body := make([]byte, blockCount*int(blockSize))
	for i := range body {
		body[i] = byte(i*37 + 11)
	}

	blocks := make(map[string][]byte, len(cached))
	for _, index := range cached {
		if index < 0 || index >= blockCount {
			tb.Fatalf("cached block index %d outside [0,%d)", index, blockCount)
		}
		start := int64(index) * blockSize
		blocks[cache.MakeBlockKey(bucket, key, etag, blockSize, int64(index))] = body[start : start+blockSize]
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = listener.Close() })
	owner := &rangeFanoutOwner{blocks: blocks, delay: delay}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(cacheclient.MaxMessageSize))
	pb.RegisterCacheServiceServer(server, owner)
	go func() { _ = server.Serve(listener) }()
	tb.Cleanup(server.Stop)

	remote, err := cacheclient.NewSimpleClient(&cacheclient.ClientConfig{
		Addrs:              []string{listener.Addr().String()},
		Mode:               cacheclient.ModeSimple,
		ConnectionPoolSize: 1,
	})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = remote.Close() })

	local := &rangeFanoutCacheClient{
		CacheClient: cacheclient.NewMemoryCache(),
		remote:      remote,
		localKeys:   make(map[string]struct{}),
	}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = config.DefaultCacheSizeThreshold
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          etag,
		StatusCode:    http.StatusOK,
		ContentLength: int64(len(body)),
		BlockSize:     blockSize,
	}
	store := cache.NewCacheWithClient(local, &cfg.Cache)
	if wrote, err := store.PutMetaIfVersion(context.Background(), bucket, key, meta, 60, cache.VersionAny); err != nil || !wrote {
		tb.Fatalf("seed block-mode metadata: wrote=%v err=%v", wrote, err)
	}

	forwarder := &rangeFanoutForwarder{
		mockForwarder: &mockForwarder{},
		body:          body,
		etag:          etag,
	}
	return &rangeFanoutFixture{
		service:   NewService(forwarder, store, cfg),
		store:     store,
		client:    local,
		remote:    remote,
		owner:     owner,
		forwarder: forwarder,
		meta:      meta,
		body:      body,
		bucket:    bucket,
		key:       key,
		etag:      etag,
		blockSize: blockSize,
	}
}

// warmRemoteConnections opens the cache-owner RPC connection before benchmark
// timing. It bypasses the Cache wrapper and does not alter stored state.
func (f *rangeFanoutFixture) warmRemoteConnections() error {
	for _, index := range []int{0, 1} {
		if index >= int(f.meta.ContentLength/f.blockSize) {
			continue
		}
		_, cached := f.owner.blocks[cache.MakeBlockKey(f.bucket, f.key, f.etag, f.blockSize, int64(index))]
		key := cache.MakeBlockKey(f.bucket, f.key, f.etag, f.blockSize, int64(index))
		err := f.remote.GetRangeStream(context.Background(), key, 0, 1, io.Discard)
		if cached && err != nil {
			return fmt.Errorf("warm cached block %d: %w", index, err)
		}
		if !cached && status.Code(err) != codes.NotFound {
			return fmt.Errorf("warm missing block %d: got %v, want NotFound", index, err)
		}
	}
	return nil
}

func (f *rangeFanoutFixture) startProbeTracking() {
	f.client.trackProbes = true
	f.owner.trackProbes = true
}

// rangeFanoutOwner serves immutable block slices and can model a fixed cache-peer
// delay. Tests optionally record the exact byte-zero probe keys at the RPC owner.
type rangeFanoutOwner struct {
	pb.UnimplementedCacheServiceServer

	blocks      map[string][]byte
	delay       time.Duration
	trackProbes bool
	failKey     string
	mu          sync.Mutex
	probeKeys   []string
}

func (s *rangeFanoutOwner) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	probe := req.Start == 0 && req.End == 1
	if s.trackProbes && probe {
		s.mu.Lock()
		s.probeKeys = append(s.probeKeys, req.Key)
		s.mu.Unlock()
	}
	if s.delay > 0 {
		timer := time.NewTimer(s.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	if req.Key == s.failKey {
		return status.Error(codes.Unavailable, "injected remote cache read failure")
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

func (s *rangeFanoutOwner) recordedProbes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.probeKeys...)
}

// rangeFanoutCacheClient keeps metadata and newly fetched blocks local, while
// initially absent block keys are read from the in-process remote cache owner.
// IsLocal mirrors that ownership for the cache's locality counter.
type rangeFanoutCacheClient struct {
	cacheclient.CacheClient
	remote cacheclient.CacheClient

	mu          sync.RWMutex
	localKeys   map[string]struct{}
	trackProbes bool
	probeKeys   []string
	afterProbe  func(key string, err error)
}

func (c *rangeFanoutCacheClient) IsLocal(key string) bool {
	c.mu.RLock()
	_, ok := c.localKeys[key]
	c.mu.RUnlock()
	return ok
}

func (c *rangeFanoutCacheClient) markLocal(key string) {
	c.mu.Lock()
	c.localKeys[key] = struct{}{}
	c.mu.Unlock()
}

func (c *rangeFanoutCacheClient) recordProbe(key string) {
	c.mu.Lock()
	c.probeKeys = append(c.probeKeys, key)
	c.mu.Unlock()
}

func (c *rangeFanoutCacheClient) recordedProbes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]string(nil), c.probeKeys...)
}

func (c *rangeFanoutCacheClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	if err := c.CacheClient.Put(ctx, key, data, ttlSeconds); err != nil {
		return err
	}
	c.markLocal(key)
	return nil
}

func (c *rangeFanoutCacheClient) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	if err := c.CacheClient.PutStream(ctx, key, r, ttlSeconds); err != nil {
		return err
	}
	c.markLocal(key)
	return nil
}

func (c *rangeFanoutCacheClient) Delete(ctx context.Context, key string) error {
	if err := c.CacheClient.Delete(ctx, key); err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.localKeys, key)
	c.mu.Unlock()
	return nil
}

func rangeFanoutPrefix(n int) []int {
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	return indices
}

func (c *rangeFanoutCacheClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	probe := start == 0 && end == 1
	if probe && c.trackProbes {
		c.recordProbe(key)
	}
	if c.IsLocal(key) {
		return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
	}
	err := c.remote.GetRangeStream(ctx, key, start, end, w)
	if probe && c.afterProbe != nil {
		c.afterProbe(key, err)
	}
	return err
}

// rangeFanoutForwarder returns the exact requested 206 bytes without an origin
// network round trip, allowing the benchmark to isolate the block-cache probes.
// The request and response still traverse HandleGetObject's ordinary contracts.
type rangeFanoutForwarder struct {
	*mockForwarder
	body []byte
	etag string

	upstreamCalls atomic.Int64
	blockGets     atomic.Int64
	onDispatch    func()
}

func (f *rangeFanoutForwarder) DoRequestWithCreds(ctx context.Context, r *http.Request, _, _ string) (*http.Response, error) {
	if f.onDispatch != nil {
		f.onDispatch()
	}
	f.upstreamCalls.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.Header.Get("Range") == "" {
		return f.fullResponse(), nil
	}
	return f.rangeResponse(r.Header.Get("Range"))
}

func (f *rangeFanoutForwarder) DoConditionalGetRequest(ctx context.Context, _, _, _, _, _ string, _ int64, rangeHeader string) (*http.Response, error) {
	f.blockGets.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.rangeResponse(rangeHeader)
}

func (f *rangeFanoutForwarder) fullResponse() *http.Response {
	header := make(http.Header)
	header.Set("ETag", f.etag)
	header.Set("Content-Type", "application/octet-stream")
	header.Set("Content-Length", fmt.Sprintf("%d", len(f.body)))
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(f.body)),
		ContentLength: int64(len(f.body)),
	}
}

func (f *rangeFanoutForwarder) rangeResponse(rangeHeader string) (*http.Response, error) {
	ranges, err := parseRangeHeader(rangeHeader, int64(len(f.body)))
	if err != nil {
		return nil, fmt.Errorf("parse range %q: %w", rangeHeader, err)
	}
	if len(ranges) != 1 {
		return nil, fmt.Errorf("range %q produced %d ranges, want one", rangeHeader, len(ranges))
	}
	rng := ranges[0]
	length := rng.end - rng.start + 1
	header := make(http.Header)
	header.Set("ETag", f.etag)
	header.Set("Content-Type", "application/octet-stream")
	header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.end, len(f.body)))
	header.Set("Content-Length", fmt.Sprintf("%d", length))
	return &http.Response{
		StatusCode:    http.StatusPartialContent,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(f.body[rng.start : rng.end+1])),
		ContentLength: length,
	}, nil
}

// rangeFanoutSink is a reusable in-memory HTTP response writer. It copies the
// full origin range body, so the timed handler includes response streaming work.
type rangeFanoutSink struct {
	header     http.Header
	body       []byte
	statusCode int
	written    int
}

func newRangeFanoutSink(bodyLen int) *rangeFanoutSink {
	return &rangeFanoutSink{header: make(http.Header), body: make([]byte, bodyLen)}
}

func (w *rangeFanoutSink) Header() http.Header { return w.header }

func (w *rangeFanoutSink) WriteHeader(statusCode int) {
	if w.statusCode == 0 {
		w.statusCode = statusCode
	}
}

func (w *rangeFanoutSink) Write(p []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	if len(p) > len(w.body)-w.written {
		return 0, io.ErrShortWrite
	}
	n := copy(w.body[w.written:], p)
	w.written += n
	return n, nil
}

func (w *rangeFanoutSink) reset() {
	for key := range w.header {
		delete(w.header, key)
	}
	w.statusCode = 0
	w.written = 0
}
