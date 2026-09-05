package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	runtimemetrics "runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const remoteBlockMissBenchmarkSize = 1 << 20

const goUserCPUMetric = "/cpu/classes/user:cpu-seconds"

// goUserCPUCounter samples Go's process-wide user CPU accounting. The
// benchmark runs one request at a time and samples through remote put
// completion, so this includes CPU used by the detached gateway writer and the
// in-process peer fixture rather than only CPU before the HTTP response.
type goUserCPUCounter struct {
	samples []runtimemetrics.Sample
}

func newGoUserCPUCounter(tb testing.TB) *goUserCPUCounter {
	tb.Helper()
	counter := &goUserCPUCounter{samples: []runtimemetrics.Sample{{Name: goUserCPUMetric}}}
	_ = counter.nanoseconds(tb)
	return counter
}

func (c *goUserCPUCounter) nanoseconds(tb testing.TB) int64 {
	tb.Helper()
	runtimemetrics.Read(c.samples)
	if c.samples[0].Value.Kind() != runtimemetrics.KindFloat64 {
		tb.Fatalf("runtime metric %q kind = %v, want float64", goUserCPUMetric, c.samples[0].Value.Kind())
		return 0
	}
	return int64(c.samples[0].Value.Float64() * float64(time.Second))
}

// remoteBlockMissBenchmarkServer is a distinct cache-owner node for the
// end-to-end assembled-range miss workload. Metadata remains local to the
// gateway fixture while versioned block keys are served over this gRPC boundary.
type remoteBlockMissBenchmarkServer struct {
	pb.UnimplementedCacheServiceServer

	mu sync.RWMutex

	blocks map[string][]byte

	putStarted chan struct{}
	putDone    chan struct{}
	putGate    <-chan struct{}

	rpcs atomic.Int64
	gets atomic.Int64
}

func newRemoteBlockMissBenchmarkServer() *remoteBlockMissBenchmarkServer {
	s := &remoteBlockMissBenchmarkServer{blocks: make(map[string][]byte)}
	s.beginMiss()
	return s
}

// beginMiss clears per-miss counters without clearing other keys. Every timed
// iteration uses a new key, so a finishing detached writer can never join or
// populate the next iteration's cold miss.
func (s *remoteBlockMissBenchmarkServer) beginMiss() {
	s.mu.Lock()
	s.putStarted = make(chan struct{}, 1)
	s.putDone = make(chan struct{})
	s.putGate = nil
	s.mu.Unlock()
	s.rpcs.Store(0)
	s.gets.Store(0)
}

// blockPuts holds a remote write before it reaches storage. It lets the trace
// test prove that a client response does not wait for remote persistence.
func (s *remoteBlockMissBenchmarkServer) blockPuts() func() {
	gate := make(chan struct{})
	var once sync.Once
	s.mu.Lock()
	s.putGate = gate
	s.mu.Unlock()
	return func() { once.Do(func() { close(gate) }) }
}

func (s *remoteBlockMissBenchmarkServer) waitForGate(ctx context.Context) error {
	s.mu.RLock()
	gate := s.putGate
	s.mu.RUnlock()
	if gate == nil {
		return nil
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *remoteBlockMissBenchmarkServer) signalPutStarted() {
	s.mu.RLock()
	started := s.putStarted
	s.mu.RUnlock()
	select {
	case started <- struct{}{}:
	default:
	}
}

func (s *remoteBlockMissBenchmarkServer) store(key string, data []byte) {
	copyData := append([]byte(nil), data...)
	s.mu.Lock()
	s.blocks[key] = copyData
	done := s.putDone
	select {
	case <-done:
	default:
		close(done)
	}
	s.mu.Unlock()
}

func (s *remoteBlockMissBenchmarkServer) Put(stream pb.CacheService_PutServer) error {
	s.rpcs.Add(1)
	s.signalPutStarted()
	if err := s.waitForGate(stream.Context()); err != nil {
		return err
	}

	var key string
	var data []byte
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if key == "" {
			key = req.Key
		}
		data = append(data, req.Data...)
	}
	s.store(key, data)
	return stream.SendAndClose(&pb.PutResponse{Success: true})
}

func (s *remoteBlockMissBenchmarkServer) PutObject(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.rpcs.Add(1)
	s.signalPutStarted()
	if err := s.waitForGate(ctx); err != nil {
		return nil, err
	}
	s.store(req.Key, req.Data)
	return &pb.PutResponse{Success: true}, nil
}

func (s *remoteBlockMissBenchmarkServer) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	s.rpcs.Add(1)
	s.gets.Add(1)
	s.mu.RLock()
	data, ok := s.blocks[req.Key]
	s.mu.RUnlock()
	if !ok {
		return status.Error(codes.NotFound, "key not found")
	}

	start, end := req.Start, req.End
	if end <= 0 {
		end = int64(len(data)) - 1
	}
	if start < 0 || start >= int64(len(data)) || start > end {
		return status.Error(codes.InvalidArgument, "invalid range")
	}
	if end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	return stream.Send(&pb.GetResponse{Data: data[start : end+1]})
}

func (s *remoteBlockMissBenchmarkServer) waitForPut(tb testing.TB) {
	tb.Helper()
	s.mu.RLock()
	done := s.putDone
	s.mu.RUnlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		tb.Fatal("remote cache write did not complete")
	}
}

func (s *remoteBlockMissBenchmarkServer) waitForPutStart(tb testing.TB) {
	tb.Helper()
	s.mu.RLock()
	started := s.putStarted
	s.mu.RUnlock()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		tb.Fatal("remote cache write did not start")
	}
}

func (s *remoteBlockMissBenchmarkServer) hasBlock(key string, want []byte) bool {
	s.mu.RLock()
	got := s.blocks[key]
	s.mu.RUnlock()
	return bytes.Equal(got, want)
}

func (s *remoteBlockMissBenchmarkServer) deleteBlock(key string) {
	s.mu.Lock()
	delete(s.blocks, key)
	s.mu.Unlock()
}

type remoteMissStageEvent struct {
	name      string
	completed time.Duration
	duration  time.Duration
}

// remoteMissStageTrace records the cache probe/recheck/reread, upstream range,
// and remote cache write as timed spans. The sum deliberately includes the
// detached write, while ns/op remains the client-visible end-to-end GET time.
type remoteMissStageTrace struct {
	mu sync.Mutex

	started    time.Time
	cacheReads int
	events     []remoteMissStageEvent
	sum        time.Duration
}

func (t *remoteMissStageTrace) reset() {
	t.mu.Lock()
	t.started = time.Now()
	t.cacheReads = 0
	t.events = t.events[:0]
	t.sum = 0
	t.mu.Unlock()
}

func (t *remoteMissStageTrace) recordCacheRead(start time.Time) {
	t.record("cache-read", start, true)
}

func (t *remoteMissStageTrace) record(name string, start time.Time, cacheRead bool) {
	end := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if cacheRead {
		t.cacheReads++
		switch t.cacheReads {
		case 1:
			name = "cache-miss-probe"
		case 2:
			name = "fetch-presence-recheck"
		case 3:
			name = "post-fetch-cache-reread"
		default:
			name = "cache-read"
		}
	}
	t.events = append(t.events, remoteMissStageEvent{
		name:      name,
		completed: end.Sub(t.started),
		duration:  end.Sub(start),
	})
	t.sum += end.Sub(start)
}

func (t *remoteMissStageTrace) mark(name string) {
	t.mu.Lock()
	t.events = append(t.events, remoteMissStageEvent{name: name, completed: time.Since(t.started)})
	t.mu.Unlock()
}

func (t *remoteMissStageTrace) snapshot() (events []remoteMissStageEvent, stageSum time.Duration, cacheReads int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	events = append([]remoteMissStageEvent(nil), t.events...)
	sort.SliceStable(events, func(i, j int) bool { return events[i].completed < events[j].completed })
	return events, t.sum, t.cacheReads
}

func (t *remoteMissStageTrace) postFetchRereads() int {
	events, _, _ := t.snapshot()
	var rereads int
	for _, event := range events {
		if event.name == "post-fetch-cache-reread" {
			rereads++
		}
	}
	return rereads
}

func (t *remoteMissStageTrace) requireOrder(tb testing.TB, want ...string) {
	tb.Helper()
	events, _, _ := t.snapshot()
	next := 0
	for _, event := range events {
		if next < len(want) && event.name == want[next] {
			next++
		}
	}
	if next != len(want) {
		tb.Fatalf("waterfall = %s, missing ordered sequence %s", t.String(), strings.Join(want, " -> "))
	}
}

func (t *remoteMissStageTrace) String() string {
	events, _, _ := t.snapshot()
	parts := make([]string, 0, len(events))
	for _, event := range events {
		if event.duration == 0 {
			parts = append(parts, fmt.Sprintf("%s@%s", event.name, event.completed))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s@%s+%s", event.name, event.completed, event.duration))
	}
	return strings.Join(parts, " -> ")
}

// remoteBlockOwnerClient sends block keys to the remote cache owner while
// keeping metadata and tombstones local. That isolates the intended topology:
// this gateway has the object metadata, but the requested versioned block is
// owned by a peer.
type remoteBlockOwnerClient struct {
	cacheclient.CacheClient
	local cacheclient.CacheClient
	rpc   pb.CacheServiceClient
	trace *remoteMissStageTrace

	mu          sync.Mutex
	putReturned chan struct{}
}

func newRemoteBlockOwnerClient(remote, local cacheclient.CacheClient, rpc pb.CacheServiceClient, trace *remoteMissStageTrace) *remoteBlockOwnerClient {
	c := &remoteBlockOwnerClient{CacheClient: remote, local: local, rpc: rpc, trace: trace}
	c.beginMiss()
	return c
}

func (c *remoteBlockOwnerClient) beginMiss() {
	c.mu.Lock()
	c.putReturned = make(chan struct{})
	c.mu.Unlock()
}

func (c *remoteBlockOwnerClient) signalPutReturned() {
	c.mu.Lock()
	returned := c.putReturned
	select {
	case <-returned:
	default:
		close(returned)
	}
	c.mu.Unlock()
}

func (c *remoteBlockOwnerClient) waitForPutReturn(tb testing.TB) {
	tb.Helper()
	c.mu.Lock()
	returned := c.putReturned
	c.mu.Unlock()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		tb.Fatal("remote cache write did not return to the gateway")
	}
}

func isRemoteBenchmarkBlock(key string) bool { return strings.HasPrefix(key, "blk|") }

func (c *remoteBlockOwnerClient) IsLocal(string) bool { return false }

func (c *remoteBlockOwnerClient) Get(ctx context.Context, key string) ([]byte, error) {
	if !isRemoteBenchmarkBlock(key) {
		return c.local.Get(ctx, key)
	}
	return c.CacheClient.Get(ctx, key)
}

func (c *remoteBlockOwnerClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	if !isRemoteBenchmarkBlock(key) {
		return c.local.Put(ctx, key, data, ttlSeconds)
	}
	return c.putRemoteBlock(ctx, key, data, ttlSeconds)
}

func (c *remoteBlockOwnerClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	started := time.Now()
	err := c.CacheClient.GetRangeStream(ctx, key, start, end, w)
	c.trace.recordCacheRead(started)
	return err
}

func (c *remoteBlockOwnerClient) PutBlockBytes(ctx context.Context, key string, data []byte, ttlSeconds int64) (bool, error) {
	return true, c.putRemoteBlock(ctx, key, data, ttlSeconds)
}

func (c *remoteBlockOwnerClient) putRemoteBlock(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	started := time.Now()
	resp, err := c.rpc.PutObject(ctx, &pb.PutRequest{Key: key, Data: data, TtlSeconds: ttlSeconds})
	c.trace.record("remote-cache-put", started, false)
	c.signalPutReturned()
	if err != nil {
		return err
	}
	if resp != nil && !resp.Success {
		return fmt.Errorf("put failed: %s", resp.Error)
	}
	return nil
}

type timedReadCloser struct {
	io.ReadCloser
	onClose   func()
	remaining int64
	once      sync.Once
}

func (r *timedReadCloser) finish() { r.once.Do(r.onClose) }

func (r *timedReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if r.remaining > 0 {
		r.remaining -= int64(n)
		if r.remaining == 0 {
			r.finish()
		}
	}
	if err != nil {
		r.finish()
	}
	return n, err
}

func (r *timedReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.finish()
	return err
}

// benchmarkRangeForwarder supplies a real local HTTP upstream hop for aligned
// range fetches. The response span ends only after fetchOneBlock has consumed
// the body, so it includes the upstream payload transfer rather than just
// request construction.
type benchmarkRangeForwarder struct {
	upstreamURL string
	client      *http.Client
	trace       *remoteMissStageTrace
}

func (f *benchmarkRangeForwarder) Forward(context.Context, http.ResponseWriter, *http.Request) error {
	return errors.New("unexpected direct upstream forward")
}

func (f *benchmarkRangeForwarder) ForwardWithCapture(context.Context, http.ResponseWriter, *http.Request) (*proxy.ResponseCapture, error) {
	return nil, errors.New("unexpected captured upstream forward")
}

func (*benchmarkRangeForwarder) ValidateAndGetCredentials(*http.Request) (proxy.AuthResult, string, string, error) {
	return proxy.AuthValidated, "access", "secret", nil
}

func (*benchmarkRangeForwarder) DoRequestWithCreds(context.Context, *http.Request, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected request with credentials")
}

func (*benchmarkRangeForwarder) DoFullObjectRequest(context.Context, string, string, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected full object request")
}

func (*benchmarkRangeForwarder) DoObjectDeleteRequest(context.Context, string, string, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected object delete request")
}

func (*benchmarkRangeForwarder) DoAnonymousFullObjectRequest(context.Context, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected anonymous full object request")
}

func (f *benchmarkRangeForwarder) DoConditionalGetRequest(ctx context.Context, bucket, key, _, _, _ string, _ int64, rangeHeader string) (*http.Response, error) {
	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.upstreamURL+"/"+url.PathEscape(bucket)+"/"+url.PathEscape(key), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", rangeHeader)
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &timedReadCloser{ReadCloser: resp.Body, remaining: resp.ContentLength, onClose: func() {
		f.trace.record("upstream-fetch", started, false)
	}}
	return resp, nil
}

func (*benchmarkRangeForwarder) DoConditionalHeadRequest(context.Context, string, string, string, string, string, int64) (*http.Response, error) {
	return nil, errors.New("unexpected conditional head request")
}

type remoteBlockMissBenchmarkFixture struct {
	owner        *remoteBlockMissBenchmarkServer
	remoteClient *remoteBlockOwnerClient
	cache        *cache.Cache
	gateway      *httptest.Server
	client       *http.Client
	trace        *remoteMissStageTrace
	body         []byte
	etag         string
	blockLen     int64
}

func newRemoteBlockMissBenchmarkFixture(tb testing.TB) *remoteBlockMissBenchmarkFixture {
	tb.Helper()

	trace := &remoteMissStageTrace{}
	owner := newRemoteBlockMissBenchmarkServer()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(cacheclient.MaxMessageSize))
	pb.RegisterCacheServiceServer(grpcServer, owner)
	go func() { _ = grpcServer.Serve(listener) }()
	tb.Cleanup(grpcServer.Stop)
	tb.Cleanup(func() { _ = listener.Close() })

	readClient, err := cacheclient.NewSimpleClient(&cacheclient.ClientConfig{
		Addrs:              []string{listener.Addr().String()},
		Mode:               cacheclient.ModeSimple,
		ConnectionPoolSize: 1,
	})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = readClient.Close() })

	conn, err := grpc.Dial(listener.Addr().String(), cacheclient.DefaultDialOptions()...)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = conn.Close() })

	body := make([]byte, remoteBlockMissBenchmarkSize)
	for i := range body {
		body[i] = byte(i)
	}
	const etag = `"benchmark"`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end, err := parseBenchmarkRange(r.Header.Get("Range"), int64(len(body)))
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[start : end+1])
	}))
	tb.Cleanup(upstream.Close)
	upstreamClient := upstream.Client()
	tb.Cleanup(upstreamClient.CloseIdleConnections)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = int64(len(body))
	cfg.Cache.SizeThreshold = int64(len(body))
	remoteClient := newRemoteBlockOwnerClient(readClient, cacheclient.NewMemoryCache(), pb.NewCacheServiceClient(conn), trace)
	cacheStore := cache.NewCacheWithClient(remoteClient, &cfg.Cache)
	forwarder := &benchmarkRangeForwarder{upstreamURL: upstream.URL, client: upstreamClient, trace: trace}
	service := proxy.NewService(forwarder, cacheStore, cfg)
	gateway := httptest.NewServer(NewServer(service, "127.0.0.1", 0, false, 0).Router())
	tb.Cleanup(gateway.Close)

	return &remoteBlockMissBenchmarkFixture{
		owner:        owner,
		remoteClient: remoteClient,
		cache:        cacheStore,
		gateway:      gateway,
		client:       gateway.Client(),
		trace:        trace,
		body:         body,
		etag:         etag,
		blockLen:     int64(len(body)),
	}
}

func (f *remoteBlockMissBenchmarkFixture) beginMiss() {
	f.owner.beginMiss()
	f.remoteClient.beginMiss()
}

func parseBenchmarkRange(header string, size int64) (int64, int64, error) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, fmt.Errorf("missing byte range %q", header)
	}
	parts := strings.Split(strings.TrimPrefix(header, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid byte range %q", header)
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if start < 0 || end < start || end >= size {
		return 0, 0, fmt.Errorf("range %d-%d outside %d-byte object", start, end, size)
	}
	return start, end, nil
}

func (f *remoteBlockMissBenchmarkFixture) seedMeta(tb testing.TB, bucket, key string) *cache.CachedObjectMeta {
	tb.Helper()
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          f.etag,
		ContentLength: f.blockLen,
		StatusCode:    http.StatusOK,
		BlockSize:     f.blockLen,
	}
	if wrote, err := f.cache.PutMetaTombstoneAware(context.Background(), bucket, key, meta, 60, time.Now().UnixNano()); err != nil || !wrote {
		tb.Fatalf("seed block metadata = (wrote=%t, err=%v)", wrote, err)
	}
	return meta
}

func (f *remoteBlockMissBenchmarkFixture) getRange(ctx context.Context, bucket, key string) (status int, cacheStatus string, body []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.gateway.URL+"/"+url.PathEscape(bucket)+"/"+url.PathEscape(key), nil)
	if err != nil {
		return 0, "", nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", f.blockLen-1))
	resp, err := f.client.Do(req)
	if err != nil {
		return 0, "", nil, err
	}
	f.trace.mark("response-headers")
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	f.trace.mark("response-complete")
	if readErr != nil {
		return resp.StatusCode, resp.Header.Get("X-Cache"), body, readErr
	}
	if closeErr != nil {
		return resp.StatusCode, resp.Header.Get("X-Cache"), body, closeErr
	}
	return resp.StatusCode, resp.Header.Get("X-Cache"), body, nil
}

func (f *remoteBlockMissBenchmarkFixture) requireResponse(tb testing.TB, status int, cacheStatus string, got []byte) {
	tb.Helper()
	if status != http.StatusPartialContent || cacheStatus != proxy.XCacheHit || !bytes.Equal(got, f.body) {
		tb.Fatalf("response = (status=%d, cache=%q, bytes=%d), want (206, HIT, %d exact bytes)", status, cacheStatus, len(got), len(f.body))
	}
}

// warm establishes the gateway HTTP, upstream HTTP, and remote cache gRPC
// connections without sharing a block or fetch state with a timed iteration.
func (f *remoteBlockMissBenchmarkFixture) warm(tb testing.TB) {
	tb.Helper()
	const (
		bucket = "benchmark"
		key    = "warmup"
	)
	meta := f.seedMeta(tb, bucket, key)
	blockKey := cache.MakeBlockKey(bucket, key, meta.ETag, meta.BlockSize, 0)
	f.beginMiss()
	f.trace.reset()
	statusCode, cacheStatus, got, err := f.getRange(context.Background(), bucket, key)
	if err != nil {
		tb.Fatalf("warm handler GET = %v", err)
	}
	f.requireResponse(tb, statusCode, cacheStatus, got)
	f.owner.waitForPut(tb)
	f.remoteClient.waitForPutReturn(tb)
	f.owner.deleteBlock(blockKey)
}

// TestRemoteBlockMissTraceStages emits the ordinary handler-path waterfall for
// both sides of the comparison. Before direct assembly it includes the serial
// presence recheck and post-fetch reread; the known-missing assembled path
// reports both redundant reads as absent.
func TestRemoteBlockMissTraceStages(t *testing.T) {
	fixture := newRemoteBlockMissBenchmarkFixture(t)
	const (
		bucket = "benchmark"
		key    = "trace-stages"
	)
	meta := fixture.seedMeta(t, bucket, key)
	fixture.beginMiss()
	fixture.trace.reset()

	statusCode, cacheStatus, got, err := fixture.getRange(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("GET through handlers.Server = %v", err)
	}
	fixture.requireResponse(t, statusCode, cacheStatus, got)
	fixture.owner.waitForPut(t)
	fixture.remoteClient.waitForPutReturn(t)
	blockKey := cache.MakeBlockKey(bucket, key, meta.ETag, meta.BlockSize, 0)
	if !fixture.owner.hasBlock(blockKey, fixture.body) {
		t.Fatal("remote owner was not populated")
	}

	events, stageSum, cacheReads := fixture.trace.snapshot()
	rereads := fixture.trace.postFetchRereads()
	switch rereads {
	case 0:
		if cacheReads != 1 {
			t.Fatalf("cache reads without redundant rereads = %d, want 1", cacheReads)
		}
		fixture.trace.requireOrder(t, "cache-miss-probe", "upstream-fetch", "remote-cache-put")
	case 1:
		if cacheReads != 3 {
			t.Fatalf("cache reads with a presence recheck and post-fetch reread = %d, want 3", cacheReads)
		}
		fixture.trace.requireOrder(t, "cache-miss-probe", "fetch-presence-recheck", "upstream-fetch", "remote-cache-put", "post-fetch-cache-reread", "response-complete")
	default:
		t.Fatalf("post-fetch cache rereads = %d, want 0 or 1", rereads)
	}
	if gotGets := fixture.owner.gets.Load(); gotGets != int64(cacheReads) {
		t.Fatalf("remote cache get RPCs = %d, trace reads = %d", gotGets, cacheReads)
	}
	if len(events) == 0 || stageSum <= 0 {
		t.Fatalf("trace = (events=%d, stage sum=%s), want timed stages", len(events), stageSum)
	}
	t.Logf("remote range-miss waterfall: %s; summed stage latency: %s; remote cache RPCs: %d", fixture.trace.String(), stageSum, fixture.owner.rpcs.Load())
}

// BenchmarkHandleGetObjectRemoteBlockMiss sends a normal range GET through
// handlers.Server.handleObject and proxy.Service.HandleGetObject. It uses a
// local metadata owner, a separate gRPC block owner, and a separate HTTP
// upstream. The per-iteration key prevents a finishing detached write from
// sharing state with the next cold miss. ns/op remains client-visible latency;
// go_user_cpu_ns/op separately covers CPU through detached put completion.
func BenchmarkHandleGetObjectRemoteBlockMiss(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	fixture := newRemoteBlockMissBenchmarkFixture(b)
	fixture.warm(b)
	const bucket = "benchmark"

	userCPU := newGoUserCPUCounter(b)
	var totalRPCs, totalGets, totalRereads, totalUserCPUNS int64
	var totalStageSum time.Duration
	b.SetBytes(fixture.blockLen)
	b.ResetTimer()
	for iteration := 0; b.Loop(); iteration++ {
		b.StopTimer()
		key := fmt.Sprintf("block-%d", iteration)
		meta := fixture.seedMeta(b, bucket, key)
		blockKey := cache.MakeBlockKey(bucket, key, meta.ETag, meta.BlockSize, 0)
		fixture.beginMiss()
		fixture.trace.reset()
		cpuStart := userCPU.nanoseconds(b)

		b.StartTimer()
		statusCode, cacheStatus, got, err := fixture.getRange(context.Background(), bucket, key)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		fixture.requireResponse(b, statusCode, cacheStatus, got)

		// The remote put is detached from ns/op, but it is part of the full
		// miss-stage accounting and must finish before this key is discarded.
		fixture.owner.waitForPut(b)
		fixture.remoteClient.waitForPutReturn(b)
		cpuEnd := userCPU.nanoseconds(b)
		if cpuEnd < cpuStart {
			b.Fatalf("Go user CPU moved backwards: start=%d end=%d", cpuStart, cpuEnd)
		}
		totalUserCPUNS += cpuEnd - cpuStart
		if !fixture.owner.hasBlock(blockKey, fixture.body) {
			b.Fatal("remote cache owner did not retain the fetched block")
		}
		events, stageSum, cacheReads := fixture.trace.snapshot()
		if len(events) == 0 || stageSum <= 0 || cacheReads < 1 {
			b.Fatalf("incomplete miss trace: events=%d stage_sum=%s cache_reads=%d", len(events), stageSum, cacheReads)
		}
		rereads := fixture.trace.postFetchRereads()
		switch {
		case rereads == 0 && cacheReads == 1:
		case rereads == 1 && cacheReads == 3:
		default:
			b.Fatalf("cache read shape = (reads=%d, post-fetch rereads=%d), want (1, 0) or (3, 1)", cacheReads, rereads)
		}
		totalRereads += int64(rereads)
		if gotGets := fixture.owner.gets.Load(); gotGets != int64(cacheReads) {
			b.Fatalf("remote cache get RPCs = %d, trace reads = %d", gotGets, cacheReads)
		}
		totalRPCs += fixture.owner.rpcs.Load()
		totalGets += fixture.owner.gets.Load()
		totalStageSum += stageSum
		fixture.owner.deleteBlock(blockKey)
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(totalUserCPUNS)/float64(b.N), "go_user_cpu_ns/op")
	b.ReportMetric(float64(totalStageSum.Nanoseconds())/float64(b.N), "miss_stage_sum_ns/op")
	b.ReportMetric(float64(totalRereads)/float64(b.N), "post_fetch_cache_rereads/op")
	b.ReportMetric(float64(totalGets)/float64(b.N), "remote_block_cache_get_rpcs/op")
	b.ReportMetric(float64(totalRPCs)/float64(b.N), "remote_cache_rpcs/op")
}
