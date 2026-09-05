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
	"runtime/metrics"
	"strings"
	"sync"
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

const (
	handlerPrefetchBenchmarkBlockSize = 64 << 10
	handlerPrefetchBenchmarkReadDelay = 2 * time.Millisecond
)

// handlerPrefetchReadTrace records only full block data reads. The byte-zero
// presence probes on the normal range path are intentionally excluded: they
// are a separate serial preflight, while these spans are the GetRangeStream
// calls that stream cached block bytes to the client.
type handlerPrefetchReadTrace struct {
	mu sync.Mutex

	started     time.Time
	inFlight    int
	maxInFlight int
	spans       []handlerPrefetchReadSpan
}

type handlerPrefetchReadSpan struct {
	key      string
	started  time.Duration
	finished time.Duration
}

func (t *handlerPrefetchReadTrace) reset() {
	t.mu.Lock()
	t.started = time.Now()
	t.inFlight = 0
	t.maxInFlight = 0
	t.spans = t.spans[:0]
	t.mu.Unlock()
}

func (t *handlerPrefetchReadTrace) begin(key string) func() {
	t.mu.Lock()
	span := len(t.spans)
	t.spans = append(t.spans, handlerPrefetchReadSpan{key: key, started: time.Since(t.started)})
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

func (t *handlerPrefetchReadTrace) peak() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.maxInFlight
}

func (t *handlerPrefetchReadTrace) String() string {
	t.mu.Lock()
	spans := append([]handlerPrefetchReadSpan(nil), t.spans...)
	t.mu.Unlock()

	parts := make([]string, 0, len(spans))
	for _, span := range spans {
		parts = append(parts, fmt.Sprintf("block %s@%s-%s", handlerPrefetchBlockIndex(span.key), span.started, span.finished))
	}
	return strings.Join(parts, " -> ")
}

// handlerPrefetchReadController supplies a comparable cache-read delay to the
// local fixture and, after the RPC has reached the peer, to the remote fixture.
type handlerPrefetchReadController struct {
	mu sync.Mutex

	delay   time.Duration
	gate    <-chan struct{}
	started chan<- struct{}
	trace   *handlerPrefetchReadTrace
}

func (c *handlerPrefetchReadController) configure(delay time.Duration, gate <-chan struct{}, started chan<- struct{}) {
	c.mu.Lock()
	c.delay = delay
	c.gate = gate
	c.started = started
	c.mu.Unlock()
}

func (c *handlerPrefetchReadController) begin(ctx context.Context, key string) (func(), error) {
	finish := c.trace.begin(key)

	c.mu.Lock()
	delay, gate, started := c.delay, c.gate, c.started
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
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return finish, ctx.Err()
		}
	}
	return finish, nil
}

func handlerPrefetchDataRead(start, end int64) bool {
	return end-start+1 > 2
}

// handlerPrefetchBusyCPUTime is the process-wide Go CPU time spent running
// code, including the in-process HTTP client, handler, and remote gRPC owner.
// runtime/metrics reports this portably as available CPU time less idle CPU
// time. It is an estimate rather than operating-system CPU accounting, but is
// a comparable guardrail for the paired benchmark runs.
func handlerPrefetchBusyCPUTime(tb testing.TB) time.Duration {
	tb.Helper()
	samples := []metrics.Sample{
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/cpu/classes/idle:cpu-seconds"},
	}
	metrics.Read(samples)
	if samples[0].Value.Kind() != metrics.KindFloat64 || samples[1].Value.Kind() != metrics.KindFloat64 {
		tb.Fatalf("runtime CPU metrics unavailable: total=%v idle=%v", samples[0].Value.Kind(), samples[1].Value.Kind())
	}
	busySeconds := samples[0].Value.Float64() - samples[1].Value.Float64()
	if busySeconds < 0 {
		tb.Fatalf("runtime CPU metrics reported negative busy time: %f", busySeconds)
	}
	return time.Duration(busySeconds * float64(time.Second))
}

// handlerPrefetchBlockServer is an immutable remote cache owner. Its data-read
// span starts inside the real gRPC handler, after the request has crossed the
// client boundary; tiny BlockExistsErr probes do not enter the timed trace.
type handlerPrefetchBlockServer struct {
	pb.UnimplementedCacheServiceServer

	blocks     map[string][]byte
	controller *handlerPrefetchReadController
}

func (s *handlerPrefetchBlockServer) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	var finish func()
	if handlerPrefetchDataRead(req.Start, req.End) {
		var err error
		finish, err = s.controller.begin(stream.Context(), req.Key)
		if err != nil {
			return err
		}
		defer finish()
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

// handlerPrefetchCacheClient keeps metadata local while routing versioned
// blocks through either a local memory owner or a real gRPC owner.
type handlerPrefetchCacheClient struct {
	cacheclient.CacheClient

	local      cacheclient.CacheClient
	remote     bool
	controller *handlerPrefetchReadController
}

func handlerPrefetchBlockKey(key string) bool { return strings.HasPrefix(key, "blk|") }

func (c *handlerPrefetchCacheClient) IsLocal(key string) bool {
	return !handlerPrefetchBlockKey(key) || !c.remote
}

func (c *handlerPrefetchCacheClient) Get(ctx context.Context, key string) ([]byte, error) {
	if !handlerPrefetchBlockKey(key) {
		return c.local.Get(ctx, key)
	}
	return c.CacheClient.Get(ctx, key)
}

func (c *handlerPrefetchCacheClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	if !handlerPrefetchBlockKey(key) {
		return c.local.Put(ctx, key, data, ttlSeconds)
	}
	return c.CacheClient.Put(ctx, key, data, ttlSeconds)
}

func (c *handlerPrefetchCacheClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if !handlerPrefetchBlockKey(key) {
		return c.local.GetRangeStream(ctx, key, start, end, w)
	}
	if !c.remote && handlerPrefetchDataRead(start, end) {
		finish, err := c.controller.begin(ctx, key)
		defer finish()
		if err != nil {
			return err
		}
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

// handlerPrefetchForwarder is only reached for auth on this warm-cache
// benchmark. Any upstream operation is a fixture failure.
type handlerPrefetchForwarder struct{}

func (handlerPrefetchForwarder) Forward(context.Context, http.ResponseWriter, *http.Request) error {
	return errors.New("unexpected upstream forward")
}

func (handlerPrefetchForwarder) ForwardWithCapture(context.Context, http.ResponseWriter, *http.Request) (*proxy.ResponseCapture, error) {
	return nil, errors.New("unexpected upstream capture")
}

func (handlerPrefetchForwarder) ValidateAndGetCredentials(*http.Request) (proxy.AuthResult, string, string, error) {
	return proxy.AuthValidated, "access", "secret", nil
}

func (handlerPrefetchForwarder) DoRequestWithCreds(context.Context, *http.Request, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected request with credentials")
}

func (handlerPrefetchForwarder) DoFullObjectRequest(context.Context, string, string, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected full object request")
}

func (handlerPrefetchForwarder) DoObjectDeleteRequest(context.Context, string, string, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected object delete request")
}

func (handlerPrefetchForwarder) DoAnonymousFullObjectRequest(context.Context, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected anonymous full object request")
}

func (handlerPrefetchForwarder) DoConditionalGetRequest(context.Context, string, string, string, string, string, int64, string) (*http.Response, error) {
	return nil, errors.New("unexpected conditional request")
}

func (handlerPrefetchForwarder) DoConditionalHeadRequest(context.Context, string, string, string, string, string, int64) (*http.Response, error) {
	return nil, errors.New("unexpected conditional head request")
}

type handlerPrefetchBenchmarkFixture struct {
	gateway    *httptest.Server
	client     *http.Client
	pacer      *handlerPrefetchResponsePacer
	body       []byte
	trace      *handlerPrefetchReadTrace
	controller *handlerPrefetchReadController
}

func newHandlerPrefetchBenchmarkFixture(tb testing.TB, remote bool, blockCount int) *handlerPrefetchBenchmarkFixture {
	tb.Helper()

	trace := &handlerPrefetchReadTrace{}
	controller := &handlerPrefetchReadController{trace: trace}
	body := make([]byte, blockCount*handlerPrefetchBenchmarkBlockSize)
	for block := 0; block < blockCount; block++ {
		for i := range body[block*handlerPrefetchBenchmarkBlockSize : (block+1)*handlerPrefetchBenchmarkBlockSize] {
			body[block*handlerPrefetchBenchmarkBlockSize+i] = byte(block)
		}
	}

	const (
		bucket = "benchmark"
		key    = "prefetch"
		etag   = `"prefetch"`
	)
	blockSize := int64(handlerPrefetchBenchmarkBlockSize)

	var blockClient cacheclient.CacheClient
	if remote {
		blocks := make(map[string][]byte, blockCount)
		for i := 0; i < blockCount; i++ {
			blockKey := cache.MakeBlockKey(bucket, key, etag, blockSize, int64(i))
			blocks[blockKey] = body[i*handlerPrefetchBenchmarkBlockSize : (i+1)*handlerPrefetchBenchmarkBlockSize]
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatal(err)
		}
		server := grpc.NewServer(grpc.MaxRecvMsgSize(cacheclient.MaxMessageSize))
		pb.RegisterCacheServiceServer(server, &handlerPrefetchBlockServer{blocks: blocks, controller: controller})
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
		blockClient = remoteClient
	} else {
		localClient := cacheclient.NewMemoryCache()
		for i := 0; i < blockCount; i++ {
			blockKey := cache.MakeBlockKey(bucket, key, etag, blockSize, int64(i))
			start := i * handlerPrefetchBenchmarkBlockSize
			if err := localClient.Put(context.Background(), blockKey, body[start:start+handlerPrefetchBenchmarkBlockSize], 0); err != nil {
				tb.Fatalf("seed local block %d: %v", i, err)
			}
		}
		blockClient = localClient
	}

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = int64(len(body))
	client := &handlerPrefetchCacheClient{
		CacheClient: blockClient,
		local:       cacheclient.NewMemoryCache(),
		remote:      remote,
		controller:  controller,
	}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          etag,
		ContentLength: int64(len(body)),
		StatusCode:    http.StatusOK,
		BlockSize:     blockSize,
	}
	if wrote, err := store.PutMetaTombstoneAware(context.Background(), bucket, key, meta, 60, time.Now().UnixNano()); err != nil || !wrote {
		tb.Fatalf("seed block metadata = (wrote=%t, err=%v)", wrote, err)
	}
	service := proxy.NewService(handlerPrefetchForwarder{}, store, cfg)
	pacer := &handlerPrefetchResponsePacer{handler: NewServer(service, "127.0.0.1", 0, false, 0).Router()}
	gateway := httptest.NewServer(pacer)
	tb.Cleanup(gateway.Close)
	httpClient := gateway.Client()
	tb.Cleanup(httpClient.CloseIdleConnections)

	return &handlerPrefetchBenchmarkFixture{
		gateway:    gateway,
		client:     httpClient,
		pacer:      pacer,
		body:       body,
		trace:      trace,
		controller: controller,
	}
}

// handlerPrefetchResponsePacer models a peer that consumes the response at a
// bounded rate. It wraps the real HTTP server's ResponseWriter, so the timed
// path still includes request parsing, routing, headers, transport, and client
// body reads.
type handlerPrefetchResponsePacer struct {
	handler http.Handler

	mu    sync.RWMutex
	delay time.Duration
}

func (p *handlerPrefetchResponsePacer) configure(delay time.Duration) {
	p.mu.Lock()
	p.delay = delay
	p.mu.Unlock()
}

func (p *handlerPrefetchResponsePacer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.RLock()
	delay := p.delay
	p.mu.RUnlock()
	p.handler.ServeHTTP(handlerPrefetchDelayedWriter{ResponseWriter: w, delay: delay}, r)
}

type handlerPrefetchDelayedWriter struct {
	http.ResponseWriter
	delay time.Duration
}

func (w handlerPrefetchDelayedWriter) Write(p []byte) (int, error) {
	if w.delay > 0 {
		time.Sleep(w.delay)
	}
	return w.ResponseWriter.Write(p)
}

func (w handlerPrefetchDelayedWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (f *handlerPrefetchBenchmarkFixture) getRange() (int, string, []byte, error) {
	req, err := http.NewRequest(http.MethodGet, f.gateway.URL+"/benchmark/prefetch", nil)
	if err != nil {
		return 0, "", nil, err
	}
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request, Signature=deadbeef")
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", len(f.body)-1))

	resp, err := f.client.Do(req)
	if err != nil {
		return 0, "", nil, err
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return resp.StatusCode, resp.Header.Get("X-Cache"), body, readErr
	}
	if closeErr != nil {
		return resp.StatusCode, resp.Header.Get("X-Cache"), body, closeErr
	}
	return resp.StatusCode, resp.Header.Get("X-Cache"), body, nil
}

func (f *handlerPrefetchBenchmarkFixture) warm(tb testing.TB) {
	tb.Helper()
	f.trace.reset()
	f.controller.configure(0, nil, nil)
	f.pacer.configure(0)
	status, cacheStatus, body, err := f.getRange()
	f.requireResponse(tb, status, cacheStatus, body, err)
}

func (f *handlerPrefetchBenchmarkFixture) requireResponse(tb testing.TB, status int, cacheStatus string, body []byte, err error) {
	tb.Helper()
	if err != nil || status != http.StatusPartialContent || cacheStatus != proxy.XCacheHit || !bytes.Equal(body, f.body) {
		tb.Fatalf("handler range response = (status=%d, cache=%q, bytes=%d, exact=%t, err=%v), want (206, HIT, %d exact bytes)", status, cacheStatus, len(body), bytes.Equal(body, f.body), err, len(f.body))
	}
}

// BenchmarkHandleGetObjectBlockPrefetch sends a real HTTP GET through
// handlers.Server.handleObject, proxy.Service.HandleGetObject, metadata lookup,
// range validation/probing, and the cache stream. It sweeps block count, owner
// locality, and response write rate; max_cache_reads/op is the full-block
// GetRangeStream trace peak, while cpu-ns/op is the paired process CPU
// guardrail.
func BenchmarkHandleGetObjectBlockPrefetch(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	localities := []struct {
		name   string
		remote bool
	}{
		{name: "local", remote: false},
		{name: "remote", remote: true},
	}
	writers := []struct {
		name  string
		delay time.Duration
	}{
		{name: "fast_client"},
		{name: "slow_client", delay: handlerPrefetchBenchmarkReadDelay},
	}

	for _, locality := range localities {
		b.Run(locality.name, func(b *testing.B) {
			for _, blockCount := range []int{2, 4, 8, 16, 32} {
				b.Run(fmt.Sprintf("%d_blocks", blockCount), func(b *testing.B) {
					for _, writer := range writers {
						b.Run(writer.name, func(b *testing.B) {
							fixture := newHandlerPrefetchBenchmarkFixture(b, locality.remote, blockCount)
							fixture.warm(b)

							var totalPeak int64
							var totalCPU time.Duration
							b.SetBytes(int64(len(fixture.body)))
							b.ResetTimer()
							for b.Loop() {
								b.StopTimer()
								fixture.trace.reset()
								fixture.controller.configure(handlerPrefetchBenchmarkReadDelay, nil, nil)
								fixture.pacer.configure(writer.delay)
								cpuStart := handlerPrefetchBusyCPUTime(b)
								b.StartTimer()
								status, cacheStatus, body, err := fixture.getRange()
								b.StopTimer()
								cpuEnd := handlerPrefetchBusyCPUTime(b)
								fixture.requireResponse(b, status, cacheStatus, body, err)
								peak := fixture.trace.peak()
								if peak < 1 {
									b.Fatal("cache-read trace recorded no full-block spans")
								}
								totalPeak += int64(peak)
								totalCPU += cpuEnd - cpuStart
								b.StartTimer()
							}
							b.StopTimer()
							b.ReportMetric(float64(totalPeak)/float64(b.N), "max_cache_reads/op")
							b.ReportMetric(float64(totalCPU)/float64(b.N), "cpu-ns/op")
						})
					}
				})
			}
		})
	}
}

func handlerPrefetchBlockIndex(key string) string {
	if at := strings.LastIndexByte(key, '|'); at >= 0 {
		return key[at+1:]
	}
	return key
}
