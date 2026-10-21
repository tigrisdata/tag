package handlers

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/ocache/embedded"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// certifiedRangeHandlerForwarder supplies exact 206 responses for the cold Range
// populate and its aligned block reads. The benchmark delegates auth validation to
// the production transparent forwarder; only the focused handler test uses the nil
// delegate to bypass authentication.
type certifiedRangeHandlerForwarder struct {
	proxy.RequestForwarder
	body  []byte
	etag  string
	calls atomic.Int64
}

func (f *certifiedRangeHandlerForwarder) ValidateAndGetCredentials(r *http.Request) (proxy.AuthResult, string, string, error) {
	if f.RequestForwarder != nil {
		return f.RequestForwarder.ValidateAndGetCredentials(r)
	}
	return proxy.AuthValidated, "access", "secret", nil
}

func (f *certifiedRangeHandlerForwarder) DoRequestWithCreds(_ context.Context, r *http.Request, _, _ string) (*http.Response, error) {
	return f.rangeResponse(r.Header.Get("Range"))
}

func (f *certifiedRangeHandlerForwarder) DoConditionalGetRequest(_ context.Context, _, _, _, _, _ string, _ int64, rangeHeader string) (*http.Response, error) {
	return f.rangeResponse(rangeHeader)
}

func (f *certifiedRangeHandlerForwarder) rangeResponse(rangeHeader string) (*http.Response, error) {
	f.calls.Add(1)
	var start, end int64
	if _, err := fmt.Sscanf(strings.TrimPrefix(rangeHeader, "bytes="), "%d-%d", &start, &end); err != nil {
		return nil, fmt.Errorf("parse fixture range %q: %w", rangeHeader, err)
	}
	total := int64(len(f.body))
	if start < 0 || start >= total || end < start {
		return nil, fmt.Errorf("invalid fixture range %q", rangeHeader)
	}
	if end >= total {
		end = total - 1
	}
	h := http.Header{}
	h.Set("ETag", f.etag)
	h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	h.Set("Content-Length", fmt.Sprintf("%d", end-start+1))
	h.Set("Content-Type", "application/octet-stream")
	return &http.Response{
		StatusCode:    http.StatusPartialContent,
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(f.body[start : end+1])),
		ContentLength: end - start + 1,
	}, nil
}

// certifiedRangeRemoteOwner is a loopback gRPC cache owner for the benchmark.
// It stores writes synchronously and applies the same configured delay to every
// Get RPC, including byte-zero presence probes and payload reads.
type certifiedRangeRemoteOwner struct {
	pb.UnimplementedCacheServiceServer

	mu       sync.RWMutex
	blocks   map[string][]byte
	getDelay time.Duration
}

func (s *certifiedRangeRemoteOwner) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	if s.getDelay > 0 {
		timer := time.NewTimer(s.getDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}

	s.mu.RLock()
	data, ok := s.blocks[req.Key]
	s.mu.RUnlock()
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

func (s *certifiedRangeRemoteOwner) PutObject(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.store(req.Key, req.Data)
	return &pb.PutResponse{Success: true}, nil
}

func (s *certifiedRangeRemoteOwner) Put(stream pb.CacheService_PutServer) error {
	var key string
	var data bytes.Buffer
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if req.Key != "" {
			key = req.Key
		}
		if _, err := data.Write(req.Data); err != nil {
			return err
		}
	}
	if key == "" {
		return status.Error(codes.InvalidArgument, "missing cache key")
	}
	s.store(key, data.Bytes())
	return stream.SendAndClose(&pb.PutResponse{Success: true})
}

func (s *certifiedRangeRemoteOwner) store(key string, data []byte) {
	copyOfData := append([]byte(nil), data...)
	s.mu.Lock()
	s.blocks[key] = copyOfData
	s.mu.Unlock()
}

// certifiedRangeBenchmarkCacheClient reuses the handler benchmark's metadata/block
// router while leaving cache reads uninstrumented in the measured request.
type certifiedRangeBenchmarkCacheClient struct {
	*handlerPrefetchCacheClient
}

func (c *certifiedRangeBenchmarkCacheClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if !handlerPrefetchBlockKey(key) {
		return c.local.GetRangeStream(ctx, key, start, end, w)
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

type certifiedRangeHandlerBenchmarkFixture struct {
	gateway       *httptest.Server
	client        *http.Client
	store         *cache.Cache
	forwarder     *certifiedRangeHandlerForwarder
	signedRequest *http.Request
	body          []byte
	bucket        string
	key           string
	etag          string
	blockSize     int64
	rangeLen      int64
	rangeHeader   string
	expectedCRC   uint32
}

func newCertifiedRangeHandlerBenchmarkFixture(tb testing.TB, remote bool, blocks int) *certifiedRangeHandlerBenchmarkFixture {
	tb.Helper()
	const (
		bucket = "benchmark"
		key    = "range-cache"
		etag   = `"range-v1"`
	)
	blockSize := int64(config.DefaultCacheBlockSize)
	rangeLen := int64(blocks) * blockSize
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	body := make([]byte, rangeLen+blockSize)
	for i := range body {
		body[i] = byte(i*31 + i/257)
	}

	localOwner, err := embedded.New(&embedded.Config{DiskPath: tb.TempDir(), TTL: cfg.Cache.TTL})
	if err != nil {
		tb.Fatalf("create embedded cache owner: %v", err)
	}
	tb.Cleanup(func() { _ = localOwner.Close() })
	localMeta := cacheclient.CacheClient(localOwner)
	var blockOwner cacheclient.CacheClient
	if remote {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatal(err)
		}
		owner := &certifiedRangeRemoteOwner{blocks: make(map[string][]byte, blocks), getDelay: handlerPrefetchBenchmarkReadDelay}
		grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(cacheclient.MaxMessageSize))
		pb.RegisterCacheServiceServer(grpcServer, owner)
		go func() { _ = grpcServer.Serve(listener) }()
		remoteClient, err := cacheclient.NewSimpleClient(&cacheclient.ClientConfig{
			Addrs:              []string{listener.Addr().String()},
			Mode:               cacheclient.ModeSimple,
			ConnectionPoolSize: 1,
		})
		if err != nil {
			grpcServer.Stop()
			_ = listener.Close()
			tb.Fatal(err)
		}
		tb.Cleanup(func() { _ = remoteClient.Close() })
		tb.Cleanup(grpcServer.Stop)
		tb.Cleanup(func() { _ = listener.Close() })
		blockOwner = remoteClient
	} else {
		blockOwner = localOwner
	}
	cacheClient := &certifiedRangeBenchmarkCacheClient{
		handlerPrefetchCacheClient: &handlerPrefetchCacheClient{
			CacheClient: blockOwner,
			local:       localMeta,
			remote:      remote,
		},
	}

	store := cache.NewCacheWithClient(cacheClient, &cfg.Cache)
	const (
		clientAccessKey = "range-benchmark-client"
		clientSecretKey = "range-benchmark-secret"
		proxyAccessKey  = "range-benchmark-proxy-client"
		proxySecretKey  = "range-benchmark-proxy-secret"
	)
	rangeHeader := fmt.Sprintf("bytes=0-%d", rangeLen-1)
	gateway := httptest.NewUnstartedServer(http.NotFoundHandler())
	tb.Cleanup(gateway.Close)
	gatewayURL := "http://" + gateway.Listener.Addr().String()
	requestSigner := auth.NewRequestSigner(gatewayURL, cfg.Upstream.Region)
	signedRequest, err := requestSigner.SignRequest(
		context.Background(), http.MethodGet, "/"+bucket+"/"+key, nil, "",
		clientAccessKey, clientSecretKey, http.Header{"Range": {rangeHeader}},
	)
	if err != nil {
		tb.Fatalf("sign benchmark GET: %v", err)
	}
	date := signedRequest.Header.Get("X-Amz-Date")
	if len(date) < 8 {
		tb.Fatalf("signed request date = %q, want YYYYMMDD prefix", date)
	}
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(clientAccessKey, clientSecretKey)
	derivedKey, err := credentials.GetSigningKey(clientAccessKey, date[:8], cfg.Upstream.Region)
	if err != nil {
		tb.Fatalf("derive benchmark signing key: %v", err)
	}
	derivedKeys := auth.NewDerivedKeyStore(auth.DefaultDerivedKeyTTL)
	derivedKeys.Store(clientAccessKey, date[:8], cfg.Upstream.Region, derivedKey)
	authzCache := auth.NewAuthzCache(cfg.Credentials.AuthzCacheTTL)
	authzCache.Grant(clientAccessKey, bucket)
	keyUnwrapper, err := auth.NewKeyUnwrapper(proxySecretKey)
	if err != nil {
		tb.Fatalf("create benchmark key unwrapper: %v", err)
	}
	productionForwarder := proxy.NewForwarder(
		nil, cfg.Upstream.Endpoint, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost,
		auth.NewProxySigner(proxyAccessKey, proxySecretKey),
		&proxy.LocalAuthConfig{
			DerivedKeyStore: derivedKeys,
			Validator:       auth.NewRequestValidator(derivedKeys),
			KeyUnwrapper:    keyUnwrapper,
			AuthzCache:      authzCache,
		},
	)
	result, _, _, err := productionForwarder.ValidateAndGetCredentials(signedRequest)
	if err != nil || result != proxy.AuthValidated {
		tb.Fatalf("validate signed benchmark GET = (%v, %v), want AuthValidated", result, err)
	}
	forwarder := &certifiedRangeHandlerForwarder{
		RequestForwarder: productionForwarder,
		body:             body,
		etag:             etag,
	}
	service := proxy.NewService(forwarder, store, cfg)
	gateway.Config.Handler = NewServer(service, "127.0.0.1", 0, false, 0).Router()
	gateway.Start()
	client := gateway.Client()
	tb.Cleanup(client.CloseIdleConnections)

	return &certifiedRangeHandlerBenchmarkFixture{
		gateway:       gateway,
		client:        client,
		store:         store,
		forwarder:     forwarder,
		signedRequest: signedRequest,
		body:          body,
		bucket:        bucket,
		key:           key,
		etag:          etag,
		blockSize:     blockSize,
		rangeLen:      rangeLen,
		rangeHeader:   rangeHeader,
		expectedCRC:   crc32.ChecksumIEEE(body[:rangeLen]),
	}
}

func (f *certifiedRangeHandlerBenchmarkFixture) getRange() (status int, cacheStatus, etag, contentRange string, bodyBytes int64, ttfb time.Duration, err error) {
	req, err := http.NewRequest(http.MethodGet, f.gateway.URL+"/"+f.bucket+"/"+f.key, nil)
	if err != nil {
		return 0, "", "", "", 0, 0, err
	}
	req.Header.Set("Authorization", f.signedRequest.Header.Get("Authorization"))
	req.Header.Set("X-Amz-Date", f.signedRequest.Header.Get("X-Amz-Date"))
	req.Header.Set("X-Amz-Content-Sha256", f.signedRequest.Header.Get("X-Amz-Content-Sha256"))
	req.Header.Set("Range", f.rangeHeader)
	started := time.Now()
	resp, err := f.client.Do(req)
	ttfb = time.Since(started)
	if err != nil {
		return 0, "", "", "", 0, ttfb, err
	}
	bodyBytes, readErr := io.Copy(io.Discard, resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		err = readErr
	} else if closeErr != nil {
		err = closeErr
	}
	return resp.StatusCode, resp.Header.Get("X-Cache"), resp.Header.Get("ETag"), resp.Header.Get("Content-Range"), bodyBytes, ttfb, err
}

func (f *certifiedRangeHandlerBenchmarkFixture) populate(tb testing.TB) {
	tb.Helper()
	status, cacheStatus, etag, contentRange, bodyBytes, _, err := f.getRange()
	if err != nil || status != http.StatusPartialContent || cacheStatus != proxy.XCacheMiss || etag != f.etag || bodyBytes != f.rangeLen {
		tb.Fatalf("cold handler Range = (status=%d cache=%q etag=%q content-range=%q bytes=%d err=%v), want complete requested 206/MISS", status, cacheStatus, etag, contentRange, bodyBytes, err)
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		meta, found, metaErr := f.store.GetMeta(context.Background(), f.bucket, f.key)
		if metaErr == nil && found && meta != nil && meta.ETag == f.etag && meta.BlockSize == f.blockSize && meta.ContentLength == int64(len(f.body)) && !meta.BlocksComplete {
			// Both B and C publish this metadata only after all touched block writes return.
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			tb.Fatalf("range metadata not published with blocks durable: found=%t meta=%#v err=%v", found, meta, metaErr)
		}
	}
}

func (f *certifiedRangeHandlerBenchmarkFixture) verifyWarm(tb testing.TB) {
	tb.Helper()
	originCalls := f.forwarder.calls.Load()
	req, err := http.NewRequest(http.MethodGet, f.gateway.URL+"/"+f.bucket+"/"+f.key, nil)
	if err != nil {
		tb.Fatal(err)
	}
	req.Header.Set("Authorization", f.signedRequest.Header.Get("Authorization"))
	req.Header.Set("X-Amz-Date", f.signedRequest.Header.Get("X-Amz-Date"))
	req.Header.Set("X-Amz-Content-Sha256", f.signedRequest.Header.Get("X-Amz-Content-Sha256"))
	req.Header.Set("Range", f.rangeHeader)
	resp, err := f.client.Do(req)
	if err != nil {
		tb.Fatal(err)
	}
	hasher := crc32.NewIEEE()
	bodyBytes, readErr := io.Copy(hasher, resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil || closeErr != nil {
		tb.Fatalf("warm response body read: read=%v close=%v", readErr, closeErr)
	}
	wantRange := fmt.Sprintf("bytes 0-%d/%d", f.rangeLen-1, len(f.body))
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("X-Cache") != proxy.XCacheHit ||
		resp.Header.Get("ETag") != f.etag || resp.Header.Get("Content-Range") != wantRange || bodyBytes != f.rangeLen || hasher.Sum32() != f.expectedCRC {
		tb.Fatalf("warm handler Range = (status=%d cache=%q etag=%q content-range=%q bytes=%d exact=%t), want exact 206/HIT",
			resp.StatusCode, resp.Header.Get("X-Cache"), resp.Header.Get("ETag"), resp.Header.Get("Content-Range"), bodyBytes, hasher.Sum32() == f.expectedCRC)
	}
	if got := f.forwarder.calls.Load(); got != originCalls {
		tb.Fatalf("warm handler Range called the origin %d times, want none", got-originCalls)
	}
}

func certifiedRangeProcessCPU(tb testing.TB) time.Duration {
	tb.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		tb.Fatalf("get process CPU time: %v", err)
	}
	return time.Duration(usage.Utime.Sec)*time.Second + time.Duration(usage.Utime.Usec)*time.Microsecond +
		time.Duration(usage.Stime.Sec)*time.Second + time.Duration(usage.Stime.Usec)*time.Microsecond
}

// BenchmarkHandleGetObjectCertifiedRange performs a real warm S3 Range request
// through the handler after an untimed cold request has created range metadata.
// The remote owner delays every block Get, including presence probes, so the
// benchmark measures the saved cache-boundary preflight as well as payload work.
func BenchmarkHandleGetObjectCertifiedRange(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	owners := []struct {
		name   string
		remote bool
	}{
		{name: "local"},
		{name: "remote_delayed", remote: true},
	}
	for _, owner := range owners {
		b.Run(owner.name, func(b *testing.B) {
			for _, blockCount := range []int{2, 4, 8, 32} {
				b.Run(fmt.Sprintf("blocks-%d", blockCount), func(b *testing.B) {
					fixture := newCertifiedRangeHandlerBenchmarkFixture(b, owner.remote, blockCount)
					fixture.populate(b)
					fixture.verifyWarm(b)
					originCalls := fixture.forwarder.calls.Load()
					cpuStart := certifiedRangeProcessCPU(b)
					b.ResetTimer()
					var totalTTFB time.Duration
					for b.Loop() {
						status, cacheStatus, etag, contentRange, bodyBytes, ttfb, err := fixture.getRange()
						if err != nil || status != http.StatusPartialContent || cacheStatus != proxy.XCacheHit || etag != fixture.etag || contentRange != fmt.Sprintf("bytes 0-%d/%d", fixture.rangeLen-1, len(fixture.body)) || bodyBytes != fixture.rangeLen {
							b.Fatalf("warm handler Range = (status=%d cache=%q etag=%q content-range=%q bytes=%d err=%v), want complete requested 206/HIT", status, cacheStatus, etag, contentRange, bodyBytes, err)
						}
						totalTTFB += ttfb
					}
					b.StopTimer()
					cpuEnd := certifiedRangeProcessCPU(b)
					if got := fixture.forwarder.calls.Load(); got != originCalls {
						b.Fatalf("warm handler Range called the origin %d times, want none", got-originCalls)
					}
					if cpuEnd < cpuStart {
						b.Fatalf("process CPU time moved backwards: start=%s end=%s", cpuStart, cpuEnd)
					}
					b.ReportMetric(float64(totalTTFB.Nanoseconds())/float64(b.N), "ttfb-ns/op")
					// Process CPU includes the HTTP client, handler, and loopback cache owner, so keep
					// it as an optional diagnostic rather than an advertised product CPU metric.
					b.Logf("process-cpu-ns/op=%.0f", float64((cpuEnd-cpuStart).Nanoseconds())/float64(b.N))
				})
			}
		})
	}
}
