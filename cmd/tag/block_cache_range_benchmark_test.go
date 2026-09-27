package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/embedded"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/handlers"
	"github.com/tigrisdata/tag/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

const (
	embeddedBlockRangeBenchmarkBlockSize = config.DefaultCacheBlockSize
	benchmarkBlockPresenceMethod         = "/tag.cache.v1.BlockPresence/Check"
)

type embeddedBlockRangeBenchmarkFixture struct {
	gateway        *httptest.Server
	client         *http.Client
	handler        http.Handler
	body           []byte
	key            string
	cacheConfig    config.CacheConfig
	cacheClient    *embeddedBlockCacheClient
	embeddedClient *embedded.Client
	closeNodes     []func()
	seedGossipAddr string
	stats          *embeddedBlockRangeRPCStats
	requestSigner  *auth.RequestSigner
	gatewaySigner  *auth.RequestSigner
	accessKey      string
	secretKey      string
	grpcToken      string
	region         string
}

type benchmark206Writer struct {
	*httptest.ResponseRecorder
	benchmark *testing.B
	stopped   bool
}

func (w *benchmark206Writer) stopTimer() {
	if w.benchmark != nil && !w.stopped {
		w.stopped = true
		w.benchmark.StopTimer()
	}
}

func (w *benchmark206Writer) WriteHeader(status int) {
	w.ResponseRecorder.WriteHeader(status)
	w.stopTimer()
}

func (w *benchmark206Writer) Write(p []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(p)
	w.stopTimer()
	return n, err
}

func (w *benchmark206Writer) Flush() {
	w.ResponseRecorder.Flush()
	w.stopTimer()
}

func (w *benchmark206Writer) Unwrap() http.ResponseWriter { return w.ResponseRecorder }

type embeddedBlockRangeRPCStats struct {
	presenceRPCs       atomic.Int64
	presenceRequests   atomic.Int64
	presenceReplyBytes atomic.Int64
	probeGetRPCs       atomic.Int64
	probePayloadBytes  atomic.Int64
}

type embeddedBlockRangeRPCKey struct{}

type embeddedBlockRangeRPCRecord struct {
	method string
	probe  atomic.Bool
}

func (s *embeddedBlockRangeRPCStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, embeddedBlockRangeRPCKey{}, &embeddedBlockRangeRPCRecord{method: info.FullMethodName})
}

func (s *embeddedBlockRangeRPCStats) countBlockPresenceStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if method == benchmarkBlockPresenceMethod {
		s.presenceRequests.Add(1)
	}
	return streamer(ctx, desc, cc, method, opts...)
}

func (s *embeddedBlockRangeRPCStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	record, _ := ctx.Value(embeddedBlockRangeRPCKey{}).(*embeddedBlockRangeRPCRecord)
	if record == nil {
		return
	}
	switch event := event.(type) {
	case *stats.InPayload:
		request, ok := event.Payload.(*pb.GetRequest)
		if ok && record.method == "/cache.CacheService/Get" {
			record.probe.Store(strings.HasPrefix(request.Key, "blk|") && request.Start == 0 && request.End == 1)
		}
	case *stats.OutPayload:
		if record.method == benchmarkBlockPresenceMethod {
			s.presenceReplyBytes.Add(int64(event.Length))
		} else if record.probe.Load() {
			if response, ok := event.Payload.(*pb.GetResponse); ok {
				s.probePayloadBytes.Add(int64(len(response.Data)))
			}
		}
	case *stats.End:
		if record.method == benchmarkBlockPresenceMethod {
			s.presenceRPCs.Add(1)
		}
		if record.probe.Load() {
			s.probeGetRPCs.Add(1)
		}
	}
}

func (*embeddedBlockRangeRPCStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (*embeddedBlockRangeRPCStats) HandleConn(context.Context, stats.ConnStats) {}

func (s *embeddedBlockRangeRPCStats) reset() {
	s.presenceRPCs.Store(0)
	s.presenceRequests.Store(0)
	s.presenceReplyBytes.Store(0)
	s.probeGetRPCs.Store(0)
	s.probePayloadBytes.Store(0)
}

func newEmbeddedBlockRangeBenchmarkFixture(tb testing.TB, blockCount int, withRPCStats ...bool) *embeddedBlockRangeBenchmarkFixture {
	return newEmbeddedBlockRangeBenchmarkFixtureWithMode(tb, blockCount, false, withRPCStats...)
}

func newEmbeddedBlockRangeTransparentBenchmarkFixture(tb testing.TB, blockCount int) *embeddedBlockRangeBenchmarkFixture {
	return newEmbeddedBlockRangeBenchmarkFixtureWithMode(tb, blockCount, true)
}

func newEmbeddedBlockRangeBenchmarkFixtureWithMode(tb testing.TB, blockCount int, transparent bool, withRPCStats ...bool) *embeddedBlockRangeBenchmarkFixture {
	return newEmbeddedBlockRangeBenchmarkFixtureWithOptions(tb, blockCount, transparent, false, withRPCStats...)
}

func newEmbeddedBlockRangeLegacyPeerFixture(tb testing.TB, blockCount int) *embeddedBlockRangeBenchmarkFixture {
	return newEmbeddedBlockRangeBenchmarkFixtureWithOptions(tb, blockCount, false, true, true)
}

func newEmbeddedBlockRangeBenchmarkFixtureWithOptions(tb testing.TB, blockCount int, transparent, legacyPeer bool, withRPCStats ...bool) *embeddedBlockRangeBenchmarkFixture {
	tb.Helper()
	if blockCount < 2 || blockCount > 32 {
		tb.Fatalf("benchmark block count %d outside 2..32", blockCount)
	}

	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	tb.Cleanup(func() { log.Logger = oldLogger })

	gossipAddrs := make([]string, 2)
	grpcAddrs := make([]string, 2)
	for i := range gossipAddrs {
		gossipAddrs[i] = embeddedBlockRangeFreeAddress(tb)
		grpcAddrs[i] = embeddedBlockRangeFreeAddress(tb)
	}

	var blockCacheClient *embeddedBlockCacheClient
	var nodes []*embedded.Client
	closeNodes := make([]func(), len(gossipAddrs))
	var stats *embeddedBlockRangeRPCStats
	if len(withRPCStats) > 0 && withRPCStats[0] {
		stats = &embeddedBlockRangeRPCStats{}
	}
	const (
		accessKey      = "range-benchmark-access"
		secretKey      = "range-benchmark-secret"
		proxyAccessKey = "range-benchmark-proxy"
		proxySecretKey = "range-benchmark-proxy-secret"
	)
	grpcToken := auth.DeriveGRPCAuthToken(proxyAccessKey, proxySecretKey)
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = embeddedBlockRangeBenchmarkBlockSize
	for i := range gossipAddrs {
		diskPath := tb.TempDir()
		writeEmbeddedBlockRangeRingTokens(tb, diskPath, i)
		var presenceServer *blockPresenceServer
		if !legacyPeer || i == 0 {
			presenceServer = &blockPresenceServer{}
		}
		embeddedCfg := &embedded.Config{
			DiskPath:      diskPath,
			TTL:           cfg.Cache.TTL,
			NodeID:        fmt.Sprintf("range-bench-%d", i),
			ClusterAddr:   gossipAddrs[i],
			GRPCAddr:      grpcAddrs[i],
			AdvertiseAddr: grpcAddrs[i],
			Registerer:    prometheus.NewRegistry(),
		}
		if cfg.Cache.IsGRPCAuthEnabled() {
			embeddedCfg.GRPCServerOptions = auth.GRPCServerOptions(grpcToken)
			embeddedCfg.GRPCDialOptions = auth.GRPCDialOptions(grpcToken)
		}
		if stats != nil {
			embeddedCfg.GRPCServerOptions = append(embeddedCfg.GRPCServerOptions, grpc.StatsHandler(stats))
			embeddedCfg.GRPCDialOptions = append(embeddedCfg.GRPCDialOptions, grpc.WithChainStreamInterceptor(stats.countBlockPresenceStream))
		}
		if presenceServer != nil {
			embeddedCfg.GRPCServerOptions = append(embeddedCfg.GRPCServerOptions, presenceServer.serverOption())
		}
		if i > 0 {
			embeddedCfg.SeedNodes = []string{gossipAddrs[0]}
		}
		client, err := embedded.New(embeddedCfg)
		if err != nil {
			tb.Fatalf("create embedded range node %d: %v", i, err)
		}
		if presenceServer != nil {
			presenceServer.client = client
		}
		nodes = append(nodes, client)
		if i == 0 {
			blockCacheClient = newEmbeddedBlockCacheClient(client, embeddedCfg.GRPCDialOptions...)
			wrapper := blockCacheClient
			var closeOnce sync.Once
			closeNodes[i] = func() { closeOnce.Do(func() { _ = wrapper.Close() }) }
		} else {
			node := client
			var closeOnce sync.Once
			closeNodes[i] = func() { closeOnce.Do(func() { _ = node.Close() }) }
		}
		tb.Cleanup(closeNodes[i])
		if err := client.StartGRPCServer(); err != nil {
			tb.Fatalf("start embedded range node %d: %v", i, err)
		}
	}
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readyCancel()
	for _, client := range nodes {
		if err := client.WaitReady(readyCtx); err != nil {
			tb.Fatalf("embedded range node not ready: %v", err)
		}
	}
	for {
		ready := true
		for _, client := range nodes {
			if len(client.Coordinator().GetRing().GetActiveNodes()) != len(nodes) {
				ready = false
				break
			}
		}
		if ready {
			break
		}
		select {
		case <-readyCtx.Done():
			tb.Fatalf("embedded range cluster did not converge: %v", readyCtx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}

	body := make([]byte, blockCount*embeddedBlockRangeBenchmarkBlockSize)
	for i := range body {
		body[i] = byte(i*31 + i/257)
	}
	const (
		bucket = "benchmark"
		etag   = `"block-range"`
	)
	key := embeddedBlockRangeObjectKey(tb, nodes[0], blockCount, bucket, etag)
	store := cache.NewCacheWithClient(blockCacheClient, &cfg.Cache)
	for i := 0; i < blockCount; i++ {
		start := i * embeddedBlockRangeBenchmarkBlockSize
		end := start + embeddedBlockRangeBenchmarkBlockSize
		if err := store.PutBlock(context.Background(), bucket, key, etag, embeddedBlockRangeBenchmarkBlockSize, int64(i), body[start:end], 60); err != nil {
			tb.Fatalf("seed embedded block %d: %v", i, err)
		}
	}
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          etag,
		ContentType:   "application/octet-stream",
		ContentLength: int64(len(body)),
		StatusCode:    http.StatusOK,
		BlockSize:     embeddedBlockRangeBenchmarkBlockSize,
	}
	if wrote, err := store.PutMetaIfVersion(context.Background(), bucket, key, meta, 60, cache.VersionAny); err != nil || !wrote {
		tb.Fatalf("seed range metadata = (wrote=%t, err=%v)", wrote, err)
	}

	upstreamEndpoint := "http://127.0.0.1:1" // a warmed hit must never reach upstream
	cfg.Upstream.Endpoint = upstreamEndpoint
	var forwarder proxy.RequestForwarder
	if transparent {
		cfg.Mode = config.ModeTransparent
		clientCredentials := auth.NewCredentialStore()
		clientCredentials.AddCredential(accessKey, secretKey)
		derivedKeyStore := auth.NewDerivedKeyStore(auth.DefaultDerivedKeyTTL)
		now := time.Now().UTC()
		for dayOffset := 0; dayOffset <= 1; dayOffset++ {
			date := now.Add(time.Duration(dayOffset) * 24 * time.Hour).Format("20060102")
			signingKey, err := clientCredentials.GetSigningKey(accessKey, date, cfg.Upstream.Region)
			if err != nil {
				tb.Fatalf("derive transparent benchmark key: %v", err)
			}
			derivedKeyStore.Store(accessKey, date, cfg.Upstream.Region, signingKey)
		}
		keyUnwrapper, err := auth.NewKeyUnwrapper(proxySecretKey)
		if err != nil {
			tb.Fatalf("create transparent benchmark key unwrapper: %v", err)
		}
		authzCache := auth.NewAuthzCache(auth.DefaultAuthzCacheTTL)
		authzCache.Grant(accessKey, bucket)
		localAuth := &proxy.LocalAuthConfig{
			DerivedKeyStore: derivedKeyStore,
			Validator:       auth.NewRequestValidator(derivedKeyStore),
			KeyUnwrapper:    keyUnwrapper,
			AuthzCache:      authzCache,
		}
		proxySigner := auth.NewProxySigner(proxyAccessKey, proxySecretKey)
		forwarder = proxy.NewForwarder(nil, upstreamEndpoint, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost, proxySigner, localAuth)
	} else {
		cfg.Mode = config.ModeSigning
		credentialStore := auth.NewCredentialStore()
		credentialStore.AddCredential(accessKey, secretKey)
		forwarder = proxy.NewForwarder(credentialStore, upstreamEndpoint, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost, nil, nil)
	}
	requestSigner := auth.NewRequestSigner("http://range-benchmark.invalid", cfg.Upstream.Region)
	service := proxy.NewService(forwarder, store, cfg)
	server := handlers.NewServer(service, "127.0.0.1", 0, false, cfg.Server.MaxInflightRequests)
	handler := server.Router()
	return &embeddedBlockRangeBenchmarkFixture{
		handler: handler, body: body, key: key,
		cacheConfig: cfg.Cache, cacheClient: blockCacheClient,
		embeddedClient: nodes[0], closeNodes: closeNodes, seedGossipAddr: gossipAddrs[0],
		stats: stats, requestSigner: requestSigner,
		accessKey: accessKey, secretKey: secretKey, grpcToken: grpcToken, region: cfg.Upstream.Region,
	}
}

// writeEmbeddedBlockRangeRingTokens gives both benchmark arms the same ownership layout.
// OCache otherwise generates fresh tokens for every temporary DiskPath, which can change
// metadata locality and the local/remote order of a measured range between arms.
func writeEmbeddedBlockRangeRingTokens(tb testing.TB, diskPath string, nodeIndex int) {
	tb.Helper()
	const tokenCount = 512
	if nodeIndex < 0 || nodeIndex > 1 {
		tb.Fatalf("benchmark ring node index %d outside 0..1", nodeIndex)
	}
	state := uint32(0x6d2b79f5) ^ uint32(nodeIndex+1)*0x9e3779b9
	tokens := make([]uint32, tokenCount)
	for i := range tokens {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		tokens[i] = state
	}
	contents, err := json.Marshal(struct {
		Tokens []uint32 `json:"tokens"`
	}{Tokens: tokens})
	if err != nil {
		tb.Fatalf("marshal benchmark ring tokens: %v", err)
	}
	tokensPath := filepath.Join(diskPath, "coordinator", "ring-tokens")
	if err := os.MkdirAll(filepath.Dir(tokensPath), 0o755); err != nil {
		tb.Fatalf("create benchmark ring-token directory: %v", err)
	}
	if err := os.WriteFile(tokensPath, contents, 0o600); err != nil {
		tb.Fatalf("write benchmark ring tokens: %v", err)
	}
}

// embeddedBlockRangeObjectKey keeps metadata local and holds the block-owner mix
// constant across both benchmark arms: block 0 is local, block 1 is remote, and
// half of the full range is remote.
func embeddedBlockRangeObjectKey(tb testing.TB, client *embedded.Client, blockCount int, bucket, etag string) string {
	tb.Helper()
	localID := client.Coordinator().GetLocalNodeID()
	wantRemote := blockCount / 2
	wantPattern := expectedEmbeddedBlockRangeOwnerPattern(blockCount)
	for candidate := 0; candidate < 65536; candidate++ {
		key := fmt.Sprintf("block-range-%05d", candidate)
		remote := 0
		remoteFirst, remoteSecond := false, false
		ownerPattern := make([]byte, blockCount)
		for index := 0; index < blockCount; index++ {
			blockKey := cache.MakeBlockKey(bucket, key, etag, embeddedBlockRangeBenchmarkBlockSize, int64(index))
			node, err := client.Coordinator().GetNodeForKey(blockKey)
			if err != nil {
				tb.Fatalf("resolve benchmark block owner: %v", err)
			}
			isRemote := node.ID != localID
			if isRemote {
				remote++
				ownerPattern[index] = '1'
			} else {
				ownerPattern[index] = '0'
			}
			if index == 0 {
				remoteFirst = isRemote
			} else if index == 1 {
				remoteSecond = isRemote
			}
		}
		metaOwner, err := client.Coordinator().GetNodeForKey(cache.MakeMetaKey(bucket, key))
		if err != nil {
			tb.Fatalf("resolve benchmark metadata owner: %v", err)
		}
		if remote == wantRemote && !remoteFirst && remoteSecond && metaOwner.ID == localID &&
			(wantPattern == "" || string(ownerPattern) == wantPattern) {
			return key
		}
	}
	tb.Fatalf("could not find a %d-block object with %d remote owners and pattern %q", blockCount, wantRemote, wantPattern)
	return ""
}

// expectedEmbeddedBlockRangeOwnerPattern guards the exact measured owner order.
func expectedEmbeddedBlockRangeOwnerPattern(blockCount int) string {
	switch blockCount {
	case 2:
		return "01"
	case 4:
		return "0110"
	case 8:
		return "01111000"
	case 32:
		return "01000011001100011011010011100111"
	default:
		return ""
	}
}

func embeddedBlockRangeFreeAddress(tb testing.TB) string {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		tb.Fatal(err)
	}
	return addr
}

func (f *embeddedBlockRangeBenchmarkFixture) doRange(tb testing.TB, benchmark *testing.B) *httptest.ResponseRecorder {
	tb.Helper()
	if benchmark != nil {
		benchmark.StopTimer()
	}
	unsigned := httptest.NewRequest(http.MethodGet, "/benchmark/"+f.key, nil)
	unsigned.Header.Set("Range", fmt.Sprintf("bytes=0-%d", len(f.body)-1))
	req, err := f.requestSigner.SignRequest(unsigned.Context(), unsigned.Method, unsigned.URL.RequestURI(), unsigned.Body, "", f.accessKey, f.secretKey, unsigned.Header)
	if err != nil {
		tb.Fatalf("sign benchmark request: %v", err)
	}
	req.RemoteAddr = unsigned.RemoteAddr
	response := httptest.NewRecorder()
	writer := &benchmark206Writer{ResponseRecorder: response, benchmark: benchmark}
	if benchmark != nil {
		benchmark.StartTimer()
	}
	f.handler.ServeHTTP(writer, req)
	writer.stopTimer()
	return response
}

func (f *embeddedBlockRangeBenchmarkFixture) verifyResponse(tb testing.TB, response *httptest.ResponseRecorder) {
	tb.Helper()
	body := response.Body.Bytes()
	if response.Code != http.StatusPartialContent || response.Header().Get(proxy.XCacheHeader) != proxy.XCacheHit || !bytes.Equal(body, f.body) {
		tb.Fatalf("range response = (status=%d, cache=%q, bytes=%d, exact=%t), want (206, HIT, %d exact bytes)", response.Code, response.Header().Get(proxy.XCacheHeader), len(body), bytes.Equal(body, f.body), len(f.body))
	}
	if got, want := response.Header().Get("Content-Range"), fmt.Sprintf("bytes 0-%d/%d", len(f.body)-1, len(f.body)); got != want {
		tb.Fatalf("Content-Range=%q, want %q", got, want)
	}
	if got, want := response.Header().Get("Content-Length"), fmt.Sprint(len(f.body)); got != want {
		tb.Fatalf("Content-Length=%q, want %q", got, want)
	}
	if got := response.Header().Get("ETag"); got != `"block-range"` {
		tb.Fatalf("ETag=%q, want %q", got, `"block-range"`)
	}
}

func BenchmarkEmbeddedBlockRangePresence(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	for _, blockCount := range []int{2, 4, 8, 32} {
		b.Run(fmt.Sprintf("%d_blocks", blockCount), func(b *testing.B) {
			benchmarkEmbeddedBlockRangePresence(b, blockCount, false)
		})
	}
}

func BenchmarkEmbeddedBlockRangePresenceTransparent(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	b.Run("32_blocks", func(b *testing.B) {
		benchmarkEmbeddedBlockRangePresence(b, 32, true)
	})
}

func benchmarkEmbeddedBlockRangePresence(b *testing.B, blockCount int, transparent bool) {
	b.StopTimer()
	var fixture *embeddedBlockRangeBenchmarkFixture
	if transparent {
		fixture = newEmbeddedBlockRangeTransparentBenchmarkFixture(b, blockCount)
	} else {
		fixture = newEmbeddedBlockRangeBenchmarkFixture(b, blockCount)
	}
	fixture.verifyResponse(b, fixture.doRange(b, nil)) // warm auth, cache, and peer connections
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fixture.verifyResponse(b, fixture.doRange(b, b))
	}
	b.StopTimer()
}
