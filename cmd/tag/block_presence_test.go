package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/ocache/coordinator/ring"
	"github.com/tigrisdata/ocache/embedded"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/handlers"
	"github.com/tigrisdata/tag/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type missingBlockPresenceOwnerCoordinator struct{}

func (missingBlockPresenceOwnerCoordinator) GetEpoch() uint64       { return 1 }
func (missingBlockPresenceOwnerCoordinator) GetLocalNodeID() string { return "local" }
func (missingBlockPresenceOwnerCoordinator) GetNodeForKey(string) (*ring.NodeInfo, error) {
	return nil, nil
}

func TestCheckBlockPresenceOwnerRetriesWhenNodeIsMissing(t *testing.T) {
	err := checkBlockPresenceOwner(missingBlockPresenceOwnerCoordinator{}, "block-key", "owner")
	if !errors.Is(err, cache.ErrBlockPresenceTopologyChanged) {
		t.Fatalf("missing owner error = %v, want topology change", err)
	}
}

func TestRemoteBlockPresenceClosesEachAddressConnection(t *testing.T) {
	requestSeen := []chan struct{}{make(chan struct{}, 1), make(chan struct{}, 1)}
	listeners := make([]net.Listener, len(requestSeen))
	servers := make([]*grpc.Server, len(requestSeen))
	for i := range requestSeen {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = listener
		index := i
		servers[i] = grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
			var request structpb.Struct
			if err := stream.RecvMsg(&request); err != nil {
				return err
			}
			requestSeen[index] <- struct{}{}
			response, err := encodeBlockPresenceResponse([]bool{true}, 0)
			if err != nil {
				return err
			}
			return stream.SendMsg(response)
		}))
		go func(i int) { _ = servers[i].Serve(listeners[i]) }(i)
		t.Cleanup(func() {
			servers[index].Stop()
			_ = listeners[index].Close()
		})
	}

	var connMu sync.Mutex
	var connections []*grpc.ClientConn
	streamInterceptor := func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		connMu.Lock()
		connections = append(connections, conn)
		connMu.Unlock()
		return streamer(ctx, desc, conn, method, opts...)
	}
	client := newEmbeddedBlockCacheClient(nil, grpc.WithStreamInterceptor(streamInterceptor))
	t.Cleanup(func() { _ = client.Close() })

	for i, listener := range listeners {
		found, err := client.remoteBlockPresence(context.Background(), "same-owner", listener.Addr().String(), 0, []string{"blk|b|k|e|4|0"})
		if err != nil || len(found) != 1 || !found[0] {
			t.Fatalf("presence exchange %d = (%v,%v), want one present result", i, found, err)
		}
		select {
		case <-requestSeen[i]:
		case <-time.After(2 * time.Second):
			t.Fatalf("address %d did not receive a presence request", i)
		}
		connMu.Lock()
		if len(connections) != i+1 {
			connMu.Unlock()
			t.Fatalf("presence exchanges opened %d ClientConns, want %d", len(connections), i+1)
		}
		conn := connections[i]
		connMu.Unlock()
		if got := conn.GetState(); got != connectivity.Shutdown {
			t.Fatalf("presence exchange %d retained its ClientConn in state %v", i, got)
		}
	}
	if connections[0] == connections[1] {
		t.Fatal("address change reused the previous owner's ClientConn")
	}
}

func TestEmbeddedBlockCacheClientCloseCancelsActivePresence(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	requestReceived := make(chan struct{})
	serverCanceled := make(chan struct{})
	server := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		var request structpb.Struct
		if err := stream.RecvMsg(&request); err != nil {
			return err
		}
		close(requestReceived)
		<-stream.Context().Done()
		close(serverCanceled)
		return status.FromContextError(stream.Context().Err()).Err()
	}))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client := newEmbeddedBlockCacheClient(nil)
	t.Cleanup(func() { _ = client.Close() })
	requestErr := make(chan error, 1)
	go func() {
		_, err := client.remoteBlockPresence(context.Background(), "owner", listener.Addr().String(), 0, []string{"blk|b|k|e|4|0"})
		requestErr <- err
	}()
	select {
	case <-requestReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("presence request did not reach the owner")
	}

	closeErr := make(chan error, 1)
	go func() { closeErr <- client.Close() }()
	select {
	case <-serverCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("client Close did not cancel the active presence RPC")
	}
	select {
	case err := <-requestErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("presence error after client Close = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("presence RPC did not return after client Close")
	}
	select {
	case err := <-closeErr:
		if err != nil {
			t.Fatalf("client Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client Close did not join the active presence RPC")
	}
}

func TestBlockPresenceRequestFallsBackForNonUTF8Keys(t *testing.T) {
	key := string([]byte{'b', 'l', 'k', '|', 0xff})
	if _, err := encodeBlockPresenceRequest("owner", 1, []string{key}); !errors.Is(err, cache.ErrBlockPresenceUnsupported) {
		t.Fatalf("non-UTF8 key error = %v, want optional capability fallback", err)
	}
}

func TestBlockPresenceResponseCodecPreservesOrderAndRejectsInvalidResults(t *testing.T) {
	want := []bool{true, false, true}
	response, err := encodeBlockPresenceResponse(want, 7)
	if err != nil {
		t.Fatalf("encode response: %v", err)
	}
	got, err := decodeBlockPresenceResponse(response, 7, len(want))
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("decoded result count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("decoded presence[%d] = %t, want %t", i, got[i], want[i])
		}
	}

	wrongEpoch, err := encodeBlockPresenceResponse(want, 8)
	if err != nil {
		t.Fatalf("encode wrong-epoch response: %v", err)
	}
	if _, err := decodeBlockPresenceResponse(wrongEpoch, 7, len(want)); !errors.Is(err, cache.ErrBlockPresenceTopologyChanged) {
		t.Errorf("wrong-epoch response error = %v, want topology change", err)
	}

	for _, results := range [][]any{{true, false}, {"true", false, true}} {
		invalidResponse, err := structpb.NewStruct(map[string]any{
			"epoch": "7",
			"found": results,
		})
		if err != nil {
			t.Fatalf("create malformed response: %v", err)
		}
		if _, err := decodeBlockPresenceResponse(invalidResponse, 7, len(want)); err == nil {
			t.Errorf("decoder accepted malformed results %v", results)
		}
	}
}

func (f *embeddedBlockRangeBenchmarkFixture) startGateway(tb testing.TB) {
	tb.Helper()
	f.gateway = httptest.NewServer(f.handler)
	tb.Cleanup(f.gateway.Close)
	f.gatewaySigner = auth.NewRequestSigner(f.gateway.URL, f.region)
	f.client = f.gateway.Client()
	tb.Cleanup(f.client.CloseIdleConnections)
}

type earlyUnimplementedPresenceClient struct {
	cacheclient.CacheClient
	peer      *embeddedBlockCacheClient
	address   string
	probeGets int
}

func (c *earlyUnimplementedPresenceClient) BlockPresence(ctx context.Context, keys []string) ([]bool, error) {
	return c.peer.remoteBlockPresence(ctx, "legacy-owner", c.address, 0, keys)
}

func (c *earlyUnimplementedPresenceClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if strings.HasPrefix(key, "blk|") && start == 0 && end == 1 {
		c.probeGets++
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

type headerBarrierPresenceStream struct {
	grpc.ClientStream
}

func (s *headerBarrierPresenceStream) SendMsg(message any) error {
	_, _ = s.ClientStream.Header()
	return s.ClientStream.SendMsg(message)
}

func TestEarlyUnimplementedPeerFallsBackToPerKeyProbe(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	defer listener.Close()

	streamInterceptor := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &headerBarrierPresenceStream{ClientStream: stream}, nil
	}
	peer := newEmbeddedBlockCacheClient(nil, grpc.WithStreamInterceptor(streamInterceptor))
	defer peer.Close()

	memory := cacheclient.NewMemoryCache()
	const bucket, object, etag = "b", "k", `"v1"`
	key := cache.MakeBlockKey(bucket, object, etag, 4, 0)
	if err := memory.Put(context.Background(), key, []byte("AB"), 0); err != nil {
		t.Fatal(err)
	}
	client := &earlyUnimplementedPresenceClient{
		CacheClient: memory,
		peer:        peer,
		address:     listener.Addr().String(),
	}
	cfg := config.NewDefault()
	store := cache.NewCacheWithClient(client, &cfg.Cache)

	present, err := store.BlockExistsBatchErr(context.Background(), bucket, object, etag, 4, []int64{0})
	if err != nil || len(present) != 1 || !present[0] {
		t.Fatalf("presence after early Unimplemented = %v, err=%v; want legacy present result", present, err)
	}
	if client.probeGets != 1 {
		t.Fatalf("legacy per-key probes = %d, want 1", client.probeGets)
	}
}

// A real embedded peer without the optional method must still serve a warm
// multi-block Range through its ordinary Get RPCs after rejecting the batch.
func TestEarlyUnimplementedPeerServesWarmMultiBlockRange(t *testing.T) {
	fixture := newEmbeddedBlockRangeLegacyPeerFixture(t, 2)
	store := cache.NewCacheWithClient(fixture.cacheClient, &fixture.cacheConfig)
	meta, found, err := store.GetMeta(context.Background(), "benchmark", fixture.key)
	if err != nil || !found || meta == nil || meta.BlockSize != fixture.cacheConfig.BlockSize {
		t.Fatalf("seeded metadata = (%v,%v,%v), want cached two-block metadata", meta, found, err)
	}
	present, err := store.BlockExistsBatchErr(context.Background(), "benchmark", fixture.key, meta.ETag, meta.BlockSize, []int64{0, 1})
	if err != nil || len(present) != 2 || !present[0] || !present[1] {
		t.Fatalf("legacy peer presence fallback = (%v,%v), want two present blocks", present, err)
	}
	if got := fixture.stats.presenceRequests.Load(); got != 1 {
		t.Fatalf("legacy peer block-presence attempts = %d, want one rejected exchange", got)
	}
	if got := fixture.stats.probeGetRPCs.Load(); got != 1 {
		t.Fatalf("legacy peer per-key probe RPCs = %d, want one remote probe", got)
	}
	if got := fixture.stats.probePayloadBytes.Load(); got != 2 {
		t.Fatalf("legacy peer probe payload bytes = %d, want 2", got)
	}

	fixture.stats.reset()
	response := fixture.doRange(t, nil)
	fixture.verifyResponse(t, response)
	if got := fixture.stats.presenceRequests.Load(); got != 1 {
		t.Fatalf("warm Range made %d legacy batch attempts, want 1", got)
	}
	if got := fixture.stats.probeGetRPCs.Load(); got != 1 || fixture.stats.probePayloadBytes.Load() != 2 {
		t.Fatalf("warm Range fallback probes = (%d RPCs, %d bytes), want (1, 2)", got, fixture.stats.probePayloadBytes.Load())
	}
}

type blockPresenceCallRecorder struct {
	*embeddedBlockCacheClient
	pageSizes []int
}

func (c *blockPresenceCallRecorder) BlockPresence(ctx context.Context, keys []string) ([]bool, error) {
	c.pageSizes = append(c.pageSizes, len(keys))
	return c.embeddedBlockCacheClient.BlockPresence(ctx, keys)
}

type legacyBlockPresenceRecorder struct {
	*embedded.Client
	probeCalls int
	probeBytes int64
}

type wrongOwnerBlockPresenceClient struct {
	*embeddedBlockCacheClient
	remoteKey     string
	remoteAddress string
	epoch         uint64
}

func (c *wrongOwnerBlockPresenceClient) BlockPresence(ctx context.Context, _ []string) ([]bool, error) {
	return c.remoteBlockPresence(ctx, "stale-owner", c.remoteAddress, c.epoch, []string{c.remoteKey})
}

func (c *legacyBlockPresenceRecorder) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if strings.HasPrefix(key, "blk|") && start == 0 && end == 1 {
		c.probeCalls++
		counter := &blockPresenceCountingWriter{Writer: w}
		err := c.Client.GetRangeStream(ctx, key, start, end, counter)
		c.probeBytes += counter.written
		return err
	}
	return c.Client.GetRangeStream(ctx, key, start, end, w)
}

type blockPresenceCountingWriter struct {
	io.Writer
	written int64
}

func (w *blockPresenceCountingWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.written += int64(n)
	return n, err
}

// The real embedded cluster path returns ordered bits with one remote exchange
// per owner, while a plain CacheClient retains the old one-stream-per-key path.
func TestEmbeddedBlockPresenceMultiNodeAndFallback(t *testing.T) {
	fixture := newEmbeddedBlockRangeBenchmarkFixture(t, 32, true)
	fixture.startGateway(t)
	ctx := context.Background()
	const (
		bucket = "benchmark"
		etag   = `"block-range"`
	)
	indices := func(count int) []int64 {
		idxs := make([]int64, count)
		for i := range idxs {
			idxs[i] = int64(i)
		}
		return idxs
	}
	recordRemoteOwners := func(idxs []int64) (remoteBlocks int, owners map[string]struct{}) {
		owners = make(map[string]struct{})
		localID := fixture.embeddedClient.Coordinator().GetLocalNodeID()
		for _, idx := range idxs {
			key := cache.MakeBlockKey(bucket, fixture.key, etag, embeddedBlockRangeBenchmarkBlockSize, idx)
			owner, err := fixture.embeddedClient.Coordinator().GetNodeForKey(key)
			if err != nil {
				t.Fatalf("resolve block %d owner: %v", idx, err)
			}
			if owner.ID != localID {
				remoteBlocks++
				owners[owner.ID] = struct{}{}
			}
		}
		return remoteBlocks, owners
	}

	recorded := &blockPresenceCallRecorder{embeddedBlockCacheClient: fixture.cacheClient}
	batchCache := cache.NewCacheWithClient(recorded, &fixture.cacheConfig)
	legacy := &legacyBlockPresenceRecorder{Client: fixture.embeddedClient}
	fallbackCache := cache.NewCacheWithClient(legacy, &fixture.cacheConfig)
	rangeConfig := config.NewDefault()
	rangeConfig.Cache.SetBlockCachingEnabled(true)
	rangeConfig.Cache.BlockSize = embeddedBlockRangeBenchmarkBlockSize
	rangeConfig.Cache.SizeThreshold = int64(len(fixture.body))
	fallbackService := proxy.NewService(embeddedCacheHitBenchmarkForwarder{}, fallbackCache, rangeConfig)
	fallbackHandler := handlers.NewServer(fallbackService, "127.0.0.1", 0, false, rangeConfig.Server.MaxInflightRequests)
	fallbackGateway := httptest.NewServer(fallbackHandler.Router())
	t.Cleanup(fallbackGateway.Close)
	fallbackHTTP := fallbackGateway.Client()
	t.Cleanup(fallbackHTTP.CloseIdleConnections)

	for _, blockCount := range []int{2, 4, 8, 32} {
		t.Run(fmt.Sprintf("%d_blocks", blockCount), func(t *testing.T) {
			idxs := indices(blockCount)
			remoteBlocks, remoteOwners := recordRemoteOwners(idxs)
			if remoteBlocks == 0 || len(remoteOwners) != 1 {
				t.Fatalf("%d-block range has %d remote blocks across %d owners; want a remote owner", blockCount, remoteBlocks, len(remoteOwners))
			}
			recorded.pageSizes = nil
			fixture.stats.reset()
			got, err := batchCache.BlockExistsBatchErr(ctx, bucket, fixture.key, etag, embeddedBlockRangeBenchmarkBlockSize, idxs)
			if err != nil {
				t.Fatalf("batched presence: %v", err)
			}
			for i, present := range got {
				if !present {
					t.Errorf("batched presence[%d] = false, want present", i)
				}
			}
			if want := []int{blockCount}; !equalInts(recorded.pageSizes, want) {
				t.Errorf("batch page sizes = %v, want %v", recorded.pageSizes, want)
			}
			if got := fixture.stats.presenceRPCs.Load(); got != int64(len(remoteOwners)) {
				t.Errorf("remote presence RPCs = %d, want one for each of %d owners", got, len(remoteOwners))
			}
			if len(remoteOwners) == 0 {
				if got := fixture.stats.presenceReplyBytes.Load(); got != 0 {
					t.Errorf("local-only page returned %d remote response bytes", got)
				}
			} else if got := fixture.stats.presenceReplyBytes.Load(); got == 0 || got > int64(len(remoteOwners))*4096 {
				t.Errorf("presence response payload bytes = %d, want a compact nonempty result", got)
			}
			if got := fixture.stats.probeGetRPCs.Load(); got != 0 {
				t.Errorf("batched path opened %d per-key probe RPCs, want 0", got)
			}
			if got := fixture.stats.probePayloadBytes.Load(); got != 0 {
				t.Errorf("batched path transferred %d probe block bytes, want 0", got)
			}

			legacy.probeCalls = 0
			legacy.probeBytes = 0
			fixture.stats.reset()
			fallback, err := fallbackCache.BlockExistsBatchErr(ctx, bucket, fixture.key, etag, embeddedBlockRangeBenchmarkBlockSize, idxs)
			if err != nil {
				t.Fatalf("fallback presence: %v", err)
			}
			for i, present := range fallback {
				if !present {
					t.Errorf("fallback presence[%d] = false, want present", i)
				}
			}
			if legacy.probeCalls != blockCount {
				t.Errorf("fallback per-key probe calls = %d, want %d", legacy.probeCalls, blockCount)
			}
			if legacy.probeBytes != int64(2*blockCount) {
				t.Errorf("fallback probe payload bytes = %d, want %d", legacy.probeBytes, 2*blockCount)
			}
			if got := fixture.stats.probeGetRPCs.Load(); got != int64(remoteBlocks) {
				t.Errorf("fallback remote probe RPCs = %d, want %d", got, remoteBlocks)
			}
			if got := fixture.stats.probePayloadBytes.Load(); got != int64(2*remoteBlocks) {
				t.Errorf("fallback remote probe bytes = %d, want %d", got, 2*remoteBlocks)
			}
			if got := fixture.stats.presenceRPCs.Load(); got != 0 {
				t.Errorf("fallback path used %d batch presence RPCs, want 0", got)
			}

			fixture.stats.reset()
			assertEmbeddedRangeResponse(t, fixture.client, fixture.gateway.URL, fixture.key, fixture.body, blockCount, fixture.gatewaySigner, fixture.accessKey, fixture.secretKey)
			if got := fixture.stats.presenceRPCs.Load(); got != int64(len(remoteOwners)) {
				t.Errorf("range handler presence RPCs = %d, want one per remote owner (%d)", got, len(remoteOwners))
			}
			if got := fixture.stats.probeGetRPCs.Load(); got != 0 {
				t.Errorf("range handler opened %d per-key probe RPCs", got)
			}

			legacy.probeCalls = 0
			legacy.probeBytes = 0
			fixture.stats.reset()
			assertEmbeddedRangeResponse(t, fallbackHTTP, fallbackGateway.URL, fixture.key, fixture.body, blockCount, nil, "", "")
			if legacy.probeCalls != blockCount || legacy.probeBytes != int64(2*blockCount) {
				t.Errorf("fallback range preflight = (%d streams, %d bytes), want (%d, %d)", legacy.probeCalls, legacy.probeBytes, blockCount, 2*blockCount)
			}
		})
	}

	// A missing key stays a zero-payload miss in both paths and retains input
	// order. The range caller can then apply its existing fetch cap.
	missing := int64(31)
	missingKey := cache.MakeBlockKey(bucket, fixture.key, etag, embeddedBlockRangeBenchmarkBlockSize, missing)
	if err := fixture.embeddedClient.Delete(ctx, missingKey); err != nil {
		t.Fatalf("delete block %d: %v", missing, err)
	}
	all := indices(32)
	fixture.stats.reset()
	batch, err := batchCache.BlockExistsBatchErr(ctx, bucket, fixture.key, etag, embeddedBlockRangeBenchmarkBlockSize, all)
	if err != nil {
		t.Fatalf("batched presence after eviction: %v", err)
	}
	if batch[missing] || !batch[0] {
		t.Fatalf("batched order/presence at indexes 0,%d = %v, want present then absent", missing, batch)
	}
	legacy.probeCalls = 0
	legacy.probeBytes = 0
	fixture.stats.reset()
	fallback, err := fallbackCache.BlockExistsBatchErr(ctx, bucket, fixture.key, etag, embeddedBlockRangeBenchmarkBlockSize, all)
	if err != nil {
		t.Fatalf("fallback presence after eviction: %v", err)
	}
	if fallback[missing] || !fallback[0] {
		t.Fatalf("fallback order/presence at indexes 0,%d = %v, want present then absent", missing, fallback)
	}
	if legacy.probeCalls != len(all) {
		t.Errorf("fallback probes after one eviction = %d, want %d", legacy.probeCalls, len(all))
	}
	if want := int64(2 * (len(all) - 1)); legacy.probeBytes != want {
		t.Errorf("fallback probe payload bytes after one eviction = %d, want %d (the missing block contributes zero)", legacy.probeBytes, want)
	}

	// A peer must reject a request naming the wrong owner; the cache wrapper
	// converts that response into a discarded page, never an absent bit.
	coord := fixture.embeddedClient.Coordinator()
	var remoteKey, remoteAddress string
	for _, idx := range all {
		candidate := cache.MakeBlockKey(bucket, fixture.key, etag, embeddedBlockRangeBenchmarkBlockSize, idx)
		owner, err := coord.GetNodeForKey(candidate)
		if err != nil {
			t.Fatalf("resolve block owner: %v", err)
		}
		if owner.ID != coord.GetLocalNodeID() {
			remoteKey, remoteAddress = candidate, owner.ListenAddress
			break
		}
	}
	if remoteKey == "" {
		t.Fatal("fixture has no remote-owned block")
	}
	_, err = fixture.cacheClient.remoteBlockPresence(ctx, "wrong-owner", remoteAddress, coord.GetEpoch(), []string{remoteKey})
	if !errors.Is(err, cache.ErrBlockPresenceTopologyChanged) {
		t.Fatalf("wrong-owner response = %v, want topology-change fallback", err)
	}
	fixture.stats.reset()
	staleOwnerClient := &wrongOwnerBlockPresenceClient{
		embeddedBlockCacheClient: fixture.cacheClient,
		remoteKey:                remoteKey,
		remoteAddress:            remoteAddress,
		epoch:                    coord.GetEpoch(),
	}
	staleOwnerCache := cache.NewCacheWithClient(staleOwnerClient, &fixture.cacheConfig)
	remoteIndex, err := strconv.ParseInt(remoteKey[strings.LastIndexByte(remoteKey, '|')+1:], 10, 64)
	if err != nil {
		t.Fatalf("parse remote block index: %v", err)
	}
	retried, err := staleOwnerCache.BlockExistsBatchErr(ctx, bucket, fixture.key, etag, embeddedBlockRangeBenchmarkBlockSize, []int64{remoteIndex})
	if err != nil || len(retried) != 1 || !retried[0] {
		t.Fatalf("legacy probe after owner mismatch = %v, err=%v; want present", retried, err)
	}
	if got := fixture.stats.probeGetRPCs.Load(); got != 1 {
		t.Errorf("owner mismatch retry opened %d legacy Get RPCs, want 1", got)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = batchCache.BlockExistsBatchErr(canceled, bucket, fixture.key, etag, embeddedBlockRangeBenchmarkBlockSize, []int64{0})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled presence = %v, want context.Canceled", err)
	}

	// A replacement with a fresh ID owns the same deterministic block keys. The
	// next Range must route its grouped presence exchange to the replacement node.
	oldOwnerKey := cache.MakeBlockKey(bucket, fixture.key, etag, embeddedBlockRangeBenchmarkBlockSize, 1)
	oldOwner, err := coord.GetNodeForKey(oldOwnerKey)
	if err != nil || oldOwner.ID != "range-bench-1" {
		t.Fatalf("initial block owner = (%v,%v), want range-bench-1", oldOwner, err)
	}

	fixture.closeNodes[1]()
	waitForEmbeddedActiveNodeIDs(t, fixture.embeddedClient, "range-bench-0")
	replacement := startEmbeddedBlockRangeReplacementNode(t, fixture, "range-bench-replacement")
	waitForEmbeddedActiveNodeIDs(t, fixture.embeddedClient, "range-bench-0", "range-bench-replacement")
	waitForMatchingEmbeddedEpoch(t, fixture.embeddedClient, replacement)

	newOwner, err := coord.GetNodeForKey(oldOwnerKey)
	if err != nil || newOwner.ID != "range-bench-replacement" {
		t.Fatalf("replacement block owner = (%v,%v), want range-bench-replacement", newOwner, err)
	}
	blockStart := embeddedBlockRangeBenchmarkBlockSize
	if err := fixture.cacheClient.Put(ctx, oldOwnerKey, fixture.body[blockStart:2*blockStart], 60); err != nil {
		t.Fatalf("seed replacement block: %v", err)
	}
	fixture.stats.reset()
	assertEmbeddedRangeResponse(t, fixture.client, fixture.gateway.URL, fixture.key, fixture.body, 2, fixture.gatewaySigner, fixture.accessKey, fixture.secretKey)
	if got := fixture.stats.presenceRequests.Load(); got != 1 {
		t.Fatalf("replacement Range made %d presence attempts, want 1", got)
	}
}

func startEmbeddedBlockRangeReplacementNode(tb testing.TB, fixture *embeddedBlockRangeBenchmarkFixture, nodeID string) *embedded.Client {
	tb.Helper()
	diskPath := tb.TempDir()
	writeEmbeddedBlockRangeRingTokens(tb, diskPath, 1)
	presenceServer := &blockPresenceServer{}
	gossipAddr := embeddedBlockRangeFreeAddress(tb)
	grpcAddr := embeddedBlockRangeFreeAddress(tb)
	cfg := &embedded.Config{
		DiskPath:      diskPath,
		TTL:           fixture.cacheConfig.TTL,
		NodeID:        nodeID,
		ClusterAddr:   gossipAddr,
		GRPCAddr:      grpcAddr,
		AdvertiseAddr: grpcAddr,
		SeedNodes:     []string{fixture.seedGossipAddr},
		Registerer:    prometheus.NewRegistry(),
	}
	if fixture.cacheConfig.IsGRPCAuthEnabled() {
		cfg.GRPCServerOptions = auth.GRPCServerOptions(fixture.grpcToken)
		cfg.GRPCDialOptions = auth.GRPCDialOptions(fixture.grpcToken)
	}
	cfg.GRPCServerOptions = append(cfg.GRPCServerOptions, presenceServer.serverOption())
	client, err := embedded.New(cfg)
	if err != nil {
		tb.Fatalf("create replacement embedded node: %v", err)
	}
	presenceServer.client = client
	var closeOnce sync.Once
	tb.Cleanup(func() { closeOnce.Do(func() { _ = client.Close() }) })
	if err := client.StartGRPCServer(); err != nil {
		tb.Fatalf("start replacement embedded node: %v", err)
	}
	readyCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.WaitReady(readyCtx); err != nil {
		tb.Fatalf("replacement embedded node did not become ready: %v", err)
	}
	return client
}

func waitForMatchingEmbeddedEpoch(tb testing.TB, first, second *embedded.Client) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if first.Coordinator().GetEpoch() == second.Coordinator().GetEpoch() {
			return
		}
		select {
		case <-ctx.Done():
			tb.Fatalf("replacement ring epochs did not converge: first=%d second=%d: %v", first.Coordinator().GetEpoch(), second.Coordinator().GetEpoch(), ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForEmbeddedActiveNodeIDs(tb testing.TB, client *embedded.Client, want ...string) {
	tb.Helper()
	wantSet := make(map[string]struct{}, len(want))
	for _, id := range want {
		wantSet[id] = struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		nodes := client.Coordinator().GetRing().GetActiveNodes()
		if len(nodes) == len(wantSet) {
			allPresent := true
			for _, node := range nodes {
				if _, ok := wantSet[node.ID]; !ok {
					allPresent = false
					break
				}
			}
			if allPresent {
				return
			}
		}
		select {
		case <-ctx.Done():
			got := make([]string, 0, len(nodes))
			for _, node := range nodes {
				got = append(got, node.ID)
			}
			tb.Fatalf("active nodes = %v, want %v: %v", got, want, ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestEmbeddedBlockPresenceUsesClusterGRPCAuth(t *testing.T) {
	token := auth.DeriveGRPCAuthToken("access", "secret")
	gossip := []string{embeddedBlockRangeFreeAddress(t), embeddedBlockRangeFreeAddress(t)}
	grpcAddrs := []string{embeddedBlockRangeFreeAddress(t), embeddedBlockRangeFreeAddress(t)}
	clients := make([]*embedded.Client, 2)
	wrappers := make([]*embeddedBlockCacheClient, 2)
	for i := range clients {
		presenceServer := &blockPresenceServer{}
		serverOptions := auth.GRPCServerOptions(token)
		serverOptions = append(serverOptions, presenceServer.serverOption())
		cfg := &embedded.Config{
			DiskPath:          t.TempDir(),
			TTL:               time.Minute,
			NodeID:            fmt.Sprintf("auth-presence-%d", i),
			ClusterAddr:       gossip[i],
			GRPCAddr:          grpcAddrs[i],
			AdvertiseAddr:     grpcAddrs[i],
			GRPCServerOptions: serverOptions,
			GRPCDialOptions:   auth.GRPCDialOptions(token),
			Registerer:        prometheus.NewRegistry(),
		}
		if i > 0 {
			cfg.SeedNodes = []string{gossip[0]}
		}
		client, err := embedded.New(cfg)
		if err != nil {
			t.Fatalf("create authenticated embedded node %d: %v", i, err)
		}
		presenceServer.client = client
		clients[i] = client
		wrappers[i] = newEmbeddedBlockCacheClient(client, cfg.GRPCDialOptions...)
		wrapper := wrappers[i]
		t.Cleanup(func() { _ = wrapper.Close() })
		if err := client.StartGRPCServer(); err != nil {
			t.Fatalf("start authenticated embedded node %d: %v", i, err)
		}
	}
	readyCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, client := range clients {
		if err := client.WaitReady(readyCtx); err != nil {
			t.Fatalf("authenticated embedded node not ready: %v", err)
		}
	}
	for {
		ready := true
		for _, client := range clients {
			if len(client.Coordinator().GetRing().GetActiveNodes()) != len(clients) {
				ready = false
				break
			}
		}
		if ready {
			break
		}
		select {
		case <-readyCtx.Done():
			t.Fatalf("authenticated embedded cluster did not converge: %v", readyCtx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = embeddedBlockRangeBenchmarkBlockSize
	store := cache.NewCacheWithClient(wrappers[0], &cfg.Cache)
	const bucket, key, etag = "auth-presence", "object", `"auth"`
	var remoteIndex int64 = -1
	var remoteAddress string
	for i := int64(0); i < 1000; i++ {
		blockKey := cache.MakeBlockKey(bucket, key, etag, embeddedBlockRangeBenchmarkBlockSize, i)
		owner, err := clients[0].Coordinator().GetNodeForKey(blockKey)
		if err != nil {
			t.Fatalf("resolve authenticated block owner: %v", err)
		}
		if owner.ID != clients[0].Coordinator().GetLocalNodeID() {
			remoteIndex, remoteAddress = i, owner.ListenAddress
			break
		}
	}
	if remoteIndex < 0 {
		t.Fatal("authenticated cluster has no remote-owned key")
	}
	if err := store.PutBlock(context.Background(), bucket, key, etag, embeddedBlockRangeBenchmarkBlockSize, remoteIndex, []byte("AB"), 60); err != nil {
		t.Fatalf("store authenticated remote block: %v", err)
	}
	present, err := store.BlockExistsBatchErr(context.Background(), bucket, key, etag, embeddedBlockRangeBenchmarkBlockSize, []int64{remoteIndex})
	if err != nil || len(present) != 1 || !present[0] {
		t.Fatalf("authenticated remote presence = %v, err=%v, want present", present, err)
	}
	if _, err := wrappers[0].remoteBlockPresence(context.Background(), "wrong-owner", remoteAddress, clients[0].Coordinator().GetEpoch(), []string{cache.MakeBlockKey(bucket, key, etag, embeddedBlockRangeBenchmarkBlockSize, remoteIndex)}); !errors.Is(err, cache.ErrBlockPresenceTopologyChanged) {
		t.Fatalf("authenticated wrong-owner response = %v, want topology mismatch", err)
	}
}

func assertEmbeddedRangeResponse(tb testing.TB, client *http.Client, gatewayURL, key string, object []byte, blockCount int, signer *auth.RequestSigner, accessKey, secretKey string) {
	tb.Helper()
	rangeLen := blockCount * embeddedBlockRangeBenchmarkBlockSize
	req, err := http.NewRequest(http.MethodGet, gatewayURL+"/benchmark/"+key, nil)
	if err != nil {
		tb.Fatal(err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", rangeLen-1))
	if signer == nil {
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request, Signature=deadbeef")
	} else {
		signed, err := signer.SignRequest(req.Context(), req.Method, req.URL.RequestURI(), req.Body, "", accessKey, secretKey, req.Header)
		if err != nil {
			tb.Fatalf("sign range request: %v", err)
		}
		signed.RemoteAddr = req.RemoteAddr
		req = signed
	}
	resp, err := client.Do(req)
	if err != nil {
		tb.Fatalf("range request: %v", err)
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		tb.Fatalf("read range response: %v", readErr)
	}
	if closeErr != nil {
		tb.Fatalf("close range response: %v", closeErr)
	}
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get(proxy.XCacheHeader) != proxy.XCacheHit || !bytes.Equal(body, object[:rangeLen]) {
		tb.Fatalf("range response = (status=%d, cache=%q, bytes=%d, exact=%t), want exact cache 206", resp.StatusCode, resp.Header.Get(proxy.XCacheHeader), len(body), bytes.Equal(body, object[:rangeLen]))
	}
	if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes 0-%d/%d", rangeLen-1, len(object)); got != want {
		tb.Errorf("Content-Range=%q, want %q", got, want)
	}
	if got, want := resp.Header.Get("Content-Length"), fmt.Sprint(rangeLen); got != want {
		tb.Errorf("Content-Length=%q, want %q", got, want)
	}
	if got := resp.Header.Get("ETag"); got != `"block-range"` {
		tb.Errorf("ETag=%q, want %q", got, `"block-range"`)
	}
}

func TestBlockPresencePeerCancellationIsFatalUnlessSiblingCanceledGroup(t *testing.T) {
	peerError := status.Error(codes.Canceled, "peer canceled the presence exchange")
	callerCtx := context.Background()
	if isSiblingBlockPresenceCancellation(callerCtx, context.Background(), peerError) {
		t.Fatal("standalone peer cancellation was classified as a sibling cancellation")
	}

	siblingCtx, cancelSibling := context.WithCancel(callerCtx)
	cancelSibling()
	if !isSiblingBlockPresenceCancellation(callerCtx, siblingCtx, peerError) {
		t.Fatal("cancellation after a sibling failure was not recognized")
	}

	canceledCaller, cancelCaller := context.WithCancel(callerCtx)
	cancelCaller()
	if isSiblingBlockPresenceCancellation(canceledCaller, canceledCaller, peerError) {
		t.Fatal("caller cancellation was classified as a sibling cancellation")
	}
}

func TestEmbeddedBlockPresencePeerCancellationIsError(t *testing.T) {
	gossip := []string{embeddedBlockRangeFreeAddress(t), embeddedBlockRangeFreeAddress(t)}
	grpcAddrs := []string{embeddedBlockRangeFreeAddress(t), embeddedBlockRangeFreeAddress(t)}
	clients := make([]*embedded.Client, 2)
	var cacheClient *embeddedBlockCacheClient
	for i := range clients {
		cfg := &embedded.Config{
			DiskPath:      t.TempDir(),
			TTL:           time.Minute,
			NodeID:        fmt.Sprintf("cancel-presence-%d", i),
			ClusterAddr:   gossip[i],
			GRPCAddr:      grpcAddrs[i],
			AdvertiseAddr: grpcAddrs[i],
			Registerer:    prometheus.NewRegistry(),
		}
		if i > 0 {
			cfg.SeedNodes = []string{gossip[0]}
			cfg.GRPCServerOptions = append(cfg.GRPCServerOptions, grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
				method, ok := grpc.MethodFromServerStream(stream)
				if !ok || method != blockPresenceMethod {
					return status.Error(codes.Unimplemented, "unsupported method")
				}
				var request structpb.Struct
				if err := stream.RecvMsg(&request); err != nil {
					return err
				}
				return status.Error(codes.Canceled, "simulated owner cancellation")
			}))
		}
		client, err := embedded.New(cfg)
		if err != nil {
			t.Fatalf("create cancellation-test node %d: %v", i, err)
		}
		clients[i] = client
		if i == 0 {
			cacheClient = newEmbeddedBlockCacheClient(client)
			wrapper := cacheClient
			t.Cleanup(func() { _ = wrapper.Close() })
		} else {
			node := client
			t.Cleanup(func() { _ = node.Close() })
		}
		if err := client.StartGRPCServer(); err != nil {
			t.Fatalf("start cancellation-test node %d: %v", i, err)
		}
	}

	readyCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, client := range clients {
		if err := client.WaitReady(readyCtx); err != nil {
			t.Fatalf("cancellation-test node not ready: %v", err)
		}
	}
	for {
		ready := true
		for _, client := range clients {
			if len(client.Coordinator().GetRing().GetActiveNodes()) != len(clients) {
				ready = false
				break
			}
		}
		if ready {
			break
		}
		select {
		case <-readyCtx.Done():
			t.Fatalf("cancellation-test cluster did not converge: %v", readyCtx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = embeddedBlockRangeBenchmarkBlockSize
	store := cache.NewCacheWithClient(cacheClient, &cfg.Cache)
	const bucket, object, etag = "cancel-presence", "object", `"cancel"`
	var remoteIndex int64 = -1
	for i := int64(0); i < 1000; i++ {
		candidate := cache.MakeBlockKey(bucket, object, etag, embeddedBlockRangeBenchmarkBlockSize, i)
		owner, err := clients[0].Coordinator().GetNodeForKey(candidate)
		if err != nil {
			t.Fatalf("resolve cancellation-test owner: %v", err)
		}
		if owner.ID != clients[0].Coordinator().GetLocalNodeID() {
			remoteIndex = i
			break
		}
	}
	if remoteIndex < 0 {
		t.Fatal("cancellation-test cluster has no remote key")
	}

	_, err := store.BlockExistsBatchErr(context.Background(), bucket, object, etag, embeddedBlockRangeBenchmarkBlockSize, []int64{remoteIndex})
	if err == nil || status.Code(err) != codes.Canceled {
		t.Fatalf("peer cancellation result = (err=%v), want a canceled error rather than an absent bit", err)
	}
}

func TestEmbeddedBlockPresenceCancelsInFlightOwnerRPC(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	server := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		method, ok := grpc.MethodFromServerStream(stream)
		if !ok || method != blockPresenceMethod {
			return status.Error(codes.Unimplemented, "unsupported method")
		}
		var request structpb.Struct
		if err := stream.RecvMsg(&request); err != nil {
			return err
		}
		close(started)
		<-stream.Context().Done()
		return status.FromContextError(stream.Context().Err()).Err()
	}))
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	defer listener.Close()

	client := newEmbeddedBlockCacheClient(nil)
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.remoteBlockPresence(ctx, "owner", listener.Addr().String(), 0, []string{"blk|b|k|e|4|0"})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("presence RPC never reached the owner")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled owner RPC error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled presence RPC did not return")
	}
}

func equalInts(got, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
