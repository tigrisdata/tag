package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// blockPutBenchmarkServer accepts block writes without retaining them. gRPC still
// unmarshals every request, so the benchmark includes the client/server transport
// work while avoiding storage I/O variability.
type blockPutBenchmarkServer struct {
	pb.UnimplementedCacheServiceServer
	blockLen int
}

func (s *blockPutBenchmarkServer) Put(stream pb.CacheService_PutServer) error {
	var total int
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		total += len(req.Data)
	}
	if total != s.blockLen {
		return status.Errorf(codes.DataLoss, "streamed block length = %d, want %d", total, s.blockLen)
	}
	return stream.SendAndClose(&pb.PutResponse{Success: true})
}

func (s *blockPutBenchmarkServer) PutObject(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if len(req.Data) != s.blockLen {
		return nil, status.Errorf(codes.DataLoss, "unary block length = %d, want %d", len(req.Data), s.blockLen)
	}
	return &pb.PutResponse{Success: true}, nil
}

func (*blockPutBenchmarkServer) Get(*pb.GetRequest, pb.CacheService_GetServer) error {
	return status.Error(codes.NotFound, "key not found")
}

// remoteBlockBenchmarkClient models the embedded cache's remote write path. Its
// stream buffer matches ocache/server/operations.DefaultStreamBufferSize; Put
// issues the unary request used by the byte-slice fast path. Read operations are
// delegated to a regular client so fetchOneBlock's presence probe reaches the
// same gRPC server.
type remoteBlockBenchmarkClient struct {
	cacheclient.CacheClient
	rpc             pb.CacheServiceClient
	streamBuf       sync.Pool
	stagedCopyBytes int64
}

func newRemoteBlockBenchmarkClient(client cacheclient.CacheClient, rpc pb.CacheServiceClient) *remoteBlockBenchmarkClient {
	c := &remoteBlockBenchmarkClient{CacheClient: client, rpc: rpc}
	c.streamBuf.New = func() any { return make([]byte, 1<<20) }
	return c
}

func (c *remoteBlockBenchmarkClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	resp, err := c.rpc.PutObject(ctx, &pb.PutRequest{Key: key, Data: data, TtlSeconds: ttlSeconds})
	if err != nil {
		return err
	}
	if resp != nil && !resp.Success {
		return fmt.Errorf("put failed: %s", resp.Error)
	}
	return nil
}

func (c *remoteBlockBenchmarkClient) PutBlockBytes(ctx context.Context, key string, data []byte, ttlSeconds int64) (bool, error) {
	return true, c.Put(ctx, key, data, ttlSeconds)
}

func (c *remoteBlockBenchmarkClient) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	stream, err := c.rpc.Put(ctx)
	if err != nil {
		return err
	}

	buf := c.streamBuf.Get().([]byte)
	defer c.streamBuf.Put(buf)

	first := true
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			// r is backed by fetchOneBlock's staged block. Copying it into buf is
			// the work the unary path is intended to remove.
			c.stagedCopyBytes += int64(n)
			req := &pb.PutRequest{Data: buf[:n]}
			if first {
				req.Key = key
				req.TtlSeconds = ttlSeconds
				first = false
			}
			if err := stream.Send(req); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if resp != nil && !resp.Success {
		return fmt.Errorf("put failed: %s", resp.Error)
	}
	return nil
}

// blockPutBenchmarkForwarder supplies a validated range response without making
// a copy of the staged block before fetchOneBlock reads it.
type blockPutBenchmarkForwarder struct {
	*blockMockForwarder
}

func newBlockPutBenchmarkForwarder(body []byte, etag string) *blockPutBenchmarkForwarder {
	return &blockPutBenchmarkForwarder{blockMockForwarder: newBlockMock(body, etag)}
}

func (m *blockPutBenchmarkForwarder) DoConditionalGetRequest(_ context.Context, _, _, _, _, _ string, _ int64, rangeHeader string) (*http.Response, error) {
	parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid range %q", rangeHeader)
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, err
	}
	if start < 0 || end < start || end >= int64(len(m.object)) {
		return nil, fmt.Errorf("range %d-%d outside %d-byte object", start, end, len(m.object))
	}

	h := http.Header{}
	h.Set("ETag", m.etag)
	h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(m.object)))
	h.Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	return &http.Response{
		StatusCode:    http.StatusPartialContent,
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(m.object[start : end+1])),
		ContentLength: end - start + 1,
	}, nil
}

func benchmarkFetchOneBlockRemoteCacheWrite(b *testing.B, blockLen int) {
	b.Helper()

	// fetchOneBlock logs every miss at debug level. Keep fixture logs out of
	// benchmark output; the production default already suppresses that path.
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(cacheclient.MaxMessageSize))
	pb.RegisterCacheServiceServer(server, &blockPutBenchmarkServer{blockLen: blockLen})
	go func() { _ = server.Serve(listener) }()
	b.Cleanup(server.Stop)

	readClient, err := cacheclient.NewSimpleClient(&cacheclient.ClientConfig{
		Addrs:              []string{listener.Addr().String()},
		Mode:               cacheclient.ModeSimple,
		ConnectionPoolSize: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = readClient.Close() })

	conn, err := grpc.Dial(listener.Addr().String(), cacheclient.DefaultDialOptions()...)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = conn.Close() })

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = int64(blockLen)
	cfg.Cache.SizeThreshold = int64(blockLen)

	body := make([]byte, blockLen)
	for i := range body {
		body[i] = byte(i)
	}
	forwarder := newBlockPutBenchmarkForwarder(body, `"benchmark"`)
	remoteClient := newRemoteBlockBenchmarkClient(readClient, pb.NewCacheServiceClient(conn))
	cacheStore := cache.NewCacheWithClient(remoteClient, &cfg.Cache)
	svc := NewService(forwarder, cacheStore, cfg)
	meta := &cache.CachedObjectMeta{
		ETag:          `"benchmark"`,
		ContentLength: int64(blockLen),
		BlockSize:     int64(blockLen),
	}

	// Establish the gRPC connections and warm the block-buffer pool before timing.
	if _, err := svc.fetchOneBlock(context.Background(), "benchmark", "block", "access", "secret", meta, 0); err != nil {
		b.Fatalf("warm fetchOneBlock: %v", err)
	}

	remoteClient.stagedCopyBytes = 0
	b.SetBytes(int64(blockLen))
	b.ResetTimer()
	for b.Loop() {
		if _, err := svc.fetchOneBlock(context.Background(), "benchmark", "block", "access", "secret", meta, 0); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(remoteClient.stagedCopyBytes)/float64(b.N), "staged_copy_B/op")
}

// BenchmarkFetchOneBlockRemoteCacheWrite measures a block-mode range miss after
// the full response has been validated and staged. It covers the default block
// size and a larger configured block size on the remote cache-owner path.
func BenchmarkFetchOneBlockRemoteCacheWrite(b *testing.B) {
	for _, blockLen := range []int{1 << 20, 2 << 20, 4 << 20} {
		b.Run(strconv.Itoa(blockLen), func(b *testing.B) {
			benchmarkFetchOneBlockRemoteCacheWrite(b, blockLen)
		})
	}
}
