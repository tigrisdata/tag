package handlers

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
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

// rangeServeBenchmarkCacheServer models a delayed remote cache owner. The benchmark seeds the
// owner before timing, so every RPC is a warm metadata or block read and the call count exposes
// the probe pass separately from payload reads.
type rangeServeBenchmarkCacheServer struct {
	pb.UnimplementedCacheServiceServer
	values map[string][]byte
	delay  time.Duration
	calls  atomic.Int64
}

func (s *rangeServeBenchmarkCacheServer) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	s.calls.Add(1)
	if s.delay > 0 {
		timer := time.NewTimer(s.delay)
		select {
		case <-timer.C:
		case <-stream.Context().Done():
			timer.Stop()
			return stream.Context().Err()
		}
	}
	data, ok := s.values[req.Key]
	if !ok {
		return status.Error(codes.NotFound, "benchmark key not found")
	}
	start, end := req.Start, req.End
	if start < 0 || start >= int64(len(data)) {
		return status.Error(codes.InvalidArgument, "benchmark range start out of bounds")
	}
	if end <= 0 || end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	if end < start {
		return status.Error(codes.InvalidArgument, "benchmark range end before start")
	}
	return stream.Send(&pb.GetResponse{Data: data[start : end+1]})
}

// rangeServeBenchmarkResponseWriter records the first committed 206 while discarding the body.
// It still checks a checksum and byte count after each operation so the benchmark cannot compare
// a truncated or reordered response as if it were a successful range serve.
type rangeServeBenchmarkResponseWriter struct {
	header   http.Header
	status   int
	written  int64
	checksum uint64
	first206 time.Time
	expected int64
	wantSum  uint64
}

func (w *rangeServeBenchmarkResponseWriter) reset(expected int64, wantSum uint64) {
	w.header = make(http.Header)
	w.status = 0
	w.written = 0
	w.checksum = 0
	w.first206 = time.Time{}
	w.expected = expected
	w.wantSum = wantSum
}

func (w *rangeServeBenchmarkResponseWriter) Header() http.Header { return w.header }

func (w *rangeServeBenchmarkResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	if status == http.StatusPartialContent {
		w.first206 = time.Now()
	}
}

func (w *rangeServeBenchmarkResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	for _, b := range p {
		w.checksum += uint64(b)
	}
	w.written += int64(len(p))
	return len(p), nil
}

func (w *rangeServeBenchmarkResponseWriter) valid() bool {
	return w.status == http.StatusPartialContent && w.written == w.expected && w.checksum == w.wantSum
}

func benchmarkRangeSum(body []byte, start, end int) uint64 {
	var sum uint64
	for _, b := range body[start : end+1] {
		sum += uint64(b)
	}
	return sum
}

func BenchmarkServeBlocksCompleteRangeRemote(b *testing.B) {
	for _, blocks := range []int{2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("%d-blocks", blocks), func(b *testing.B) {
			oldLogger := log.Logger
			log.Logger = log.Logger.Level(zerolog.WarnLevel)
			b.Cleanup(func() { log.Logger = oldLogger })

			const blockSize = 16 << 10
			const (
				bucket = "benchmark"
				key    = "complete-range"
				etag   = `"benchmark"`
			)
			body := make([]byte, blocks*blockSize)
			for i := range body {
				body[i] = byte(i / blockSize)
			}
			values := make(map[string][]byte, blocks+1)
			meta := &cache.CachedObjectMeta{
				Bucket:         bucket,
				Key:            key,
				ETag:           etag,
				ContentLength:  int64(len(body)),
				StatusCode:     http.StatusOK,
				BlockSize:      blockSize,
				BlocksComplete: true,
				CachedAt:       time.Now().Unix(),
			}
			for idx := 0; idx < blocks; idx++ {
				start := idx * blockSize
				values[cache.MakeBlockKey(bucket, key, etag, blockSize, int64(idx))] = body[start : start+blockSize]
			}
			metaBytes, err := meta.Encode()
			if err != nil {
				b.Fatalf("encode benchmark metadata: %v", err)
			}
			values[cache.MakeMetaKey(bucket, key)] = metaBytes

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			owner := &rangeServeBenchmarkCacheServer{
				values: values,
				delay:  50 * time.Microsecond,
			}
			grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(cacheclient.MaxMessageSize))
			pb.RegisterCacheServiceServer(grpcServer, owner)
			go func() { _ = grpcServer.Serve(listener) }()
			b.Cleanup(func() {
				grpcServer.Stop()
				_ = listener.Close()
			})

			remote, err := cacheclient.NewSimpleClient(&cacheclient.ClientConfig{
				Addrs:              []string{listener.Addr().String()},
				Mode:               cacheclient.ModeSimple,
				ConnectionPoolSize: 1,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = remote.Close() })

			cfg := config.NewDefault()
			cfg.Cache.SetBlockCachingEnabled(true)
			cfg.Cache.BlockSize = blockSize
			cfg.Cache.SizeThreshold = int64(len(body))
			store := cache.NewCacheWithClient(remote, &cfg.Cache)
			svc := proxy.NewService(handlerPrefetchForwarder{}, store, cfg)
			handler := NewServer(svc, "127.0.0.1", 0, false, 0)
			rangeEnd := len(body) - 1
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request, Signature=deadbeef")
			req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", rangeEnd))
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": key})

			// Establish the gRPC connection and validate the fixture before timing. The cache owner
			// remains immutable and all later operations are warm range hits.
			warmWriter := &rangeServeBenchmarkResponseWriter{}
			warmWriter.reset(int64(len(body)), benchmarkRangeSum(body, 0, rangeEnd))
			handler.handleObject(warmWriter, req)
			if !warmWriter.valid() {
				b.Fatalf("warm benchmark request: status=%d written=%d", warmWriter.status, warmWriter.written)
			}
			owner.calls.Store(0)

			var totalFirst206 int64
			b.ResetTimer()
			for b.Loop() {
				writer := &rangeServeBenchmarkResponseWriter{}
				writer.reset(int64(len(body)), benchmarkRangeSum(body, 0, rangeEnd))
				started := time.Now()
				handler.handleObject(writer, req)
				if !writer.valid() {
					b.Fatalf("invalid range response: status=%d written=%d checksum=%d", writer.status, writer.written, writer.checksum)
				}
				if writer.first206.IsZero() {
					b.Fatal("range serve did not commit a 206")
				}
				totalFirst206 += writer.first206.Sub(started).Nanoseconds()
			}
			b.StopTimer()
			b.ReportMetric(float64(totalFirst206)/float64(b.N), "first-206-ns/op")
			b.ReportMetric(float64(owner.calls.Load())/float64(b.N), "cache-calls/op")
		})
	}
}

var _ io.Writer = (*rangeServeBenchmarkResponseWriter)(nil)
