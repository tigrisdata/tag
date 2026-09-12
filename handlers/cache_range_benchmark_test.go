package handlers

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	runtimemetrics "runtime/metrics"
	"strconv"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

type rangeBenchmarkForwarder struct{}

func (*rangeBenchmarkForwarder) Forward(context.Context, http.ResponseWriter, *http.Request) error {
	return errors.New("unexpected upstream request")
}

func (*rangeBenchmarkForwarder) ForwardWithCapture(context.Context, http.ResponseWriter, *http.Request) (*proxy.ResponseCapture, error) {
	return nil, errors.New("unexpected upstream request")
}

func (*rangeBenchmarkForwarder) ValidateAndGetCredentials(*http.Request) (proxy.AuthResult, string, string, error) {
	return proxy.AuthValidated, "access", "secret", nil
}

func (*rangeBenchmarkForwarder) DoRequestWithCreds(context.Context, *http.Request, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected upstream request")
}

func (*rangeBenchmarkForwarder) DoFullObjectRequest(context.Context, string, string, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected upstream request")
}

func (*rangeBenchmarkForwarder) DoAnonymousFullObjectRequest(context.Context, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected upstream request")
}

func (*rangeBenchmarkForwarder) DoObjectDeleteRequest(context.Context, string, string, string, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected upstream request")
}

func (*rangeBenchmarkForwarder) DoConditionalGetRequest(context.Context, string, string, string, string, string, int64, string) (*http.Response, error) {
	return nil, errors.New("unexpected upstream request")
}

func (*rangeBenchmarkForwarder) DoConditionalHeadRequest(context.Context, string, string, string, string, string, int64) (*http.Response, error) {
	return nil, errors.New("unexpected upstream request")
}

var rangeBenchmarkSink []byte

func benchmarkServeRangeThroughRouter(b *testing.B, size int) {
	b.Helper()

	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	const (
		bucket = "range-benchmark-bucket"
		key    = "range-benchmark-key"
		etag   = `"range-benchmark"`
	)
	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i)
	}

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	cacheStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          etag,
		ContentType:   "application/octet-stream",
		ContentLength: int64(size),
		StatusCode:    http.StatusOK,
	}
	if err := cacheStore.PutWithMeta(context.Background(), bucket, key, meta, body, 0); err != nil {
		b.Fatalf("seed cache: %v", err)
	}

	server := NewServer(proxy.NewService(&rangeBenchmarkForwarder{}, cacheStore, cfg), "127.0.0.1", 0, false, 0)
	route := server.Router()
	rangeHeader := "bytes=0-" + strconv.Itoa(size-1)
	serve := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
		r.Header.Set("Range", rangeHeader)
		route.ServeHTTP(w, r)
		return w
	}

	// Warm the cache path and validate the complete response before timing.
	w := serve()
	if w.Code != http.StatusPartialContent {
		b.Fatalf("status = %d, want %d", w.Code, http.StatusPartialContent)
	}
	if got := w.Header().Get(proxy.XCacheHeader); got != proxy.XCacheHit {
		b.Fatalf("%s = %q, want %q", proxy.XCacheHeader, got, proxy.XCacheHit)
	}
	if got := w.Header().Get("Content-Length"); got != strconv.Itoa(size) {
		b.Fatalf("Content-Length = %q, want %d", got, size)
	}
	wantRange := "bytes 0-" + strconv.Itoa(size-1) + "/" + strconv.Itoa(size)
	if got := w.Header().Get("Content-Range"); got != wantRange {
		b.Fatalf("Content-Range = %q, want %q", got, wantRange)
	}
	if !bytes.Equal(w.Body.Bytes(), body) {
		b.Fatalf("body length = %d, want %d", w.Body.Len(), len(body))
	}

	b.ReportAllocs()
	cpuSamples := []runtimemetrics.Sample{{Name: "/cpu/classes/user:cpu-seconds"}}
	runtimemetrics.Read(cpuSamples)
	cpuStart := cpuSamples[0].Value.Float64()
	b.ResetTimer()
	for b.Loop() {
		w := serve()
		rangeBenchmarkSink = w.Body.Bytes()
	}
	b.StopTimer()
	runtimemetrics.Read(cpuSamples)
	b.ReportMetric((cpuSamples[0].Value.Float64()-cpuStart)*1e9/float64(b.N), "cpu-ns/op")
}

// BenchmarkServeRangeFromCache measures warm, non-block, single-range cache hits
// through the transparent-mode production HTTP router at the small, default-scale,
// and large range sizes served in production. The response is validated before
// timing and each measured iteration writes the complete range into an HTTP response
// recorder.
func BenchmarkServeRangeFromCache(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{name: "1K", size: 1 << 10},
		{name: "64K", size: 64 << 10},
		{name: "1M", size: 1 << 20},
		{name: "4M", size: 4 << 20},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkServeRangeThroughRouter(b, tc.size)
		})
	}
}
