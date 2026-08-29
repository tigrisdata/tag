package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

// BenchmarkBackgroundFullObjectCachePopulate measures the detached full-object
// warm path with a cacheable whole object. The response body is reused as an
// upstream fixture, while the cache entry is overwritten each iteration so the
// benchmark measures the write path rather than setup or cache lookup.
func BenchmarkBackgroundFullObjectCachePopulate(b *testing.B) {
	const objectSize = 4 << 20
	body := bytes.Repeat([]byte("x"), objectSize)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.BlockSize = objectSize + 1
	cacheStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	forwarder := &mockForwarder{
		doFullObjectFunc: func(_ context.Context, _, _, _, _ string) (*http.Response, error) {
			headers := make(http.Header)
			headers.Set("Content-Length", "4194304")
			headers.Set("Content-Type", "application/octet-stream")
			headers.Set("ETag", `"background-benchmark"`)
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        headers,
				Body:          io.NopCloser(bytes.NewReader(body)),
				ContentLength: int64(len(body)),
			}, nil
		},
	}
	svc := NewService(forwarder, cacheStore, cfg)

	b.ReportAllocs()
	for b.Loop() {
		broadcaster := broadcast.NewBroadcaster(cfg.Broadcast.ChannelBuffer)
		if err := svc.fetchFullObjectToCache(
			context.Background(),
			"benchmark-bucket",
			"benchmark-key",
			"access",
			"secret",
			false,
			broadcaster,
			priorityReadMiss,
		); err != nil {
			b.Fatal(err)
		}
	}
}
