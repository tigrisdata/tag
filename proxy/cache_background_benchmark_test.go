package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"runtime"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

type benchmarkCompletionClient struct {
	cacheclient.CacheClient
	metadataKey string
	completed   chan<- struct{}
}

func (c *benchmarkCompletionClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	err := c.CacheClient.Put(ctx, key, data, ttlSeconds)
	if err == nil && key == c.metadataKey {
		c.completed <- struct{}{}
	}
	return err
}

// BenchmarkBackgroundFullObjectCachePopulate measures the ordinary detached
// background warm entry point with a cacheable whole object. The completion
// client synchronizes with the asynchronous trigger so the benchmark includes
// the background populate rather than only the trigger call. The same cache key
// is reused after the trigger removes its in-flight dedup marker.
func BenchmarkBackgroundFullObjectCachePopulate(b *testing.B) {
	const objectSize = 4 << 20
	body := bytes.Repeat([]byte("x"), objectSize)
	bucket, key := "benchmark-bucket", "benchmark-key"
	completed := make(chan struct{}, 1)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.BlockSize = objectSize + 1
	baseClient := cacheclient.NewMemoryCache()
	cacheStore := cache.NewCacheWithClient(&benchmarkCompletionClient{
		CacheClient: baseClient,
		metadataKey: cache.MakeMetaKey(bucket, key),
		completed:   completed,
	}, &cfg.Cache)
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
		svc.triggerBackgroundCacheFetch(bucket, key, "access", "secret", false, priorityReadMiss)
		select {
		case <-completed:
		case <-time.After(10 * time.Second):
			b.Fatal("background cache populate did not complete")
		}
		// Metadata is committed just before triggerBackgroundCacheFetch removes the
		// dedup marker. Do not let the next iteration be coalesced as a false no-op.
		for {
			if _, loaded := svc.activeBackgroundFetches.Load("bg:" + bucket + "/" + key); !loaded {
				break
			}
			runtime.Gosched()
		}
	}
}
