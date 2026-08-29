package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

type benchmarkCompletionClient struct {
	cacheclient.CacheClient
	metadataPrefix string
	completed      chan<- struct{}
}

func (c *benchmarkCompletionClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	err := c.CacheClient.Put(ctx, key, data, ttlSeconds)
	if err == nil && strings.HasPrefix(key, c.metadataPrefix) {
		c.completed <- struct{}{}
	}
	return err
}

type benchmarkWarmForwarder struct {
	*mockForwarder
	body []byte

	mu      sync.RWMutex
	gate    <-chan struct{}
	started chan<- struct{}
}

func (f *benchmarkWarmForwarder) setBatch(gate <-chan struct{}, started chan<- struct{}) {
	f.mu.Lock()
	f.gate = gate
	f.started = started
	f.mu.Unlock()
}

func (f *benchmarkWarmForwarder) DoFullObjectRequest(_ context.Context, _, _, _, _ string) (*http.Response, error) {
	f.mu.RLock()
	gate, started := f.gate, f.started
	f.mu.RUnlock()
	started <- struct{}{}

	headers := make(http.Header)
	headers.Set("Content-Length", "4194304")
	headers.Set("Content-Type", "application/octet-stream")
	headers.Set("ETag", `"background-benchmark"`)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     headers,
		Body: &benchmarkGateReader{
			Reader: bytes.NewReader(f.body),
			gate:   gate,
		},
		ContentLength: int64(len(f.body)),
	}, nil
}

type benchmarkGateReader struct {
	io.Reader
	gate <-chan struct{}
	once sync.Once
}

func (r *benchmarkGateReader) Read(p []byte) (int, error) {
	r.once.Do(func() { <-r.gate })
	return r.Reader.Read(p)
}

func (r *benchmarkGateReader) Close() error { return nil }

// benchmarkBackgroundCachePopulate measures the ordinary detached background-warm
// entry point with two concurrent cacheable 4 MiB objects. A shared 256 MiB populate
// budget leaves room for both the old relay reservations and the direct writers, so
// both arms perform the same cache work while admission remains in the measured
// execution. The gate releases both origin bodies together so the run does not
// accidentally become a serial warm benchmark.
func benchmarkBackgroundCachePopulate(b *testing.B, blockMode bool) {
	const (
		objectSize  = 4 << 20
		concurrency = 2
	)
	body := bytes.Repeat([]byte("x"), objectSize)
	bucket := "benchmark-bucket"
	keys := []string{"benchmark-key-0", "benchmark-key-1"}
	completed := make(chan struct{}, concurrency)

	cfg := config.NewDefault()
	cfg.Cache.MaxConcurrentWrites = concurrency
	cfg.Cache.MaxPopulateMemoryBytes = 256 << 20
	cfg.Cache.SetBlockCachingEnabled(blockMode)
	cfg.Cache.BlockSize = 1 << 20
	if !blockMode {
		cfg.Cache.BlockSize = objectSize + 1
	}

	baseClient := cacheclient.NewMemoryCache()
	cacheStore := cache.NewCacheWithClient(&benchmarkCompletionClient{
		CacheClient:    baseClient,
		metadataPrefix: cache.MakeMetaKey(bucket, ""),
		completed:      completed,
	}, &cfg.Cache)
	forwarder := &benchmarkWarmForwarder{
		mockForwarder: &mockForwarder{},
		body:          body,
	}
	svc := NewService(forwarder, cacheStore, cfg)

	b.ReportAllocs()
	for b.Loop() {
		gate := make(chan struct{})
		started := make(chan struct{}, concurrency)
		forwarder.setBatch(gate, started)
		for _, key := range keys {
			svc.triggerBackgroundCacheFetch(bucket, key, "access", "secret", false, priorityReadMiss)
		}

		for range keys {
			select {
			case <-started:
			case <-time.After(10 * time.Second):
				b.Fatal("background cache fetch did not reach the origin")
			}
		}
		close(gate)

		for range keys {
			select {
			case <-completed:
			case <-time.After(10 * time.Second):
				b.Fatal("background cache populate did not complete")
			}
		}

		// Metadata is committed just before triggerBackgroundCacheFetch removes each
		// in-flight marker. Wait for both removals before reusing the keys next round.
		deadline := time.NewTimer(10 * time.Second)
		for {
			allGone := true
			for _, key := range keys {
				if _, loaded := svc.activeBackgroundFetches.Load("bg:" + bucket + "/" + key); loaded {
					allGone = false
					break
				}
			}
			if allGone {
				break
			}
			select {
			case <-deadline.C:
				b.Fatal("background cache dedup markers did not clear")
			default:
				runtime.Gosched()
			}
		}
		if !deadline.Stop() {
			<-deadline.C
		}
	}
}

func BenchmarkBackgroundBlockCachePopulate(b *testing.B) {
	benchmarkBackgroundCachePopulate(b, true)
}

func BenchmarkBackgroundWholeObjectCachePopulate(b *testing.B) {
	benchmarkBackgroundCachePopulate(b, false)
}
