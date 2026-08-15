package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

func cacheableGetResponse(body, etag string) *http.Response {
	headers := make(http.Header)
	headers.Set("Content-Length", strconv.Itoa(len(body)))
	headers.Set("Content-Type", "text/plain")
	headers.Set("ETag", etag)
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        headers,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

// waitForCacheMeta observes publication through the cache's public read path.
// Cache writes commit the body before metadata, so a visible metadata entry can
// safely serve a later GET.
func waitForCacheMeta(t testing.TB, cacheStore *cache.Cache, bucket, key string, timeout time.Duration) {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		_, found, err := cacheStore.GetMeta(context.Background(), bucket, key)
		if err != nil {
			t.Fatalf("read cache metadata for %s/%s: %v", bucket, key, err)
		}
		if found {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("cache metadata was not published for %s/%s", bucket, key)
		case <-ticker.C:
		}
	}
}

// handlerCompletionTracker records the point at which a serial test request's
// TAG handler has returned. A Content-Length lets a client finish reading the
// first body before that point, so this records completion separately.
type handlerCompletionTracker struct {
	done chan error
}

func newHandlerCompletionTracker() *handlerCompletionTracker {
	return &handlerCompletionTracker{done: make(chan error, 1)}
}

func (t *handlerCompletionTracker) complete(err error) {
	t.done <- err
}

// newCacheableGetCompletionBenchmark constructs a real HTTP boundary around a
// cacheable full-GET miss. The benchmark waits for the handler return while its
// timer is running, then waits for asynchronous cache publication with timing
// stopped so each iteration starts with the same admitted-populate state.
func newCacheableGetCompletionBenchmark(b *testing.B) (string, *http.Client, func(), *handlerCompletionTracker, *cache.Cache) {
	b.Helper()

	const body = "cacheable full GET benchmark body"
	cfg := config.NewDefault()
	cacheStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	forwarder := &mockForwarder{
		doRequestFunc: func(_ context.Context, _ *http.Request, _ string, _ string) (*http.Response, error) {
			return cacheableGetResponse(body, `"cacheable-get-benchmark"`), nil
		},
	}
	svc := NewService(forwarder, cacheStore, cfg)
	completions := newHandlerCompletionTracker()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := svc.HandleGetObject(w, r)
		completions.complete(err)
	}))
	// Keep the connection alive after the Content-Length body is read so the
	// request context remains available while the cache listener starts.
	client := &http.Client{
		Transport: &http.Transport{},
		Timeout:   5 * time.Second,
	}

	return server.URL, client, func() {
		client.CloseIdleConnections()
		server.Close()
	}, completions, cacheStore
}

// BenchmarkCacheableGetHandlerCompletion measures a cacheable full GET miss
// through an HTTP server. It includes the first client's body read and waits
// until TAG's handler has returned; it validates eventual cache publication
// outside the timed region before issuing the next unique-key miss.
func BenchmarkCacheableGetHandlerCompletion(b *testing.B) {
	const (
		bucket = "benchmark-bucket"
		body   = "cacheable full GET benchmark body"
	)
	serverURL, client, cleanup, completions, cacheStore := newCacheableGetCompletionBenchmark(b)
	defer cleanup()

	for i := 0; b.Loop(); i++ {
		key := strconv.Itoa(i)
		path := "/" + bucket + "/" + key
		resp, err := client.Get(serverURL + path)
		if err != nil {
			b.Fatalf("GET %s: %v", path, err)
		}
		got, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			resp.Body.Close()
			b.Fatalf("read %s: %v", path, readErr)
		}
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("GET %s status = %d, want %d", path, resp.StatusCode, http.StatusOK)
		}
		if string(got) != body {
			b.Fatalf("GET %s body = %q, want %q", path, got, body)
		}
		select {
		case handlerErr := <-completions.done:
			if handlerErr != nil {
				resp.Body.Close()
				b.Fatalf("handler %s: %v", path, handlerErr)
			}
		case <-time.After(5 * time.Second):
			resp.Body.Close()
			b.Fatalf("handler did not complete for %s", path)
		}
		if err := resp.Body.Close(); err != nil {
			b.Fatalf("close %s: %v", path, err)
		}

		// Cache publication is asynchronous and not paid by the first client.
		// Waiting with timing stopped both validates the eventual cache hit and
		// prevents outstanding populates from changing the next iteration's work.
		b.StopTimer()
		waitForCacheMeta(b, cacheStore, bucket, key, 5*time.Second)
		b.StartTimer()
	}
}
