package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/embedded"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/handlers"
	"github.com/tigrisdata/tag/proxy"
)

// embeddedCacheHitBenchmarkForwarder is only used for authentication on a warm
// cache hit. Any request that needs the upstream is a fixture failure.
type embeddedCacheHitBenchmarkForwarder struct{}

func (embeddedCacheHitBenchmarkForwarder) Forward(context.Context, http.ResponseWriter, *http.Request) error {
	return errors.New("unexpected upstream forward")
}

func (embeddedCacheHitBenchmarkForwarder) ForwardWithCapture(context.Context, http.ResponseWriter, *http.Request) (*proxy.ResponseCapture, error) {
	return nil, errors.New("unexpected upstream capture")
}

func (embeddedCacheHitBenchmarkForwarder) ValidateAndGetCredentials(*http.Request) (proxy.AuthResult, string, string, error) {
	return proxy.AuthValidated, "access", "secret", nil
}

func (embeddedCacheHitBenchmarkForwarder) DoRequestWithCreds(context.Context, *http.Request, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected upstream request")
}

func (embeddedCacheHitBenchmarkForwarder) DoFullObjectRequest(context.Context, string, string, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected upstream full-object request")
}

func (embeddedCacheHitBenchmarkForwarder) DoAnonymousFullObjectRequest(context.Context, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected upstream anonymous request")
}

func (embeddedCacheHitBenchmarkForwarder) DoConditionalGetRequest(context.Context, string, string, string, string, string, int64, string) (*http.Response, error) {
	return nil, errors.New("unexpected upstream conditional GET")
}

func (embeddedCacheHitBenchmarkForwarder) DoConditionalHeadRequest(context.Context, string, string, string, string, string, int64) (*http.Response, error) {
	return nil, errors.New("unexpected upstream conditional HEAD")
}

type embeddedCacheHitBenchmarkFixture struct {
	gateway *httptest.Server
	client  *http.Client
	body    []byte
}

func newEmbeddedCacheHitBenchmarkFixture(tb testing.TB, size int) *embeddedCacheHitBenchmarkFixture {
	tb.Helper()

	cfg := config.NewDefault()
	cfg.Cache.DiskPath = tb.TempDir()
	cfg.Cache.SizeThreshold = int64(size)
	cfg.Cache.SetBlockCachingEnabled(false)

	embeddedCache, err := embedded.New(&embedded.Config{
		DiskPath: cfg.Cache.DiskPath,
		TTL:      cfg.Cache.TTL,
	})
	if err != nil {
		tb.Fatalf("create embedded cache: %v", err)
	}
	tb.Cleanup(func() { _ = embeddedCache.Close() })

	store := cache.NewCacheWithClient(newEmbeddedBlockCacheClient(embeddedCache), &cfg.Cache)
	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i*31 + i/257)
	}
	meta := &cache.CachedObjectMeta{
		Bucket:        "benchmark",
		Key:           "cache-hit",
		ETag:          `"cache-hit"`,
		ContentType:   "application/octet-stream",
		ContentLength: int64(size),
		StatusCode:    http.StatusOK,
	}
	if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, body, 0); err != nil {
		tb.Fatalf("seed embedded cache: %v", err)
	}

	service := proxy.NewService(embeddedCacheHitBenchmarkForwarder{}, store, cfg)
	server := handlers.NewServer(service, "127.0.0.1", 0, false, cfg.Server.MaxInflightRequests)
	gateway := httptest.NewServer(server.Router())
	tb.Cleanup(gateway.Close)
	client := gateway.Client()
	tb.Cleanup(client.CloseIdleConnections)

	return &embeddedCacheHitBenchmarkFixture{gateway: gateway, client: client, body: body}
}

func (f *embeddedCacheHitBenchmarkFixture) get() error {
	resp, err := f.client.Get(f.gateway.URL + "/benchmark/cache-hit")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	n, readErr := io.Copy(io.Discard, resp.Body)
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if resp.Header.Get(proxy.XCacheHeader) != proxy.XCacheHit {
		return fmt.Errorf("%s = %q, want %q", proxy.XCacheHeader, resp.Header.Get(proxy.XCacheHeader), proxy.XCacheHit)
	}
	if n != int64(len(f.body)) {
		return fmt.Errorf("body bytes = %d, want %d", n, len(f.body))
	}
	return nil
}

func (f *embeddedCacheHitBenchmarkFixture) warm(tb testing.TB) {
	tb.Helper()

	resp, err := f.client.Get(f.gateway.URL + "/benchmark/cache-hit")
	if err != nil {
		tb.Fatalf("warm cache hit: %v", err)
	}
	got, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		tb.Fatalf("read warm cache hit: %v", readErr)
	}
	if closeErr != nil {
		tb.Fatalf("close warm cache hit: %v", closeErr)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get(proxy.XCacheHeader) != proxy.XCacheHit {
		tb.Fatalf("warm cache hit response = (status=%d, cache=%q), want (200, %q)", resp.StatusCode, resp.Header.Get(proxy.XCacheHeader), proxy.XCacheHit)
	}
	if !bytes.Equal(got, f.body) {
		tb.Fatalf("warm cache hit body mismatch: got %d bytes, want %d", len(got), len(f.body))
	}
}

func BenchmarkHandlerWarmedEmbeddedCacheHit(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	for _, size := range []int{128 << 10, 1 << 20, 4 << 20} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			fixture := newEmbeddedCacheHitBenchmarkFixture(b, size)
			fixture.warm(b)
			b.SetBytes(int64(len(fixture.body)))
			b.ResetTimer()
			for b.Loop() {
				if err := fixture.get(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
