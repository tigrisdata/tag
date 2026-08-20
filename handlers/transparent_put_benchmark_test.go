package handlers

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

const (
	transparentPutBenchmarkBucket = "benchmark-bucket"
	transparentPutBenchmarkKey    = "transparent-put-object"
	transparentPutBenchmarkETag   = `"transparent-put-etag"`
)

var (
	transparentPutBenchmarkBody     = bytes.Repeat([]byte("transparent-put-body-"), 256)
	transparentPutBenchmarkResponse = []byte("transparent-put-response")
)

// transparentPutBenchmarkFixture sends authenticated PUTs through the public
// handler route and transparent forwarder to a real HTTP upstream. The upstream
// checks the forwarded body and returns ordinary response metadata, so each
// benchmark operation consumes the same response a client would receive.
type transparentPutBenchmarkFixture struct {
	gateway          *httptest.Server
	client           *http.Client
	upstreamRequests atomic.Int64
}

func newTransparentPutBenchmarkFixture(tb testing.TB) *transparentPutBenchmarkFixture {
	tb.Helper()

	fixture := &transparentPutBenchmarkFixture{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixture.upstreamRequests.Add(1)
		if r.Method != http.MethodPut {
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Authorization") == "" {
			http.Error(w, "missing client authorization", http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, transparentPutBenchmarkBody) {
			http.Error(w, "unexpected request body", http.StatusBadRequest)
			return
		}

		w.Header().Set("ETag", transparentPutBenchmarkETag)
		w.Header().Set("X-Amz-Request-Id", "benchmark-request")
		w.Header().Set("X-Upstream-Metadata", "benchmark-metadata")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(transparentPutBenchmarkResponse)
	}))
	tb.Cleanup(upstream.Close)

	cfg := config.NewDefault()
	cfg.Upstream.Endpoint = upstream.URL
	cacheStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	forwarder := proxy.NewForwarder(
		nil,
		cfg.Upstream.Endpoint,
		cfg.Upstream.Region,
		cfg.Upstream.MaxIdleConnsPerHost,
		auth.NewProxySigner("benchmark-proxy-access", "benchmark-proxy-secret"),
		nil,
	)
	service := proxy.NewService(forwarder, cacheStore, cfg)
	fixture.gateway = httptest.NewServer(NewServer(service, "127.0.0.1", 0, false, cfg.Server.MaxInflightRequests).Router())
	tb.Cleanup(fixture.gateway.Close)

	fixture.client = &http.Client{Transport: &http.Transport{
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 16,
	}}
	tb.Cleanup(fixture.client.CloseIdleConnections)
	return fixture
}

func (f *transparentPutBenchmarkFixture) put() error {
	req, err := http.NewRequest(
		http.MethodPut,
		f.gateway.URL+"/"+transparentPutBenchmarkBucket+"/"+transparentPutBenchmarkKey,
		bytes.NewReader(transparentPutBenchmarkBody),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=benchmark/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=deadbeef")
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("ETag") != transparentPutBenchmarkETag || !bytes.Equal(body, transparentPutBenchmarkResponse) {
		return &transparentPutBenchmarkResponseError{status: resp.StatusCode, etag: resp.Header.Get("ETag"), body: body}
	}
	return nil
}

type transparentPutBenchmarkResponseError struct {
	status int
	etag   string
	body   []byte
}

func (e *transparentPutBenchmarkResponseError) Error() string {
	return fmt.Sprintf("transparent PUT response = (status=%d, etag=%q, body=%q)", e.status, e.etag, e.body)
}

// BenchmarkTransparentPutObject measures concurrent authenticated PUTs through
// handlers.Server, proxy.Service.HandlePutObject, and the transparent forwarder
// with the default enabled-cache configuration. The warm request establishes both
// HTTP connection pools before the timed normal PUT load.
func BenchmarkTransparentPutObject(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	fixture := newTransparentPutBenchmarkFixture(b)
	if err := fixture.put(); err != nil {
		b.Fatal(err)
	}

	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := fixture.put(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()

	if got, want := fixture.upstreamRequests.Load(), int64(b.N+1); got != want {
		b.Fatalf("upstream PUTs = %d, want %d", got, want)
	}
}
