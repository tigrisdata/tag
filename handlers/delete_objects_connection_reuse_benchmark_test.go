package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

const (
	deleteObjectsConnectionBenchmarkConcurrency = config.DefaultServerMaxInflightRequests
	deleteObjectsConnectionBenchmarkBatchSize   = 3
	deleteObjectsConnectionBenchmarkBucket      = "benchmark-bucket"
	deleteObjectsConnectionBenchmarkClientHost  = "s3-client.example.com"
)

type deleteObjectsConnectionBurstGate struct {
	want    int64
	arrived atomic.Int64
	release chan struct{}
	once    sync.Once
}

func (g *deleteObjectsConnectionBurstGate) wait() {
	if g.arrived.Add(1) >= g.want {
		g.releaseAll()
	}
	<-g.release
}

func (g *deleteObjectsConnectionBurstGate) releaseAll() {
	g.once.Do(func() { close(g.release) })
}

type deleteObjectsConnectionReuseBenchmark struct {
	client   *http.Client
	requests []*http.Request
	bodies   [][]byte
	gate     atomic.Pointer[deleteObjectsConnectionBurstGate]
}

func newDeleteObjectsConnectionReuseBenchmark(tb testing.TB) *deleteObjectsConnectionReuseBenchmark {
	tb.Helper()

	for _, name := range []string{
		"TAG_MODE",
		"TAG_TRANSPARENT_PROXY",
		"TAG_UPSTREAM_ENDPOINT",
		"TAG_UPSTREAM_REGION",
		"TAG_MAX_IDLE_CONNS_PER_HOST",
		"TAG_MAX_INFLIGHT_REQUESTS",
		"TAG_CACHE_DISABLED",
	} {
		tb.Setenv(name, "")
	}

	fixture := &deleteObjectsConnectionReuseBenchmark{}
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Proto != "HTTP/1.1" {
			http.Error(w, "benchmark origin requires HTTP/1.1", http.StatusBadRequest)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/"+deleteObjectsConnectionBenchmarkBucket || r.URL.RawQuery != "delete" {
			http.Error(w, "unexpected DeleteObjects request", http.StatusBadRequest)
			return
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
			http.Error(w, "signed S3 request missing Authorization", http.StatusBadRequest)
			return
		}
		if r.Header.Get("X-Tigris-Proxy-Signature") == "" || r.Header.Get("X-Tigris-Forwarded-Host") != deleteObjectsConnectionBenchmarkClientHost {
			http.Error(w, "transparent proxy headers missing", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.HasPrefix(body, []byte("<Delete")) {
			http.Error(w, "invalid DeleteObjects body", http.StatusBadRequest)
			return
		}
		var deleteRequest struct {
			Objects []struct {
				Key string `xml:"Key"`
			} `xml:"Object"`
		}
		if err := xml.Unmarshal(body, &deleteRequest); err != nil || len(deleteRequest.Objects) != deleteObjectsConnectionBenchmarkBatchSize {
			http.Error(w, "invalid DeleteObjects object list", http.StatusBadRequest)
			return
		}
		gate := fixture.gate.Load()
		if gate == nil {
			http.Error(w, "missing burst gate", http.StatusInternalServerError)
			return
		}
		gate.wait()

		var response strings.Builder
		response.WriteString(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for _, object := range deleteRequest.Objects {
			fmt.Fprintf(&response, "<Deleted><Key>%s</Key></Deleted>", object.Key)
		}
		response.WriteString("</DeleteResult>")
		responseBody := []byte(response.String())
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("Content-Length", fmt.Sprint(len(responseBody)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(responseBody)
	}))
	upstream.EnableHTTP2 = false
	upstream.StartTLS()
	tb.Cleanup(upstream.Close)

	certPath := filepath.Join(tb.TempDir(), "delete-objects-upstream-cert.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: upstream.Certificate().Raw})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		tb.Fatalf("write local upstream certificate: %v", err)
	}
	tb.Setenv("SSL_CERT_FILE", certPath)

	cfg := config.NewDefault()
	if cfg.ResolvedMode() != config.ModeTransparent || !cfg.Cache.IsEnabled() {
		tb.Fatal("benchmark requires the default transparent proxy with cache enabled")
	}
	if cfg.Server.MaxInflightRequests != config.DefaultServerMaxInflightRequests {
		tb.Fatalf("server admission = %d, want default %d", cfg.Server.MaxInflightRequests, config.DefaultServerMaxInflightRequests)
	}
	if cfg.Upstream.MaxIdleConnsPerHost != config.DefaultMaxIdleConnsPerHost {
		tb.Fatalf("upstream idle cap = %d, want configured default %d", cfg.Upstream.MaxIdleConnsPerHost, config.DefaultMaxIdleConnsPerHost)
	}
	cfg.Upstream.Endpoint = upstream.URL
	cacheStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	forwarder := proxy.NewForwarder(
		nil,
		cfg.Upstream.Endpoint,
		cfg.Upstream.Region,
		cfg.Upstream.MaxIdleConnsPerHost,
		auth.NewProxySigner("delete-objects-benchmark", "delete-objects-benchmark-secret"),
		nil,
	)
	service := proxy.NewService(forwarder, cacheStore, cfg)
	router := NewServer(service, "127.0.0.1", 0, false, cfg.Server.MaxInflightRequests).Router()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		router.ServeHTTP(w, r)
	}))
	tb.Cleanup(gateway.Close)

	clientTransport := &http.Transport{
		MaxIdleConns:        deleteObjectsConnectionBenchmarkConcurrency,
		MaxIdleConnsPerHost: deleteObjectsConnectionBenchmarkConcurrency,
		IdleConnTimeout:     90 * time.Second,
	}
	fixture.client = &http.Client{Transport: clientTransport, Timeout: 30 * time.Second}
	tb.Cleanup(fixture.client.CloseIdleConnections)

	gatewayURL, err := url.Parse(gateway.URL)
	if err != nil {
		tb.Fatalf("parse gateway URL: %v", err)
	}
	signer := auth.NewRequestSigner("http://"+deleteObjectsConnectionBenchmarkClientHost, cfg.Upstream.Region)
	fixture.requests = make([]*http.Request, deleteObjectsConnectionBenchmarkConcurrency)
	fixture.bodies = make([][]byte, deleteObjectsConnectionBenchmarkConcurrency)
	for worker := 0; worker < deleteObjectsConnectionBenchmarkConcurrency; worker++ {
		var body strings.Builder
		body.WriteString(`<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for object := 0; object < deleteObjectsConnectionBenchmarkBatchSize; object++ {
			fmt.Fprintf(&body, "<Object><Key>burst-object-%03d-%02d</Key></Object>", worker, object)
		}
		body.WriteString(`</Delete>`)
		fixture.bodies[worker] = []byte(body.String())
		hash := sha256.Sum256(fixture.bodies[worker])
		req, err := signer.SignRequest(
			context.Background(),
			http.MethodPost,
			"/"+deleteObjectsConnectionBenchmarkBucket+"?delete",
			bytes.NewReader(fixture.bodies[worker]),
			hex.EncodeToString(hash[:]),
			"BENCHMARKACCESSKEY",
			"benchmark-secret-key",
			http.Header{"Content-Type": {"application/xml"}},
		)
		if err != nil {
			tb.Fatalf("sign benchmark DeleteObjects request: %v", err)
		}
		requestURL := *gatewayURL
		requestURL.Path = "/" + deleteObjectsConnectionBenchmarkBucket
		requestURL.RawQuery = "delete"
		req.URL = &requestURL
		req.Host = deleteObjectsConnectionBenchmarkClientHost
		fixture.requests[worker] = req
	}

	return fixture
}

func (f *deleteObjectsConnectionReuseBenchmark) runBurst() error {
	gate := &deleteObjectsConnectionBurstGate{
		want:    deleteObjectsConnectionBenchmarkConcurrency,
		release: make(chan struct{}),
	}
	f.gate.Store(gate)

	var wg sync.WaitGroup
	errs := make(chan error, deleteObjectsConnectionBenchmarkConcurrency)
	wg.Add(deleteObjectsConnectionBenchmarkConcurrency)
	for worker := 0; worker < deleteObjectsConnectionBenchmarkConcurrency; worker++ {
		go func(worker int) {
			defer wg.Done()
			request := f.requests[worker].Clone(context.Background())
			request.Body = io.NopCloser(bytes.NewReader(f.bodies[worker]))
			request.ContentLength = int64(len(f.bodies[worker]))
			response, err := f.client.Do(request)
			if err != nil {
				gate.releaseAll()
				errs <- fmt.Errorf("send DeleteObjects request %d: %w", worker, err)
				return
			}
			if response.StatusCode != http.StatusOK {
				gate.releaseAll()
				_ = response.Body.Close()
				errs <- fmt.Errorf("DeleteObjects status = %d, want %d", response.StatusCode, http.StatusOK)
				return
			}
			if _, err := io.Copy(io.Discard, response.Body); err != nil {
				gate.releaseAll()
				_ = response.Body.Close()
				errs <- fmt.Errorf("read DeleteObjects response %d: %w", worker, err)
				return
			}
			if err := response.Body.Close(); err != nil {
				gate.releaseAll()
				errs <- fmt.Errorf("close DeleteObjects response %d: %w", worker, err)
				return
			}
		}(worker)
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

// BenchmarkDeleteObjectsConnectionReuse measures the registered, cache-enabled
// transparent DeleteObjects handler in warmed HTTP/1.1 TLS upstream bursts. Each
// benchmark operation is one default-admission-sized burst of signed three-key
// deletes, matching the repository's TestSDK_DeleteObjects batch shape.
func BenchmarkDeleteObjectsConnectionReuse(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	fixture := newDeleteObjectsConnectionReuseBenchmark(b)
	if err := fixture.runBurst(); err != nil {
		b.Fatalf("warm DeleteObjects burst: %v", err)
	}
	for b.Loop() {
		if err := fixture.runBurst(); err != nil {
			b.Fatal(err)
		}
	}
}
