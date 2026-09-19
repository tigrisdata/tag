package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	runtimemetrics "runtime/metrics"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

const (
	transparentAuthGETBenchmarkAccessKey = "AKIAIOSFODNN7EXAMPLE"
	transparentAuthGETBenchmarkSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	transparentAuthGETBenchmarkRegion    = "us-east-1"
	transparentAuthGETBenchmarkBucket    = "benchmark-bucket"
	transparentAuthGETBenchmarkCPUMetric = "/cpu/classes/user:cpu-seconds"
)

type transparentAuthGETBenchmarkCPUCounter struct {
	samples []runtimemetrics.Sample
}

func newTransparentAuthGETBenchmarkCPUCounter(tb testing.TB) *transparentAuthGETBenchmarkCPUCounter {
	tb.Helper()
	counter := &transparentAuthGETBenchmarkCPUCounter{
		samples: []runtimemetrics.Sample{{Name: transparentAuthGETBenchmarkCPUMetric}},
	}
	_ = counter.nanoseconds(tb)
	return counter
}

func (c *transparentAuthGETBenchmarkCPUCounter) nanoseconds(tb testing.TB) int64 {
	tb.Helper()
	runtimemetrics.Read(c.samples)
	if c.samples[0].Value.Kind() != runtimemetrics.KindFloat64 {
		tb.Fatalf("runtime metric %q kind = %v, want float64", transparentAuthGETBenchmarkCPUMetric, c.samples[0].Value.Kind())
	}
	return int64(c.samples[0].Value.Float64() * float64(time.Second))
}

type transparentAuthGETBenchmarkKeyProvider struct {
	store *auth.DerivedKeyStore
	calls atomic.Uint64
}

func (p *transparentAuthGETBenchmarkKeyProvider) GetSigningKey(accessKey, date, region string) ([]byte, error) {
	p.calls.Add(1)
	return p.store.GetSigningKey(accessKey, date, region)
}

func (p *transparentAuthGETBenchmarkKeyProvider) HasKey(accessKey string) bool {
	return p.store.HasKey(accessKey)
}

type transparentAuthGETBenchmarkResponseWriter struct {
	header http.Header
	bytes  int
}

func (w *transparentAuthGETBenchmarkResponseWriter) Header() http.Header {
	return w.header
}

func (w *transparentAuthGETBenchmarkResponseWriter) WriteHeader(int) {}

func (w *transparentAuthGETBenchmarkResponseWriter) Write(p []byte) (int, error) {
	w.bytes += len(p)
	return len(p), nil
}

func (w *transparentAuthGETBenchmarkResponseWriter) reset() {
	for key := range w.header {
		delete(w.header, key)
	}
	w.bytes = 0
}

func newTransparentAuthGETBenchmarkCase(b *testing.B, grant bool) (http.Handler, *http.Request, *transparentAuthGETBenchmarkKeyProvider, *atomic.Uint64, *atomic.Uint64) {
	b.Helper()

	var upstreamRequests atomic.Uint64
	var upstreamOK atomic.Uint64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("benchmark"))
		upstreamOK.Add(1)
	}))
	b.Cleanup(upstream.Close)

	derivedKeyStore := auth.NewDerivedKeyStore(auth.DefaultDerivedKeyTTL)
	keyProvider := &transparentAuthGETBenchmarkKeyProvider{store: derivedKeyStore}
	authzCache := auth.NewAuthzCache(time.Hour)
	if grant {
		authzCache.Grant(transparentAuthGETBenchmarkAccessKey, transparentAuthGETBenchmarkBucket)
	}

	localAuth := &proxy.LocalAuthConfig{
		DerivedKeyStore: derivedKeyStore,
		Validator:       auth.NewRequestValidator(keyProvider),
		AuthzCache:      authzCache,
	}
	forwarder := proxy.NewForwarder(
		auth.NewCredentialStore(),
		upstream.URL,
		transparentAuthGETBenchmarkRegion,
		1,
		auth.NewProxySigner("benchmark-proxy-access-key", "benchmark-proxy-secret-key"),
		localAuth,
	)
	cfg := config.NewDefault()
	cfg.Cache.SetEnabled(false)
	service := proxy.NewService(forwarder, cache.NewDisabledCache(), cfg)
	server := NewServer(service, "127.0.0.1", 0, false, 0)

	signer := auth.NewRequestSigner(upstream.URL, transparentAuthGETBenchmarkRegion)
	req, err := signer.SignRequest(
		context.Background(),
		http.MethodGet,
		"/"+transparentAuthGETBenchmarkBucket+"/object.txt?partNumber=1",
		nil,
		"",
		transparentAuthGETBenchmarkAccessKey,
		transparentAuthGETBenchmarkSecretKey,
		nil,
	)
	if err != nil {
		b.Fatal(err)
	}

	info, err := auth.ParseAuthInfo(req)
	if err != nil {
		b.Fatal(err)
	}
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(transparentAuthGETBenchmarkAccessKey, transparentAuthGETBenchmarkSecretKey)
	signingKey, err := credentials.GetSigningKey(transparentAuthGETBenchmarkAccessKey, info.Date, info.Region)
	if err != nil {
		b.Fatal(err)
	}
	derivedKeyStore.Store(info.AccessKey, info.Date, info.Region, signingKey)

	return server.Router(), req, keyProvider, &upstreamRequests, &upstreamOK
}

// BenchmarkTransparentAuthGETRoute measures the normal handlers.Server route for
// transparent authenticated GETs. The upstream test server returns a fixed 200
// response, so the benchmark includes ValidateAndGetCredentials, HandleGetObject,
// the transparent upstream exchange, and response streaming rather than only the
// local decision helper.
func BenchmarkTransparentAuthGETRoute(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	b.Run("GrantMiss", func(b *testing.B) {
		router, req, keyProvider, upstreamRequests, upstreamOK := newTransparentAuthGETBenchmarkCase(b, false)
		writer := &transparentAuthGETBenchmarkResponseWriter{header: make(http.Header)}
		cpu := newTransparentAuthGETBenchmarkCPUCounter(b)
		b.ReportAllocs()
		cpuStart := cpu.nanoseconds(b)
		b.ResetTimer()
		for b.Loop() {
			writer.reset()
			router.ServeHTTP(writer, req)
		}
		b.StopTimer()
		cpuEnd := cpu.nanoseconds(b)
		b.ReportMetric(float64(cpuEnd-cpuStart)/float64(b.N), "cpu-ns/op")
		b.ReportMetric(float64(keyProvider.calls.Load())/float64(b.N), "validator-calls/op")
		b.ReportMetric(float64(upstreamRequests.Load())/float64(b.N), "upstream-requests/op")
		b.ReportMetric(float64(upstreamOK.Load())/float64(b.N), "upstream-200/op")
	})

	b.Run("GrantHit", func(b *testing.B) {
		router, req, keyProvider, upstreamRequests, upstreamOK := newTransparentAuthGETBenchmarkCase(b, true)
		writer := &transparentAuthGETBenchmarkResponseWriter{header: make(http.Header)}
		cpu := newTransparentAuthGETBenchmarkCPUCounter(b)
		cpuStart := cpu.nanoseconds(b)
		b.ResetTimer()
		for b.Loop() {
			writer.reset()
			router.ServeHTTP(writer, req)
		}
		b.StopTimer()
		cpuEnd := cpu.nanoseconds(b)
		b.ReportMetric(float64(cpuEnd-cpuStart)/float64(b.N), "cpu-ns/op")
		b.ReportMetric(float64(keyProvider.calls.Load())/float64(b.N), "validator-calls/op")
		b.ReportMetric(float64(upstreamRequests.Load())/float64(b.N), "upstream-requests/op")
		b.ReportMetric(float64(upstreamOK.Load())/float64(b.N), "upstream-200/op")
	})

	b.Run("GrantMix50_50", func(b *testing.B) {
		missRouter, missReq, missProvider, missUpstreamRequests, missUpstreamOK := newTransparentAuthGETBenchmarkCase(b, false)
		hitRouter, hitReq, hitProvider, hitUpstreamRequests, hitUpstreamOK := newTransparentAuthGETBenchmarkCase(b, true)
		missWriter := &transparentAuthGETBenchmarkResponseWriter{header: make(http.Header)}
		hitWriter := &transparentAuthGETBenchmarkResponseWriter{header: make(http.Header)}
		cpu := newTransparentAuthGETBenchmarkCPUCounter(b)
		b.ReportAllocs()
		iteration := 0
		cpuStart := cpu.nanoseconds(b)
		b.ResetTimer()
		for b.Loop() {
			if iteration%2 == 0 {
				missWriter.reset()
				missRouter.ServeHTTP(missWriter, missReq)
			} else {
				hitWriter.reset()
				hitRouter.ServeHTTP(hitWriter, hitReq)
			}
			iteration++
		}
		b.StopTimer()
		cpuEnd := cpu.nanoseconds(b)
		b.ReportMetric(float64(cpuEnd-cpuStart)/float64(b.N), "cpu-ns/op")
		b.ReportMetric(float64(missProvider.calls.Load()+hitProvider.calls.Load())/float64(b.N), "validator-calls/op")
		b.ReportMetric(float64(missUpstreamRequests.Load()+hitUpstreamRequests.Load())/float64(b.N), "upstream-requests/op")
		b.ReportMetric(float64(missUpstreamOK.Load()+hitUpstreamOK.Load())/float64(b.N), "upstream-200/op")
	})
}
