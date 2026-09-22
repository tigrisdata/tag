package handlers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	bodyReadBenchmarkConcurrency = 16
	bodyReadBenchmarkStall       = 40 * time.Millisecond
	bodyReadBenchmarkGap         = 1 * time.Millisecond
	bodyReadBenchmarkChunkSize   = 32 * 1024
)

var errBodyReadBenchmarkStall = status.Error(codes.Unavailable, "benchmark cache peer stalled")

type bodyReadBenchmarkContextKey struct{}

type bodyReadBenchmarkControl struct {
	started chan<- struct{}
	release <-chan struct{}
}

type bodyReadBenchmarkResult struct {
	latency time.Duration
	err     error
}

type bodyReadBenchmarkMode uint8

const (
	bodyReadBenchmarkStalled bodyReadBenchmarkMode = iota
	bodyReadBenchmarkHealthy
)

type bodyReadBenchmarkCacheClient struct {
	cacheclient.CacheClient
	mode bodyReadBenchmarkMode
	body []byte
}

func (c *bodyReadBenchmarkCacheClient) GetStream(ctx context.Context, _ string, w io.Writer) error {
	if control, ok := ctx.Value(bodyReadBenchmarkContextKey{}).(bodyReadBenchmarkControl); ok {
		control.started <- struct{}{}
		if control.release != nil {
			<-control.release
		}
	}

	if c.mode == bodyReadBenchmarkStalled {
		timer := time.NewTimer(bodyReadBenchmarkStall)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return errBodyReadBenchmarkStall
		}
	}

	for offset := 0; offset < len(c.body); offset += bodyReadBenchmarkChunkSize {
		end := offset + bodyReadBenchmarkChunkSize
		if end > len(c.body) {
			end = len(c.body)
		}
		if _, err := w.Write(c.body[offset:end]); err != nil {
			return err
		}
		if end < len(c.body) {
			time.Sleep(bodyReadBenchmarkGap)
		}
	}
	return nil
}

type bodyReadBenchmarkForwarder struct {
	body []byte
}

func (bodyReadBenchmarkForwarder) Forward(context.Context, http.ResponseWriter, *http.Request) error {
	return errors.New("unexpected upstream forward")
}

func (bodyReadBenchmarkForwarder) ForwardWithCapture(context.Context, http.ResponseWriter, *http.Request) (*proxy.ResponseCapture, error) {
	return nil, errors.New("unexpected upstream capture")
}

func (bodyReadBenchmarkForwarder) ValidateAndGetCredentials(*http.Request) (proxy.AuthResult, string, string, error) {
	return proxy.AuthValidated, "access", "secret", nil
}

func (f bodyReadBenchmarkForwarder) DoRequestWithCreds(context.Context, *http.Request, string, string) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Cache-Control":  []string{"no-store"},
			"Content-Length": []string{"13"},
			"Content-Type":   []string{"text/plain"},
		},
		Body: io.NopCloser(bytes.NewReader(f.body)),
	}, nil
}

func (bodyReadBenchmarkForwarder) DoFullObjectRequest(context.Context, string, string, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected full object request")
}

func (bodyReadBenchmarkForwarder) DoAnonymousFullObjectRequest(context.Context, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected anonymous full object request")
}

func (bodyReadBenchmarkForwarder) DoConditionalGetRequest(context.Context, string, string, string, string, string, int64, string) (*http.Response, error) {
	return nil, errors.New("unexpected conditional request")
}

func (bodyReadBenchmarkForwarder) DoConditionalHeadRequest(context.Context, string, string, string, string, string, int64) (*http.Response, error) {
	return nil, errors.New("unexpected conditional head request")
}

type bodyReadBenchmarkFixture struct {
	handler http.Handler
	body    []byte
	keys    []string
	mode    bodyReadBenchmarkMode
}

func newBodyReadBenchmarkFixture(tb testing.TB, mode bodyReadBenchmarkMode) *bodyReadBenchmarkFixture {
	tb.Helper()

	bodySize := bodyReadBenchmarkChunkSize * 128
	if mode == bodyReadBenchmarkStalled {
		bodySize = bodyReadBenchmarkChunkSize * 6
	}
	body := bytes.Repeat([]byte("cached body"), (bodySize+len("cached body")-1)/len("cached body"))[:bodySize]
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	base := cacheclient.NewMemoryCache()
	client := &bodyReadBenchmarkCacheClient{
		CacheClient: base,
		mode:        mode,
		body:        body,
	}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	keys := make([]string, bodyReadBenchmarkConcurrency+1)
	for i := range keys {
		keys[i] = "key-" + string(rune('a'+i))
		meta := &cache.CachedObjectMeta{
			Bucket:        "bucket",
			Key:           keys[i],
			ETag:          `"etag"`,
			ContentType:   "text/plain",
			ContentLength: int64(len(body)),
			StatusCode:    http.StatusOK,
		}
		if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, body, 0); err != nil {
			tb.Fatalf("seed cache: %v", err)
		}
	}

	service := proxy.NewService(&bodyReadBenchmarkForwarder{body: []byte("upstream body")}, store, cfg)
	server := NewServer(service, "127.0.0.1", 0, false, bodyReadBenchmarkConcurrency)
	return &bodyReadBenchmarkFixture{handler: server.Router(), body: body, keys: keys, mode: mode}
}

func bodyReadBenchmarkRequest(key string, control *bodyReadBenchmarkControl) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://example.com/bucket/"+key, nil)
	if control != nil {
		r = r.WithContext(context.WithValue(r.Context(), bodyReadBenchmarkContextKey{}, *control))
	}
	return r
}

func validateBodyReadBenchmarkResponse(w *httptest.ResponseRecorder, want []byte, wantCache string) error {
	if w.Code != http.StatusOK {
		return errors.New("unexpected HTTP status")
	}
	if w.Header().Get(proxy.XCacheHeader) != wantCache {
		return errors.New("unexpected cache status")
	}
	if !bytes.Equal(w.Body.Bytes(), want) {
		return errors.New("unexpected response body")
	}
	return nil
}

func validateStalledBodyReadBenchmarkResponse(w *httptest.ResponseRecorder) error {
	return validateBodyReadBenchmarkResponse(w, []byte("upstream body"), proxy.XCacheMiss)
}

func (f *bodyReadBenchmarkFixture) runStalled(b *testing.B) (time.Duration, []time.Duration) {
	started := make(chan struct{}, bodyReadBenchmarkConcurrency)
	results := make(chan bodyReadBenchmarkResult, bodyReadBenchmarkConcurrency)
	for i := 0; i < bodyReadBenchmarkConcurrency; i++ {
		go func(key string) {
			control := bodyReadBenchmarkControl{started: started}
			w := httptest.NewRecorder()
			startedAt := time.Now()
			f.handler.ServeHTTP(w, bodyReadBenchmarkRequest(key, &control))
			result := bodyReadBenchmarkResult{latency: time.Since(startedAt)}
			result.err = validateStalledBodyReadBenchmarkResponse(w)
			results <- result
		}(f.keys[i])
	}
	for i := 0; i < bodyReadBenchmarkConcurrency; i++ {
		<-started
	}

	// The admission middleware must still be saturated before any stalled stream
	// reaches its idle deadline. A completed handler sends its result only after
	// the middleware's deferred semaphore release has run.
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, bodyReadBenchmarkRequest(f.keys[bodyReadBenchmarkConcurrency], nil))
	if w.Code != http.StatusServiceUnavailable {
		b.Fatalf("saturation probe status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
	capacityStartedAt := time.Now()

	first := <-results
	capacityRecovery := time.Since(capacityStartedAt)
	if first.err != nil {
		b.Fatal(first.err)
	}
	probe := httptest.NewRecorder()
	f.handler.ServeHTTP(probe, bodyReadBenchmarkRequest(f.keys[bodyReadBenchmarkConcurrency], nil))
	if err := validateStalledBodyReadBenchmarkResponse(probe); err != nil {
		b.Fatal(err)
	}

	latencies := make([]time.Duration, 0, bodyReadBenchmarkConcurrency)
	latencies = append(latencies, first.latency)
	for i := 1; i < bodyReadBenchmarkConcurrency; i++ {
		result := <-results
		if result.err != nil {
			b.Fatal(result.err)
		}
		latencies = append(latencies, result.latency)
	}
	return capacityRecovery, latencies
}

func (f *bodyReadBenchmarkFixture) runHealthy(b *testing.B) {
	started := make(chan struct{}, bodyReadBenchmarkConcurrency)
	release := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, bodyReadBenchmarkConcurrency)
	wg.Add(bodyReadBenchmarkConcurrency)
	for i := 0; i < bodyReadBenchmarkConcurrency; i++ {
		go func(key string) {
			defer wg.Done()
			control := bodyReadBenchmarkControl{started: started, release: release}
			w := httptest.NewRecorder()
			f.handler.ServeHTTP(w, bodyReadBenchmarkRequest(key, &control))
			if err := validateBodyReadBenchmarkResponse(w, f.body, proxy.XCacheHit); err != nil {
				errs <- err
			}
		}(f.keys[i])
	}
	for i := 0; i < bodyReadBenchmarkConcurrency; i++ {
		<-started
	}
	close(release)

	wg.Wait()
	close(errs)
	for err := range errs {
		b.Fatal(err)
	}
}

func BenchmarkCacheBodyReadAdmission(b *testing.B) {
	b.Run("StalledConcurrent16", func(b *testing.B) {
		fixture := newBodyReadBenchmarkFixture(b, bodyReadBenchmarkStalled)
		userCPU := newGoUserCPUCounter(b)
		var totalCapacityRecovery time.Duration
		requestLatencies := make([]time.Duration, 0, bodyReadBenchmarkConcurrency)
		cpuStart := userCPU.nanoseconds(b)
		b.ResetTimer()
		for b.Loop() {
			capacityRecovery, latencies := fixture.runStalled(b)
			totalCapacityRecovery += capacityRecovery
			requestLatencies = append(requestLatencies, latencies...)
		}
		b.StopTimer()
		cpuEnd := userCPU.nanoseconds(b)
		if cpuEnd < cpuStart {
			b.Fatalf("Go CPU moved backwards: start=%d end=%d", cpuStart, cpuEnd)
		}
		sort.Slice(requestLatencies, func(i, j int) bool {
			return requestLatencies[i] < requestLatencies[j]
		})
		p95Index := (len(requestLatencies)*95+99)/100 - 1
		b.ReportMetric(float64(requestLatencies[p95Index].Nanoseconds()), "request_p95_ns")
		b.ReportMetric(float64(totalCapacityRecovery.Nanoseconds())/float64(b.N), "capacity_recovery_ns/op")
		b.ReportMetric(float64(cpuEnd-cpuStart)/float64(b.N), "go_cpu_ns/op")
	})

	b.Run("HealthyConcurrent16", func(b *testing.B) {
		fixture := newBodyReadBenchmarkFixture(b, bodyReadBenchmarkHealthy)
		userCPU := newGoUserCPUCounter(b)
		cpuStart := userCPU.nanoseconds(b)
		b.ResetTimer()
		for b.Loop() {
			fixture.runHealthy(b)
		}
		b.StopTimer()
		cpuEnd := userCPU.nanoseconds(b)
		if cpuEnd < cpuStart {
			b.Fatalf("Go CPU moved backwards: start=%d end=%d", cpuStart, cpuEnd)
		}
		b.ReportMetric(float64(cpuEnd-cpuStart)/float64(b.N), "go_cpu_ns/op")
	})
}
