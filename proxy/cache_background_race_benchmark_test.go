package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// benchmarkRaceBody keeps the first background read in flight while the PUT
// invalidates its cache write. It is proof support for the production race, not
// a second cache implementation.
type benchmarkRaceBody struct {
	io.Reader
	release <-chan struct{}
	started chan<- struct{}
	once    sync.Once
}

func (b *benchmarkRaceBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return b.Reader.Read(p)
}

func (b *benchmarkRaceBody) Close() error { return nil }

// waitBenchmarkBackgroundFetchGone waits for the entire serialized bg worker,
// including a pending replacement on Candidate, to finish before the post-write
// read in the measured write-to-read lifecycle.
func waitBenchmarkTriggerWindow(b *testing.B) {
	b.Helper()
	deadline := time.After(5 * time.Millisecond)
	for {
		select {
		case <-deadline:
			return
		default:
			runtime.Gosched()
		}
	}
}

func waitBenchmarkBackgroundFetchGone(b *testing.B, svc *Service, bucket, key string) {
	b.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, loaded := svc.activeBackgroundFetches.Load("bg:" + bucket + "/" + key); !loaded {
			return
		}
		if time.Now().After(deadline) {
			b.Fatal("background fetch did not finish")
		}
		runtime.Gosched()
	}
}

// BenchmarkPostWriteCacheWarmAfterStaleBackgroundFetch measures the write-to-read
// lifecycle after a same-key read-miss background fetch is already in flight. The
// timed operation drives a successful small authenticated PUT through the tee
// fallback, releases the old fetch through the tombstone race, and then issues
// the post-write GET. The origin GET counters are the contract signal: Candidate
// shifts the replacement fetch before the read, so the post-write read no longer
// reaches origin while total origin GET work remains bounded to the same two
// requests.
func BenchmarkPostWriteCacheWarmAfterStaleBackgroundFetch(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	const (
		bucket  = "benchmark-race-bucket"
		key     = "benchmark-race-key"
		oldBody = "old-body"
		newBody = "new-body"
	)

	var postWriteOriginGets, totalOriginGets atomic.Int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		releaseOld := make(chan struct{})
		oldBodyStarted := make(chan struct{})
		headDone := make(chan struct{})
		fullCalls := atomic.Int32{}
		puts := atomic.Int32{}

		forwarder := &teeMockForwarder{
			mockForwarder: &mockForwarder{
				conditionalResp: headResp(`"head-etag"`, "text/plain", int64(len(newBody))),
				doRequestFunc: func(_ context.Context, _ *http.Request, _, _ string) (*http.Response, error) {
					postWriteOriginGets.Add(1)
					totalOriginGets.Add(1)
					return cacheableGetResponse(newBody, `"new-etag"`), nil
				},
				doFullObjectFunc: func(_ context.Context, _, _, _, _ string) (*http.Response, error) {
					call := fullCalls.Add(1)
					totalOriginGets.Add(1)
					if call == 1 {
						resp := cacheableGetResponse(oldBody, `"old-etag"`)
						resp.Body = &benchmarkRaceBody{
							Reader:  resp.Body,
							release: releaseOld,
							started: oldBodyStarted,
						}
						return resp, nil
					}
					if call == 2 {
						return cacheableGetResponse(newBody, `"new-etag"`), nil
					}
					return nil, errors.New("unexpected background fetch")
				},
			},
			teeFunc:  teeUpstream(&puts, `"put-etag"`),
			headHook: func() { close(headDone) },
		}
		cfg := newTestConfigForBenchmark()
		cacheStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
		svc := NewService(forwarder, cacheStore, cfg)
		svc.triggerBackgroundCacheFetch(bucket, key, "old-access", "old-secret", false, priorityReadMiss)
		select {
		case <-oldBodyStarted:
		case <-time.After(time.Second):
			b.Fatal("old background fetch did not reach the body")
		}

		b.StartTimer()
		putResponse := httptest.NewRecorder()
		if err := svc.HandlePutObject(putResponse, authedPut(bucket, key, newBody)); err != nil {
			b.Fatal(err)
		}
		select {
		case <-headDone:
		case <-time.After(time.Second):
			b.Fatal("tee fallback did not issue HEAD")
		}
		// The fallback trigger is detached from HandlePutObject. Poll through a
		// bounded channel window so it observes the active marker on both arms.
		waitBenchmarkTriggerWindow(b)
		close(releaseOld)
		waitBenchmarkBackgroundFetchGone(b, svc, bucket, key)

		readReq := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
		readReq.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request, Signature=deadbeef")
		readResponse := httptest.NewRecorder()
		if err := svc.HandleGetObject(readResponse, readReq); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if got := readResponse.Body.String(); got != newBody {
			b.Fatalf("post-write body = %q, want %q", got, newBody)
		}
	}

	b.ReportMetric(float64(postWriteOriginGets.Load())/float64(b.N), "post_write_origin_gets/op")
	b.ReportMetric(float64(totalOriginGets.Load())/float64(b.N), "origin_gets/op")
}

// newTestConfigForBenchmark keeps the benchmark's setup independent from the
// production default block representation while reusing the repository's test
// service construction.
func newTestConfigForBenchmark() *config.Config {
	cfg := config.NewDefault()
	cfg.Cache.WarmOnWrite = true
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SizeThreshold = 1 << 20
	cfg.Cache.BlockSize = 1 << 20
	return cfg
}
