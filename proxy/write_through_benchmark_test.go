package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// writeThroughBenchmarkForwarder counts metadata HEADs and signals when a HEAD
// response closes. cacheTeedBodyFromHead closes that response only after it has
// finished the metadata/body cache write, so the signal is a precise detached-commit
// boundary without polling the populate limiter.
type writeThroughBenchmarkForwarder struct {
	*teeMockForwarder
	heads     atomic.Int64
	committed chan struct{}
}

type writeThroughBenchmarkCommitBody struct {
	io.ReadCloser
	committed chan<- struct{}
}

func (b *writeThroughBenchmarkCommitBody) Close() error {
	err := b.ReadCloser.Close()
	select {
	case b.committed <- struct{}{}:
	default: // Close is idempotent for the benchmark signal.
	}
	return err
}

func (f *writeThroughBenchmarkForwarder) DoConditionalHeadRequest(
	ctx context.Context,
	bucket, key, accessKey, secretKey, etag string,
	lastModified int64,
) (*http.Response, error) {
	f.heads.Add(1)
	resp, err := f.teeMockForwarder.DoConditionalHeadRequest(ctx, bucket, key, accessKey, secretKey, etag, lastModified)
	if err != nil || resp == nil {
		return resp, err
	}

	// The mock's HEAD metadata is immutable, but each detached commit needs its own body
	// close notification. A HEAD has no meaningful body, so use a fresh empty one.
	clone := *resp
	clone.Header = resp.Header.Clone()
	clone.Body = &writeThroughBenchmarkCommitBody{
		ReadCloser: io.NopCloser(strings.NewReader("")),
		committed:  f.committed,
	}
	return &clone, nil
}

// waitForWriteThroughCommit waits for the detached cache commit that follows a teed PUT.
func waitForWriteThroughCommit(b testing.TB, committed <-chan struct{}) {
	b.Helper()
	select {
	case <-committed:
	case <-time.After(time.Second):
		b.Fatal("timed out waiting for write-through cache commit")
	}
}

// benchmarkHandlePutObjectWriteThroughTee measures an eligible PUT through
// HandlePutObject. When includeDetachedCommit is true, the detached tee commit is included
// in the timed operation. Otherwise it is synchronized between requests but excluded because
// it runs after the client receives its PUT response. The mock HEAD deliberately has no
// Cache-Control directive, so its origin metadata is cacheable.
func benchmarkHandlePutObjectWriteThroughTee(b *testing.B, putCacheControl string, includeDetachedCommit bool) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	const (
		bucket   = "benchmark-bucket"
		key      = "write-through-object"
		bodySize = 64 << 10
		etag     = `"tee-etag"`
	)
	body := strings.Repeat("t", bodySize)

	var teedPuts, directPuts atomic.Int32
	head := headResp(etag, "application/octet-stream", int64(len(body)))
	forwarder := &writeThroughBenchmarkForwarder{
		committed: make(chan struct{}, 1),
		teeMockForwarder: &teeMockForwarder{
			mockForwarder: &mockForwarder{
				conditionalResp: head,
				forwardFunc: func(_ context.Context, w http.ResponseWriter, r *http.Request) error {
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						return err
					}
					directPuts.Add(1)
					w.WriteHeader(http.StatusOK)
					return nil
				},
			},
			teeFunc: teeUpstream(&teedPuts, etag),
		},
	}
	svc, c := newTestService(forwarder, true)
	svc.config.Cache.WarmOnWrite = true
	svc.config.Cache.SizeThreshold = 1 << 20
	svc.config.Cache.BlockSize = 1 << 20
	// waitForWriteThroughCommit serializes commits, so no benchmark iteration can overlap
	// another. Disabling the limiter avoids turning its observed state into a polling signal.
	svc.cacheSemaphore = nil

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		teedBefore := teedPuts.Load()
		r := authedPut(bucket, key, body)
		if putCacheControl != "" {
			r.Header.Set("Cache-Control", putCacheControl)
		}
		w := httptest.NewRecorder()
		if err := svc.HandlePutObject(w, r); err != nil {
			b.Fatalf("HandlePutObject: %v", err)
		}
		if w.Code != http.StatusOK {
			b.Fatalf("client status = %d, want %d", w.Code, http.StatusOK)
		}
		if teedPuts.Load() != teedBefore {
			if !includeDetachedCommit {
				b.StopTimer()
			}
			waitForWriteThroughCommit(b, forwarder.committed)
			if !includeDetachedCommit {
				b.StartTimer()
			}
		} else if putCacheControl != "no-store" {
			b.Fatal("cacheable PUT did not use the write-through tee")
		}
	}
	b.StopTimer()

	if got := teedPuts.Load() + directPuts.Load(); got != int32(b.N) {
		b.Fatalf("upstream PUTs = %d, want %d", got, b.N)
	}
	heads := forwarder.heads.Load()
	wantCached := true
	switch putCacheControl {
	case "":
		if directPuts.Load() != 0 {
			b.Fatalf("plain-forward PUTs = %d, want 0 for cacheable PUTs", directPuts.Load())
		}
		if heads != int64(b.N) {
			b.Fatalf("metadata HEADs = %d, want %d for cacheable PUTs", heads, b.N)
		}
	case "no-store":
		switch heads {
		case 0:
			if directPuts.Load() != int32(b.N) {
				b.Fatalf("plain-forward PUTs = %d, want %d after policy rejection", directPuts.Load(), b.N)
			}
			wantCached = false
		case int64(b.N):
			if teedPuts.Load() != int32(b.N) {
				b.Fatalf("tee upstream PUTs = %d, want %d before policy rejection", teedPuts.Load(), b.N)
			}
		default:
			b.Fatalf("metadata HEADs = %d, want 0 or %d", heads, b.N)
		}
	default:
		b.Fatalf("unsupported PUT Cache-Control %q", putCacheControl)
	}
	_, found, err := c.GetMeta(context.Background(), bucket, key)
	if err != nil {
		b.Fatalf("GetMeta: %v", err)
	}
	if found != wantCached {
		b.Fatalf("cached = %t, want %t", found, wantCached)
	}
	b.ReportMetric(float64(heads)/float64(b.N), "upstream_HEADs/op")
}

// BenchmarkHandlePutObjectWriteThroughTeeNoStoreCacheableOrigin measures a completed
// eligible 64 KiB PUT declaring Cache-Control: no-store when the origin's HEAD metadata is
// cacheable. The baseline follows that HEAD and publishes a cache entry; the candidate
// conservatively honors the request policy and plain-forwards without cache work.
func BenchmarkHandlePutObjectWriteThroughTeeNoStoreCacheableOrigin(b *testing.B) {
	benchmarkHandlePutObjectWriteThroughTee(b, "no-store", true)
}

// BenchmarkHandlePutObjectWriteThroughTeeCacheable measures the ordinary cacheable
// PUT's client-visible path. It keeps the authoritative metadata HEAD and waits for each
// detached tee commit outside the timed span to verify cache publication before the next PUT.
func BenchmarkHandlePutObjectWriteThroughTeeCacheable(b *testing.B) {
	benchmarkHandlePutObjectWriteThroughTee(b, "", false)
}
