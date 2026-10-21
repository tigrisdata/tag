package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
)

// countingCacheClient records every read and delete the proxy issues so a test
// can pin the exact number of cache round trips a whole-object hit costs.
type countingCacheClient struct {
	cacheclient.CacheClient
	reads   atomic.Int64
	deletes atomic.Int64
}

func (c *countingCacheClient) Get(ctx context.Context, key string) ([]byte, error) {
	c.reads.Add(1)
	return c.CacheClient.Get(ctx, key)
}

func (c *countingCacheClient) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	c.reads.Add(1)
	return c.CacheClient.GetWithVersion(ctx, key)
}

func (c *countingCacheClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	c.reads.Add(1)
	return c.CacheClient.GetStream(ctx, key, w)
}

func (c *countingCacheClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	c.reads.Add(1)
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

func (c *countingCacheClient) Delete(ctx context.Context, key string) error {
	c.deletes.Add(1)
	return c.CacheClient.Delete(ctx, key)
}

func (c *countingCacheClient) DeleteIfVersion(ctx context.Context, key string, expected uint64) error {
	c.deletes.Add(1)
	return c.CacheClient.DeleteIfVersion(ctx, key, expected)
}

// newBodyLengthService seeds a whole-object entry whose metadata claims
// claimedLength bytes while the stored body is exactly storedBody.
func newBodyLengthService(t *testing.T, storedBody []byte, claimedLength int64) (*Service, *cache.Cache, *countingCacheClient, *cache.CachedObjectMeta) {
	t.Helper()

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	client := &countingCacheClient{CacheClient: cacheclient.NewMemoryCache()}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	meta := &cache.CachedObjectMeta{
		Bucket:        "bucket",
		Key:           "key",
		ETag:          `"stale"`,
		ContentType:   "text/plain",
		ContentLength: claimedLength,
		StatusCode:    http.StatusOK,
	}
	if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, storedBody, 0); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	return NewService(&mockForwarder{}, store, cfg), store, client, meta
}

func lengthMismatchCount(t *testing.T) float64 {
	t.Helper()

	var metric dto.Metric
	if err := metrics.CacheOperations.WithLabelValues("get", "length_mismatch").Write(&metric); err != nil {
		t.Fatalf("read length mismatch count: %v", err)
	}
	return metric.GetCounter().GetValue()
}

// A small (buffered) entry whose stored body is shorter than its Content-Length
// must not be served as a hit: nothing is committed, the request falls through
// to upstream, and the corrupt entry is invalidated.
func TestHandleGetObject_ShortCachedBodyFallsThroughToUpstream(t *testing.T) {
	stored := bytes.Repeat([]byte("x"), 60)
	svc, store, client, meta := newBodyLengthService(t, stored, 100)
	beforeMismatches := lengthMismatchCount(t)

	upstreamBody := bytes.Repeat([]byte("y"), 100)
	var upstreamCalls atomic.Int32
	svc.forwarder = &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header: http.Header{
					"Content-Length": []string{strconv.Itoa(len(upstreamBody))},
					"Content-Type":   []string{"text/plain"},
					"ETag":           []string{`"fresh"`},
				},
				Body: io.NopCloser(bytes.NewReader(upstreamBody)),
			}, nil
		},
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheMiss {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheMiss)
	}
	if !bytes.Equal(w.Body.Bytes(), upstreamBody) {
		t.Fatalf("body = %d bytes of %q, want the %d-byte upstream body", w.Body.Len(), w.Body.Bytes()[:1], len(upstreamBody))
	}
	if got := client.deletes.Load(); got != 1 {
		t.Fatalf("guarded deletes = %d, want 1", got)
	}
	// The miss path may re-warm the key under the upstream ETag; the stale entry
	// itself must be gone either way.
	if cur, found, err := store.GetMeta(context.Background(), meta.Bucket, meta.Key); err != nil {
		t.Fatalf("GetMeta: %v", err)
	} else if found && cur.ETag == meta.ETag {
		t.Fatal("short-body entry still cached under its stale ETag")
	}
	if got := lengthMismatchCount(t) - beforeMismatches; got != 1 {
		t.Fatalf("length_mismatch count delta = %v, want 1", got)
	}
}

// A large (streamed) entry commits headers on its first chunk, so a short body
// is only provable once the stream ends. The response must then be left
// incomplete (declared Content-Length unmet, no upstream append), counted as an
// error, and the entry invalidated so the next request is a clean miss.
func TestHandleGetObject_LargeShortCachedBodyAbortsAfterCommit(t *testing.T) {
	stored := bytes.Repeat([]byte("x"), smallObjectThreshold+1024)
	claimed := int64(len(stored)) + 4096
	svc, store, client, meta := newBodyLengthService(t, stored, claimed)
	beforeErrors := requestCount(t, "error")
	beforeSuccesses := requestCount(t, "success")
	beforeMismatches := lengthMismatchCount(t)

	var upstreamCalls atomic.Int32
	svc.forwarder = &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return nil, errors.New("unexpected upstream request")
		},
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Fatalf("upstream calls = %d, want 0 after committed cache response", got)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get("Content-Length"); got != strconv.FormatInt(claimed, 10) {
		t.Fatalf("Content-Length = %q, want %d", got, claimed)
	}
	if !bytes.Equal(w.Body.Bytes(), stored) {
		t.Fatalf("body = %d bytes, want the %d stored bytes and nothing fabricated", w.Body.Len(), len(stored))
	}
	if got := requestCount(t, "error"); got != beforeErrors+1 {
		t.Fatalf("error request count = %v, want %v", got, beforeErrors+1)
	}
	if got := requestCount(t, "success"); got != beforeSuccesses {
		t.Fatalf("success request count = %v, want %v", got, beforeSuccesses)
	}
	if got := lengthMismatchCount(t) - beforeMismatches; got != 1 {
		t.Fatalf("length_mismatch count delta = %v, want 1", got)
	}
	if got := client.deletes.Load(); got != 1 {
		t.Fatalf("guarded deletes = %d, want 1", got)
	}
	if _, found, err := store.GetMeta(context.Background(), meta.Bucket, meta.Key); err != nil {
		t.Fatalf("GetMeta: %v", err)
	} else if found {
		t.Fatal("short-body entry still cached after a committed length mismatch")
	}
}

// A correct entry is still a plain hit: one meta read, one body read, nothing
// else — the length check costs no extra cache round trip.
func TestHandleGetObject_CorrectCachedBodyIsHitWithoutExtraCacheCalls(t *testing.T) {
	stored := bytes.Repeat([]byte("x"), 100)
	svc, _, client, _ := newBodyLengthService(t, stored, int64(len(stored)))
	client.reads.Store(0)
	client.deletes.Store(0)
	beforeMismatches := lengthMismatchCount(t)

	var upstreamCalls atomic.Int32
	svc.forwarder = &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return nil, errors.New("unexpected upstream request")
		},
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Fatalf("upstream calls = %d, want 0", got)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if !bytes.Equal(w.Body.Bytes(), stored) {
		t.Fatalf("body = %d bytes, want %d", w.Body.Len(), len(stored))
	}
	if got := client.reads.Load(); got != 2 {
		t.Fatalf("cache reads = %d, want 2 (meta + body)", got)
	}
	if got := client.deletes.Load(); got != 0 {
		t.Fatalf("cache deletes = %d, want 0", got)
	}
	if got := lengthMismatchCount(t) - beforeMismatches; got != 0 {
		t.Fatalf("length_mismatch count delta = %v, want 0", got)
	}
}

// Direct contract of the small path: the mismatch is reported before any header
// is written, and bodyGone classifies it as an unusable body so every caller's
// existing guarded invalidation applies.
func TestServeFromCache_ShortBodyFailsBeforeHeaders(t *testing.T) {
	stored := bytes.Repeat([]byte("x"), 60)
	svc, _, _, meta := newBodyLengthService(t, stored, 100)

	w := httptest.NewRecorder()
	err := svc.serveFromCache(context.Background(), w, meta.Bucket, meta.Key, meta, time.Now())
	if !errors.Is(err, errCacheBodyLengthMismatch) {
		t.Fatalf("serveFromCache() error = %v, want %v", err, errCacheBodyLengthMismatch)
	}
	if !bodyGone(err) {
		t.Fatalf("bodyGone(%v) = false, want true", err)
	}
	if got := w.Header().Get(XCacheHeader); got != "" {
		t.Fatalf("%s = %q, want no cache status before commit", XCacheHeader, got)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("body length = %d, want 0", w.Body.Len())
	}
}
