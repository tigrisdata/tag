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

// chunkedCacheClient streams bodies in fixed-size chunks. The memory cache
// hands a body to the writer in a single Write, which can never commit headers
// and then overflow; chunking reproduces a real cache stream whose excess
// arrives after the response is committed.
type chunkedCacheClient struct {
	cacheclient.CacheClient
	chunk int
}

func (c *chunkedCacheClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	data, err := c.CacheClient.Get(ctx, key)
	if err != nil {
		return err
	}
	for len(data) > 0 {
		n := min(c.chunk, len(data))
		if _, err := w.Write(data[:n]); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// newBodyLengthService seeds a whole-object entry whose metadata claims
// claimedLength bytes while the stored body is exactly storedBody.
func newBodyLengthService(t *testing.T, storedBody []byte, claimedLength int64) (*Service, *cache.Cache, *countingCacheClient, *cache.CachedObjectMeta) {
	t.Helper()
	return newBodyLengthServiceWithClient(t, cacheclient.NewMemoryCache(), storedBody, claimedLength)
}

func newBodyLengthServiceWithClient(t *testing.T, base cacheclient.CacheClient, storedBody []byte, claimedLength int64) (*Service, *cache.Cache, *countingCacheClient, *cache.CachedObjectMeta) {
	t.Helper()

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	client := &countingCacheClient{CacheClient: base}
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

// A small (buffered) entry whose stored body is longer than its Content-Length
// is rejected the same way as a short one: before any header, with the request
// falling through to upstream and the entry invalidated.
func TestHandleGetObject_LongCachedSmallBodyFallsThroughToUpstream(t *testing.T) {
	stored := bytes.Repeat([]byte("x"), 100)
	svc, store, client, meta := newBodyLengthService(t, stored, 60)
	beforeMismatches := lengthMismatchCount(t)

	upstreamBody := bytes.Repeat([]byte("y"), 60)
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
	if got := w.Header().Get(XCacheHeader); got != XCacheMiss {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheMiss)
	}
	if !bytes.Equal(w.Body.Bytes(), upstreamBody) {
		t.Fatalf("body = %d bytes, want the %d-byte upstream body", w.Body.Len(), len(upstreamBody))
	}
	if got := client.deletes.Load(); got != 1 {
		t.Fatalf("guarded deletes = %d, want 1", got)
	}
	if cur, found, err := store.GetMeta(context.Background(), meta.Bucket, meta.Key); err != nil {
		t.Fatalf("GetMeta: %v", err)
	} else if found && cur.ETag == meta.ETag {
		t.Fatal("long-body entry still cached under its stale ETag")
	}
	if got := lengthMismatchCount(t) - beforeMismatches; got != 1 {
		t.Fatalf("length_mismatch count delta = %v, want 1", got)
	}
}

// A large entry whose whole over-long body arrives in one chunk (as the memory
// cache delivers it) is refused before that chunk commits anything: the client
// never sees a cache byte, the request falls through to upstream, and the
// entry is invalidated.
func TestHandleGetObject_LargeLongCachedBodyFallsThroughToUpstream(t *testing.T) {
	stored := bytes.Repeat([]byte("x"), 70*1024)
	claimed := int64(68 * 1024)
	svc, store, client, meta := newBodyLengthService(t, stored, claimed)
	beforeMismatches := lengthMismatchCount(t)

	upstreamBody := bytes.Repeat([]byte("y"), int(claimed))
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
	if got := w.Header().Get(XCacheHeader); got != XCacheMiss {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheMiss)
	}
	if !bytes.Equal(w.Body.Bytes(), upstreamBody) {
		t.Fatalf("body = %d bytes, want the %d-byte upstream body and no cache bytes", w.Body.Len(), len(upstreamBody))
	}
	if got := client.deletes.Load(); got != 1 {
		t.Fatalf("guarded deletes = %d, want 1", got)
	}
	if cur, found, err := store.GetMeta(context.Background(), meta.Bucket, meta.Key); err != nil {
		t.Fatalf("GetMeta: %v", err)
	} else if found && cur.ETag == meta.ETag {
		t.Fatal("long-body entry still cached under its stale ETag")
	}
	if got := lengthMismatchCount(t) - beforeMismatches; got != 1 {
		t.Fatalf("length_mismatch count delta = %v, want 1", got)
	}
}

// A large entry streamed in chunks commits on its first chunk, so the excess
// is only seen mid-stream. The chunk that would overrun Content-Length must
// never reach the response writer (net/http would reject it with
// http.ErrContentLength and the entry would survive as a transient failure):
// the client gets fewer than Content-Length bytes, the request is counted as
// an error, the entry is invalidated, and the next request is a clean miss.
func TestHandleGetObject_LargeLongCachedBodyAbortsAfterCommit(t *testing.T) {
	const chunk = 16 * 1024
	stored := bytes.Repeat([]byte("x"), 70*1024)
	claimed := int64(68 * 1024)
	svc, store, client, meta := newBodyLengthServiceWithClient(t,
		&chunkedCacheClient{CacheClient: cacheclient.NewMemoryCache(), chunk: chunk}, stored, claimed)
	beforeErrors := requestCount(t, "error")
	beforeSuccesses := requestCount(t, "success")
	beforeMismatches := lengthMismatchCount(t)

	var upstreamCalls atomic.Int32
	svc.forwarder = &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return nil, errors.New("upstream unavailable")
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
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if got := w.Header().Get("Content-Length"); got != strconv.FormatInt(claimed, 10) {
		t.Fatalf("Content-Length = %q, want %d", got, claimed)
	}
	// Four whole chunks fit under 68 KiB; the fifth would overrun and is dropped
	// in full, so the response is left short of its declared length.
	wantDelivered := (int(claimed) / chunk) * chunk
	if w.Body.Len() != wantDelivered {
		t.Fatalf("body = %d bytes, want %d (every whole chunk under Content-Length, nothing past it)", w.Body.Len(), wantDelivered)
	}
	if int64(w.Body.Len()) > claimed {
		t.Fatalf("body = %d bytes, exceeds Content-Length %d", w.Body.Len(), claimed)
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
		t.Fatal("long-body entry still cached after a committed length mismatch")
	}

	// The corrupt entry is gone, so the next request cannot repeat the failure:
	// it goes to upstream.
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	_ = svc.HandleGetObject(w2, r2)
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstream calls after invalidation = %d, want 1", got)
	}
	if got := w2.Header().Get(XCacheHeader); got == XCacheHit {
		t.Fatalf("%s = %q on the request after invalidation, want a non-hit", XCacheHeader, got)
	}
}

// When the cache stream's chunks align with Content-Length, refusing only the
// excess chunk is not enough: the client would already hold every promised
// byte and see a clean, complete response from an entry detected as corrupt.
// The last promised byte is held back until the stream ends cleanly, so the
// client receives Content-Length-1 bytes (an aborted body), the entry is
// invalidated and the mismatch is counted.
func TestHandleGetObject_LargeLongCachedBodyAlignedChunksAbortsAfterCommit(t *testing.T) {
	const chunk = 32 * 1024
	stored := bytes.Repeat([]byte("x"), 4*chunk)
	claimed := int64(3 * chunk)
	svc, store, client, meta := newBodyLengthServiceWithClient(t,
		&chunkedCacheClient{CacheClient: cacheclient.NewMemoryCache(), chunk: chunk}, stored, claimed)
	beforeErrors := requestCount(t, "error")
	beforeSuccesses := requestCount(t, "success")
	beforeMismatches := lengthMismatchCount(t)

	var upstreamCalls atomic.Int32
	svc.forwarder = &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return nil, errors.New("upstream unavailable")
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
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if got := w.Header().Get("Content-Length"); got != strconv.FormatInt(claimed, 10) {
		t.Fatalf("Content-Length = %q, want %d", got, claimed)
	}
	// Three chunks land exactly on Content-Length and the fourth is refused;
	// the held last byte is never released, so the body stays one short.
	if int64(w.Body.Len()) != claimed-1 {
		t.Fatalf("body = %d bytes, want %d (Content-Length minus the held last byte)", w.Body.Len(), claimed-1)
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
		t.Fatal("long-body entry still cached after a committed length mismatch")
	}
}

// The boundary the holdback must not disturb: a body of exactly Content-Length
// delivered in aligned chunks is a complete hit. The held last byte is released
// when the stream ends cleanly, the entry stays, and nothing is counted as a
// mismatch.
func TestHandleGetObject_LargeExactCachedBodyAlignedChunksIsHit(t *testing.T) {
	const chunk = 32 * 1024
	stored := bytes.Repeat([]byte("x"), 3*chunk)
	stored[len(stored)-1] = 'z' // the held byte must come back verbatim
	svc, store, client, meta := newBodyLengthServiceWithClient(t,
		&chunkedCacheClient{CacheClient: cacheclient.NewMemoryCache(), chunk: chunk}, stored, int64(len(stored)))
	client.deletes.Store(0)
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
		t.Fatalf("upstream calls = %d, want 0", got)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if !bytes.Equal(w.Body.Bytes(), stored) {
		t.Fatalf("body = %d bytes, want the full %d stored bytes", w.Body.Len(), len(stored))
	}
	if got := requestCount(t, "success"); got != beforeSuccesses+1 {
		t.Fatalf("success request count = %v, want %v", got, beforeSuccesses+1)
	}
	if got := requestCount(t, "error"); got != beforeErrors {
		t.Fatalf("error request count = %v, want %v", got, beforeErrors)
	}
	if got := lengthMismatchCount(t) - beforeMismatches; got != 0 {
		t.Fatalf("length_mismatch count delta = %v, want 0", got)
	}
	if got := client.deletes.Load(); got != 0 {
		t.Fatalf("guarded deletes = %d, want 0", got)
	}
	if cur, found, err := store.GetMeta(context.Background(), meta.Bucket, meta.Key); err != nil {
		t.Fatalf("GetMeta: %v", err)
	} else if !found {
		t.Fatal("entry missing after a correct hit, want it to survive")
	} else if cur.ETag != meta.ETag {
		t.Fatalf("entry ETag = %q, want %q (the original entry must survive a correct hit)", cur.ETag, meta.ETag)
	}
}

// Writer-level contract of the holdback at its smallest: with Content-Length
// 1 the only byte is held, so nothing commits before release; release then
// writes exactly that byte; and any byte after the held one is a mismatch.
func TestLengthBoundedWriter_HoldsLastByteUntilRelease(t *testing.T) {
	meta := &cache.CachedObjectMeta{ContentLength: 1, StatusCode: http.StatusOK}
	rec := httptest.NewRecorder()
	cw := &lazyCommitWriter{w: rec, meta: meta}
	bw := &lengthBoundedWriter{w: cw, bucket: "bucket", key: "key", meta: meta}

	if n, err := bw.Write([]byte("a")); err != nil || n != 1 {
		t.Fatalf("Write(1 byte) = %d, %v; want 1, nil", n, err)
	}
	if cw.committed || rec.Body.Len() != 0 {
		t.Fatalf("committed=%v body=%d after the held byte, want nothing committed", cw.committed, rec.Body.Len())
	}
	if got := bw.seen(); got != 1 {
		t.Fatalf("seen() = %d, want 1", got)
	}
	if _, err := bw.Write([]byte("b")); !errors.Is(err, errCacheBodyLengthMismatch) {
		t.Fatalf("Write past the held byte: err = %v, want %v", err, errCacheBodyLengthMismatch)
	}
	if err := bw.release(); err != nil {
		t.Fatalf("release() error = %v", err)
	}
	if !cw.committed || rec.Body.String() != "a" {
		t.Fatalf("committed=%v body=%q after release, want committed and %q", cw.committed, rec.Body.String(), "a")
	}
	if bw.written != 1 || bw.seen() != 1 {
		t.Fatalf("written=%d seen=%d after release, want 1 and 1", bw.written, bw.seen())
	}
	// Releasing again is a no-op.
	if err := bw.release(); err != nil || rec.Body.Len() != 1 {
		t.Fatalf("second release: err=%v body=%d, want nil and 1", err, rec.Body.Len())
	}
}

// contentLengthEnforcingWriter mimics net/http's own check: a write that would
// carry the body past limit fails with http.ErrContentLength.
type contentLengthEnforcingWriter struct {
	http.ResponseWriter
	limit   int64
	written int64
}

func (w *contentLengthEnforcingWriter) Write(p []byte) (int, error) {
	if w.written+int64(len(p)) > w.limit {
		return 0, http.ErrContentLength
	}
	n, err := w.ResponseWriter.Write(p)
	w.written += int64(n)
	return n, err
}

// Belt and braces for the committed path: if a response writer enforces a
// length ahead of the proxy's own bound, its http.ErrContentLength is still
// classified as a corrupt entry and invalidated, not left to recur as a
// transient stream failure.
func TestServeFromCache_ContentLengthErrorAfterCommitInvalidates(t *testing.T) {
	const chunk = 16 * 1024
	stored := bytes.Repeat([]byte("x"), 70*1024)
	claimed := int64(68 * 1024)
	svc, store, client, meta := newBodyLengthServiceWithClient(t,
		&chunkedCacheClient{CacheClient: cacheclient.NewMemoryCache(), chunk: chunk}, stored, claimed)
	beforeErrors := requestCount(t, "error")
	beforeMismatches := lengthMismatchCount(t)

	// The writer trips at 60 KiB, on the fourth chunk (48 -> 64 KiB), which the
	// proxy's own 68 KiB bound would still have let through.
	rec := httptest.NewRecorder()
	w := &contentLengthEnforcingWriter{ResponseWriter: rec, limit: 60 * 1024}
	err := svc.serveFromCache(context.Background(), w, meta.Bucket, meta.Key, meta, time.Now())
	if err != nil {
		t.Fatalf("serveFromCache() error = %v, want nil after a committed failure", err)
	}
	if got := rec.Header().Get(XCacheHeader); got != XCacheHit {
		t.Fatalf("%s = %q, want %q (response must have committed)", XCacheHeader, got, XCacheHit)
	}
	if rec.Body.Len() != 3*chunk {
		t.Fatalf("body = %d bytes, want %d (chunks before the writer refused)", rec.Body.Len(), 3*chunk)
	}
	if got := requestCount(t, "error"); got != beforeErrors+1 {
		t.Fatalf("error request count = %v, want %v", got, beforeErrors+1)
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
		t.Fatal("entry still cached after http.ErrContentLength on a committed stream")
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
