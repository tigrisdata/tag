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
)

// teeMockForwarder is a mockForwarder that also implements bodyTeeingForwarder, so
// HandlePutObject takes the write-through tee path. teeFunc simulates the signing
// forwarder: it reads the request body into the tee (as decodeChunkedIfNeeded + TeeReader
// would) and returns the upstream PUT status + response headers (with the ETag). The
// authoritative HEAD the tee then issues is served from the embedded mock's conditionalResp.
type teeMockForwarder struct {
	*mockForwarder
	teeFunc func(ctx context.Context, w http.ResponseWriter, r *http.Request, tee io.Writer) (int, http.Header, error)
}

func (m *teeMockForwarder) ForwardTeeingBody(ctx context.Context, w http.ResponseWriter, r *http.Request, tee io.Writer) (int, http.Header, error) {
	return m.teeFunc(ctx, w, r, tee)
}

// teeUpstream returns a teeFunc that tees the whole body, responds 200 with the given
// ETag, and counts how many PUTs reached "upstream".
func teeUpstream(puts *atomic.Int32, etag string) func(context.Context, http.ResponseWriter, *http.Request, io.Writer) (int, http.Header, error) {
	return func(_ context.Context, w http.ResponseWriter, r *http.Request, tee io.Writer) (int, http.Header, error) {
		_, _ = io.Copy(tee, r.Body) // tee the object bytes into the caller's buffer
		puts.Add(1)
		h := http.Header{}
		h.Set("ETag", etag)
		w.WriteHeader(http.StatusOK)
		return http.StatusOK, h, nil
	}
}

// headResp builds the HEAD response the tee uses to source authoritative cache metadata.
func headResp(etag, contentType string, contentLength int64) *http.Response {
	h := http.Header{}
	h.Set("ETag", etag)
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	h.Set("Content-Length", strconv.FormatInt(contentLength, 10))
	h.Set("Last-Modified", "Wed, 21 Oct 2026 07:28:00 GMT")
	return &http.Response{StatusCode: http.StatusOK, Header: h, Body: http.NoBody, ContentLength: contentLength}
}

// A single authenticated PutObject within the size threshold is cached by teeing the body,
// with metadata sourced from an authoritative HEAD (not the read-back full-object GET). The
// cached ETag/Content-Type/Content-Length/Last-Modified come from the HEAD, and the body
// matches the write.
func TestHandlePutObject_WriteThroughTee_CachesFromHead(t *testing.T) {
	var puts, warmGets atomic.Int32
	body := "write-through-tee-body-contents"

	mock := &teeMockForwarder{
		mockForwarder: &mockForwarder{
			conditionalResp:  headResp(`"tee-etag"`, "text/plain", int64(len(body))),
			doFullObjectFunc: warmObjectResponder(&warmGets, "warm-body"), // fires only on fallback
		},
		teeFunc: teeUpstream(&puts, `"tee-etag"`),
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true
	svc.config.Cache.SizeThreshold = 1 << 20

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, body)); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("client status = %d, want 200", w.Code)
	}

	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("object was not cached via write-through tee")
	}
	if got := warmGets.Load(); got != 0 {
		t.Errorf("read-back warm GETs = %d, want 0 (tee sources meta from HEAD, no full read-back)", got)
	}
	if got := puts.Load(); got != 1 {
		t.Errorf("upstream PUTs = %d, want 1", got)
	}

	meta, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if !found {
		t.Fatal("no cached meta")
	}
	if meta.ETag != `"tee-etag"` {
		t.Errorf("cached ETag = %q, want %q", meta.ETag, `"tee-etag"`)
	}
	if meta.ContentType != "text/plain" {
		t.Errorf("cached Content-Type = %q, want text/plain (from HEAD)", meta.ContentType)
	}
	if meta.ContentLength != int64(len(body)) {
		t.Errorf("cached ContentLength = %d, want %d", meta.ContentLength, len(body))
	}
	if meta.LastModified == 0 {
		t.Error("cached LastModified = 0, want the HEAD's value")
	}
	var buf bytes.Buffer
	if err := c.GetBodyStream(context.Background(), wowBucket, wowKey, meta.ETag, &buf); err != nil {
		t.Fatalf("GetBodyStream: %v", err)
	}
	if buf.String() != body {
		t.Errorf("cached body = %q, want %q", buf.String(), body)
	}
}

// If the HEAD reports a different ETag than the PUT (a concurrent overwrite landed between
// our PUT and HEAD), the teed body is stale and must NOT be cached against that version; the
// tee falls back to a read-back warm.
func TestHandlePutObject_WriteThroughTee_ETagMismatchFallsBackToWarm(t *testing.T) {
	var puts, warmGets atomic.Int32
	body := "teed-body"

	mock := &teeMockForwarder{
		mockForwarder: &mockForwarder{
			conditionalResp:  headResp(`"newer-etag"`, "text/plain", int64(len(body))), // != tee ETag
			doFullObjectFunc: warmObjectResponder(&warmGets, "warm-body"),
		},
		teeFunc: teeUpstream(&puts, `"tee-etag"`),
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true
	svc.config.Cache.SizeThreshold = 1 << 20

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, body)); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("object was not cached (expected the warm fallback to cache it)")
	}
	if got := warmGets.Load(); got != 1 {
		t.Errorf("warm read-back GETs = %d, want 1 (ETag mismatch must fall back to warm)", got)
	}
	// Cached via the warm fallback (its ETag), not the stale teed version.
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if meta.ETag != `"warm-etag"` {
		t.Errorf("cached ETag = %q, want %q (warm fallback, not the teed body)", meta.ETag, `"warm-etag"`)
	}
}

// If the HEAD itself fails, the tee can't source authoritative metadata, so it falls back to
// a read-back warm rather than caching with guessed headers.
func TestHandlePutObject_WriteThroughTee_HeadFailureFallsBackToWarm(t *testing.T) {
	var puts, warmGets atomic.Int32
	mock := &teeMockForwarder{
		mockForwarder: &mockForwarder{
			conditionalErr:   errors.New("head failed"),
			doFullObjectFunc: warmObjectResponder(&warmGets, "warm-body"),
		},
		teeFunc: teeUpstream(&puts, `"tee-etag"`),
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true
	svc.config.Cache.SizeThreshold = 1 << 20

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, "body")); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("object was not cached (expected the warm fallback after HEAD failure)")
	}
	if got := warmGets.Load(); got != 1 {
		t.Errorf("warm read-back GETs = %d, want 1 (HEAD failure must fall back to warm)", got)
	}
}

// With warm-on-write disabled (the default), an eligible PUT must NOT be cached by the
// tee — the tee is an optimization of warm-on-write and must respect the flag.
func TestHandlePutObject_WriteThroughTee_DisabledWhenWarmOnWriteOff(t *testing.T) {
	var puts atomic.Int32
	mock := &teeMockForwarder{
		mockForwarder: &mockForwarder{
			forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
				w.WriteHeader(http.StatusOK)
				return nil
			},
		},
		teeFunc: teeUpstream(&puts, `"tee-etag"`),
	}
	svc, c := newTestService(mock, true) // WarmOnWrite defaults to false
	svc.config.Cache.SizeThreshold = 1 << 20

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, "body")); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}
	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("object was cached by the tee despite warm_on_write=false")
	}
	if got := puts.Load(); got != 0 {
		t.Errorf("tee upstream PUTs = %d, want 0 (tee must not run when warm_on_write is off)", got)
	}
}

// An object larger than the cache size threshold is not eligible for the tee; it falls
// back to warm-on-write (the read-back path).
func TestHandlePutObject_WriteThroughTee_FallsBackAboveSizeThreshold(t *testing.T) {
	var puts, warmGets atomic.Int32
	body := "this-body-exceeds-the-tiny-threshold"

	mock := &teeMockForwarder{
		mockForwarder: &mockForwarder{
			forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
				w.WriteHeader(http.StatusOK)
				return nil
			},
			doFullObjectFunc: warmObjectResponder(&warmGets, "warm-body"),
		},
		teeFunc: teeUpstream(&puts, `"tee-etag"`),
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true
	// Threshold sits between the PUT body (36 bytes) and the warm object (9 bytes): the tee
	// is skipped (body too big) but the read-back warm still caches the smaller fetched object.
	svc.config.Cache.SizeThreshold = 20

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, body)); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("object was not warmed after an above-threshold PUT")
	}
	if got := warmGets.Load(); got != 1 {
		t.Errorf("read-back warm GETs = %d, want 1 (above threshold must fall back to warm)", got)
	}
	if got := puts.Load(); got != 0 {
		t.Errorf("tee upstream PUTs = %d, want 0 (tee must not run above threshold)", got)
	}
}
