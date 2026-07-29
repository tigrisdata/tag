package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// teeMockForwarder is a mockForwarder that also implements bodyTeeingForwarder, so
// HandlePutObject takes the write-through tee path. teeFunc simulates the signing
// forwarder: it reads the request body into the tee (as decodeChunkedIfNeeded + TeeReader
// would) and returns the upstream PUT status + response headers (with the ETag).
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

// A single authenticated PutObject within the size threshold is cached by teeing the body
// on the write path — no read-back warm GET — and the cached body/ETag match the write.
func TestHandlePutObject_WriteThroughTee_CachesWithoutReadBack(t *testing.T) {
	var puts, warmGets atomic.Int32
	body := "write-through-tee-body-contents"

	mock := &teeMockForwarder{
		mockForwarder: &mockForwarder{
			// If this fires, the tee fell back to a read-back warm — which we assert against.
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
	if w.Code != http.StatusOK {
		t.Fatalf("client status = %d, want 200", w.Code)
	}

	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("object was not cached via write-through tee")
	}
	if got := warmGets.Load(); got != 0 {
		t.Errorf("read-back warm GETs = %d, want 0 (tee must avoid the read-back)", got)
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
	if meta.ContentLength != int64(len(body)) {
		t.Errorf("cached ContentLength = %d, want %d", meta.ContentLength, len(body))
	}
	var buf bytes.Buffer
	if err := c.GetBodyStream(context.Background(), wowBucket, wowKey, meta.ETag, &buf); err != nil {
		t.Fatalf("GetBodyStream: %v", err)
	}
	if buf.String() != body {
		t.Errorf("cached body = %q, want %q", buf.String(), body)
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
