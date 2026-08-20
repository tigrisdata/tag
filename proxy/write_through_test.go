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
	teeFunc func(ctx context.Context, w http.ResponseWriter, r *http.Request, tee io.Writer) (int, http.Header, string, string, error)
	// headHook, if set, runs when the tee issues its authoritative HEAD — used to simulate a
	// competing write landing during the HEAD window.
	headHook func()
}

func (m *teeMockForwarder) ForwardTeeingBody(ctx context.Context, w http.ResponseWriter, r *http.Request, tee io.Writer) (int, http.Header, string, string, error) {
	return m.teeFunc(ctx, w, r, tee)
}

func (m *teeMockForwarder) DoConditionalHeadRequest(ctx context.Context, bucket, key, accessKey, secretKey, etag string, lastModified int64) (*http.Response, error) {
	if m.headHook != nil {
		m.headHook()
	}
	return m.mockForwarder.DoConditionalHeadRequest(ctx, bucket, key, accessKey, secretKey, etag, lastModified)
}

// teeUpstream returns a teeFunc that tees the whole body, responds 200 with the given
// ETag and validated credentials, and counts how many PUTs reached "upstream".
func teeUpstream(puts *atomic.Int32, etag string) func(context.Context, http.ResponseWriter, *http.Request, io.Writer) (int, http.Header, string, string, error) {
	return func(_ context.Context, w http.ResponseWriter, r *http.Request, tee io.Writer) (int, http.Header, string, string, error) {
		_, _ = io.Copy(tee, r.Body) // tee the object bytes into the caller's buffer
		puts.Add(1)
		h := http.Header{}
		h.Set("ETag", etag)
		w.WriteHeader(http.StatusOK)
		return http.StatusOK, h, "access", "secret", nil
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
	svc.config.Cache.BlockSize = 1 << 20

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

// A negative max_populate_memory_bytes disables the byte budget (svc.populateBudget == nil).
// The tee must still run in that supported config — acquireCacheSlot treats a nil budget as
// unlimited (count-semaphore only), just like warm-on-write — instead of silently regressing
// every eligible PUT to a read-back warm.
func TestHandlePutObject_WriteThroughTee_CachesWithNilPopulateBudget(t *testing.T) {
	var puts, warmGets atomic.Int32
	body := "tee-body-without-populate-budget"

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
	svc.config.Cache.BlockSize = 1 << 20
	svc.populateBudget = nil // byte budget explicitly disabled (max_populate_memory_bytes < 0)

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, body)); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("client status = %d, want 200", w.Code)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("object was not cached via write-through tee with a nil populate budget")
	}
	if got := warmGets.Load(); got != 0 {
		t.Errorf("read-back warm GETs = %d, want 0 (tee must run without a byte budget)", got)
	}
	if got := puts.Load(); got != 1 {
		t.Errorf("upstream PUTs = %d, want 1", got)
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
	svc.config.Cache.BlockSize = 1 << 20

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
	svc.config.Cache.BlockSize = 1 << 20

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

// When the authoritative HEAD shows the object is not cacheable (Cache-Control: no-store),
// the tee must NOT fall back to a read-back warm — a warm would only re-download the full
// body to reject it again, defeating the bandwidth win.
func TestHandlePutObject_WriteThroughTee_NotCacheableSkipsWarm(t *testing.T) {
	var puts, warmGets, heads atomic.Int32
	body := "no-store-body"

	head := headResp(`"tee-etag"`, "text/plain", int64(len(body)))
	head.Header.Set("Cache-Control", "no-store")

	mock := &teeMockForwarder{
		mockForwarder: &mockForwarder{
			conditionalResp:  head,
			doFullObjectFunc: warmObjectResponder(&warmGets, "warm-body"), // must NOT fire
		},
		teeFunc:  teeUpstream(&puts, `"tee-etag"`),
		headHook: func() { heads.Add(1) },
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true
	svc.config.Cache.SizeThreshold = 1 << 20
	svc.config.Cache.BlockSize = 1 << 20

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, body)); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}
	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("no-store object was cached")
	}
	if got := warmGets.Load(); got != 0 {
		t.Errorf("read-back warm GETs = %d, want 0 (HEAD said not cacheable — warm would only re-download and re-reject)", got)
	}
	if got := puts.Load(); got != 1 {
		t.Errorf("tee upstream PUTs = %d, want 1", got)
	}
	if got := heads.Load(); got != 1 {
		t.Errorf("metadata HEADs = %d, want 1 (no policy was available on the PUT)", got)
	}
}

// A Cache-Control policy supplied with the PUT is already enough to reject shared caching.
// The request must take the plain-forward path before tee admission: no metadata HEAD, tee
// buffer, cache entry, or read-back warm is needed for no-store/private.
func TestHandlePutObject_WriteThroughTee_RequestCacheControlPlainForwards(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cacheControls []string
	}{
		{name: "no-store", cacheControls: []string{"No-Store"}},
		{name: "private", cacheControls: []string{"max-age=60, PRIVATE"}},
		{name: "private-with-field-list", cacheControls: []string{`private="Set-Cookie"`}},
		{name: "repeated-no-store", cacheControls: []string{"max-age=60", "no-store"}},
		{name: "repeated-private", cacheControls: []string{"max-age=60", "private"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var teedPuts, directPuts, warmGets, heads atomic.Int32
			var forwardedBody bytes.Buffer
			body := "request-cache-control-body"

			mock := &teeMockForwarder{
				mockForwarder: &mockForwarder{
					// A cacheable response makes an accidental HEAD observable as both a
					// second request and a wrongly published entry.
					conditionalResp:  headResp(`"tee-etag"`, "text/plain", int64(len(body))),
					doFullObjectFunc: warmObjectResponder(&warmGets, "warm-body"),
					forwardFunc: func(_ context.Context, w http.ResponseWriter, r *http.Request) error {
						if _, err := io.Copy(&forwardedBody, r.Body); err != nil {
							return err
						}
						directPuts.Add(1)
						w.WriteHeader(http.StatusOK)
						return nil
					},
				},
				teeFunc:  teeUpstream(&teedPuts, `"tee-etag"`),
				headHook: func() { heads.Add(1) },
			}
			svc, c := newTestService(mock, true)
			svc.config.Cache.WarmOnWrite = true
			svc.config.Cache.SizeThreshold = 1 << 20
			svc.config.Cache.BlockSize = 1 << 20
			svc.cacheSemaphore = make(chan struct{}, 1)

			r := authedPut(wowBucket, wowKey, body)
			for _, cacheControl := range tc.cacheControls {
				r.Header.Add("Cache-Control", cacheControl)
			}
			w := httptest.NewRecorder()
			if err := svc.HandlePutObject(w, r); err != nil {
				t.Fatalf("HandlePutObject: %v", err)
			}
			if w.Code != http.StatusOK {
				t.Fatalf("client status = %d, want 200", w.Code)
			}
			if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
				t.Error("request-declared non-cacheable object was cached")
			}
			if got := teedPuts.Load(); got != 0 {
				t.Errorf("tee upstream PUTs = %d, want 0", got)
			}
			if got := directPuts.Load(); got != 1 {
				t.Errorf("plain-forward upstream PUTs = %d, want 1", got)
			}
			if got := forwardedBody.String(); got != body {
				t.Errorf("plain-forward body = %q, want %q", got, body)
			}
			if got := heads.Load(); got != 0 {
				t.Errorf("metadata HEADs = %d, want 0", got)
			}
			if got := warmGets.Load(); got != 0 {
				t.Errorf("read-back warm GETs = %d, want 0", got)
			}
			if !svc.acquireCacheSlot(context.Background(), int64(len(body)), priorityReadMiss) {
				t.Fatal("request policy path held a cache slot")
			}
			svc.releaseCacheSlot(int64(len(body)))
		})
	}
}

// Only exact no-store/private directives reject shared storage. no-cache and extension
// directives whose names or values merely contain those strings retain the normal tee path
// and its authoritative metadata HEAD.
func TestHandlePutObject_WriteThroughTee_RequestCacheControlExtensionsUseHead(t *testing.T) {
	var teedPuts, directPuts, warmGets, heads atomic.Int32
	body := "request-no-cache-body"

	mock := &teeMockForwarder{
		mockForwarder: &mockForwarder{
			conditionalResp:  headResp(`"tee-etag"`, "text/plain", int64(len(body))),
			doFullObjectFunc: warmObjectResponder(&warmGets, "warm-body"),
			forwardFunc: func(_ context.Context, w http.ResponseWriter, r *http.Request) error {
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					return err
				}
				directPuts.Add(1)
				w.WriteHeader(http.StatusOK)
				return nil
			},
		},
		teeFunc:  teeUpstream(&teedPuts, `"tee-etag"`),
		headHook: func() { heads.Add(1) },
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true
	svc.config.Cache.SizeThreshold = 1 << 20
	svc.config.Cache.BlockSize = 1 << 20

	r := authedPut(wowBucket, wowKey, body)
	r.Header.Set("Cache-Control", `no-cache, no-storex, privatex, foo="private, no-store"`)
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, r); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("cacheable request-policy PUT was not cached through the tee")
	}
	if got := teedPuts.Load(); got != 1 {
		t.Errorf("tee upstream PUTs = %d, want 1", got)
	}
	if got := directPuts.Load(); got != 0 {
		t.Errorf("plain-forward upstream PUTs = %d, want 0", got)
	}
	if got := heads.Load(); got != 1 {
		t.Errorf("metadata HEADs = %d, want 1", got)
	}
	if got := warmGets.Load(); got != 0 {
		t.Errorf("read-back warm GETs = %d, want 0", got)
	}
}

// A competing overwrite/DELETE that lands during the HEAD window must not be resurrected:
// writeStartTime is stamped BEFORE the HEAD, so a tombstone written during the HEAD is newer
// and the tombstone-aware write skips the stale teed body. Because it was skipped (not an
// error and not a real write), the tee falls back to a warm that caches the CURRENT version.
func TestHandlePutObject_WriteThroughTee_TombstoneDuringHeadFallsBackToWarm(t *testing.T) {
	var puts, warmGets atomic.Int32
	var svc *Service
	body := "stale-teed-body"

	mock := &teeMockForwarder{
		mockForwarder: &mockForwarder{
			conditionalResp: headResp(`"tee-etag"`, "text/plain", int64(len(body))),
			// The fallback warm fetches the current version (ETag "warm-etag").
			doFullObjectFunc: warmObjectResponder(&warmGets, "current-body"),
		},
		teeFunc: teeUpstream(&puts, `"tee-etag"`),
		// Simulate the competing write's invalidation landing in the HEAD window.
		headHook: func() { svc.invalidateObject(context.Background(), wowBucket, wowKey) },
	}
	s, c := newTestService(mock, true)
	svc = s
	svc.config.Cache.WarmOnWrite = true
	svc.config.Cache.SizeThreshold = 1 << 20
	svc.config.Cache.BlockSize = 1 << 20

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, body)); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}
	// The stale teed body is tombstone-skipped; the fallback warm caches the current version.
	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("expected the warm fallback to cache the current version after the tombstone skip")
	}
	if got := warmGets.Load(); got != 1 {
		t.Errorf("warm fallback GETs = %d, want 1 (a tombstone-skip must fall back to warm, not count as a write-through)", got)
	}
	meta, _, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if meta.ETag != `"warm-etag"` {
		t.Errorf("cached ETag = %q, want the warm/current version, not the stale teed body %q", meta.ETag, `"tee-etag"`)
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
	svc.config.Cache.BlockSize = 1 << 20

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

// An object at or above BlockSize is not eligible for the in-memory tee; the write path falls
// back to the streaming warm-on-write read-back.
func TestHandlePutObject_WriteThroughTee_FallsBackAboveBlockSize(t *testing.T) {
	var puts, warmGets atomic.Int32
	body := "this-body-exceeds-the-tiny-write-through-cap"

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
	// BlockSize sits at or below the PUT body: the tee is skipped (too big to buffer),
	// but the object is still cacheable (SizeThreshold large) so the warm read-back caches it.
	svc.config.Cache.SizeThreshold = 1 << 20
	svc.config.Cache.BlockSize = 8

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, body)); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("object was not warmed after an above-cap PUT")
	}
	if got := warmGets.Load(); got != 1 {
		t.Errorf("read-back warm GETs = %d, want 1 (above write-through cap must fall back to warm)", got)
	}
	if got := puts.Load(); got != 0 {
		t.Errorf("tee upstream PUTs = %d, want 0 (tee must not run above the write-through cap)", got)
	}
}
