package proxy

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// newTieredTestService builds a Service in tiered mode with an in-memory cache
// and the given small/large tier boundary.
func newTieredTestService(forwarder RequestForwarder, threshold int64) (*Service, *cache.Cache) {
	cfg := config.NewDefault()
	cfg.Mode = config.ModeTiered
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SizeThreshold = threshold
	// Re-validate AFTER setting Mode so the mode-derived defaults apply:
	// production tiered mode auto-selects the CAS coordinator (validateMode),
	// and this suite must exercise the coordinator it actually runs —
	// under legacy the cleanup repair chain (DeleteMetaIfVersion) is a stub.
	if err := cfg.Validate(); err != nil {
		panic(err)
	}

	memCache := cacheclient.NewMemoryCache()
	c := cache.NewCacheWithClient(memCache, &cfg.Cache)
	svc := NewService(forwarder, c, cfg)
	return svc, c
}

// tieredMock wires a mockForwarder that counts upstream traffic and answers
// like a healthy upstream: PUT → 200 + ETag, GET → 200 + body, DELETE → 204.
func tieredMock() (*mockForwarder, *atomic.Int64, *atomic.Int64) {
	forwards := &atomic.Int64{}
	deletes := &atomic.Int64{}
	m := &mockForwarder{
		forwardFunc: func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
			forwards.Add(1)
			switch r.Method {
			case http.MethodPut:
				w.Header().Set("ETag", `"upstream-etag"`)
				w.WriteHeader(http.StatusOK)
			case http.MethodDelete:
				w.WriteHeader(http.StatusNoContent)
			default:
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("upstream body"))
			}
			return nil
		},
		doObjectDeleteFunc: func(ctx context.Context, bucket, key, etag, accessKey, secretKey string) (*http.Response, error) {
			deletes.Add(1)
			return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
		},
	}
	return m, forwards, deletes
}

func tieredDo(t *testing.T, svc *Service, method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body != "" || method == http.MethodPut {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()

	var err error
	switch method {
	case http.MethodGet, http.MethodHead:
		err = svc.HandleGetObject(w, req)
		if method == http.MethodHead {
			w = httptest.NewRecorder()
			req = httptest.NewRequest(method, path, nil)
			for k, v := range hdr {
				req.Header.Set(k, v)
			}
			err = svc.HandleHeadObject(w, req)
		}
	case http.MethodPut:
		err = svc.HandlePutObject(w, req)
	case http.MethodDelete:
		err = svc.HandleDeleteObject(w, req)
	}
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return w
}

// A small object's whole lifecycle — PUT, GET, HEAD, DELETE, GET-after-delete —
// must complete without a single upstream request.
func TestTieredSmallObjectLifecycleZeroUpstream(t *testing.T) {
	mock, forwards, deletes := tieredMock()
	svc, _ := newTieredTestService(mock, 1024)

	if w := tieredDo(t, svc, http.MethodPut, "/b/small", "hello tiered", nil); w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d", w.Code)
	}
	w := tieredDo(t, svc, http.MethodGet, "/b/small", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "hello tiered" {
		t.Fatalf("GET status = %d body %q", w.Code, w.Body.String())
	}
	if w := tieredDo(t, svc, http.MethodHead, "/b/small", "", nil); w.Code != http.StatusOK {
		t.Fatalf("HEAD status = %d", w.Code)
	}
	if w := tieredDo(t, svc, http.MethodDelete, "/b/small", "", nil); w.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d", w.Code)
	}
	if w := tieredDo(t, svc, http.MethodGet, "/b/small", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("GET after DELETE status = %d, want 404", w.Code)
	}

	if n := forwards.Load(); n != 0 {
		t.Fatalf("upstream forwards = %d, want 0", n)
	}
	if n := deletes.Load(); n != 0 {
		t.Fatalf("upstream deletes = %d, want 0", n)
	}
}

// A metadata miss is the authoritative answer: NoSuchKey, zero upstream.
func TestTieredMetaMissIsLocal404(t *testing.T) {
	mock, forwards, _ := tieredMock()
	svc, _ := newTieredTestService(mock, 1024)

	if w := tieredDo(t, svc, http.MethodGet, "/b/absent", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("GET status = %d, want 404", w.Code)
	}
	if w := tieredDo(t, svc, http.MethodHead, "/b/absent", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("HEAD status = %d, want 404", w.Code)
	}
	if n := forwards.Load(); n != 0 {
		t.Fatalf("upstream forwards = %d, want 0", n)
	}
}

// A large PUT passes through and stamps a BodyUpstream marker; HEAD then
// answers from the marker (no forward) while GET forwards for the body.
func TestTieredLargePutStampsMarker(t *testing.T) {
	mock, forwards, _ := tieredMock()
	svc, c := newTieredTestService(mock, 8)

	body := "sixteen byte body!"
	if w := tieredDo(t, svc, http.MethodPut, "/b/large", body, nil); w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d", w.Code)
	}
	if n := forwards.Load(); n != 1 {
		t.Fatalf("forwards after large PUT = %d, want 1", n)
	}

	meta, found, err := c.GetMeta(context.Background(), "b", "large")
	if err != nil || !found || meta == nil {
		t.Fatalf("marker meta not found: found=%v err=%v", found, err)
	}
	if !meta.BodyUpstream {
		t.Fatal("marker meta.BodyUpstream = false, want true")
	}
	if meta.ETag != `"upstream-etag"` {
		t.Fatalf("marker ETag = %q", meta.ETag)
	}
	if meta.ContentLength != int64(len(body)) {
		t.Fatalf("marker ContentLength = %d, want %d", meta.ContentLength, len(body))
	}

	if w := tieredDo(t, svc, http.MethodHead, "/b/large", "", nil); w.Code != http.StatusOK {
		t.Fatalf("HEAD status = %d", w.Code)
	}
	if n := forwards.Load(); n != 1 {
		t.Fatalf("forwards after HEAD = %d, want 1 (HEAD must serve from marker)", n)
	}

	w := tieredDo(t, svc, http.MethodGet, "/b/large", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "upstream body" {
		t.Fatalf("GET status = %d body %q", w.Code, w.Body.String())
	}
	if n := forwards.Load(); n != 2 {
		t.Fatalf("forwards after GET = %d, want 2 (GET body forwards)", n)
	}
}

// A small write over an upstream-tier object must delete the upstream copy.
func TestTieredSmallOverUpstreamCleansUp(t *testing.T) {
	mock, _, _ := tieredMock()
	deleted := make(chan string, 1)
	mock.doObjectDeleteFunc = func(ctx context.Context, bucket, key, etag, accessKey, secretKey string) (*http.Response, error) {
		deleted <- bucket + "/" + key + " " + etag
		return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
	}
	svc, c := newTieredTestService(mock, 8)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusOK {
		t.Fatalf("large PUT status = %d", w.Code)
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "tiny", nil); w.Code != http.StatusOK {
		t.Fatalf("small PUT status = %d", w.Code)
	}

	select {
	case got := <-deleted:
		if got != `b/obj "upstream-etag"` {
			t.Fatalf("cross-tier delete = %q, want key b/obj bound to the displaced ETag", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cross-tier upstream delete never issued")
	}

	meta, found, err := c.GetMeta(context.Background(), "b", "obj")
	if err != nil || !found || meta == nil {
		t.Fatalf("meta not found after small overwrite: found=%v err=%v", found, err)
	}
	if meta.BodyUpstream {
		t.Fatal("meta still marked BodyUpstream after small overwrite")
	}
	if w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil); w.Body.String() != "tiny" {
		t.Fatalf("GET body = %q, want local copy", w.Body.String())
	}
}

// A large write over a local-tier object replaces it with an upstream marker.
func TestTieredLargeOverLocalReplacesMeta(t *testing.T) {
	mock, _, deletes := tieredMock()
	svc, c := newTieredTestService(mock, 8)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "tiny", nil); w.Code != http.StatusOK {
		t.Fatalf("small PUT status = %d", w.Code)
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusOK {
		t.Fatalf("large PUT status = %d", w.Code)
	}

	meta, found, err := c.GetMeta(context.Background(), "b", "obj")
	if err != nil || !found || meta == nil {
		t.Fatalf("meta not found after large overwrite: found=%v err=%v", found, err)
	}
	if !meta.BodyUpstream {
		t.Fatal("meta.BodyUpstream = false after large overwrite")
	}
	if n := deletes.Load(); n != 0 {
		t.Fatalf("upstream deletes = %d, want 0 (local displacement is free)", n)
	}
}

// Requests TAG cannot validate forward to upstream — the auth authority —
// even when the object is cached locally.
func TestTieredUnvalidatedForwards(t *testing.T) {
	mock, forwards, _ := tieredMock()
	svc, _ := newTieredTestService(mock, 1024)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "cached", nil); w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d", w.Code)
	}
	base := forwards.Load()

	mock.validateFunc = func(r *http.Request) (AuthResult, string, string, error) {
		return AuthNotValidated, "", "", nil
	}
	w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil)
	if w.Body.String() != "upstream body" {
		t.Fatalf("GET body = %q, want the forwarded upstream response", w.Body.String())
	}
	if n := forwards.Load(); n != base+1 {
		t.Fatalf("forwards = %d, want %d (unvalidated GET must forward)", n, base+1)
	}
}

// Deleting an upstream-tier object forwards the DELETE and drops the marker.
func TestTieredDeleteUpstreamTierForwards(t *testing.T) {
	mock, forwards, _ := tieredMock()
	svc, c := newTieredTestService(mock, 8)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusOK {
		t.Fatalf("large PUT status = %d", w.Code)
	}
	base := forwards.Load()

	if w := tieredDo(t, svc, http.MethodDelete, "/b/obj", "", nil); w.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d", w.Code)
	}
	if n := forwards.Load(); n != base+1 {
		t.Fatalf("forwards = %d, want %d (upstream-tier DELETE must forward)", n, base+1)
	}
	if _, found, _ := c.GetMeta(context.Background(), "b", "obj"); found {
		t.Fatal("marker meta still present after DELETE")
	}
}

// A conditional write against an upstream-tier prior forwards: only upstream
// can evaluate preconditions against the version it owns.
func TestTieredConditionalPutOverUpstreamForwards(t *testing.T) {
	mock, forwards, _ := tieredMock()
	svc, c := newTieredTestService(mock, 8)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusOK {
		t.Fatalf("large PUT status = %d", w.Code)
	}
	base := forwards.Load()

	w := tieredDo(t, svc, http.MethodPut, "/b/obj", "tiny", map[string]string{"If-Match": `"upstream-etag"`})
	if w.Code != http.StatusOK {
		t.Fatalf("conditional PUT status = %d", w.Code)
	}
	if n := forwards.Load(); n != base+1 {
		t.Fatalf("forwards = %d, want %d (conditional PUT over upstream prior must forward)", n, base+1)
	}
	meta, found, _ := c.GetMeta(context.Background(), "b", "obj")
	if !found || meta == nil || !meta.BodyUpstream {
		t.Fatal("expected a refreshed upstream marker after forwarded conditional PUT")
	}
}

// A failed large overwrite must leave the prior local-tier copy intact: the
// local tier holds the only copy, and a rejected PUT changes nothing.
func TestTieredFailedLargeOverwriteKeepsLocalCopy(t *testing.T) {
	mock, _, _ := tieredMock()
	svc, _ := newTieredTestService(mock, 8)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "tiny", nil); w.Code != http.StatusOK {
		t.Fatalf("small PUT status = %d", w.Code)
	}

	mock.forwardFunc = func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		w.WriteHeader(http.StatusServiceUnavailable)
		return nil
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("failed large PUT status = %d, want 503", w.Code)
	}

	mock.forwardFunc = nil
	w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "tiny" {
		t.Fatalf("GET after failed overwrite = %d %q, want the intact prior copy", w.Code, w.Body.String())
	}
}

// A DELETE whose tombstone lands while a large PUT is in flight must suppress
// the marker: the marker is stamped with the handler's start, so the newer
// tombstone wins and the deleted object is never resurrected as metadata.
func TestTieredConcurrentDeleteSuppressesMarker(t *testing.T) {
	mock, _, _ := tieredMock()
	svc, c := newTieredTestService(mock, 8)

	mock.forwardFunc = func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		// The concurrent DELETE completes (tombstone written) while the PUT's
		// forward is still in flight.
		if err := c.Delete(context.Background(), "b", "obj"); err != nil {
			t.Errorf("mid-flight delete: %v", err)
		}
		w.Header().Set("ETag", `"upstream-etag"`)
		w.WriteHeader(http.StatusOK)
		return nil
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusOK {
		t.Fatalf("large PUT status = %d", w.Code)
	}

	if _, found, _ := c.GetMeta(context.Background(), "b", "obj"); found {
		t.Fatal("marker written despite a newer DELETE tombstone - deleted object resurrected")
	}
	if w := tieredDo(t, svc, http.MethodHead, "/b/obj", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("HEAD after suppressed marker = %d, want 404", w.Code)
	}
}

// When the marker cannot be established, the convergence sweep must not
// destroy a newer local write that raced the forward — it has no upstream
// copy. Here the upstream PUT response carries no ETag (marker skipped) and a
// small write lands mid-flight: the newer local object must keep the key.
func TestTieredMarkerFailureSparesNewerLocalWrite(t *testing.T) {
	mock, _, _ := tieredMock()
	svc, _ := newTieredTestService(mock, 8)

	mock.forwardFunc = func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		// A newer small write completes while the large PUT's forward is in
		// flight. The mock is restored first so the inner PUT takes the plain
		// small-tier path.
		inner := mock.forwardFunc
		mock.forwardFunc = nil
		if w2 := tieredDo(t, svc, http.MethodPut, "/b/obj", "newer", nil); w2.Code != http.StatusOK {
			t.Errorf("mid-flight small PUT status = %d", w2.Code)
		}
		mock.forwardFunc = inner
		// No ETag header: the marker cannot be established.
		w.WriteHeader(http.StatusOK)
		return nil
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusOK {
		t.Fatalf("large PUT status = %d", w.Code)
	}

	w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "newer" {
		t.Fatalf("GET = %d %q, want the surviving newer local write", w.Code, w.Body.String())
	}
}

// The other edge of the identity guard: when the marker fails and the current
// metadata still IS the displaced prior, it must be removed — the failed
// marker must not leave the old version serving after the client's 200.
func TestTieredMarkerFailureRemovesDisplacedPrior(t *testing.T) {
	mock, _, _ := tieredMock()
	svc, c := newTieredTestService(mock, 8)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "stale", nil); w.Code != http.StatusOK {
		t.Fatalf("small PUT status = %d", w.Code)
	}

	mock.forwardFunc = func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		// 2xx with no ETag: the marker cannot be established.
		w.WriteHeader(http.StatusOK)
		return nil
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusOK {
		t.Fatalf("large PUT status = %d", w.Code)
	}

	if _, found, _ := c.GetMeta(context.Background(), "b", "obj"); found {
		t.Fatal("displaced prior metadata still present after marker failure")
	}
	mock.forwardFunc = nil
	if w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("GET after failed marker = %d, want authoritative 404 (never the stale prior)", w.Code)
	}
}

// waitRetierDone polls until no re-tier is in flight for the key. The
// LoadOrStore happens synchronously before the read returns, so absence
// afterwards means the background attempt ran to completion.
func waitRetierDone(t *testing.T, svc *Service, bucket, key string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if !svc.retierRunning(bucket, key) {
			return
		}
		select {
		case <-deadline:
			t.Fatal("re-tier still in flight after 2s")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// A small object that landed upstream-tier (a cold-start PUT forwarded before
// its key was learned) is healed by the first validated read: the body moves
// into the local tier and subsequent reads stop forwarding.
func TestTieredRetierOnReadHealsMisplacedObject(t *testing.T) {
	mock, forwards, _ := tieredMock()
	svc, c := newTieredTestService(mock, 1024)

	// Cold start: the key is not learned yet, so the small PUT forwards and
	// gets an upstream-tier marker.
	mock.validateFunc = func(r *http.Request) (AuthResult, string, string, error) {
		return AuthNotValidated, "", "", nil
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "tiny", nil); w.Code != http.StatusOK {
		t.Fatalf("cold-start PUT status = %d", w.Code)
	}
	meta, found, _ := c.GetMeta(context.Background(), "b", "obj")
	if !found || meta == nil || !meta.BodyUpstream {
		t.Fatal("cold-start PUT did not stamp an upstream-tier marker")
	}

	// Keys learned: reads validate locally now.
	mock.validateFunc = nil
	mock.doFullObjectFunc = func(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error) {
		h := http.Header{}
		h.Set("ETag", `"upstream-etag"`)
		h.Set("Content-Type", "text/plain")
		return &http.Response{StatusCode: http.StatusOK, Header: h, ContentLength: 4, Body: io.NopCloser(strings.NewReader("tiny"))}, nil
	}

	// This read still forwards (the marker is upstream-tier) and triggers the heal.
	if w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil); w.Code != http.StatusOK {
		t.Fatalf("GET status = %d", w.Code)
	}
	waitRetierDone(t, svc, "b", "obj")

	meta, found, _ = c.GetMeta(context.Background(), "b", "obj")
	if !found || meta == nil || meta.BodyUpstream {
		t.Fatalf("object not re-tiered: found=%v meta=%+v", found, meta)
	}
	base := forwards.Load()
	w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "tiny" {
		t.Fatalf("GET after re-tier = %d %q, want the local body", w.Code, w.Body.String())
	}
	if n := forwards.Load(); n != base {
		t.Fatalf("forwards = %d, want %d (re-tiered object must serve locally)", n, base)
	}
}

// The re-tier must refuse to commit when the object was replaced mid-flight:
// a fetched ETag that no longer matches the marker means a concurrent write
// owns the key, and its state is left alone.
func TestTieredRetierSkipsReplacedObject(t *testing.T) {
	mock, _, _ := tieredMock()
	svc, c := newTieredTestService(mock, 1024)

	mock.validateFunc = func(r *http.Request) (AuthResult, string, string, error) {
		return AuthNotValidated, "", "", nil
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "tiny", nil); w.Code != http.StatusOK {
		t.Fatalf("cold-start PUT status = %d", w.Code)
	}
	mock.validateFunc = nil
	mock.doFullObjectFunc = func(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error) {
		h := http.Header{}
		h.Set("ETag", `"a-newer-version"`)
		return &http.Response{StatusCode: http.StatusOK, Header: h, ContentLength: 5, Body: io.NopCloser(strings.NewReader("newer"))}, nil
	}

	if w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil); w.Code != http.StatusOK {
		t.Fatalf("GET status = %d", w.Code)
	}
	waitRetierDone(t, svc, "b", "obj")

	// The replaced object's state wins the key — and the marker CONVERGES to
	// it: the re-tier must never commit the older body, but leaving the old
	// marker authoritative would advertise an ETag on HEAD that no GET
	// serves. The rewritten marker carries the live upstream identity.
	meta, found, _ := c.GetMeta(context.Background(), "b", "obj")
	if !found || meta == nil || !meta.BodyUpstream {
		t.Fatalf("marker lost after a version-mismatched re-tier: %+v", meta)
	}
	if meta.ETag != `"a-newer-version"` {
		t.Fatalf("marker ETag = %q, want convergence to the live upstream version", meta.ETag)
	}
}

// A PUT that lands while a re-tier is in flight must win the key: the PUT
// cancels the re-tier, and the older upstream version is never committed over
// the newer write — which, for a local-tier PUT, is the only copy.
func TestTieredPutCancelsInflightRetier(t *testing.T) {
	mock, _, _ := tieredMock()
	svc, c := newTieredTestService(mock, 1024)

	mock.validateFunc = func(r *http.Request) (AuthResult, string, string, error) {
		return AuthNotValidated, "", "", nil
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "old!", nil); w.Code != http.StatusOK {
		t.Fatalf("cold-start PUT status = %d", w.Code)
	}
	mock.validateFunc = nil

	fetching := make(chan struct{})
	mock.doFullObjectFunc = func(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error) {
		close(fetching)
		// Block until the racing PUT cancels the re-tier.
		<-ctx.Done()
		return nil, ctx.Err()
	}

	if w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil); w.Code != http.StatusOK {
		t.Fatalf("GET status = %d", w.Code)
	}
	<-fetching

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "newer", nil); w.Code != http.StatusOK {
		t.Fatalf("racing PUT status = %d", w.Code)
	}
	waitRetierDone(t, svc, "b", "obj")

	meta, found, _ := c.GetMeta(context.Background(), "b", "obj")
	if !found || meta == nil || meta.BodyUpstream || meta.ContentLength != 5 {
		t.Fatalf("newer write lost to the re-tier: %+v", meta)
	}
	w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil)
	if w.Code != http.StatusOK || w.Body.String() != "newer" {
		t.Fatalf("GET = %d %q, want the racing PUT's body", w.Code, w.Body.String())
	}
}

// A GET arriving while a PUT is in flight sees the old marker but must not
// start a re-tier against it: the PUT's write claim excludes new re-tiers for
// the key until the PUT completes.
func TestTieredWriteClaimExcludesRetier(t *testing.T) {
	mock, _, _ := tieredMock()
	svc, c := newTieredTestService(mock, 8)

	// Cold-start marker for a small object (ContentLength 4 ≤ threshold).
	mock.validateFunc = func(r *http.Request) (AuthResult, string, string, error) {
		return AuthNotValidated, "", "", nil
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "old!", nil); w.Code != http.StatusOK {
		t.Fatalf("cold-start PUT status = %d", w.Code)
	}
	mock.validateFunc = nil

	var fetches atomic.Int64
	mock.doFullObjectFunc = func(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error) {
		fetches.Add(1)
		return nil, context.Canceled
	}

	// A large overwrite blocks mid-forward, holding the write claim.
	putEntered := make(chan struct{})
	putRelease := make(chan struct{})
	base := mock.forwardFunc
	mock.forwardFunc = func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		if r.Method == http.MethodPut {
			close(putEntered)
			<-putRelease
		}
		return base(ctx, w, r)
	}
	putDone := make(chan struct{})
	go func() {
		defer close(putDone)
		req := httptest.NewRequest(http.MethodPut, "/b/obj", strings.NewReader("way past threshold"))
		if err := svc.HandlePutObject(httptest.NewRecorder(), req); err != nil {
			t.Errorf("blocked PUT: %v", err)
		}
	}()
	<-putEntered

	// GET mid-PUT: forwards (old marker) but must not register a re-tier.
	if w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil); w.Code != http.StatusOK {
		t.Fatalf("GET status = %d", w.Code)
	}
	if svc.retierRunning("b", "obj") {
		t.Fatal("re-tier registered while a write held the claim")
	}

	close(putRelease)
	<-putDone
	if n := fetches.Load(); n != 0 {
		t.Fatalf("re-tier fetches = %d, want 0 (claim must exclude the heal)", n)
	}
	meta, found, _ := c.GetMeta(context.Background(), "b", "obj")
	if !found || meta == nil || !meta.BodyUpstream || meta.ContentLength != 18 {
		t.Fatalf("completed PUT's marker disturbed: %+v", meta)
	}
}

// RFC 7232 §3.3: a request carrying If-None-Match is judged by it alone. A
// non-matching If-None-Match must never fall back to an unexpired
// If-Modified-Since — LastModified is second-granular, so that fallback would
// 304 a client across a same-second overwrite.
func TestTieredConditionalGetIfNoneMatchBeatsIfModifiedSince(t *testing.T) {
	mock, _, _ := tieredMock()
	svc, _ := newTieredTestService(mock, 1024)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "v2-body", nil); w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d", w.Code)
	}

	// The client holds a STALE ETag but a fresh-enough date: the mismatching
	// If-None-Match must win and return the full 200 body.
	w := tieredDo(t, svc, http.MethodGet, "/b/obj", "", map[string]string{
		"If-None-Match":     `"stale-etag"`,
		"If-Modified-Since": time.Now().UTC().Add(time.Hour).Format(http.TimeFormat),
	})
	if w.Code != http.StatusOK || w.Body.String() != "v2-body" {
		t.Fatalf("GET = %d %q, want 200 with the current body (If-Modified-Since must be ignored)", w.Code, w.Body.String())
	}
}

// raceOnConditionalReadClient injects a replacement right after the versioned
// read that evaluates a conditional PUT's precondition, so the store's CAS
// must refuse the write.
type raceOnConditionalReadClient struct {
	cacheclient.CacheClient
	armed   bool
	replace func()
}

func (r *raceOnConditionalReadClient) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	data, version, found, err := r.CacheClient.GetWithVersion(ctx, key)
	if r.armed && found && r.replace != nil {
		r.armed = false
		r.replace()
	}
	return data, version, found, err
}

// A conditional PUT whose precondition races away between evaluation and
// store must answer 412, not 200: the version observed at the If-Match check
// is enforced by the store's CAS.
func TestTieredConditionalPutRacedPreconditionAnswers412(t *testing.T) {
	mock, _, _ := tieredMock()
	wrapper := &raceOnConditionalReadClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	cfg.Mode = config.ModeTiered
	// Closing the check-then-store race against a competing WRITE is
	// CAS-coordinator strength; legacy coordination orders commits against
	// deletes only (the pre-CAS engine's documented accepted race).
	cfg.Cache.SetLegacyCoordination(false)
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SizeThreshold = 1024
	c := cache.NewCacheWithClient(wrapper, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "v1-body", nil); w.Code != http.StatusOK {
		t.Fatalf("seed PUT status = %d", w.Code)
	}
	etagV1 := func() string {
		meta, _, _ := c.GetMeta(context.Background(), "b", "obj")
		return meta.ETag
	}()

	// Arm the race: the moment the conditional PUT's evaluation reads the
	// versioned row, a concurrent overwrite replaces it.
	wrapper.replace = func() {
		req := httptest.NewRequest(http.MethodPut, "/b/obj", strings.NewReader("racer!!"))
		if err := svc.HandlePutObject(httptest.NewRecorder(), req); err != nil {
			t.Errorf("racing PUT: %v", err)
		}
	}
	wrapper.armed = true

	w := tieredDo(t, svc, http.MethodPut, "/b/obj", "cond-body", map[string]string{"If-Match": etagV1})
	if w.Code != http.StatusPreconditionFailed {
		t.Fatalf("raced conditional PUT = %d, want 412", w.Code)
	}
	if g := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil); g.Body.String() != "racer!!" {
		t.Fatalf("GET = %q, want the racer's body to survive", g.Body.String())
	}
}

// The same-ETag cleanup race must REPAIR: a marker with the displaced prior's
// ETag re-established while the cross-tier DELETE is in flight (an
// identical-content large PUT — MD5 ETags collide on identical bytes) may
// point at the body that DELETE just removed. The repair converges the key on
// an authoritative miss via DeleteMetaIfVersion. Fails if the suite runs the
// legacy coordinator (whose deleteMetaIfVersion is a refusing stub) — the
// production tiered configuration is CAS.
func TestTieredCleanupRepairsRacedSameETagMarker(t *testing.T) {
	mock, _, _ := tieredMock()
	var c *cache.Cache
	raced := make(chan struct{})
	mock.doObjectDeleteFunc = func(ctx context.Context, bucket, key, etag, accessKey, secretKey string) (*http.Response, error) {
		// Mid-DELETE: an identical-content large PUT re-establishes the marker
		// under the same ETag, exactly the interleaving the pre-check missed.
		marker := &cache.CachedObjectMeta{
			Bucket: bucket, Key: key, ETag: etag,
			BodyUpstream: true, StatusCode: http.StatusOK, ContentLength: 18,
		}
		_, tok, _, _ := c.GetMetaWithVersion(ctx, bucket, key)
		if wrote, err := c.PutMetaIfVersion(ctx, bucket, key, marker, 60, tok); err != nil || !wrote {
			t.Errorf("raced marker re-establishment: wrote=%v err=%v", wrote, err)
		}
		close(raced)
		return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
	}
	var svc *Service
	svc, c = newTieredTestService(mock, 8)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "way past threshold", nil); w.Code != http.StatusOK {
		t.Fatalf("large PUT status = %d", w.Code)
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "tiny", nil); w.Code != http.StatusOK {
		t.Fatalf("small PUT status = %d", w.Code)
	}

	select {
	case <-raced:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup DELETE never issued")
	}
	// The repair runs after the DELETE returns; converge = authoritative miss.
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, found, _ := c.GetMeta(context.Background(), "b", "obj")
		if !found {
			return // repaired: the raced-in marker is gone
		}
		if time.Now().After(deadline) {
			t.Fatal("raced-in same-ETag marker survived the cleanup repair (repair chain not exercised?)")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// bodyPutRaceClient fires race once, on the first PutStream of a body| key —
// the window between an unconditional PUT's decision-token read and its meta
// commit (the engine writes body first, meta second).
type bodyPutRaceClient struct {
	cacheclient.CacheClient
	armed atomic.Bool
	race  func()
}

func (b *bodyPutRaceClient) PutStream(ctx context.Context, key string, rd io.Reader, ttl int64) error {
	err := b.CacheClient.PutStream(ctx, key, rd, ttl)
	if err == nil && strings.HasPrefix(key, "body|") && b.armed.CompareAndSwap(true, false) && b.race != nil {
		b.race()
	}
	return err
}

// An unconditional client PUT whose versioned store is refused must RETRY and
// win, never ack 200 without storing: the racer may be TAG's own re-tier heal
// committing the PRE-PUT version (the write claim is process-local — a
// re-tier on another cluster node is invisible to it), and dropping the
// client's bytes on that refusal silently reverts an acknowledged write in
// the authoritative store. A client write is by definition the newest state.
func TestTieredUnconditionalPutRetriesOverRacedCommit(t *testing.T) {
	mock, _, _ := tieredMock()
	wrapper := &bodyPutRaceClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	cfg.Mode = config.ModeTiered
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SizeThreshold = 1024
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	c := cache.NewCacheWithClient(wrapper, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	// Seed a local-tier prior (the version the "re-tier" will re-commit).
	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "old-content", nil); w.Code != http.StatusOK {
		t.Fatalf("seed PUT status = %d", w.Code)
	}
	oldMeta, _, _ := c.GetMeta(context.Background(), "b", "obj")

	// The moment the client PUT's body lands (after its decision token was
	// read, before its meta commit): a re-tier-shaped racer commits the OLD
	// metadata under the current version, bumping it.
	wrapper.race = func() {
		ctx := context.Background()
		_, tok, _, _ := c.GetMetaWithVersion(ctx, "b", "obj")
		redo := *oldMeta
		if wrote, err := c.PutMetaIfVersion(ctx, "b", "obj", &redo, 60, tok); err != nil || !wrote {
			t.Errorf("racer commit: wrote=%v err=%v", wrote, err)
		}
	}
	wrapper.armed.Store(true)

	w := tieredDo(t, svc, http.MethodPut, "/b/obj", "client-new-bytes", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("client PUT status = %d", w.Code)
	}
	// The 200 must mean the CLIENT's bytes are what the key serves.
	g := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil)
	if g.Code != http.StatusOK || g.Body.String() != "client-new-bytes" {
		t.Fatalf("GET after acked PUT = %d %q, want the client's bytes (acked write reverted?)", g.Code, g.Body.String())
	}
}

// The engine validates Content-MD5 (it IS the store — no upstream will):
// malformed = InvalidDigest, well-formed-but-wrong = BadDigest, matching
// stores. And every error TAG originates carries an x-amz-request-id whose
// body RequestId echoes it — clients cross-check the two.
func TestTieredEngineContentMD5AndRequestID(t *testing.T) {
	mock, _, _ := tieredMock()
	svc, _ := newTieredTestService(mock, 1024)

	put := func(md5hdr string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPut, "/b/obj", strings.NewReader("hello"))
		if md5hdr != "" {
			req.Header.Set("Content-MD5", md5hdr)
		}
		w := httptest.NewRecorder()
		if err := svc.HandlePutObject(w, req); err != nil {
			t.Fatalf("PUT: %v", err)
		}
		return w
	}

	if w := put("not-base64!"); w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "InvalidDigest") {
		t.Fatalf("malformed Content-MD5 = %d %q, want 400 InvalidDigest", w.Code, w.Body.String())
	}
	// PRESENT-but-empty is InvalidDigest on real S3; Header.Get cannot see it
	// (returns "" for absent too), so the engine reads Values.
	reqEmpty := httptest.NewRequest(http.MethodPut, "/b/obj", strings.NewReader("hello"))
	reqEmpty.Header["Content-Md5"] = []string{""}
	wEmpty := httptest.NewRecorder()
	if err := svc.HandlePutObject(wEmpty, reqEmpty); err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if wEmpty.Code != http.StatusBadRequest || !strings.Contains(wEmpty.Body.String(), "InvalidDigest") {
		t.Fatalf("empty Content-MD5 = %d %q, want 400 InvalidDigest", wEmpty.Code, wEmpty.Body.String())
	}
	if w := put(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 16))); w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "BadDigest") {
		t.Fatalf("wrong Content-MD5 = %d %q, want 400 BadDigest", w.Code, w.Body.String())
	}
	sum := md5.Sum([]byte("hello"))
	if w := put(base64.StdEncoding.EncodeToString(sum[:])); w.Code != http.StatusOK {
		t.Fatalf("matching Content-MD5 = %d, want 200", w.Code)
	}

	// Engine-originated error: header and body request ids exist and match.
	req := httptest.NewRequest(http.MethodGet, "/b/definitely-absent", nil)
	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, req); err != nil {
		t.Fatalf("GET: %v", err)
	}
	rid := w.Header().Get("x-amz-request-id")
	if w.Code != http.StatusNotFound || rid == "" {
		t.Fatalf("engine 404 = %d request-id %q, want a minted id", w.Code, rid)
	}
	if !strings.Contains(w.Body.String(), "<RequestId>"+rid+"</RequestId>") {
		t.Fatalf("body RequestId does not echo header %q: %s", rid, w.Body.String())
	}
}

// A multipart completion in tiered mode stamps a BodyUpstream marker (the
// assembled body lives upstream by construction), replacing the old
// read-as-miss punt: HEAD answers from the marker with the HEAD-sourced
// length, GET forwards for the body.
func TestTieredMultipartCompletionStampsMarker(t *testing.T) {
	mock, forwards, _ := tieredMock()
	completionXML := `<CompleteMultipartUploadResult><ETag>"mp-etag-3"</ETag></CompleteMultipartUploadResult>`
	mock.captureFunc = func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(completionXML))
		return &ResponseCapture{StatusCode: http.StatusOK, Body: []byte(completionXML), Complete: true, Headers: http.Header{}}, nil
	}
	headHdr := http.Header{}
	headHdr.Set("ETag", `"mp-etag-3"`)
	headHdr.Set("Content-Length", "5242880")
	headHdr.Set("Content-Type", "application/octet-stream")
	mock.conditionalResp = &http.Response{StatusCode: http.StatusOK, Header: headHdr, Body: http.NoBody}
	svc, c := newTieredTestService(mock, 1024)

	req := httptest.NewRequest(http.MethodPost, "/b/mp-obj?uploadId=u1", strings.NewReader("<CompleteMultipartUpload/>"))
	w := httptest.NewRecorder()
	if err := svc.HandleCompleteMultipartUpload(w, req); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("completion status = %d", w.Code)
	}

	// Phase 1 (immediate): the ETag-only marker exists the moment the handler
	// returns — no read-after-write NoSuchKey window while the HEAD runs.
	meta, found, _ := c.GetMeta(context.Background(), "b", "mp-obj")
	if !found || meta == nil || !meta.BodyUpstream || meta.ETag != `"mp-etag-3"` {
		t.Fatalf("no BodyUpstream marker after completion: %+v", meta)
	}
	// Phase 2 (background): the HEAD upgrade lands the real length.
	deadline := time.Now().Add(2 * time.Second)
	for {
		m, f2, _ := c.GetMeta(context.Background(), "b", "mp-obj")
		if f2 && m != nil && m.ContentLength == 5242880 {
			meta = m
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("marker never upgraded with the HEAD-sourced length: %+v", m)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// HEAD from the marker, no forward; GET forwards for the body.
	before := forwards.Load()
	if hw := tieredDo(t, svc, http.MethodHead, "/b/mp-obj", "", nil); hw.Code != http.StatusOK || hw.Header().Get("Content-Length") != "5242880" {
		t.Fatalf("HEAD = %d CL %q, want 200 with the marker's length", hw.Code, hw.Header().Get("Content-Length"))
	}
	if forwards.Load() != before {
		t.Fatal("HEAD of a marker forwarded upstream")
	}
	gw := tieredDo(t, svc, http.MethodGet, "/b/mp-obj", "", nil)
	if gw.Code != http.StatusOK || gw.Body.String() != "upstream body" {
		t.Fatalf("GET = %d %q, want the forwarded body", gw.Code, gw.Body.String())
	}
}

// A completion whose upstream HEAD is unavailable still stamps an ETag-only
// marker (unknown length): the object must exist in the authoritative view —
// GETs forward regardless, HEAD just omits the length.
func TestTieredMultipartCompletionMarkerWithoutHead(t *testing.T) {
	mock, _, _ := tieredMock()
	completionXML := `<CompleteMultipartUploadResult><ETag>"mp-etag-9"</ETag></CompleteMultipartUploadResult>`
	mock.captureFunc = func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(completionXML))
		return &ResponseCapture{StatusCode: http.StatusOK, Body: []byte(completionXML), Complete: true, Headers: http.Header{}}, nil
	}
	mock.conditionalErr = errors.New("upstream HEAD unavailable")
	svc, c := newTieredTestService(mock, 1024)

	req := httptest.NewRequest(http.MethodPost, "/b/mp-obj?uploadId=u2", strings.NewReader("<CompleteMultipartUpload/>"))
	if err := svc.HandleCompleteMultipartUpload(httptest.NewRecorder(), req); err != nil {
		t.Fatalf("complete: %v", err)
	}
	meta, found, _ := c.GetMeta(context.Background(), "b", "mp-obj")
	if !found || meta == nil || !meta.BodyUpstream || meta.ETag != `"mp-etag-9"` {
		t.Fatalf("no fallback marker: %+v", meta)
	}
	if meta.ContentLength >= 0 {
		t.Fatalf("fallback marker CL = %d, want unknown (-1)", meta.ContentLength)
	}
	if gw := tieredDo(t, svc, http.MethodGet, "/b/mp-obj", "", nil); gw.Code != http.StatusOK || gw.Body.String() != "upstream body" {
		t.Fatalf("GET via fallback marker = %d %q", gw.Code, gw.Body.String())
	}
}

// bodyKeyLedger records body-key writes and deletes so tests can assert the
// staged-body lifecycle: exactly one stream per PUT, staged keys reclaimed on
// abort, and meta-only retries never re-streaming.
type bodyKeyLedger struct {
	cacheclient.CacheClient
	mu      sync.Mutex
	puts    []string
	deletes []string
}

func (b *bodyKeyLedger) PutStream(ctx context.Context, key string, r io.Reader, ttl int64) error {
	if strings.HasPrefix(key, "body|") {
		b.mu.Lock()
		b.puts = append(b.puts, key)
		b.mu.Unlock()
	}
	return b.CacheClient.PutStream(ctx, key, r, ttl)
}

func (b *bodyKeyLedger) Delete(ctx context.Context, key string) error {
	if strings.HasPrefix(key, "body|") {
		b.mu.Lock()
		b.deletes = append(b.deletes, key)
		b.mu.Unlock()
	}
	return b.CacheClient.Delete(ctx, key)
}

func newLedgerTieredService(t *testing.T, threshold int64) (*Service, *cache.Cache, *bodyKeyLedger) {
	t.Helper()
	mock, _, _ := tieredMock()
	ledger := &bodyKeyLedger{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	cfg.Mode = config.ModeTiered
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SizeThreshold = threshold
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	c := cache.NewCacheWithClient(ledger, &cfg.Cache)
	return NewService(mock, c, cfg), c, ledger
}

// A streamed engine PUT stores the body under its BodyRef — never under the
// ETag — with the correct MD5 ETag, and serves back through the discriminator
// on both the small buffered path and the large streaming path.
func TestTieredStreamedPutBodyRefRoundTrip(t *testing.T) {
	svc, c, ledger := newLedgerTieredService(t, 1<<20)

	big := strings.Repeat("0123456789abcdef", 8192) // 128 KiB > smallObjectThreshold
	if w := tieredDo(t, svc, http.MethodPut, "/b/big", big, nil); w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d", w.Code)
	}
	meta, found, _ := c.GetMeta(context.Background(), "b", "big")
	if !found || meta == nil || meta.BodyRef == "" {
		t.Fatalf("engine entry missing BodyRef: %+v", meta)
	}
	sum := md5.Sum([]byte(big))
	if meta.ETag != `"`+hex.EncodeToString(sum[:])+`"` {
		t.Fatalf("ETag = %q, want streamed MD5", meta.ETag)
	}
	if len(ledger.puts) != 1 || ledger.puts[0] != cache.MakeBodyKey("b", "big", meta.BodyRef) {
		t.Fatalf("body writes = %v, want exactly one under the BodyRef", ledger.puts)
	}
	if w := tieredDo(t, svc, http.MethodGet, "/b/big", "", nil); w.Code != http.StatusOK || w.Body.String() != big {
		t.Fatalf("large GET = %d len %d, want the streamed body", w.Code, w.Body.Len())
	}
	if w := tieredDo(t, svc, http.MethodPut, "/b/small", "tiny", nil); w.Code != http.StatusOK {
		t.Fatalf("small PUT status = %d", w.Code)
	}
	if w := tieredDo(t, svc, http.MethodGet, "/b/small", "", nil); w.Code != http.StatusOK || w.Body.String() != "tiny" {
		t.Fatalf("small GET = %d %q", w.Code, w.Body.String())
	}
}

// A pre-existing content-addressed entry (proxy-written shape: body keyed by
// ETag, BodyRef null) serves through the same engine unchanged — the
// discriminator falls back to the ETag.
func TestTieredServesETagKeyedEntryFromPriorMode(t *testing.T) {
	svc, c, _ := newLedgerTieredService(t, 1<<20)
	meta := &cache.CachedObjectMeta{Bucket: "b", Key: "old", ETag: `"v1"`, ContentLength: 5, StatusCode: 200}
	if err := c.PutWithMeta(context.Background(), "b", "old", meta, []byte("hello"), 60); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if w := tieredDo(t, svc, http.MethodGet, "/b/old", "", nil); w.Code != http.StatusOK || w.Body.String() != "hello" {
		t.Fatalf("GET of ETag-keyed entry = %d %q", w.Code, w.Body.String())
	}
}

// A body longer than its declared size aborts after the stream and reclaims
// the staged body: no orphan, no meta.
func TestTieredStreamedPutOverrunDiscardsStagedBody(t *testing.T) {
	svc, c, ledger := newLedgerTieredService(t, 1<<20)
	req := httptest.NewRequest(http.MethodPut, "/b/obj", strings.NewReader("0123456789"))
	req.ContentLength = 4 // declares 4, sends 10
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, req); err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "IncompleteBody") {
		t.Fatalf("overrun PUT = %d %q, want 400 IncompleteBody", w.Code, w.Body.String())
	}
	if _, found, _ := c.GetMeta(context.Background(), "b", "obj"); found {
		t.Fatal("meta committed for an overrun body")
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if len(ledger.puts) != 1 || len(ledger.deletes) != 1 || ledger.puts[0] != ledger.deletes[0] {
		t.Fatalf("staged lifecycle = puts %v deletes %v, want the one staged key reclaimed", ledger.puts, ledger.deletes)
	}
}

// A refused unconditional commit retries METADATA ONLY: one body stream
// total, no staged-body churn, and the client's bytes win. The racer commits
// between the handler-start token read and the meta commit (fired from the
// conditional-read hook), so the first PutMetaIfVersion is refused.
func TestTieredStreamedPutRetryIsMetaOnly(t *testing.T) {
	mock, _, _ := tieredMock()
	ledger := &bodyKeyLedger{CacheClient: cacheclient.NewMemoryCache()}
	wrapper := &raceOnConditionalReadClient{CacheClient: ledger}
	cfg := config.NewDefault()
	cfg.Mode = config.ModeTiered
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SizeThreshold = 1024
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	c := cache.NewCacheWithClient(wrapper, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "old-content", nil); w.Code != http.StatusOK {
		t.Fatalf("seed PUT status = %d", w.Code)
	}
	oldMeta, _, _ := c.GetMeta(context.Background(), "b", "obj")

	wrapper.replace = func() {
		ctx := context.Background()
		_, tok, _, _ := c.GetMetaWithVersion(ctx, "b", "obj")
		redo := *oldMeta
		if wrote, err := c.PutMetaIfVersion(ctx, "b", "obj", &redo, 60, tok); err != nil || !wrote {
			t.Errorf("racer commit: wrote=%v err=%v", wrote, err)
		}
	}
	ledger.mu.Lock()
	before := len(ledger.puts)
	ledger.mu.Unlock()
	wrapper.armed = true

	if w := tieredDo(t, svc, http.MethodPut, "/b/obj", "client-new-bytes", nil); w.Code != http.StatusOK {
		t.Fatalf("client PUT status = %d", w.Code)
	}
	ledger.mu.Lock()
	streamed := len(ledger.puts) - before
	ledger.mu.Unlock()
	if streamed != 1 {
		t.Fatalf("body streams during retried PUT = %d, want exactly 1 (meta-only retries)", streamed)
	}
	if g := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil); g.Body.String() != "client-new-bytes" {
		t.Fatalf("GET = %q, want the client's bytes", g.Body.String())
	}
}

// truncatedReader yields some bytes then a non-sentinel EOF-family error —
// the shape of a client disconnect mid-body.
type truncatedReader struct {
	data []byte
	done bool
}

func (t *truncatedReader) Read(p []byte) (int, error) {
	if !t.done {
		t.done = true
		n := copy(p, t.data)
		return n, nil
	}
	return 0, io.ErrUnexpectedEOF
}

// A truncated or malformed streamed body answers 400 IncompleteBody — the
// client misdescribed the request — never a retryable 500, and the staged
// body is reclaimed. Covers the chunk decoder's WRAPPED EOF (which a naive
// errors.Is(err, io.EOF) exemption would swallow into a 500) and the
// plain-body mid-transfer disconnect.
func TestTieredStreamedPutTruncatedBodyAnswersIncomplete(t *testing.T) {
	svc, c, ledger := newLedgerTieredService(t, 1<<20)

	// Truncated aws-chunked framing: the header declares a 5-byte chunk but
	// the body ends mid-chunk — the decoder reports a wrapped EOF.
	chunked := "5;chunk-signature=deadbeef\r\nhel"
	req := httptest.NewRequest(http.MethodPut, "/b/trunc-chunked", strings.NewReader(chunked))
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("X-Amz-Decoded-Content-Length", "5")
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, req); err != nil {
		t.Fatalf("chunked PUT: %v", err)
	}
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "IncompleteBody") {
		t.Fatalf("truncated chunked PUT = %d %q, want 400 IncompleteBody", w.Code, w.Body.String())
	}

	// Plain body that dies mid-transfer with ErrUnexpectedEOF.
	req2 := httptest.NewRequest(http.MethodPut, "/b/trunc-plain", nil)
	req2.Body = io.NopCloser(&truncatedReader{data: []byte("hel")})
	req2.ContentLength = 10
	w2 := httptest.NewRecorder()
	if err := svc.HandlePutObject(w2, req2); err != nil {
		t.Fatalf("plain PUT: %v", err)
	}
	if w2.Code != http.StatusBadRequest || !strings.Contains(w2.Body.String(), "IncompleteBody") {
		t.Fatalf("truncated plain PUT = %d %q, want 400 IncompleteBody", w2.Code, w2.Body.String())
	}

	// Nothing committed, every staged body reclaimed.
	for _, k := range []string{"trunc-chunked", "trunc-plain"} {
		if _, found, _ := c.GetMeta(context.Background(), "b", k); found {
			t.Fatalf("meta committed for truncated body %s", k)
		}
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if len(ledger.puts) != len(ledger.deletes) {
		t.Fatalf("staged lifecycle = puts %v deletes %v, want every staged key reclaimed", ledger.puts, ledger.deletes)
	}
}

// metaCommitErrClient makes the FIRST meta-key PutIfVersion return an error
// AFTER forwarding it to the store — the ambiguous-commit shape: the write
// may have landed, only the confirmation is lost.
type metaCommitErrClient struct {
	cacheclient.CacheClient
	armed atomic.Bool
}

func (m *metaCommitErrClient) PutIfVersion(ctx context.Context, key string, value []byte, ttl int64, expected uint64) (uint64, error) {
	v, err := m.CacheClient.PutIfVersion(ctx, key, value, ttl, expected)
	if strings.HasPrefix(key, "meta|") && m.armed.CompareAndSwap(true, false) {
		return v, errors.New("simulated lost confirmation")
	}
	return v, err
}

// An AMBIGUOUS meta-commit error must NOT delete the staged body: the write
// may have landed (here it provably did), and deleting the body a visible
// meta references would erase the object — a 500-answering PUT destroying
// data. TTL reclaims genuine orphans.
func TestTieredStreamedPutAmbiguousCommitKeepsStagedBody(t *testing.T) {
	mock, _, _ := tieredMock()
	inner := &bodyKeyLedger{CacheClient: cacheclient.NewMemoryCache()}
	wrapper := &metaCommitErrClient{CacheClient: inner}
	cfg := config.NewDefault()
	cfg.Mode = config.ModeTiered
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SizeThreshold = 1024
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	c := cache.NewCacheWithClient(wrapper, &cfg.Cache)
	svc := NewService(mock, c, cfg)

	wrapper.armed.Store(true)
	req := httptest.NewRequest(http.MethodPut, "/b/obj", strings.NewReader("hello"))
	w := httptest.NewRecorder()
	err := svc.HandlePutObject(w, req)
	if err == nil && w.Code == http.StatusOK {
		t.Fatal("ambiguous commit did not surface as an error")
	}

	// The commit actually landed (the wrapper forwarded before erroring):
	// the entry must be fully servable — its staged body NOT deleted.
	meta, found, _ := c.GetMeta(context.Background(), "b", "obj")
	if !found || meta == nil || meta.BodyRef == "" {
		t.Fatalf("landed meta missing: %+v", meta)
	}
	inner.mu.Lock()
	deletes := len(inner.deletes)
	inner.mu.Unlock()
	if deletes != 0 {
		t.Fatalf("staged body deleted on an ambiguous commit error (deletes=%v)", inner.deletes)
	}
	if g := tieredDo(t, svc, http.MethodGet, "/b/obj", "", nil); g.Code != http.StatusOK || g.Body.String() != "hello" {
		t.Fatalf("GET after ambiguous-commit PUT = %d %q, want the landed object", g.Code, g.Body.String())
	}
}

// If-None-Match:* against a live upstream-tier MARKER is a 412: the object
// EXISTS (its body upstream), and the local-body servability probe must not
// read it as absent and let a create-only PUT overwrite it.
func TestTieredConditionalPutMarkerCountsAsExisting(t *testing.T) {
	svc, c, _ := newLedgerTieredService(t, 1024)
	marker := &cache.CachedObjectMeta{
		Bucket: "b", Key: "obj", ETag: `"up-1"`, BodyUpstream: true,
		ContentLength: 5000, StatusCode: 200,
	}
	_, tok, _, _ := c.GetMetaWithVersion(context.Background(), "b", "obj")
	if wrote, err := c.PutMetaIfVersion(context.Background(), "b", "obj", marker, 60, tok); err != nil || !wrote {
		t.Fatalf("seed marker: wrote=%v err=%v", wrote, err)
	}

	// The steady-state router forwards marker-prior conditionals upstream;
	// the ENGINE sees this shape only in the double-read race (a marker
	// committing between the router's read and the engine's). Exercise the
	// engine directly — the race's exact entry point.
	req := httptest.NewRequest(http.MethodPut, "/b/obj", strings.NewReader("tiny"))
	req.Header.Set("If-None-Match", "*")
	w := httptest.NewRecorder()
	if err := svc.HandleOriginlessPut(w, req); err != nil {
		t.Fatalf("engine PUT: %v", err)
	}
	if w.Code != http.StatusPreconditionFailed {
		t.Fatalf("If-None-Match:* over a live marker = %d, want 412", w.Code)
	}
	cur, found, _ := c.GetMeta(context.Background(), "b", "obj")
	if !found || cur == nil || !cur.BodyUpstream || cur.ETag != `"up-1"` {
		t.Fatalf("marker disturbed by refused conditional: %+v", cur)
	}
}

// A sub-resource GET (?tagging) against a small upstream-tier marker must
// NOT trigger a re-tier heal: the full-body download would serve nothing the
// triggering request needs.
func TestTieredSubresourceGetDoesNotRetier(t *testing.T) {
	mock, _, _ := tieredMock()
	var fullFetches atomic.Int32
	mock.doFullObjectFunc = func(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error) {
		fullFetches.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
	}
	svc, c := newTieredTestService(mock, 1024)
	marker := &cache.CachedObjectMeta{
		Bucket: "b", Key: "obj", ETag: `"up-1"`, BodyUpstream: true,
		ContentLength: 100, StatusCode: 200,
	}
	_, tok, _, _ := c.GetMetaWithVersion(context.Background(), "b", "obj")
	if wrote, err := c.PutMetaIfVersion(context.Background(), "b", "obj", marker, 60, tok); err != nil || !wrote {
		t.Fatalf("seed marker: wrote=%v err=%v", wrote, err)
	}

	req := httptest.NewRequest(http.MethodGet, "/b/obj?tagging", nil)
	if err := svc.HandleGetObject(httptest.NewRecorder(), req); err != nil {
		t.Fatalf("sub-resource GET: %v", err)
	}
	waitRetierDone(t, svc, "b", "obj")
	if n := fullFetches.Load(); n != 0 {
		t.Fatalf("sub-resource GET triggered %d re-tier fetches, want 0", n)
	}
}

// A chunked body whose bytes match the declared length but whose framing was
// never terminated (no data-CRLF, no terminal chunk) is TRUNCATED — the
// decoder now surfaces every truncation shape as a wrapped error, so the
// coincidental-length case answers IncompleteBody instead of committing 200.
func TestTieredStreamedPutUnterminatedChunkingAnswersIncomplete(t *testing.T) {
	svc, c, _ := newLedgerTieredService(t, 1<<20)
	body := "5;chunk-signature=deadbeef\r\nhello" // 5 data bytes, then EOF: no CRLF, no 0-chunk
	req := httptest.NewRequest(http.MethodPut, "/b/obj", strings.NewReader(body))
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("X-Amz-Decoded-Content-Length", "5")
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, req); err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "IncompleteBody") {
		t.Fatalf("unterminated chunked PUT = %d %q, want 400 IncompleteBody", w.Code, w.Body.String())
	}
	if _, found, _ := c.GetMeta(context.Background(), "b", "obj"); found {
		t.Fatal("meta committed for an unterminated chunked body")
	}
}

// A tiered DELETE forwarded (upstream-tier or unknown key) must converge the
// cache under the PRE-FORWARD version — never the unconditional coordinator
// delete that out-deletes racing writers. A small PUT acked DURING the
// forward is the local tier's only copy and must survive.
var svcForDelete *Service

func TestTieredForwardedDeleteConvergeSparesRacingPut(t *testing.T) {
	mock, _, _ := tieredMock()
	var c *cache.Cache
	// A large upstream-tier marker exists, so DELETE forwards (not local).
	raced := make(chan struct{})
	mock.doObjectDeleteFunc = func(ctx context.Context, bucket, key, etag, ak, sk string) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
	}
	mock.forwardFunc = func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		if r.Method == http.MethodDelete {
			// Mid-forward: a small client PUT commits a new local-tier object.
			put := httptest.NewRequest(http.MethodPut, "/b/obj", strings.NewReader("racer-wins"))
			if err := svcForDelete.HandlePutObject(httptest.NewRecorder(), put); err != nil {
				t.Errorf("racing PUT: %v", err)
			}
			close(raced)
			w.WriteHeader(http.StatusNoContent)
			return nil
		}
		w.WriteHeader(http.StatusOK)
		return nil
	}
	cfg := config.NewDefault()
	cfg.Mode = config.ModeTiered
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SizeThreshold = 1024
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	c = cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	svcForDelete = NewService(mock, c, cfg)

	// Seed an upstream-tier marker so the DELETE forwards.
	marker := &cache.CachedObjectMeta{Bucket: "b", Key: "obj", ETag: `"up-1"`, BodyUpstream: true, ContentLength: 5000, StatusCode: 200}
	_, tok, _, _ := c.GetMetaWithVersion(context.Background(), "b", "obj")
	if wrote, err := c.PutMetaIfVersion(context.Background(), "b", "obj", marker, 60, tok); err != nil || !wrote {
		t.Fatalf("seed marker: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/b/obj", nil)
	if err := svcForDelete.HandleDeleteObject(httptest.NewRecorder(), req); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	<-raced

	// The racing PUT's object must survive the converge.
	cur, found, _ := c.GetMeta(context.Background(), "b", "obj")
	if !found || cur == nil || cur.BodyUpstream {
		t.Fatalf("racing PUT's local object was out-deleted by the DELETE converge: %+v", cur)
	}
	g := tieredDo(t, svcForDelete, http.MethodGet, "/b/obj", "", nil)
	if g.Code != http.StatusOK || g.Body.String() != "racer-wins" {
		t.Fatalf("GET = %d %q, want the racing PUT's bytes", g.Code, g.Body.String())
	}
}
