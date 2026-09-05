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

	meta, found, _ := c.GetMeta(context.Background(), "b", "obj")
	if !found || meta == nil || !meta.BodyUpstream || meta.ETag != `"upstream-etag"` {
		t.Fatalf("marker disturbed by a version-mismatched re-tier: %+v", meta)
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
