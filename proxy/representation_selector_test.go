package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tigrisdata/tag/cache"
)

// Object GETs/HEADs carrying representation selectors (?versionId, ?partNumber,
// response-* header overrides) must go upstream: the cache holds only the
// current object under its own headers, so a cached answer would silently
// ignore the selector. They must not populate the cache either.

func TestHasRepresentationSelectors(t *testing.T) {
	cases := []struct {
		query string
		want  bool
	}{
		{"", false},
		{"x-id=GetObject", false},
		{"X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc&X-Amz-Expires=300", false},
		{"tagging", false},
		{"versionid=old", false}, // S3 query parameter names are case-sensitive
		{"versionId=old", true},
		{"partNumber=2", true},
		{"response-cache-control=no-cache", true},
		{"response-content-disposition=attachment%3B%20filename%3Dx.txt", true},
		{"response-content-encoding=gzip", true},
		{"response-content-language=en", true},
		{"response-content-type=text%2Fplain", true},
		{"response-expires=Thu%2C%2001%20Jan%202026", true},
		{"X-Amz-Signature=abc&versionId=old", true},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(http.MethodGet, "/b/k?"+tc.query, nil)
		if got := hasRepresentationSelectors(r); got != tc.want {
			t.Errorf("hasRepresentationSelectors(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}
}

// selectorUpstream counts every upstream-reaching forwarder call and answers
// with a distinct version, so a leaked cache read or populate shows up as the
// wrong ETag/body on the client side or in the cache.
type selectorUpstream struct {
	calls atomic.Int64
	etag  string
	body  string
}

func (u *selectorUpstream) response() *http.Response {
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"ETag": {u.etag}, "Content-Type": {"text/plain"}},
		Body:          io.NopCloser(strings.NewReader(u.body)),
		ContentLength: int64(len(u.body)),
	}
}

func newSelectorFixture(t *testing.T) (*Service, *cache.Cache, *selectorUpstream, *cache.CachedObjectMeta) {
	t.Helper()
	up := &selectorUpstream{etag: `"upstream-old"`, body: "upstream old version"}
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, r *http.Request) error {
			up.calls.Add(1)
			w.Header().Set("ETag", up.etag)
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			if r.Method != http.MethodHead {
				_, _ = w.Write([]byte(up.body))
			}
			return nil
		},
		// Traps: a request that reaches the broadcast miss path or a background
		// warm would call these; they answer so that a leak is caught by the
		// call count and the cache-untouched checks instead of a nil deref.
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			up.calls.Add(1)
			return up.response(), nil
		},
		doFullObjectFunc: func(context.Context, string, string, string, string) (*http.Response, error) {
			up.calls.Add(1)
			return up.response(), nil
		},
	}
	svc, c := newTestService(mock, true)
	t.Cleanup(svc.Stop)

	meta := &cache.CachedObjectMeta{
		Bucket: "b", Key: "k", ETag: `"cached"`, ContentType: "text/plain",
		ContentLength: int64(len("cached body")), StatusCode: http.StatusOK,
	}
	if err := c.PutWithMeta(context.Background(), "b", "k", meta, []byte("cached body"), 0); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	return svc, c, up, meta
}

// requireCacheUntouched waits for any background cache work, then checks the
// warm entry is exactly as seeded.
func requireCacheUntouched(t *testing.T, svc *Service, c *cache.Cache, want *cache.CachedObjectMeta) {
	t.Helper()
	svc.Stop() // joins every admitted background task (a leaked populate/warm would be one)
	got, found, err := c.GetMeta(context.Background(), "b", "k")
	if err != nil || !found || got == nil {
		t.Fatalf("GetMeta after request: found=%v err=%v", found, err)
	}
	if got.ETag != want.ETag || got.ContentLength != want.ContentLength {
		t.Fatalf("cache entry changed: ETag=%s len=%d, want ETag=%s len=%d", got.ETag, got.ContentLength, want.ETag, want.ContentLength)
	}
	var buf bytes.Buffer
	if err := c.GetBodyStream(context.Background(), "b", "k", want.ETag, &buf); err != nil {
		t.Fatalf("cached body unreadable after request: %v", err)
	}
	if buf.String() != "cached body" {
		t.Fatalf("cached body = %q, want %q", buf.String(), "cached body")
	}
}

func TestRepresentationSelectors_ForwardUpstreamAndLeaveCacheUntouched(t *testing.T) {
	cases := []struct {
		name   string
		method string
		target string
	}{
		{"GET response-content-disposition", http.MethodGet, "/b/k?response-content-disposition=attachment%3B%20filename%3Dx.txt"},
		{"GET versionId", http.MethodGet, "/b/k?versionId=old"},
		{"GET partNumber", http.MethodGet, "/b/k?partNumber=2"},
		{"GET presigned with versionId", http.MethodGet, "/b/k?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc&versionId=old"},
		{"HEAD versionId", http.MethodHead, "/b/k?versionId=old"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, c, up, meta := newSelectorFixture(t)

			w := httptest.NewRecorder()
			r := httptest.NewRequest(tc.method, tc.target, nil)
			var err error
			if tc.method == http.MethodHead {
				err = svc.HandleHeadObject(w, r)
			} else {
				err = svc.HandleGetObject(w, r)
			}
			if err != nil {
				t.Fatalf("handler error: %v", err)
			}
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", w.Code)
			}
			if got := w.Header().Get(XCacheHeader); got == XCacheHit || got != XCacheBypass {
				t.Errorf("X-Cache = %q, want %q", got, XCacheBypass)
			}
			if got := w.Header().Get("ETag"); got != up.etag {
				t.Errorf("ETag = %q, want upstream %q (served from cache?)", got, up.etag)
			}
			if tc.method == http.MethodGet && w.Body.String() != up.body {
				t.Errorf("body = %q, want upstream %q", w.Body.String(), up.body)
			}
			if n := up.calls.Load(); n != 1 {
				t.Errorf("upstream calls = %d, want 1", n)
			}
			requireCacheUntouched(t, svc, c, meta)
		})
	}
}

// Control: without a selector the same warm entry is a plain cache hit.
func TestRepresentationSelectors_PlainGetStillHits(t *testing.T) {
	svc, _, up, _ := newSelectorFixture(t)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, httptest.NewRequest(http.MethodGet, "/b/k", nil)); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Errorf("X-Cache = %q, want %q", got, XCacheHit)
	}
	if w.Body.String() != "cached body" {
		t.Errorf("body = %q, want cached body", w.Body.String())
	}
	if n := up.calls.Load(); n != 0 {
		t.Errorf("upstream calls = %d, want 0", n)
	}
}

// Tiered mode: a selector never serves the local tier's current version. An
// upstream-tier object forwards (GET and HEAD alike, HEAD not answered from
// the marker); a local-tier object is not served from its cached copy.
func TestTieredRepresentationSelectorsNeverServeLocalCopy(t *testing.T) {
	mock, forwards, _ := tieredMock()
	svc, c := newTieredTestService(mock, 1024)
	t.Cleanup(svc.Stop)
	ctx := context.Background()

	local := &cache.CachedObjectMeta{Bucket: "b", Key: "small", ETag: `"local-version"`, ContentLength: 3, StatusCode: http.StatusOK}
	if err := c.PutWithMeta(ctx, "b", "small", local, []byte("old"), 60); err != nil {
		t.Fatalf("seed local-tier object: %v", err)
	}
	marker := &cache.CachedObjectMeta{Bucket: "b", Key: "large", ETag: `"upstream-version"`, BodyUpstream: true, ContentLength: 5000, StatusCode: http.StatusOK}
	if err := c.PutWithMeta(ctx, "b", "large", marker, nil, 60); err != nil {
		t.Fatalf("seed upstream marker: %v", err)
	}

	handle := func(w http.ResponseWriter, r *http.Request) error {
		if r.Method == http.MethodHead {
			return svc.HandleHeadObject(w, r)
		}
		return svc.HandleGetObject(w, r)
	}

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		w := httptest.NewRecorder()
		if err := handle(w, httptest.NewRequest(method, "/b/small?versionId=old", nil)); err != nil {
			t.Fatalf("%s local-tier selector: %v", method, err)
		}
		if w.Header().Get(XCacheHeader) == XCacheHit || w.Body.String() == "old" || w.Header().Get("ETag") == local.ETag {
			t.Fatalf("%s local-tier selector served the current local copy: %d X-Cache=%q ETag=%q body=%q",
				method, w.Code, w.Header().Get(XCacheHeader), w.Header().Get("ETag"), w.Body.String())
		}
	}

	before := forwards.Load()
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		w := httptest.NewRecorder()
		if err := handle(w, httptest.NewRequest(method, "/b/large?versionId=old", nil)); err != nil {
			t.Fatalf("%s upstream-tier selector: %v", method, err)
		}
		if w.Code != http.StatusOK || w.Header().Get("ETag") == marker.ETag {
			t.Fatalf("%s upstream-tier selector not forwarded: %d ETag=%q", method, w.Code, w.Header().Get("ETag"))
		}
	}
	if n := forwards.Load() - before; n != 2 {
		t.Fatalf("upstream-tier selector forwards = %d, want 2", n)
	}
}
