package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

func TestOriginFor_DerivesModeFromEndpoint(t *testing.T) {
	if got := originFor("https://t3.storage.dev"); !got.CanFill() || !got.CanRevalidate() {
		t.Fatalf("configured endpoint must yield a filling, revalidating origin, got %#v", got)
	}
	if got := originFor(""); got.CanFill() || got.CanRevalidate() {
		t.Fatalf("empty endpoint must yield an origin-less policy, got %#v", got)
	}
}

// Unsigned requests must not gain access to non-public cached objects merely
// because the upstream went away. Opening that up is a separate decision.
func TestOrigin_NeitherModeTrustsUnauthenticated(t *testing.T) {
	for name, o := range map[string]Origin{"proxy": proxyOrigin{}, "originless": noOrigin{}} {
		if o.TrustsUnauthenticated() {
			t.Errorf("%s: TrustsUnauthenticated must be false until explicitly enabled", name)
		}
	}
}

func TestRevalidationRequested_HonorsClientWhenOriginExists(t *testing.T) {
	s := &Service{origin: proxyOrigin{}}
	r := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	r.Header.Set("Cache-Control", "no-cache")

	if !s.revalidationRequested(r) {
		t.Fatal("with an origin, a client's no-cache must still trigger revalidation")
	}
}

// The regression this guards: with no origin, both revalidation paths end at an
// upstream — the conditional GET directly, and the block-mode path by falling
// through to the miss handler, which would answer NoSuchKey. Honoring the header
// would turn a healthy cached object into a 404.
func TestRevalidationRequested_IgnoredWithoutOrigin(t *testing.T) {
	s := &Service{origin: noOrigin{}}
	r := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	r.Header.Set("Cache-Control", "no-cache")

	if s.revalidationRequested(r) {
		t.Fatal("without an origin, no-cache must be ignored, not acted on")
	}
}

// Services built as literals (tests do this widely) must keep describing the mode
// they were written for rather than panicking on a nil origin.
func TestRevalidationRequested_NilOriginDefaultsToProxy(t *testing.T) {
	s := &Service{}
	r := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	r.Header.Set("Cache-Control", "no-cache")

	if !s.revalidationRequested(r) {
		t.Fatal("a Service with no origin set must behave as the proxying mode")
	}
}

func TestNewForwarder_SelectsOriginlessWhenNoEndpoint(t *testing.T) {
	if _, ok := NewForwarder(nil, "", "auto", 10, nil, nil).(originlessForwarder); !ok {
		t.Fatal("an empty endpoint must select the origin-less forwarder")
	}
	if _, ok := NewForwarder(nil, "https://t3.storage.dev", "auto", 10, nil, nil).(originlessForwarder); ok {
		t.Fatal("a configured endpoint must not select the origin-less forwarder")
	}
}

func TestOriginlessForwarder_ForwardAnswersNoSuchKey(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)

	if err := (originlessForwarder{}).Forward(context.Background(), w, r); err != nil {
		t.Fatalf("Forward: %v", err)
	}
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
	if body := w.Body.String(); !strings.Contains(body, "NoSuchKey") {
		t.Fatalf("body must carry the S3 NoSuchKey code, got %q", body)
	}
}

// A blanket 404 null-object would be tidy and wrong: revalidation reads 404 as
// "deleted upstream" and would invalidate a healthy entry. These paths must fail
// loudly instead, so a wiring mistake surfaces rather than corrupting the cache.
func TestOriginlessForwarder_UpstreamOnlyCallsErrorRatherThanLie(t *testing.T) {
	f := originlessForwarder{}
	ctx := context.Background()

	if _, err := f.DoConditionalGetRequest(ctx, "b", "k", "ak", "sk", "etag", 0, ""); err == nil {
		t.Error("DoConditionalGetRequest must error, never report a 404")
	}
	if _, err := f.DoConditionalHeadRequest(ctx, "b", "k", "ak", "sk", "etag", 0); err == nil {
		t.Error("DoConditionalHeadRequest must error, never report a 404")
	}
	if _, err := f.DoFullObjectRequest(ctx, "b", "k", "ak", "sk"); err == nil {
		t.Error("DoFullObjectRequest must error")
	}
	if _, err := f.DoAnonymousFullObjectRequest(ctx, "b", "k"); err == nil {
		t.Error("DoAnonymousFullObjectRequest must error")
	}
	if _, err := f.DoRequestWithCreds(ctx, httptest.NewRequest(http.MethodGet, "/b/k", nil), "ak", "sk"); err == nil {
		t.Error("DoRequestWithCreds must error")
	}
}

func TestOriginlessForwarder_CaptureReportsNotFound(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)

	capture, err := (originlessForwarder{}).ForwardWithCapture(context.Background(), w, r)
	if err != nil {
		t.Fatalf("ForwardWithCapture: %v", err)
	}
	// Callers gate cache population on a 2xx; reporting the 404 is what makes them
	// skip it, exactly as they would for a genuine upstream 404.
	if capture.StatusCode != http.StatusNotFound {
		t.Fatalf("capture status = %d, want %d", capture.StatusCode, http.StatusNotFound)
	}
}

// Regression: the miss path must be short-circuited before the fetch paths, not
// left to the forwarder. Both fetch paths (range-with-background-fetch and
// broadcast coalescing) are built around an upstream response arriving; a
// forwarder that cannot produce one leaves the broadcaster waiting on a fetch that
// never completes, and the request hangs instead of 404ing. This asserts it
// returns, and returns quickly.
func TestOriginless_MissReturnsNoSuchKeyWithoutHanging(t *testing.T) {
	svc := &Service{
		origin:           noOrigin{},
		cache:            cache.NewDisabledCache(),
		config:           config.NewDefault(),
		broadcastManager: broadcast.NewManager(broadcast.DefaultChannelBuffer),
		forwarder:        originlessForwarder{},
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/never-written", nil)

	done := make(chan error, 1)
	go func() { done <- svc.HandleGetObject(w, r) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("HandleGetObject: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HandleGetObject hung on a miss with no upstream — it must answer NoSuchKey")
	}

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
	if !strings.Contains(w.Body.String(), "NoSuchKey") {
		t.Fatalf("body must carry NoSuchKey, got %q", w.Body.String())
	}
}
