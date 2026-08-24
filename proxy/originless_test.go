package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tigrisdata/tag/config"
)

func TestNewForwarder_SelectsOriginlessWhenNoEndpoint(t *testing.T) {
	if _, ok := NewForwarder(nil, "", "auto", 10, nil, nil).(originlessForwarder); !ok {
		t.Fatal("an empty endpoint must select the origin-less forwarder")
	}
	if _, ok := NewForwarder(nil, "https://t3.storage.dev", "auto", 10, nil, nil).(originlessForwarder); ok {
		t.Fatal("a configured endpoint must not select the origin-less forwarder")
	}
}

// The mode is expressed at the router, so no request should reach this forwarder
// at all. If one does, the operation cannot be named — answer NotImplemented, and
// never fabricate an object-level response.
func TestOriginlessForwarder_ForwardIsA501Backstop(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)

	if err := (originlessForwarder{}).Forward(context.Background(), w, r); err != nil {
		t.Fatalf("Forward: %v", err)
	}
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNotImplemented)
	}
	if !strings.Contains(w.Body.String(), "NotImplemented") {
		t.Fatalf("body must carry NotImplemented, got %q", w.Body.String())
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

// The mode is derived twice — the forwarder from its endpoint argument, the router
// from cfg — and the mismatched pairings fail in opposite directions, one of them
// silently (origin-less routes with a real forwarder would fetch missing blocks
// from an upstream the mode promises not to have). Construction must refuse the
// incoherent pair.
func TestNewService_RefusesForwarderConfigMismatch(t *testing.T) {
	proxyCfg := config.NewDefault() // has an origin
	originlessFwd := NewForwarder(nil, "", "auto", 10, nil, nil)

	defer func() {
		if recover() == nil {
			t.Fatal("NewService must panic when the forwarder and config disagree about the origin")
		}
	}()
	NewService(originlessFwd, nil, proxyCfg)
}

func TestServiceOriginless_DerivedFromConfig(t *testing.T) {
	cfg := config.NewDefault()
	if (&Service{config: cfg}).Originless() {
		t.Fatal("a config with an endpoint must not be origin-less")
	}
	cfg2 := config.NewDefault()
	cfg2.Upstream.Disabled = true
	cfg2.Upstream.Endpoint = ""
	if !(&Service{config: cfg2}).Originless() {
		t.Fatal("a config without an endpoint must be origin-less")
	}
	if (&Service{}).Originless() {
		t.Fatal("a Service without config must default to the proxying mode")
	}
}
