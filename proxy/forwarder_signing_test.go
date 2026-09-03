package proxy

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/tigrisdata/tag/auth"
)

func TestSigningForwarder_InvalidPresignedDateIsMalformedAuth(t *testing.T) {
	store := auth.NewCredentialStore()
	store.AddCredential("access-key", "secret-key")
	fwd := &signingForwarder{
		credStore: store,
		validator: auth.NewRequestValidator(store),
	}
	req := httptest.NewRequest(
		http.MethodGet,
		"/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access-key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=invalid&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=abc",
		nil,
	)

	_, err := fwd.validateRequest(req)
	if !errors.Is(err, auth.ErrInvalidAuthFormat) {
		t.Fatalf("validateRequest() error = %v, want %v", err, auth.ErrInvalidAuthFormat)
	}
}

func TestBuildHeaderSigningPath_StripsPresignedAuthForNonRead(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodPut,
		"/bucket/key?X-Amz-Credential=key%2Fdate%2Fregion%2Fs3%2Faws4_request&X-Amz-Signature=signature&uploadId=upload",
		nil,
	)

	path := buildHeaderSigningPath(req)
	parsed, err := url.Parse(path)
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	if auth.HasQueryAuthentication(&http.Request{URL: parsed}) {
		t.Errorf("buildHeaderSigningPath() retained query authentication: %q", parsed.RawQuery)
	}
	if parsed.Query().Get("uploadId") != "upload" {
		t.Errorf("uploadId = %q, want preserved", parsed.Query().Get("uploadId"))
	}
}

func TestSigningForwarder_RewritesVirtualHostPath(t *testing.T) {
	fwd := &signingForwarder{
		baseForwarder: newBaseForwarder("https://t3.storage.dev", "auto", 10),
	}
	req := httptest.NewRequest(
		http.MethodGet,
		"http://tag.internal/tagcheck/small.bin?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2F20260717%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20260717T144748Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=signature",
		nil,
	)
	req.Host = "example-bucket.t3.tigrisfiles.io"

	upstreamReq, err := fwd.signUpstreamRequest(
		t.Context(),
		req,
		nil,
		"",
		"key",
		"secret",
	)
	if err != nil {
		t.Fatalf("signUpstreamRequest() error = %v", err)
	}
	if upstreamReq.URL.Path != "/example-bucket/tagcheck/small.bin" {
		t.Errorf("path = %q, want path-style upstream bucket and key", upstreamReq.URL.Path)
	}
	if upstreamReq.URL.Host != "t3.storage.dev" {
		t.Errorf("host = %q, want configured upstream", upstreamReq.URL.Host)
	}
}
