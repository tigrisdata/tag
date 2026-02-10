package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/tigrisdata/tag/auth"
)

// transparentForwarder forwards client requests as-is (preserving their
// Authorization header) and adds proxy headers so Tigris validates the
// signature against the original host. Used when TAG acts as a transparent proxy.
//
// DoFullObjectRequest is inherited from baseForwarder (always uses SigV4 signing).
type transparentForwarder struct {
	baseForwarder
	proxySigner      *auth.ProxySigner
	upstreamEndpoint string
}

// buildTransparentRequest creates a forwarded request for transparent proxy mode.
// Copies ALL headers from the original request (including Authorization, X-Amz-Date, etc.)
// and adds the 4 proxy headers. The body is streamed as-is without decoding.
func (f *transparentForwarder) buildTransparentRequest(ctx context.Context, r *http.Request) (*http.Request, error) {
	baseURL, err := url.Parse(f.upstreamEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint: %w", err)
	}

	baseURL.Path = r.URL.Path
	baseURL.RawPath = r.URL.RawPath
	baseURL.RawQuery = r.URL.RawQuery

	fwdReq, err := http.NewRequestWithContext(ctx, r.Method, baseURL.String(), r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Clone ALL headers from the original request (preserving Authorization, X-Amz-Date, etc.)
	// Clone avoids mutating the original request headers when adding proxy headers below.
	fwdReq.Header = r.Header.Clone()

	// Preserve Content-Length from original request
	fwdReq.ContentLength = r.ContentLength

	// Capture original Host before it gets overwritten by the upstream URL
	forwardedHost := r.Host

	// Compute and set the 4 proxy headers
	proxyHeaders := f.proxySigner.ComputeProxyHeaders(forwardedHost, r.Method, r.URL.Path)
	fwdReq.Header.Set("X-Tigris-Forwarded-Host", proxyHeaders.ForwardedHost)
	fwdReq.Header.Set("X-Tigris-Proxy-Access-Key", proxyHeaders.ProxyAccessKey)
	fwdReq.Header.Set("X-Tigris-Proxy-Timestamp", proxyHeaders.ProxyTimestamp)
	fwdReq.Header.Set("X-Tigris-Proxy-Signature", proxyHeaders.ProxySignature)

	return fwdReq, nil
}

// Forward forwards a request to Tigris in transparent proxy mode.
// The client's original signed request is forwarded as-is with proxy headers added.
func (f *transparentForwarder) Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	fwdReq, err := f.buildTransparentRequest(ctx, r)
	if err != nil {
		return err
	}

	return f.executeAndStream(w, fwdReq, r.ContentLength)
}

// ForwardWithCapture forwards request in transparent proxy mode and captures the response.
func (f *transparentForwarder) ForwardWithCapture(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
	fwdReq, err := f.buildTransparentRequest(ctx, r)
	if err != nil {
		return nil, err
	}

	return f.executeAndCapture(w, fwdReq, r.ContentLength)
}

// ValidateAndGetCredentials returns proxy credentials for background operations.
// In transparent mode, there is no client signature validation - the proxy credentials
// (TAG's own Tigris credentials) are returned for use by DoFullObjectRequest.
func (f *transparentForwarder) ValidateAndGetCredentials(r *http.Request) (accessKey, secretKey string, err error) {
	return f.proxySigner.AccessKey(), f.proxySigner.SecretKey(), nil
}

// DoRequestWithCreds executes a request with transparent proxy headers.
// Returns the raw response for streaming. Caller is responsible for closing the response body.
// accessKey and secretKey are unused in transparent mode (the client's original
// Authorization header is preserved as-is), but are accepted to satisfy the
// RequestForwarder interface.
func (f *transparentForwarder) DoRequestWithCreds(ctx context.Context, r *http.Request, accessKey, secretKey string) (*http.Response, error) {
	fwdReq, err := f.buildTransparentRequest(ctx, r)
	if err != nil {
		return nil, err
	}

	return f.executeRequest(fwdReq, r.ContentLength)
}
