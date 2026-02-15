// Package proxy provides HTTP proxying and caching logic for TAG.
package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/metrics"
)

// AuthResult represents the outcome of authentication validation.
type AuthResult int

const (
	// AuthValidated means the request was locally validated — safe to serve from cache.
	AuthValidated AuthResult = iota
	// AuthNotValidated means local validation was skipped (unknown key, signature
	// mismatch, expired authZ) — skip cache, forward to Tigris.
	AuthNotValidated
)

// RequestForwarder is the interface for forwarding requests to the upstream Tigris server.
// Two implementations exist: signingForwarder (validate + re-sign) and
// transparentForwarder (clone headers + proxy headers).
type RequestForwarder interface {
	// Forward forwards a request to Tigris and writes the response to the client.
	Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error

	// ForwardWithCapture forwards request and captures response for caching.
	ForwardWithCapture(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error)

	// ValidateAndGetCredentials validates the request signature and returns credentials.
	// Returns AuthResult to indicate whether the request was locally validated.
	// On AuthValidated, the returned credentials can be used for cache-miss forwarding.
	// On AuthNotValidated, skip cache and forward to Tigris for authoritative validation.
	ValidateAndGetCredentials(r *http.Request) (result AuthResult, accessKey, secretKey string, err error)

	// DoRequestWithCreds executes a request with pre-validated credentials.
	// Returns the raw response for streaming. Caller is responsible for closing the response body.
	DoRequestWithCreds(ctx context.Context, r *http.Request, accessKey, secretKey string) (*http.Response, error)

	// DoFullObjectRequest executes a full object GET request (no Range header).
	// Used for background cache population after a Range request cache miss.
	// Caller is responsible for closing the response body.
	DoFullObjectRequest(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error)
}

// AuthErrorCode represents the type of authentication error.
type AuthErrorCode int

const (
	// ErrCodeSignatureMismatch indicates the signature doesn't match.
	ErrCodeSignatureMismatch AuthErrorCode = iota
	// ErrCodeInvalidAccessKey indicates an unknown access key.
	ErrCodeInvalidAccessKey
	// ErrCodeExpiredRequest indicates the request has expired.
	ErrCodeExpiredRequest
	// ErrCodeMalformedAuth indicates a malformed authorization header.
	ErrCodeMalformedAuth
	// ErrCodeInternal indicates an internal error during auth processing.
	ErrCodeInternal
)

// AuthError represents an authentication error with a specific code.
type AuthError struct {
	Code AuthErrorCode
	Err  error
}

func (e *AuthError) Error() string {
	return e.Err.Error()
}

func (e *AuthError) Unwrap() error {
	return e.Err
}

// mapAuthError maps auth package errors to AuthError with appropriate code.
func mapAuthError(err error) *AuthError {
	if err == nil {
		return nil
	}

	var reason string
	var code AuthErrorCode

	// Check for specific auth errors
	if errors.Is(err, auth.ErrSignatureMismatch) {
		reason = "signature_mismatch"
		code = ErrCodeSignatureMismatch
	} else if errors.Is(err, auth.ErrUnknownAccessKey) {
		reason = "unknown_access_key"
		code = ErrCodeInvalidAccessKey
	} else if errors.Is(err, auth.ErrExpiredRequest) {
		reason = "expired_request"
		code = ErrCodeExpiredRequest
	} else if errors.Is(err, auth.ErrMissingAuth) || errors.Is(err, auth.ErrInvalidAuthFormat) ||
		errors.Is(err, auth.ErrUnsupportedAuthScheme) || errors.Is(err, auth.ErrMissingContentHash) {
		reason = "malformed_auth"
		code = ErrCodeMalformedAuth
	} else {
		// Default to internal error
		reason = "internal_error"
		code = ErrCodeInternal
	}

	// Record the auth failure metric
	metrics.RecordAuthFailure(reason)

	return &AuthError{Code: code, Err: err}
}

// IsAuthError checks if the error is an AuthError and returns it.
func IsAuthError(err error) (*AuthError, bool) {
	var authErr *AuthError
	if errors.As(err, &authErr) {
		return authErr, true
	}
	return nil, false
}

// Compile-time interface satisfaction checks.
var (
	_ RequestForwarder = (*signingForwarder)(nil)
	_ RequestForwarder = (*transparentForwarder)(nil)
)

// ResponseInterceptor is called after receiving the upstream response but before
// headers are sent to the client. Used by transparentForwarder to extract signing
// keys and strip internal headers from the response.
// The originalReq parameter is the original client request (needed for parsing auth info).
type ResponseInterceptor func(resp *http.Response, originalReq *http.Request)

// baseForwarder contains shared HTTP execution logic used by both
// signingForwarder and transparentForwarder.
type baseForwarder struct {
	signer              *auth.RequestSigner // Both modes need this for DoFullObjectRequest
	httpClient          *http.Client
	responseInterceptor ResponseInterceptor // Optional: called before headers are sent to client
}

// newBaseForwarder creates the shared base with HTTP client and signer.
func newBaseForwarder(tigrisEndpoint, region string, maxIdleConnsPerHost int) baseForwarder {
	if maxIdleConnsPerHost <= 0 {
		maxIdleConnsPerHost = 100 // Default
	}

	return baseForwarder{
		signer: auth.NewRequestSigner(tigrisEndpoint, region),
		httpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        maxIdleConnsPerHost,
				MaxIdleConnsPerHost: maxIdleConnsPerHost,
				IdleConnTimeout:     90 * time.Second,
			},
			Timeout: 5 * time.Minute,
		},
	}
}

// executeAndStream executes the request and streams the response to the client.
// originalReq is the original client request, passed to the response interceptor
// for parsing auth info. It can be nil if no interceptor is set.
func (b *baseForwarder) executeAndStream(w http.ResponseWriter, fwdReq *http.Request, inContentLength int64, originalReq *http.Request) error {
	if inContentLength > 0 {
		metrics.BytesTransferred.WithLabelValues("in").Add(float64(inContentLength))
	}

	upstreamStart := time.Now()
	resp, err := b.httpClient.Do(fwdReq)
	metrics.RecordUpstreamRequest(fwdReq.Method, time.Since(upstreamStart).Seconds(), err)
	if err != nil {
		log.Error().Err(err).Str("method", fwdReq.Method).Str("path", fwdReq.URL.Path).Msg("Failed to forward request")
		return err
	}
	defer resp.Body.Close()

	// Run response interceptor before sending headers to client
	if b.responseInterceptor != nil {
		b.responseInterceptor(resp, originalReq)
	}

	// Stream response back to client
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	n, copyErr := io.Copy(w, resp.Body)
	if copyErr != nil {
		log.Warn().Err(copyErr).Msg("Failed to copy response body to client")
	}
	metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))

	return nil
}

// executeAndCapture executes the request, streams to client, and captures the response.
// originalReq is the original client request, passed to the response interceptor
// for parsing auth info. It can be nil if no interceptor is set.
func (b *baseForwarder) executeAndCapture(w http.ResponseWriter, fwdReq *http.Request, inContentLength int64, originalReq *http.Request) (*ResponseCapture, error) {
	if inContentLength > 0 {
		metrics.BytesTransferred.WithLabelValues("in").Add(float64(inContentLength))
	}

	upstreamStart := time.Now()
	resp, err := b.httpClient.Do(fwdReq)
	metrics.RecordUpstreamRequest(fwdReq.Method, time.Since(upstreamStart).Seconds(), err)
	if err != nil {
		log.Error().Err(err).Str("method", fwdReq.Method).Str("path", fwdReq.URL.Path).Msg("Failed to forward request")
		return nil, err
	}
	defer resp.Body.Close()

	// Run response interceptor before sending headers to client
	if b.responseInterceptor != nil {
		b.responseInterceptor(resp, originalReq)
	}

	// Capture response
	capture := &ResponseCapture{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header.Clone(),
	}

	// Copy headers to response writer
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	// Capture body while streaming to client
	var readErr error
	capture.Body, readErr = io.ReadAll(io.TeeReader(resp.Body, w))
	if readErr != nil {
		log.Warn().Err(readErr).Msg("Failed to fully capture response body")
		capture.Complete = false
	} else {
		capture.Complete = true
	}

	// Track bytes out from response body
	metrics.BytesTransferred.WithLabelValues("out").Add(float64(len(capture.Body)))

	return capture, nil
}

// executeRequest executes the request and returns the raw response.
// Caller is responsible for closing the response body.
// originalReq is the original client request, passed to the response interceptor
// for parsing auth info. It can be nil if no interceptor is set.
func (b *baseForwarder) executeRequest(fwdReq *http.Request, inContentLength int64, originalReq *http.Request) (*http.Response, error) {
	if inContentLength > 0 {
		metrics.BytesTransferred.WithLabelValues("in").Add(float64(inContentLength))
	}

	upstreamStart := time.Now()
	resp, err := b.httpClient.Do(fwdReq)
	metrics.RecordUpstreamRequest(fwdReq.Method, time.Since(upstreamStart).Seconds(), err)
	if err != nil {
		log.Error().Err(err).Str("method", fwdReq.Method).Str("path", fwdReq.URL.Path).Msg("Failed to forward request")
		return nil, err
	}

	// Run response interceptor (e.g., signing key learning, header stripping)
	if b.responseInterceptor != nil {
		b.responseInterceptor(resp, originalReq)
	}

	return resp, nil
}

// DoFullObjectRequest executes a full object GET request (no Range header).
// Used for background cache population after a Range request cache miss.
// Caller is responsible for closing the response body.
//
// This always uses standard SigV4 signing because it is a synthetic request
// initiated by TAG itself, not a forwarded client request. TAG's credentials
// (from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY) are valid Tigris credentials
// that can sign requests directly.
func (b *baseForwarder) DoFullObjectRequest(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error) {
	path := "/" + bucket + "/" + key

	// Create request without Range header - just a simple GET for the full object
	fwdReq, err := b.signer.SignRequest(ctx, "GET", path, nil, "UNSIGNED-PAYLOAD", accessKey, secretKey, nil)
	if err != nil {
		return nil, err
	}

	upstreamStart := time.Now()
	resp, err := b.httpClient.Do(fwdReq)
	metrics.RecordUpstreamRequest("GET", time.Since(upstreamStart).Seconds(), err)
	if err != nil {
		log.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("Background fetch failed")
		return nil, err
	}

	return resp, nil
}

// LocalAuthConfig holds components for local SigV4 validation in transparent proxy mode.
type LocalAuthConfig struct {
	DerivedKeyStore *auth.DerivedKeyStore
	Validator       *auth.RequestValidator
	KeyUnwrapper    *auth.KeyUnwrapper
	AuthzCache      *auth.AuthzCache
}

// NewForwarder creates a new forwarder with configurable HTTP connection pool.
// If proxySigner is non-nil, transparent proxy mode is enabled.
// If localAuth is non-nil, local SigV4 validation is enabled for cache hits.
func NewForwarder(credStore *auth.CredentialStore, tigrisEndpoint, region string, maxIdleConnsPerHost int, proxySigner *auth.ProxySigner, localAuth *LocalAuthConfig) RequestForwarder {
	base := newBaseForwarder(tigrisEndpoint, region, maxIdleConnsPerHost)

	if proxySigner != nil {
		f := &transparentForwarder{
			baseForwarder:    base,
			proxySigner:      proxySigner,
			upstreamEndpoint: strings.TrimSuffix(tigrisEndpoint, "/"),
		}
		if localAuth != nil {
			f.derivedKeyStore = localAuth.DerivedKeyStore
			f.validator = localAuth.Validator
			f.keyUnwrapper = localAuth.KeyUnwrapper
			f.authzCache = localAuth.AuthzCache
		}
		f.initInterceptor()
		return f
	}

	return &signingForwarder{
		baseForwarder: base,
		credStore:     credStore,
		validator:     auth.NewRequestValidator(credStore),
	}
}

// prepareForwardedRequest sets Content-Length on the forwarded request and strips
// AWS chunked-encoding headers when the request body was decoded from chunked format.
func prepareForwardedRequest(fwdReq *http.Request, contentLength int64, chunked bool) {
	if chunked {
		fwdReq.ContentLength = contentLength
		fwdReq.Header.Del("X-Amz-Decoded-Content-Length")
		stripAWSChunkedEncoding(fwdReq)
		// When decoded content length is 0, set Body to http.NoBody so Go's
		// HTTP transport sends "Content-Length: 0" instead of using
		// "Transfer-Encoding: chunked" (which happens when ContentLength == 0
		// but Body is a non-nil reader — Go's outgoingLength returns -1).
		if contentLength == 0 {
			fwdReq.Body = http.NoBody
		}
	} else if contentLength > 0 {
		fwdReq.ContentLength = contentLength
	}
}

// stripAWSChunkedEncoding removes "aws-chunked" from the Content-Encoding header.
// AWS S3 allows combined values like "aws-chunked,gzip". After decoding the chunked
// layer, we strip only the aws-chunked token and preserve any remaining encodings.
func stripAWSChunkedEncoding(req *http.Request) {
	ce := req.Header.Get("Content-Encoding")
	if ce == "" {
		return
	}

	var remaining []string
	for _, part := range strings.Split(ce, ",") {
		if strings.TrimSpace(part) != "aws-chunked" {
			remaining = append(remaining, strings.TrimSpace(part))
		}
	}

	if len(remaining) == 0 {
		req.Header.Del("Content-Encoding")
	} else {
		req.Header.Set("Content-Encoding", strings.Join(remaining, ","))
	}
}

// ensureAWSChunkedEncoding adds "aws-chunked" to the Content-Encoding header
// if not already present. Some S3 clients (e.g., minio-go) use streaming SigV4
// (X-Amz-Content-Sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD) but omit the
// required Content-Encoding: aws-chunked header. This ensures the upstream
// server can correctly identify and process the chunked body.
func ensureAWSChunkedEncoding(req *http.Request) {
	ce := req.Header.Get("Content-Encoding")
	if ce == "" {
		req.Header.Set("Content-Encoding", "aws-chunked")
		return
	}

	for _, part := range strings.Split(ce, ",") {
		if strings.EqualFold(strings.TrimSpace(part), "aws-chunked") {
			return // Already present
		}
	}

	// Prepend aws-chunked (it's the outermost encoding layer)
	req.Header.Set("Content-Encoding", "aws-chunked,"+ce)
}

// ResponseCapture holds captured response data.
type ResponseCapture struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
	Complete   bool // True if body was fully captured without errors
}

// ContentLength returns the content length from headers or body length.
func (r *ResponseCapture) ContentLength() int64 {
	return int64(len(r.Body))
}

// copyHeaders copies headers from src to dst.
// For x-amz-meta-* headers, the key is lowercased to match S3 convention,
// since Go's http.Header canonicalizes keys (e.g., x-amz-meta-foo → X-Amz-Meta-Foo).
func copyHeaders(dst, src http.Header) {
	for k, v := range src {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-amz-meta-") {
			// Use lowercase for metadata headers per S3 convention
			dst[lower] = v
		} else {
			dst[k] = v
		}
	}
}
