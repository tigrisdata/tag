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

// Forwarder handles forwarding requests to the upstream Tigris server.
type Forwarder struct {
	credStore  *auth.CredentialStore
	validator  *auth.RequestValidator
	signer     *auth.RequestSigner
	httpClient *http.Client
}

// NewForwarder creates a new forwarder with configurable HTTP connection pool.
func NewForwarder(credStore *auth.CredentialStore, tigrisEndpoint, region string, maxIdleConnsPerHost int) *Forwarder {
	if maxIdleConnsPerHost <= 0 {
		maxIdleConnsPerHost = 100 // Default
	}

	return &Forwarder{
		credStore: credStore,
		validator: auth.NewRequestValidator(credStore),
		signer:    auth.NewRequestSigner(tigrisEndpoint, region),
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

// Forward forwards a request to Tigris and writes the response to the client.
// Uses zero-copy streaming - the X-Amz-Content-Sha256 header is required (all AWS SDKs set this).
// If the request uses AWS chunked transfer encoding (streaming SigV4), the body
// is decoded on-the-fly and forwarded as UNSIGNED-PAYLOAD.
func (f *Forwarder) Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	// Decode AWS chunked encoding if present, otherwise pass through unchanged
	body, bodyHash, contentLength := decodeChunkedIfNeeded(r)

	// Validate incoming request signature
	accessKey, err := f.validator.ValidateRequest(r)
	if err != nil {
		log.Warn().Err(err).Str("path", r.URL.Path).Msg("Request signature validation failed")
		return mapAuthError(err)
	}

	// Look up secret key from credential store
	secretKey, err := f.credStore.GetSecretKey(accessKey)
	if err != nil {
		return mapAuthError(err)
	}

	// Build the path with query string
	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path = path + "?" + r.URL.RawQuery
	}

	// Create signed request (passes body hash, streams body directly)
	fwdReq, err := f.signer.SignRequest(ctx, r.Method, path, body, bodyHash, accessKey, secretKey, r.Header)
	if err != nil {
		return err
	}

	// Set content length for the forwarded request
	if contentLength > 0 {
		fwdReq.ContentLength = contentLength
	}

	// Strip chunked-encoding headers that don't apply to the decoded body
	fwdReq.Header.Del("X-Amz-Decoded-Content-Length")
	if fwdReq.Header.Get("Content-Encoding") == "aws-chunked" {
		fwdReq.Header.Del("Content-Encoding")
	}

	// Track bytes in from request body
	if contentLength > 0 {
		metrics.BytesTransferred.WithLabelValues("in").Add(float64(contentLength))
	}

	// Forward to Tigris
	upstreamStart := time.Now()
	resp, err := f.httpClient.Do(fwdReq)
	metrics.RecordUpstreamRequest(r.Method, time.Since(upstreamStart).Seconds(), err)
	if err != nil {
		log.Error().Err(err).Str("method", r.Method).Str("path", path).Msg("Failed to forward request")
		return err
	}
	defer resp.Body.Close()

	// Stream response back to client
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	n, copyErr := io.Copy(w, resp.Body)
	if copyErr != nil {
		log.Warn().Err(copyErr).Msg("Failed to copy response body to client")
	}
	// Track bytes out from response body
	metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))

	return nil
}

// ForwardWithCapture forwards request and captures response for caching.
// Uses zero-copy streaming for the request body (X-Amz-Content-Sha256 header is required).
// The response body is captured for caching while streaming to the client.
// If the request uses AWS chunked transfer encoding, the body is decoded on-the-fly.
func (f *Forwarder) ForwardWithCapture(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
	// Decode AWS chunked encoding if present, otherwise pass through unchanged
	body, bodyHash, contentLength := decodeChunkedIfNeeded(r)

	// Validate incoming request signature
	accessKey, err := f.validator.ValidateRequest(r)
	if err != nil {
		log.Warn().Err(err).Str("path", r.URL.Path).Msg("Request signature validation failed")
		return nil, mapAuthError(err)
	}

	// Look up secret key
	secretKey, err := f.credStore.GetSecretKey(accessKey)
	if err != nil {
		return nil, mapAuthError(err)
	}

	// Build the path with query string
	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path = path + "?" + r.URL.RawQuery
	}

	// Create signed request (passes body hash, streams body directly)
	fwdReq, err := f.signer.SignRequest(ctx, r.Method, path, body, bodyHash, accessKey, secretKey, r.Header)
	if err != nil {
		return nil, err
	}

	// Set content length for the forwarded request
	if contentLength > 0 {
		fwdReq.ContentLength = contentLength
	}

	// Strip chunked-encoding headers that don't apply to the decoded body
	fwdReq.Header.Del("X-Amz-Decoded-Content-Length")
	if fwdReq.Header.Get("Content-Encoding") == "aws-chunked" {
		fwdReq.Header.Del("Content-Encoding")
	}

	// Track bytes in from request body
	if contentLength > 0 {
		metrics.BytesTransferred.WithLabelValues("in").Add(float64(contentLength))
	}

	// Forward to Tigris
	upstreamStart := time.Now()
	resp, err := f.httpClient.Do(fwdReq)
	metrics.RecordUpstreamRequest(r.Method, time.Since(upstreamStart).Seconds(), err)
	if err != nil {
		log.Error().Err(err).Str("method", r.Method).Str("path", path).Msg("Failed to forward request")
		return nil, err
	}
	defer resp.Body.Close()

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

// ValidateAndGetCredentials validates the request signature and returns credentials.
// This is used to validate credentials before joining a broadcast.
func (f *Forwarder) ValidateAndGetCredentials(r *http.Request) (accessKey, secretKey string, err error) {
	accessKey, err = f.validator.ValidateRequest(r)
	if err != nil {
		log.Warn().Err(err).Str("path", r.URL.Path).Msg("Request signature validation failed")
		return "", "", mapAuthError(err)
	}

	secretKey, err = f.credStore.GetSecretKey(accessKey)
	if err != nil {
		return "", "", mapAuthError(err)
	}

	return accessKey, secretKey, nil
}

// DoRequestWithCreds executes a request with pre-validated credentials.
// Returns the raw response for streaming. Caller is responsible for closing the response body.
// If the request uses AWS chunked transfer encoding, the body is decoded on-the-fly.
func (f *Forwarder) DoRequestWithCreds(ctx context.Context, r *http.Request, accessKey, secretKey string) (*http.Response, error) {
	// Decode AWS chunked encoding if present, otherwise pass through unchanged
	body, bodyHash, contentLength := decodeChunkedIfNeeded(r)

	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path = path + "?" + r.URL.RawQuery
	}

	fwdReq, err := f.signer.SignRequest(ctx, r.Method, path, body, bodyHash, accessKey, secretKey, r.Header)
	if err != nil {
		return nil, err
	}

	if contentLength > 0 {
		fwdReq.ContentLength = contentLength
	}

	// Strip chunked-encoding headers that don't apply to the decoded body
	fwdReq.Header.Del("X-Amz-Decoded-Content-Length")
	if fwdReq.Header.Get("Content-Encoding") == "aws-chunked" {
		fwdReq.Header.Del("Content-Encoding")
	}

	// Track bytes in from request body
	if contentLength > 0 {
		metrics.BytesTransferred.WithLabelValues("in").Add(float64(contentLength))
	}

	upstreamStart := time.Now()
	resp, err := f.httpClient.Do(fwdReq)
	metrics.RecordUpstreamRequest(r.Method, time.Since(upstreamStart).Seconds(), err)
	if err != nil {
		log.Error().Err(err).Str("method", r.Method).Str("path", path).Msg("Failed to forward request")
		return nil, err
	}

	return resp, nil
}

// DoFullObjectRequest executes a full object GET request (no Range header).
// Used for background cache population after a Range request cache miss.
// Caller is responsible for closing the response body.
func (f *Forwarder) DoFullObjectRequest(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error) {
	path := "/" + bucket + "/" + key

	// Create request without Range header - just a simple GET for the full object
	fwdReq, err := f.signer.SignRequest(ctx, "GET", path, nil, "UNSIGNED-PAYLOAD", accessKey, secretKey, nil)
	if err != nil {
		return nil, err
	}

	upstreamStart := time.Now()
	resp, err := f.httpClient.Do(fwdReq)
	metrics.RecordUpstreamRequest("GET", time.Since(upstreamStart).Seconds(), err)
	if err != nil {
		log.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("Background fetch failed")
		return nil, err
	}

	return resp, nil
}
