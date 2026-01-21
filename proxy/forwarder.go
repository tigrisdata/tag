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

	// Check for specific auth errors
	if errors.Is(err, auth.ErrSignatureMismatch) {
		return &AuthError{Code: ErrCodeSignatureMismatch, Err: err}
	}
	if errors.Is(err, auth.ErrUnknownAccessKey) {
		return &AuthError{Code: ErrCodeInvalidAccessKey, Err: err}
	}
	if errors.Is(err, auth.ErrExpiredRequest) {
		return &AuthError{Code: ErrCodeExpiredRequest, Err: err}
	}
	if errors.Is(err, auth.ErrMissingAuth) || errors.Is(err, auth.ErrInvalidAuthFormat) ||
		errors.Is(err, auth.ErrUnsupportedAuthScheme) || errors.Is(err, auth.ErrMissingContentHash) {
		return &AuthError{Code: ErrCodeMalformedAuth, Err: err}
	}

	// Default to internal error
	return &AuthError{Code: ErrCodeInternal, Err: err}
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

// NewForwarder creates a new forwarder.
func NewForwarder(credStore *auth.CredentialStore, tigrisEndpoint, region string) *Forwarder {
	return &Forwarder{
		credStore: credStore,
		validator: auth.NewRequestValidator(credStore),
		signer:    auth.NewRequestSigner(tigrisEndpoint, region),
		httpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
			Timeout: 5 * time.Minute,
		},
	}
}

// Forward forwards a request to Tigris and writes the response to the client.
// Uses zero-copy streaming - the X-Amz-Content-Sha256 header is required (all AWS SDKs set this).
func (f *Forwarder) Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	// Get body hash from request header (required for all AWS SDK clients)
	bodyHash := r.Header.Get("X-Amz-Content-Sha256")

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
	fwdReq, err := f.signer.SignRequest(ctx, r.Method, path, r.Body, bodyHash, accessKey, secretKey, r.Header)
	if err != nil {
		return err
	}

	// Set content length if known (needed for streaming)
	if r.ContentLength > 0 {
		fwdReq.ContentLength = r.ContentLength
	}

	// Forward to Tigris
	resp, err := f.httpClient.Do(fwdReq)
	if err != nil {
		log.Error().Err(err).Str("method", r.Method).Str("path", path).Msg("Failed to forward request")
		return err
	}
	defer resp.Body.Close()

	// Stream response back to client
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Warn().Err(err).Msg("Failed to copy response body to client")
	}

	return nil
}

// ForwardWithCapture forwards request and captures response for caching.
// Uses zero-copy streaming for the request body (X-Amz-Content-Sha256 header is required).
// The response body is captured for caching while streaming to the client.
func (f *Forwarder) ForwardWithCapture(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
	// Get body hash from request header (required for all AWS SDK clients)
	bodyHash := r.Header.Get("X-Amz-Content-Sha256")

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
	fwdReq, err := f.signer.SignRequest(ctx, r.Method, path, r.Body, bodyHash, accessKey, secretKey, r.Header)
	if err != nil {
		return nil, err
	}

	// Set content length if known
	if r.ContentLength > 0 {
		fwdReq.ContentLength = r.ContentLength
	}

	// Forward to Tigris
	resp, err := f.httpClient.Do(fwdReq)
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
	}

	return capture, nil
}

// ResponseCapture holds captured response data.
type ResponseCapture struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
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
func (f *Forwarder) DoRequestWithCreds(ctx context.Context, r *http.Request, accessKey, secretKey string) (*http.Response, error) {
	bodyHash := r.Header.Get("X-Amz-Content-Sha256")

	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path = path + "?" + r.URL.RawQuery
	}

	fwdReq, err := f.signer.SignRequest(ctx, r.Method, path, r.Body, bodyHash, accessKey, secretKey, r.Header)
	if err != nil {
		return nil, err
	}

	if r.ContentLength > 0 {
		fwdReq.ContentLength = r.ContentLength
	}

	resp, err := f.httpClient.Do(fwdReq)
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

	resp, err := f.httpClient.Do(fwdReq)
	if err != nil {
		log.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("Background fetch failed")
		return nil, err
	}

	return resp, nil
}
