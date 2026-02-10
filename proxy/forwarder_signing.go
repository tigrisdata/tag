package proxy

import (
	"context"
	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
)

// signingForwarder validates incoming request signatures and re-signs requests
// before forwarding to upstream Tigris. This is the default mode where TAG
// acts as a credential-translating proxy.
//
// DoFullObjectRequest is inherited from baseForwarder (always uses SigV4 signing).
type signingForwarder struct {
	baseForwarder
	credStore *auth.CredentialStore
	validator *auth.RequestValidator
}

// Forward forwards a request to Tigris and writes the response to the client.
// Validates the incoming request signature, re-signs with upstream credentials,
// and streams the response back. If the request uses AWS chunked transfer encoding
// (streaming SigV4), the body is decoded on-the-fly and forwarded as UNSIGNED-PAYLOAD.
func (f *signingForwarder) Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	// Decode AWS chunked encoding if present, otherwise pass through unchanged
	body, bodyHash, contentLength, chunked := decodeChunkedIfNeeded(r)

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
	prepareForwardedRequest(fwdReq, contentLength, chunked)

	return f.executeAndStream(w, fwdReq, contentLength)
}

// ForwardWithCapture forwards request and captures response for caching.
// Validates and re-signs like Forward, but also captures the response body
// for caching while streaming to the client.
func (f *signingForwarder) ForwardWithCapture(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
	// Decode AWS chunked encoding if present, otherwise pass through unchanged
	body, bodyHash, contentLength, chunked := decodeChunkedIfNeeded(r)

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
	prepareForwardedRequest(fwdReq, contentLength, chunked)

	return f.executeAndCapture(w, fwdReq, contentLength)
}

// ValidateAndGetCredentials validates the request signature and returns credentials.
// This is used to validate credentials before joining a broadcast.
func (f *signingForwarder) ValidateAndGetCredentials(r *http.Request) (accessKey, secretKey string, err error) {
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
func (f *signingForwarder) DoRequestWithCreds(ctx context.Context, r *http.Request, accessKey, secretKey string) (*http.Response, error) {
	// Decode AWS chunked encoding if present, otherwise pass through unchanged
	body, bodyHash, contentLength, chunked := decodeChunkedIfNeeded(r)

	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path = path + "?" + r.URL.RawQuery
	}

	fwdReq, err := f.signer.SignRequest(ctx, r.Method, path, body, bodyHash, accessKey, secretKey, r.Header)
	if err != nil {
		return nil, err
	}
	prepareForwardedRequest(fwdReq, contentLength, chunked)

	return f.executeRequest(fwdReq, contentLength)
}
