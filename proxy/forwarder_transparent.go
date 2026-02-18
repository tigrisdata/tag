package proxy

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/metrics"
)

// signingKeysHeader is the response header containing encrypted derived signing keys.
const signingKeysHeader = "X-Tigris-Proxy-Signing-Keys"

// transparentForwarder forwards client requests as-is (preserving their
// Authorization header) and adds proxy headers so Tigris validates the
// signature against the original host. Used when TAG acts as a transparent proxy.
//
// DoFullObjectRequest is inherited from baseForwarder (always uses SigV4 signing).
type transparentForwarder struct {
	baseForwarder
	proxySigner      *auth.ProxySigner
	upstreamEndpoint string

	// Local auth validation (nil when feature disabled)
	derivedKeyStore *auth.DerivedKeyStore
	validator       *auth.RequestValidator
	keyUnwrapper    *auth.KeyUnwrapper
	authzCache      *auth.AuthzCache
}

// initInterceptor sets the response interceptor on the base forwarder.
// Must be called after all fields are set.
func (f *transparentForwarder) initInterceptor() {
	f.responseInterceptor = f.interceptResponse
}

// interceptResponse is called by base forwarder methods after receiving the
// upstream response but before headers are sent to the client. It extracts
// signing keys from successful responses and revokes authZ on 403s.
func (f *transparentForwarder) interceptResponse(resp *http.Response, originalReq *http.Request) {
	f.learnSigningKeys(resp, originalReq)
	f.learnPublicAccess(resp, originalReq)
	f.handleAuthzRevocation(resp, originalReq)
}

// buildTransparentRequest creates a forwarded request for transparent proxy mode.
// Copies ALL headers from the original request (including Authorization, X-Amz-Date, etc.)
// and adds the 4 proxy headers. The body is streamed as-is without decoding.
//
// For AWS chunked transfer encoding (streaming SigV4), ensures the required
// Content-Encoding: aws-chunked header is present per the S3 spec. Some clients
// (e.g., minio-go) omit this header despite using chunked encoding on the wire.
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

	// Ensure X-Amz-Date is present for Tigris's proxy validation path.
	// Some SDK versions (botocore 1.42+) sign with "date" in SignedHeaders
	// and don't set X-Amz-Date at all. Tigris's proxy code path requires
	// X-Amz-Date, so synthesize it from Date when missing.
	// We never remove Date — it may be in SignedHeaders and required for
	// signature verification.
	if fwdReq.Header.Get("X-Amz-Date") == "" {
		if dateStr := fwdReq.Header.Get("Date"); dateStr != "" {
			if t, err := auth.ParseHTTPDate(dateStr); err == nil {
				fwdReq.Header.Set("X-Amz-Date", t.UTC().Format(auth.TimeFormat))
			}
		}
	}

	// Preserve Content-Length from original request
	fwdReq.ContentLength = r.ContentLength

	// AWS S3 spec requires Content-Encoding: aws-chunked for streaming SigV4
	// uploads. Some clients (e.g., minio-go) send STREAMING-AWS4-HMAC-SHA256-PAYLOAD
	// in X-Amz-Content-Sha256 but omit the Content-Encoding header. Ensure it's
	// present so upstream Tigris can correctly process the chunked body.
	// Only add the header if content-encoding is NOT in the client's SignedHeaders,
	// since mutating a signed header would invalidate the Authorization signature.
	if IsStreamingPayload(fwdReq.Header.Get("X-Amz-Content-Sha256")) && !isContentEncodingSigned(r) {
		ensureAWSChunkedEncoding(fwdReq)
	}

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
// Response interception (signing key learning, header stripping, authZ revocation)
// is handled by the base forwarder's response interceptor.
func (f *transparentForwarder) Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	fwdReq, err := f.buildTransparentRequest(ctx, r)
	if err != nil {
		return err
	}

	return f.executeAndStream(w, fwdReq, r.ContentLength, r)
}

// ForwardWithCapture forwards request in transparent proxy mode and captures the response.
// Response interception (signing key learning, header stripping, authZ revocation)
// is handled by the base forwarder's response interceptor.
func (f *transparentForwarder) ForwardWithCapture(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
	fwdReq, err := f.buildTransparentRequest(ctx, r)
	if err != nil {
		return nil, err
	}

	return f.executeAndCapture(w, fwdReq, r.ContentLength, r)
}

// ValidateAndGetCredentials validates the client's request locally if possible.
// Returns AuthResult to indicate whether the request was locally validated:
//   - AuthValidated: safe to serve from cache; credentials are TAG's proxy credentials
//   - AuthNotValidated with nil error: skip cache, forward to Tigris for validation
//   - AuthNotValidated with error: malformed auth header — return error to client
func (f *transparentForwarder) ValidateAndGetCredentials(r *http.Request) (AuthResult, string, string, error) {
	result, err := f.validateLocally(r)
	if err != nil {
		return AuthNotValidated, "", "", err
	}
	// Always return proxy credentials regardless of auth result.
	// AuthResult controls cache reads (line 88 in get_object.go).
	// Credentials are needed for background cache fetch (DoFullObjectRequest),
	// which always uses TAG's own credentials via SigV4 signing.
	// DoRequestWithCreds in transparent mode ignores these (uses client's auth).
	return result, f.proxySigner.AccessKey(), f.proxySigner.SecretKey(), nil
}

// validateLocally performs local SigV4 validation of the client's request.
func (f *transparentForwarder) validateLocally(r *http.Request) (AuthResult, error) {
	// If local auth is not configured, always treat as validated (legacy behavior)
	if f.validator == nil {
		return AuthValidated, nil
	}

	// Parse auth info from the client's request
	authInfo, err := auth.ParseAuthInfo(r)
	if err != nil {
		// Missing auth (anonymous request) → forward to Tigris for authoritative
		// handling (e.g., public bucket access). Only truly malformed auth headers
		// are rejected as client errors.
		if errors.Is(err, auth.ErrMissingAuth) {
			// Check if this bucket has been previously confirmed as publicly accessible
			bucket, _ := ParseBucketKey(r)
			if bucket != "" && f.authzCache != nil && f.authzCache.IsPublicAuthorized(bucket) {
				metrics.RecordLocalAuthValidation("public_access")
				log.Debug().Str("bucket", bucket).Msg("Local auth: public bucket - serving from cache")
				return AuthValidated, nil
			}
			metrics.RecordLocalAuthValidation("missing_auth")
			return AuthNotValidated, nil
		}
		metrics.RecordLocalAuthValidation("parse_error")
		return AuthNotValidated, mapAuthError(err)
	}

	// Check if we have any signing key for this access key
	if !f.derivedKeyStore.HasKey(authInfo.AccessKey) {
		metrics.RecordLocalAuthValidation("unknown_key")
		log.Debug().Msg("Local auth: unknown key - no signing keys learned for this access key")
		return AuthNotValidated, nil // Unknown key → skip cache, forward to Tigris
	}

	// Validate the SigV4 signature locally
	if _, err := f.validator.ValidateRequest(r); err != nil {
		// Any validation failure (signature mismatch, unknown date/region, key rotation)
		// → skip cache, forward to Tigris to get authoritative decision + fresh keys
		metrics.RecordLocalAuthValidation("signature_mismatch")
		log.Debug().Err(err).Msg("Local auth: signature mismatch")
		return AuthNotValidated, nil
	}

	// Check authorization cache
	bucket, _ := ParseBucketKey(r)
	if !f.authzCache.IsAuthorized(authInfo.AccessKey, bucket) {
		metrics.RecordLocalAuthValidation("authz_expired")
		log.Debug().Str("bucket", bucket).Msg("Local auth: authz expired for bucket")
		return AuthNotValidated, nil // AuthZ expired → forward to Tigris
	}

	metrics.RecordLocalAuthValidation("success")
	log.Debug().Msg("Local auth: validated successfully")
	return AuthValidated, nil
}

// isContentEncodingSigned returns true if "content-encoding" is listed in the
// request's SigV4 SignedHeaders. When it is signed, we must not modify the
// Content-Encoding header because doing so would invalidate the Authorization
// signature. Returns false if the auth header can't be parsed (anonymous or
// malformed requests will be forwarded to Tigris for authoritative handling).
func isContentEncodingSigned(r *http.Request) bool {
	authInfo, err := auth.ParseAuthInfo(r)
	if err != nil {
		return false
	}
	for _, h := range authInfo.SignedHeaders {
		if strings.EqualFold(h, "content-encoding") {
			return true
		}
	}
	return false
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

	return f.executeRequest(fwdReq, r.ContentLength, r)
}

// learnSigningKeys extracts and caches derived signing keys from the Tigris response.
// The signing keys header is always stripped before the response reaches the client.
func (f *transparentForwarder) learnSigningKeys(resp *http.Response, r *http.Request) {
	// Always strip the internal header, even when local auth is disabled.
	headerVal := resp.Header.Get(signingKeysHeader)
	resp.Header.Del(signingKeysHeader)

	if f.keyUnwrapper == nil {
		return
	}

	// Header may be absent on 2xx if feature is disabled on Tigris side, or non-proxy request
	if headerVal == "" || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if headerVal == "" && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Debug().Int("status", resp.StatusCode).Msg("Signing keys header absent from successful response")
		}
		return
	}

	authInfo, err := auth.ParseAuthInfo(r)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to parse auth info for signing key learning")
		return
	}

	entries, err := f.keyUnwrapper.Unwrap(headerVal, authInfo.AccessKey)
	if err != nil {
		log.Warn().Err(err).Str("access_key", authInfo.AccessKey).Msg("Failed to unwrap signing keys")
		return
	}

	for _, entry := range entries {
		keyBytes, err := hex.DecodeString(entry.SigningKey)
		if err != nil {
			log.Warn().Err(err).Str("date", entry.Date).Msg("Failed to decode signing key hex")
			continue
		}
		f.derivedKeyStore.Store(authInfo.AccessKey, entry.Date, entry.Region, keyBytes)
	}

	bucket, _ := ParseBucketKey(r)
	f.authzCache.Grant(authInfo.AccessKey, bucket)

	log.Debug().
		Str("bucket", bucket).
		Int("keys_learned", len(entries)).
		Int("store_size", f.derivedKeyStore.Count()).
		Msg("Signing keys learned successfully")

	metrics.ProxySigningKeysReceived.Inc()
	metrics.DerivedKeyStoreSize.Set(float64(f.derivedKeyStore.Count()))
	metrics.AuthzCacheSize.Set(float64(f.authzCache.Count()))
}

// learnPublicAccess grants public bucket authorization when an anonymous request
// receives a successful response from Tigris. Uses dedicated public access methods
// on AuthzCache to track which buckets are publicly accessible.
func (f *transparentForwarder) learnPublicAccess(resp *http.Response, r *http.Request) {
	if f.authzCache == nil {
		return
	}

	// Only learn from anonymous requests (no header or query param auth)
	if r.Header.Get("Authorization") != "" || r.URL.Query().Get("X-Amz-Credential") != "" {
		return
	}

	// Only learn from successful responses
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return
	}

	bucket, _ := ParseBucketKey(r)
	if bucket == "" {
		return
	}

	f.authzCache.GrantPublic(bucket)
	log.Debug().Str("bucket", bucket).Msg("Public bucket access learned")
	metrics.AuthzCacheSize.Set(float64(f.authzCache.Count()))
}

// handleAuthzRevocation revokes authorization when Tigris returns 403.
func (f *transparentForwarder) handleAuthzRevocation(resp *http.Response, r *http.Request) {
	if f.authzCache == nil || resp.StatusCode != http.StatusForbidden {
		return
	}

	authInfo, err := auth.ParseAuthInfo(r)
	if err != nil {
		// Anonymous request receiving 403 — revoke public access
		if errors.Is(err, auth.ErrMissingAuth) {
			bucket, _ := ParseBucketKey(r)
			if bucket != "" {
				f.authzCache.RevokePublic(bucket)
				log.Debug().Str("bucket", bucket).Msg("Public bucket access revoked on 403")
			}
		}
		return
	}

	bucket, _ := ParseBucketKey(r)
	f.authzCache.Revoke(authInfo.AccessKey, bucket)
}
