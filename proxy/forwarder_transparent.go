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
	credentialStore *auth.CredentialStore
	credentialAuth  *auth.RequestValidator
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
	f.handleAuthzRevocation(resp, originalReq)
}

// shallowHeaderCopy gives the forwarded request its own header map while
// reusing the immutable value slices owned by the client request. Header.Set
// and Header.Del replace or remove entries, so the values for headers changed
// by this forwarder do not alias the inbound map. The unchanged slices are
// read-only for the lifetime of the outbound request, as required by the
// net/http RoundTripper contract.
func shallowHeaderCopy(header http.Header) http.Header {
	if header == nil {
		return make(http.Header)
	}

	copied := make(http.Header, len(header)+4)
	for key, values := range header {
		if values != nil {
			// Keep the data borrowed but prevent an append on the forwarded
			// entry from reusing the inbound slice's spare capacity.
			values = values[:len(values):len(values)]
		}
		copied[key] = values
	}
	return copied
}

// buildTransparentRequest creates a forwarded request for transparent proxy mode.
// Copies the header map while borrowing unchanged header-value slices. The body
// is streamed as-is without decoding. Headers changed for the upstream request
// remain private to the forwarded request.
//
// For AWS chunked transfer encoding (streaming SigV4), ensures the required
// Content-Encoding: aws-chunked header is present per the S3 spec. Some clients
// (e.g., minio-go) omit this header despite using chunked encoding on the wire.
func (f *transparentForwarder) buildTransparentRequest(ctx context.Context, r *http.Request) (*http.Request, error) {
	bucket, _ := ParseBucketKey(r)

	// Use vhost-style addressing only for anonymous requests (no Authorization header).
	// Tigris requires vhost-style for anonymous access to public buckets.
	// Authenticated requests must keep path-style because the client's SigV4 signature
	// was computed against the path-style canonical URI — changing it would break validation.
	isAnonymous := hasNoAuthCredentials(r)
	var endpointURL, reqPath, rawPath string
	if isAnonymous && bucket != "" && SupportsVHost(f.upstreamEndpoint) {
		endpointURL = VHostEndpoint(f.upstreamEndpoint, bucket)
		reqPath = RemoveBucketPrefix(r.URL.Path, bucket)
		if r.URL.RawPath != "" {
			rawPath = RemoveBucketPrefix(r.URL.RawPath, bucket)
		}
	} else {
		endpointURL = f.upstreamEndpoint
		reqPath = r.URL.Path
		rawPath = r.URL.RawPath
	}

	baseURL, err := url.Parse(endpointURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint: %w", err)
	}

	baseURL.Path = reqPath
	baseURL.RawPath = rawPath
	baseURL.RawQuery = r.URL.RawQuery

	fwdReq, err := http.NewRequestWithContext(ctx, r.Method, baseURL.String(), r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Keep the map itself private so proxy credentials and any synthesized
	// headers are never visible through the inbound request. Unchanged value
	// slices are borrowed and must not be mutated by the transport.
	fwdReq.Header = shallowHeaderCopy(r.Header)
	if _, ok := PresignedVirtualHostBucket(r); ok {
		// Keep dialing the configured upstream endpoint for TLS and connection
		// policy, but route the HTTP request through the original bucket host.
		fwdReq.Host = r.Host
	}

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
	proxyHeaders := f.proxySigner.ComputeProxyHeaders(forwardedHost, r.Method, reqPath)
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
			metrics.RecordLocalAuthValidation("missing_auth")
			return AuthNotValidated, nil
		}
		metrics.RecordLocalAuthValidation("parse_error")
		return AuthNotValidated, mapAuthError(err)
	}

	bucket, _ := ParseBucketKey(r)
	if authInfo.IsPresigned && hasSessionToken(r) {
		metrics.RecordLocalAuthValidation("temporary_credentials")
		return AuthNotValidated, nil
	}

	if f.credentialStore != nil && f.credentialStore.HasCredential(authInfo.AccessKey) {
		if _, err := f.credentialAuth.ValidateRequest(r); err != nil {
			metrics.RecordLocalAuthValidation("signature_mismatch")
			return AuthNotValidated, nil
		}
		if !f.authzCache.IsAuthorized(authInfo.AccessKey, bucket) {
			metrics.RecordLocalAuthValidation("authz_expired")
			return AuthNotValidated, nil
		}
		metrics.RecordLocalAuthValidation("success")
		return AuthValidated, nil
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

func (f *transparentForwarder) AuthorizePresignedRequest(
	ctx context.Context,
	r *http.Request,
	accessKey, secretKey string,
	rangeProbe bool,
) (*http.Response, error) {
	probe := r.Clone(ctx)
	probe.Body = nil
	probe.ContentLength = 0
	if rangeProbe {
		probe.Header.Set("Range", "bytes=0-0")
	} else {
		probe.Header.Del("Range")
	}

	fwdReq, err := f.buildTransparentRequest(ctx, probe)
	if err != nil {
		return nil, err
	}
	return f.executeRequest(fwdReq, 0, probe)
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

	var authInfo *auth.AuthInfo
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var err error
		authInfo, err = auth.ParseAuthInfo(r)
		if err == nil {
			bucket, _ := ParseBucketKey(r)
			if authInfo.IsPresigned && hasSessionToken(r) {
				log.Debug().Str("bucket", bucket).Msg("Skipping local auth learning for temporary credentials")
				return
			}
			if f.credentialStore != nil && f.credentialStore.HasCredential(authInfo.AccessKey) {
				f.authzCache.Grant(authInfo.AccessKey, bucket)
				metrics.AuthzCacheSize.Set(float64(f.authzCache.Count()))
			}
		}
	}

	// Header may be absent on 2xx if feature is disabled on Tigris side, or non-proxy request
	if headerVal == "" || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if headerVal == "" && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Debug().Int("status", resp.StatusCode).Msg("Signing keys header absent from successful response")
		}
		return
	}

	if authInfo == nil {
		var err error
		authInfo, err = auth.ParseAuthInfo(r)
		if err != nil {
			log.Debug().Err(err).Msg("Failed to parse auth info for signing key learning")
			return
		}
	}

	entries, err := f.keyUnwrapper.Unwrap(headerVal, authInfo.AccessKey)
	if err != nil {
		log.Warn().Err(err).Str("access_key", authInfo.AccessKey).Msg("Failed to unwrap signing keys")
		return
	}

	newKeys := 0
	for _, entry := range entries {
		keyBytes, err := hex.DecodeString(entry.SigningKey)
		if err != nil {
			log.Warn().Err(err).Str("date", entry.Date).Msg("Failed to decode signing key hex")
			continue
		}
		if _, err := f.derivedKeyStore.GetSigningKey(authInfo.AccessKey, entry.Date, entry.Region); err != nil {
			newKeys++
		}
		f.derivedKeyStore.Store(authInfo.AccessKey, entry.Date, entry.Region, keyBytes)
	}

	bucket, _ := ParseBucketKey(r)
	f.authzCache.Grant(authInfo.AccessKey, bucket)

	if newKeys > 0 {
		log.Debug().
			Str("bucket", bucket).
			Int("keys_learned", newKeys).
			Int("store_size", f.derivedKeyStore.Count()).
			Msg("Signing keys learned successfully")
	}

	metrics.ProxySigningKeysReceived.Inc()
	metrics.DerivedKeyStoreSize.Set(float64(f.derivedKeyStore.Count()))
	metrics.AuthzCacheSize.Set(float64(f.authzCache.Count()))
}

// handleAuthzRevocation revokes authorization when Tigris returns 403.
func (f *transparentForwarder) handleAuthzRevocation(resp *http.Response, r *http.Request) {
	if f.authzCache == nil || resp.StatusCode != http.StatusForbidden {
		return
	}

	authInfo, err := auth.ParseAuthInfo(r)
	if err != nil {
		return
	}

	bucket, _ := ParseBucketKey(r)
	f.authzCache.Revoke(authInfo.AccessKey, bucket)
	log.Debug().Str("access_key", maskAccessKey(authInfo.AccessKey)).Str("bucket", bucket).Msg("Authorization revoked (upstream 403)")
}

// maskAccessKey returns the last 4 characters of an access key prefixed with "...",
// or the full key if it's too short to mask.
func maskAccessKey(key string) string {
	if len(key) <= 4 {
		return key
	}
	return "..." + key[len(key)-4:]
}
