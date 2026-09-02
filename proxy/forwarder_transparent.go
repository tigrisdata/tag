package proxy

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

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
	f.handleAuthzRevocation(resp, originalReq)
}

type transparentHeaderValue struct {
	values  []string
	present bool
}

type transparentHeaderRestore struct {
	header http.Header

	forwardedHost    transparentHeaderValue
	proxyAccessKey   transparentHeaderValue
	proxyTimestamp   transparentHeaderValue
	proxySignature   transparentHeaderValue
	xAmzDate         transparentHeaderValue
	contentEncoding  transparentHeaderValue
	xAmzDateCaptured bool
	contentCaptured  bool
	once             sync.Once
}

func snapshotTransparentHeader(header http.Header, key string) transparentHeaderValue {
	values, present := header[key]
	return transparentHeaderValue{values: values, present: present}
}

func restoreTransparentHeader(header http.Header, key string, value transparentHeaderValue) {
	if value.present {
		header[key] = value.values
	} else {
		delete(header, key)
	}
}

func newTransparentHeaderRestore(header http.Header) *transparentHeaderRestore {
	return &transparentHeaderRestore{
		header:         header,
		forwardedHost:  snapshotTransparentHeader(header, "X-Tigris-Forwarded-Host"),
		proxyAccessKey: snapshotTransparentHeader(header, "X-Tigris-Proxy-Access-Key"),
		proxyTimestamp: snapshotTransparentHeader(header, "X-Tigris-Proxy-Timestamp"),
		proxySignature: snapshotTransparentHeader(header, "X-Tigris-Proxy-Signature"),
	}
}

func (r *transparentHeaderRestore) captureXAmzDate() {
	r.xAmzDate = snapshotTransparentHeader(r.header, "X-Amz-Date")
	r.xAmzDateCaptured = true
}

func (r *transparentHeaderRestore) captureContentEncoding() {
	r.contentEncoding = snapshotTransparentHeader(r.header, "Content-Encoding")
	r.contentCaptured = true
}

func (r *transparentHeaderRestore) restore() {
	r.once.Do(func() {
		restoreTransparentHeader(r.header, "X-Tigris-Forwarded-Host", r.forwardedHost)
		restoreTransparentHeader(r.header, "X-Tigris-Proxy-Access-Key", r.proxyAccessKey)
		restoreTransparentHeader(r.header, "X-Tigris-Proxy-Timestamp", r.proxyTimestamp)
		restoreTransparentHeader(r.header, "X-Tigris-Proxy-Signature", r.proxySignature)
		if r.xAmzDateCaptured {
			restoreTransparentHeader(r.header, "X-Amz-Date", r.xAmzDate)
		}
		if r.contentCaptured {
			restoreTransparentHeader(r.header, "Content-Encoding", r.contentEncoding)
		}
	})
}

// restoreOnCloseBody restores the borrowed request headers after the transport
// response body has been closed. The underlying response body is closed before
// restoration so a transport that retains the request until Close can finish
// reading the forwarded headers safely.
type restoreOnCloseBody struct {
	io.ReadCloser
	restore func()
	once    sync.Once
	err     error
}

func (b *restoreOnCloseBody) Close() error {
	b.once.Do(func() {
		defer b.restore()
		b.err = b.ReadCloser.Close()
	})
	return b.err
}

// buildTransparentRequest creates a forwarded request for transparent proxy mode.
// Borrows the original header map and adds the 4 proxy headers. The body is
// streamed as-is without decoding. The returned restore function must run after
// the transport no longer uses the forwarded headers.
//
// For AWS chunked transfer encoding (streaming SigV4), ensures the required
// Content-Encoding: aws-chunked header is present per the S3 spec. Some clients
// (e.g., minio-go) omit this header despite using chunked encoding on the wire.
func (f *transparentForwarder) buildTransparentRequest(ctx context.Context, r *http.Request) (*http.Request, func(), error) {
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
		return nil, nil, fmt.Errorf("failed to parse endpoint: %w", err)
	}

	baseURL.Path = reqPath
	baseURL.RawPath = rawPath
	baseURL.RawQuery = r.URL.RawQuery

	fwdReq, err := http.NewRequestWithContext(ctx, r.Method, baseURL.String(), r.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Borrow the original header map. Only the headers changed below are saved so
	// the original request can be restored after the transport is finished with it.
	if r.Header == nil {
		fwdReq.Header = make(http.Header)
	} else {
		fwdReq.Header = r.Header
	}
	headerRestore := newTransparentHeaderRestore(fwdReq.Header)

	// Ensure X-Amz-Date is present for Tigris's proxy validation path.
	// Some SDK versions (botocore 1.42+) sign with "date" in SignedHeaders
	// and don't set X-Amz-Date at all. Tigris's proxy code path requires
	// X-Amz-Date, so synthesize it from Date when missing.
	// We never remove Date — it may be in SignedHeaders and required for
	// signature verification.
	if fwdReq.Header.Get("X-Amz-Date") == "" {
		if dateStr := fwdReq.Header.Get("Date"); dateStr != "" {
			if t, err := auth.ParseHTTPDate(dateStr); err == nil {
				headerRestore.captureXAmzDate()
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
		headerRestore.captureContentEncoding()
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

	return fwdReq, headerRestore.restore, nil
}

// Forward forwards a request to Tigris in transparent proxy mode.
// The client's original signed request is forwarded as-is with proxy headers added.
// Response interception (signing key learning, header stripping, authZ revocation)
// is handled by the base forwarder's response interceptor.
func (f *transparentForwarder) Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	fwdReq, restore, err := f.buildTransparentRequest(ctx, r)
	if err != nil {
		return err
	}
	defer restore()

	return f.executeAndStream(w, fwdReq, r.ContentLength, r)
}

// ForwardWithCapture forwards request in transparent proxy mode and captures the response.
// Response interception (signing key learning, header stripping, authZ revocation)
// is handled by the base forwarder's response interceptor.
func (f *transparentForwarder) ForwardWithCapture(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
	fwdReq, restore, err := f.buildTransparentRequest(ctx, r)
	if err != nil {
		return nil, err
	}
	defer restore()

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
	fwdReq, restore, err := f.buildTransparentRequest(ctx, r)
	if err != nil {
		return nil, err
	}

	resp, err := f.executeRequest(fwdReq, r.ContentLength, r)
	if err != nil {
		restore()
		return nil, err
	}
	if resp.Body == nil {
		restore()
		return resp, nil
	}
	resp.Body = &restoreOnCloseBody{
		ReadCloser: resp.Body,
		restore:    restore,
	}
	return resp, nil
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
