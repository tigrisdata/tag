package auth

import (
	"crypto/hmac"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrSignatureMismatch is returned when the request signature does not match.
	ErrSignatureMismatch = errors.New("signature does not match")

	// ErrExpiredRequest is returned when the request timestamp is too old.
	ErrExpiredRequest = errors.New("request has expired")

	// ErrInvalidDate is returned when the X-Amz-Date header is invalid.
	ErrInvalidDate = errors.New("invalid date format")

	// ErrMissingContentHash is returned when X-Amz-Content-Sha256 header is required but missing.
	ErrMissingContentHash = errors.New("missing X-Amz-Content-Sha256 header")

	// ErrInvalidExpires is returned when a presigned URL's X-Amz-Expires is
	// missing, repeated, not an integer, or outside [1, maxPresignedExpires].
	ErrInvalidExpires = errors.New("invalid X-Amz-Expires")
)

const (
	// maxRequestAge is the maximum age of a request before it's considered expired.
	// It is also the tolerated clock skew: a request dated further than this in
	// the future is rejected.
	maxRequestAge = 15 * time.Minute

	// maxPresignedExpires is the largest X-Amz-Expires a presigned URL may carry,
	// in seconds (seven days, matching AWS).
	maxPresignedExpires int64 = 604800
)

// RequestValidator validates incoming AWS SigV4 signed requests.
type RequestValidator struct {
	keyProvider KeyProvider
}

// NewRequestValidator creates a new request validator.
func NewRequestValidator(keyProvider KeyProvider) *RequestValidator {
	return &RequestValidator{
		keyProvider: keyProvider,
	}
}

// ValidatedRequest is the outcome of a successful signature validation: the
// access key, plus the signing material the request signature was computed
// with. That material is retained so the chunk chain of a
// STREAMING-AWS4-HMAC-SHA256-PAYLOAD body can be verified against the same
// key, signing time and credential scope, seeded with the request signature.
type ValidatedRequest struct {
	AccessKey string

	signingKey  []byte
	signingTime string // X-Amz-Date form, as it appears in the string to sign
	scope       string
	signature   string
}

// ChunkVerifier returns a verifier for the request's signed chunk chain.
func (vr *ValidatedRequest) ChunkVerifier() *ChunkVerifier {
	return &ChunkVerifier{
		signingKey: vr.signingKey,
		prefix:     streamingAlgorithm + "\n" + vr.signingTime + "\n" + vr.scope + "\n",
		previous:   vr.signature,
	}
}

// ValidateRequest validates the AWS SigV4 signature of an incoming request.
// Header-authenticated requests and non-GET requests require X-Amz-Content-Sha256.
// Presigned GETs use UNSIGNED-PAYLOAD and may omit the header.
// The header lets uploads be validated without buffering the request body.
// Returns the access key if validation succeeds, or an error if it fails.
func (v *RequestValidator) ValidateRequest(r *http.Request) (string, error) {
	vr, err := v.Validate(r)
	if err != nil {
		return "", err
	}
	return vr.AccessKey, nil
}

// Validate is ValidateRequest returning the full validation outcome, for
// callers that must go on to verify a signed streaming body.
func (v *RequestValidator) Validate(r *http.Request) (*ValidatedRequest, error) {
	bodyHash := r.Header.Get("X-Amz-Content-Sha256")

	// Classify the authentication form before enforcing the hash requirement:
	// a presigned GET uses UNSIGNED-PAYLOAD and does not need this header.
	authInfo, err := ParseAuthInfo(r)
	if err != nil {
		if bodyHash == "" {
			return nil, ErrMissingContentHash
		}
		return nil, fmt.Errorf("failed to parse auth: %w", err)
	}
	if bodyHash == "" && (!authInfo.IsPresigned || r.Method != http.MethodGet) {
		return nil, ErrMissingContentHash
	}

	// Get the date string for signing key lookup
	dateStr := r.Header.Get("X-Amz-Date")
	if dateStr == "" {
		dateStr = r.Header.Get("Date")
	}
	if authInfo.IsPresigned {
		dateStr = r.URL.Query().Get("X-Amz-Date")
	}
	// Parse the date and extract short date (YYYYMMDD) for signing key lookup.
	shortDate := authInfo.Date // From credential scope, already YYYYMMDD
	if shortDate == "" {
		t, err := ParseHTTPDate(dateStr)
		if err != nil {
			return nil, ErrInvalidDate
		}
		shortDate = t.UTC().Format(shortTimeFormat)
	}

	// Look up signing key
	signingKey, err := v.keyProvider.GetSigningKey(authInfo.AccessKey, shortDate, authInfo.Region)
	if err != nil {
		return nil, fmt.Errorf("failed to get signing key: %w", err)
	}

	// Validate based on auth type
	var signingTime time.Time
	if authInfo.IsPresigned {
		signingTime, err = v.validatePresigned(r, authInfo, signingKey)
	} else {
		signingTime, err = v.validateSigned(r, authInfo, signingKey, bodyHash)
	}
	if err != nil {
		return nil, err
	}
	return &ValidatedRequest{
		AccessKey:   authInfo.AccessKey,
		signingKey:  signingKey,
		signingTime: signingTime.Format(TimeFormat),
		scope:       credentialScope(signingTime, authInfo.Region),
		signature:   authInfo.Signature,
	}, nil
}

// validateSigned validates a header-based signed request using a pre-computed
// body hash and returns the signing time the signature was verified under.
func (v *RequestValidator) validateSigned(r *http.Request, authInfo *AuthInfo, signingKey []byte, bodyHash string) (time.Time, error) {
	// Get the date from the request
	dateStr := r.Header.Get("X-Amz-Date")
	if dateStr == "" {
		dateStr = r.Header.Get("Date")
	}
	if dateStr == "" {
		return time.Time{}, ErrInvalidDate
	}

	// Parse and validate the date
	requestTime, err := ParseHTTPDate(dateStr)
	if err != nil {
		return time.Time{}, ErrInvalidDate
	}

	// Check request age
	age := time.Since(requestTime)
	if age > maxRequestAge || age < -maxRequestAge {
		return time.Time{}, ErrExpiredRequest
	}

	// Compute expected signature
	expectedSig := v.computeSignature(r, authInfo, signingKey, bodyHash, requestTime)

	// Compare signatures (constant time)
	if !hmac.Equal([]byte(authInfo.Signature), []byte(expectedSig)) {
		return time.Time{}, ErrSignatureMismatch
	}

	return requestTime, nil
}

// validatePresigned validates a presigned URL request and returns the signing
// time the signature was verified under.
//
// Beyond the signature, a presigned URL must satisfy the AWS rules for its
// lifetime: X-Amz-Expires is mandatory, appears exactly once, is an integer in
// [1, maxPresignedExpires], the URL is rejected once that many seconds have
// passed since X-Amz-Date, and X-Amz-Date may not sit more than maxRequestAge
// in the future (the same clock-skew tolerance header-signed requests get).
func (v *RequestValidator) validatePresigned(r *http.Request, authInfo *AuthInfo, signingKey []byte) (time.Time, error) {
	query := r.URL.Query()

	// Get expiration time
	dateStr := query.Get("X-Amz-Date")
	if dateStr == "" {
		return time.Time{}, ErrInvalidDate
	}

	requestTime, err := time.Parse(TimeFormat, dateStr)
	if err != nil {
		return time.Time{}, ErrInvalidDate
	}

	// Check expires. The range check runs before the Duration conversion so
	// the multiplication below can never overflow.
	expiresValues := query["X-Amz-Expires"]
	if len(expiresValues) != 1 {
		return time.Time{}, ErrInvalidExpires
	}
	expires, err := strconv.ParseInt(expiresValues[0], 10, 64)
	if err != nil || expires < 1 || expires > maxPresignedExpires {
		return time.Time{}, ErrInvalidExpires
	}
	age := time.Since(requestTime)
	if age < -maxRequestAge {
		return time.Time{}, ErrExpiredRequest
	}
	if age > time.Duration(expires)*time.Second {
		return time.Time{}, ErrExpiredRequest
	}

	// For presigned URLs, body hash is typically UNSIGNED-PAYLOAD
	bodyHash := unsignedPayload

	// Compute expected signature
	expectedSig := v.computePresignedSignature(r, authInfo, signingKey, bodyHash, requestTime)

	// Compare signatures (constant time)
	if !hmac.Equal([]byte(authInfo.Signature), []byte(expectedSig)) {
		return time.Time{}, ErrSignatureMismatch
	}

	return requestTime, nil
}

// credentialScope builds the SigV4 credential scope for a signing time and region.
func credentialScope(signingTime time.Time, region string) string {
	return fmt.Sprintf("%s/%s/%s/%s", signingTime.Format(shortTimeFormat), region, service, terminationString)
}

// computeSignature computes the expected signature for a signed request.
func (v *RequestValidator) computeSignature(r *http.Request, authInfo *AuthInfo, signingKey []byte, bodyHash string, signingTime time.Time) string {
	// Build canonical request
	canonicalRequest := v.buildCanonicalRequest(r, authInfo.SignedHeaders, bodyHash)

	// Build string to sign
	stringToSign := strings.Join([]string{
		algorithm,
		signingTime.Format(TimeFormat),
		credentialScope(signingTime, authInfo.Region),
		hashSHA256([]byte(canonicalRequest)),
	}, "\n")

	// Compute signature using pre-derived signing key
	return hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
}

// computePresignedSignature computes the expected signature for a presigned URL.
func (v *RequestValidator) computePresignedSignature(r *http.Request, authInfo *AuthInfo, signingKey []byte, bodyHash string, signingTime time.Time) string {
	// For presigned URLs, we need to exclude the signature from the query string
	// when computing the canonical request
	canonicalRequest := v.buildCanonicalRequestPresigned(r, authInfo.SignedHeaders, bodyHash)

	// Build string to sign
	stringToSign := strings.Join([]string{
		algorithm,
		signingTime.Format(TimeFormat),
		credentialScope(signingTime, authInfo.Region),
		hashSHA256([]byte(canonicalRequest)),
	}, "\n")

	// Compute signature using pre-derived signing key
	return hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
}

// buildCanonicalRequest builds the canonical request for signature verification.
func (v *RequestValidator) buildCanonicalRequest(r *http.Request, signedHeaders []string, bodyHash string) string {
	// Canonical URI - use AWS SigV4 encoding which encodes more characters than Go's EscapedPath
	// Specifically, + must be encoded as %2B per AWS spec, but Go's EscapedPath leaves it unencoded
	canonicalURI := awsURIEncode(r.URL.Path, false)
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	// Canonical query string
	canonicalQueryString := v.buildCanonicalQueryString(r.URL.Query())

	// Canonical headers
	canonicalHeaders := v.buildCanonicalHeadersFromList(r, signedHeaders)

	// Signed headers string
	signedHeadersStr := strings.Join(signedHeaders, ";")

	return strings.Join([]string{
		r.Method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeadersStr,
		bodyHash,
	}, "\n")
}

// buildCanonicalRequestPresigned builds the canonical request for presigned URL verification.
func (v *RequestValidator) buildCanonicalRequestPresigned(r *http.Request, signedHeaders []string, bodyHash string) string {
	// Canonical URI - use AWS SigV4 encoding which encodes more characters than Go's EscapedPath
	// Specifically, + must be encoded as %2B per AWS spec, but Go's EscapedPath leaves it unencoded
	canonicalURI := awsURIEncode(r.URL.Path, false)
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	// Canonical query string (excluding X-Amz-Signature)
	query := r.URL.Query()
	delete(query, "X-Amz-Signature")
	canonicalQueryString := v.buildCanonicalQueryString(query)

	// Canonical headers
	canonicalHeaders := v.buildCanonicalHeadersFromList(r, signedHeaders)

	// Signed headers string
	signedHeadersStr := strings.Join(signedHeaders, ";")

	return strings.Join([]string{
		r.Method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeadersStr,
		bodyHash,
	}, "\n")
}

// buildCanonicalQueryString builds the canonical query string.
func (*RequestValidator) buildCanonicalQueryString(query url.Values) string {
	return canonicalQueryString(query)
}

// buildCanonicalHeadersFromList builds canonical headers from a list of header names.
func (v *RequestValidator) buildCanonicalHeadersFromList(r *http.Request, signedHeaders []string) string {
	var builder strings.Builder

	for _, name := range signedHeaders {
		builder.WriteString(name)
		builder.WriteString(":")
		if name == "host" {
			value := r.Host
			if value == "" {
				value = r.URL.Host
			}
			builder.WriteString(canonicalHeaderValue(value))
		} else {
			writeCanonicalHeaderValues(&builder, r.Header.Values(name))
		}
		builder.WriteString("\n")
	}

	return builder.String()
}
