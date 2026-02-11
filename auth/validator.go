package auth

import (
	"crypto/hmac"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
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
)

const (
	// maxRequestAge is the maximum age of a request before it's considered expired.
	maxRequestAge = 15 * time.Minute
)

// RequestValidator validates incoming AWS SigV4 signed requests.
type RequestValidator struct {
	credStore *CredentialStore
}

// NewRequestValidator creates a new request validator.
func NewRequestValidator(credStore *CredentialStore) *RequestValidator {
	return &RequestValidator{
		credStore: credStore,
	}
}

// ValidateRequest validates the AWS SigV4 signature of an incoming request.
// This requires the X-Amz-Content-Sha256 header to be present (all AWS SDKs set this).
// This enables zero-copy streaming of the request body.
// Returns the access key if validation succeeds, or an error if it fails.
func (v *RequestValidator) ValidateRequest(r *http.Request) (string, error) {
	// Require X-Amz-Content-Sha256 header - all AWS SDKs set this
	bodyHash := r.Header.Get("X-Amz-Content-Sha256")
	if bodyHash == "" {
		return "", ErrMissingContentHash
	}

	// Parse auth info
	authInfo, err := ParseAuthInfo(r)
	if err != nil {
		return "", fmt.Errorf("failed to parse auth: %w", err)
	}

	// Look up secret key
	secretKey, err := v.credStore.GetSecretKey(authInfo.AccessKey)
	if err != nil {
		return "", fmt.Errorf("failed to get secret key: %w", err)
	}

	// Validate based on auth type
	if authInfo.IsPresigned {
		return authInfo.AccessKey, v.validatePresigned(r, authInfo, secretKey)
	}
	return authInfo.AccessKey, v.validateSigned(r, authInfo, secretKey, bodyHash)
}

// validateSigned validates a header-based signed request using a pre-computed body hash.
func (v *RequestValidator) validateSigned(r *http.Request, authInfo *AuthInfo, secretKey string, bodyHash string) error {
	// Get the date from the request
	dateStr := r.Header.Get("X-Amz-Date")
	if dateStr == "" {
		dateStr = r.Header.Get("Date")
	}
	if dateStr == "" {
		return ErrInvalidDate
	}

	// Parse and validate the date
	requestTime, err := time.Parse(TimeFormat, dateStr)
	if err != nil {
		// Try HTTP date format
		requestTime, err = time.Parse(time.RFC1123, dateStr)
		if err != nil {
			return ErrInvalidDate
		}
	}

	// Check request age
	age := time.Since(requestTime)
	if age > maxRequestAge || age < -maxRequestAge {
		return ErrExpiredRequest
	}

	// Compute expected signature
	expectedSig := v.computeSignature(r, authInfo, secretKey, bodyHash, requestTime)

	// Compare signatures (constant time)
	if !hmac.Equal([]byte(authInfo.Signature), []byte(expectedSig)) {
		return ErrSignatureMismatch
	}

	return nil
}

// validatePresigned validates a presigned URL request.
func (v *RequestValidator) validatePresigned(r *http.Request, authInfo *AuthInfo, secretKey string) error {
	query := r.URL.Query()

	// Get expiration time
	dateStr := query.Get("X-Amz-Date")
	if dateStr == "" {
		return ErrInvalidDate
	}

	requestTime, err := time.Parse(TimeFormat, dateStr)
	if err != nil {
		return ErrInvalidDate
	}

	// Check expires
	expiresStr := query.Get("X-Amz-Expires")
	if expiresStr != "" {
		expires, err := strconv.Atoi(expiresStr)
		if err == nil && time.Since(requestTime) > time.Duration(expires)*time.Second {
			return ErrExpiredRequest
		}
	}

	// For presigned URLs, body hash is typically UNSIGNED-PAYLOAD
	bodyHash := unsignedPayload

	// Compute expected signature
	expectedSig := v.computePresignedSignature(r, authInfo, secretKey, bodyHash, requestTime)

	// Compare signatures (constant time)
	if !hmac.Equal([]byte(authInfo.Signature), []byte(expectedSig)) {
		return ErrSignatureMismatch
	}

	return nil
}

// computeSignature computes the expected signature for a signed request.
func (v *RequestValidator) computeSignature(r *http.Request, authInfo *AuthInfo, secretKey, bodyHash string, signingTime time.Time) string {
	// Build credential scope
	dateStr := signingTime.Format(shortTimeFormat)
	credentialScope := fmt.Sprintf("%s/%s/%s/%s", dateStr, authInfo.Region, service, terminationString)

	// Build canonical request
	canonicalRequest := v.buildCanonicalRequest(r, authInfo.SignedHeaders, bodyHash)

	// Build string to sign
	stringToSign := strings.Join([]string{
		algorithm,
		signingTime.Format(TimeFormat),
		credentialScope,
		hashSHA256([]byte(canonicalRequest)),
	}, "\n")

	// Derive signing key and compute signature
	signingKey := v.deriveSigningKey(secretKey, dateStr, authInfo.Region)
	return hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
}

// computePresignedSignature computes the expected signature for a presigned URL.
func (v *RequestValidator) computePresignedSignature(r *http.Request, authInfo *AuthInfo, secretKey, bodyHash string, signingTime time.Time) string {
	// Build credential scope
	dateStr := signingTime.Format(shortTimeFormat)
	credentialScope := fmt.Sprintf("%s/%s/%s/%s", dateStr, authInfo.Region, service, terminationString)

	// For presigned URLs, we need to exclude the signature from the query string
	// when computing the canonical request
	canonicalRequest := v.buildCanonicalRequestPresigned(r, authInfo.SignedHeaders, bodyHash)

	// Build string to sign
	stringToSign := strings.Join([]string{
		algorithm,
		signingTime.Format(TimeFormat),
		credentialScope,
		hashSHA256([]byte(canonicalRequest)),
	}, "\n")

	// Derive signing key and compute signature
	signingKey := v.deriveSigningKey(secretKey, dateStr, authInfo.Region)
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
func (v *RequestValidator) buildCanonicalQueryString(query url.Values) string {
	if len(query) == 0 {
		return ""
	}

	// Get sorted keys
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build sorted key=value pairs using AWS SigV4 encoding
	pairs := make([]string, 0, len(query))
	for _, k := range keys {
		values := query[k]
		sort.Strings(values)
		for _, val := range values {
			pairs = append(pairs, awsURIEncode(k, true)+"="+awsURIEncode(val, true))
		}
	}

	return strings.Join(pairs, "&")
}

// buildCanonicalHeadersFromList builds canonical headers from a list of header names.
func (v *RequestValidator) buildCanonicalHeadersFromList(r *http.Request, signedHeaders []string) string {
	var builder strings.Builder

	for _, name := range signedHeaders {
		var value string
		if name == "host" {
			value = r.Host
			if value == "" {
				value = r.URL.Host
			}
		} else {
			values := r.Header.Values(name)
			// Trim and join values
			for i, val := range values {
				values[i] = strings.TrimSpace(val)
			}
			value = strings.Join(values, ",")
		}

		builder.WriteString(name)
		builder.WriteString(":")
		builder.WriteString(value)
		builder.WriteString("\n")
	}

	return builder.String()
}

// deriveSigningKey derives the signing key from the secret key.
func (v *RequestValidator) deriveSigningKey(secretKey, dateStr, region string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStr))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte(terminationString))
	return kSigning
}
