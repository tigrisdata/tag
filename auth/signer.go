package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	// emptyBodyHash is the SHA256 hash of an empty body.
	emptyBodyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	// unsignedPayload is used for streaming uploads or when payload hash is not computed.
	unsignedPayload = "UNSIGNED-PAYLOAD"

	// TimeFormat is the format for X-Amz-Date header.
	TimeFormat = "20060102T150405Z"

	// shortTimeFormat is the format for the date in the credential scope.
	shortTimeFormat = "20060102"

	// algorithm is the AWS SigV4 algorithm identifier.
	algorithm = "AWS4-HMAC-SHA256"

	// service is the S3 service name.
	service = "s3"

	// terminationString is the termination string for AWS SigV4.
	terminationString = "aws4_request"
)

// ParseHTTPDate parses a date string in common HTTP/AWS formats.
func ParseHTTPDate(dateStr string) (time.Time, error) {
	for _, layout := range []string{
		TimeFormat,
		time.RFC1123,
		time.RFC1123Z,
	} {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized date format: %s", dateStr)
}

// RequestSigner signs HTTP requests using AWS SigV4.
type RequestSigner struct {
	endpoint string
	region   string
}

// NewRequestSigner creates a new request signer.
func NewRequestSigner(endpoint, region string) *RequestSigner {
	return &RequestSigner{
		endpoint: strings.TrimSuffix(endpoint, "/"),
		region:   region,
	}
}

// SignRequest creates a new HTTP request signed for Tigris using streaming.
// It accepts a pre-computed body hash (from X-Amz-Content-Sha256 header) to avoid
// buffering the entire body in memory. The body is passed through as-is.
//
// If bodyHash is empty, it defaults to the SHA256 of an empty body, which is
// correct for requests without a body (GET, HEAD, DELETE).
func (s *RequestSigner) SignRequest(ctx context.Context, method, path string,
	body io.Reader, bodyHash string, accessKey, secretKey string, headers http.Header) (*http.Request, error) {

	// Build the full URL using url.URL to properly handle encoding
	// This ensures special characters like % are properly encoded
	baseURL, err := url.Parse(s.endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint: %w", err)
	}

	// Split path and query string
	pathPart := path
	queryPart := ""
	if idx := strings.Index(path, "?"); idx != -1 {
		pathPart = path[:idx]
		queryPart = path[idx+1:]
	}

	// Set path (Go will properly encode special characters like % when converting to string)
	baseURL.Path = pathPart
	baseURL.RawQuery = queryPart

	fullURL := baseURL.String()

	// Create the new request with streaming body
	req, err := http.NewRequestWithContext(ctx, method, fullURL, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Copy relevant headers (content headers and user metadata)
	for k, v := range headers {
		if shouldCopyHeader(k) {
			req.Header[k] = v
		}
	}

	// Use provided body hash or default to empty body hash for requests without body
	if bodyHash == "" {
		bodyHash = emptyBodyHash
	}

	// Set required headers for signing
	now := time.Now().UTC()
	req.Header.Set("X-Amz-Date", now.Format(TimeFormat))
	req.Header.Set("X-Amz-Content-Sha256", bodyHash)
	req.Header.Set("Host", req.URL.Host)

	// Sign the request
	if err := s.signHTTP(req, accessKey, secretKey, bodyHash, now); err != nil {
		return nil, fmt.Errorf("failed to sign request: %w", err)
	}

	return req, nil
}

// signHTTP signs an HTTP request using AWS SigV4.
func (s *RequestSigner) signHTTP(req *http.Request, accessKey, secretKey, bodyHash string, signingTime time.Time) error {
	// Build credential scope
	dateStr := signingTime.Format(shortTimeFormat)
	credentialScope := fmt.Sprintf("%s/%s/%s/%s", dateStr, s.region, service, terminationString)

	// Build canonical request
	canonicalRequest, signedHeaders := s.buildCanonicalRequest(req, bodyHash)

	// Build string to sign
	stringToSign := s.buildStringToSign(signingTime, credentialScope, canonicalRequest)

	// Calculate signature
	signingKey := s.deriveSigningKey(secretKey, dateStr)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	// Build Authorization header
	authHeader := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm, accessKey, credentialScope, signedHeaders, signature)

	req.Header.Set("Authorization", authHeader)
	return nil
}

// buildCanonicalRequest builds the canonical request string for signing.
func (s *RequestSigner) buildCanonicalRequest(req *http.Request, bodyHash string) (string, string) {
	// Canonical URI - use AWS SigV4 encoding which encodes more characters than Go's EscapedPath
	// Specifically, + must be encoded as %2B per AWS spec, but Go's EscapedPath leaves it unencoded
	canonicalURI := awsURIEncode(req.URL.Path, false)
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	// Canonical query string (sorted parameters)
	canonicalQueryString := s.buildCanonicalQueryString(req.URL.Query())

	// Canonical headers (sorted, lowercase)
	canonicalHeaders, signedHeaders := s.buildCanonicalHeaders(req)

	// Build the canonical request
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		bodyHash,
	}, "\n")

	return canonicalRequest, signedHeaders
}

// buildCanonicalQueryString builds the canonical query string from URL parameters.
func (s *RequestSigner) buildCanonicalQueryString(query url.Values) string {
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
		for _, v := range values {
			pairs = append(pairs, awsURIEncode(k, true)+"="+awsURIEncode(v, true))
		}
	}

	return strings.Join(pairs, "&")
}

// buildCanonicalHeaders builds canonical headers and signed headers string.
func (s *RequestSigner) buildCanonicalHeaders(req *http.Request) (string, string) {
	// Get headers to sign
	headers := make(map[string][]string)
	for k, v := range req.Header {
		lower := strings.ToLower(k)
		// Sign host and all x-amz-* headers, plus content-type
		if lower == "host" || strings.HasPrefix(lower, "x-amz-") || lower == "content-type" {
			headers[lower] = v
		}
	}

	// Always include host
	if _, ok := headers["host"]; !ok {
		headers["host"] = []string{req.URL.Host}
	}

	// Sort header names
	headerNames := make([]string, 0, len(headers))
	for k := range headers {
		headerNames = append(headerNames, k)
	}
	sort.Strings(headerNames)

	// Build canonical headers string
	var canonicalHeaders strings.Builder
	for _, name := range headerNames {
		values := headers[name]
		// Trim and collapse whitespace
		for i, v := range values {
			values[i] = strings.TrimSpace(v)
		}
		canonicalHeaders.WriteString(name)
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(strings.Join(values, ","))
		canonicalHeaders.WriteString("\n")
	}

	// Build signed headers string
	signedHeaders := strings.Join(headerNames, ";")

	return canonicalHeaders.String(), signedHeaders
}

// buildStringToSign builds the string to sign.
func (s *RequestSigner) buildStringToSign(signingTime time.Time, credentialScope, canonicalRequest string) string {
	return strings.Join([]string{
		algorithm,
		signingTime.Format(TimeFormat),
		credentialScope,
		hashSHA256([]byte(canonicalRequest)),
	}, "\n")
}

// deriveSigningKey derives the signing key from the secret key.
func (s *RequestSigner) deriveSigningKey(secretKey, dateStr string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStr))
	kRegion := hmacSHA256(kDate, []byte(s.region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte(terminationString))
	return kSigning
}

// shouldCopyHeader returns true if the header should be copied to the upstream request.
func shouldCopyHeader(key string) bool {
	lower := strings.ToLower(key)
	switch lower {
	// Content headers
	case "content-type", "content-length", "content-encoding",
		"content-disposition", "content-language", "cache-control",
		"expires", "content-md5":
		return true
	// Range requests
	case "range":
		return true
	// Conditional request headers
	case "if-match", "if-none-match", "if-modified-since", "if-unmodified-since":
		return true
	}
	// All x-amz-* headers (S3 operations, metadata, etc.)
	if strings.HasPrefix(lower, "x-amz-") {
		return true
	}
	// All Tigris-specific headers (tigris-* and x-tigris-*), except proxy headers
	// which must not be forwarded in signing mode to prevent client injection.
	// The transparent forwarder overwrites these with .Set() so it's unaffected.
	if strings.HasPrefix(lower, "x-tigris-proxy-") || lower == "x-tigris-forwarded-host" {
		return false
	}
	if strings.HasPrefix(lower, "tigris-") || strings.HasPrefix(lower, "x-tigris-") {
		return true
	}
	return false
}

// hmacSHA256 computes HMAC-SHA256.
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// hashSHA256 computes SHA256 hash and returns hex string.
func hashSHA256(data []byte) string {
	h := sha256.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

// awsURIEncode encodes a string per AWS SigV4 spec (RFC 3986).
// Unlike url.QueryEscape, this encodes spaces as %20 not +.
// Set encodeSlash to false for path encoding (S3 bucket/key paths).
func awsURIEncode(s string, encodeSlash bool) string {
	var result strings.Builder
	result.Grow(len(s) * 3) // Worst case: all chars need encoding
	for _, c := range []byte(s) {
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '-' || c == '~' || c == '.' {
			result.WriteByte(c)
		} else if c == '/' && !encodeSlash {
			result.WriteByte(c)
		} else {
			result.WriteString(fmt.Sprintf("%%%02X", c))
		}
	}
	return result.String()
}
