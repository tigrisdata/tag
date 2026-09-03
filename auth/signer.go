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
	endpoint    string
	endpointURL *url.URL
	endpointErr error
	region      string
}

// NewRequestSigner creates a new request signer.
func NewRequestSigner(endpoint, region string) *RequestSigner {
	endpoint = strings.TrimSuffix(endpoint, "/")

	// Build the template once with the same URL parsing and host normalization
	// that SignRequest previously received from http.NewRequestWithContext.
	templateRequest, err := http.NewRequest(http.MethodGet, endpoint, nil)
	var endpointURL *url.URL
	if err == nil {
		endpointURL = templateRequest.URL
	}

	return &RequestSigner{
		endpoint:    endpoint,
		endpointURL: endpointURL,
		endpointErr: err,
		region:      region,
	}
}

// Endpoint returns the upstream endpoint this signer targets.
func (s *RequestSigner) Endpoint() string {
	return s.endpoint
}

// SignRequest creates a new HTTP request signed for Tigris using streaming.
// It accepts a pre-computed body hash (from X-Amz-Content-Sha256 header) to avoid
// buffering the entire body in memory. The body is passed through as-is.
//
// If bodyHash is empty, it defaults to the SHA256 of an empty body, which is
// correct for requests without a body (GET, HEAD, DELETE).
func (s *RequestSigner) SignRequest(ctx context.Context, method, path string,
	body io.Reader, bodyHash string, accessKey, secretKey string, headers http.Header) (*http.Request, error) {

	if s.endpointErr != nil {
		return nil, fmt.Errorf("failed to parse endpoint: %w", s.endpointErr)
	}

	// Copy the immutable endpoint template before applying request-specific fields.
	baseURL := *s.endpointURL

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
	if baseURL.Opaque != "" {
		// URL.String ignores Path for opaque URLs, so the old stringify-and-parse
		// path returned an empty Path as well.
		baseURL.Path = ""
		baseURL.RawPath = ""
	} else {
		if baseURL.RawPath != "" {
			unescapedPath, err := url.PathUnescape(baseURL.RawPath)
			if err != nil || unescapedPath != baseURL.Path {
				baseURL.RawPath = ""
			}
		}
		// URL.String inserts this slash before a relative path when a host is
		// present; preserve the URL fields the old parse produced.
		if baseURL.Host != "" && baseURL.Path != "" && baseURL.Path[0] != '/' {
			baseURL.Path = "/" + baseURL.Path
			baseURL.RawPath = ""
		}
	}
	baseURL.ForceQuery = baseURL.ForceQuery && baseURL.RawQuery == ""

	// A raw '#' in a query becomes a fragment when the old URL string was
	// reparsed. Keep that unusual, accepted request-target behavior while the
	// ordinary path attaches its completed URL directly.
	var (
		req *http.Request
		err error
	)
	if strings.Contains(baseURL.RawQuery, "#") {
		req, err = http.NewRequestWithContext(ctx, method, baseURL.String(), body)
	} else {
		// Use NewRequestWithContext for its method, context, and body setup, then
		// attach the completed URL directly rather than serializing and reparsing it.
		req, err = http.NewRequestWithContext(ctx, method, "", body)
		if err == nil {
			req.URL = &baseURL
			req.Host = baseURL.Host
		}
	}
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
func (*RequestSigner) buildCanonicalQueryString(query url.Values) string {
	return canonicalQueryString(query)
}

// canonicalQueryString builds sorted key=value pairs using AWS SigV4 encoding.
func canonicalQueryString(query url.Values) string {
	if len(query) == 0 {
		return ""
	}

	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Every byte can become a three-byte escape, plus the pair separators.
	size := 0
	for _, key := range keys {
		values := query[key]
		sort.Strings(values)
		for _, value := range values {
			size += 3*len(key) + 3*len(value) + 2
		}
	}

	var result strings.Builder
	result.Grow(size)
	pairs := 0
	for _, key := range keys {
		for _, value := range query[key] {
			if pairs > 0 {
				result.WriteByte('&')
			}
			writeAWSURIEncoded(&result, key, true)
			result.WriteByte('=')
			writeAWSURIEncoded(&result, value, true)
			pairs++
		}
	}

	return result.String()
}

// canonicalSigningHeaderName returns a lowercase SigV4 header name, if key is signed.
func canonicalSigningHeaderName(key string) string {
	switch len(key) {
	case len("host"):
		if strings.EqualFold(key, "host") {
			return "host"
		}
	case len("content-type"):
		if strings.EqualFold(key, "content-type") {
			return "content-type"
		}
	}
	if hasPrefixFold(key, "x-amz-") {
		return strings.ToLower(key)
	}
	return ""
}

// buildCanonicalHeaders builds canonical headers and signed headers string.
func (s *RequestSigner) buildCanonicalHeaders(req *http.Request) (string, string) {
	// Filter before normalizing keys to avoid lowercase copies for excluded headers.
	headers := make(map[string][]string)
	for k, v := range req.Header {
		// Sign host and all x-amz-* headers, plus content-type.
		if lower := canonicalSigningHeaderName(k); lower != "" {
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

// hasPrefixFold compares an ASCII HTTP header prefix case-insensitively.
func hasPrefixFold(key, prefix string) bool {
	if len(key) < len(prefix) {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		c := key[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != prefix[i] {
			return false
		}
	}
	return true
}

// shouldCopyHeader returns true if the header should be copied to the upstream request.
// It compares case-insensitively without allocating a normalized key.
func shouldCopyHeader(key string) bool {
	if key == "" {
		return false
	}

	// HTTP header field names are ASCII tokens, so their first byte selects the
	// small group of case-insensitive comparisons that can match.
	switch key[0] {
	case 'c', 'C':
		// Content headers
		switch len(key) {
		case len("content-type"):
			return strings.EqualFold(key, "content-type")
		case len("content-length"):
			return strings.EqualFold(key, "content-length")
		case len("content-encoding"):
			return strings.EqualFold(key, "content-encoding") || strings.EqualFold(key, "content-language")
		case len("content-disposition"):
			return strings.EqualFold(key, "content-disposition")
		case len("cache-control"):
			return strings.EqualFold(key, "cache-control")
		case len("content-md5"):
			return strings.EqualFold(key, "content-md5")
		}
	case 'e', 'E':
		return strings.EqualFold(key, "expires")
	case 'r', 'R':
		// Range requests
		return strings.EqualFold(key, "range")
	case 'i', 'I':
		// Conditional request headers
		switch len(key) {
		case len("if-match"):
			return strings.EqualFold(key, "if-match")
		case len("if-none-match"):
			return strings.EqualFold(key, "if-none-match")
		case len("if-modified-since"):
			return strings.EqualFold(key, "if-modified-since")
		case len("if-unmodified-since"):
			return strings.EqualFold(key, "if-unmodified-since")
		}
	case 'x', 'X':
		// All x-amz-* headers (S3 operations, metadata, etc.)
		if hasPrefixFold(key, "x-amz-") {
			return true
		}
		// All x-tigris-* headers, except proxy headers which must not be
		// forwarded in signing mode to prevent client injection. The transparent
		// forwarder overwrites these with .Set() so it's unaffected.
		if hasPrefixFold(key, "x-tigris-proxy-") || strings.EqualFold(key, "x-tigris-forwarded-host") {
			return false
		}
		return hasPrefixFold(key, "x-tigris-")
	case 't', 'T':
		return hasPrefixFold(key, "tigris-")
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
	writeAWSURIEncoded(&result, s, encodeSlash)
	return result.String()
}

const awsURIHex = "0123456789ABCDEF"

func writeAWSURIEncoded(result *strings.Builder, s string, encodeSlash bool) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '-' || c == '~' || c == '.' ||
			(c == '/' && !encodeSlash) {
			result.WriteByte(c)
			continue
		}

		result.WriteByte('%')
		result.WriteByte(awsURIHex[c>>4])
		result.WriteByte(awsURIHex[c&0x0F])
	}
}
