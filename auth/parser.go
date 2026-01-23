package auth

import (
	"errors"
	"net/http"
	"regexp"
	"strings"
)

var (
	// ErrMissingAuth is returned when no Authorization header or query auth is present.
	ErrMissingAuth = errors.New("missing authorization")

	// ErrUnsupportedAuthScheme is returned for non-SigV4 authorization schemes.
	ErrUnsupportedAuthScheme = errors.New("unsupported authorization scheme")

	// ErrInvalidAuthFormat is returned when the Authorization header format is invalid.
	ErrInvalidAuthFormat = errors.New("invalid authorization header format")

	// credentialRegex extracts the access key from AWS SigV4 Authorization header.
	// Format: AWS4-HMAC-SHA256 Credential=<access_key>/<date>/<region>/<service>/aws4_request, ...
	credentialRegex = regexp.MustCompile(`Credential=([^/]+)/`)

	// scopeRegex extracts the full credential scope from Authorization header.
	scopeRegex = regexp.MustCompile(`Credential=([^,]+)`)

	// signedHeadersRegex extracts the SignedHeaders from Authorization header.
	signedHeadersRegex = regexp.MustCompile(`SignedHeaders=([^,]+)`)

	// signatureRegex extracts the Signature from Authorization header.
	signatureRegex = regexp.MustCompile(`Signature=([a-f0-9]+)`)
)

// AuthInfo contains parsed authentication information from a request.
type AuthInfo struct {
	AccessKey     string
	IsPresigned   bool
	SignedHeaders []string
	Signature     string
	Region        string
	Date          string
}

// ExtractAccessKey extracts the access key from the Authorization header or query string.
// Supports both header-based auth and presigned URLs.
func ExtractAccessKey(r *http.Request) (string, error) {
	auth := r.Header.Get("Authorization")

	// Check for header-based auth
	if auth != "" {
		return extractFromHeader(auth)
	}

	// Check for query string auth (presigned URLs)
	return extractFromQuery(r)
}

// ParseAuthInfo extracts detailed authentication information from a request.
func ParseAuthInfo(r *http.Request) (*AuthInfo, error) {
	auth := r.Header.Get("Authorization")

	// Check for header-based auth
	if auth != "" {
		return parseFromHeader(auth)
	}

	// Check for query string auth (presigned URLs)
	return parseFromQuery(r)
}

// extractFromHeader extracts the access key from an Authorization header.
func extractFromHeader(auth string) (string, error) {
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
		return "", ErrUnsupportedAuthScheme
	}

	matches := credentialRegex.FindStringSubmatch(auth)
	if len(matches) < 2 {
		return "", ErrInvalidAuthFormat
	}

	return matches[1], nil
}

// extractFromQuery extracts the access key from query string parameters (presigned URLs).
func extractFromQuery(r *http.Request) (string, error) {
	// Check X-Amz-Credential for presigned URLs
	credential := r.URL.Query().Get("X-Amz-Credential")
	if credential == "" {
		return "", ErrMissingAuth
	}

	// Format: <access_key>/<date>/<region>/<service>/aws4_request
	parts := strings.Split(credential, "/")
	if len(parts) < 1 || parts[0] == "" {
		return "", ErrInvalidAuthFormat
	}

	return parts[0], nil
}

// parseFromHeader parses detailed auth info from an Authorization header.
func parseFromHeader(auth string) (*AuthInfo, error) {
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
		return nil, ErrUnsupportedAuthScheme
	}

	info := &AuthInfo{
		IsPresigned: false,
	}

	// Extract Credential
	credMatch := credentialRegex.FindStringSubmatch(auth)
	if len(credMatch) < 2 {
		return nil, ErrInvalidAuthFormat
	}
	info.AccessKey = credMatch[1]

	// Extract full credential scope to get region and date
	scopeMatch := scopeRegex.FindStringSubmatch(auth)
	if len(scopeMatch) >= 2 {
		parts := strings.Split(scopeMatch[1], "/")
		if len(parts) >= 4 {
			info.Date = parts[1]
			info.Region = parts[2]
		}
	}

	// Extract SignedHeaders
	headersMatch := signedHeadersRegex.FindStringSubmatch(auth)
	if len(headersMatch) >= 2 {
		info.SignedHeaders = strings.Split(headersMatch[1], ";")
	}

	// Extract Signature
	sigMatch := signatureRegex.FindStringSubmatch(auth)
	if len(sigMatch) >= 2 {
		info.Signature = sigMatch[1]
	}

	return info, nil
}

// parseFromQuery parses detailed auth info from query string parameters (presigned URLs).
func parseFromQuery(r *http.Request) (*AuthInfo, error) {
	query := r.URL.Query()

	credential := query.Get("X-Amz-Credential")
	if credential == "" {
		return nil, ErrMissingAuth
	}

	info := &AuthInfo{
		IsPresigned: true,
	}

	// Parse credential: <access_key>/<date>/<region>/<service>/aws4_request
	parts := strings.Split(credential, "/")
	if len(parts) < 4 {
		return nil, ErrInvalidAuthFormat
	}
	info.AccessKey = parts[0]
	info.Date = parts[1]
	info.Region = parts[2]

	// Extract SignedHeaders
	signedHeaders := query.Get("X-Amz-SignedHeaders")
	if signedHeaders != "" {
		info.SignedHeaders = strings.Split(signedHeaders, ";")
	}

	// Extract Signature
	info.Signature = query.Get("X-Amz-Signature")

	return info, nil
}

// IsPresignedRequest checks if the request is using presigned URL authentication.
func IsPresignedRequest(r *http.Request) bool {
	return r.URL.Query().Get("X-Amz-Credential") != ""
}
