package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func newReservedCanonicalQueryValues() url.Values {
	return url.Values{
		"prefix/with space": {"one/two", "a b", "100%"},
		"x+y":               {"日本", ""},
	}
}

const reservedCanonicalQueryString = "prefix%2Fwith%20space=100%25&prefix%2Fwith%20space=a%20b&prefix%2Fwith%20space=one%2Ftwo&x%2By=&x%2By=%E6%97%A5%E6%9C%AC"

func TestRequestSigner_SignRequest(t *testing.T) {
	signer := NewRequestSigner("https://s3.amazonaws.com", "us-east-1")

	tests := []struct {
		name      string
		method    string
		path      string
		body      []byte
		accessKey string
		secretKey string
		headers   http.Header
		wantErr   bool
	}{
		{
			name:      "sign GET request",
			method:    http.MethodGet,
			path:      "/test-bucket/test-key",
			body:      nil,
			accessKey: "AKIAIOSFODNN7EXAMPLE",
			secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			headers:   http.Header{},
			wantErr:   false,
		},
		{
			name:      "sign PUT request with body",
			method:    http.MethodPut,
			path:      "/test-bucket/test-key",
			body:      []byte("test content"),
			accessKey: "AKIAIOSFODNN7EXAMPLE",
			secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			headers:   http.Header{"Content-Type": []string{"application/octet-stream"}},
			wantErr:   false,
		},
		{
			name:      "sign DELETE request",
			method:    http.MethodDelete,
			path:      "/test-bucket/test-key",
			body:      nil,
			accessKey: "AKIAIOSFODNN7EXAMPLE",
			secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			headers:   http.Header{},
			wantErr:   false,
		},
		{
			name:      "sign request with query string",
			method:    http.MethodGet,
			path:      "/test-bucket?list-type=2&prefix=test/",
			body:      nil,
			accessKey: "AKIAIOSFODNN7EXAMPLE",
			secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			headers:   http.Header{},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bodyReader io.Reader
			var bodyHash string
			if len(tt.body) > 0 {
				bodyReader = bytes.NewReader(tt.body)
				// Compute body hash like AWS SDKs do
				h := sha256.Sum256(tt.body)
				bodyHash = hex.EncodeToString(h[:])
			}

			req, err := signer.SignRequest(
				t.Context(),
				tt.method,
				tt.path,
				bodyReader,
				bodyHash,
				tt.accessKey,
				tt.secretKey,
				tt.headers,
			)

			if tt.wantErr {
				if err == nil {
					t.Error("SignRequest() error = nil, want error")
				}
				return
			}

			if err != nil {
				t.Fatalf("SignRequest() error = %v, want nil", err)
			}

			// Verify required headers are present
			auth := req.Header.Get("Authorization")
			if auth == "" {
				t.Error("Authorization header is missing")
			}
			if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
				t.Errorf("Authorization header should start with AWS4-HMAC-SHA256, got %q", auth)
			}
			if !strings.Contains(auth, "Credential="+tt.accessKey) {
				t.Errorf("Authorization header should contain Credential=%s", tt.accessKey)
			}
			if !strings.Contains(auth, "SignedHeaders=") {
				t.Error("Authorization header should contain SignedHeaders")
			}
			if !strings.Contains(auth, "Signature=") {
				t.Error("Authorization header should contain Signature")
			}

			if req.Header.Get("X-Amz-Date") == "" {
				t.Error("X-Amz-Date header is missing")
			}

			if req.Header.Get("X-Amz-Content-Sha256") == "" {
				t.Error("X-Amz-Content-Sha256 header is missing")
			}

			// Verify method
			if req.Method != tt.method {
				t.Errorf("Request method = %q, want %q", req.Method, tt.method)
			}

			// Verify URL
			if !strings.HasPrefix(req.URL.String(), "https://s3.amazonaws.com") {
				t.Errorf("Request URL should start with endpoint, got %q", req.URL.String())
			}
		})
	}
}

func TestBuildCanonicalQueryString(t *testing.T) {
	signer := NewRequestSigner("https://s3.amazonaws.com", "us-east-1")

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "empty query",
			query:    "",
			expected: "",
		},
		{
			name:     "single parameter",
			query:    "prefix=test",
			expected: "prefix=test",
		},
		{
			name:     "multiple parameters sorted",
			query:    "prefix=test&delimiter=/&max-keys=100",
			expected: "delimiter=%2F&max-keys=100&prefix=test",
		},
		{
			name:     "parameters with special characters",
			query:    "prefix=test/path&marker=file name.txt",
			expected: "marker=file%20name.txt&prefix=test%2Fpath", // AWS SigV4 uses %20 for spaces, not +
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, "https://s3.amazonaws.com/bucket?"+tt.query, nil)
			result := signer.buildCanonicalQueryString(req.URL.Query())

			if result != tt.expected {
				t.Errorf("buildCanonicalQueryString() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestCanonicalQueryStringReservedBytes(t *testing.T) {
	signer := NewRequestSigner("https://s3.amazonaws.com", "us-east-1")
	validator := NewRequestValidator(NewCredentialStore())

	tests := []struct {
		name  string
		build func(url.Values) string
	}{
		{name: "signer", build: signer.buildCanonicalQueryString},
		{name: "validator", build: validator.buildCanonicalQueryString},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.build(newReservedCanonicalQueryValues()); got != reservedCanonicalQueryString {
				t.Errorf("buildCanonicalQueryString() = %q, want %q", got, reservedCanonicalQueryString)
			}
		})
	}
}

func TestCanonicalSigningHeaderName(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{"lowercase host", "host", "host"},
		{"mixed-case host", "hOsT", "host"},
		{"lowercase content type", "content-type", "content-type"},
		{"mixed-case content type", "cOnTeNt-TyPe", "content-type"},
		{"lowercase x-amz", "x-amz-meta-color", "x-amz-meta-color"},
		{"mixed-case x-amz", "X-aMz-MeTa-Color", "x-amz-meta-color"},
		{"content encoding", "Content-Encoding", ""},
		{"range", "Range", ""},
		{"conditional", "If-None-Match", ""},
		{"tigris", "Tigris-Force-Delete", ""},
		{"authorization", "Authorization", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := canonicalSigningHeaderName(tt.header); got != tt.want {
				t.Errorf("canonicalSigningHeaderName(%q) = %q, want %q", tt.header, got, tt.want)
			}
		})
	}
}

func TestRequestSignerBuildCanonicalHeadersMixedCase(t *testing.T) {
	signer := NewRequestSigner("https://upstream.example.com", "us-east-1")
	req, err := http.NewRequest(http.MethodGet, "https://upstream.example.com/bucket/key", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header = http.Header{
		"hOsT":                {" signing.example.com "},
		"cOnTeNt-TyPe":        {" application/octet-stream "},
		"X-aMz-MeTa-Color":    {" blue ", "green\t"},
		"Content-Encoding":    {"gzip"},
		"Range":               {"bytes=0-1"},
		"Tigris-Force-Delete": {"true"},
		"X-Tigris-Custom":     {"true"},
	}

	canonicalHeaders, signedHeaders := signer.buildCanonicalHeaders(req)
	wantCanonicalHeaders := "content-type:application/octet-stream\n" +
		"host:signing.example.com\n" +
		"x-amz-meta-color:blue,green\n"
	if canonicalHeaders != wantCanonicalHeaders {
		t.Errorf("canonical headers = %q, want %q", canonicalHeaders, wantCanonicalHeaders)
	}
	const wantSignedHeaders = "content-type;host;x-amz-meta-color"
	if signedHeaders != wantSignedHeaders {
		t.Errorf("signed headers = %q, want %q", signedHeaders, wantSignedHeaders)
	}
}

func TestRequestSignerBuildCanonicalHeadersCoalescesCaseVariants(t *testing.T) {
	signer := NewRequestSigner("https://upstream.example.com", "us-east-1")
	req, err := http.NewRequest(http.MethodGet, "https://upstream.example.com/bucket/key", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header = http.Header{
		"Content-Type":     {"application/json"},
		"content-type":     {"text/plain"},
		"Host":             {"first.example.com"},
		"host":             {"second.example.com"},
		"X-Amz-Meta-Color": {"blue"},
		"x-aMz-mEtA-cOlOr": {"green"},
	}

	canonicalHeaders, signedHeaders := signer.buildCanonicalHeaders(req)
	for _, header := range []struct {
		name   string
		first  string
		second string
	}{
		{"content-type", "application/json", "text/plain"},
		{"host", "first.example.com", "second.example.com"},
		{"x-amz-meta-color", "blue", "green"},
	} {
		if got := strings.Count(canonicalHeaders, header.name+":"); got != 1 {
			t.Errorf("%s count = %d, want 1; canonical headers = %q", header.name, got, canonicalHeaders)
		}
		if !strings.Contains(canonicalHeaders, header.name+":"+header.first+"\n") &&
			!strings.Contains(canonicalHeaders, header.name+":"+header.second+"\n") {
			t.Errorf("canonical headers = %q, want one %s case-variant value", canonicalHeaders, header.name)
		}
	}
	const wantSignedHeaders = "content-type;host;x-amz-meta-color"
	if signedHeaders != wantSignedHeaders {
		t.Errorf("signed headers = %q, want %q", signedHeaders, wantSignedHeaders)
	}
}

func TestHasPrefixFold(t *testing.T) {
	for _, tt := range []struct {
		key    string
		prefix string
		want   bool
	}{
		{"X-AmZ-Meta-Color", "x-amz-", true},
		{"tIgRiS-Force-Delete", "tigris-", true},
		{"x-am", "x-amz-", false},
		{"Content-Type", "x-amz-", false},
	} {
		if got := hasPrefixFold(tt.key, tt.prefix); got != tt.want {
			t.Errorf("hasPrefixFold(%q, %q) = %v, want %v", tt.key, tt.prefix, got, tt.want)
		}
	}
}

func TestShouldCopyHeader(t *testing.T) {
	tests := []struct {
		header   string
		expected bool
	}{
		// Content headers
		{"Content-Type", true},
		{"Content-Length", true},
		{"Content-Encoding", true},
		{"Content-Disposition", true},
		{"Content-Language", true},
		{"Cache-Control", true},
		{"Expires", true},
		{"Content-MD5", true},
		{"cOnTeNt-TyPe", true},
		{"cOnTeNt-LaNgUaGe", true},
		// All x-amz-* headers are allowed (signer overwrites X-Amz-Date etc.)
		{"X-Amz-Meta-Custom", true},
		{"x-AmZ-mEtA-CuStOm", true},
		{"X-Amz-Meta-Another-Header", true},
		{"X-Amz-Date", true},
		{"X-Amz-Content-Sha256", true},
		{"X-Amz-Copy-Source", true},
		{"X-Amz-Tagging", true},
		// Tigris-specific headers (tigris-* and x-tigris-*)
		{"Tigris-Force-Delete", true},
		{"X-Tigris-Custom", true},
		{"tIgRiS-FoRcE-DeLeTe", true},
		// Proxy headers are blocked to prevent client injection in signing mode
		{"X-Tigris-Forwarded-Host", false},
		{"x-TiGrIs-FoRwArDeD-HoSt", false},
		{"X-Tigris-Proxy-Access-Key", false},
		{"X-Tigris-Proxy-Timestamp", false},
		{"X-Tigris-Proxy-Signature", false},
		// Conditional request headers
		{"If-Match", true},
		{"If-None-Match", true},
		{"If-Modified-Since", true},
		{"If-Unmodified-Since", true},
		{"iF-NoNe-MaTcH", true},
		{"iF-UnMoDiFiEd-SiNcE", true},
		// Not copied
		{"", false},
		{"Authorization", false},
		{"Host", false},
		{"Random-Header", false},
	}

	for _, tt := range tests {
		t.Run(tt.header, func(t *testing.T) {
			result := shouldCopyHeader(tt.header)
			if result != tt.expected {
				t.Errorf("shouldCopyHeader(%q) = %v, want %v", tt.header, result, tt.expected)
			}
		})
	}
}

func TestHashSHA256(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "",
			expected: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			input:    "test",
			expected: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := hashSHA256([]byte(tt.input))
			if result != tt.expected {
				t.Errorf("hashSHA256(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseHTTPDate(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "ISO 8601", input: "20260211T055514Z", wantErr: false},
		{name: "RFC 1123", input: "Wed, 11 Feb 2026 05:55:14 GMT", wantErr: false},
		{name: "RFC 1123Z", input: "Wed, 11 Feb 2026 05:55:14 -0000", wantErr: false},
		{name: "invalid format", input: "not-a-date", wantErr: true},
		{name: "empty string", input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseHTTPDate(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseHTTPDate(%q) error = nil, want error", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseHTTPDate(%q) error = %v, want nil", tt.input, err)
			}
			if got.IsZero() {
				t.Errorf("ParseHTTPDate(%q) returned zero time", tt.input)
			}
		})
	}
}

func TestAwsURIEncode(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		encodeSlash bool
		expected    string
	}{
		// Unreserved characters should not be encoded
		{name: "lowercase letters", input: "abcxyz", encodeSlash: true, expected: "abcxyz"},
		{name: "uppercase letters", input: "ABCXYZ", encodeSlash: true, expected: "ABCXYZ"},
		{name: "digits", input: "0123456789", encodeSlash: true, expected: "0123456789"},
		{name: "unreserved special", input: "_-~.", encodeSlash: true, expected: "_-~."},

		// Spaces should be %20, not +
		{name: "space", input: "hello world", encodeSlash: true, expected: "hello%20world"},
		{name: "multiple spaces", input: "a b c", encodeSlash: true, expected: "a%20b%20c"},

		// Slash handling
		{name: "slash with encodeSlash true", input: "a/b/c", encodeSlash: true, expected: "a%2Fb%2Fc"},
		{name: "slash with encodeSlash false", input: "a/b/c", encodeSlash: false, expected: "a/b/c"},

		// Special characters should be percent-encoded
		{name: "at sign", input: "user@example.com", encodeSlash: true, expected: "user%40example.com"},
		{name: "ampersand", input: "a&b", encodeSlash: true, expected: "a%26b"},
		{name: "equals", input: "a=b", encodeSlash: true, expected: "a%3Db"},
		{name: "plus sign", input: "a+b", encodeSlash: true, expected: "a%2Bb"},
		{name: "percent", input: "100%", encodeSlash: true, expected: "100%25"},
		{name: "colon", input: "a:b", encodeSlash: true, expected: "a%3Ab"},
		{name: "question mark", input: "a?b", encodeSlash: true, expected: "a%3Fb"},
		{name: "hash", input: "a#b", encodeSlash: true, expected: "a%23b"},

		// Mixed content
		{name: "path with space", input: "path/to/my file.txt", encodeSlash: false, expected: "path/to/my%20file.txt"},
		{name: "query param value", input: "hello world!", encodeSlash: true, expected: "hello%20world%21"},

		// Empty string
		{name: "empty string", input: "", encodeSlash: true, expected: ""},

		// Unicode characters should be UTF-8 encoded then percent-encoded
		{name: "unicode", input: "日本", encodeSlash: true, expected: "%E6%97%A5%E6%9C%AC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := awsURIEncode(tt.input, tt.encodeSlash)
			if result != tt.expected {
				t.Errorf("awsURIEncode(%q, %v) = %q, want %q",
					tt.input, tt.encodeSlash, result, tt.expected)
			}
		})
	}
}
