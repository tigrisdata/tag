package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"
)

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

func TestShouldCopyHeader(t *testing.T) {
	tests := []struct {
		header   string
		expected bool
	}{
		{"Content-Type", true},
		{"Content-Length", true},
		{"Content-Encoding", true},
		{"Content-Disposition", true},
		{"Cache-Control", true},
		{"Expires", true},
		{"Content-MD5", true},
		{"X-Amz-Meta-Custom", true},
		{"X-Amz-Meta-Another-Header", true},
		{"Authorization", false},
		{"X-Amz-Date", false},
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
