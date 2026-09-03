package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	testAccessKey = "AKIAIOSFODNN7EXAMPLE"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	testRegion    = "us-east-1"
)

func TestRequestValidator_ValidateRequest(t *testing.T) {
	// Create a credential store with test credentials
	credStore := NewCredentialStore()
	credStore.AddCredential("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

	validator := NewRequestValidator(credStore)
	signer := NewRequestSigner("https://s3.amazonaws.com", "us-east-1")

	tests := []struct {
		name        string
		method      string
		path        string
		body        []byte
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid GET request",
			method:  http.MethodGet,
			path:    "/test-bucket/test-key",
			body:    nil,
			wantErr: false,
		},
		{
			name:    "valid PUT request with body",
			method:  http.MethodPut,
			path:    "/test-bucket/test-key",
			body:    []byte("test content"),
			wantErr: false,
		},
		{
			name:    "valid DELETE request",
			method:  http.MethodDelete,
			path:    "/test-bucket/test-key",
			body:    nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Compute body hash like AWS SDKs do
			var bodyReader io.Reader
			var bodyHash string
			if len(tt.body) > 0 {
				bodyReader = bytes.NewReader(tt.body)
				h := sha256.Sum256(tt.body)
				bodyHash = hex.EncodeToString(h[:])
			}

			signedReq, err := signer.SignRequest(
				t.Context(),
				tt.method,
				tt.path,
				bodyReader,
				bodyHash,
				"AKIAIOSFODNN7EXAMPLE",
				"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				http.Header{},
			)
			if err != nil {
				t.Fatalf("Failed to sign request: %v", err)
			}

			// Create a new request for validation (simulate incoming request)
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader(tt.body))
			req.Header = signedReq.Header.Clone()
			req.Host = signedReq.Host

			// Validate the request
			accessKey, err := validator.ValidateRequest(req)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateRequest() error = nil, want error containing %q", tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateRequest() error = %v, want nil", err)
				}
				if accessKey != "AKIAIOSFODNN7EXAMPLE" {
					t.Errorf("ValidateRequest() accessKey = %q, want %q", accessKey, "AKIAIOSFODNN7EXAMPLE")
				}
			}
		})
	}
}

func TestRequestValidator_ValidateRequest_InvalidSignature(t *testing.T) {
	credStore := NewCredentialStore()
	credStore.AddCredential("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

	validator := NewRequestValidator(credStore)

	// Create a request with tampered signature
	req := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=invalidsignature")
	req.Header.Set("X-Amz-Date", time.Now().UTC().Format("20060102T150405Z"))
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	_, err := validator.ValidateRequest(req)
	if err == nil {
		t.Error("ValidateRequest() should fail with invalid signature")
	}
}

func TestRequestValidator_ValidateRequest_UnknownAccessKey(t *testing.T) {
	credStore := NewCredentialStore()
	// Don't add any credentials

	validator := NewRequestValidator(credStore)

	req := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=UNKNOWNACCESSKEY/20230101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=test")
	req.Header.Set("X-Amz-Date", time.Now().UTC().Format("20060102T150405Z"))
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	_, err := validator.ValidateRequest(req)
	if err == nil {
		t.Error("ValidateRequest() should fail with unknown access key")
	}
}

func TestRequestValidator_ValidateRequest_ExpiredRequest(t *testing.T) {
	credStore := NewCredentialStore()
	credStore.AddCredential("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

	validator := NewRequestValidator(credStore)

	// Create a request with an old timestamp
	oldTime := time.Now().Add(-30 * time.Minute).UTC()
	req := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"+oldTime.Format("20060102")+"/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=test")
	req.Header.Set("X-Amz-Date", oldTime.Format("20060102T150405Z"))
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	_, err := validator.ValidateRequest(req)
	if err == nil {
		t.Error("ValidateRequest() should fail with expired request")
	}
}

func TestRequestValidator_ValidateRequest_MissingContentHash(t *testing.T) {
	credStore := NewCredentialStore()
	credStore.AddCredential("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

	validator := NewRequestValidator(credStore)

	// Create a request without X-Amz-Content-Sha256 header
	req := httptest.NewRequest(http.MethodGet, "/test-bucket/test-key", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=test")
	req.Header.Set("X-Amz-Date", time.Now().UTC().Format("20060102T150405Z"))
	// Intentionally not setting X-Amz-Content-Sha256

	_, err := validator.ValidateRequest(req)
	if err == nil {
		t.Error("ValidateRequest() should fail when X-Amz-Content-Sha256 is missing")
	}
	if err != ErrMissingContentHash {
		t.Errorf("ValidateRequest() error = %v, want %v", err, ErrMissingContentHash)
	}
}

func TestRequestValidator_ValidateRequest_PresignedRead(t *testing.T) {
	validator := newTestRequestValidator()
	signingTime := time.Now().UTC().Truncate(time.Second)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			req := newPresignedTestRequest(t, method, "/test-bucket/test-key", signingTime, 15*time.Minute, nil)

			accessKey, err := validator.ValidateRequest(req)
			if err != nil {
				t.Fatalf("ValidateRequest() error = %v, want nil", err)
			}
			if accessKey != testAccessKey {
				t.Errorf("ValidateRequest() accessKey = %q, want %q", accessKey, testAccessKey)
			}
			if req.Header.Get("X-Amz-Content-Sha256") != "" {
				t.Error("presigned request unexpectedly has X-Amz-Content-Sha256 header")
			}
		})
	}
}

func TestRequestValidator_ValidateRequest_PresignedExtraQuery(t *testing.T) {
	validator := newTestRequestValidator()
	extraQuery := url.Values{"response-content-disposition": {`attachment; filename="test.txt"`}}
	req := newPresignedTestRequest(
		t,
		http.MethodGet,
		"/test-bucket/test-key",
		time.Now().UTC().Truncate(time.Second),
		15*time.Minute,
		extraQuery,
	)

	if _, err := validator.ValidateRequest(req); err != nil {
		t.Fatalf("ValidateRequest() error = %v, want nil", err)
	}
}

func TestRequestValidator_ValidateRequest_PresignedInvalidSignature(t *testing.T) {
	validator := newTestRequestValidator()
	req := newPresignedTestRequest(
		t,
		http.MethodGet,
		"/test-bucket/test-key",
		time.Now().UTC().Truncate(time.Second),
		15*time.Minute,
		nil,
	)
	query := req.URL.Query()
	query.Set("X-Amz-Signature", strings.Repeat("0", 64))
	req.URL.RawQuery = query.Encode()

	_, err := validator.ValidateRequest(req)
	if !errors.Is(err, ErrSignatureMismatch) {
		t.Fatalf("ValidateRequest() error = %v, want %v", err, ErrSignatureMismatch)
	}
}

func TestRequestValidator_ValidateRequest_PresignedExpiry(t *testing.T) {
	tests := []struct {
		name        string
		signingTime time.Time
		expires     time.Duration
		mutate      func(url.Values)
		wantErr     error
	}{
		{
			name:        "expired",
			signingTime: time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second),
			expires:     time.Minute,
			wantErr:     ErrExpiredRequest,
		},
		{
			name:        "future beyond clock skew",
			signingTime: time.Now().UTC().Add(maxRequestAge + time.Minute).Truncate(time.Second),
			expires:     15 * time.Minute,
			wantErr:     ErrExpiredRequest,
		},
		{
			name:        "missing date",
			signingTime: time.Now().UTC().Truncate(time.Second),
			expires:     15 * time.Minute,
			mutate:      func(query url.Values) { query.Del("X-Amz-Date") },
			wantErr:     ErrInvalidDate,
		},
		{
			name:        "invalid date",
			signingTime: time.Now().UTC().Truncate(time.Second),
			expires:     15 * time.Minute,
			mutate:      func(query url.Values) { query.Set("X-Amz-Date", "invalid") },
			wantErr:     ErrInvalidDate,
		},
		{
			name:        "credential scope date mismatch",
			signingTime: time.Now().UTC().Truncate(time.Second),
			expires:     15 * time.Minute,
			mutate: func(query url.Values) {
				scopeDate := time.Now().UTC().AddDate(0, 0, -1).Format(shortTimeFormat)
				query.Set(
					"X-Amz-Credential",
					testAccessKey+"/"+scopeDate+"/"+testRegion+"/"+service+"/"+terminationString,
				)
			},
			wantErr: ErrInvalidDate,
		},
		{
			name:        "missing expires",
			signingTime: time.Now().UTC().Truncate(time.Second),
			expires:     15 * time.Minute,
			mutate:      func(query url.Values) { query.Del("X-Amz-Expires") },
			wantErr:     ErrInvalidAuthFormat,
		},
		{
			name:        "non-numeric expires",
			signingTime: time.Now().UTC().Truncate(time.Second),
			expires:     15 * time.Minute,
			mutate:      func(query url.Values) { query.Set("X-Amz-Expires", "invalid") },
			wantErr:     ErrInvalidAuthFormat,
		},
		{
			name:        "zero expires",
			signingTime: time.Now().UTC().Truncate(time.Second),
			expires:     15 * time.Minute,
			mutate:      func(query url.Values) { query.Set("X-Amz-Expires", "0") },
			wantErr:     ErrInvalidAuthFormat,
		},
		{
			name:        "over seven days",
			signingTime: time.Now().UTC().Truncate(time.Second),
			expires:     15 * time.Minute,
			mutate:      func(query url.Values) { query.Set("X-Amz-Expires", "604801") },
			wantErr:     ErrInvalidAuthFormat,
		},
		{
			name:        "duration overflow",
			signingTime: time.Now().UTC().Truncate(time.Second),
			expires:     15 * time.Minute,
			mutate:      func(query url.Values) { query.Set("X-Amz-Expires", "18446830473") },
			wantErr:     ErrInvalidAuthFormat,
		},
	}

	validator := newTestRequestValidator()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newPresignedTestRequest(t, http.MethodGet, "/test-bucket/test-key", tt.signingTime, tt.expires, nil)
			if tt.mutate != nil {
				query := req.URL.Query()
				tt.mutate(query)
				req.URL.RawQuery = query.Encode()
			}

			_, err := validator.ValidateRequest(req)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateRequest() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRequestValidator_ValidateRequest_PresignedMaxExpiry(t *testing.T) {
	validator := newTestRequestValidator()
	req := newPresignedTestRequest(
		t,
		http.MethodGet,
		"/test-bucket/test-key",
		time.Now().UTC().Truncate(time.Second),
		maxPresignedExpiry,
		nil,
	)

	if _, err := validator.ValidateRequest(req); err != nil {
		t.Fatalf("ValidateRequest() error = %v, want nil", err)
	}
}

func TestRequestValidator_ValidateRequest_PresignedWriteRequiresContentHash(t *testing.T) {
	validator := newTestRequestValidator()
	req := newPresignedTestRequest(
		t,
		http.MethodPut,
		"/test-bucket/test-key",
		time.Now().UTC().Truncate(time.Second),
		15*time.Minute,
		nil,
	)

	_, err := validator.ValidateRequest(req)
	if !errors.Is(err, ErrMissingContentHash) {
		t.Fatalf("ValidateRequest() error = %v, want %v", err, ErrMissingContentHash)
	}
}

func newTestRequestValidator() *RequestValidator {
	credStore := NewCredentialStore()
	credStore.AddCredential(testAccessKey, testSecretKey)
	return NewRequestValidator(credStore)
}

func newPresignedTestRequest(
	t *testing.T,
	method, path string,
	signingTime time.Time,
	expires time.Duration,
	extraQuery url.Values,
) *http.Request {
	t.Helper()

	req := httptest.NewRequest(method, "https://s3.amazonaws.com"+path, nil)
	req.Host = req.URL.Host
	query := req.URL.Query()
	for key, values := range extraQuery {
		for _, value := range values {
			query.Add(key, value)
		}
	}

	credentialScope := strings.Join([]string{
		signingTime.Format(shortTimeFormat),
		testRegion,
		service,
		terminationString,
	}, "/")
	query.Set("X-Amz-Algorithm", algorithm)
	query.Set("X-Amz-Credential", testAccessKey+"/"+credentialScope)
	query.Set("X-Amz-Date", signingTime.Format(TimeFormat))
	query.Set("X-Amz-Expires", strconv.FormatInt(int64(expires/time.Second), 10))
	query.Set("X-Amz-SignedHeaders", "host")
	req.URL.RawQuery = query.Encode()

	signer := NewRequestSigner("https://s3.amazonaws.com", testRegion)
	canonicalRequest := strings.Join([]string{
		method,
		awsURIEncode(req.URL.Path, false),
		signer.buildCanonicalQueryString(query),
		"host:" + req.Host + "\n",
		"host",
		unsignedPayload,
	}, "\n")
	stringToSign := signer.buildStringToSign(signingTime, credentialScope, canonicalRequest)
	signingKey := signer.deriveSigningKey(testSecretKey, signingTime.Format(shortTimeFormat))
	query.Set("X-Amz-Signature", hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign))))
	req.URL.RawQuery = query.Encode()

	return req
}
