package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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
