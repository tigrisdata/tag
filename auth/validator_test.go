package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
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

// A learned derived key must re-sign a changed request without substituting
// TAG's background identity. Compare with the SDK, not TAG's own validator.
func TestValidatedRequestResign(t *testing.T) {
	const accessKey = "caller-access-key"
	const secretKey = "caller-secret-key"
	for _, derived := range []bool{false, true} {
		t.Run(fmt.Sprintf("derived=%v", derived), func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			r := httptest.NewRequest(http.MethodPut, "https://objects.example/bucket/a%20key?x=two&x=one", nil)
			r.Header.Set("If-Match", `"local-only-etag"`)
			r.Header.Set("X-Amz-Content-Sha256", emptyBodyHash)
			r.Header.Add("X-Amz-Meta-Colour", "blue")
			r.Header.Add("X-Amz-Meta-Colour", "green")
			creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}
			sdk := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
			if err := sdk.SignHTTP(t.Context(), creds, r, emptyBodyHash, "s3", "us-east-1", now); err != nil {
				t.Fatal(err)
			}
			store := NewCredentialStore()
			store.AddCredential(accessKey, secretKey)
			validator := NewRequestValidator(store)
			if derived {
				keys := NewDerivedKeyStore(time.Hour)
				date := now.Format("20060102")
				keys.Store(accessKey, date, "us-east-1", NewRequestSigner("https://objects.example", "us-east-1").deriveSigningKey(secretKey, date))
				validator = NewRequestValidator(keys)
			}
			// Match net/http's inbound request shape: Host is not URL.Host.
			r.URL.Scheme, r.URL.Host = "", ""
			vr, err := validator.Validate(r)
			if err != nil {
				t.Fatalf("validate caller: %v", err)
			}
			r.Header.Del("If-Match")
			vr.Resign(r)
			got := r.Header.Get("Authorization")
			expected := r.Clone(t.Context())
			expected.Header.Del("Host")
			if err := sdk.SignHTTP(t.Context(), creds, expected, emptyBodyHash, "s3", "us-east-1", now); err != nil {
				t.Fatal(err)
			}
			if got != expected.Header.Get("Authorization") {
				t.Fatalf("re-signed caller request differs from SDK: got %q, want %q", got, expected.Header.Get("Authorization"))
			}
		})
	}
}

func TestValidatedRequestResignPresigned(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	credentials := aws.Credentials{AccessKeyID: "caller", SecretAccessKey: "secret", SessionToken: "session-token"}
	r := httptest.NewRequest(http.MethodPut, "https://objects.example/bucket/a%20key?X-Amz-Expires=60", nil)
	r.Header.Set("If-Match", `"local-only-etag"`)
	r.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
	sdk := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true; o.DisableHeaderHoisting = true })
	uri, _, err := sdk.PresignHTTP(t.Context(), credentials, r, unsignedPayload, "s3", "us-east-1", now)
	if err != nil {
		t.Fatal(err)
	}
	r.URL, err = url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	store := NewCredentialStore()
	store.AddCredential(credentials.AccessKeyID, credentials.SecretAccessKey)
	vr, err := NewRequestValidator(store).Validate(r)
	if err != nil {
		t.Fatal(err)
	}
	r.URL.Scheme, r.URL.Host = "", ""
	r.Header.Del("If-Match")
	vr.Resign(r)
	if r.Header.Get("Authorization") != "" {
		t.Fatal("re-signing replaced query authentication with header authentication")
	}
	expected := r.Clone(t.Context())
	expected.Header.Del("Host")
	query := expected.URL.Query()
	query.Del("X-Amz-Signature")
	expected.URL.RawQuery = query.Encode()
	uri, _, err = sdk.PresignHTTP(t.Context(), credentials, expected, unsignedPayload, "s3", "us-east-1", now)
	if err != nil {
		t.Fatal(err)
	}
	sdkURL, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.URL.Query(), sdkURL.Query()) {
		t.Fatalf("re-signed query differs from SDK: got %v, want %v", r.URL.Query(), sdkURL.Query())
	}
}
