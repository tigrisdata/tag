package integration

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestAuth_InvalidSignature verifies that requests with invalid signatures are rejected.
// This test requires manual request construction because the AWS SDK won't send invalid signatures.
func TestAuth_InvalidSignature(t *testing.T) {
	upstreamCalled := false

	env := NewTestEnvironmentWithHandler(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		w.WriteHeader(http.StatusOK)
	})
	defer env.Close()

	// Create request with tampered/invalid signature
	req := env.RequestWithInvalidSignature(http.MethodGet, "/test-bucket/test-key")

	// Execute request using real HTTP client (not httptest)
	client := &http.Client{}
	actualReq, _ := http.NewRequest(http.MethodGet, env.TAGServer.URL+"/test-bucket/test-key", nil)
	actualReq.Header = req.Header

	resp, err := client.Do(actualReq)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	// Verify auth failure - should be 403 Forbidden
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("Expected status 403, got %d", resp.StatusCode)
	}

	// Verify upstream was NOT called
	if upstreamCalled {
		t.Error("Upstream should not be called for invalid signature")
	}

	// Verify S3 error response format
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "<Error>") {
		t.Errorf("Expected S3 error XML in response, got %q", body)
	}

	if !strings.Contains(string(body), "SignatureDoesNotMatch") {
		t.Errorf("Expected SignatureDoesNotMatch error code, got %q", body)
	}
}

// TestAuth_UnknownAccessKey verifies that requests with unknown access keys are rejected.
// This test requires manual request construction because the AWS SDK uses configured credentials.
func TestAuth_UnknownAccessKey(t *testing.T) {
	upstreamCalled := false

	env := NewTestEnvironmentWithHandler(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		w.WriteHeader(http.StatusOK)
	})
	defer env.Close()

	// Create request with unknown access key
	req := env.RequestWithUnknownAccessKey(http.MethodGet, "/test-bucket/test-key")

	// Execute request
	client := &http.Client{}
	actualReq, _ := http.NewRequest(http.MethodGet, env.TAGServer.URL+"/test-bucket/test-key", nil)
	actualReq.Header = req.Header

	resp, err := client.Do(actualReq)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	// Verify auth failure
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("Expected status 403, got %d", resp.StatusCode)
	}

	// Verify upstream was NOT called
	if upstreamCalled {
		t.Error("Upstream should not be called for unknown access key")
	}

	// Verify S3 error response
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "<Error>") {
		t.Errorf("Expected S3 error XML in response, got %q", body)
	}

	if !strings.Contains(string(body), "InvalidAccessKeyId") {
		t.Errorf("Expected InvalidAccessKeyId error code, got %q", body)
	}
}

// TestAuth_MissingAuthorization verifies that requests without Authorization header are rejected.
func TestAuth_MissingAuthorization(t *testing.T) {
	upstreamCalled := false

	env := NewTestEnvironmentWithHandler(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		w.WriteHeader(http.StatusOK)
	})
	defer env.Close()

	// Create request with no Authorization header
	client := &http.Client{}
	req, _ := http.NewRequest(http.MethodGet, env.TAGServer.URL+"/test-bucket/test-key", nil)
	// Don't set any auth headers

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	// Should fail with 400 or 403
	if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusForbidden {
		t.Errorf("Expected status 400 or 403, got %d", resp.StatusCode)
	}

	// Verify upstream was NOT called
	if upstreamCalled {
		t.Error("Upstream should not be called for missing authorization")
	}
}

// TestAuth_ExpiredRequest verifies that requests with expired timestamps are rejected.
func TestAuth_ExpiredRequest(t *testing.T) {
	upstreamCalled := false

	env := NewTestEnvironmentWithHandler(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		w.WriteHeader(http.StatusOK)
	})
	defer env.Close()

	// Create request with old timestamp (uses 2023-01-01 which is definitely expired)
	req := env.RequestWithExpiredTimestamp(http.MethodGet, "/test-bucket/test-key")

	// Execute request
	client := &http.Client{}
	actualReq, _ := http.NewRequest(http.MethodGet, env.TAGServer.URL+"/test-bucket/test-key", nil)
	actualReq.Header = req.Header

	resp, err := client.Do(actualReq)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	// Verify auth failure (expired requests return 403 with AccessDenied)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("Expected status 403, got %d", resp.StatusCode)
	}

	// Verify upstream was NOT called
	if upstreamCalled {
		t.Error("Upstream should not be called for expired request")
	}
}

// TestAuth_WrongCredentials verifies that SDK requests with wrong credentials fail.
func TestAuth_WrongCredentials(t *testing.T) {
	upstreamCalled := false

	env := NewTestEnvironmentWithHandler(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		w.WriteHeader(http.StatusOK)
	})
	defer env.Close()

	// Use SDK client with wrong credentials
	client := env.GetS3ClientWithCreds("WRONGACCESSKEY", "WRONGSECRETKEY")

	_, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-key"),
	})

	// Should fail with auth error
	if err == nil {
		t.Error("Expected auth error for wrong credentials")
	}

	// Verify upstream was NOT called
	if upstreamCalled {
		t.Error("Upstream should not be called for wrong credentials")
	}
}
