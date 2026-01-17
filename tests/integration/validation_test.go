package integration

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestValidation_MissingContentLength verifies that PUT requests without Content-Length are rejected.
func TestValidation_MissingContentLength(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()
	env.CreateTestBucket("test-bucket")

	// Create PUT request without Content-Length
	// We use TransferEncoding: chunked with ContentLength = -1 to omit Content-Length header
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodPut, env.TAGServer.URL+"/test-bucket/test-key", strings.NewReader("content"))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.ContentLength = -1 // Force no Content-Length by using chunked transfer

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	// Should return 411 Length Required
	if resp.StatusCode != http.StatusLengthRequired {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 411 Length Required, got %d. Body: %s", resp.StatusCode, body)
		return
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "MissingContentLength") {
		t.Errorf("Expected MissingContentLength error code, got %s", body)
	}
}

// TestValidation_ContentLengthZero verifies that PUT requests with Content-Length: 0 are accepted.
// This tests the edge case where an empty object is being created.
func TestValidation_ContentLengthZero(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()
	env.CreateTestBucket("test-bucket")

	client := &http.Client{}

	// Sign the request with empty body - this sets Content-Length: 0
	signedReq, err := env.SignedRequest(http.MethodPut, "/test-bucket/empty-key", []byte{})
	if err != nil {
		t.Fatalf("Failed to sign request: %v", err)
	}

	resp, err := client.Do(signedReq)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	// Should succeed (200 OK) - zero-length objects are valid
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200 OK, got %d. Body: %s", resp.StatusCode, body)
	}
}

// TestValidation_ValidContentLength verifies that PUT requests with valid Content-Length succeed.
func TestValidation_ValidContentLength(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()
	env.CreateTestBucket("test-bucket")

	client := &http.Client{}
	content := "valid content"

	// Sign the request - this sets proper Content-Length
	signedReq, err := env.SignedRequest(http.MethodPut, "/test-bucket/test-key", []byte(content))
	if err != nil {
		t.Fatalf("Failed to sign request: %v", err)
	}

	resp, err := client.Do(signedReq)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	// Should succeed (200 OK)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200 OK, got %d. Body: %s", resp.StatusCode, body)
	}
}
