package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestComputeProxyHeaders(t *testing.T) {
	signer := NewProxySigner("test-access-key", "test-secret-key")

	headers := signer.ComputeProxyHeaders("cache.customer.com", "GET", "/bucket/object.jpg")

	if headers.ForwardedHost != "cache.customer.com" {
		t.Errorf("ForwardedHost = %q, want %q", headers.ForwardedHost, "cache.customer.com")
	}
	if headers.ProxyAccessKey != "test-access-key" {
		t.Errorf("ProxyAccessKey = %q, want %q", headers.ProxyAccessKey, "test-access-key")
	}

	// Verify timestamp is a valid Unix timestamp within 5 seconds of now
	ts, err := strconv.ParseInt(headers.ProxyTimestamp, 10, 64)
	if err != nil {
		t.Fatalf("ProxyTimestamp %q is not a valid integer: %v", headers.ProxyTimestamp, err)
	}
	now := time.Now().Unix()
	if ts < now-5 || ts > now+5 {
		t.Errorf("ProxyTimestamp %d is not within 5 seconds of now (%d)", ts, now)
	}

	// Verify signature is a valid hex string
	sigBytes, err := hex.DecodeString(headers.ProxySignature)
	if err != nil {
		t.Fatalf("ProxySignature %q is not valid hex: %v", headers.ProxySignature, err)
	}
	// HMAC-SHA256 produces 32 bytes
	if len(sigBytes) != 32 {
		t.Errorf("ProxySignature decoded to %d bytes, want 32", len(sigBytes))
	}

	// Verify the signature matches the expected HMAC computation
	canonical := fmt.Sprintf("%s\n%s\n%s\n%s", "cache.customer.com", headers.ProxyTimestamp, "GET", "/bucket/object.jpg")
	mac := hmac.New(sha256.New, []byte("test-secret-key"))
	mac.Write([]byte(canonical))
	expected := hex.EncodeToString(mac.Sum(nil))
	if headers.ProxySignature != expected {
		t.Errorf("ProxySignature = %q, want %q", headers.ProxySignature, expected)
	}
}

func TestComputeProxyHeaders_DifferentMethods(t *testing.T) {
	signer := NewProxySigner("ak", "sk")

	methods := []string{"GET", "PUT", "DELETE", "HEAD", "POST"}
	signatures := make(map[string]bool)

	for _, method := range methods {
		headers := signer.ComputeProxyHeaders("host.example.com", method, "/bucket/key")
		if headers.ProxySignature == "" {
			t.Errorf("Empty signature for method %s", method)
		}
		signatures[headers.ProxySignature] = true
	}

	// All methods should produce different signatures (since method is part of canonical string)
	// Note: there's a tiny chance of collision, but practically impossible with SHA256
	if len(signatures) != len(methods) {
		t.Errorf("Expected %d unique signatures, got %d", len(methods), len(signatures))
	}
}

func TestProxySignerAccessors(t *testing.T) {
	signer := NewProxySigner("my-access-key", "my-secret-key")

	if signer.AccessKey() != "my-access-key" {
		t.Errorf("AccessKey() = %q, want %q", signer.AccessKey(), "my-access-key")
	}
	if signer.SecretKey() != "my-secret-key" {
		t.Errorf("SecretKey() = %q, want %q", signer.SecretKey(), "my-secret-key")
	}
}
