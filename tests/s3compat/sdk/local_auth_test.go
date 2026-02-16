package sdk

import (
	"io"
	"net/http"
	"testing"
	"time"
)

// TestSDK_LocalAuth_CacheHitAfterKeyLearning verifies that local auth enables
// cache hits after signing keys are learned from Tigris.
//
// Flow:
//  1. PUT object → Tigris stores it
//  2. First GET → TAG forwards to Tigris (unknown signing key), learns signing key from response, caches object
//  3. Second GET → TAG validates locally (known signing key + authz), serves from cache (X-Cache: HIT)
func TestSDK_LocalAuth_CacheHitAfterKeyLearning(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("local-auth-cache")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	content := []byte("local auth cache test content")
	if err := globalEnv.PutTestObject(bucket, "auth-cache-key", content); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	// First GET: forwards to Tigris, learns signing keys, populates cache
	resp1, err := globalEnv.DoRawGet(bucket, "auth-cache-key")
	if err != nil {
		t.Fatalf("First raw GET failed: %v", err)
	}
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()

	if resp1.StatusCode != 200 {
		t.Fatalf("First GET: expected status 200, got %d", resp1.StatusCode)
	}
	if string(body1) != string(content) {
		t.Errorf("First GET: expected body %q, got %q", content, body1)
	}

	// First request should be a cache miss (signing keys not yet known)
	xCache1 := resp1.Header.Get("X-Cache")
	t.Logf("First GET X-Cache: %s", xCache1)

	// Wait for cache to be populated
	time.Sleep(500 * time.Millisecond)

	// Second GET: local auth validates (signing key learned), serves from cache
	resp2, err := globalEnv.DoRawGet(bucket, "auth-cache-key")
	if err != nil {
		t.Fatalf("Second raw GET failed: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if resp2.StatusCode != 200 {
		t.Fatalf("Second GET: expected status 200, got %d", resp2.StatusCode)
	}
	if string(body2) != string(content) {
		t.Errorf("Second GET: expected body %q, got %q", content, body2)
	}

	xCache2 := resp2.Header.Get("X-Cache")
	if xCache2 != "HIT" {
		t.Errorf("Second GET: expected X-Cache HIT, got %q (local auth may not have validated)", xCache2)
	}

	t.Logf("Local auth cache hit verified: first=%s, second=%s", xCache1, xCache2)
}

// TestSDK_LocalAuth_InternalHeadersStripped verifies that the internal
// X-Tigris-Proxy-Signing-Keys header is never exposed to clients.
func TestSDK_LocalAuth_InternalHeadersStripped(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("local-auth-headers")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	content := []byte("header stripping test content")
	if err := globalEnv.PutTestObject(bucket, "header-strip-key", content); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	// GET request - the response from Tigris may contain X-Tigris-Proxy-Signing-Keys
	// but TAG's response interceptor must strip it before it reaches the client.
	resp, err := globalEnv.DoRawGet(bucket, "header-strip-key")
	if err != nil {
		t.Fatalf("Raw GET failed: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	// The signing keys header must never be visible to clients
	if val := resp.Header.Get("X-Tigris-Proxy-Signing-Keys"); val != "" {
		t.Errorf("Internal header X-Tigris-Proxy-Signing-Keys leaked to client (length=%d)", len(val))
	}

	t.Logf("Internal header stripping verified")
}

// TestSDK_LocalAuth_MultipleAccessPatterns verifies that local auth correctly
// tracks per-bucket authorization across multiple buckets.
func TestSDK_LocalAuth_MultipleAccessPatterns(t *testing.T) {
	bucketA, err := globalEnv.CreateTestBucket("local-auth-multi-a")
	if err != nil {
		t.Fatalf("Failed to create bucket A: %v", err)
	}
	bucketB, err := globalEnv.CreateTestBucket("local-auth-multi-b")
	if err != nil {
		t.Fatalf("Failed to create bucket B: %v", err)
	}

	contentA := []byte("content for bucket A")
	contentB := []byte("content for bucket B")

	if err := globalEnv.PutTestObject(bucketA, "key-a", contentA); err != nil {
		t.Fatalf("Failed to put object in bucket A: %v", err)
	}
	if err := globalEnv.PutTestObject(bucketB, "key-b", contentB); err != nil {
		t.Fatalf("Failed to put object in bucket B: %v", err)
	}

	// First GET from bucket A: learns signing keys + grants authz for bucket A
	resp1, err := globalEnv.DoRawGet(bucketA, "key-a")
	if err != nil {
		t.Fatalf("First GET (bucket A) failed: %v", err)
	}
	io.ReadAll(resp1.Body)
	resp1.Body.Close()

	// Bucket A first request must not be a cache HIT (no signing keys known yet)
	xCacheA1 := resp1.Header.Get("X-Cache")
	if xCacheA1 == "HIT" {
		t.Errorf("First GET (bucket A): expected X-Cache != HIT (should forward to upstream), got %q", xCacheA1)
	}
	t.Logf("Bucket A first GET forwarded to upstream: X-Cache=%s", xCacheA1)

	// First GET from bucket B: even though signing keys are now known from bucket A,
	// authz is per-bucket so bucket B must still forward to upstream (not served from cache).
	resp2, err := globalEnv.DoRawGet(bucketB, "key-b")
	if err != nil {
		t.Fatalf("First GET (bucket B) failed: %v", err)
	}
	io.ReadAll(resp2.Body)
	resp2.Body.Close()

	// Bucket B first request must not be a cache HIT (bucket A's authz doesn't cover bucket B)
	xCacheB1 := resp2.Header.Get("X-Cache")
	if xCacheB1 == "HIT" {
		t.Errorf("First GET (bucket B): expected X-Cache != HIT (bucket A authz should not cover bucket B), got %q", xCacheB1)
	}
	t.Logf("Bucket B first GET forwarded to upstream: X-Cache=%s", xCacheB1)

	// Wait for cache population
	time.Sleep(500 * time.Millisecond)

	// Second GET from bucket A: should be served from cache (signing keys + authz both known)
	resp3, err := globalEnv.DoRawGet(bucketA, "key-a")
	if err != nil {
		t.Fatalf("Second GET (bucket A) failed: %v", err)
	}
	body3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()

	if string(body3) != string(contentA) {
		t.Errorf("Bucket A: expected body %q, got %q", contentA, body3)
	}
	if resp3.Header.Get("X-Cache") != "HIT" {
		t.Errorf("Bucket A second GET: expected X-Cache HIT, got %q", resp3.Header.Get("X-Cache"))
	}

	// Second GET from bucket B: should be served from cache (signing keys + authz both known)
	resp4, err := globalEnv.DoRawGet(bucketB, "key-b")
	if err != nil {
		t.Fatalf("Second GET (bucket B) failed: %v", err)
	}
	body4, _ := io.ReadAll(resp4.Body)
	resp4.Body.Close()

	if string(body4) != string(contentB) {
		t.Errorf("Bucket B: expected body %q, got %q", contentB, body4)
	}
	if resp4.Header.Get("X-Cache") != "HIT" {
		t.Errorf("Bucket B second GET: expected X-Cache HIT, got %q", resp4.Header.Get("X-Cache"))
	}

	t.Logf("Multi-bucket local auth verified: per-bucket authz enforced, both buckets cache HIT after learning")
}

// TestSDK_LocalAuth_CacheNotServedWithoutValidAuth verifies that cached objects
// are NOT served from cache when the client has invalid credentials or no auth.
//
// Even though the object is in cache, TAG must not serve it because local auth
// cannot validate the request — it falls through to Tigris which rejects it.
func TestSDK_LocalAuth_CacheNotServedWithoutValidAuth(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("local-auth-denied")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	content := []byte("secret content that requires valid auth")
	if err := globalEnv.PutTestObject(bucket, "secret-key", content); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	// Step 1: GET with valid creds to populate cache + learn signing keys
	resp1, err := globalEnv.DoRawGet(bucket, "secret-key")
	if err != nil {
		t.Fatalf("First GET (valid creds) failed: %v", err)
	}
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("First GET: expected 200, got %d", resp1.StatusCode)
	}
	if string(body1) != string(content) {
		t.Errorf("First GET: expected body %q, got %q", content, body1)
	}

	// Wait for cache to be populated
	time.Sleep(500 * time.Millisecond)

	// Step 2: Sanity check — valid creds should get cache HIT
	resp2, err := globalEnv.DoRawGet(bucket, "secret-key")
	if err != nil {
		t.Fatalf("Second GET (valid creds) failed: %v", err)
	}
	io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("Second GET (valid creds): expected 200, got %d", resp2.StatusCode)
	}
	if resp2.Header.Get("X-Cache") != "HIT" {
		t.Errorf("Second GET (valid creds): expected X-Cache HIT, got %q", resp2.Header.Get("X-Cache"))
	}

	// Step 3: GET with fake credentials (unknown access key)
	// TAG's local auth: derivedKeyStore.HasKey() returns false → AuthNotValidated → forward to Tigris → 403
	resp3, err := globalEnv.DoRawGetWithCreds(bucket, "secret-key", "FAKE_ACCESS_KEY_12345", "fake-secret-key-for-testing")
	if err != nil {
		t.Fatalf("GET with fake creds failed: %v", err)
	}
	io.ReadAll(resp3.Body)
	resp3.Body.Close()

	if resp3.StatusCode != http.StatusForbidden {
		t.Errorf("GET with fake creds: expected 403 Forbidden, got %d", resp3.StatusCode)
	}
	if resp3.Header.Get("X-Cache") == "HIT" {
		t.Errorf("GET with fake creds: must NOT get X-Cache HIT (cached object served to unauthorized client)")
	}
	t.Logf("Fake creds correctly rejected: status=%d, X-Cache=%s", resp3.StatusCode, resp3.Header.Get("X-Cache"))

	// Step 4: GET with no auth (anonymous request)
	// TAG's local auth: ParseAuthInfo returns ErrMissingAuth → AuthNotValidated → forward to Tigris → 403
	resp4, err := globalEnv.DoRawGetUnauthenticated(bucket, "secret-key")
	if err != nil {
		t.Fatalf("GET without auth failed: %v", err)
	}
	io.ReadAll(resp4.Body)
	resp4.Body.Close()

	if resp4.StatusCode != http.StatusForbidden {
		t.Errorf("GET without auth: expected 403 Forbidden, got %d", resp4.StatusCode)
	}
	if resp4.Header.Get("X-Cache") == "HIT" {
		t.Errorf("GET without auth: must NOT get X-Cache HIT (cached object served to anonymous client)")
	}
	t.Logf("Unauthenticated request correctly rejected: status=%d, X-Cache=%s", resp4.StatusCode, resp4.Header.Get("X-Cache"))
}
