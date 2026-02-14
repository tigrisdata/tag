package sdk

import (
	"io"
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

	// First GET from bucket B: learns signing keys + grants authz for bucket B
	resp2, err := globalEnv.DoRawGet(bucketB, "key-b")
	if err != nil {
		t.Fatalf("First GET (bucket B) failed: %v", err)
	}
	io.ReadAll(resp2.Body)
	resp2.Body.Close()

	// Wait for cache population
	time.Sleep(500 * time.Millisecond)

	// Second GET from bucket A: should be served from cache
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
		t.Errorf("Bucket A: expected X-Cache HIT, got %q", resp3.Header.Get("X-Cache"))
	}

	// Second GET from bucket B: should be served from cache
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
		t.Errorf("Bucket B: expected X-Cache HIT, got %q", resp4.Header.Get("X-Cache"))
	}

	t.Logf("Multi-bucket local auth verified: both buckets served from cache")
}
