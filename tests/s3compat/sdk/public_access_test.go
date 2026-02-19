package sdk

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestSDK_PublicBucket_AnonymousReadsFromCache verifies that anonymous GET and HEAD
// requests on objects in a public-read bucket are served from cache.
//
// Object A tests the authenticated-GET → anonymous-GET → re-cache path.
// Object B tests that anonymous GET also populates cache for public objects.
//
// Flow:
//  1. Create public-read bucket (all objects inherit public-read)
//  2. PUT two objects (no per-object ACL needed)
//  3. Authenticated GET obj-a only → populates cache + learns signing keys
//  4. Anonymous GET obj-a → MISS (auth GET cached without ACL), re-caches with inferred public-read
//  5. Anonymous GET obj-a again → cache HIT (cached by anonymous GET with public-read ACL)
//  6. Anonymous GET obj-b → forwarded to Tigris (not yet cached), populates cache
//  7. Anonymous GET obj-b again → cache HIT (cached by anonymous GET in step 6)
//  8. Anonymous HEAD obj-b → cache HIT
func TestSDK_PublicBucket_AnonymousReadsFromCache(t *testing.T) {
	bucket, err := globalEnv.CreatePublicTestBucket("public-anon-cache")
	if err != nil {
		t.Fatalf("Failed to create public test bucket: %v", err)
	}

	contentA := []byte("public object A content")
	contentB := []byte("public object B content")

	if err := globalEnv.PutTestObject(bucket, "obj-a", contentA); err != nil {
		t.Fatalf("Failed to put object A: %v", err)
	}
	if err := globalEnv.PutTestObject(bucket, "obj-b", contentB); err != nil {
		t.Fatalf("Failed to put object B: %v", err)
	}

	// Authenticated GET obj-a only — populates cache and learns signing keys
	resp, err := globalEnv.DoRawGet(bucket, "obj-a")
	if err != nil {
		t.Fatalf("Authenticated GET obj-a failed: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Authenticated GET obj-a: expected 200, got %d", resp.StatusCode)
	}

	// Wait for cache population
	time.Sleep(500 * time.Millisecond)

	// Anonymous GET obj-a — MISS because authenticated GET cached without ACL
	// (Tigris omits X-Amz-Acl for inherited ACLs). This anonymous GET fetches from
	// Tigris, infers public-read, and re-caches with the ACL.
	respA1, err := globalEnv.DoRawGetUnauthenticated(bucket, "obj-a")
	if err != nil {
		t.Fatalf("First anonymous GET obj-a failed: %v", err)
	}
	bodyA1, _ := io.ReadAll(respA1.Body)
	respA1.Body.Close()

	if respA1.StatusCode != http.StatusOK {
		t.Errorf("First anonymous GET obj-a: expected 200, got %d", respA1.StatusCode)
	}
	if string(bodyA1) != string(contentA) {
		t.Errorf("First anonymous GET obj-a: expected body %q, got %q", contentA, bodyA1)
	}
	t.Logf("First anonymous GET obj-a: status=%d, X-Cache=%s", respA1.StatusCode, respA1.Header.Get("X-Cache"))

	// Wait for cache population from anonymous GET (with inferred public-read ACL)
	time.Sleep(500 * time.Millisecond)

	// Anonymous GET obj-a again — cache HIT (cached by anonymous GET with public-read ACL)
	respA2, err := globalEnv.DoRawGetUnauthenticated(bucket, "obj-a")
	if err != nil {
		t.Fatalf("Second anonymous GET obj-a failed: %v", err)
	}
	bodyA2, _ := io.ReadAll(respA2.Body)
	respA2.Body.Close()

	if respA2.StatusCode != http.StatusOK {
		t.Errorf("Second anonymous GET obj-a: expected 200, got %d", respA2.StatusCode)
	}
	if string(bodyA2) != string(contentA) {
		t.Errorf("Second anonymous GET obj-a: expected body %q, got %q", contentA, bodyA2)
	}
	if respA2.Header.Get("X-Cache") != "HIT" {
		t.Errorf("Second anonymous GET obj-a: expected X-Cache HIT, got %q", respA2.Header.Get("X-Cache"))
	}
	t.Logf("Second anonymous GET obj-a: status=%d, X-Cache=%s", respA2.StatusCode, respA2.Header.Get("X-Cache"))

	// Anonymous GET obj-b — not yet cached, forwarded to Tigris, should succeed and populate cache
	respB1, err := globalEnv.DoRawGetUnauthenticated(bucket, "obj-b")
	if err != nil {
		t.Fatalf("First anonymous GET obj-b failed: %v", err)
	}
	bodyB1, _ := io.ReadAll(respB1.Body)
	respB1.Body.Close()

	if respB1.StatusCode != http.StatusOK {
		t.Errorf("First anonymous GET obj-b: expected 200, got %d", respB1.StatusCode)
	}
	if string(bodyB1) != string(contentB) {
		t.Errorf("First anonymous GET obj-b: expected body %q, got %q", contentB, bodyB1)
	}
	t.Logf("First anonymous GET obj-b: status=%d, X-Cache=%s", respB1.StatusCode, respB1.Header.Get("X-Cache"))

	// Wait for cache population from anonymous GET
	time.Sleep(500 * time.Millisecond)

	// Anonymous GET obj-b again — should be cache HIT (populated by anonymous GET above)
	respB2, err := globalEnv.DoRawGetUnauthenticated(bucket, "obj-b")
	if err != nil {
		t.Fatalf("Second anonymous GET obj-b failed: %v", err)
	}
	bodyB2, _ := io.ReadAll(respB2.Body)
	respB2.Body.Close()

	if respB2.StatusCode != http.StatusOK {
		t.Errorf("Second anonymous GET obj-b: expected 200, got %d", respB2.StatusCode)
	}
	if string(bodyB2) != string(contentB) {
		t.Errorf("Second anonymous GET obj-b: expected body %q, got %q", contentB, bodyB2)
	}
	if respB2.Header.Get("X-Cache") != "HIT" {
		t.Errorf("Second anonymous GET obj-b: expected X-Cache HIT, got %q (anonymous GET did not populate cache)", respB2.Header.Get("X-Cache"))
	}
	t.Logf("Second anonymous GET obj-b: status=%d, X-Cache=%s", respB2.StatusCode, respB2.Header.Get("X-Cache"))

	// Anonymous HEAD obj-b — should also be cache HIT
	headResp, err := globalEnv.DoRawHeadUnauthenticated(bucket, "obj-b")
	if err != nil {
		t.Fatalf("Anonymous HEAD obj-b failed: %v", err)
	}
	io.ReadAll(headResp.Body)
	headResp.Body.Close()

	if headResp.StatusCode != http.StatusOK {
		t.Errorf("Anonymous HEAD obj-b: expected 200, got %d", headResp.StatusCode)
	}
	if headResp.Header.Get("X-Cache") != "HIT" {
		t.Errorf("Anonymous HEAD obj-b: expected X-Cache HIT, got %q", headResp.Header.Get("X-Cache"))
	}
	t.Logf("Anonymous HEAD obj-b: status=%d, X-Cache=%s, Content-Length=%s",
		headResp.StatusCode, headResp.Header.Get("X-Cache"), headResp.Header.Get("Content-Length"))
}

// TestSDK_PublicBucket_PrivateObjectDeniedAnonymously verifies that an explicitly
// private object in a public-read bucket is NOT served from cache to anonymous requests.
//
// Flow:
//  1. Create public-read bucket
//  2. PUT one normal object (inherits public-read) and one with ACL: private
//  3. Authenticated GET both → populates cache
//  4. Anonymous GET on public object → should succeed
//  5. Anonymous GET on private object → should return 403 (forwarded to Tigris)
//  6. Anonymous HEAD on private object → should return 403
func TestSDK_PublicBucket_PrivateObjectDeniedAnonymously(t *testing.T) {
	t.Skip("Tigris does not enforce per-object ACL in public buckets")

	bucket, err := globalEnv.CreatePublicTestBucket("public-private-obj")
	if err != nil {
		t.Fatalf("Failed to create public test bucket: %v", err)
	}

	publicContent := []byte("this is a public object")
	privateContent := []byte("this is a private object")

	// Normal object inherits public-read from bucket
	if err := globalEnv.PutTestObject(bucket, "public-obj", publicContent); err != nil {
		t.Fatalf("Failed to put public object: %v", err)
	}
	// Explicitly private object overrides bucket default
	if err := globalEnv.PutTestObjectWithACL(bucket, "private-obj", privateContent, types.ObjectCannedACLPrivate); err != nil {
		t.Fatalf("Failed to put private object: %v", err)
	}

	// Authenticated GET both to populate cache
	for _, key := range []string{"public-obj", "private-obj"} {
		resp, err := globalEnv.DoRawGet(bucket, key)
		if err != nil {
			t.Fatalf("Authenticated GET %s failed: %v", key, err)
		}
		io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Authenticated GET %s: expected 200, got %d", key, resp.StatusCode)
		}
	}

	// Wait for cache population
	time.Sleep(500 * time.Millisecond)

	// Anonymous GET on public object — should succeed
	pubResp, err := globalEnv.DoRawGetUnauthenticated(bucket, "public-obj")
	if err != nil {
		t.Fatalf("Anonymous GET public-obj failed: %v", err)
	}
	pubBody, _ := io.ReadAll(pubResp.Body)
	pubResp.Body.Close()

	if pubResp.StatusCode != http.StatusOK {
		t.Errorf("Anonymous GET public-obj: expected 200, got %d", pubResp.StatusCode)
	}
	if string(pubBody) != string(publicContent) {
		t.Errorf("Anonymous GET public-obj: expected body %q, got %q", publicContent, pubBody)
	}
	t.Logf("Anonymous GET public-obj: status=%d, X-Cache=%s", pubResp.StatusCode, pubResp.Header.Get("X-Cache"))

	// Anonymous GET on private object — should be denied (403)
	privResp, err := globalEnv.DoRawGetUnauthenticated(bucket, "private-obj")
	if err != nil {
		t.Fatalf("Anonymous GET private-obj failed: %v", err)
	}
	io.ReadAll(privResp.Body)
	privResp.Body.Close()

	if privResp.StatusCode != http.StatusForbidden {
		t.Errorf("Anonymous GET private-obj: expected 403, got %d", privResp.StatusCode)
	}
	if privResp.Header.Get("X-Cache") == "HIT" {
		t.Errorf("Anonymous GET private-obj: must NOT get X-Cache HIT (private object leaked from cache)")
	}
	t.Logf("Anonymous GET private-obj: status=%d, X-Cache=%s", privResp.StatusCode, privResp.Header.Get("X-Cache"))

	// Anonymous HEAD on private object — should also be denied
	privHeadResp, err := globalEnv.DoRawHeadUnauthenticated(bucket, "private-obj")
	if err != nil {
		t.Fatalf("Anonymous HEAD private-obj failed: %v", err)
	}
	io.ReadAll(privHeadResp.Body)
	privHeadResp.Body.Close()

	if privHeadResp.StatusCode != http.StatusForbidden {
		t.Errorf("Anonymous HEAD private-obj: expected 403, got %d", privHeadResp.StatusCode)
	}
	if privHeadResp.Header.Get("X-Cache") == "HIT" {
		t.Errorf("Anonymous HEAD private-obj: must NOT get X-Cache HIT")
	}
	t.Logf("Anonymous HEAD private-obj: status=%d, X-Cache=%s", privHeadResp.StatusCode, privHeadResp.Header.Get("X-Cache"))
}
