package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tag/auth"
)

// TestTransparentAuth_SigningKeyLearning_ThenCacheHit verifies the core transparent
// auth flow: first request learns signing keys from Tigris, second request validates
// locally and is served from cache.
func TestTransparentAuth_SigningKeyLearning_ThenCacheHit(t *testing.T) {
	backend := s3mem.New()
	env := NewTestEnvironmentWithTransparentAuth(t, newSigningKeysUpstreamHandler(t, backend))
	env.S3Backend = backend
	defer env.Close()

	bucket := "tp-learn-bucket"
	key := "test-object.txt"
	content := []byte("transparent proxy test content")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// Request 1: unknown access key → forwarded to Tigris → signing keys learned
	resp1, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	defer resp1.Body.Close()

	body1, err := io.ReadAll(resp1.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp1.StatusCode)
	assert.Equal(t, content, body1)
	assert.Equal(t, int32(1), env.GetUpstreamRequestCount(), "First request should go to upstream")

	// Internal header must NOT leak to client
	assert.Empty(t, resp1.Header.Get("X-Tigris-Proxy-Signing-Keys"), "Internal signing keys header must not leak to client")

	// Signing keys should have been learned
	assert.True(t, env.DerivedKeyStore.HasKey(TestAccessKey), "DerivedKeyStore should have learned keys for TestAccessKey")
	assert.True(t, env.AuthzCache.IsAuthorized(TestAccessKey, bucket), "AuthzCache should have granted access for TestAccessKey+bucket")

	// Wait for cache population
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// Request 2: local auth succeeds + cache hit
	resp2, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	defer resp2.Body.Close()

	body2, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	assert.Equal(t, content, body2)
	env.AssertXCacheHit(t)
	assert.Equal(t, int32(1), env.GetUpstreamRequestCount(), "Second request should be served from cache, upstream count should not increase")
}

// TestTransparentAuth_UnknownAccessKey_ForwardsToTigris verifies that requests
// signed with an unknown access key bypass the cache and are forwarded to Tigris.
func TestTransparentAuth_UnknownAccessKey_ForwardsToTigris(t *testing.T) {
	backend := s3mem.New()
	env := NewTestEnvironmentWithTransparentAuth(t, newSigningKeysUpstreamHandler(t, backend))
	env.S3Backend = backend
	defer env.Close()

	bucket := "tp-unknown-key-bucket"
	key := "test-object.txt"
	content := []byte("content for unknown key test")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// Initial request with TestAccessKey to populate cache + learn keys
	resp1, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()
	require.Equal(t, http.StatusOK, resp1.StatusCode)

	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached")

	// Sanity check: TestAccessKey request now gets cache hit
	resp2, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	env.AssertXCacheHit(t)

	countBefore := env.GetUpstreamRequestCount()

	// Request with unknown access key — should bypass cache, forward to Tigris
	unknownSigner := auth.NewRequestSigner(env.TAGServer.URL, TestRegion)
	unknownAccessKey := "AKID_UNKNOWN_EXAMPLE1234"
	unknownSecretKey := "unknown-secret-key-for-testing"

	path := "/" + bucket + "/" + key
	req, err := unknownSigner.SignRequest(
		context.Background(), "GET", path, nil, "",
		unknownAccessKey, unknownSecretKey, http.Header{},
	)
	require.NoError(t, err)

	resp3, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, resp3.Body)
	resp3.Body.Close()

	// Should have gone to upstream (unknown key → AuthNotValidated → forwarded)
	assert.Greater(t, env.GetUpstreamRequestCount(), countBefore, "Unknown access key request should be forwarded to upstream")
}

// TestTransparentAuth_UnauthenticatedRequest_ForwardsToTigris verifies that
// requests without an Authorization header are forwarded to Tigris (not rejected).
func TestTransparentAuth_UnauthenticatedRequest_ForwardsToTigris(t *testing.T) {
	publicContent := []byte("public bucket content")

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write(publicContent)
	})

	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	defer env.Close()

	// Send unsigned request (no Authorization header)
	req, err := http.NewRequest("GET", env.TAGServer.URL+"/public-bucket/test.txt", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Unauthenticated request should be forwarded, not rejected")
	assert.Equal(t, publicContent, body)
	assert.Equal(t, int32(1), env.GetUpstreamRequestCount(), "Request should have been forwarded to upstream")
}

// TestTransparentAuth_AuthzRevocationOn403 verifies that when Tigris returns 403,
// the authz cache entry is revoked and subsequent requests are forwarded.
func TestTransparentAuth_AuthzRevocationOn403(t *testing.T) {
	backend := s3mem.New()

	// Stateful handler: returns 403 when forbidden flag is set
	var returnForbidden int32
	baseHandler := newSigningKeysUpstreamHandler(t, backend)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&returnForbidden) == 1 {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`))
			return
		}
		baseHandler.ServeHTTP(w, r)
	})

	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	env.S3Backend = backend
	defer env.Close()

	bucket := "tp-revoke-bucket"
	key := "test-object.txt"
	content := []byte("content for revocation test")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// Request 1: succeeds, keys learned, authz granted
	resp1, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()
	require.Equal(t, http.StatusOK, resp1.StatusCode)
	assert.True(t, env.AuthzCache.IsAuthorized(TestAccessKey, bucket))

	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	// Request 2: cache hit (sanity check)
	resp2, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	env.AssertXCacheHit(t)

	// Revoke authz and enable 403 from upstream
	env.AuthzCache.Revoke(TestAccessKey, bucket)
	atomic.StoreInt32(&returnForbidden, 1)

	// Request 3: authz expired → forwarded → gets 403 → revocation
	resp3, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp3.Body)
	resp3.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp3.StatusCode, "Should return 403 from Tigris")
	assert.False(t, env.AuthzCache.IsAuthorized(TestAccessKey, bucket), "AuthzCache should have revoked access on 403")

	// Request 4: still not authorized → forwarded → 403
	resp4, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp4.Body)
	resp4.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp4.StatusCode)
}

// TestTransparentAuth_InternalHeaderAlwaysStripped verifies that the
// X-Tigris-Proxy-Signing-Keys header never reaches the client on any request type.
func TestTransparentAuth_InternalHeaderAlwaysStripped(t *testing.T) {
	backend := s3mem.New()

	// Upstream always sets the internal header, even on PUTs
	faker := newSigningKeysUpstreamHandler(t, backend)
	alwaysHeaderHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		faker.ServeHTTP(rec, r)

		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		// Force header on all responses regardless of status
		if rec.Header().Get("X-Tigris-Proxy-Signing-Keys") == "" {
			w.Header().Set("X-Tigris-Proxy-Signing-Keys", "should-be-stripped")
		}
		w.WriteHeader(rec.Code)
		w.Write(rec.Body.Bytes())
	})

	env := NewTestEnvironmentWithTransparentAuth(t, alwaysHeaderHandler)
	env.S3Backend = backend
	defer env.Close()

	bucket := "tp-strip-bucket"
	key := "test-object.txt"
	content := []byte("content for header stripping test")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// GET (cache miss) — header stripped
	resp1, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()
	assert.Equal(t, http.StatusOK, resp1.StatusCode)
	assert.Empty(t, resp1.Header.Get("X-Tigris-Proxy-Signing-Keys"), "Internal header must be stripped on cache miss GET")

	// Wait for cache
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	// GET (cache hit) — header should not appear at all (response from cache)
	resp2, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	env.AssertXCacheHit(t)
	assert.Empty(t, resp2.Header.Get("X-Tigris-Proxy-Signing-Keys"), "Internal header must not appear on cache hit")

	// PUT request — header stripped
	putBody := []byte("new content")
	h := sha256.Sum256(putBody)
	bodyHash := hex.EncodeToString(h[:])
	putReq, err := env.Signer.SignRequest(
		context.Background(), "PUT", "/"+bucket+"/"+key,
		bytes.NewReader(putBody), bodyHash,
		TestAccessKey, TestSecretKey, http.Header{},
	)
	require.NoError(t, err)

	resp3, err := http.DefaultClient.Do(putReq)
	require.NoError(t, err)
	io.Copy(io.Discard, resp3.Body)
	resp3.Body.Close()
	assert.Empty(t, resp3.Header.Get("X-Tigris-Proxy-Signing-Keys"), "Internal header must be stripped on PUT")
}

// TestTransparentAuth_InternalHeaderStrippedOnErrors verifies that the
// X-Tigris-Proxy-Signing-Keys header is stripped on error responses (403, 404, 500).
func TestTransparentAuth_InternalHeaderStrippedOnErrors(t *testing.T) {
	var statusCode int32
	atomic.StoreInt32(&statusCode, http.StatusForbidden)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always set the internal header
		w.Header().Set("X-Tigris-Proxy-Signing-Keys", "leaked-header-value")
		code := int(atomic.LoadInt32(&statusCode))
		w.WriteHeader(code)
		w.Write([]byte("error response"))
	})

	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	defer env.Close()

	tests := []struct {
		name string
		code int
	}{
		{"403 Forbidden", http.StatusForbidden},
		{"404 Not Found", http.StatusNotFound},
		{"500 Internal Server Error", http.StatusInternalServerError},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			atomic.StoreInt32(&statusCode, int32(tc.code))

			resp, err := env.DoSignedRequest("GET", "/error-bucket/test.txt", nil)
			require.NoError(t, err)
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			assert.Equal(t, tc.code, resp.StatusCode)
			assert.Empty(t, resp.Header.Get("X-Tigris-Proxy-Signing-Keys"),
				"Internal header must be stripped on %d response", tc.code)
		})
	}
}
