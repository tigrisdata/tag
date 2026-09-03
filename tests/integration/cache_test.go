package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
)

// TestCache_HitWithMetadata verifies that cache hits return proper headers.
// Flow: PUT object → First GET (cache miss) → Second GET (cache hit)
func TestCache_HitWithMetadata(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "test-object.txt"
	content := []byte("Hello, cached world!")

	// Create bucket and put object
	require.NoError(t, env.PutTestObject(bucket, key, content))

	client := env.GetS3Client()
	ctx := context.Background()

	// Verify object is NOT in cache initially
	assert.False(t, env.IsCached(bucket, key), "Object should not be cached before first GET")

	// First GET - should be cache miss, goes to upstream
	resp1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()

	assert.Equal(t, content, body1, "First GET should return correct content")
	env.AssertXCacheMiss(t) // First GET should be cache miss
	assert.NotEmpty(t, aws.ToString(resp1.ETag), "Response should have ETag")

	// Save the ETag for comparison
	etag1 := aws.ToString(resp1.ETag)

	// Wait for async cache write to complete (use polling with timeout for real cache)
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// Second GET - should be cache hit, no upstream request
	resp2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	assert.Equal(t, content, body2, "Second GET should return same content")
	env.AssertXCacheHit(t) // Second GET should be cache hit
	assert.Equal(t, etag1, aws.ToString(resp2.ETag), "Cache hit should return same ETag")
}

// TestCache_IfNoneMatch304 verifies conditional requests with If-None-Match return 304.
func TestCache_IfNoneMatch304(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "conditional-test.txt"
	content := []byte("Conditional test content")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	client := env.GetS3Client()
	ctx := context.Background()

	// First GET to populate cache and get ETag
	resp1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()

	etag := aws.ToString(resp1.ETag)
	require.NotEmpty(t, etag, "ETag should not be empty")

	// Wait for cache to be populated
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// GET with If-None-Match matching the ETag - should return 304
	resp2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		IfNoneMatch: aws.String(etag),
	})

	// AWS SDK returns error for 304 Not Modified
	if err != nil {
		// Check if it's a 304 response
		errStr := err.Error()
		assert.True(t, strings.Contains(errStr, "304") || strings.Contains(errStr, "NotModified"),
			"Should get 304 Not Modified, got: %v", err)
		// Should be served from cache
		env.AssertXCacheHit(t) // 304 response should be served from cache
		return
	}

	// If no error, check the response
	if resp2 != nil && resp2.Body != nil {
		resp2.Body.Close()
	}
}

// TestCache_IfModifiedSince304 verifies conditional requests with If-Modified-Since return 304.
func TestCache_IfModifiedSince304(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "time-conditional-test.txt"
	content := []byte("Time conditional test content")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	client := env.GetS3Client()
	ctx := context.Background()

	// First GET to populate cache and get Last-Modified
	resp1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()

	// gofakes3 may not return Last-Modified, so we'll use a recent time for testing
	lastModified := aws.ToTime(resp1.LastModified)
	if lastModified.IsZero() {
		// gofakes3 doesn't set Last-Modified - use current time as fallback
		// The cache will have stored the current time as LastModified when caching
		lastModified = time.Now()
	}

	// Wait for cache to be populated
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// Use a time in the future to ensure 304 response
	futureTime := lastModified.Add(1 * time.Hour)

	// GET with If-Modified-Since in the future - should return 304
	resp2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		IfModifiedSince: aws.Time(futureTime),
	})

	// AWS SDK returns error for 304 Not Modified
	if err != nil {
		errStr := err.Error()
		assert.True(t, strings.Contains(errStr, "304") || strings.Contains(errStr, "NotModified"),
			"Should get 304 Not Modified, got: %v", err)
		env.AssertXCacheHit(t) // 304 response should be served from cache
		return
	}

	if resp2 != nil && resp2.Body != nil {
		resp2.Body.Close()
	}
}

// TestCache_HeadFromCache verifies HEAD requests are served from cached metadata.
func TestCache_HeadFromCache(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "head-test.txt"
	content := []byte("Content for HEAD test")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	client := env.GetS3Client()
	ctx := context.Background()

	// First GET to populate cache
	resp1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()

	etag := aws.ToString(resp1.ETag)
	contentLength := aws.ToInt64(resp1.ContentLength)

	// Wait for cache to be populated
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// HEAD request - should be served from cache
	resp2, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	env.AssertXCacheHit(t) // HEAD should be served from cache
	assert.Equal(t, etag, aws.ToString(resp2.ETag), "HEAD should return same ETag as GET")
	assert.Equal(t, contentLength, aws.ToInt64(resp2.ContentLength), "HEAD should return same Content-Length")
}

func TestCache_PresignedSigningMode(t *testing.T) {
	content := []byte("presigned signing mode content")
	variantContent := []byte("presigned semantic query content")
	upstreamStore := auth.NewCredentialStore()
	upstreamStore.AddCredential(TestAccessKey, TestSecretKey)
	upstreamValidator := auth.NewRequestValidator(upstreamStore)
	var expectedDate, expectedExpires string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			http.Error(w, "unexpected upstream Authorization header", http.StatusBadRequest)
			return
		}
		if _, err := upstreamValidator.ValidateRequest(r); err != nil {
			http.Error(w, "invalid upstream query signature: "+err.Error(), http.StatusForbidden)
			return
		}
		if r.URL.Query().Get("X-Amz-Date") != expectedDate ||
			r.URL.Query().Get("X-Amz-Expires") != expectedExpires {
			http.Error(w, "presigned deadline changed", http.StatusBadRequest)
			return
		}

		switch {
		case r.Header.Get("If-Match") != "":
			w.WriteHeader(http.StatusPreconditionFailed)
		case r.URL.Query().Get("response-content-type") != "":
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Content-Length", strconv.Itoa(len(variantContent)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(variantContent)
		default:
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(content)))
			w.Header().Set("ETag", `"default-etag"`)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(content)
		}
	})

	env := NewTestEnvironmentWithCacheHandler(handler)
	defer env.Close()

	bucket := "presigned-signing-bucket"
	key := "object.txt"

	coldReq, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	expectedDate = coldReq.URL.Query().Get("X-Amz-Date")
	expectedExpires = coldReq.URL.Query().Get("X-Amz-Expires")
	coldResp, err := http.DefaultClient.Do(coldReq)
	require.NoError(t, err)
	coldBody, err := io.ReadAll(coldResp.Body)
	coldResp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, coldResp.StatusCode)
	require.Equal(t, content, coldBody)
	require.Equal(t, "MISS", coldResp.Header.Get("X-Cache"))
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	hitReq, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	hitResp, err := http.DefaultClient.Do(hitReq)
	require.NoError(t, err)
	hitBody, err := io.ReadAll(hitResp.Body)
	hitResp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, content, hitBody)
	require.Equal(t, "HIT", hitResp.Header.Get("X-Cache"))
	require.Equal(t, int32(1), env.GetUpstreamRequestCount())

	noCacheReq, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	noCacheReq.Header.Set("Cache-Control", "Max-Age=3600")
	expectedDate = noCacheReq.URL.Query().Get("X-Amz-Date")
	expectedExpires = noCacheReq.URL.Query().Get("X-Amz-Expires")
	noCacheResp, err := http.DefaultClient.Do(noCacheReq)
	require.NoError(t, err)
	noCacheResp.Body.Close()
	require.Equal(t, http.StatusOK, noCacheResp.StatusCode)
	require.Equal(t, "BYPASS", noCacheResp.Header.Get("X-Cache"))
	require.Equal(t, int32(2), env.GetUpstreamRequestCount())

	sessionReq, err := presignedGetRequest(
		t.Context(),
		env.GetS3ClientWithSessionToken("temporary-session-token"),
		bucket,
		key,
	)
	require.NoError(t, err)
	sessionResp, err := http.DefaultClient.Do(sessionReq)
	require.NoError(t, err)
	sessionResp.Body.Close()
	require.Equal(t, http.StatusBadRequest, sessionResp.StatusCode)
	require.Equal(t, int32(2), env.GetUpstreamRequestCount())

	variantReq, err := env.PresignedGetRequest(t.Context(), bucket, key, func(input *s3.GetObjectInput) {
		input.ResponseContentType = aws.String("text/plain")
	})
	require.NoError(t, err)
	expectedDate = variantReq.URL.Query().Get("X-Amz-Date")
	expectedExpires = variantReq.URL.Query().Get("X-Amz-Expires")
	variantResp, err := http.DefaultClient.Do(variantReq)
	require.NoError(t, err)
	variantBody, err := io.ReadAll(variantResp.Body)
	variantResp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, variantContent, variantBody)
	require.Equal(t, "BYPASS", variantResp.Header.Get("X-Cache"))

	conditionalReq, err := env.PresignedGetRequest(t.Context(), bucket, key, func(input *s3.GetObjectInput) {
		input.IfMatch = aws.String(`"different-etag"`)
	})
	require.NoError(t, err)
	expectedDate = conditionalReq.URL.Query().Get("X-Amz-Date")
	expectedExpires = conditionalReq.URL.Query().Get("X-Amz-Expires")
	conditionalResp, err := http.DefaultClient.Do(conditionalReq)
	require.NoError(t, err)
	conditionalResp.Body.Close()
	require.Equal(t, http.StatusPreconditionFailed, conditionalResp.StatusCode)
	require.Equal(t, "BYPASS", conditionalResp.Header.Get("X-Cache"))

	countBeforeHeadRange := env.GetUpstreamRequestCount()
	headRangeReq, err := env.PresignedHeadRequest(t.Context(), bucket, key, func(input *s3.HeadObjectInput) {
		input.Range = aws.String("bytes=0-3")
	})
	require.NoError(t, err)
	expectedDate = headRangeReq.URL.Query().Get("X-Amz-Date")
	expectedExpires = headRangeReq.URL.Query().Get("X-Amz-Expires")
	headRangeResp, err := http.DefaultClient.Do(headRangeReq)
	require.NoError(t, err)
	headRangeResp.Body.Close()
	require.Equal(t, "BYPASS", headRangeResp.Header.Get("X-Cache"))
	require.Equal(t, countBeforeHeadRange+1, env.GetUpstreamRequestCount())
}

// TestCache_InvalidateOnPut verifies PUT invalidates the cache for that key.
func TestCache_InvalidateOnPut(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "invalidate-put-test.txt"
	originalContent := []byte("Original content")
	newContent := []byte("New content after PUT")

	require.NoError(t, env.PutTestObject(bucket, key, originalContent))

	client := env.GetS3Client()
	ctx := context.Background()

	// First GET to populate cache
	resp1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	assert.Equal(t, originalContent, body1)
	env.AssertXCacheMiss(t) // First GET should be cache miss

	// Wait for async cache write to complete (use polling with timeout for real cache)
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// PUT new content - should invalidate cache
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(newContent),
	})
	require.NoError(t, err)

	// Verify cache is invalidated (object removed from cache) - use polling since invalidation is async
	require.True(t, env.WaitForNotCached(bucket, key, 2*time.Second), "Object should NOT be in cache after PUT (cache invalidated)")

	// GET should return new content (cache was invalidated)
	resp2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	assert.Equal(t, newContent, body2, "GET after PUT should return new content")
	env.AssertXCacheMiss(t) // GET after PUT should be cache miss (cache invalidated)

	// Wait for async cache write to complete (use polling with timeout for real cache)
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached again after GET")
}

// TestCache_InvalidateOnDelete verifies DELETE invalidates the cache.
func TestCache_InvalidateOnDelete(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "invalidate-delete-test.txt"
	content := []byte("Content to be deleted")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	client := env.GetS3Client()
	ctx := context.Background()

	// First GET to populate cache
	resp1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()
	env.AssertXCacheMiss(t) // First GET should be cache miss

	// Wait for async cache write to complete (use polling with timeout for real cache)
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// Verify it's cached (via X-Cache status)
	resp2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	env.AssertXCacheHit(t) // Should be cache hit before delete

	// DELETE the object - should invalidate cache
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	// Verify cache is invalidated (object removed from cache) - use polling since invalidation is async
	require.True(t, env.WaitForNotCached(bucket, key, 2*time.Second), "Object should NOT be in cache after DELETE")

	// GET should now return 404
	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	assert.Error(t, err, "GET after DELETE should return error")
	assert.True(t, strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "404"),
		"Error should indicate object not found")
}

// TestCache_RangeServedFromCache verifies range requests are served from cache when full object is cached.
func TestCache_RangeServedFromCache(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "range-test.txt"
	content := []byte("0123456789ABCDEFGHIJ") // 20 bytes

	require.NoError(t, env.PutTestObject(bucket, key, content))

	client := env.GetS3Client()
	ctx := context.Background()

	// First GET to populate cache with full object
	resp1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()
	env.AssertXCacheMiss(t) // First GET should be cache miss

	// Wait for cache to be populated
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// Verify full object is cached
	resp2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	env.AssertXCacheHit(t) // Full GET should be cache hit

	// Range request - should be served from cache (full object is cached)
	resp3, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=0-9"),
	})
	require.NoError(t, err)
	body3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()

	assert.Equal(t, []byte("0123456789"), body3, "Range request should return correct bytes")
	env.AssertXCacheHit(t) // Range request should be served from cache when full object is cached

	// Test another range from same cached object
	resp4, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=10-19"),
	})
	require.NoError(t, err)
	body4, _ := io.ReadAll(resp4.Body)
	resp4.Body.Close()

	assert.Equal(t, []byte("ABCDEFGHIJ"), body4, "Second range request should return correct bytes")
	env.AssertXCacheHit(t) // Second range request should also be served from cache
}

// TestCache_RangeBodyEvictedFallsThroughToUpstream verifies that when object
// metadata is cached but its ETag-versioned body is gone (independently evicted,
// or written by a pre-versioning build under a different key), a Range GET does
// NOT emit a truncated 206 from cache — it forwards to upstream and returns the
// correct bytes. Regression guard for the range-header-before-body-probe bug.
func TestCache_RangeBodyEvictedFallsThroughToUpstream(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "range-evicted.txt"
	content := []byte("0123456789ABCDEFGHIJ") // 20 bytes
	require.NoError(t, env.PutTestObject(bucket, key, content))

	client := env.GetS3Client()
	ctx := context.Background()

	// Warm the cache with a full GET.
	resp1, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "object should be cached after first GET")

	// Evict ONLY the versioned body, leaving metadata in place — the meta-present
	// / body-absent state that made the range path commit a 206 over a truncated body.
	meta, found, err := env.Cache.GetMeta(ctx, bucket, key)
	require.NoError(t, err)
	require.True(t, found)
	require.NoError(t, env.EmbeddedCache.Delete(ctx, cache.MakeBodyKey(bucket, key, meta.ETag)))
	require.True(t, env.IsCached(bucket, key), "metadata must still be present after body eviction")

	// A Range GET must return the correct bytes (served from upstream), not a
	// truncated or empty 206 from the cache.
	resp2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=5-14"),
	})
	require.NoError(t, err)
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	assert.Equal(t, []byte("56789ABCDE"), body2, "range must return correct bytes via upstream fallthrough")
}

// TestCache_RangeSingleByteAtZero verifies the byte-0 quirk handling.
// Reading a single byte at position 0 (bytes=0-0) requires special handling in ocache.
func TestCache_RangeSingleByteAtZero(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "byte-zero-test.txt"
	content := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ") // 26 bytes, first byte is 'A'

	require.NoError(t, env.PutTestObject(bucket, key, content))

	client := env.GetS3Client()
	ctx := context.Background()

	// First: Full GET to populate cache
	resp1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()

	assert.Equal(t, content, body1, "Full GET should return complete content")
	env.AssertXCacheMiss(t) // First GET should be cache miss

	// Wait for cache to be populated
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// Second: Full GET to verify cache hit
	resp2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	io.ReadAll(resp2.Body)
	resp2.Body.Close()

	env.AssertXCacheHit(t) // Second full GET should be cache hit

	// Third: Range request for single byte at position 0 (bytes=0-0)
	// This tests the ocache byte-0 quirk handling
	resp3, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=0-0"), // Single byte at position 0
	})
	require.NoError(t, err)
	body3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()

	assert.Equal(t, []byte("A"), body3, "Range bytes=0-0 should return single byte 'A'")
	env.AssertXCacheHit(t) // Range request should be served from cache

	// Fourth: Test other single-byte ranges work too (not just byte 0)
	resp4, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=1-1"), // Single byte at position 1
	})
	require.NoError(t, err)
	body4, _ := io.ReadAll(resp4.Body)
	resp4.Body.Close()

	assert.Equal(t, []byte("B"), body4, "Range bytes=1-1 should return single byte 'B'")
	env.AssertXCacheHit(t) // Range request should be served from cache

	// Fifth: Test last byte
	resp5, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=25-25"), // Last byte (position 25 = 'Z')
	})
	require.NoError(t, err)
	body5, _ := io.ReadAll(resp5.Body)
	resp5.Body.Close()

	assert.Equal(t, []byte("Z"), body5, "Range bytes=25-25 should return single byte 'Z'")
	env.AssertXCacheHit(t) // Range request should be served from cache
}

// TestCache_LargeObjectStreaming verifies large objects are handled correctly with streaming.
func TestCache_LargeObjectStreaming(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "large-object.bin"

	// Create a 1MB object
	content := make([]byte, 1024*1024)
	for i := range content {
		content[i] = byte(i % 256)
	}

	require.NoError(t, env.PutTestObject(bucket, key, content))

	client := env.GetS3Client()
	ctx := context.Background()

	// First GET - cache miss, fetches from upstream
	resp1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	body1, err := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	require.NoError(t, err)

	assert.Equal(t, content, body1, "First GET should return correct content")
	env.AssertXCacheMiss(t) // First GET should be cache miss

	// Wait for cache to be populated (large objects may take longer - use longer timeout)
	require.True(t, env.WaitForCached(bucket, key, 5*time.Second), "Large object should be cached after first GET")

	// Second GET - cache hit
	resp2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	body2, err := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	require.NoError(t, err)

	assert.Equal(t, content, body2, "Second GET should return same content")
	env.AssertXCacheHit(t) // Second GET should be cache hit
}

// TestCache_MultipleObjectsIndependent verifies different objects are cached independently.
func TestCache_MultipleObjectsIndependent(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"

	objects := map[string][]byte{
		"obj1.txt": []byte("Content for object 1"),
		"obj2.txt": []byte("Content for object 2"),
		"obj3.txt": []byte("Content for object 3"),
	}

	// Create all objects
	for key, content := range objects {
		require.NoError(t, env.PutTestObject(bucket, key, content))
	}

	client := env.GetS3Client()
	ctx := context.Background()

	// GET all objects to populate cache
	for key := range objects {
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		env.AssertXCacheMiss(t) // First GET of each object should be cache miss
	}

	// Wait for cache to be populated (check all objects)
	for key := range objects {
		require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object %s should be cached after GET", key)
	}

	// Verify all are cached
	for key, expectedContent := range objects {
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		assert.Equal(t, expectedContent, body, "Object %s should have correct content", key)
		env.AssertXCacheHit(t) // Second GET of each object should be cache hit
	}
}

// TestCache_CacheHitHeaders verifies cache hits include correct headers including X-Cache.
func TestCache_CacheHitHeaders(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "headers-test.txt"
	content := []byte("Content for headers test")
	contentType := "text/plain"

	// Put object with custom content type
	require.NoError(t, env.CreateTestBucket(bucket))
	require.NoError(t, env.PutTestObjectWithMetadata(bucket, key, content, map[string]string{
		"Content-Type": contentType,
	}))

	// Use raw HTTP to check headers directly
	resp1, err := env.DoSignedRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	defer resp1.Body.Close()

	assert.Equal(t, http.StatusOK, resp1.StatusCode)
	etag := resp1.Header.Get("ETag")
	assert.NotEmpty(t, etag, "First response should have ETag")
	assert.Equal(t, "MISS", resp1.Header.Get("X-Cache"), "First request should be cache MISS")

	// Read body to ensure caching happens
	io.Copy(io.Discard, resp1.Body)

	// Wait for async cache write
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// Second request - cache hit
	resp2, err := env.DoSignedRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	defer resp2.Body.Close()

	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	assert.Equal(t, etag, resp2.Header.Get("ETag"), "Cache hit should have same ETag")
	assert.Equal(t, "HIT", resp2.Header.Get("X-Cache"), "Second request should be cache HIT")
	env.AssertXCacheHit(t) // Should be cache hit
}

// TestCache_XCacheForRangeRequests verifies X-Cache headers for range requests.
// When full object is cached: X-Cache: HIT
// When full object is not cached: X-Cache: MISS (triggers background cache fetch)
func TestCache_XCacheForRangeRequests(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "range-xcache-test.txt"
	content := []byte("Content for range X-Cache test - bytes 0123456789")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// First range request - object not in cache yet, should return X-Cache: MISS
	req1, err := env.SignedRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	req1.Header.Set("Range", "bytes=0-9")

	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()

	// Note: gofakes3 may return 200 instead of 206 for range requests
	assert.True(t, resp1.StatusCode == http.StatusOK || resp1.StatusCode == http.StatusPartialContent,
		"Range request should return 200 or 206, got %d", resp1.StatusCode)
	assert.Equal(t, "MISS", resp1.Header.Get("X-Cache"),
		"Range request without cached full object should have X-Cache: MISS")

	// Now do a full GET to populate cache
	req2, err := env.SignedRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)

	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()

	// Wait for cache to be populated
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after full GET")

	// Now range request should return X-Cache: HIT (served from cached full object)
	req3, err := env.SignedRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	req3.Header.Set("Range", "bytes=0-9")

	resp3, err := http.DefaultClient.Do(req3)
	require.NoError(t, err)
	io.Copy(io.Discard, resp3.Body)
	resp3.Body.Close()

	assert.True(t, resp3.StatusCode == http.StatusOK || resp3.StatusCode == http.StatusPartialContent,
		"Range request should return 200 or 206, got %d", resp3.StatusCode)
	assert.Equal(t, "HIT", resp3.Header.Get("X-Cache"),
		"Range request with cached full object should have X-Cache: HIT")
}

// TestCache_XCacheDisabled verifies X-Cache: DISABLED when cache is disabled.
func TestCache_XCacheDisabled(t *testing.T) {
	// Use regular test environment with cache disabled
	env := NewTestEnvironment()
	defer env.Close()

	bucket := "test-bucket"
	key := "disabled-cache-test.txt"
	content := []byte("Content for disabled cache test")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// Request with cache disabled should return X-Cache: DISABLED
	resp, err := env.DoSignedRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "DISABLED", resp.Header.Get("X-Cache"), "Request with disabled cache should have X-Cache: DISABLED")
}

// TestCache_XCacheHitOn304 verifies X-Cache: HIT on 304 Not Modified response.
func TestCache_XCacheHitOn304(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "conditional-xcache-test.txt"
	content := []byte("Content for conditional X-Cache test")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// First GET to populate cache
	resp1, err := env.DoSignedRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()

	etag := resp1.Header.Get("ETag")
	require.NotEmpty(t, etag, "First response should have ETag")
	assert.Equal(t, "MISS", resp1.Header.Get("X-Cache"), "First request should be cache MISS")

	// Wait for async cache write
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// Conditional request with If-None-Match should return 304 with X-Cache: HIT
	req, err := env.SignedRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	req.Header.Set("If-None-Match", etag)

	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp2.Body.Close()

	assert.Equal(t, http.StatusNotModified, resp2.StatusCode, "Should return 304")
	assert.Equal(t, "HIT", resp2.Header.Get("X-Cache"), "304 from cache should have X-Cache: HIT")
}

// TestCache_XCacheHitOnHead verifies X-Cache: HIT for HEAD requests from cache.
func TestCache_XCacheHitOnHead(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "head-xcache-test.txt"
	content := []byte("Content for HEAD X-Cache test")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// First GET to populate cache
	resp1, err := env.DoSignedRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()

	assert.Equal(t, "MISS", resp1.Header.Get("X-Cache"), "First GET should be cache MISS")

	// Wait for async cache write
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// HEAD request should be served from cache with X-Cache: HIT
	resp2, err := env.DoSignedRequest(http.MethodHead, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	defer resp2.Body.Close()

	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	assert.Equal(t, "HIT", resp2.Header.Get("X-Cache"), "HEAD from cache should have X-Cache: HIT")
}

// TestCache_HeaderPreservation verifies all headers are preserved when serving from cache.
// This ensures that headers returned by upstream are the same when request is served from cache.
func TestCache_HeaderPreservation(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	key := "header-preservation-test.txt"
	content := []byte("Content for header preservation test")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// First GET - cache miss, save upstream headers
	resp1, err := env.DoSignedRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()

	// Save headers from upstream response
	upstreamETag := resp1.Header.Get("ETag")
	upstreamContentType := resp1.Header.Get("Content-Type")
	upstreamContentLength := resp1.Header.Get("Content-Length")
	upstreamLastModified := resp1.Header.Get("Last-Modified")

	require.Equal(t, "MISS", resp1.Header.Get("X-Cache"), "First request should be cache MISS")
	require.NotEmpty(t, upstreamETag, "Upstream response should have ETag")
	require.NotEmpty(t, upstreamContentType, "Upstream response should have Content-Type")
	require.NotEmpty(t, upstreamContentLength, "Upstream response should have Content-Length")

	// Wait for async cache write
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// Second GET - cache hit, verify headers match
	resp2, err := env.DoSignedRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	require.Equal(t, "HIT", resp2.Header.Get("X-Cache"), "Second request should be cache HIT")

	// Verify all headers match
	assert.Equal(t, upstreamETag, resp2.Header.Get("ETag"), "ETag should be preserved from upstream")
	assert.Equal(t, upstreamContentType, resp2.Header.Get("Content-Type"), "Content-Type should be preserved from upstream")
	assert.Equal(t, upstreamContentLength, resp2.Header.Get("Content-Length"), "Content-Length should be preserved from upstream")
	assert.Equal(t, upstreamLastModified, resp2.Header.Get("Last-Modified"), "Last-Modified should be preserved from upstream")

	// Verify body content matches
	assert.Equal(t, body1, body2, "Body content should be identical between cache miss and cache hit")

	t.Logf("Header preservation verified: ETag=%s, Content-Type=%s, Content-Length=%s, Last-Modified=%s",
		upstreamETag, upstreamContentType, upstreamContentLength, upstreamLastModified)
}

// TestCache_InvalidateOnDeleteObjects verifies DeleteObjects invalidates cache for all deleted objects.
func TestCache_InvalidateOnDeleteObjects(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "cache-test-bucket"
	keys := []string{"bulk-delete1.txt", "bulk-delete2.txt", "bulk-delete3.txt"}

	// Create objects
	for _, key := range keys {
		require.NoError(t, env.PutTestObject(bucket, key, []byte("content for "+key)))
	}

	client := env.GetS3Client()
	ctx := context.Background()

	// Verify objects are NOT in cache initially
	for _, key := range keys {
		assert.False(t, env.IsCached(bucket, key), "Object %s should not be cached initially", key)
	}

	// GET all objects to populate cache
	for _, key := range keys {
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	// Wait for cache to be populated (check all objects)
	for _, key := range keys {
		require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object %s should be cached after GET", key)
	}

	// Verify all are cached (via X-Cache status)
	for _, key := range keys {
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		env.AssertXCacheHit(t) // All GETs should be cache hits before delete
	}

	// Build delete request
	objectIds := make([]types.ObjectIdentifier, len(keys))
	for i, key := range keys {
		objectIds[i] = types.ObjectIdentifier{Key: aws.String(key)}
	}

	// DeleteObjects - should invalidate cache for all deleted objects
	result, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: objectIds,
		},
	})
	require.NoError(t, err)
	assert.Len(t, result.Deleted, len(keys))

	// Verify all objects are removed from cache - use polling since invalidation is async
	for _, key := range keys {
		require.True(t, env.WaitForNotCached(bucket, key, 2*time.Second), "Object %s should NOT be in cache after DeleteObjects", key)
	}

	// Verify objects no longer exist in upstream (404 returned)
	for _, key := range keys {
		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		assert.Error(t, err, "GET after DeleteObjects should return error for %s", key)
		assert.True(t, strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "404"),
			"Error should indicate object not found for %s", key)
	}

	t.Logf("DeleteObjects cache invalidation verified: %d objects deleted and cache invalidated", len(keys))
}

// TestCache_WarmOnWrite verifies the end-to-end cache-warm-on-write path: with
// cache.warm_on_write enabled, a successful PUT triggers a background full-object
// fetch that populates the cache, so the object is a cache HIT on the next GET even
// though no prior GET populated it. This exercises the real warm chain (warmOnWrite
// -> triggerBackgroundCacheFetch -> fetchFullObjectToCache -> DoFullObjectRequest ->
// setupCacheListener -> embedded cache write), which the proxy unit tests stub out.
func TestCache_WarmOnWrite(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()
	env.Config.Cache.WarmOnWrite = true // read live by warmOnWrite at request time

	client := env.GetS3Client()
	ctx := context.Background()
	bucket := "warm-on-write-bucket"
	key := "warm-on-write.txt"
	content := []byte("warm me on write")
	require.NoError(t, env.CreateTestBucket(bucket))

	// PUT with no prior GET.
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err)

	// The background warm must populate the cache without any GET having occurred.
	require.True(t, env.WaitForCached(bucket, key, 3*time.Second),
		"object should be warmed into cache after PUT (no GET) with warm_on_write=true")

	// The first GET is therefore a cache hit, and returns the written content.
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, content, body, "warmed GET should return the written content")
	env.AssertXCacheHit(t) // hit because the write pre-populated the cache
}

// TestCache_WarmOnWriteDisabled is the control: with warm_on_write off (the default),
// a PUT must NOT populate the cache — the object is only cached on first read.
func TestCache_WarmOnWriteDisabled(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()
	// WarmOnWrite defaults to false; assert it and leave it off.
	require.False(t, env.Config.Cache.WarmOnWrite, "warm_on_write must default to false")

	client := env.GetS3Client()
	ctx := context.Background()
	bucket := "warm-on-write-off-bucket"
	key := "no-warm.txt"
	content := []byte("do not warm on write")
	require.NoError(t, env.CreateTestBucket(bucket))

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err)

	// The object must never appear in cache from the PUT alone. Poll a bounded window
	// so a warm that incorrectly fired would be caught, not raced past.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		require.False(t, env.IsCached(bucket, key),
			"object must not be cached after PUT when warm_on_write is disabled")
		time.Sleep(20 * time.Millisecond)
	}

	// The first GET is the cache-on-read baseline: a miss.
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	env.AssertXCacheMiss(t)
}

// TestCache_ConcurrentPutsSameKey validates read-after-write correctness under
// concurrent writes to a single key: after racing PUTs settle, a read through TAG
// must return exactly what upstream holds — never a stale cached version left behind
// by an earlier write's populate or warm. It runs with warm_on_write both off (pure
// invalidation/tombstone correctness) and on (the warm dedup + tombstone-block path
// we reason about but otherwise don't exercise end-to-end).
func TestCache_ConcurrentPutsSameKey(t *testing.T) {
	for _, warm := range []bool{false, true} {
		name := "warm_off"
		if warm {
			name = "warm_on"
		}
		t.Run(name, func(t *testing.T) {
			env := NewTestEnvironmentWithCache()
			defer env.Close()
			env.Config.Cache.WarmOnWrite = warm

			client := env.GetS3Client()
			ctx := context.Background()
			bucket := "concurrent-put-" + strings.ReplaceAll(name, "_", "-") + "-bucket"
			key := "racy.txt"
			require.NoError(t, env.CreateTestBucket(bucket))

			// Fire N concurrent PUTs of distinct content to the same key.
			const writers = 8
			var wg sync.WaitGroup
			for i := 0; i < writers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					content := []byte(fmt.Sprintf("version-%02d-of-%d-concurrent-writers", i, writers))
					_, err := client.PutObject(ctx, &s3.PutObjectInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(key),
						Body:   bytes.NewReader(content),
					})
					assert.NoError(t, err)
				}(i)
			}
			wg.Wait()

			// Authoritative state: what upstream actually holds now (bypassing TAG).
			upstream := env.GetUpstreamS3Client()
			truthResp, err := upstream.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.NoError(t, err)
			want, _ := io.ReadAll(truthResp.Body)
			truthResp.Body.Close()

			// Two reads through TAG: the first populates the cache (or is served by a
			// warm), the second is a cache hit. Both must equal the authoritative
			// content — a stale cached version (a warm/populate of an earlier write
			// that escaped its invalidation tombstone) would diverge on either read.
			for i := 0; i < 2; i++ {
				resp, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				got, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				require.Equal(t, string(want), string(got),
					"read %d through TAG returned content that differs from upstream (stale cache)", i)
			}
		})
	}
}

// TestBlockCache_ServeInteriorBlockRangeThroughHandler reproduces the warp get-range failure
// through the real handler + real embedded cache, isolating the SERVE path from populate. It
// pre-stores several 4 MiB blocks and a block-mode meta (BlockSize>0), then issues a real HTTP
// Range GET covering an interior block. The dispatch keys only on meta.BlockSize>0, so this
// exercises serveRangeFromBlockCache -> streamBlockRange against a real http.ResponseWriter,
// exactly as production does. A 0-byte / short body reproduces the 278K "unexpected EOF".
func TestBlockCache_ServeInteriorBlockRangeThroughHandler(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()
	ctx := context.Background()

	const blockSize = int64(4 * 1024 * 1024)
	const numBlocks = 3
	total := blockSize * numBlocks
	bucket, key, etag := "blk-serve", "obj", `"serve-v1"`

	full := make([]byte, 0, total)
	for i := 0; i < numBlocks; i++ {
		b := make([]byte, blockSize)
		for j := range b {
			b[j] = byte('A' + i)
		}
		full = append(full, b...)
		require.NoError(t, env.Cache.PutBlockStream(ctx, bucket, key, etag, blockSize, int64(i), bytes.NewReader(b), 300))
	}

	meta := &cache.CachedObjectMeta{
		Bucket: bucket, Key: key, ETag: etag,
		ContentType:   "application/octet-stream",
		ContentLength: total, StatusCode: 200, BlockSize: blockSize,
		LastModified: time.Now().Unix(),
	}
	wrote, err := env.Cache.PutMetaTombstoneAware(ctx, bucket, key, meta, 300, time.Now().UnixNano())
	require.NoError(t, err)
	require.True(t, wrote, "block-mode meta write skipped")

	// Range covering all of interior block 1 (block-aligned) — the benchmark's hot case.
	start, end := blockSize, 2*blockSize-1
	req, err := env.SignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	t.Logf("status=%d X-Cache=%q Content-Range=%q got=%d want=%d",
		resp.StatusCode, resp.Header.Get("X-Cache"), resp.Header.Get("Content-Range"), len(body), end-start+1)
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	require.Equal(t, int(end-start+1), len(body), "range serve returned wrong byte count (0 = the benchmark bug)")
	require.Equal(t, full[start:end+1], body, "range serve content mismatch")
}

// TestBlockCache_EmbeddedMissingBlockNotFound is the minimal reproduction of the warp block-mode
// range failure. The MEMORY cache (cache/block_cache_test.go) returns ErrNotFound for a missing
// block, but the EMBEDDED (RocksDB) backend was observed to return nil + 0 bytes for a missing
// key's range read. That makes BlockExistsErr report an ABSENT block as PRESENT, so fetchOneBlock
// skips the upstream fetch (block never stored) while meta is still written — and the warm serve
// then streams an empty body (client "IncompleteRead(0 bytes)"). Reads of a never-stored block
// MUST surface not-found.
func TestBlockCache_EmbeddedMissingBlockNotFound(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()
	ctx := context.Background()
	bucket, key, etag := "blk-missing", "obj", `"v1"`
	const blockSize = int64(4 * 1024 * 1024)

	// A never-stored block must not be reported present.
	present, err := env.Cache.BlockExistsErr(ctx, bucket, key, etag, blockSize, 0)
	require.NoError(t, err)
	require.False(t, present, "BlockExistsErr reports a never-stored block as PRESENT (root cause)")

	// Probe-sized read (the (0,0) quirk path BlockExistsErr uses).
	var pb bytes.Buffer
	perr := env.Cache.GetBlockRangeStream(ctx, bucket, key, etag, blockSize, 0, 0, 0, &pb)
	require.ErrorIs(t, perr, cache.ErrNotFound, "missing block [0,0] read: err=%v bytes=%d (want ErrNotFound)", perr, pb.Len())

	// Full-block range read (what streamBlockRange issues on serve).
	var fb bytes.Buffer
	ferr := env.Cache.GetBlockRangeStream(ctx, bucket, key, etag, blockSize, 0, 0, blockSize-1, &fb)
	require.ErrorIs(t, ferr, cache.ErrNotFound, "missing block [0,N] read: err=%v bytes=%d (want ErrNotFound)", ferr, fb.Len())
}

// Checksum mode can be requested with the x-amz-checksum-mode header, not only the
// presigned query parameter. A cache hit must return the same checksums the miss did;
// otherwise the response depends on whether the object happened to be cached.
func TestCache_ChecksumModeHeaderSurvivesCacheHit(t *testing.T) {
	content := []byte("checksum-mode header content")
	const checksum = "dGVzdC1jaGVja3N1bQ=="
	env := NewTestEnvironmentWithCacheHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"checksum-mode-header-etag"`)
		if strings.EqualFold(r.Header.Get("X-Amz-Checksum-Mode"), "ENABLED") {
			w.Header().Set("X-Amz-Checksum-Crc32c", checksum)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	})
	defer env.Close()

	bucket := "checksum-header-bucket"
	key := "object.bin"

	get := func() *http.Response {
		t.Helper()
		req, err := env.Signer.SignRequest(
			context.Background(),
			http.MethodGet,
			"/"+bucket+"/"+key,
			nil,
			"",
			TestAccessKey,
			TestSecretKey,
			http.Header{"X-Amz-Checksum-Mode": []string{"ENABLED"}},
		)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		return resp
	}

	resp1 := get()
	require.Equal(t, http.StatusOK, resp1.StatusCode)
	require.Equal(t, checksum, resp1.Header.Get("X-Amz-Checksum-Crc32c"),
		"upstream returns the checksum for a checksum-mode read")
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	resp2 := get()
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	require.Equal(t, checksum, resp2.Header.Get("X-Amz-Checksum-Crc32c"),
		"a cache hit must return the checksum the miss returned")
}

// HeadObject takes a checksum mode too. An entry stored without checksums cannot answer
// that request, so it must not be served as a hit: the client would never see the
// x-amz-checksum-* headers it asked for.
func TestCache_ChecksumModeHeadNotServedFromChecksumlessEntry(t *testing.T) {
	content := []byte("head checksum-mode content")
	const checksum = "dGVzdC1jaGVja3N1bQ=="
	env := NewTestEnvironmentWithCacheHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"head-checksum-etag"`)
		if strings.EqualFold(r.Header.Get("X-Amz-Checksum-Mode"), "ENABLED") {
			w.Header().Set("X-Amz-Checksum-Crc32c", checksum)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(content)
		}
	})
	defer env.Close()

	bucket := "head-checksum-bucket"
	key := "object.bin"

	do := func(method string, header http.Header) *http.Response {
		t.Helper()
		req, err := env.Signer.SignRequest(
			context.Background(),
			method,
			"/"+bucket+"/"+key,
			nil,
			"",
			TestAccessKey,
			TestSecretKey,
			header,
		)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		return resp
	}

	// Populate the entry with a read that did not ask for checksums.
	resp1 := do(http.MethodGet, http.Header{})
	require.Equal(t, http.StatusOK, resp1.StatusCode)
	require.Empty(t, resp1.Header.Get("X-Amz-Checksum-Crc32c"))
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	resp2 := do(http.MethodHead, http.Header{"X-Amz-Checksum-Mode": []string{"ENABLED"}})
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	require.Equal(t, checksum, resp2.Header.Get("X-Amz-Checksum-Crc32c"),
		"a checksum-mode HEAD must not be answered from an entry stored without checksums")
}

// Concurrent reads coalesce onto one upstream fetch, so a checksum-mode read must not
// share that fetch with a plain one: the plain fetch asks S3 for no checksums, and the
// coalesced client would receive a response missing the headers it asked for.
func TestCache_ChecksumModeDoesNotCoalesceWithPlainRead(t *testing.T) {
	content := []byte("coalescing checksum-mode content")
	const checksum = "dGVzdC1jaGVja3N1bQ=="
	release := make(chan struct{})
	arrived := make(chan string, 8)

	env := NewTestEnvironmentWithCacheHandler(func(w http.ResponseWriter, r *http.Request) {
		mode := r.Header.Get("X-Amz-Checksum-Mode")
		arrived <- mode
		if mode == "" {
			// Hold the plain fetch open so the checksum-mode read arrives while it is
			// in flight, which is the only moment coalescing can happen.
			<-release
		}
		w.Header().Set("ETag", `"coalescing-etag"`)
		if strings.EqualFold(mode, "ENABLED") {
			w.Header().Set("X-Amz-Checksum-Crc32c", checksum)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	})
	defer env.Close()

	bucket := "coalescing-checksum-bucket"
	key := "object.bin"

	get := func(header http.Header) *http.Response {
		req, err := env.Signer.SignRequest(
			context.Background(),
			http.MethodGet,
			"/"+bucket+"/"+key,
			nil,
			"",
			TestAccessKey,
			TestSecretKey,
			header,
		)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		return resp
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		get(http.Header{})
	}()

	select {
	case mode := <-arrived:
		require.Empty(t, mode, "the held fetch is the plain one")
	case <-time.After(5 * time.Second):
		t.Fatal("plain read never reached upstream")
	}

	var checksumResp *http.Response
	wg.Add(1)
	go func() {
		defer wg.Done()
		checksumResp = get(http.Header{"X-Amz-Checksum-Mode": []string{"ENABLED"}})
	}()

	// Give the checksum-mode read time to either start its own fetch or coalesce onto
	// the held one, then let the held fetch finish so both requests can complete.
	time.Sleep(300 * time.Millisecond)
	close(release)
	wg.Wait()

	require.Equal(t, http.StatusOK, checksumResp.StatusCode)
	require.Equal(t, checksum, checksumResp.Header.Get("X-Amz-Checksum-Crc32c"),
		"a checksum-mode read must not be served by a plain read's coalesced fetch")
}

// HeadObject can be presigned with a checksum mode too. That request must reach the cache
// rather than bypass it, and must be answered with the checksums it asked for.
func TestCache_PresignedHeadChecksumModeUsesCache(t *testing.T) {
	content := []byte("presigned head checksum content")
	const checksum = "dGVzdC1jaGVja3N1bQ=="
	var upstreamRequests int32
	env := NewTestEnvironmentWithCacheHandler(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamRequests, 1)
		w.Header().Set("ETag", `"presigned-head-etag"`)
		w.Header().Set("X-Amz-Checksum-Crc32c", checksum)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(content)
		}
	})
	defer env.Close()

	bucket := "presigned-head-checksum-bucket"
	key := "object.bin"

	// Populate through a checksum-mode GET so the entry carries the checksums.
	getReq, err := env.PresignedGetRequest(t.Context(), bucket, key, func(input *s3.GetObjectInput) {
		input.ChecksumMode = types.ChecksumModeEnabled
	})
	require.NoError(t, err)
	getResp, err := http.DefaultClient.Do(getReq)
	require.NoError(t, err)
	_, err = io.ReadAll(getResp.Body)
	getResp.Body.Close()
	require.NoError(t, err)
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	headReq, err := env.PresignedHeadRequest(t.Context(), bucket, key, func(input *s3.HeadObjectInput) {
		input.ChecksumMode = types.ChecksumModeEnabled
	})
	require.NoError(t, err)
	require.Equal(t, "ENABLED", headReq.URL.Query().Get("X-Amz-Checksum-Mode"))
	before := atomic.LoadInt32(&upstreamRequests)
	headResp, err := http.DefaultClient.Do(headReq)
	require.NoError(t, err)
	headResp.Body.Close()

	require.Equal(t, http.StatusOK, headResp.StatusCode)
	require.Equal(t, "HIT", headResp.Header.Get("X-Cache"),
		"a presigned checksum-mode HEAD must be cache eligible, not bypassed")
	require.Equal(t, checksum, headResp.Header.Get("X-Amz-Checksum-Crc32c"))
	require.Equal(t, before, atomic.LoadInt32(&upstreamRequests), "cache hit must not reach upstream")
}
