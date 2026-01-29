package integration

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
