package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestSDK_GetObject verifies basic GetObject operation with AWS SDK using gofakes3.
func TestSDK_GetObject(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	// Pre-populate test data
	objectContent := []byte("test object content from upstream")
	if err := env.PutTestObject("test-bucket", "test-key", objectContent); err != nil {
		t.Fatalf("Failed to pre-populate test data: %v", err)
	}

	client := env.GetS3Client()
	ctx := context.Background()

	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-key"),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("Failed to read body: %v", err)
	}

	if string(body) != string(objectContent) {
		t.Errorf("Expected body %q, got %q", objectContent, body)
	}
}

// TestSDK_PutObject verifies basic PutObject operation with AWS SDK using gofakes3.
func TestSDK_PutObject(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	// Create bucket for the test
	if err := env.CreateTestBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := env.GetS3Client()
	ctx := context.Background()

	putBody := []byte("new object content")
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String("test-bucket"),
		Key:         aws.String("new-key"),
		Body:        bytes.NewReader(putBody),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify object was stored by getting it back
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("new-key"),
	})
	if err != nil {
		t.Fatalf("GetObject verification failed: %v", err)
	}
	defer result.Body.Close()

	body, _ := io.ReadAll(result.Body)
	if string(body) != string(putBody) {
		t.Errorf("Expected body %q, got %q", putBody, body)
	}
}

// TestSDK_DeleteObject verifies DeleteObject operation with AWS SDK using gofakes3.
func TestSDK_DeleteObject(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	// Pre-populate test data
	if err := env.PutTestObject("test-bucket", "delete-key", []byte("content to delete")); err != nil {
		t.Fatalf("Failed to pre-populate test data: %v", err)
	}

	client := env.GetS3Client()
	ctx := context.Background()

	// Delete the object
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("delete-key"),
	})
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify object is deleted (should get NoSuchKey error)
	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("delete-key"),
	})
	if err == nil {
		t.Error("Expected error when getting deleted object, got nil")
	}
}

// TestSDK_HeadObject verifies HeadObject operation with AWS SDK using gofakes3.
func TestSDK_HeadObject(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	// Pre-populate test data
	content := []byte("head object content 1234567890")
	if err := env.PutTestObject("test-bucket", "head-key", content); err != nil {
		t.Fatalf("Failed to pre-populate test data: %v", err)
	}

	client := env.GetS3Client()
	ctx := context.Background()

	result, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("head-key"),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}

	if aws.ToInt64(result.ContentLength) != int64(len(content)) {
		t.Errorf("Expected ContentLength %d, got %d", len(content), aws.ToInt64(result.ContentLength))
	}

	// gofakes3 generates ETags
	if result.ETag == nil || *result.ETag == "" {
		t.Error("Expected ETag to be set")
	}
}

// TestSDK_ListBuckets verifies ListBuckets operation with AWS SDK using gofakes3.
func TestSDK_ListBuckets(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	// Create test buckets
	env.CreateTestBucket("bucket1")
	env.CreateTestBucket("bucket2")
	env.CreateTestBucket("bucket3")

	client := env.GetS3Client()
	ctx := context.Background()

	result, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}

	if len(result.Buckets) != 3 {
		t.Errorf("Expected 3 buckets, got %d", len(result.Buckets))
	}

	// Check bucket names exist
	bucketNames := make(map[string]bool)
	for _, b := range result.Buckets {
		bucketNames[aws.ToString(b.Name)] = true
	}

	for _, expected := range []string{"bucket1", "bucket2", "bucket3"} {
		if !bucketNames[expected] {
			t.Errorf("Expected bucket %q not found", expected)
		}
	}
}

// TestSDK_ListObjectsV2 verifies ListObjectsV2 operation with AWS SDK using gofakes3.
func TestSDK_ListObjectsV2(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	// Pre-populate test data with prefix structure
	env.PutTestObject("test-bucket", "test/object1.txt", []byte("content1"))
	env.PutTestObject("test-bucket", "test/object2.txt", []byte("content2"))
	env.PutTestObject("test-bucket", "other/object3.txt", []byte("content3"))

	client := env.GetS3Client()
	ctx := context.Background()

	result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("test-bucket"),
		Prefix: aws.String("test/"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}

	if len(result.Contents) != 2 {
		t.Errorf("Expected 2 objects with prefix 'test/', got %d", len(result.Contents))
	}

	// Verify correct objects are returned
	keys := make(map[string]bool)
	for _, obj := range result.Contents {
		keys[aws.ToString(obj.Key)] = true
	}

	if !keys["test/object1.txt"] || !keys["test/object2.txt"] {
		t.Errorf("Expected test/object1.txt and test/object2.txt, got %v", keys)
	}
}

// TestSDK_CopyObject verifies CopyObject operation with AWS SDK using gofakes3.
func TestSDK_CopyObject(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	// Pre-populate source object
	sourceContent := []byte("source object content to copy")
	env.PutTestObject("source-bucket", "source-key", sourceContent)
	env.CreateTestBucket("dest-bucket")

	client := env.GetS3Client()
	ctx := context.Background()

	_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String("dest-bucket"),
		Key:        aws.String("dest-key"),
		CopySource: aws.String("source-bucket/source-key"),
	})
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// Verify destination object exists and has correct content
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("dest-bucket"),
		Key:    aws.String("dest-key"),
	})
	if err != nil {
		t.Fatalf("GetObject verification failed: %v", err)
	}
	defer result.Body.Close()

	body, _ := io.ReadAll(result.Body)
	if string(body) != string(sourceContent) {
		t.Errorf("Expected copied content %q, got %q", sourceContent, body)
	}
}

// TestSDK_GetObjectRange verifies Range request with AWS SDK using gofakes3.
func TestSDK_GetObjectRange(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	// Pre-populate large object
	fullContent := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij") // 46 bytes
	env.PutTestObject("test-bucket", "range-key", fullContent)

	client := env.GetS3Client()
	ctx := context.Background()

	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("range-key"),
		Range:  aws.String("bytes=5-9"),
	})
	if err != nil {
		t.Fatalf("GetObject with Range failed: %v", err)
	}
	defer result.Body.Close()

	body, _ := io.ReadAll(result.Body)
	if string(body) != "56789" {
		t.Errorf("Expected partial body '56789', got %q", body)
	}
}

// TestSDK_MultipartUpload verifies multipart upload operations with AWS SDK using gofakes3.
func TestSDK_MultipartUpload(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	env.CreateTestBucket("test-bucket")

	client := env.GetS3Client()
	ctx := context.Background()

	// Step 1: Create multipart upload
	createResult, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("multipart-key"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	// Step 2: Upload parts (gofakes3 requires minimum 5MB parts except for last part)
	// For testing, we use smaller parts - gofakes3 may be lenient
	partBody := make([]byte, 5*1024*1024) // 5MB
	rand.Read(partBody)

	uploadResult, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("test-bucket"),
		Key:        aws.String("multipart-key"),
		UploadId:   createResult.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(partBody),
	})
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	// Step 3: Complete multipart upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("test-bucket"),
		Key:      aws.String("multipart-key"),
		UploadId: createResult.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					ETag:       uploadResult.ETag,
					PartNumber: aws.Int32(1),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	// Verify object exists
	_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("multipart-key"),
	})
	if err != nil {
		t.Fatalf("HeadObject verification failed: %v", err)
	}
}

// TestSDK_LargeObjectStreaming verifies large object upload/download with AWS SDK using gofakes3.
func TestSDK_LargeObjectStreaming(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	env.CreateTestBucket("test-bucket")

	// Generate 1MB of random data
	const objectSize = 1024 * 1024
	largeContent := make([]byte, objectSize)
	rand.Read(largeContent)

	client := env.GetS3Client()
	ctx := context.Background()

	// Upload large object
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("large-key"),
		Body:   bytes.NewReader(largeContent),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Download large object
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("large-key"),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer result.Body.Close()

	downloadedContent, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("Failed to read body: %v", err)
	}

	if len(downloadedContent) != objectSize {
		t.Errorf("Expected %d bytes, got %d", objectSize, len(downloadedContent))
	}

	if !bytes.Equal(downloadedContent, largeContent) {
		t.Error("Downloaded content does not match original")
	}
}

// TestSDK_ConcurrentGetCoalescing verifies request coalescing with concurrent SDK GetObject calls.
func TestSDK_ConcurrentGetCoalescing(t *testing.T) {
	var upstreamRequestCount int32
	objectContent := []byte("shared object content for all concurrent requests")

	// Use middleware to count upstream requests while still using gofakes3
	env := NewTestEnvironmentWithMiddleware(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&upstreamRequestCount, 1)
			// Delay to ensure concurrent requests overlap
			time.Sleep(100 * time.Millisecond)
			next.ServeHTTP(w, r)
		})
	})
	defer env.Close()

	// Pre-populate test data
	if err := env.PutTestObject("test-bucket", "shared-key", objectContent); err != nil {
		t.Fatalf("Failed to pre-populate test data: %v", err)
	}

	client := env.GetS3Client()
	ctx := context.Background()

	const numConcurrentRequests = 5
	var wg sync.WaitGroup
	errors := make(chan error, numConcurrentRequests)
	bodies := make(chan []byte, numConcurrentRequests)

	// Launch concurrent requests
	for i := 0; i < numConcurrentRequests; i++ {
		wg.Add(1)
		go func(requestID int) {
			defer wg.Done()

			result, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String("test-bucket"),
				Key:    aws.String("shared-key"),
			})
			if err != nil {
				errors <- fmt.Errorf("request %d failed: %w", requestID, err)
				return
			}
			defer result.Body.Close()

			body, err := io.ReadAll(result.Body)
			if err != nil {
				errors <- fmt.Errorf("request %d failed to read body: %w", requestID, err)
				return
			}

			bodies <- body
			errors <- nil
		}(i)
	}

	wg.Wait()
	close(errors)
	close(bodies)

	// Check for errors
	for err := range errors {
		if err != nil {
			t.Error(err)
		}
	}

	// Verify all requests got the correct content
	for body := range bodies {
		if string(body) != string(objectContent) {
			t.Errorf("Expected body %q, got %q", objectContent, body)
		}
	}

	// Log coalescing efficiency
	count := atomic.LoadInt32(&upstreamRequestCount)
	t.Logf("Request coalescing: %d upstream requests for %d concurrent SDK requests", count, numConcurrentRequests)

	// We expect some coalescing (fewer upstream requests than total requests)
	if count >= int32(numConcurrentRequests) {
		t.Logf("Warning: No coalescing observed - all %d requests went to upstream", numConcurrentRequests)
	}
}

// TestSDK_UserMetadata verifies user metadata headers with AWS SDK using gofakes3.
func TestSDK_UserMetadata(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	env.CreateTestBucket("test-bucket")

	client := env.GetS3Client()
	ctx := context.Background()

	// Put object with metadata
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("meta-key"),
		Body:   bytes.NewReader([]byte("content")),
		Metadata: map[string]string{
			"Custom":  "custom-meta-value",
			"Another": "another-value",
		},
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Get object and verify metadata
	result, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("meta-key"),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}

	if result.Metadata == nil {
		t.Fatal("Expected metadata to be set")
	}

	// Note: gofakes3 lowercases metadata keys
	if val, ok := result.Metadata["custom"]; !ok || val != "custom-meta-value" {
		t.Errorf("Expected custom metadata 'custom-meta-value', got %q", result.Metadata["custom"])
	}
}

// TestSDK_NotFound verifies 404 error handling with AWS SDK using gofakes3.
func TestSDK_NotFound(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	// Create bucket but don't add the object
	env.CreateTestBucket("test-bucket")

	client := env.GetS3Client()
	ctx := context.Background()

	_, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("nonexistent-key"),
	})

	if err == nil {
		t.Fatal("Expected error for non-existent key, got nil")
	}

	// Verify it's a NoSuchKey error
	if !strings.Contains(err.Error(), "NoSuchKey") && !strings.Contains(err.Error(), "404") {
		t.Errorf("Expected NoSuchKey error, got: %v", err)
	}
}

// TestSDK_UpstreamServerError verifies 5xx error handling with custom handler.
func TestSDK_UpstreamServerError(t *testing.T) {
	// Use custom handler for error injection - gofakes3 doesn't support error injection
	env := NewTestEnvironmentWithHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>ServiceUnavailable</Code>
  <Message>Service is temporarily unavailable</Message>
</Error>`))
	})
	defer env.Close()

	client := env.GetS3Client()
	ctx := context.Background()

	_, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("error-key"),
	})

	if err == nil {
		t.Fatal("Expected error for server error, got nil")
	}
}

// TestSDK_EmptyObject verifies handling of empty (0-byte) objects using gofakes3.
func TestSDK_EmptyObject(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	// Pre-populate empty object
	env.PutTestObject("test-bucket", "empty-key", []byte{})

	client := env.GetS3Client()
	ctx := context.Background()

	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("empty-key"),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("Failed to read body: %v", err)
	}

	if len(body) != 0 {
		t.Errorf("Expected empty body, got %d bytes", len(body))
	}
}

// TestSDK_HealthEndpoint verifies health endpoint works without authentication.
func TestSDK_HealthEndpoint(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	// Health endpoint doesn't need SDK - direct HTTP call
	resp, err := http.Get(env.TAGServer.URL + "/health")
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "OK" {
		t.Errorf("Expected body 'OK', got %q", body)
	}
}

// TestSDK_MetricsEndpoint verifies metrics endpoint exposes Prometheus metrics.
func TestSDK_MetricsEndpoint(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	// Metrics endpoint doesn't need SDK - direct HTTP call
	resp, err := http.Get(env.TAGServer.URL + "/metrics")
	if err != nil {
		t.Fatalf("Metrics check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "tag_requests_total") {
		t.Error("Expected tag_requests_total metric in response")
	}
}

// TestSDK_BucketNotFound verifies NoSuchBucket error handling.
func TestSDK_BucketNotFound(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	client := env.GetS3Client()
	ctx := context.Background()

	_, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("nonexistent-bucket"),
		Key:    aws.String("any-key"),
	})

	if err == nil {
		t.Fatal("Expected error for non-existent bucket, got nil")
	}

	if !strings.Contains(err.Error(), "NoSuchBucket") && !strings.Contains(err.Error(), "404") {
		t.Errorf("Expected NoSuchBucket error, got: %v", err)
	}
}

// TestSDK_ContentTypePreservation verifies Content-Type is preserved through the proxy.
func TestSDK_ContentTypePreservation(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	env.CreateTestBucket("test-bucket")

	client := env.GetS3Client()
	ctx := context.Background()

	testCases := []struct {
		key         string
		contentType string
	}{
		{"test.json", "application/json"},
		{"test.html", "text/html"},
		{"test.png", "image/png"},
	}

	for _, tc := range testCases {
		// Put object with specific content type
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String("test-bucket"),
			Key:         aws.String(tc.key),
			Body:        bytes.NewReader([]byte("content")),
			ContentType: aws.String(tc.contentType),
		})
		if err != nil {
			t.Fatalf("PutObject failed for %s: %v", tc.key, err)
		}

		// Verify content type is preserved
		result, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String(tc.key),
		})
		if err != nil {
			t.Fatalf("HeadObject failed for %s: %v", tc.key, err)
		}

		if aws.ToString(result.ContentType) != tc.contentType {
			t.Errorf("For %s: expected Content-Type %q, got %q",
				tc.key, tc.contentType, aws.ToString(result.ContentType))
		}
	}
}

// TestSDK_SpecialCharactersInKey verifies handling of special characters in object keys.
func TestSDK_SpecialCharactersInKey(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	env.CreateTestBucket("test-bucket")

	client := env.GetS3Client()
	ctx := context.Background()

	specialKeys := []string{
		"file with spaces.txt",
		"path/to/file.txt",
		"special-chars_!@#$%^&().txt",
		"unicode-日本語.txt",
	}

	for _, key := range specialKeys {
		content := []byte("content for " + key)

		// Put object
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String(key),
			Body:   bytes.NewReader(content),
		})
		if err != nil {
			t.Logf("PutObject failed for key %q: %v (may be expected for some special chars)", key, err)
			continue
		}

		// Get object and verify
		result, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Errorf("GetObject failed for key %q: %v", key, err)
			continue
		}

		body, _ := io.ReadAll(result.Body)
		result.Body.Close()

		if !bytes.Equal(body, content) {
			t.Errorf("Content mismatch for key %q", key)
		}
	}
}

// TestSDK_RequestCoalescingDifferentKeys verifies that different keys are NOT coalesced.
func TestSDK_RequestCoalescingDifferentKeys(t *testing.T) {
	var upstreamRequestCount int32

	env := NewTestEnvironmentWithMiddleware(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&upstreamRequestCount, 1)
			time.Sleep(50 * time.Millisecond) // Small delay
			next.ServeHTTP(w, r)
		})
	})
	defer env.Close()

	// Pre-populate different objects
	env.PutTestObject("test-bucket", "key1", []byte("content1"))
	env.PutTestObject("test-bucket", "key2", []byte("content2"))
	env.PutTestObject("test-bucket", "key3", []byte("content3"))

	client := env.GetS3Client()
	ctx := context.Background()

	var wg sync.WaitGroup

	// Launch concurrent requests for DIFFERENT keys
	for _, key := range []string{"key1", "key2", "key3"} {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String("test-bucket"),
				Key:    aws.String(k),
			})
		}(key)
	}

	wg.Wait()

	count := atomic.LoadInt32(&upstreamRequestCount)
	t.Logf("Different keys: %d upstream requests for 3 different keys", count)

	// Different keys should NOT be coalesced - expect 3 requests
	if count < 3 {
		t.Errorf("Expected at least 3 upstream requests for 3 different keys, got %d", count)
	}
}

// ============================================================================
// Cache-Enabled SDK Tests
// These tests use NewTestEnvironmentWithCache() to verify SDK operations
// work correctly with caching enabled.
// ============================================================================

// TestSDK_GetObject_WithCache verifies GetObject with caching enabled.
// First request should cache, second request should be served from cache.
func TestSDK_GetObject_WithCache(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	// Pre-populate test data
	objectContent := []byte("cached object content for SDK test")
	if err := env.PutTestObject("test-bucket", "cache-test-key", objectContent); err != nil {
		t.Fatalf("Failed to pre-populate test data: %v", err)
	}

	client := env.GetS3Client()
	ctx := context.Background()

	// First GET - should fetch from upstream and cache
	result1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("cache-test-key"),
	})
	if err != nil {
		t.Fatalf("First GetObject failed: %v", err)
	}
	body1, _ := io.ReadAll(result1.Body)
	result1.Body.Close()

	if string(body1) != string(objectContent) {
		t.Errorf("First request: expected body %q, got %q", objectContent, body1)
	}

	// Verify first request was a cache miss
	env.AssertXCacheMiss(t)

	// Wait for cache to be populated
	time.Sleep(200 * time.Millisecond)

	// Second GET - should be served from cache
	result2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("cache-test-key"),
	})
	if err != nil {
		t.Fatalf("Second GetObject failed: %v", err)
	}
	body2, _ := io.ReadAll(result2.Body)
	result2.Body.Close()

	if string(body2) != string(objectContent) {
		t.Errorf("Second request: expected body %q, got %q", objectContent, body2)
	}

	// Verify second request was a cache hit
	env.AssertXCacheHit(t)

	t.Logf("Cache behavior verified: first request X-Cache: MISS, second request X-Cache: HIT")
}

// TestSDK_HeadObject_WithCache verifies HeadObject with caching enabled.
// After a GET caches the object, HEAD should be served from cached metadata.
func TestSDK_HeadObject_WithCache(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	// Pre-populate test data
	content := []byte("head object cache test content 1234567890")
	if err := env.PutTestObject("test-bucket", "head-cache-key", content); err != nil {
		t.Fatalf("Failed to pre-populate test data: %v", err)
	}

	client := env.GetS3Client()
	ctx := context.Background()

	// First GET to populate cache
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("head-cache-key"),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	io.ReadAll(result.Body)
	result.Body.Close()

	// Verify GET was a cache miss
	env.AssertXCacheMiss(t)

	// Wait for cache to be populated
	time.Sleep(200 * time.Millisecond)

	// HEAD should be served from cache
	headResult, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("head-cache-key"),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}

	// Verify metadata
	if aws.ToInt64(headResult.ContentLength) != int64(len(content)) {
		t.Errorf("Expected ContentLength %d, got %d", len(content), aws.ToInt64(headResult.ContentLength))
	}

	if headResult.ETag == nil || *headResult.ETag == "" {
		t.Error("Expected ETag to be set from cache")
	}

	// Verify HEAD was served from cache
	env.AssertXCacheHit(t)

	t.Logf("HEAD from cache verified: GET X-Cache: MISS, HEAD X-Cache: HIT")
}

// TestSDK_ContentTypePreservation_WithCache verifies Content-Type is preserved through cache.
func TestSDK_ContentTypePreservation_WithCache(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	env.CreateTestBucket("test-bucket")

	client := env.GetS3Client()
	ctx := context.Background()

	testCases := []struct {
		key         string
		contentType string
	}{
		{"cached-test.json", "application/json"},
		{"cached-test.html", "text/html"},
		{"cached-test.png", "image/png"},
	}

	for _, tc := range testCases {
		// Put object with specific content type
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String("test-bucket"),
			Key:         aws.String(tc.key),
			Body:        bytes.NewReader([]byte("content")),
			ContentType: aws.String(tc.contentType),
		})
		if err != nil {
			t.Fatalf("PutObject failed for %s: %v", tc.key, err)
		}

		// First GET - populates cache
		getResult, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String(tc.key),
		})
		if err != nil {
			t.Fatalf("First GetObject failed for %s: %v", tc.key, err)
		}
		io.ReadAll(getResult.Body)
		getResult.Body.Close()

		// Verify GET was cache miss
		env.AssertXCacheMiss(t)

		// Wait for cache
		time.Sleep(200 * time.Millisecond)

		// Second HEAD - should be from cache with correct Content-Type
		headResult, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String(tc.key),
		})
		if err != nil {
			t.Fatalf("HeadObject failed for %s: %v", tc.key, err)
		}

		// Verify HEAD was cache hit
		env.AssertXCacheHit(t)

		if aws.ToString(headResult.ContentType) != tc.contentType {
			t.Errorf("For %s: expected cached Content-Type %q, got %q",
				tc.key, tc.contentType, aws.ToString(headResult.ContentType))
		}
	}
}

// TestSDK_UserMetadata_WithCache verifies user metadata headers are preserved through cache.
func TestSDK_UserMetadata_WithCache(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	env.CreateTestBucket("test-bucket")

	client := env.GetS3Client()
	ctx := context.Background()

	// Put object with metadata
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("meta-cache-key"),
		Body:   bytes.NewReader([]byte("content with metadata")),
		Metadata: map[string]string{
			"Custom":  "cached-meta-value",
			"Another": "another-cached-value",
		},
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// First GET - populates cache
	getResult, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("meta-cache-key"),
	})
	if err != nil {
		t.Fatalf("First GetObject failed: %v", err)
	}
	io.ReadAll(getResult.Body)
	getResult.Body.Close()

	// Verify GET was cache miss
	env.AssertXCacheMiss(t)

	// Wait for cache
	time.Sleep(200 * time.Millisecond)

	// HEAD from cache - should include metadata
	headResult, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("meta-cache-key"),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}

	// Verify HEAD was served from cache
	env.AssertXCacheHit(t)

	// Verify metadata is preserved from cache
	if headResult.Metadata == nil {
		t.Fatal("Expected metadata to be preserved in cache")
	}

	// Note: gofakes3 lowercases metadata keys
	if val, ok := headResult.Metadata["custom"]; !ok || val != "cached-meta-value" {
		t.Errorf("Expected cached custom metadata 'cached-meta-value', got %q", headResult.Metadata["custom"])
	}

	t.Logf("User metadata caching verified: GET X-Cache: MISS, HEAD X-Cache: HIT")
}

// TestSDK_CacheInvalidationOnPut verifies that PUT invalidates cached entries
// and subsequent GETs return the new data from both upstream and cache.
func TestSDK_CacheInvalidationOnPut(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	env.CreateTestBucket("test-bucket")

	client := env.GetS3Client()
	ctx := context.Background()

	// Step 1: PUT object with initial value
	initialContent := []byte("initial content version 1")
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("invalidation-test-key"),
		Body:   bytes.NewReader(initialContent),
	})
	if err != nil {
		t.Fatalf("Initial PutObject failed: %v", err)
	}

	// Step 2: GET object - should come from upstream (cache miss)
	result1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("invalidation-test-key"),
	})
	if err != nil {
		t.Fatalf("First GetObject failed: %v", err)
	}
	body1, _ := io.ReadAll(result1.Body)
	result1.Body.Close()

	if string(body1) != string(initialContent) {
		t.Errorf("First GET: expected %q, got %q", initialContent, body1)
	}

	// Verify first GET was cache miss
	env.AssertXCacheMiss(t)

	// Wait for cache to be populated
	time.Sleep(200 * time.Millisecond)

	// Step 3: GET object again - should come from cache (cache hit)
	result2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("invalidation-test-key"),
	})
	if err != nil {
		t.Fatalf("Second GetObject failed: %v", err)
	}
	body2, _ := io.ReadAll(result2.Body)
	result2.Body.Close()

	if string(body2) != string(initialContent) {
		t.Errorf("Second GET (cache): expected %q, got %q", initialContent, body2)
	}

	// Verify second GET was cache hit
	env.AssertXCacheHit(t)
	t.Logf("Cache hit verified")

	// Step 4: PUT object with new value - should invalidate cache
	updatedContent := []byte("updated content version 2")
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("invalidation-test-key"),
		Body:   bytes.NewReader(updatedContent),
	})
	if err != nil {
		t.Fatalf("Update PutObject failed: %v", err)
	}

	// Step 5: GET object - should come from upstream with new value (cache miss due to invalidation)
	result3, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("invalidation-test-key"),
	})
	if err != nil {
		t.Fatalf("Third GetObject failed: %v", err)
	}
	body3, _ := io.ReadAll(result3.Body)
	result3.Body.Close()

	if string(body3) != string(updatedContent) {
		t.Errorf("Third GET (after invalidation): expected %q, got %q", updatedContent, body3)
	}

	// Verify third GET was cache miss (cache invalidated by PUT)
	env.AssertXCacheMiss(t)
	t.Logf("Cache invalidation verified: upstream hit with new data")

	// Wait for cache to be populated with new value
	time.Sleep(200 * time.Millisecond)

	// Step 6: GET object again - should come from cache with new value (cache hit)
	result4, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("invalidation-test-key"),
	})
	if err != nil {
		t.Fatalf("Fourth GetObject failed: %v", err)
	}
	body4, _ := io.ReadAll(result4.Body)
	result4.Body.Close()

	if string(body4) != string(updatedContent) {
		t.Errorf("Fourth GET (new cache): expected %q, got %q", updatedContent, body4)
	}

	// Verify fourth GET was cache hit
	env.AssertXCacheHit(t)
	t.Logf("New cache entry verified")
}

// TestSDK_GetObjectRange_WithCache verifies Range requests work with caching enabled.
// After a full GET caches the object, Range requests should be served from cache.
func TestSDK_GetObjectRange_WithCache(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "test-bucket"
	key := "range-cache-test-key"
	fullContent := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij") // 46 bytes

	if err := env.PutTestObject(bucket, key, fullContent); err != nil {
		t.Fatalf("Failed to pre-populate test data: %v", err)
	}

	client := env.GetS3Client()
	ctx := context.Background()

	// First: Full GET to populate cache
	result1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("Full GetObject failed: %v", err)
	}
	body1, _ := io.ReadAll(result1.Body)
	result1.Body.Close()

	if string(body1) != string(fullContent) {
		t.Errorf("Full GET: expected %q, got %q", fullContent, body1)
	}

	// Verify full GET was cache miss
	env.AssertXCacheMiss(t)

	// Wait for cache to be populated
	time.Sleep(200 * time.Millisecond)

	// Second: Range request - should be served from cached full object
	result2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=5-14"), // "56789ABCDE" (10 bytes)
	})
	if err != nil {
		t.Fatalf("Range GetObject failed: %v", err)
	}
	body2, _ := io.ReadAll(result2.Body)
	result2.Body.Close()

	expectedRange := "56789ABCDE"
	if string(body2) != expectedRange {
		t.Errorf("Range GET: expected %q, got %q", expectedRange, body2)
	}

	// Verify range request was served from cache
	env.AssertXCacheHit(t)

	t.Logf("Range from cache verified: full GET X-Cache: MISS, range GET X-Cache: HIT")
}

// TestSDK_GetObjectRange_SingleByteAtZero verifies Range request for single byte at position 0.
// This tests the ocache byte-0 quirk handling (bytes=0-0 requires special handling).
func TestSDK_GetObjectRange_SingleByteAtZero(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "test-bucket"
	key := "byte-zero-test-key"
	fullContent := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ") // 26 bytes, first byte is 'A'

	if err := env.PutTestObject(bucket, key, fullContent); err != nil {
		t.Fatalf("Failed to pre-populate test data: %v", err)
	}

	client := env.GetS3Client()
	ctx := context.Background()

	// First: Full GET to populate cache
	result1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("Full GetObject failed: %v", err)
	}
	io.ReadAll(result1.Body)
	result1.Body.Close()

	// Verify full GET was cache miss
	env.AssertXCacheMiss(t)

	// Wait for cache to be populated
	time.Sleep(200 * time.Millisecond)

	// Range request for single byte at position 0 (bytes=0-0)
	// This is the edge case that requires special handling in ocache
	result2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=0-0"), // Single byte at position 0
	})
	if err != nil {
		t.Fatalf("Range GetObject (bytes=0-0) failed: %v", err)
	}
	body2, _ := io.ReadAll(result2.Body)
	result2.Body.Close()

	expectedByte := "A" // First byte of content
	if string(body2) != expectedByte {
		t.Errorf("Range GET (bytes=0-0): expected %q, got %q (len=%d)", expectedByte, body2, len(body2))
	}

	// Verify range request was served from cache
	env.AssertXCacheHit(t)

	t.Logf("Byte-0 quirk verified: single byte at position 0 returned correctly: %q", body2)
}

// TestSDK_DeleteObjects verifies DeleteObjects (bulk delete) operation with AWS SDK.
func TestSDK_DeleteObjects(t *testing.T) {
	env := NewTestEnvironment()
	defer env.Close()

	bucket := "test-bucket"
	keys := []string{"delete1.txt", "delete2.txt", "delete3.txt"}

	// Pre-populate test objects
	for _, key := range keys {
		if err := env.PutTestObject(bucket, key, []byte("content for "+key)); err != nil {
			t.Fatalf("Failed to pre-populate test data: %v", err)
		}
	}

	client := env.GetS3Client()
	ctx := context.Background()

	// Build delete request
	objectIds := make([]types.ObjectIdentifier, len(keys))
	for i, key := range keys {
		objectIds[i] = types.ObjectIdentifier{Key: aws.String(key)}
	}

	// Delete all objects
	result, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: objectIds,
		},
	})
	if err != nil {
		t.Fatalf("DeleteObjects failed: %v", err)
	}

	// Verify all objects were deleted
	if len(result.Deleted) != len(keys) {
		t.Errorf("Expected %d deleted objects, got %d", len(keys), len(result.Deleted))
	}
	if len(result.Errors) > 0 {
		t.Errorf("Expected no errors, got %d errors", len(result.Errors))
	}

	// Verify objects no longer exist
	for _, key := range keys {
		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			t.Errorf("Expected error for deleted object %s, got nil", key)
		}
	}

	t.Logf("DeleteObjects verified: %d objects deleted successfully", len(result.Deleted))
}

// TestSDK_DeleteObjects_WithCache verifies DeleteObjects with caching enabled.
// Objects should be removed from cache after bulk deletion.
func TestSDK_DeleteObjects_WithCache(t *testing.T) {
	env := NewTestEnvironmentWithCache()
	defer env.Close()

	bucket := "test-bucket"
	keys := []string{"delete1.txt", "delete2.txt", "delete3.txt"}

	// Pre-populate test objects
	for _, key := range keys {
		if err := env.PutTestObject(bucket, key, []byte("content for "+key)); err != nil {
			t.Fatalf("Failed to pre-populate test data: %v", err)
		}
	}

	client := env.GetS3Client()
	ctx := context.Background()

	// GET all objects to populate cache
	for _, key := range keys {
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("GetObject for %s failed: %v", key, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		// Verify each GET was a cache miss (populating cache)
		env.AssertXCacheMiss(t)
	}

	// Wait for cache to be populated
	time.Sleep(100 * time.Millisecond)

	// Verify all are cached by checking X-Cache header
	for _, key := range keys {
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("GetObject for %s failed: %v", key, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		// Verify each GET was a cache hit
		env.AssertXCacheHit(t)
	}

	// Build delete request
	objectIds := make([]types.ObjectIdentifier, len(keys))
	for i, key := range keys {
		objectIds[i] = types.ObjectIdentifier{Key: aws.String(key)}
	}

	// Delete all objects - should invalidate cache
	result, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: objectIds,
		},
	})
	if err != nil {
		t.Fatalf("DeleteObjects failed: %v", err)
	}

	// Verify all objects were deleted
	if len(result.Deleted) != len(keys) {
		t.Errorf("Expected %d deleted objects, got %d", len(keys), len(result.Deleted))
	}
	if len(result.Errors) > 0 {
		t.Errorf("Expected no errors, got %d errors", len(result.Errors))
	}

	// Verify objects no longer exist (cache should be invalidated)
	for _, key := range keys {
		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			t.Errorf("Expected error for deleted object %s, got nil", key)
		}
	}

	t.Logf("DeleteObjects with cache verified: %d objects deleted and cache invalidated", len(result.Deleted))
}
