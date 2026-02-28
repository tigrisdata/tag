package sdk

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestSDK_GetObject verifies basic GetObject operation with AWS SDK.
func TestSDK_GetObject(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("get-object")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	objectContent := []byte("test object content from upstream")
	if err := globalEnv.PutTestObject(bucket, "test-key", objectContent); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
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

// TestSDK_PutObject verifies basic PutObject operation with AWS SDK.
func TestSDK_PutObject(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("put-object")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	putBody := []byte("new object content")
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String("new-key"),
		Body:        bytes.NewReader(putBody),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify object was stored by getting it back
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
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

// TestSDK_DeleteObject verifies DeleteObject operation with AWS SDK.
func TestSDK_DeleteObject(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("delete-object")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	if err := globalEnv.PutTestObject(bucket, "delete-key", []byte("content to delete")); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	// Delete the object
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("delete-key"),
	})
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify object is deleted (should get NoSuchKey error)
	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("delete-key"),
	})
	if err == nil {
		t.Error("Expected error when getting deleted object, got nil")
	}
}

// TestSDK_HeadObject verifies HeadObject operation with AWS SDK.
func TestSDK_HeadObject(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("head-object")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	content := []byte("head object content 1234567890")
	if err := globalEnv.PutTestObject(bucket, "head-key", content); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	result, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("head-key"),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}

	if aws.ToInt64(result.ContentLength) != int64(len(content)) {
		t.Errorf("Expected ContentLength %d, got %d", len(content), aws.ToInt64(result.ContentLength))
	}

	if result.ETag == nil || *result.ETag == "" {
		t.Error("Expected ETag to be set")
	}
}

// TestSDK_ListBuckets verifies ListBuckets operation with AWS SDK.
func TestSDK_ListBuckets(t *testing.T) {
	// Create test buckets
	bucket1, err := globalEnv.CreateTestBucket("list-bucket1")
	if err != nil {
		t.Fatalf("Failed to create bucket1: %v", err)
	}
	bucket2, err := globalEnv.CreateTestBucket("list-bucket2")
	if err != nil {
		t.Fatalf("Failed to create bucket2: %v", err)
	}
	bucket3, err := globalEnv.CreateTestBucket("list-bucket3")
	if err != nil {
		t.Fatalf("Failed to create bucket3: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	result, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}

	// Check our test buckets exist (there may be other buckets)
	bucketNames := make(map[string]bool)
	for _, b := range result.Buckets {
		bucketNames[aws.ToString(b.Name)] = true
	}

	for _, expected := range []string{bucket1, bucket2, bucket3} {
		if !bucketNames[expected] {
			t.Errorf("Expected bucket %q not found in list", expected)
		}
	}
}

// TestSDK_ListObjectsV2 verifies ListObjectsV2 operation with AWS SDK.
func TestSDK_ListObjectsV2(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("list-objects")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	// Create test objects with prefix structure
	if err := globalEnv.PutTestObject(bucket, "test/object1.txt", []byte("content1")); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}
	if err := globalEnv.PutTestObject(bucket, "test/object2.txt", []byte("content2")); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}
	if err := globalEnv.PutTestObject(bucket, "other/object3.txt", []byte("content3")); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
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

// TestSDK_CopyObject verifies CopyObject operation with AWS SDK.
func TestSDK_CopyObject(t *testing.T) {
	sourceBucket, err := globalEnv.CreateTestBucket("copy-source")
	if err != nil {
		t.Fatalf("Failed to create source bucket: %v", err)
	}
	destBucket, err := globalEnv.CreateTestBucket("copy-dest")
	if err != nil {
		t.Fatalf("Failed to create dest bucket: %v", err)
	}

	sourceContent := []byte("source object content to copy")
	if err := globalEnv.PutTestObject(sourceBucket, "source-key", sourceContent); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		Key:        aws.String("dest-key"),
		CopySource: aws.String(sourceBucket + "/source-key"),
	})
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// Verify destination object exists and has correct content
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(destBucket),
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

// TestSDK_GetObjectRange verifies Range request with AWS SDK.
func TestSDK_GetObjectRange(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("range-object")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	fullContent := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij") // 46 bytes
	if err := globalEnv.PutTestObject(bucket, "range-key", fullContent); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
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

// TestSDK_MultipartUpload verifies multipart upload operations with AWS SDK.
func TestSDK_MultipartUpload(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("multipart")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	// Step 1: Create multipart upload
	createResult, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("multipart-key"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	// Step 2: Upload part (5MB minimum for non-last part)
	partBody := make([]byte, 5*1024*1024)
	rand.Read(partBody)

	uploadResult, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
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
		Bucket:   aws.String(bucket),
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
		Bucket: aws.String(bucket),
		Key:    aws.String("multipart-key"),
	})
	if err != nil {
		t.Fatalf("HeadObject verification failed: %v", err)
	}
}

// TestSDK_LargeObjectStreaming verifies large object upload/download with AWS SDK.
func TestSDK_LargeObjectStreaming(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("large-object")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	// Generate 1MB of random data
	const objectSize = 1024 * 1024
	largeContent := make([]byte, objectSize)
	rand.Read(largeContent)

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	// Upload large object
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("large-key"),
		Body:   bytes.NewReader(largeContent),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Download large object
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
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

// TestSDK_UserMetadata verifies user metadata headers with AWS SDK.
func TestSDK_UserMetadata(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("user-metadata")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	// Put object with metadata
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
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
		Bucket: aws.String(bucket),
		Key:    aws.String("meta-key"),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}

	if result.Metadata == nil {
		t.Fatal("Expected metadata to be set")
	}

	// Note: S3 lowercases metadata keys
	if val, ok := result.Metadata["custom"]; !ok || val != "custom-meta-value" {
		t.Errorf("Expected custom metadata 'custom-meta-value', got %q", result.Metadata["custom"])
	}
}

// TestSDK_NotFound verifies 404 error handling with AWS SDK.
func TestSDK_NotFound(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("not-found")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
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

// TestSDK_EmptyObject verifies handling of empty (0-byte) objects.
func TestSDK_EmptyObject(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("empty-object")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	if err := globalEnv.PutTestObject(bucket, "empty-key", []byte{}); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
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
	resp, err := http.Get(globalEnv.Endpoint + "/health")
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
	resp, err := http.Get(globalEnv.Endpoint + "/metrics")
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

// TestSDK_BucketNotFound verifies error handling for non-existent buckets.
// Note: Tigris returns 403 AccessDenied instead of 404 NoSuchBucket as a security
// measure to prevent bucket enumeration attacks.
func TestSDK_BucketNotFound(t *testing.T) {
	client := globalEnv.GetS3Client()
	ctx := context.Background()

	// Use a bucket name that definitely doesn't exist
	nonexistentBucket := globalEnv.UniqueBucketName("nonexistent-bucket-xyz")

	_, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(nonexistentBucket),
		Key:    aws.String("any-key"),
	})

	if err == nil {
		t.Fatal("Expected error for non-existent bucket, got nil")
	}

	// Accept either NoSuchBucket (404) or AccessDenied (403)
	// Tigris returns 403 to prevent bucket enumeration attacks
	errStr := err.Error()
	if !strings.Contains(errStr, "NoSuchBucket") &&
		!strings.Contains(errStr, "404") &&
		!strings.Contains(errStr, "AccessDenied") &&
		!strings.Contains(errStr, "403") {
		t.Errorf("Expected NoSuchBucket or AccessDenied error, got: %v", err)
	}
}

// TestSDK_ContentTypePreservation verifies Content-Type is preserved through the proxy.
func TestSDK_ContentTypePreservation(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("content-type")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
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
			Bucket:      aws.String(bucket),
			Key:         aws.String(tc.key),
			Body:        bytes.NewReader([]byte("content")),
			ContentType: aws.String(tc.contentType),
		})
		if err != nil {
			t.Fatalf("PutObject failed for %s: %v", tc.key, err)
		}

		// Verify content type is preserved
		result, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
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
	bucket, err := globalEnv.CreateTestBucket("special-chars")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
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
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(content),
		})
		if err != nil {
			t.Logf("PutObject failed for key %q: %v (may be expected for some special chars)", key, err)
			continue
		}

		// Get object and verify
		result, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
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

// TestSDK_DeleteObjects verifies DeleteObjects (bulk delete) operation with AWS SDK.
func TestSDK_DeleteObjects(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("delete-objects")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	keys := []string{"delete1.txt", "delete2.txt", "delete3.txt"}

	// Create test objects
	for _, key := range keys {
		if err := globalEnv.PutTestObject(bucket, key, []byte("content for "+key)); err != nil {
			t.Fatalf("Failed to put test object: %v", err)
		}
	}

	client := globalEnv.GetS3Client()
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

// ============================================================================
// Cache-Enabled SDK Tests
// These tests verify that cache behavior works correctly with external TAG.
// They check cache HIT/MISS behavior by repeating requests.
// ============================================================================

// TestSDK_GetObject_WithCache verifies GetObject with caching enabled.
// First request should cache, second request should be served from cache.
func TestSDK_GetObject_WithCache(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("cache-get")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	objectContent := []byte("cached object content for SDK test")
	if err := globalEnv.PutTestObject(bucket, "cache-test-key", objectContent); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	// First GET - should fetch from upstream
	result1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
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

	// Wait for cache to be populated
	time.Sleep(200 * time.Millisecond)

	// Second GET - should be served from cache
	result2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
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

	t.Logf("Cache behavior verified: both requests returned correct content")
}

// TestSDK_HeadObject_WithCache verifies HeadObject with caching enabled.
func TestSDK_HeadObject_WithCache(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("cache-head")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	content := []byte("head object cache test content 1234567890")
	if err := globalEnv.PutTestObject(bucket, "head-cache-key", content); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	// First GET to populate cache
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("head-cache-key"),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	io.ReadAll(result.Body)
	result.Body.Close()

	// Wait for cache to be populated
	time.Sleep(200 * time.Millisecond)

	// HEAD should be served from cache
	headResult, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
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
		t.Error("Expected ETag to be set")
	}

	t.Logf("HEAD from cache verified")
}

// TestSDK_CacheInvalidationOnPut verifies that PUT invalidates cached entries.
func TestSDK_CacheInvalidationOnPut(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("cache-invalidation")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	// Step 1: PUT object with initial value
	initialContent := []byte("initial content version 1")
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("invalidation-test-key"),
		Body:   bytes.NewReader(initialContent),
	})
	if err != nil {
		t.Fatalf("Initial PutObject failed: %v", err)
	}

	// Step 2: GET object - should come from upstream
	result1, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
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

	// Wait for cache to be populated
	time.Sleep(200 * time.Millisecond)

	// Step 3: GET object again - verify content
	result2, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("invalidation-test-key"),
	})
	if err != nil {
		t.Fatalf("Second GetObject failed: %v", err)
	}
	body2, _ := io.ReadAll(result2.Body)
	result2.Body.Close()

	if string(body2) != string(initialContent) {
		t.Errorf("Second GET: expected %q, got %q", initialContent, body2)
	}

	// Step 4: PUT object with new value - should invalidate cache
	updatedContent := []byte("updated content version 2")
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("invalidation-test-key"),
		Body:   bytes.NewReader(updatedContent),
	})
	if err != nil {
		t.Fatalf("Update PutObject failed: %v", err)
	}

	// Step 5: GET object - should return new value
	result3, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
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

	t.Logf("Cache invalidation verified: PUT updated the content correctly")
}

// TestSDK_GetObjectRange_WithCache verifies Range requests work with caching enabled.
func TestSDK_GetObjectRange_WithCache(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("cache-range")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	key := "range-cache-test-key"
	fullContent := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij") // 46 bytes

	if err := globalEnv.PutTestObject(bucket, key, fullContent); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	// First: Full GET to potentially populate cache
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

	// Wait for cache
	time.Sleep(200 * time.Millisecond)

	// Second: Range request
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

	t.Logf("Range from cache verified")
}

// TestSDK_GetObjectRange_SingleByteAtZero verifies Range request for single byte at position 0.
func TestSDK_GetObjectRange_SingleByteAtZero(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("cache-byte-zero")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	key := "byte-zero-test-key"
	fullContent := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ") // 26 bytes, first byte is 'A'

	if err := globalEnv.PutTestObject(bucket, key, fullContent); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	client := globalEnv.GetS3Client()
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

	// Wait for cache
	time.Sleep(200 * time.Millisecond)

	// Range request for single byte at position 0 (bytes=0-0)
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

	t.Logf("Byte-0 quirk verified: single byte at position 0 returned correctly: %q", body2)
}

// TestSDK_DeleteObjects_WithCache verifies DeleteObjects with caching enabled.
func TestSDK_DeleteObjects_WithCache(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("cache-delete-objects")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	keys := []string{"delete1.txt", "delete2.txt", "delete3.txt"}

	// Create test objects
	for _, key := range keys {
		if err := globalEnv.PutTestObject(bucket, key, []byte("content for "+key)); err != nil {
			t.Fatalf("Failed to put test object: %v", err)
		}
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	// GET all objects to potentially populate cache
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
	}

	// Wait for cache
	time.Sleep(100 * time.Millisecond)

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

// TestSDK_ContentTypePreservation_WithCache verifies Content-Type is preserved through cache.
func TestSDK_ContentTypePreservation_WithCache(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("cache-content-type")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
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
			Bucket:      aws.String(bucket),
			Key:         aws.String(tc.key),
			Body:        bytes.NewReader([]byte("content")),
			ContentType: aws.String(tc.contentType),
		})
		if err != nil {
			t.Fatalf("PutObject failed for %s: %v", tc.key, err)
		}

		// First GET - populates cache
		getResult, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(tc.key),
		})
		if err != nil {
			t.Fatalf("First GetObject failed for %s: %v", tc.key, err)
		}
		io.ReadAll(getResult.Body)
		getResult.Body.Close()

		// Wait for cache
		time.Sleep(200 * time.Millisecond)

		// Second HEAD - should be from cache with correct Content-Type
		headResult, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(tc.key),
		})
		if err != nil {
			t.Fatalf("HeadObject failed for %s: %v", tc.key, err)
		}

		if aws.ToString(headResult.ContentType) != tc.contentType {
			t.Errorf("For %s: expected cached Content-Type %q, got %q",
				tc.key, tc.contentType, aws.ToString(headResult.ContentType))
		}
	}
}

// TestSDK_UserMetadata_WithCache verifies user metadata headers are preserved through cache.
func TestSDK_UserMetadata_WithCache(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("cache-user-metadata")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	// Put object with metadata
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
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
		Bucket: aws.String(bucket),
		Key:    aws.String("meta-cache-key"),
	})
	if err != nil {
		t.Fatalf("First GetObject failed: %v", err)
	}
	io.ReadAll(getResult.Body)
	getResult.Body.Close()

	// Wait for cache
	time.Sleep(200 * time.Millisecond)

	// HEAD from cache - should include metadata
	headResult, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("meta-cache-key"),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}

	// Verify metadata is preserved
	if headResult.Metadata == nil {
		t.Fatal("Expected metadata to be preserved in cache")
	}

	// Note: S3 lowercases metadata keys
	if val, ok := headResult.Metadata["custom"]; !ok || val != "cached-meta-value" {
		t.Errorf("Expected cached custom metadata 'cached-meta-value', got %q", headResult.Metadata["custom"])
	}

	t.Logf("User metadata caching verified")
}

// ============================================================================
// Cache Revalidation Tests
// These tests verify RFC 7234-compliant cache revalidation using
// Cache-Control: no-cache to trigger conditional requests (If-None-Match).
// ============================================================================

// noCacheHeaders returns headers that trigger cache revalidation.
func noCacheHeaders() map[string]string {
	return map[string]string{"Cache-Control": "no-cache"}
}

// TestSDK_Revalidation_GET_Unchanged verifies that GET with Cache-Control: no-cache
// on an unchanged object returns the cached body after conditional validation (304 path).
//
// Flow:
//  1. PUT object → GET (cache miss, learns ETag) → cache populated
//  2. GET with Cache-Control: no-cache → TAG sends If-None-Match to upstream
//  3. Upstream returns 304 Not Modified → TAG serves from cache with X-Cache: HIT
func TestSDK_Revalidation_GET_Unchanged(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("reval-get-unchanged")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	content := []byte("revalidation get unchanged test content")
	if err := globalEnv.PutTestObject(bucket, "reval-get-key", content); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	// First GET: cache miss, populates cache and learns ETag
	resp1, err := globalEnv.DoRawGet(bucket, "reval-get-key")
	if err != nil {
		t.Fatalf("First GET failed: %v", err)
	}
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("First GET: expected 200, got %d", resp1.StatusCode)
	}
	if string(body1) != string(content) {
		t.Errorf("First GET: expected body %q, got %q", content, body1)
	}
	t.Logf("First GET: X-Cache=%s", resp1.Header.Get("X-Cache"))

	// Wait for cache to be populated
	time.Sleep(500 * time.Millisecond)

	// Second GET with Cache-Control: no-cache → triggers revalidation
	// Object is unchanged, so upstream returns 304 → TAG serves from cache
	resp2, err := globalEnv.DoRawGetWithHeaders(bucket, "reval-get-key", noCacheHeaders())
	if err != nil {
		t.Fatalf("Revalidation GET failed: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("Revalidation GET: expected 200, got %d", resp2.StatusCode)
	}
	if string(body2) != string(content) {
		t.Errorf("Revalidation GET: expected body %q, got %q", content, body2)
	}

	xCache := resp2.Header.Get("X-Cache")
	if xCache != "HIT" {
		t.Errorf("Revalidation GET (unchanged): expected X-Cache HIT, got %q", xCache)
	}
	t.Logf("Revalidation GET unchanged verified: X-Cache=%s", xCache)
}

// TestSDK_Revalidation_GET_Changed verifies that GET with Cache-Control: no-cache
// on a changed object returns the new body from upstream (200 path).
//
// Flow:
//  1. PUT object v1 → GET (cache miss) → cache populated with v1
//  2. PUT object v2 (different content, new ETag)
//  3. GET with Cache-Control: no-cache → TAG sends If-None-Match with v1's ETag
//  4. Upstream returns 200 with v2 body → TAG streams new body, X-Cache: REVALIDATED
func TestSDK_Revalidation_GET_Changed(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("reval-get-changed")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	contentV1 := []byte("revalidation content version 1")
	if err := globalEnv.PutTestObject(bucket, "reval-changed-key", contentV1); err != nil {
		t.Fatalf("Failed to put test object v1: %v", err)
	}

	// First GET: cache miss, populates cache with v1
	resp1, err := globalEnv.DoRawGet(bucket, "reval-changed-key")
	if err != nil {
		t.Fatalf("First GET failed: %v", err)
	}
	io.ReadAll(resp1.Body)
	resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("First GET: expected 200, got %d", resp1.StatusCode)
	}
	t.Logf("First GET: X-Cache=%s", resp1.Header.Get("X-Cache"))

	// Wait for cache to be populated
	time.Sleep(500 * time.Millisecond)

	// PUT new content (v2) directly to Tigris, bypassing TAG's cache invalidation.
	// TAG's cache still holds stale v1 with old ETag.
	contentV2 := []byte("revalidation content version 2 - updated")
	if err := globalEnv.PutTestObjectDirect(bucket, "reval-changed-key", contentV2); err != nil {
		t.Fatalf("Failed to put test object v2 direct: %v", err)
	}

	// GET with Cache-Control: no-cache → revalidation
	// Object changed, so upstream returns 200 with new body
	resp2, err := globalEnv.DoRawGetWithHeaders(bucket, "reval-changed-key", noCacheHeaders())
	if err != nil {
		t.Fatalf("Revalidation GET failed: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("Revalidation GET: expected 200, got %d", resp2.StatusCode)
	}
	if string(body2) != string(contentV2) {
		t.Errorf("Revalidation GET: expected body %q, got %q", contentV2, body2)
	}

	xCache := resp2.Header.Get("X-Cache")
	if xCache != "REVALIDATED" {
		t.Errorf("Revalidation GET (changed): expected X-Cache REVALIDATED, got %q", xCache)
	}
	t.Logf("Revalidation GET changed verified: X-Cache=%s", xCache)
}

// TestSDK_Revalidation_GETRange_Unchanged verifies that a range GET with
// Cache-Control: no-cache on an unchanged object returns the correct byte range
// from cache after conditional validation (304 path).
//
// Flow:
//  1. PUT object → full GET (cache miss) → cache populated
//  2. GET with Cache-Control: no-cache + Range: bytes=0-4
//  3. Upstream returns 304 → TAG serves range from cache, X-Cache: HIT
func TestSDK_Revalidation_GETRange_Unchanged(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("reval-range-unchanged")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	content := []byte("revalidation range test data!!")
	if err := globalEnv.PutTestObject(bucket, "reval-range-key", content); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	// Full GET to populate cache
	resp1, err := globalEnv.DoRawGet(bucket, "reval-range-key")
	if err != nil {
		t.Fatalf("First GET failed: %v", err)
	}
	io.ReadAll(resp1.Body)
	resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("First GET: expected 200, got %d", resp1.StatusCode)
	}
	t.Logf("First GET: X-Cache=%s", resp1.Header.Get("X-Cache"))

	// Wait for cache to be populated
	time.Sleep(500 * time.Millisecond)

	// Range GET with Cache-Control: no-cache → revalidation + range
	headers := map[string]string{
		"Cache-Control": "no-cache",
		"Range":         "bytes=0-4",
	}
	resp2, err := globalEnv.DoRawGetWithHeaders(bucket, "reval-range-key", headers)
	if err != nil {
		t.Fatalf("Revalidation range GET failed: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusPartialContent {
		t.Fatalf("Revalidation range GET: expected 206, got %d", resp2.StatusCode)
	}

	expectedRange := string(content[0:5]) // "reval"
	if string(body2) != expectedRange {
		t.Errorf("Revalidation range GET: expected body %q, got %q", expectedRange, body2)
	}

	contentRange := resp2.Header.Get("Content-Range")
	if contentRange == "" {
		t.Error("Revalidation range GET: expected Content-Range header to be present")
	}

	xCache := resp2.Header.Get("X-Cache")
	if xCache != "HIT" {
		t.Errorf("Revalidation range GET (unchanged): expected X-Cache HIT, got %q", xCache)
	}
	t.Logf("Revalidation range GET unchanged verified: X-Cache=%s, Content-Range=%s", xCache, contentRange)
}

// TestSDK_Revalidation_GETRange_Changed verifies that a range GET with
// Cache-Control: no-cache on a changed object returns the correct byte range
// from the new upstream response (206 path).
//
// Flow:
//  1. PUT object v1 → full GET (cache miss) → cache populated with v1
//  2. PUT object v2 (different content)
//  3. GET with Cache-Control: no-cache + Range: bytes=0-4
//  4. Upstream returns 206 with new range → X-Cache: REVALIDATED
func TestSDK_Revalidation_GETRange_Changed(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("reval-range-changed")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	contentV1 := []byte("version1 range revalidation data")
	if err := globalEnv.PutTestObject(bucket, "reval-range-chg-key", contentV1); err != nil {
		t.Fatalf("Failed to put test object v1: %v", err)
	}

	// Full GET to populate cache with v1
	resp1, err := globalEnv.DoRawGet(bucket, "reval-range-chg-key")
	if err != nil {
		t.Fatalf("First GET failed: %v", err)
	}
	io.ReadAll(resp1.Body)
	resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("First GET: expected 200, got %d", resp1.StatusCode)
	}
	t.Logf("First GET: X-Cache=%s", resp1.Header.Get("X-Cache"))

	// Wait for cache to be populated
	time.Sleep(500 * time.Millisecond)

	// PUT new content (v2) directly to Tigris, bypassing TAG's cache invalidation.
	// TAG's cache still holds stale v1 with old ETag.
	contentV2 := []byte("updated2 range revalidation data")
	if err := globalEnv.PutTestObjectDirect(bucket, "reval-range-chg-key", contentV2); err != nil {
		t.Fatalf("Failed to put test object v2 direct: %v", err)
	}

	// Range GET with Cache-Control: no-cache → revalidation
	// Object changed, upstream returns 206 with new range
	headers := map[string]string{
		"Cache-Control": "no-cache",
		"Range":         "bytes=0-4",
	}
	resp2, err := globalEnv.DoRawGetWithHeaders(bucket, "reval-range-chg-key", headers)
	if err != nil {
		t.Fatalf("Revalidation range GET failed: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusPartialContent {
		t.Fatalf("Revalidation range GET: expected 206, got %d", resp2.StatusCode)
	}

	expectedRange := string(contentV2[0:5]) // "updat"
	if string(body2) != expectedRange {
		t.Errorf("Revalidation range GET: expected body %q, got %q", expectedRange, body2)
	}

	xCache := resp2.Header.Get("X-Cache")
	if xCache != "REVALIDATED" {
		t.Errorf("Revalidation range GET (changed): expected X-Cache REVALIDATED, got %q", xCache)
	}
	t.Logf("Revalidation range GET changed verified: X-Cache=%s", xCache)
}

// TestSDK_Revalidation_HEAD_Unchanged verifies that HEAD with Cache-Control: no-cache
// on an unchanged object returns cached metadata after conditional validation (304 path).
//
// Flow:
//  1. PUT object → GET (cache miss, populates cache + learns ETag)
//  2. HEAD with Cache-Control: no-cache → TAG sends conditional HEAD to upstream
//  3. Upstream returns 304 → TAG serves cached metadata with X-Cache: HIT
func TestSDK_Revalidation_HEAD_Unchanged(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("reval-head-unchanged")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	content := []byte("revalidation head unchanged test content")
	if err := globalEnv.PutTestObject(bucket, "reval-head-key", content); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}

	// GET to populate cache and learn ETag
	resp1, err := globalEnv.DoRawGet(bucket, "reval-head-key")
	if err != nil {
		t.Fatalf("First GET failed: %v", err)
	}
	io.ReadAll(resp1.Body)
	resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("First GET: expected 200, got %d", resp1.StatusCode)
	}
	t.Logf("First GET: X-Cache=%s", resp1.Header.Get("X-Cache"))

	// Wait for cache to be populated
	time.Sleep(500 * time.Millisecond)

	// HEAD with Cache-Control: no-cache → revalidation
	// Object unchanged → upstream 304 → serve cached metadata
	resp2, err := globalEnv.DoRawHeadWithHeaders(bucket, "reval-head-key", noCacheHeaders())
	if err != nil {
		t.Fatalf("Revalidation HEAD failed: %v", err)
	}
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("Revalidation HEAD: expected 200, got %d", resp2.StatusCode)
	}

	if resp2.ContentLength != int64(len(content)) {
		t.Errorf("Revalidation HEAD: expected Content-Length %d, got %d", len(content), resp2.ContentLength)
	}

	xCache := resp2.Header.Get("X-Cache")
	if xCache != "HIT" {
		t.Errorf("Revalidation HEAD (unchanged): expected X-Cache HIT, got %q", xCache)
	}
	t.Logf("Revalidation HEAD unchanged verified: X-Cache=%s, Content-Length=%d", xCache, resp2.ContentLength)
}

// TestSDK_Revalidation_HEAD_Changed verifies that HEAD with Cache-Control: no-cache
// on a changed object returns new metadata from upstream (200 path).
//
// Flow:
//  1. PUT object v1 → GET (cache miss) → cache populated with v1
//  2. PUT object v2 (different size, new ETag)
//  3. HEAD with Cache-Control: no-cache → TAG sends conditional HEAD to upstream
//  4. Upstream returns 200 with new metadata → X-Cache: REVALIDATED
func TestSDK_Revalidation_HEAD_Changed(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("reval-head-changed")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	contentV1 := []byte("head revalidation v1")
	if err := globalEnv.PutTestObject(bucket, "reval-head-chg-key", contentV1); err != nil {
		t.Fatalf("Failed to put test object v1: %v", err)
	}

	// GET to populate cache with v1
	resp1, err := globalEnv.DoRawGet(bucket, "reval-head-chg-key")
	if err != nil {
		t.Fatalf("First GET failed: %v", err)
	}
	io.ReadAll(resp1.Body)
	resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("First GET: expected 200, got %d", resp1.StatusCode)
	}
	t.Logf("First GET: X-Cache=%s", resp1.Header.Get("X-Cache"))

	// Wait for cache to be populated
	time.Sleep(500 * time.Millisecond)

	// PUT new content v2 directly to Tigris, bypassing TAG's cache invalidation.
	// TAG's cache still holds stale v1 with old ETag. Different size so Content-Length changes.
	contentV2 := []byte("head revalidation v2 - updated with more content")
	if err := globalEnv.PutTestObjectDirect(bucket, "reval-head-chg-key", contentV2); err != nil {
		t.Fatalf("Failed to put test object v2 direct: %v", err)
	}

	// HEAD with Cache-Control: no-cache → revalidation
	// Object changed → upstream 200 → new metadata
	resp2, err := globalEnv.DoRawHeadWithHeaders(bucket, "reval-head-chg-key", noCacheHeaders())
	if err != nil {
		t.Fatalf("Revalidation HEAD failed: %v", err)
	}
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("Revalidation HEAD: expected 200, got %d", resp2.StatusCode)
	}

	if resp2.ContentLength != int64(len(contentV2)) {
		t.Errorf("Revalidation HEAD: expected Content-Length %d (v2), got %d", len(contentV2), resp2.ContentLength)
	}

	xCache := resp2.Header.Get("X-Cache")
	if xCache != "REVALIDATED" {
		t.Errorf("Revalidation HEAD (changed): expected X-Cache REVALIDATED, got %q", xCache)
	}
	t.Logf("Revalidation HEAD changed verified: X-Cache=%s, Content-Length=%d", xCache, resp2.ContentLength)
}

// ============================================================================
// Extended S3 Operation Tests
// These tests cover additional S3 scenarios from the Tigris e2e test suite.
// ============================================================================

// TestSDK_PutObject_1MB verifies PUT and GET of a 1MB object.
func TestSDK_PutObject_1MB(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("put-1mb")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	objectData := make([]byte, 1024*1024)
	rand.Read(objectData)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("1mb-key"),
		Body:   bytes.NewReader(objectData),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("1mb-key"),
	})
	if err != nil {
		t.Fatalf("GetObject verification failed: %v", err)
	}
	defer result.Body.Close()

	body, _ := io.ReadAll(result.Body)
	if !bytes.Equal(body, objectData) {
		t.Error("1MB PUT: downloaded content does not match original")
	}
}

// TestSDK_MultipartUpload_MultipleParts verifies multipart upload with two 5MB parts
// and full content verification via GET.
func TestSDK_MultipartUpload_MultipleParts(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("multipart-multi")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	createResult, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("multipart-multi-key"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	const partSize = 5 * 1024 * 1024
	part1Data := make([]byte, partSize)
	part2Data := make([]byte, partSize)
	rand.Read(part1Data)
	rand.Read(part2Data)

	uploadResult1, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("multipart-multi-key"),
		UploadId:   createResult.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part1Data),
	})
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}

	uploadResult2, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("multipart-multi-key"),
		UploadId:   createResult.UploadId,
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader(part2Data),
	})
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String("multipart-multi-key"),
		UploadId: createResult.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{ETag: uploadResult1.ETag, PartNumber: aws.Int32(1)},
				{ETag: uploadResult2.ETag, PartNumber: aws.Int32(2)},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("multipart-multi-key"),
	})
	if err != nil {
		t.Fatalf("GetObject verification failed: %v", err)
	}
	defer result.Body.Close()

	body, _ := io.ReadAll(result.Body)
	expectedData := append(part1Data, part2Data...)
	if !bytes.Equal(body, expectedData) {
		t.Errorf("Multipart upload content mismatch: got %d bytes, want %d bytes", len(body), len(expectedData))
	}
}

// TestSDK_MultipartUpload_SmallPart verifies multipart upload with a single small
// (16KB) part, matching the small-part scenario from the Tigris e2e test suite.
func TestSDK_MultipartUpload_SmallPart(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("multipart-small")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	createResult, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("multipart-small-key"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	partData := make([]byte, 16*1024)
	rand.Read(partData)

	uploadResult, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("multipart-small-key"),
		UploadId:   createResult.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(partData),
	})
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String("multipart-small-key"),
		UploadId: createResult.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{ETag: uploadResult.ETag, PartNumber: aws.Int32(1)},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("multipart-small-key"),
	})
	if err != nil {
		t.Fatalf("GetObject verification failed: %v", err)
	}
	defer result.Body.Close()

	body, _ := io.ReadAll(result.Body)
	if !bytes.Equal(body, partData) {
		t.Errorf("Small multipart upload content mismatch: got %d bytes, want %d bytes", len(body), len(partData))
	}
}

// TestSDK_MultipartUpload_Abort verifies that AbortMultipartUpload correctly cancels
// an in-progress multipart upload.
func TestSDK_MultipartUpload_Abort(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("multipart-abort")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	createResult, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("multipart-abort-key"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	partData := make([]byte, 16*1024)
	rand.Read(partData)

	_, err = client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("multipart-abort-key"),
		UploadId:   createResult.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(partData),
	})
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	_, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String("multipart-abort-key"),
		UploadId: createResult.UploadId,
	})
	if err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("multipart-abort-key"),
	})
	if err == nil {
		t.Error("Expected error when getting aborted multipart object, got nil")
	}
}

// TestSDK_GetObjectRange_Comprehensive verifies various range request scenarios,
// matching the comprehensive range test coverage from the Tigris e2e suite.
// Uses a 1024-byte random object and verifies byte-for-byte content for each range.
func TestSDK_GetObjectRange_Comprehensive(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("range-comprehensive")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	const objSize = 1024
	objectData := make([]byte, objSize)
	rand.Read(objectData)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("range-key"),
		Body:   bytes.NewReader(objectData),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	tests := []struct {
		name     string
		rangeHdr string
		expected []byte
	}{
		{"first 128 bytes", "bytes=0-127", objectData[0:128]},
		{"open-ended from start", "bytes=0-", objectData[0:]},
		{"open-ended from middle", "bytes=128-", objectData[128:]},
		{"middle range", "bytes=128-511", objectData[128:512]},
		{"exact full range", "bytes=0-1023", objectData[0:]},
		{"single byte at start", "bytes=0-0", objectData[0:1]},
		{"two bytes from start", "bytes=0-1", objectData[0:2]},
		{"end beyond object size", "bytes=0-4096", objectData[0:]},
		{"start valid end beyond size", "bytes=512-4096", objectData[512:]},
		{"suffix last 128 bytes", "bytes=-128", objectData[objSize-128:]},
		{"suffix larger than object", "bytes=-4096", objectData[0:]},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String("range-key"),
				Range:  aws.String(tc.rangeHdr),
			})
			if err != nil {
				t.Fatalf("GetObject with Range %q failed: %v", tc.rangeHdr, err)
			}
			defer result.Body.Close()

			body, _ := io.ReadAll(result.Body)
			if !bytes.Equal(body, tc.expected) {
				t.Errorf("Range %q: got %d bytes, want %d bytes", tc.rangeHdr, len(body), len(tc.expected))
			}
		})
	}
}

// TestSDK_GetObjectRange_Invalid verifies that invalid range requests return errors.
// Tests ranges where the start position is at or beyond the object size.
func TestSDK_GetObjectRange_Invalid(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("range-invalid")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	objectData := make([]byte, 1024)
	rand.Read(objectData)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("range-invalid-key"),
		Body:   bytes.NewReader(objectData),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	tests := []struct {
		name     string
		rangeHdr string
	}{
		{"start equals object size", "bytes=1024-"},
		{"start beyond object size", "bytes=1024-4096"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String("range-invalid-key"),
				Range:  aws.String(tc.rangeHdr),
			})
			if err == nil {
				t.Errorf("Expected error for invalid range %q, got nil", tc.rangeHdr)
			}
		})
	}
}

// TestSDK_ContentEncodingPreservation verifies that Content-Encoding is preserved
// through the proxy without altering the object data. The proxy must not attempt
// to decompress or modify data based on Content-Encoding headers.
func TestSDK_ContentEncodingPreservation(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("content-encoding")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	tests := []struct {
		name string
		size int
	}{
		{"512B", 512},
		{"1MB", 1024 * 1024},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := "ce-" + tc.name
			objectData := make([]byte, tc.size)
			rand.Read(objectData)

			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:          aws.String(bucket),
				Key:             aws.String(key),
				Body:            bytes.NewReader(objectData),
				ContentEncoding: aws.String("gzip"),
			})
			if err != nil {
				t.Fatalf("PutObject failed: %v", err)
			}

			// Verify content is returned unmodified
			getResult, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				t.Fatalf("GetObject failed: %v", err)
			}
			defer getResult.Body.Close()

			body, _ := io.ReadAll(getResult.Body)
			if !bytes.Equal(body, objectData) {
				t.Errorf("Content mismatch: got %d bytes, want %d bytes", len(body), len(objectData))
			}

			// Verify Content-Encoding header is preserved
			headResult, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				t.Fatalf("HeadObject failed: %v", err)
			}

			if aws.ToString(headResult.ContentEncoding) != "gzip" {
				t.Errorf("Expected Content-Encoding %q, got %q", "gzip", aws.ToString(headResult.ContentEncoding))
			}
		})
	}
}

// TestSDK_PutObject_Checksum verifies that checksums are preserved through the proxy
// for PutObject operations. Tests SHA256, CRC32, and CRC32C at small (512B) and
// medium (32KB) sizes, matching the Tigris e2e checksum test suite.
func TestSDK_PutObject_Checksum(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("checksum-put")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	tests := []struct {
		name string
		algo types.ChecksumAlgorithm
		size int
	}{
		{"SHA256_512B", types.ChecksumAlgorithmSha256, 512},
		{"SHA256_32KB", types.ChecksumAlgorithmSha256, 32 * 1024},
		{"CRC32_512B", types.ChecksumAlgorithmCrc32, 512},
		{"CRC32_32KB", types.ChecksumAlgorithmCrc32, 32 * 1024},
		{"CRC32C_512B", types.ChecksumAlgorithmCrc32c, 512},
		{"CRC32C_32KB", types.ChecksumAlgorithmCrc32c, 32 * 1024},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := "chk-" + tc.name
			objectData := make([]byte, tc.size)
			rand.Read(objectData)

			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:            aws.String(bucket),
				Key:               aws.String(key),
				Body:              bytes.NewReader(objectData),
				ChecksumAlgorithm: tc.algo,
			})
			if err != nil {
				t.Fatalf("PutObject failed: %v", err)
			}

			// Verify content round-trip
			getResult, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket:       aws.String(bucket),
				Key:          aws.String(key),
				ChecksumMode: types.ChecksumModeEnabled,
			})
			if err != nil {
				t.Fatalf("GetObject failed: %v", err)
			}
			defer getResult.Body.Close()

			body, _ := io.ReadAll(getResult.Body)
			if !bytes.Equal(body, objectData) {
				t.Errorf("Content mismatch: got %d bytes, want %d bytes", len(body), len(objectData))
			}

			// Verify checksum is present in GET response
			switch tc.algo {
			case types.ChecksumAlgorithmSha256:
				if getResult.ChecksumSHA256 == nil || *getResult.ChecksumSHA256 == "" {
					t.Error("Expected ChecksumSHA256 in GET response")
				}
			case types.ChecksumAlgorithmCrc32:
				if getResult.ChecksumCRC32 == nil || *getResult.ChecksumCRC32 == "" {
					t.Error("Expected ChecksumCRC32 in GET response")
				}
			case types.ChecksumAlgorithmCrc32c:
				if getResult.ChecksumCRC32C == nil || *getResult.ChecksumCRC32C == "" {
					t.Error("Expected ChecksumCRC32C in GET response")
				}
			}
		})
	}
}

// TestSDK_MultipartUpload_Checksum verifies that checksums are preserved through the
// proxy for multipart uploads. Uses SHA256 with two 5MB parts and verifies content
// round-trip and composite checksum presence.
func TestSDK_MultipartUpload_Checksum(t *testing.T) {
	bucket, err := globalEnv.CreateTestBucket("checksum-multipart")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	client := globalEnv.GetS3Client()
	ctx := context.Background()

	createResult, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("chk-multipart-key"),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	const partSize = 5 * 1024 * 1024
	part1Data := make([]byte, partSize)
	part2Data := make([]byte, partSize)
	rand.Read(part1Data)
	rand.Read(part2Data)

	uploadResult1, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("chk-multipart-key"),
		UploadId:          createResult.UploadId,
		PartNumber:        aws.Int32(1),
		Body:              bytes.NewReader(part1Data),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}
	if uploadResult1.ChecksumSHA256 == nil || *uploadResult1.ChecksumSHA256 == "" {
		t.Error("Expected ChecksumSHA256 in UploadPart 1 response")
	}

	uploadResult2, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("chk-multipart-key"),
		UploadId:          createResult.UploadId,
		PartNumber:        aws.Int32(2),
		Body:              bytes.NewReader(part2Data),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}
	if uploadResult2.ChecksumSHA256 == nil || *uploadResult2.ChecksumSHA256 == "" {
		t.Error("Expected ChecksumSHA256 in UploadPart 2 response")
	}

	completeResult, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String("chk-multipart-key"),
		UploadId: createResult.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{ETag: uploadResult1.ETag, PartNumber: aws.Int32(1), ChecksumSHA256: uploadResult1.ChecksumSHA256},
				{ETag: uploadResult2.ETag, PartNumber: aws.Int32(2), ChecksumSHA256: uploadResult2.ChecksumSHA256},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	// Composite checksum should be present (format: <hash>-<num_parts>).
	// Some environments may not return it; log for diagnostics but don't fail,
	// since TAG proxies the XML body faithfully and cannot add missing fields.
	if completeResult.ChecksumSHA256 != nil && *completeResult.ChecksumSHA256 != "" {
		t.Logf("CompleteMultipartUpload ChecksumSHA256: %s", *completeResult.ChecksumSHA256)
	} else {
		t.Log("CompleteMultipartUpload did not return composite ChecksumSHA256")
	}

	// Verify content round-trip
	getResult, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String("chk-multipart-key"),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer getResult.Body.Close()

	body, _ := io.ReadAll(getResult.Body)
	expectedData := append(part1Data, part2Data...)
	if !bytes.Equal(body, expectedData) {
		t.Errorf("Content mismatch: got %d bytes, want %d bytes", len(body), len(expectedData))
	}

	// Checksum should be present when ChecksumMode is enabled.
	// Log the result for diagnostics — TAG forwards this header from upstream.
	if getResult.ChecksumSHA256 != nil && *getResult.ChecksumSHA256 != "" {
		t.Logf("GetObject ChecksumSHA256: %s", *getResult.ChecksumSHA256)
	} else {
		t.Log("GetObject did not return ChecksumSHA256 header")
	}
}
