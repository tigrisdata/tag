package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ============================================================================
// TLS/Chunked Encoding Tests
// These tests require httptest.NewTLSServer because the AWS Go SDK v2 only uses
// aws-chunked encoding with trailing checksums over HTTPS connections.
// They must remain in this package to use the TestEnvironment with TLS support.
// ============================================================================

// TestSDK_PutObject_ChunkedEncoding verifies that AWS chunked transfer encoding
// is correctly decoded by TAG. Over TLS, the AWS Go SDK v2 automatically uses
// aws-chunked encoding with trailing checksums (STREAMING-UNSIGNED-PAYLOAD-TRAILER).
func TestSDK_PutObject_ChunkedEncoding(t *testing.T) {
	env := NewTestEnvironmentWithTLS()
	defer env.Close()

	env.CreateTestBucket("test-bucket")

	client := env.GetS3ClientTLS()
	ctx := context.Background()

	objectData := []byte("hello from chunked encoding test!")
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("chunked-key"),
		Body:   bytes.NewReader(objectData),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify object was stored correctly via GET
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("chunked-key"),
	})
	if err != nil {
		t.Fatalf("GetObject verification failed: %v", err)
	}
	defer result.Body.Close()

	body, _ := io.ReadAll(result.Body)
	if !bytes.Equal(body, objectData) {
		t.Errorf("GET body = %q, want %q", body, objectData)
	}
}

// TestSDK_PutObject_ChunkedEncoding_LargeObject verifies chunked encoding with a
// larger object that spans multiple 64KB chunks.
func TestSDK_PutObject_ChunkedEncoding_LargeObject(t *testing.T) {
	env := NewTestEnvironmentWithTLS()
	defer env.Close()

	env.CreateTestBucket("test-bucket")

	client := env.GetS3ClientTLS()
	ctx := context.Background()

	// 128KB object — the SDK will split this into multiple 64KB aws-chunked chunks
	objectData := make([]byte, 128*1024)
	rand.Read(objectData)

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("chunked-large-key"),
		Body:   bytes.NewReader(objectData),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify via GET
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("chunked-large-key"),
	})
	if err != nil {
		t.Fatalf("GetObject verification failed: %v", err)
	}
	defer result.Body.Close()

	body, _ := io.ReadAll(result.Body)
	if !bytes.Equal(body, objectData) {
		t.Error("Large chunked PUT: downloaded content does not match original")
	}

	t.Logf("Chunked encoding verified: %d bytes uploaded via SDK over TLS, content matches", len(objectData))
}

// TestSDK_PutObject_ChunkedEncoding_ZeroByte verifies that zero-byte PutObject
// works correctly over TLS (where the SDK uses aws-chunked encoding).
func TestSDK_PutObject_ChunkedEncoding_ZeroByte(t *testing.T) {
	env := NewTestEnvironmentWithTLS()
	defer env.Close()

	env.CreateTestBucket("test-bucket")

	client := env.GetS3ClientTLS()
	ctx := context.Background()

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("chunked-empty-key"),
		Body:   bytes.NewReader([]byte{}),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify via GET
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("chunked-empty-key"),
	})
	if err != nil {
		t.Fatalf("GetObject verification failed: %v", err)
	}
	defer result.Body.Close()

	body, _ := io.ReadAll(result.Body)
	if len(body) != 0 {
		t.Errorf("Expected empty body, got %d bytes", len(body))
	}
}
