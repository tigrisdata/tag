package sdk

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/tigrisdata/tag/auth"
)

const (
	// DefaultTAGEndpoint is the default TAG endpoint when running via make s3-test-local.
	DefaultTAGEndpoint = "http://localhost:8080"
	// DefaultRegion is the default AWS region for signing.
	DefaultRegion = "auto"
	// BucketPrefixBase is the base prefix for test buckets.
	BucketPrefixBase = "sdk-test-"
)

// TestEnvironment provides utilities for SDK tests against an external TAG instance.
type TestEnvironment struct {
	// Endpoint is the TAG server endpoint.
	Endpoint string
	// AccessKeyID is the AWS access key.
	AccessKeyID string
	// SecretAccessKey is the AWS secret key.
	SecretAccessKey string
	// Region is the AWS region for signing.
	Region string
	// BucketPrefix is the unique prefix for test buckets in this test run.
	BucketPrefix string
	// S3Client is the pre-configured S3 client.
	S3Client *s3.Client

	// buckets tracks created buckets for cleanup.
	buckets []string
	mu      sync.Mutex
}

// NewTestEnvironment creates a new test environment connected to external TAG.
// It reads configuration from environment variables and verifies TAG is running.
func NewTestEnvironment() (*TestEnvironment, error) {
	env := &TestEnvironment{
		Endpoint:        getEnvOrDefault("TAG_ENDPOINT", DefaultTAGEndpoint),
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		Region:          getEnvOrDefault("TAG_REGION", DefaultRegion),
		BucketPrefix:    BucketPrefixBase + randomString(8) + "-",
		buckets:         make([]string, 0),
	}

	// Validate credentials
	if env.AccessKeyID == "" || env.SecretAccessKey == "" {
		return nil, fmt.Errorf("AWS credentials not set: export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
	}

	// Create S3 client
	env.S3Client = s3.NewFromConfig(aws.Config{}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(env.Endpoint)
		o.Region = env.Region
		o.UsePathStyle = true
		o.Credentials = credentials.NewStaticCredentialsProvider(
			env.AccessKeyID, env.SecretAccessKey, "")
	})

	// Verify TAG is running
	if err := env.waitForTAG(5 * time.Second); err != nil {
		return nil, err
	}

	return env, nil
}

// waitForTAG polls the health endpoint until TAG responds or timeout.
func (e *TestEnvironment) waitForTAG(timeout time.Duration) error {
	healthURL := e.Endpoint + "/health"
	deadline := time.Now().Add(timeout)

	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL)
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("health check returned status %d", resp.StatusCode)
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("TAG not responding at %s: %w", healthURL, lastErr)
}

// GetS3Client returns the S3 client configured for TAG.
func (e *TestEnvironment) GetS3Client() *s3.Client {
	return e.S3Client
}

// UniqueBucketName generates a unique bucket name with the test prefix.
func (e *TestEnvironment) UniqueBucketName(suffix string) string {
	return e.BucketPrefix + suffix
}

// CreateTestBucket creates a bucket with a unique name and tracks it for cleanup.
func (e *TestEnvironment) CreateTestBucket(suffix string) (string, error) {
	bucketName := e.UniqueBucketName(suffix)

	_, err := e.S3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
	}

	e.mu.Lock()
	e.buckets = append(e.buckets, bucketName)
	e.mu.Unlock()

	return bucketName, nil
}

// PutTestObject uploads an object to the specified bucket via S3 SDK.
func (e *TestEnvironment) PutTestObject(bucket, key string, data []byte) error {
	_, err := e.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

// PutTestObjectWithContentType uploads an object with a specific content type.
func (e *TestEnvironment) PutTestObjectWithContentType(bucket, key string, data []byte, contentType string) error {
	_, err := e.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	return err
}

// PutTestObjectWithMetadata uploads an object with custom metadata.
func (e *TestEnvironment) PutTestObjectWithMetadata(bucket, key string, data []byte, metadata map[string]string) error {
	_, err := e.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		Body:     bytes.NewReader(data),
		Metadata: metadata,
	})
	return err
}

// DeleteTestObject deletes an object from the specified bucket.
func (e *TestEnvironment) DeleteTestObject(bucket, key string) error {
	_, err := e.S3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err
}

// deleteBucket force-deletes a bucket and all its objects using Tigris-Force-Delete header.
func (e *TestEnvironment) deleteBucket(bucket string) error {
	_, err := e.S3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	}, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, smithyhttp.AddHeaderValue("Tigris-Force-Delete", "true"))
	})
	return err
}

// Cleanup deletes all test buckets created during this test run.
func (e *TestEnvironment) Cleanup() error {
	e.mu.Lock()
	buckets := make([]string, len(e.buckets))
	copy(buckets, e.buckets)
	e.mu.Unlock()

	var errs []error
	for _, bucket := range buckets {
		if err := e.deleteBucket(bucket); err != nil {
			errs = append(errs, fmt.Errorf("failed to delete bucket %s: %w", bucket, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

// Close is an alias for Cleanup to match the old TestEnvironment interface.
func (e *TestEnvironment) Close() error {
	return e.Cleanup()
}

// DoGet performs a GET request and returns the body.
// This is used for cache verification tests.
func (e *TestEnvironment) DoGet(bucket, key string) (body []byte, err error) {
	result, err := e.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()

	body, err = io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// getEnvOrDefault returns the environment variable value or a default.
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// DoRawGet performs a SigV4-signed raw HTTP GET request and returns the full response.
// Unlike DoGet (which uses the AWS SDK), this returns the raw *http.Response so callers
// can inspect response headers like X-Cache and X-Tigris-Proxy-Signing-Keys.
// Caller is responsible for closing the response body.
func (e *TestEnvironment) DoRawGet(bucket, key string) (*http.Response, error) {
	signer := auth.NewRequestSigner(e.Endpoint, e.Region)
	path := "/" + bucket + "/" + key
	req, err := signer.SignRequest(context.Background(), "GET", path, nil, "", e.AccessKeyID, e.SecretAccessKey, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to sign raw GET request: %w", err)
	}
	return http.DefaultClient.Do(req)
}

// randomString generates a random alphanumeric string of the specified length.
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		// Fallback to timestamp-based if crypto/rand fails
		return fmt.Sprintf("%d", time.Now().UnixNano())[:length]
	}
	for i := range b {
		b[i] = charset[int(b[i])%len(charset)]
	}
	return string(b)
}
