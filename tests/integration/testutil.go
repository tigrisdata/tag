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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/handlers"
	"github.com/tigrisdata/tag/proxy"
)

const (
	// TestAccessKey is the access key used for testing.
	TestAccessKey = "AKIAIOSFODNN7EXAMPLE"
	// TestSecretKey is the secret key used for testing.
	TestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	// TestRegion is the region used for testing.
	TestRegion = "us-east-1"
)

// TestEnvironment holds all components needed for integration testing.
type TestEnvironment struct {
	// UpstreamServer is the mock upstream Tigris server.
	UpstreamServer *httptest.Server
	// TAGServer is the TAG proxy server.
	TAGServer *httptest.Server
	// CredStore is the credential store with test credentials.
	CredStore *auth.CredentialStore
	// Cache is the cache instance (disabled for most tests).
	Cache *cache.Cache
	// Config is the test configuration.
	Config *config.Config
	// Service is the proxy service.
	Service *proxy.Service
	// Signer is the request signer for creating signed requests.
	Signer *auth.RequestSigner
	// S3Backend is the in-memory S3 backend (nil if using custom handler).
	S3Backend *s3mem.Backend
	// MemoryCache is the in-memory cache client for cache-enabled tests (nil for non-cache tests).
	MemoryCache *cacheclient.MemoryCache
	// UpstreamRequestCount tracks the number of requests made to upstream (for cache tests).
	UpstreamRequestCount *int32
}

// NewTestEnvironment creates a new test environment with gofakes3 in-memory backend.
func NewTestEnvironment() *TestEnvironment {
	// Create in-memory S3 backend
	backend := s3mem.New()
	faker := gofakes3.New(backend)

	// Start fake S3 server
	upstream := httptest.NewServer(faker.Server())

	return newTestEnvironmentWithUpstream(upstream, backend)
}

// NewTestEnvironmentWithMiddleware creates a test environment with middleware wrapping gofakes3.
// This is useful for request counting or error injection tests.
func NewTestEnvironmentWithMiddleware(middleware func(http.Handler) http.Handler) *TestEnvironment {
	// Create in-memory S3 backend
	backend := s3mem.New()
	faker := gofakes3.New(backend)

	// Wrap with middleware
	handler := middleware(faker.Server())
	upstream := httptest.NewServer(handler)

	return newTestEnvironmentWithUpstream(upstream, backend)
}

// NewTestEnvironmentWithHandler creates a test environment with a custom HTTP handler.
// This is useful for auth tests that don't need a real S3 backend.
func NewTestEnvironmentWithHandler(upstreamHandler http.HandlerFunc) *TestEnvironment {
	upstream := httptest.NewServer(upstreamHandler)
	return newTestEnvironmentWithUpstream(upstream, nil)
}

// NewTestEnvironmentWithCache creates a test environment with caching enabled using an in-memory cache.
// This uses cacheclient.NewMemoryCache() from ocache for integration testing cache behavior.
func NewTestEnvironmentWithCache() *TestEnvironment {
	// Create in-memory S3 backend
	backend := s3mem.New()
	faker := gofakes3.New(backend)

	// Track upstream requests to verify cache behavior
	var requestCount int32
	counter := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requestCount, 1)
			next.ServeHTTP(w, r)
		})
	}

	// Wrap with request counter
	upstream := httptest.NewServer(counter(faker.Server()))

	// Create credential store with test credentials
	credStore := auth.NewCredentialStore()
	credStore.AddCredential(TestAccessKey, TestSecretKey)

	// Create config with cache ENABLED
	cfg := &config.Config{
		Server: config.ServerConfig{
			HTTPPort: 8080,
			BindIP:   "127.0.0.1",
		},
		Upstream: config.UpstreamConfig{
			Endpoint: upstream.URL,
			Region:   TestRegion,
		},
		Cache: config.CacheConfig{
			Enabled:       true, // Cache enabled for these tests
			TTL:           config.DefaultCacheTTL,
			SizeThreshold: config.DefaultCacheSizeThreshold,
		},
	}

	// Create in-memory cache using ocache's MemoryCache
	memoryCache := cacheclient.NewMemoryCache()
	testCache := cache.NewCacheWithClient(memoryCache, &cfg.Cache)

	// Create forwarder
	forwarder := proxy.NewForwarder(credStore, cfg.Upstream.Endpoint, cfg.Upstream.Region)

	// Create service
	service := proxy.NewService(forwarder, testCache, cfg)

	// Create TAG server
	server := handlers.NewServer(service, "127.0.0.1", 0)
	tagServer := httptest.NewServer(server.Router())

	// Create signer for test requests (pointing to TAG server)
	signer := auth.NewRequestSigner(tagServer.URL, TestRegion)

	return &TestEnvironment{
		UpstreamServer:       upstream,
		TAGServer:            tagServer,
		CredStore:            credStore,
		Cache:                testCache,
		Config:               cfg,
		Service:              service,
		Signer:               signer,
		S3Backend:            backend,
		MemoryCache:          memoryCache,
		UpstreamRequestCount: &requestCount,
	}
}

// newTestEnvironmentWithUpstream creates a test environment with the given upstream server.
func newTestEnvironmentWithUpstream(upstream *httptest.Server, backend *s3mem.Backend) *TestEnvironment {
	// Create credential store with test credentials
	credStore := auth.NewCredentialStore()
	credStore.AddCredential(TestAccessKey, TestSecretKey)

	// Create config with cache disabled
	cfg := &config.Config{
		Server: config.ServerConfig{
			HTTPPort: 8080,
			BindIP:   "127.0.0.1",
		},
		Upstream: config.UpstreamConfig{
			Endpoint: upstream.URL,
			Region:   TestRegion,
		},
		Cache: config.CacheConfig{
			Enabled:       false, // Cache disabled for integration tests
			TTL:           config.DefaultCacheTTL,
			SizeThreshold: config.DefaultCacheSizeThreshold,
		},
	}

	// Create disabled cache
	testCache, _ := cache.NewCache(&cfg.Cache)

	// Create forwarder
	forwarder := proxy.NewForwarder(credStore, cfg.Upstream.Endpoint, cfg.Upstream.Region)

	// Create service
	service := proxy.NewService(forwarder, testCache, cfg)

	// Create TAG server
	server := handlers.NewServer(service, "127.0.0.1", 0)
	tagServer := httptest.NewServer(server.Router())

	// Create signer for test requests (pointing to TAG server)
	signer := auth.NewRequestSigner(tagServer.URL, TestRegion)

	return &TestEnvironment{
		UpstreamServer: upstream,
		TAGServer:      tagServer,
		CredStore:      credStore,
		Cache:          testCache,
		Config:         cfg,
		Service:        service,
		Signer:         signer,
		S3Backend:      backend,
	}
}

// Close cleans up the test environment.
func (e *TestEnvironment) Close() {
	if e.TAGServer != nil {
		e.TAGServer.Close()
	}
	if e.UpstreamServer != nil {
		e.UpstreamServer.Close()
	}
	if e.Cache != nil {
		e.Cache.Close()
	}
}

// PutTestObject creates a bucket (if needed) and puts an object in the backend.
// This is used to pre-populate test data.
func (e *TestEnvironment) PutTestObject(bucket, key string, data []byte) error {
	if e.S3Backend == nil {
		return nil // No backend to populate
	}

	// Create bucket (ignore error if already exists)
	_ = e.S3Backend.CreateBucket(bucket)

	// Put object (nil conditions means no preconditions)
	_, err := e.S3Backend.PutObject(bucket, key, map[string]string{}, bytes.NewReader(data), int64(len(data)), nil)
	return err
}

// PutTestObjectWithMetadata creates an object with custom metadata.
func (e *TestEnvironment) PutTestObjectWithMetadata(bucket, key string, data []byte, metadata map[string]string) error {
	if e.S3Backend == nil {
		return nil
	}

	_ = e.S3Backend.CreateBucket(bucket)
	_, err := e.S3Backend.PutObject(bucket, key, metadata, bytes.NewReader(data), int64(len(data)), nil)
	return err
}

// CreateTestBucket creates a bucket in the backend.
func (e *TestEnvironment) CreateTestBucket(bucket string) error {
	if e.S3Backend == nil {
		return nil
	}

	return e.S3Backend.CreateBucket(bucket)
}

// GetS3Client returns an AWS SDK S3 client configured to use the TAG server.
func (e *TestEnvironment) GetS3Client() *s3.Client {
	return s3.NewFromConfig(aws.Config{}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(e.TAGServer.URL)
		o.Region = TestRegion
		o.EndpointOptions.DisableHTTPS = true
		o.UsePathStyle = true
		o.Credentials = credentials.NewStaticCredentialsProvider(
			TestAccessKey, TestSecretKey, "")
	})
}

// GetS3ClientWithCreds returns an AWS SDK S3 client with custom credentials.
func (e *TestEnvironment) GetS3ClientWithCreds(accessKey, secretKey string) *s3.Client {
	return s3.NewFromConfig(aws.Config{}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(e.TAGServer.URL)
		o.Region = TestRegion
		o.EndpointOptions.DisableHTTPS = true
		o.UsePathStyle = true
		o.Credentials = credentials.NewStaticCredentialsProvider(
			accessKey, secretKey, "")
	})
}

// SignedRequest creates a signed HTTP request for testing.
func (e *TestEnvironment) SignedRequest(method, path string, body []byte) (*http.Request, error) {
	var bodyReader io.Reader
	var bodyHash string

	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
		h := sha256.Sum256(body)
		bodyHash = hex.EncodeToString(h[:])
	}

	req, err := e.Signer.SignRequest(
		context.Background(),
		method,
		path,
		bodyReader,
		bodyHash,
		TestAccessKey,
		TestSecretKey,
		http.Header{},
	)
	if err != nil {
		return nil, err
	}

	return req, nil
}

// DoSignedRequest creates and executes a signed HTTP request.
func (e *TestEnvironment) DoSignedRequest(method, path string, body []byte) (*http.Response, error) {
	req, err := e.SignedRequest(method, path, body)
	if err != nil {
		return nil, err
	}

	return http.DefaultClient.Do(req)
}

// UnsignedRequest creates an unsigned HTTP request for testing auth failures.
func (e *TestEnvironment) UnsignedRequest(method, path string, body []byte) *http.Request {
	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}

	req := httptest.NewRequest(method, e.TAGServer.URL+path, bodyReader)
	return req
}

// RequestWithInvalidSignature creates a request with a tampered signature.
// Uses current time to avoid "request expired" rejection.
func (e *TestEnvironment) RequestWithInvalidSignature(method, path string) *http.Request {
	now := time.Now().UTC()
	dateStr := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")

	req := httptest.NewRequest(method, e.TAGServer.URL+path, nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+TestAccessKey+"/"+dateStr+"/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=invalidsignature")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	return req
}

// RequestWithExpiredTimestamp creates a request with an old timestamp that should be rejected.
func (e *TestEnvironment) RequestWithExpiredTimestamp(method, path string) *http.Request {
	req := httptest.NewRequest(method, e.TAGServer.URL+path, nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+TestAccessKey+"/20230101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=anysignature")
	req.Header.Set("X-Amz-Date", "20230101T000000Z")
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	return req
}

// RequestWithUnknownAccessKey creates a request with an unknown access key.
func (e *TestEnvironment) RequestWithUnknownAccessKey(method, path string) *http.Request {
	req := httptest.NewRequest(method, e.TAGServer.URL+path, nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=UNKNOWNACCESSKEY/20230101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=test")
	req.Header.Set("X-Amz-Date", "20230101T000000Z")
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	return req
}

// RequestCounter returns middleware that counts requests to the upstream.
func RequestCounter(count *int32) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(count, 1)
			next.ServeHTTP(w, r)
		})
	}
}

// GetUpstreamRequestCount returns the number of requests made to the upstream server.
// This is useful for cache tests to verify cache hits (count shouldn't increase) vs cache misses.
func (e *TestEnvironment) GetUpstreamRequestCount() int32 {
	if e.UpstreamRequestCount == nil {
		return 0
	}
	return atomic.LoadInt32(e.UpstreamRequestCount)
}

// ResetUpstreamRequestCount resets the upstream request counter to zero.
func (e *TestEnvironment) ResetUpstreamRequestCount() {
	if e.UpstreamRequestCount != nil {
		atomic.StoreInt32(e.UpstreamRequestCount, 0)
	}
}

// GetCachedObject reads an object directly from the cache for test verification.
// Returns (body, found). If not found or cache is disabled, returns (nil, false).
func (e *TestEnvironment) GetCachedObject(bucket, key string) ([]byte, bool) {
	if e.MemoryCache == nil {
		return nil, false
	}

	// Use the same key format as the cache package
	bodyKey := "body|" + bucket + "|" + key
	// Body is stored via PutStream, so we need to use GetStream to retrieve it
	var buf bytes.Buffer
	err := e.MemoryCache.GetStream(context.Background(), bodyKey, &buf)
	if err != nil {
		return nil, false
	}
	return buf.Bytes(), true
}

// GetCachedMeta reads object metadata directly from the cache for test verification.
// Returns (metadata bytes, found). If not found or cache is disabled, returns (nil, false).
func (e *TestEnvironment) GetCachedMeta(bucket, key string) ([]byte, bool) {
	if e.MemoryCache == nil {
		return nil, false
	}

	// Use the same key format as the cache package
	metaKey := "meta|" + bucket + "|" + key
	meta, err := e.MemoryCache.Get(context.Background(), metaKey)
	if err != nil || meta == nil {
		return nil, false
	}
	return meta, true
}

// IsCached checks if an object exists in the cache.
func (e *TestEnvironment) IsCached(bucket, key string) bool {
	_, found := e.GetCachedMeta(bucket, key)
	return found
}
