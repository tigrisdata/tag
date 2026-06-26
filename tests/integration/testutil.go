package integration

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/tigrisdata/ocache/embedded"
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
	// TestProxyAccessKey is TAG's own Tigris access key for transparent proxy mode.
	TestProxyAccessKey = "PROXY_ACCESS_KEY_EXAMPLE"
	// TestProxySecretKey is TAG's own Tigris secret key for transparent proxy mode.
	TestProxySecretKey = "proxy-secret-key-for-testing"
)

// sharedEmbeddedCache is a singleton embedded cache instance shared across all cache tests.
// This avoids Prometheus metrics registration conflicts between tests.
var sharedEmbeddedCache *embedded.Client

// sharedCacheTempDir is the temp directory for the shared embedded cache.
var sharedCacheTempDir string

// XCacheStatus represents the possible X-Cache header values.
type XCacheStatus string

const (
	// XCacheHit indicates the response was served from cache.
	XCacheHit XCacheStatus = "HIT"
	// XCacheMiss indicates the response was fetched from upstream (cache miss).
	XCacheMiss XCacheStatus = "MISS"
	// XCacheDisabled indicates caching is disabled.
	XCacheDisabled XCacheStatus = "DISABLED"
	// XCacheUnknown indicates no X-Cache header was present.
	XCacheUnknown XCacheStatus = ""
)

// XCacheTracker tracks X-Cache header values from TAG server responses.
// It is thread-safe and can track both the last response and per-key values.
type XCacheTracker struct {
	mu          sync.RWMutex
	lastStatus  XCacheStatus
	lastBucket  string
	lastKey     string
	perKeyCache map[string]XCacheStatus // key format: "bucket/key"
}

// NewXCacheTracker creates a new X-Cache tracker.
func NewXCacheTracker() *XCacheTracker {
	return &XCacheTracker{
		perKeyCache: make(map[string]XCacheStatus),
	}
}

// Record records an X-Cache header value from a response.
func (t *XCacheTracker) Record(bucket, key string, status XCacheStatus) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.lastStatus = status
	t.lastBucket = bucket
	t.lastKey = key

	if bucket != "" && key != "" {
		cacheKey := bucket + "/" + key
		t.perKeyCache[cacheKey] = status
	}
}

// GetLast returns the most recent X-Cache status.
func (t *XCacheTracker) GetLast() XCacheStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastStatus
}

// Get returns the X-Cache status for a specific bucket/key.
func (t *XCacheTracker) Get(bucket, key string) XCacheStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	cacheKey := bucket + "/" + key
	return t.perKeyCache[cacheKey]
}

// Reset clears all tracked values.
func (t *XCacheTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastStatus = XCacheUnknown
	t.lastBucket = ""
	t.lastKey = ""
	t.perKeyCache = make(map[string]XCacheStatus)
}

// xCacheResponseWriter wraps http.ResponseWriter to capture the X-Cache header.
type xCacheResponseWriter struct {
	http.ResponseWriter
	tracker     *XCacheTracker
	bucket      string
	key         string
	wroteHeader bool
}

func (w *xCacheResponseWriter) WriteHeader(statusCode int) {
	if !w.wroteHeader {
		w.wroteHeader = true
		// Capture X-Cache header before writing
		xCache := w.Header().Get("X-Cache")
		if xCache != "" {
			w.tracker.Record(w.bucket, w.key, XCacheStatus(xCache))
		}
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *xCacheResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// Flush implements http.Flusher if underlying writer supports it.
func (w *xCacheResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// xCacheTrackingMiddleware returns middleware that captures X-Cache headers from responses.
func xCacheTrackingMiddleware(tracker *XCacheTracker) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Parse bucket/key from path for tracking
			bucket, key := parseBucketKeyFromPath(r.URL.Path)

			// Wrap response writer to capture X-Cache
			wrapper := &xCacheResponseWriter{
				ResponseWriter: w,
				tracker:        tracker,
				bucket:         bucket,
				key:            key,
			}

			// Call the actual handler
			next.ServeHTTP(wrapper, r)
		})
	}
}

// parseBucketKeyFromPath extracts bucket and key from URL path.
// Path format: /{bucket}/{key} or /{bucket}
func parseBucketKeyFromPath(path string) (bucket, key string) {
	path = strings.TrimPrefix(path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) >= 1 {
		bucket = parts[0]
	}
	if len(parts) >= 2 {
		key = parts[1]
	}
	return
}

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
	// EmbeddedCache is the embedded RocksDB cache for cache-enabled tests (nil for non-cache tests).
	EmbeddedCache *embedded.Client
	// UpstreamRequestCount tracks the number of requests made to upstream (for coalescing tests).
	UpstreamRequestCount *int32
	// XCacheTracker tracks X-Cache headers from TAG server responses.
	XCacheTracker *XCacheTracker
	// TLSHTTPClient is the HTTP client configured to trust the TLS test server certificate.
	// Non-nil only for TLS test environments.
	TLSHTTPClient *http.Client
	// DerivedKeyStore is the derived key store for transparent auth tests (nil otherwise).
	DerivedKeyStore *auth.DerivedKeyStore
	// AuthzCache is the authorization cache for transparent auth tests (nil otherwise).
	AuthzCache *auth.AuthzCache
}

// NewTestEnvironment creates a new test environment with gofakes3 in-memory backend.
func NewTestEnvironment() *TestEnvironment {
	// Create in-memory S3 backend
	backend := s3mem.New()
	faker := gofakes3.New(backend)

	// Start fake S3 server
	upstream := httptest.NewServer(faker.Server())

	return newTestEnvironmentWithUpstream(upstream, backend, false)
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

	return newTestEnvironmentWithUpstream(upstream, backend, false)
}

// NewTestEnvironmentWithHandler creates a test environment with a custom HTTP handler.
// This is useful for auth tests that don't need a real S3 backend.
func NewTestEnvironmentWithHandler(upstreamHandler http.HandlerFunc) *TestEnvironment {
	upstream := httptest.NewServer(upstreamHandler)
	return newTestEnvironmentWithUpstream(upstream, nil, false)
}

// NewTestEnvironmentWithTLS creates a test environment with a TLS-enabled TAG server.
// Over HTTPS, the AWS Go SDK v2 uses aws-chunked encoding with trailing checksums,
// which exercises TAG's chunked decoding path.
func NewTestEnvironmentWithTLS() *TestEnvironment {
	// Create in-memory S3 backend
	backend := s3mem.New()
	faker := gofakes3.New(backend)

	// Start fake S3 server (plain HTTP is fine for upstream)
	upstream := httptest.NewServer(faker.Server())

	return newTestEnvironmentWithUpstream(upstream, backend, true)
}

// NewTestEnvironmentWithCache creates a test environment with caching enabled using embedded RocksDB cache.
// This uses a shared embedded cache instance (initialized in TestMain) for realistic integration testing.
// The shared cache avoids Prometheus metrics registration conflicts between tests.
func NewTestEnvironmentWithCache() *TestEnvironment {
	if sharedEmbeddedCache == nil {
		panic("Shared embedded cache not initialized - TestMain must run first")
	}

	// Create in-memory S3 backend
	backend := s3mem.New()
	faker := gofakes3.New(backend)

	// Track upstream requests for coalescing tests
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
			TTL:           config.DefaultCacheTTL,
			SizeThreshold: config.DefaultCacheSizeThreshold,
			// Enabled defaults to true via IsEnabled() when nil
		},
	}

	// Wrap shared embedded cache with cache.Cache interface
	testCache := cache.NewCacheWithClient(sharedEmbeddedCache, &cfg.Cache)

	// Create forwarder
	forwarder := proxy.NewForwarder(credStore, cfg.Upstream.Endpoint, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost, nil, nil)

	// Create service
	service := proxy.NewService(forwarder, testCache, cfg)

	// Create X-Cache tracker
	xCacheTracker := NewXCacheTracker()

	// Create TAG server with X-Cache tracking middleware
	server := handlers.NewServer(service, "127.0.0.1", 0, true)
	wrappedRouter := xCacheTrackingMiddleware(xCacheTracker)(server.Router())
	tagServer := httptest.NewServer(wrappedRouter)

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
		EmbeddedCache:        sharedEmbeddedCache,
		UpstreamRequestCount: &requestCount,
		XCacheTracker:        xCacheTracker,
	}
}

// NewTestEnvironmentWithCacheHandler creates a cache-enabled test environment whose
// upstream is a custom HTTP handler (instead of gofakes3). This is useful for tests
// that need precise control over the upstream response — e.g. blocking a range
// response mid-stream to simulate a client cancel. Uses the shared embedded cache.
func NewTestEnvironmentWithCacheHandler(upstreamHandler http.HandlerFunc) *TestEnvironment {
	if sharedEmbeddedCache == nil {
		panic("Shared embedded cache not initialized - TestMain must run first")
	}

	// Track upstream requests
	var requestCount int32
	counter := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requestCount, 1)
			next.ServeHTTP(w, r)
		})
	}

	upstream := httptest.NewServer(counter(upstreamHandler))

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
			TTL:           config.DefaultCacheTTL,
			SizeThreshold: config.DefaultCacheSizeThreshold,
		},
	}

	testCache := cache.NewCacheWithClient(sharedEmbeddedCache, &cfg.Cache)

	forwarder := proxy.NewForwarder(credStore, cfg.Upstream.Endpoint, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost, nil, nil)
	service := proxy.NewService(forwarder, testCache, cfg)

	xCacheTracker := NewXCacheTracker()
	server := handlers.NewServer(service, "127.0.0.1", 0, true)
	wrappedRouter := xCacheTrackingMiddleware(xCacheTracker)(server.Router())
	tagServer := httptest.NewServer(wrappedRouter)

	signer := auth.NewRequestSigner(tagServer.URL, TestRegion)

	return &TestEnvironment{
		UpstreamServer:       upstream,
		TAGServer:            tagServer,
		CredStore:            credStore,
		Cache:                testCache,
		Config:               cfg,
		Service:              service,
		Signer:               signer,
		EmbeddedCache:        sharedEmbeddedCache,
		UpstreamRequestCount: &requestCount,
		XCacheTracker:        xCacheTracker,
	}
}

// newTestEnvironmentWithUpstream creates a test environment with the given upstream server.
// If useTLS is true, the TAG server uses HTTPS (required for SDK chunked encoding tests).
func newTestEnvironmentWithUpstream(upstream *httptest.Server, backend *s3mem.Backend, useTLS bool) *TestEnvironment {
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
			TTL:           config.DefaultCacheTTL,
			SizeThreshold: config.DefaultCacheSizeThreshold,
		},
	}
	cfg.Cache.SetEnabled(false) // Cache disabled for integration tests

	// Create disabled cache
	testCache := cache.NewDisabledCache()

	// Create forwarder
	forwarder := proxy.NewForwarder(credStore, cfg.Upstream.Endpoint, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost, nil, nil)

	// Create service
	service := proxy.NewService(forwarder, testCache, cfg)

	// Create X-Cache tracker
	xCacheTracker := NewXCacheTracker()

	// Create TAG server with X-Cache tracking middleware
	server := handlers.NewServer(service, "127.0.0.1", 0, true)
	wrappedRouter := xCacheTrackingMiddleware(xCacheTracker)(server.Router())

	var tagServer *httptest.Server
	if useTLS {
		tagServer = httptest.NewTLSServer(wrappedRouter)
	} else {
		tagServer = httptest.NewServer(wrappedRouter)
	}

	// Create signer for test requests (pointing to TAG server)
	signer := auth.NewRequestSigner(tagServer.URL, TestRegion)

	env := &TestEnvironment{
		UpstreamServer: upstream,
		TAGServer:      tagServer,
		CredStore:      credStore,
		Cache:          testCache,
		Config:         cfg,
		Service:        service,
		Signer:         signer,
		S3Backend:      backend,
		XCacheTracker:  xCacheTracker,
	}

	if useTLS {
		env.TLSHTTPClient = tagServer.Client()
	}

	return env
}

// Close cleans up the test environment.
func (e *TestEnvironment) Close() {
	if e.TAGServer != nil {
		e.TAGServer.Close()
	}
	if e.UpstreamServer != nil {
		e.UpstreamServer.Close()
	}
	// Only close cache if it's NOT using the shared embedded cache
	// The shared cache is managed by TestMain
	if e.Cache != nil && e.EmbeddedCache != sharedEmbeddedCache {
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

// GetS3ClientTLS returns an AWS SDK S3 client configured for a TLS TAG server.
// The client uses the test server's HTTP client which trusts the test certificate.
// Unlike GetS3Client, this does NOT disable HTTPS, allowing the SDK to use
// aws-chunked encoding with trailing checksums.
func (e *TestEnvironment) GetS3ClientTLS() *s3.Client {
	return s3.NewFromConfig(aws.Config{
		HTTPClient: e.TLSHTTPClient,
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(e.TAGServer.URL)
		o.Region = TestRegion
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
	if e.EmbeddedCache == nil {
		return nil, false
	}

	// Use the same key format as the cache package
	bodyKey := "body|" + bucket + "|" + key
	// Body is stored via PutStream, so we need to use GetStream to retrieve it
	var buf bytes.Buffer
	err := e.EmbeddedCache.GetStream(context.Background(), bodyKey, &buf)
	if err != nil {
		return nil, false
	}
	return buf.Bytes(), true
}

// GetCachedMeta reads object metadata directly from the cache for test verification.
// Returns (metadata bytes, found). If not found or cache is disabled, returns (nil, false).
func (e *TestEnvironment) GetCachedMeta(bucket, key string) ([]byte, bool) {
	if e.EmbeddedCache == nil {
		return nil, false
	}

	// Use the same key format as the cache package
	metaKey := "meta|" + bucket + "|" + key
	meta, err := e.EmbeddedCache.Get(context.Background(), metaKey)
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

// GetLastXCacheStatus returns the X-Cache status from the most recent response.
func (e *TestEnvironment) GetLastXCacheStatus() XCacheStatus {
	if e.XCacheTracker == nil {
		return XCacheUnknown
	}
	return e.XCacheTracker.GetLast()
}

// GetXCacheStatus returns the X-Cache status for a specific bucket/key.
func (e *TestEnvironment) GetXCacheStatus(bucket, key string) XCacheStatus {
	if e.XCacheTracker == nil {
		return XCacheUnknown
	}
	return e.XCacheTracker.Get(bucket, key)
}

// ResetXCacheTracker clears the X-Cache tracking history.
func (e *TestEnvironment) ResetXCacheTracker() {
	if e.XCacheTracker != nil {
		e.XCacheTracker.Reset()
	}
}

// AssertXCacheHit asserts the last request was a cache hit.
func (e *TestEnvironment) AssertXCacheHit(t *testing.T) {
	t.Helper()
	status := e.GetLastXCacheStatus()
	if status != XCacheHit {
		t.Errorf("Expected X-Cache: HIT, got: %s", status)
	}
}

// AssertXCacheMiss asserts the last request was a cache miss.
func (e *TestEnvironment) AssertXCacheMiss(t *testing.T) {
	t.Helper()
	status := e.GetLastXCacheStatus()
	if status != XCacheMiss {
		t.Errorf("Expected X-Cache: MISS, got: %s", status)
	}
}

// AssertXCacheDisabled asserts the last request had cache disabled.
func (e *TestEnvironment) AssertXCacheDisabled(t *testing.T) {
	t.Helper()
	status := e.GetLastXCacheStatus()
	if status != XCacheDisabled {
		t.Errorf("Expected X-Cache: DISABLED, got: %s", status)
	}
}

// AssertXCacheStatusFor asserts X-Cache status for a specific bucket/key.
func (e *TestEnvironment) AssertXCacheStatusFor(t *testing.T, bucket, key string, expected XCacheStatus) {
	t.Helper()
	status := e.GetXCacheStatus(bucket, key)
	if status != expected {
		t.Errorf("Expected X-Cache: %s for %s/%s, got: %s", expected, bucket, key, status)
	}
}

// waitForCondition polls until condition returns true or timeout expires.
// Returns true if condition became true, false if timeout expired.
func waitForCondition(condition func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// WaitForCached waits for an object to appear in cache with polling and timeout.
// Returns true if the object is cached within the timeout, false otherwise.
func (e *TestEnvironment) WaitForCached(bucket, key string, timeout time.Duration) bool {
	return waitForCondition(func() bool { return e.IsCached(bucket, key) }, timeout)
}

// WaitForNotCached waits for an object to be removed from cache with polling and timeout.
// Returns true if the object is NOT in cache within the timeout, false otherwise.
func (e *TestEnvironment) WaitForNotCached(bucket, key string, timeout time.Duration) bool {
	return waitForCondition(func() bool { return !e.IsCached(bucket, key) }, timeout)
}

// setupSharedCache initializes the shared embedded cache instance.
func setupSharedCache() error {
	// Create temp directory for embedded cache
	tempDir, err := os.MkdirTemp("", "tag-test-cache-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir for cache: %w", err)
	}
	sharedCacheTempDir = tempDir

	// Get free ports for embedded cache cluster and gRPC
	clusterPort, err := getFreePortForMain()
	if err != nil {
		os.RemoveAll(tempDir)
		return fmt.Errorf("failed to get free port for cluster: %w", err)
	}
	grpcPort, err := getFreePortForMain()
	if err != nil {
		os.RemoveAll(tempDir)
		return fmt.Errorf("failed to get free port for gRPC: %w", err)
	}

	// Initialize embedded cache
	embeddedCache, err := embedded.New(&embedded.Config{
		DiskPath:    tempDir,
		TTL:         config.DefaultCacheTTL,
		NodeID:      "test-node",
		ClusterAddr: fmt.Sprintf(":%d", clusterPort),
		GRPCAddr:    fmt.Sprintf(":%d", grpcPort),
	})
	if err != nil {
		os.RemoveAll(tempDir)
		return fmt.Errorf("failed to initialize embedded cache: %w", err)
	}

	// Start gRPC server
	if err := embeddedCache.StartGRPCServer(); err != nil {
		embeddedCache.Close()
		os.RemoveAll(tempDir)
		return fmt.Errorf("failed to start embedded cache gRPC: %w", err)
	}

	// Wait for ready
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := embeddedCache.WaitReady(ctx); err != nil {
		cancel()
		// Continue anyway - single node should be ready
	}
	cancel()

	sharedEmbeddedCache = embeddedCache
	return nil
}

// teardownSharedCache cleans up the shared embedded cache.
func teardownSharedCache() {
	if sharedEmbeddedCache != nil {
		sharedEmbeddedCache.Close()
		sharedEmbeddedCache = nil
	}
	if sharedCacheTempDir != "" {
		os.RemoveAll(sharedCacheTempDir)
		sharedCacheTempDir = ""
	}
}

// getFreePortForMain returns a free TCP port (used in TestMain before tests run).
func getFreePortForMain() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return port, nil
}

// --- Transparent Auth Test Helpers ---

// hmacSHA256 computes HMAC-SHA256.
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// deriveSigningKeyForTest derives the SigV4 signing key from a secret key, date, and region.
// Replicates auth.deriveSigningKey (unexported).
func deriveSigningKeyForTest(secretKey, dateStr, region string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStr))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}

// encryptSigningKeysHeader encrypts signing key entries the same way Tigris does.
// Replicates auth.encryptForTest (unexported).
func encryptSigningKeysHeader(t *testing.T, proxySecret, accessKey string, entries []auth.SigningKeyEntry) string {
	t.Helper()

	payload, err := json.Marshal(entries)
	if err != nil {
		t.Fatalf("failed to marshal entries: %v", err)
	}

	keyHash := sha256.Sum256([]byte(proxySecret))
	block, err := aes.NewCipher(keyHash[:])
	if err != nil {
		t.Fatalf("failed to create cipher: %v", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("failed to create GCM: %v", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("failed to generate nonce: %v", err)
	}

	ciphertext := gcm.Seal(nil, nonce, payload, []byte(accessKey))
	raw := append(nonce, ciphertext...)
	return base64.StdEncoding.EncodeToString(raw)
}

// newSigningKeysUpstreamHandler creates an upstream handler that wraps gofakes3
// and injects encrypted signing keys in the response header on 2xx.
func newSigningKeysUpstreamHandler(t *testing.T, backend *s3mem.Backend) http.HandlerFunc {
	t.Helper()
	faker := gofakes3.New(backend)
	s3Handler := faker.Server()

	return func(w http.ResponseWriter, r *http.Request) {
		// Use a ResponseRecorder to capture the gofakes3 response
		rec := httptest.NewRecorder()
		s3Handler.ServeHTTP(rec, r)

		// On 2xx, inject the signing keys header
		if rec.Code >= 200 && rec.Code < 300 {
			accessKey, _ := auth.ExtractAccessKey(r)
			if accessKey != "" {
				today := time.Now().UTC().Format("20060102")
				signingKey := deriveSigningKeyForTest(TestSecretKey, today, TestRegion)
				entries := []auth.SigningKeyEntry{
					{Date: today, Region: TestRegion, SigningKey: hex.EncodeToString(signingKey)},
				}
				encrypted := encryptSigningKeysHeader(t, TestProxySecretKey, accessKey, entries)
				rec.Header().Set("X-Tigris-Proxy-Signing-Keys", encrypted)
			}
		}

		// Copy recorded response to the real writer
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		w.Write(rec.Body.Bytes())
	}
}

// NewTestEnvironmentWithTransparentAuth creates a test environment with transparent proxy mode
// and local auth enabled. Uses shared embedded cache for cache-hit testing.
func NewTestEnvironmentWithTransparentAuth(t *testing.T, upstreamHandler http.HandlerFunc) *TestEnvironment {
	t.Helper()

	if sharedEmbeddedCache == nil {
		panic("Shared embedded cache not initialized - TestMain must run first")
	}

	// Track upstream requests
	var requestCount int32
	counter := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requestCount, 1)
			next.ServeHTTP(w, r)
		})
	}

	upstream := httptest.NewServer(counter(upstreamHandler))

	// Create credential store (empty — transparent mode uses ProxySigner)
	credStore := auth.NewCredentialStore()

	// Create proxy signer (TAG's own Tigris credentials)
	proxySigner := auth.NewProxySigner(TestProxyAccessKey, TestProxySecretKey)

	// Create local auth components
	derivedKeyStore := auth.NewDerivedKeyStore(auth.DefaultDerivedKeyTTL)
	keyUnwrapper, err := auth.NewKeyUnwrapper(TestProxySecretKey)
	if err != nil {
		t.Fatalf("failed to create key unwrapper: %v", err)
	}
	authzCache := auth.NewAuthzCache(auth.DefaultAuthzCacheTTL)
	validator := auth.NewRequestValidator(derivedKeyStore)

	localAuth := &proxy.LocalAuthConfig{
		DerivedKeyStore: derivedKeyStore,
		Validator:       validator,
		KeyUnwrapper:    keyUnwrapper,
		AuthzCache:      authzCache,
	}

	// Create config with cache enabled
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
			TTL:           config.DefaultCacheTTL,
			SizeThreshold: config.DefaultCacheSizeThreshold,
		},
	}

	testCache := cache.NewCacheWithClient(sharedEmbeddedCache, &cfg.Cache)

	forwarder := proxy.NewForwarder(credStore, cfg.Upstream.Endpoint, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost, proxySigner, localAuth)
	service := proxy.NewService(forwarder, testCache, cfg)

	xCacheTracker := NewXCacheTracker()
	server := handlers.NewServer(service, "127.0.0.1", 0, true)
	wrappedRouter := xCacheTrackingMiddleware(xCacheTracker)(server.Router())
	tagServer := httptest.NewServer(wrappedRouter)

	signer := auth.NewRequestSigner(tagServer.URL, TestRegion)

	return &TestEnvironment{
		UpstreamServer:       upstream,
		TAGServer:            tagServer,
		CredStore:            credStore,
		Cache:                testCache,
		Config:               cfg,
		Service:              service,
		Signer:               signer,
		EmbeddedCache:        sharedEmbeddedCache,
		UpstreamRequestCount: &requestCount,
		XCacheTracker:        xCacheTracker,
		DerivedKeyStore:      derivedKeyStore,
		AuthzCache:           authzCache,
	}
}
