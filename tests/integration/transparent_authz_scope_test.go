package integration

import (
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigrisdata/tag/auth"
)

const (
	authzScopeBucket            = "scope-write-bucket"
	authzScopeObject            = "private-object"
	authzScopeBody              = "private reader-only object body"
	authzScopeWriter            = "WRITEONLYKEY"
	authzScopeWriterSecret      = "writer-secret"
	authzScopeReader            = "READERKEY"
	authzScopeReaderSecret      = "reader-secret"
	authzScopeHeadReader        = "HEADONLYKEY"
	authzScopeHeadSecret        = "head-reader-secret"
	authzScopeOtherReader       = "OTHERREADERKEY"
	authzScopeOtherReaderSecret = "other-reader-secret"
)

// This origin enforces permissions independently of TAG's AuthzCache.
type authzScopeOrigin struct {
	bucket      string
	otherBucket string
	validator   *auth.RequestValidator

	mu        sync.RWMutex
	envelopes map[string]string

	writerReads            atomic.Int64
	writerAttributes       atomic.Int64
	readerReads            atomic.Int64
	readerOtherBucketReads atomic.Int64
	headReads              atomic.Int64
	writerTags             atomic.Int64
	otherReaderReads       atomic.Int64
	readerDenied           atomic.Bool
}

func newAuthzScopeTestEnvironment(t *testing.T, bucket string) (*TestEnvironment, *authzScopeOrigin, *auth.CredentialStore) {
	t.Helper()
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(authzScopeWriter, authzScopeWriterSecret)
	credentials.AddCredential(authzScopeReader, authzScopeReaderSecret)
	credentials.AddCredential(authzScopeHeadReader, authzScopeHeadSecret)
	credentials.AddCredential(authzScopeOtherReader, authzScopeOtherReaderSecret)
	origin := &authzScopeOrigin{
		bucket:      bucket,
		otherBucket: bucket + "-other",
		validator:   auth.NewRequestValidator(credentials),
		envelopes:   make(map[string]string),
	}
	env := NewTestEnvironmentWithTransparentAuth(t, origin.serveHTTP)
	return env, origin, credentials
}

func (o *authzScopeOrigin) setSigningKeys(accessKey, envelope string) {
	o.mu.Lock()
	o.envelopes[accessKey] = envelope
	o.mu.Unlock()
}

func (o *authzScopeOrigin) signingKeys(accessKey string) string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.envelopes[accessKey]
}

func (o *authzScopeOrigin) serveHTTP(w http.ResponseWriter, r *http.Request) {
	original := r.Clone(r.Context())
	if forwardedHost := r.Header.Get("X-Tigris-Forwarded-Host"); forwardedHost != "" {
		original.Host = forwardedHost
	}
	accessKey, err := o.validator.ValidateRequest(original)
	if err != nil {
		authzScopeDeny(w)
		return
	}

	objectPath := "/" + o.bucket + "/" + authzScopeObject
	otherObjectPath := "/" + o.otherBucket + "/" + authzScopeObject
	switch {
	case accessKey == authzScopeWriter && r.Method == http.MethodPost && r.URL.Path == objectPath && r.URL.Query().Has("uploads"):
		envelope := o.signingKeys(authzScopeWriter)
		if envelope == "" {
			authzScopeDeny(w)
			return
		}
		w.Header().Set("X-Tigris-Proxy-Signing-Keys", envelope)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "<InitiateMultipartUploadResult><UploadId>scope-upload</UploadId></InitiateMultipartUploadResult>")
	case accessKey == authzScopeWriter && r.Method == http.MethodGet && r.URL.Path == objectPath && r.URL.Query().Has("attributes"):
		o.writerAttributes.Add(1)
		envelope := o.signingKeys(authzScopeWriter)
		if envelope == "" {
			authzScopeDeny(w)
			return
		}
		w.Header().Set("X-Tigris-Proxy-Signing-Keys", envelope)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "<GetObjectAttributesOutput/>")
	case accessKey == authzScopeWriter && r.Method == http.MethodGet && r.URL.Path == objectPath && r.URL.Query().Has("tagging"):
		o.writerTags.Add(1)
		envelope := o.signingKeys(authzScopeWriter)
		if envelope == "" {
			authzScopeDeny(w)
			return
		}
		w.Header().Set("X-Tigris-Proxy-Signing-Keys", envelope)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "<Tagging/>")
	case r.URL.Path == objectPath && r.URL.RawQuery == "" && r.Method == http.MethodGet && accessKey == authzScopeWriter:
		o.writerReads.Add(1)
		authzScopeDeny(w)
	case r.URL.Path == objectPath && r.URL.RawQuery == "" && r.Method == http.MethodGet && accessKey == authzScopeReader:
		o.readerReads.Add(1)
		if o.readerDenied.Load() {
			authzScopeDeny(w)
			return
		}
		envelope := o.signingKeys(authzScopeReader)
		if envelope == "" {
			authzScopeDeny(w)
			return
		}
		authzScopeWriteObject(w, true, envelope)
	case r.URL.Path == objectPath && r.URL.RawQuery == "" && r.Method == http.MethodHead && accessKey == authzScopeHeadReader:
		o.headReads.Add(1)
		envelope := o.signingKeys(authzScopeHeadReader)
		if envelope == "" {
			authzScopeDeny(w)
			return
		}
		authzScopeWriteObject(w, false, envelope)
	case r.URL.Path == otherObjectPath && r.URL.RawQuery == "" && r.Method == http.MethodGet && accessKey == authzScopeOtherReader:
		o.otherReaderReads.Add(1)
		envelope := o.signingKeys(authzScopeOtherReader)
		if envelope == "" {
			authzScopeDeny(w)
			return
		}
		authzScopeWriteObject(w, true, envelope)
	case r.URL.Path == otherObjectPath && r.URL.RawQuery == "" && r.Method == http.MethodGet && accessKey == authzScopeReader:
		o.readerOtherBucketReads.Add(1)
		authzScopeDeny(w)
	default:
		authzScopeDeny(w)
	}
}

func authzScopeDeny(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusForbidden)
	_, _ = io.WriteString(w, "<Error><Code>AccessDenied</Code></Error>")
}

func authzScopeWriteObject(w http.ResponseWriter, withBody bool, envelope string) {
	w.Header().Set("ETag", `"scope-etag"`)
	w.Header().Set("Content-Length", "31")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Amz-Acl", "private")
	w.Header().Set("X-Tigris-Proxy-Signing-Keys", envelope)
	w.WriteHeader(http.StatusOK)
	if withBody {
		_, _ = io.WriteString(w, authzScopeBody)
	}
}

func authzScopeRequest(t *testing.T, endpoint, method, path, accessKey, secret string) *http.Request {
	t.Helper()
	request, err := auth.NewRequestSigner(endpoint, TestRegion).SignRequest(context.Background(), method, path, nil, "", accessKey, secret, nil)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func authzScopeKeysForRequest(t *testing.T, credentials *auth.CredentialStore, accessKey string, request *http.Request) (string, []string) {
	t.Helper()
	info, err := auth.ParseAuthInfo(request)
	if err != nil {
		t.Fatal(err)
	}
	date, err := time.Parse("20060102", info.Date)
	if err != nil {
		t.Fatalf("parse signing date %q: %v", info.Date, err)
	}
	dates := []string{info.Date, date.Add(-24 * time.Hour).Format("20060102")}
	entries := make([]auth.SigningKeyEntry, 0, len(dates))
	for _, signingDate := range dates {
		key, err := credentials.GetSigningKey(accessKey, signingDate, info.Region)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, auth.SigningKeyEntry{Date: signingDate, Region: info.Region, SigningKey: hex.EncodeToString(key)})
	}
	return encryptSigningKeysHeader(t, TestProxySecretKey, accessKey, entries), dates
}

func authzScopeRead(t *testing.T, client *http.Client, request *http.Request) (int, http.Header, string) {
	t.Helper()
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, response.Header, string(body)
}

func waitForAuthzScopeCache(env *TestEnvironment, bucket, key string, timeout time.Duration) bool {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(timeout)
	for {
		if env.IsCached(bucket, key) {
			return true
		}
		select {
		case <-ticker.C:
		case <-deadline:
			return false
		}
	}
}

func TestTransparentAuth_WriteOnlyMultipartDoesNotGrantCachedRead(t *testing.T) {
	env, origin, credentials := newAuthzScopeTestEnvironment(t, authzScopeBucket)
	defer env.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	path := "/" + authzScopeBucket + "/" + authzScopeObject

	// The origin denies a valid writer GetObject independently of TAG.
	status, _, body := authzScopeRead(t, client, authzScopeRequest(t, env.UpstreamServer.URL, http.MethodGet, path, authzScopeWriter, authzScopeWriterSecret))
	if status != http.StatusForbidden || strings.Contains(body, authzScopeBody) {
		t.Fatalf("origin must deny writer GetObject: status=%d private_body=%v", status, strings.Contains(body, authzScopeBody))
	}

	// An allowed reader populates the private object through TAG's real router.
	readerRequest := authzScopeRequest(t, env.TAGServer.URL, http.MethodGet, path, authzScopeReader, authzScopeReaderSecret)
	readerEnvelope, _ := authzScopeKeysForRequest(t, credentials, authzScopeReader, readerRequest)
	origin.setSigningKeys(authzScopeReader, readerEnvelope)
	status, headers, responseBody := authzScopeRead(t, client, readerRequest)
	if status != http.StatusOK || responseBody != authzScopeBody || headers.Get("X-Tigris-Proxy-Signing-Keys") != "" {
		t.Fatalf("authorized reader GetObject: status=%d body=%q leaked_header=%q", status, responseBody, headers.Get("X-Tigris-Proxy-Signing-Keys"))
	}
	if !waitForAuthzScopeCache(env, authzScopeBucket, authzScopeObject, 2*time.Second) {
		t.Fatal("authorized reader did not populate the private object cache")
	}
	meta, found, err := env.Cache.GetMeta(context.Background(), authzScopeBucket, authzScopeObject)
	if err != nil || !found || meta == nil || meta.IsPublicRead() {
		t.Fatalf("cached object must remain private: found=%v meta=%v error=%v", found, meta, err)
	}

	readerOriginGets := origin.readerReads.Load()
	status, headers, responseBody = authzScopeRead(t, client, readerRequest.Clone(context.Background()))
	if status != http.StatusOK || headers.Get("X-Cache") != "HIT" || responseBody != authzScopeBody || origin.readerReads.Load() != readerOriginGets {
		t.Fatalf("authorized reader cache hit: status=%d cache=%q body=%q origin GET delta=%d", status, headers.Get("X-Cache"), responseBody, origin.readerReads.Load()-readerOriginGets)
	}

	// A separate HeadObject identity also learns keys from an upstream HEAD and
	// then serves the object's metadata from the shared integration cache.
	headRequest := authzScopeRequest(t, env.TAGServer.URL, http.MethodHead, path, authzScopeHeadReader, authzScopeHeadSecret)
	headEnvelope, _ := authzScopeKeysForRequest(t, credentials, authzScopeHeadReader, headRequest)
	origin.setSigningKeys(authzScopeHeadReader, headEnvelope)
	status, headers, responseBody = authzScopeRead(t, client, headRequest)
	if status != http.StatusOK || responseBody != "" || headers.Get("X-Tigris-Proxy-Signing-Keys") != "" || headers.Get("Content-Length") != "31" || !env.AuthzCache.IsAuthorized(authzScopeHeadReader, authzScopeBucket) {
		t.Fatalf("authorized HeadObject: status=%d body=%q content_length=%q leaked_header=%q grant=%v", status, responseBody, headers.Get("Content-Length"), headers.Get("X-Tigris-Proxy-Signing-Keys"), env.AuthzCache.IsAuthorized(authzScopeHeadReader, authzScopeBucket))
	}
	headOriginGets := origin.headReads.Load()
	status, headers, responseBody = authzScopeRead(t, client, headRequest.Clone(context.Background()))
	if status != http.StatusOK || responseBody != "" || headers.Get("Content-Length") != "31" || headers.Get("X-Cache") != "HIT" || origin.headReads.Load() != headOriginGets {
		t.Fatalf("authorized HeadObject cache hit: status=%d body=%q cache=%q origin HEAD delta=%d", status, responseBody, headers.Get("X-Cache"), origin.headReads.Load()-headOriginGets)
	}

	// The signing-key response still completes multipart initiation and teaches
	// the writer the keys for its exact later GetObject signature.
	writerReadRequest := authzScopeRequest(t, env.TAGServer.URL, http.MethodGet, path, authzScopeWriter, authzScopeWriterSecret)
	writeRequest := authzScopeRequest(t, env.TAGServer.URL, http.MethodPost, path+"?uploads", authzScopeWriter, authzScopeWriterSecret)
	writerEnvelope, writerDates := authzScopeKeysForRequest(t, credentials, authzScopeWriter, writeRequest)
	origin.setSigningKeys(authzScopeWriter, writerEnvelope)
	status, headers, responseBody = authzScopeRead(t, client, writeRequest)
	if status != http.StatusOK || responseBody != "<InitiateMultipartUploadResult><UploadId>scope-upload</UploadId></InitiateMultipartUploadResult>" || headers.Get("X-Tigris-Proxy-Signing-Keys") != "" {
		t.Fatalf("multipart initiation: status=%d body=%q leaked_header=%q", status, responseBody, headers.Get("X-Tigris-Proxy-Signing-Keys"))
	}
	if env.AuthzCache.IsAuthorized(authzScopeWriter, authzScopeBucket) {
		t.Fatal("successful multipart initiation granted the writer cached-read access")
	}
	for _, date := range writerDates {
		key, err := env.DerivedKeyStore.GetSigningKey(authzScopeWriter, date, TestRegion)
		if err != nil || len(key) != 32 {
			t.Fatalf("multipart response did not teach the writer's 32-byte key for %s: length=%d error=%v", date, len(key), err)
		}
	}

	// A successful tagging subresource is still passthrough, not GetObject.
	tagRequest := authzScopeRequest(t, env.TAGServer.URL, http.MethodGet, path+"?tagging", authzScopeWriter, authzScopeWriterSecret)
	status, headers, _ = authzScopeRead(t, client, tagRequest)
	if status != http.StatusOK || headers.Get("X-Tigris-Proxy-Signing-Keys") != "" || origin.writerTags.Load() != 1 {
		t.Fatalf("object-tagging passthrough: status=%d leaked_header=%q origin calls=%d", status, headers.Get("X-Tigris-Proxy-Signing-Keys"), origin.writerTags.Load())
	}
	if env.AuthzCache.IsAuthorized(authzScopeWriter, authzScopeBucket) {
		t.Fatal("successful object-tagging passthrough granted the writer cached-read access")
	}

	// GetObjectAttributes also uses GET, but its success does not prove that the
	// client can read the object's body. The basic object route must not turn it
	// into a bucket cache-read grant.
	attributesRequest := authzScopeRequest(t, env.TAGServer.URL, http.MethodGet, path+"?attributes", authzScopeWriter, authzScopeWriterSecret)
	attributesEnvelope, _ := authzScopeKeysForRequest(t, credentials, authzScopeWriter, attributesRequest)
	origin.setSigningKeys(authzScopeWriter, attributesEnvelope)
	attributesRequest.Header.Set("Cache-Control", "no-store")
	status, headers, responseBody = authzScopeRead(t, client, attributesRequest)
	if status != http.StatusOK || responseBody != "<GetObjectAttributesOutput/>" || headers.Get("X-Tigris-Proxy-Signing-Keys") != "" || origin.writerAttributes.Load() != 1 {
		t.Fatalf("GetObjectAttributes: status=%d body=%q leaked_header=%q origin calls=%d", status, responseBody, headers.Get("X-Tigris-Proxy-Signing-Keys"), origin.writerAttributes.Load())
	}
	if env.AuthzCache.IsAuthorized(authzScopeWriter, authzScopeBucket) {
		t.Fatal("successful GetObjectAttributes granted the writer cached-read access")
	}

	// Check that this request is locally verifiable from the keys learned above.
	if _, err := auth.NewRequestValidator(env.DerivedKeyStore).ValidateRequest(writerReadRequest); err != nil {
		t.Fatalf("multipart response keys do not validate the writer's GetObject signature: %v", err)
	}
	writerOriginGets := origin.writerReads.Load()
	status, headers, responseBody = authzScopeRead(t, client, writerReadRequest)
	if status != http.StatusForbidden || strings.Contains(responseBody, authzScopeBody) || origin.writerReads.Load() != writerOriginGets+1 {
		t.Fatalf("writer GetObject must reach origin and be denied: status=%d cache=%q private_body=%v origin GET delta=%d", status, headers.Get("X-Cache"), strings.Contains(responseBody, authzScopeBody), origin.writerReads.Load()-writerOriginGets)
	}
}

func TestTransparentAuth_ReadGrantRevokedAfter403(t *testing.T) {
	const bucket = "scope-revoke-bucket"
	env, origin, credentials := newAuthzScopeTestEnvironment(t, bucket)
	defer env.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	path := "/" + bucket + "/" + authzScopeObject
	readerRequest := authzScopeRequest(t, env.TAGServer.URL, http.MethodGet, path, authzScopeReader, authzScopeReaderSecret)
	readerEnvelope, _ := authzScopeKeysForRequest(t, credentials, authzScopeReader, readerRequest)
	origin.setSigningKeys(authzScopeReader, readerEnvelope)
	status, _, responseBody := authzScopeRead(t, client, readerRequest)
	if status != http.StatusOK || responseBody != authzScopeBody || !env.AuthzCache.IsAuthorized(authzScopeReader, bucket) {
		t.Fatalf("initial authorized GetObject: status=%d grant=%v", status, env.AuthzCache.IsAuthorized(authzScopeReader, bucket))
	}

	origin.readerDenied.Store(true)
	request := authzScopeRequest(t, env.TAGServer.URL, http.MethodGet, path, authzScopeReader, authzScopeReaderSecret)
	request.Header.Set("Cache-Control", "no-store")
	status, _, responseBody = authzScopeRead(t, client, request)
	if status != http.StatusForbidden || strings.Contains(responseBody, authzScopeBody) {
		t.Fatalf("origin-denied GetObject: status=%d private_body=%v", status, strings.Contains(responseBody, authzScopeBody))
	}
	if env.AuthzCache.IsAuthorized(authzScopeReader, bucket) {
		t.Fatal("upstream 403 did not revoke the read grant")
	}
}

func TestTransparentAuth_AuthzGrantRemainsBucketScoped(t *testing.T) {
	const bucketA = "scope-authz-bucket-a"
	bucketB := bucketA + "-other"
	env, origin, credentials := newAuthzScopeTestEnvironment(t, bucketA)
	defer env.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	pathA := "/" + bucketA + "/" + authzScopeObject
	pathB := "/" + bucketB + "/" + authzScopeObject

	// The same access key is allowed to read bucket A but not bucket B.
	readerARequest := authzScopeRequest(t, env.TAGServer.URL, http.MethodGet, pathA, authzScopeReader, authzScopeReaderSecret)
	readerBRequest := authzScopeRequest(t, env.TAGServer.URL, http.MethodGet, pathB, authzScopeReader, authzScopeReaderSecret)
	readerEnvelope, _ := authzScopeKeysForRequest(t, credentials, authzScopeReader, readerBRequest)
	origin.setSigningKeys(authzScopeReader, readerEnvelope)
	status, headers, body := authzScopeRead(t, client, readerARequest)
	if status != http.StatusOK || body != authzScopeBody || headers.Get("X-Tigris-Proxy-Signing-Keys") != "" {
		t.Fatalf("authorized bucket-A GetObject: status=%d body=%q leaked_header=%q", status, body, headers.Get("X-Tigris-Proxy-Signing-Keys"))
	}
	if !waitForAuthzScopeCache(env, bucketA, authzScopeObject, 2*time.Second) {
		t.Fatal("reader A did not populate bucket A")
	}
	if !env.AuthzCache.IsAuthorized(authzScopeReader, bucketA) || env.AuthzCache.IsAuthorized(authzScopeReader, bucketB) {
		t.Fatalf("bucket A grant leaked to bucket B: A=%v B=%v", env.AuthzCache.IsAuthorized(authzScopeReader, bucketA), env.AuthzCache.IsAuthorized(authzScopeReader, bucketB))
	}

	// Another identity populates a private cache entry in bucket B. Reader A's
	// bucket-A grant must not make this private object readable from cache.
	otherReaderRequest := authzScopeRequest(t, env.TAGServer.URL, http.MethodGet, pathB, authzScopeOtherReader, authzScopeOtherReaderSecret)
	otherReaderEnvelope, _ := authzScopeKeysForRequest(t, credentials, authzScopeOtherReader, otherReaderRequest)
	origin.setSigningKeys(authzScopeOtherReader, otherReaderEnvelope)
	status, headers, body = authzScopeRead(t, client, otherReaderRequest)
	if status != http.StatusOK || body != authzScopeBody || headers.Get("X-Tigris-Proxy-Signing-Keys") != "" {
		t.Fatalf("authorized bucket-B cache fill: status=%d body=%q leaked_header=%q", status, body, headers.Get("X-Tigris-Proxy-Signing-Keys"))
	}
	if !waitForAuthzScopeCache(env, bucketB, authzScopeObject, 2*time.Second) || !env.AuthzCache.IsAuthorized(authzScopeOtherReader, bucketB) {
		t.Fatal("other reader did not authorize and populate bucket B")
	}
	meta, found, err := env.Cache.GetMeta(context.Background(), bucketB, authzScopeObject)
	if err != nil || !found || meta == nil || meta.IsPublicRead() {
		t.Fatalf("bucket-B cache entry must remain private: found=%v meta=%v error=%v", found, meta, err)
	}
	otherReaderOriginGets := origin.otherReaderReads.Load()
	status, headers, body = authzScopeRead(t, client, otherReaderRequest.Clone(context.Background()))
	if status != http.StatusOK || headers.Get("X-Cache") != "HIT" || body != authzScopeBody || origin.otherReaderReads.Load() != otherReaderOriginGets {
		t.Fatalf("authorized other-reader bucket-B cache hit: status=%d cache=%q body=%q origin GET delta=%d", status, headers.Get("X-Cache"), body, origin.otherReaderReads.Load()-otherReaderOriginGets)
	}

	readerOriginGets := origin.readerOtherBucketReads.Load()
	status, headers, body = authzScopeRead(t, client, readerBRequest)
	if status != http.StatusForbidden || strings.Contains(body, authzScopeBody) || origin.readerOtherBucketReads.Load() != readerOriginGets+1 {
		t.Fatalf("reader A must reach origin and be denied in bucket B: status=%d cache=%q private_body=%v origin GET delta=%d", status, headers.Get("X-Cache"), strings.Contains(body, authzScopeBody), origin.readerOtherBucketReads.Load()-readerOriginGets)
	}
	if !env.AuthzCache.IsAuthorized(authzScopeReader, bucketA) || env.AuthzCache.IsAuthorized(authzScopeReader, bucketB) {
		t.Fatalf("bucket-B denial changed bucket grants: A=%v B=%v", env.AuthzCache.IsAuthorized(authzScopeReader, bucketA), env.AuthzCache.IsAuthorized(authzScopeReader, bucketB))
	}
}
