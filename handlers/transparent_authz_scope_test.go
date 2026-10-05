package handlers

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

const (
	authzScopeBucket       = "scope-bucket"
	authzScopeObject       = "private-object"
	authzScopeBody         = "private reader-only object body"
	authzScopeWriter       = "WRITEONLYKEY"
	authzScopeWriterSecret = "writer-secret"
	authzScopeReader       = "READERKEY"
	authzScopeReaderSecret = "reader-secret"
	authzScopeHeadReader   = "HEADONLYKEY"
	authzScopeHeadSecret   = "head-reader-secret"
	authzScopeProxySecret  = "proxy-secret"
)

// The independent origin owns this permission matrix. It allows the writer to
// initiate multipart uploads and read object tags, but denies its GetObject.
// The other two identities can read the private object body or metadata only.
type authzScopeFixture struct {
	origin  *httptest.Server
	gateway *httptest.Server
	client  *http.Client
	cache   *cache.Cache
	keys    *auth.DerivedKeyStore
	authz   *auth.AuthzCache

	writerReads  atomic.Int64
	readerReads  atomic.Int64
	headReads    atomic.Int64
	writerTags   atomic.Int64
	readerDenied atomic.Bool

	writerDates []string
}

func authzScopeEnvelope(tb testing.TB, credentials *auth.CredentialStore, accessKey string) (string, []string) {
	tb.Helper()
	var entries []auth.SigningKeyEntry
	now := time.Now().UTC()
	dates := []string{now.Format("20060102"), now.Add(-24 * time.Hour).Format("20060102")}
	for _, date := range dates {
		key, err := credentials.GetSigningKey(accessKey, date, "auto")
		if err != nil {
			tb.Fatal(err)
		}
		entries = append(entries, auth.SigningKeyEntry{Date: date, Region: "auto", SigningKey: hex.EncodeToString(key)})
	}
	plaintext, err := json.Marshal(entries)
	if err != nil {
		tb.Fatal(err)
	}
	hash := sha256.Sum256([]byte(authzScopeProxySecret))
	block, err := aes.NewCipher(hash[:])
	if err != nil {
		tb.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		tb.Fatal(err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		tb.Fatal(err)
	}
	raw := gcm.Seal(nonce, nonce, plaintext, []byte(accessKey))
	return base64.StdEncoding.EncodeToString(raw), dates
}

func newAuthzScopeFixture(tb testing.TB, authzTTL time.Duration) *authzScopeFixture {
	tb.Helper()
	fixture := &authzScopeFixture{client: &http.Client{Timeout: 5 * time.Second}}
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(authzScopeWriter, authzScopeWriterSecret)
	credentials.AddCredential(authzScopeReader, authzScopeReaderSecret)
	credentials.AddCredential(authzScopeHeadReader, authzScopeHeadSecret)
	originValidator := auth.NewRequestValidator(credentials)
	writerEnvelope, writerDates := authzScopeEnvelope(tb, credentials, authzScopeWriter)
	fixture.writerDates = writerDates
	readerEnvelope, _ := authzScopeEnvelope(tb, credentials, authzScopeReader)
	headEnvelope, _ := authzScopeEnvelope(tb, credentials, authzScopeHeadReader)

	writeObject := func(w http.ResponseWriter, body bool, envelope string) {
		w.Header().Set("ETag", `"scope-etag"`)
		w.Header().Set("Content-Length", "31")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Amz-Acl", "private")
		w.Header().Set("X-Tigris-Proxy-Signing-Keys", envelope)
		w.WriteHeader(http.StatusOK)
		if body {
			_, _ = io.WriteString(w, authzScopeBody)
		}
	}
	writeDenied := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, "<Error><Code>AccessDenied</Code></Error>")
	}
	objectPath := "/" + authzScopeBucket + "/" + authzScopeObject
	fixture.origin = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		original := r.Clone(r.Context())
		if forwardedHost := r.Header.Get("X-Tigris-Forwarded-Host"); forwardedHost != "" {
			original.Host = forwardedHost
		}
		accessKey, err := originValidator.ValidateRequest(original)
		if err != nil {
			writeDenied(w)
			return
		}

		switch {
		case accessKey == authzScopeWriter && r.Method == http.MethodPost && r.URL.Path == objectPath && r.URL.Query().Has("uploads"):
			w.Header().Set("X-Tigris-Proxy-Signing-Keys", writerEnvelope)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "<InitiateMultipartUploadResult><UploadId>scope-upload</UploadId></InitiateMultipartUploadResult>")
		case accessKey == authzScopeWriter && r.Method == http.MethodGet && r.URL.Path == objectPath && r.URL.Query().Has("tagging"):
			fixture.writerTags.Add(1)
			w.Header().Set("X-Tigris-Proxy-Signing-Keys", writerEnvelope)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "<Tagging/>")
		case r.URL.Path == objectPath && r.URL.RawQuery == "" && r.Method == http.MethodGet && accessKey == authzScopeWriter:
			fixture.writerReads.Add(1)
			writeDenied(w)
		case r.URL.Path == objectPath && r.URL.RawQuery == "" && r.Method == http.MethodGet && accessKey == authzScopeReader:
			fixture.readerReads.Add(1)
			if fixture.readerDenied.Load() {
				writeDenied(w)
				return
			}
			writeObject(w, true, readerEnvelope)
		case r.URL.Path == objectPath && r.URL.RawQuery == "" && r.Method == http.MethodHead && accessKey == authzScopeHeadReader:
			fixture.headReads.Add(1)
			writeObject(w, false, headEnvelope)
		default:
			writeDenied(w)
		}
	}))
	tb.Cleanup(fixture.origin.Close)

	fixture.keys = auth.NewDerivedKeyStore(auth.DefaultDerivedKeyTTL)
	fixture.authz = auth.NewAuthzCache(authzTTL)
	unwrapper, err := auth.NewKeyUnwrapper(authzScopeProxySecret)
	if err != nil {
		tb.Fatal(err)
	}
	localAuth := &proxy.LocalAuthConfig{
		DerivedKeyStore: fixture.keys,
		Validator:       auth.NewRequestValidator(fixture.keys),
		KeyUnwrapper:    unwrapper,
		AuthzCache:      fixture.authz,
	}
	forwarder := proxy.NewForwarder(auth.NewCredentialStore(), fixture.origin.URL, "auto", 10, auth.NewProxySigner("PROXYKEY", authzScopeProxySecret), localAuth)
	cfg := config.NewDefault()
	fixture.cache = cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	service := proxy.NewService(forwarder, fixture.cache, cfg)
	fixture.gateway = httptest.NewServer(NewServer(service, "127.0.0.1", 0, false, 0).Router())
	tb.Cleanup(fixture.gateway.Close)
	tb.Cleanup(fixture.client.CloseIdleConnections)
	return fixture
}

func authzScopeRequest(tb testing.TB, endpoint, method, path, accessKey, secret string) *http.Request {
	tb.Helper()
	request, err := auth.NewRequestSigner(endpoint, "auto").SignRequest(context.Background(), method, path, nil, "", accessKey, secret, nil)
	if err != nil {
		tb.Fatal(err)
	}
	return request
}

func authzScopeRead(tb testing.TB, client *http.Client, request *http.Request) (int, http.Header, string) {
	tb.Helper()
	response, err := client.Do(request)
	if err != nil {
		tb.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		tb.Fatal(err)
	}
	return response.StatusCode, response.Header, string(body)
}

func requirePrivateCacheEntry(tb testing.TB, fixture *authzScopeFixture) {
	tb.Helper()
	timeout := time.After(2 * time.Second)
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		meta, found, err := fixture.cache.GetMeta(context.Background(), authzScopeBucket, authzScopeObject)
		if err == nil && found && meta != nil {
			if meta.IsPublicRead() {
				tb.Fatal("fixture must cache a private, not anonymous-readable, object")
			}
			return
		}
		select {
		case <-timeout:
			tb.Fatal("authorized reader's ordinary GET did not populate private cache metadata")
		case <-poll.C:
		}
	}
}

func TestTransparentPassthroughWriteDoesNotGrantPrivateRead(t *testing.T) {
	fixture := newAuthzScopeFixture(t, time.Minute)
	path := "/" + authzScopeBucket + "/" + authzScopeObject

	// The independent origin denies the writer's valid GetObject signature.
	status, _, _ := authzScopeRead(t, fixture.client, authzScopeRequest(t, fixture.origin.URL, http.MethodGet, path, authzScopeWriter, authzScopeWriterSecret))
	if status != http.StatusForbidden {
		t.Fatalf("origin must deny writer GetObject, got %d", status)
	}

	// A separate authorized reader fills a private cache entry using the
	// ordinary routed GetObject path.
	status, readerHeaders, body := authzScopeRead(t, fixture.client, authzScopeRequest(t, fixture.gateway.URL, http.MethodGet, path, authzScopeReader, authzScopeReaderSecret))
	if status != http.StatusOK || body != authzScopeBody {
		t.Fatalf("authorized reader GetObject: status=%d body=%q", status, body)
	}
	if readerHeaders.Get("X-Tigris-Proxy-Signing-Keys") != "" {
		t.Fatal("reader response leaked the internal signing-keys header")
	}
	requirePrivateCacheEntry(t, fixture)

	// The successful reader GET grants access to an authorized object-cache hit.
	readerOriginGets := fixture.readerReads.Load()
	status, headers, body := authzScopeRead(t, fixture.client, authzScopeRequest(t, fixture.gateway.URL, http.MethodGet, path, authzScopeReader, authzScopeReaderSecret))
	if status != http.StatusOK || headers.Get("X-Cache") != "HIT" || body != authzScopeBody || fixture.readerReads.Load() != readerOriginGets {
		t.Fatalf("authorized reader cache hit: status=%d cache=%q body=%q origin GET delta=%d", status, headers.Get("X-Cache"), body, fixture.readerReads.Load()-readerOriginGets)
	}

	// A separate HeadObject-only identity earns a grant from its own successful
	// HEAD, then may use the cached object's metadata without contacting origin.
	status, headers, headBody := authzScopeRead(t, fixture.client, authzScopeRequest(t, fixture.gateway.URL, http.MethodHead, path, authzScopeHeadReader, authzScopeHeadSecret))
	if status != http.StatusOK || headBody != "" || headers.Get("X-Tigris-Proxy-Signing-Keys") != "" || headers.Get("Content-Length") != "31" || !fixture.authz.IsAuthorized(authzScopeHeadReader, authzScopeBucket) {
		t.Fatalf("authorized HeadObject grant: status=%d body=%q content_length=%q leaked_header=%q grant=%v", status, headBody, headers.Get("Content-Length"), headers.Get("X-Tigris-Proxy-Signing-Keys"), fixture.authz.IsAuthorized(authzScopeHeadReader, authzScopeBucket))
	}
	headOriginGets := fixture.headReads.Load()
	status, headers, headBody = authzScopeRead(t, fixture.client, authzScopeRequest(t, fixture.gateway.URL, http.MethodHead, path, authzScopeHeadReader, authzScopeHeadSecret))
	if status != http.StatusOK || headBody != "" || headers.Get("Content-Length") != "31" || headers.Get("X-Cache") != "HIT" || fixture.headReads.Load() != headOriginGets {
		t.Fatalf("authorized HeadObject cache hit: status=%d body=%q cache=%q origin HEAD delta=%d", status, headBody, headers.Get("X-Cache"), fixture.headReads.Load()-headOriginGets)
	}

	// A successful multipart-initiation reply still teaches both signing keys,
	// but a write does not create a read-authorization grant.
	status, headers, writeBody := authzScopeRead(t, fixture.client, authzScopeRequest(t, fixture.gateway.URL, http.MethodPost, path+"?uploads", authzScopeWriter, authzScopeWriterSecret))
	if status != http.StatusOK || writeBody != "<InitiateMultipartUploadResult><UploadId>scope-upload</UploadId></InitiateMultipartUploadResult>" || headers.Get("X-Tigris-Proxy-Signing-Keys") != "" {
		t.Fatalf("multipart initiation response: status=%d body=%q leaked_header=%q", status, writeBody, headers.Get("X-Tigris-Proxy-Signing-Keys"))
	}
	if fixture.authz.IsAuthorized(authzScopeWriter, authzScopeBucket) {
		t.Error("successful multipart initiation granted the writer read access")
	}
	for _, date := range fixture.writerDates {
		key, err := fixture.keys.GetSigningKey(authzScopeWriter, date, "auto")
		if err != nil || len(key) != 32 {
			t.Errorf("multipart response did not preserve writer's 32-byte key for %s: length=%d error=%v", date, len(key), err)
		}
	}

	// A successful GET subresource is still passthrough, not GetObject, and
	// likewise must not grant access to the cached object body.
	status, headers, _ = authzScopeRead(t, fixture.client, authzScopeRequest(t, fixture.gateway.URL, http.MethodGet, path+"?tagging", authzScopeWriter, authzScopeWriterSecret))
	if status != http.StatusOK || headers.Get("X-Tigris-Proxy-Signing-Keys") != "" || fixture.writerTags.Load() != 1 {
		t.Errorf("successful object-tagging passthrough: status=%d leaked_header=%q origin calls=%d", status, headers.Get("X-Tigris-Proxy-Signing-Keys"), fixture.writerTags.Load())
	}
	if fixture.authz.IsAuthorized(authzScopeWriter, authzScopeBucket) {
		t.Error("successful object-tagging passthrough granted the writer read access")
	}

	// The writer's next valid GetObject must reach origin and preserve its 403;
	// no private cached bytes may be returned.
	writerOriginGets := fixture.writerReads.Load()
	status, headers, body = authzScopeRead(t, fixture.client, authzScopeRequest(t, fixture.gateway.URL, http.MethodGet, path, authzScopeWriter, authzScopeWriterSecret))
	if status != http.StatusForbidden || strings.Contains(body, authzScopeBody) || fixture.writerReads.Load() != writerOriginGets+1 {
		t.Errorf("writer GetObject must reach origin and be denied: status=%d cache=%q private_body=%v origin GET delta=%d", status, headers.Get("X-Cache"), strings.Contains(body, authzScopeBody), fixture.writerReads.Load()-writerOriginGets)
	}
}

func TestTransparentReadGrantIsRevokedAfterUpstream403(t *testing.T) {
	fixture := newAuthzScopeFixture(t, time.Minute)
	path := "/" + authzScopeBucket + "/" + authzScopeObject
	status, _, body := authzScopeRead(t, fixture.client, authzScopeRequest(t, fixture.gateway.URL, http.MethodGet, path, authzScopeReader, authzScopeReaderSecret))
	if status != http.StatusOK || body != authzScopeBody || !fixture.authz.IsAuthorized(authzScopeReader, authzScopeBucket) {
		t.Fatalf("initial authorized read: status=%d grant=%v", status, fixture.authz.IsAuthorized(authzScopeReader, authzScopeBucket))
	}

	fixture.readerDenied.Store(true)
	request := authzScopeRequest(t, fixture.gateway.URL, http.MethodGet, path, authzScopeReader, authzScopeReaderSecret)
	request.Header.Set("Cache-Control", "no-store")
	status, _, body = authzScopeRead(t, fixture.client, request)
	if status != http.StatusForbidden || strings.Contains(body, authzScopeBody) {
		t.Fatalf("origin-denied revalidation: status=%d private_body=%v", status, strings.Contains(body, authzScopeBody))
	}
	if fixture.authz.IsAuthorized(authzScopeReader, authzScopeBucket) {
		t.Fatal("upstream 403 did not revoke the read grant")
	}
}
