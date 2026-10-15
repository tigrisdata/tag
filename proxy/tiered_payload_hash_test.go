package proxy

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/s3err"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	payloadHashTestAccessKey = "payload-hash-test-access-key"
	payloadHashTestSecretKey = "payload-hash-test-secret-key"
	payloadHashTestRegion    = "us-east-1"
	payloadHashTestBucket    = "bucket"
)

type payloadHashTestFlavor string

const (
	payloadHashSigningFlavor     payloadHashTestFlavor = "signing"
	payloadHashTransparentFlavor payloadHashTestFlavor = "transparent"
)

type payloadHashTestClient struct {
	*bodyKeyLedger
	metaMu   sync.Mutex
	metaPuts int
}

func (c *payloadHashTestClient) Put(ctx context.Context, key string, data []byte, ttl int64) error {
	if strings.HasPrefix(key, "meta|") {
		c.metaMu.Lock()
		c.metaPuts++
		c.metaMu.Unlock()
	}
	return c.CacheClient.Put(ctx, key, data, ttl)
}

func (c *payloadHashTestClient) PutIfVersion(ctx context.Context, key string, data []byte, ttl int64, version uint64) (uint64, error) {
	if strings.HasPrefix(key, "meta|") {
		c.metaMu.Lock()
		c.metaPuts++
		c.metaMu.Unlock()
	}
	return c.CacheClient.PutIfVersion(ctx, key, data, ttl, version)
}

func (c *payloadHashTestClient) metaPutCount() int {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	return c.metaPuts
}

func (c *payloadHashTestClient) bodyCounts() (puts, deletes int) {
	c.bodyKeyLedger.mu.Lock()
	defer c.bodyKeyLedger.mu.Unlock()
	return len(c.bodyKeyLedger.puts), len(c.bodyKeyLedger.deletes)
}

func (c *payloadHashTestClient) lastBodyPair() (put, deleted string, ok bool) {
	c.bodyKeyLedger.mu.Lock()
	defer c.bodyKeyLedger.mu.Unlock()
	if len(c.bodyKeyLedger.puts) == 0 || len(c.bodyKeyLedger.deletes) == 0 {
		return "", "", false
	}
	return c.bodyKeyLedger.puts[len(c.bodyKeyLedger.puts)-1], c.bodyKeyLedger.deletes[len(c.bodyKeyLedger.deletes)-1], true
}

type tieredPayloadHashFixture struct {
	service     *Service
	cache       *cache.Cache
	client      *payloadHashTestClient
	forwarder   RequestForwarder
	forwards    *atomic.Int64
	signingTime time.Time
}

func newTieredPayloadHashFixture(t *testing.T, flavor payloadHashTestFlavor) *tieredPayloadHashFixture {
	t.Helper()

	var forwards atomic.Int64
	signingTime := time.Now().UTC()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwards.Add(1)
		if r.Method == http.MethodPut {
			w.Header().Set("ETag", `"upstream"`)
		}
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("upstream body"))
		}
	}))
	t.Cleanup(upstream.Close)

	cfg := config.NewDefault()
	cfg.Mode = config.ModeTiered
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SetLegacyCoordination(false)
	cfg.Cache.SizeThreshold = 1024
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate tiered test config: %v", err)
	}

	credentials := auth.NewCredentialStore()
	credentials.AddCredential(payloadHashTestAccessKey, payloadHashTestSecretKey)

	var forwarder RequestForwarder
	switch flavor {
	case payloadHashSigningFlavor:
		forwarder = NewForwarder(credentials, upstream.URL, payloadHashTestRegion, 10, nil, nil)
	case payloadHashTransparentFlavor:
		derivedKeys := auth.NewDerivedKeyStore(auth.DefaultDerivedKeyTTL)
		date := signingTime.Format("20060102")
		key, err := credentials.GetSigningKey(payloadHashTestAccessKey, date, payloadHashTestRegion)
		if err != nil {
			t.Fatalf("derive transparent signing key: %v", err)
		}
		derivedKeys.Store(payloadHashTestAccessKey, date, payloadHashTestRegion, key)
		authorization := auth.NewAuthzCache(auth.DefaultAuthzCacheTTL)
		authorization.Grant(payloadHashTestAccessKey, payloadHashTestBucket)
		forwarder = NewForwarder(credentials, upstream.URL, payloadHashTestRegion, 10,
			auth.NewProxySigner("tag-proxy-access-key", "tag-proxy-secret-key"),
			&LocalAuthConfig{
				DerivedKeyStore: derivedKeys,
				Validator:       auth.NewRequestValidator(derivedKeys),
				AuthzCache:      authorization,
			})
	default:
		t.Fatalf("unknown forwarder flavor %q", flavor)
	}

	client := &payloadHashTestClient{bodyKeyLedger: &bodyKeyLedger{CacheClient: cacheclient.NewMemoryCache()}}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	return &tieredPayloadHashFixture{
		service:     NewService(forwarder, store, cfg),
		cache:       store,
		client:      client,
		forwarder:   forwarder,
		forwards:    &forwards,
		signingTime: signingTime,
	}
}

func payloadSHA256(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func (f *tieredPayloadHashFixture) signedRequest(t *testing.T, method, key string, body []byte, bodyHash, contentMD5 string, extraHeaders ...http.Header) *http.Request {
	t.Helper()

	r := httptest.NewRequest(method, "http://s3.test/"+payloadHashTestBucket+"/"+key, bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.Header.Set("X-Amz-Content-Sha256", bodyHash)
	if contentMD5 != "" {
		r.Header.Set("Content-MD5", contentMD5)
	}
	if len(extraHeaders) > 1 {
		t.Fatal("signed request accepts at most one extra header map")
	}
	if len(extraHeaders) == 1 {
		for name, values := range extraHeaders[0] {
			for _, value := range values {
				r.Header.Add(name, value)
			}
		}
	}
	credentials := aws.Credentials{
		AccessKeyID:     payloadHashTestAccessKey,
		SecretAccessKey: payloadHashTestSecretKey,
		Source:          "payload hash integrity test",
	}
	contentLength := r.ContentLength
	// net/http exposes Content-Length as Request.ContentLength rather than a
	// header, while TAG's validator canonicalizes only header-map entries.
	// Leave it unsigned just as an HTTP server's received request does, then
	// restore the framing length used by the tiered admission gate.
	r.ContentLength = -1
	if err := v4.NewSigner().SignHTTP(context.Background(), credentials, r, bodyHash, "s3", payloadHashTestRegion, f.signingTime); err != nil {
		t.Fatalf("sign %s request: %v", method, err)
	}
	r.ContentLength = contentLength
	return r
}

func (f *tieredPayloadHashFixture) do(t *testing.T, method, key string, body []byte, bodyHash, contentMD5 string, extraHeaders ...http.Header) *httptest.ResponseRecorder {
	t.Helper()

	r := f.signedRequest(t, method, key, body, bodyHash, contentMD5, extraHeaders...)
	result, _, _, err := f.forwarder.ValidateAndGetCredentials(r)
	if err != nil || result != AuthValidated {
		t.Fatalf("independently signed %s %s did not pass %T authentication: result=%v err=%v authorization=%q host=%q url=%s headers=%v", method, key, f.forwarder, result, err, r.Header.Get("Authorization"), r.Host, r.URL, r.Header)
	}
	// The validators must not consume a body, but give the operation its own
	// reader even if that contract changes in a future implementation.
	r.Body = io.NopCloser(bytes.NewReader(body))
	w := httptest.NewRecorder()
	switch method {
	case http.MethodPut:
		err = f.service.HandlePutObject(w, r)
	case http.MethodGet:
		err = f.service.HandleGetObject(w, r)
	case http.MethodHead:
		err = f.service.HandleHeadObject(w, r)
	default:
		t.Fatalf("unsupported test method %s", method)
	}
	if err != nil {
		t.Fatalf("%s %s: %v", method, key, err)
	}
	return w
}

func tieredPayloadHashErrorCode(t *testing.T, w *httptest.ResponseRecorder) s3err.ErrorCode {
	t.Helper()
	var response s3err.ErrorResponse
	if err := xml.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode S3 error response: %v; body=%q", err, w.Body.String())
	}
	return response.Code
}

func requirePayloadHashBodyAbsentAfterRejection(t *testing.T, fixture *tieredPayloadHashFixture, beforePuts, beforeDeletes int) {
	t.Helper()
	puts, deletes := fixture.client.bodyCounts()
	if puts != beforePuts+1 || deletes != beforeDeletes+1 {
		t.Fatalf("staged body lifecycle = puts %d->%d, deletes %d->%d; want one body write and cleanup attempt", beforePuts, puts, beforeDeletes, deletes)
	}
	putKey, deleteKey, ok := fixture.client.lastBodyPair()
	if !ok || putKey != deleteKey {
		t.Fatalf("staged body keys = put %q, delete %q, want the same key", putKey, deleteKey)
	}
	if body, err := fixture.client.CacheClient.Get(context.Background(), putKey); status.Code(err) != codes.NotFound {
		t.Fatalf("staged body %q lookup = %d bytes, err=%v; want NotFound", putKey, len(body), err)
	}
}

func requireEnginePutCapacityReleased(t *testing.T, service *Service) {
	t.Helper()
	if service.enginePutSlots != nil && len(service.enginePutSlots) != 0 {
		t.Fatalf("engine PUT slots still held after rejection: %d", len(service.enginePutSlots))
	}
	if service.populateBudget == nil {
		return
	}
	service.populateBudget.mu.Lock()
	remaining, total := service.populateBudget.remaining, service.populateBudget.total
	service.populateBudget.mu.Unlock()
	if remaining != total {
		t.Fatalf("engine PUT budget remaining = %d, want restored total %d", remaining, total)
	}
}

func TestTieredLocalPayloadHashIntegrity(t *testing.T) {
	for _, flavor := range []payloadHashTestFlavor{payloadHashSigningFlavor, payloadHashTransparentFlavor} {
		t.Run(string(flavor), func(t *testing.T) {
			fixture := newTieredPayloadHashFixture(t, flavor)
			trustedBody := []byte("trusted body")
			mutatedBody := []byte("changed body")
			if len(trustedBody) != len(mutatedBody) {
				t.Fatal("mutation must preserve the signed content length")
			}

			// A same-length mutation with no Content-MD5 has no other body
			// checksum to reject it. Authentication proves the signature over
			// the original digest in both real forwarder bindings.
			metaWrites := fixture.client.metaPutCount()
			bodyPuts, bodyDeletes := fixture.client.bodyCounts()
			w := fixture.do(t, http.MethodPut, "new-key", mutatedBody, payloadSHA256(trustedBody), "")
			meta, found, err := fixture.cache.GetMeta(context.Background(), payloadHashTestBucket, "new-key")
			if err != nil {
				t.Fatalf("read new-key metadata after rejected PUT: %v", err)
			}
			get := fixture.do(t, http.MethodGet, "new-key", nil, "UNSIGNED-PAYLOAD", "")
			head := fixture.do(t, http.MethodHead, "new-key", nil, "UNSIGNED-PAYLOAD", "")
			if w.Code != http.StatusBadRequest || tieredPayloadHashErrorCode(t, w) != "XAmzContentSHA256Mismatch" ||
				fixture.client.metaPutCount() != metaWrites || found || meta != nil ||
				get.Code != http.StatusNotFound || head.Code != http.StatusNotFound || fixture.forwards.Load() != 0 {
				t.Fatalf("mismatched new-key PUT was not rejected before publication: PUT=%d %q metadata=(%+v, found=%v) meta commits=%d->%d GET=%d %q HEAD=%d upstream=%d",
					w.Code, w.Body.String(), meta, found, metaWrites, fixture.client.metaPutCount(), get.Code, get.Body.String(), head.Code, fixture.forwards.Load())
			}
			requirePayloadHashBodyAbsentAfterRejection(t, fixture, bodyPuts, bodyDeletes)
			requireEnginePutCapacityReleased(t, fixture.service)

			// A concrete digest does not replace the stream's existing
			// incomplete-body response when a client stops mid-transfer.
			expectedBody := []byte("complete body")
			partialBody := expectedBody[:len(expectedBody)-2]
			r := fixture.signedRequest(t, http.MethodPut, "truncated", partialBody, payloadSHA256(expectedBody), "")
			r.ContentLength = int64(len(expectedBody))
			r.Body = io.NopCloser(&truncatedReader{data: partialBody})
			result, _, _, err := fixture.forwarder.ValidateAndGetCredentials(r)
			if err != nil || result != AuthValidated {
				t.Fatalf("truncated signed PUT did not pass %T authentication: result=%v err=%v", fixture.forwarder, result, err)
			}
			metaWrites = fixture.client.metaPutCount()
			bodyPuts, bodyDeletes = fixture.client.bodyCounts()
			w = httptest.NewRecorder()
			if err := fixture.service.HandlePutObject(w, r); err != nil {
				t.Fatalf("truncated PUT: %v", err)
			}
			meta, found, err = fixture.cache.GetMeta(context.Background(), payloadHashTestBucket, "truncated")
			if err != nil || w.Code != http.StatusBadRequest || tieredPayloadHashErrorCode(t, w) != s3err.ErrIncompleteBody ||
				fixture.client.metaPutCount() != metaWrites || found || meta != nil {
				t.Fatalf("truncated concrete-hash PUT = %d %q metadata=(%+v, found=%v) meta commits=%d->%d, want IncompleteBody before metadata",
					w.Code, w.Body.String(), meta, found, metaWrites, fixture.client.metaPutCount())
			}
			requirePayloadHashBodyAbsentAfterRejection(t, fixture, bodyPuts, bodyDeletes)
			requireEnginePutCapacityReleased(t, fixture.service)

			// Seed a previous version with a valid independently signed digest,
			// then prove a mismatched overwrite leaves both its meta and bytes.
			oldBody := []byte("previous bytes")
			seed := fixture.do(t, http.MethodPut, "overwrite-key", oldBody, payloadSHA256(oldBody), "")
			if seed.Code != http.StatusOK {
				t.Fatalf("seed overwrite target = %d %q, want 200", seed.Code, seed.Body.String())
			}
			oldMeta, found, err := fixture.cache.GetMeta(context.Background(), payloadHashTestBucket, "overwrite-key")
			if err != nil || !found || oldMeta == nil {
				t.Fatalf("seed metadata = %+v, found=%v err=%v", oldMeta, found, err)
			}
			metaWrites = fixture.client.metaPutCount()
			bodyPuts, bodyDeletes = fixture.client.bodyCounts()
			w = fixture.do(t, http.MethodPut, "overwrite-key", mutatedBody, payloadSHA256(trustedBody), "", http.Header{"If-Match": []string{oldMeta.ETag}})
			newMeta, found, err := fixture.cache.GetMeta(context.Background(), payloadHashTestBucket, "overwrite-key")
			if err != nil || !found || newMeta == nil {
				t.Fatalf("read overwrite metadata after rejection: meta=%+v found=%v err=%v", newMeta, found, err)
			}
			get = fixture.do(t, http.MethodGet, "overwrite-key", nil, "UNSIGNED-PAYLOAD", "")
			head = fixture.do(t, http.MethodHead, "overwrite-key", nil, "UNSIGNED-PAYLOAD", "")
			if w.Code != http.StatusBadRequest || tieredPayloadHashErrorCode(t, w) != "XAmzContentSHA256Mismatch" ||
				fixture.client.metaPutCount() != metaWrites || !reflect.DeepEqual(newMeta, oldMeta) ||
				get.Code != http.StatusOK || get.Body.String() != string(oldBody) ||
				head.Code != http.StatusOK || head.Result().Header.Get("ETag") != oldMeta.ETag ||
				head.Result().Header.Get("Content-Length") != fmt.Sprint(len(oldBody)) {
				t.Fatalf("mismatched overwrite changed published state: PUT=%d %q meta commits=%d->%d old=%+v new=%+v GET=%d %q HEAD=%d headers=%v",
					w.Code, w.Body.String(), metaWrites, fixture.client.metaPutCount(), oldMeta, newMeta, get.Code, get.Body.String(), head.Code, head.Result().Header)
			}
			requirePayloadHashBodyAbsentAfterRejection(t, fixture, bodyPuts, bodyDeletes)
			requireEnginePutCapacityReleased(t, fixture.service)

			// A matching concrete digest and an unsigned payload remain valid.
			validBody := []byte("valid hashed body")
			if w := fixture.do(t, http.MethodPut, "valid-hash", validBody, payloadSHA256(validBody), ""); w.Code != http.StatusOK {
				t.Fatalf("matching hashed PUT = %d %q, want 200", w.Code, w.Body.String())
			}
			if w := fixture.do(t, http.MethodGet, "valid-hash", nil, "UNSIGNED-PAYLOAD", ""); w.Code != http.StatusOK || w.Body.String() != string(validBody) {
				t.Fatalf("GET of valid hashed PUT = %d %q", w.Code, w.Body.String())
			}

			unsignedBody := []byte("unsigned body")
			if w := fixture.do(t, http.MethodPut, "unsigned", unsignedBody, "UNSIGNED-PAYLOAD", ""); w.Code != http.StatusOK {
				t.Fatalf("UNSIGNED-PAYLOAD PUT = %d %q, want 200", w.Code, w.Body.String())
			}
			if w := fixture.do(t, http.MethodGet, "unsigned", nil, "UNSIGNED-PAYLOAD", ""); w.Code != http.StatusOK || w.Body.String() != string(unsignedBody) {
				t.Fatalf("GET of unsigned PUT = %d %q", w.Code, w.Body.String())
			}

			// Content-MD5 keeps its existing failure precedence over the new
			// SHA-256 check, and a failed digest also cleans up its staged body.
			md5Body := []byte("content md5 control")
			wrongMD5 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, md5.Size))
			bodyPuts, bodyDeletes = fixture.client.bodyCounts()
			metaWrites = fixture.client.metaPutCount()
			w = fixture.do(t, http.MethodPut, "bad-md5", md5Body, payloadSHA256(md5Body), wrongMD5)
			if w.Code != http.StatusBadRequest || tieredPayloadHashErrorCode(t, w) != s3err.ErrBadDigest ||
				fixture.client.metaPutCount() != metaWrites {
				t.Fatalf("wrong Content-MD5 result = %d %q; meta commits %d->%d, want 400 BadDigest before metadata", w.Code, w.Body.String(), metaWrites, fixture.client.metaPutCount())
			}
			requirePayloadHashBodyAbsentAfterRejection(t, fixture, bodyPuts, bodyDeletes)
			requireEnginePutCapacityReleased(t, fixture.service)

			if got := fixture.forwards.Load(); got != 0 {
				t.Fatalf("upstream requests = %d, want zero for locally validated small PUTs and reads", got)
			}
		})
	}
}
