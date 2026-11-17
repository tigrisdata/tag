package handlers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

type conditionBodyReader struct {
	io.Reader
	n int
}

func (r *conditionBodyReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.n += n
	return n, err
}

type conditionCacheClient struct {
	cacheclient.CacheClient
	used atomic.Int64
	race func()
}

func (c *conditionCacheClient) DiskUsageBytes() int64 { return c.used.Load() }

func (c *conditionCacheClient) PutIfVersion(ctx context.Context, key string, data []byte, ttl int64, version uint64) (uint64, error) {
	if strings.HasPrefix(key, "meta|") && c.race != nil {
		race := c.race
		c.race = nil
		race()
	}
	return c.CacheClient.PutIfVersion(ctx, key, data, ttl, version)
}

type tieredConditionFixture struct {
	router    http.Handler
	store     *cache.Cache
	client    *conditionCacheClient
	forwarder proxy.RequestForwarder
	forwards  atomic.Int64
}

func newTieredConditionFixture(t *testing.T, transparent bool) *tieredConditionFixture {
	t.Helper()
	f := &tieredConditionFixture{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.forwards.Add(1)
		w.WriteHeader(http.StatusPreconditionFailed)
	}))
	t.Cleanup(upstream.Close)
	cfg := config.NewDefault()
	cfg.Mode = config.ModeTiered
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SetLegacyCoordination(false)
	cfg.Cache.SizeThreshold = 1024
	cfg.Cache.MaxDiskUsageBytes = 1000
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signingMetadataAccessKey, signingMetadataSecretKey)
	var localAuth *proxy.LocalAuthConfig
	var proxySigner *auth.ProxySigner
	if transparent {
		keys := auth.NewDerivedKeyStore(auth.DefaultDerivedKeyTTL)
		date := time.Now().UTC().Format("20060102")
		key, err := credentials.GetSigningKey(signingMetadataAccessKey, date, signingMetadataRegion)
		if err != nil {
			t.Fatal(err)
		}
		keys.Store(signingMetadataAccessKey, date, signingMetadataRegion, key)
		authz := auth.NewAuthzCache(auth.DefaultAuthzCacheTTL)
		authz.Grant(signingMetadataAccessKey, signingMetadataBucket)
		localAuth = &proxy.LocalAuthConfig{DerivedKeyStore: keys, Validator: auth.NewRequestValidator(keys), AuthzCache: authz}
		proxySigner = auth.NewProxySigner("proxy-access-key", "proxy-secret-key")
	}
	f.forwarder = proxy.NewForwarder(credentials, upstream.URL, signingMetadataRegion, 10, proxySigner, localAuth)
	f.client = &conditionCacheClient{CacheClient: cacheclient.NewMemoryCache()}
	f.store = cache.NewCacheWithClient(f.client, &cfg.Cache)
	service := proxy.NewService(f.forwarder, f.store, cfg)
	t.Cleanup(service.Stop)
	f.router = NewServer(service, "127.0.0.1", 0, false, 0).Router()
	return f
}

func (f *tieredConditionFixture) request(t *testing.T, method, key, body, header string, values []string) (*httptest.ResponseRecorder, int) {
	t.Helper()
	r := httptest.NewRequest(method, "http://s3.test/"+signingMetadataBucket+"/"+key, strings.NewReader(body))
	if method == http.MethodPut {
		r.Header.Set("Content-Length", strconv.Itoa(len(body)))
	}
	for _, value := range values {
		r.Header.Add(header, value)
	}
	hash := sha256Hex([]byte(body))
	r.Header.Set("X-Amz-Content-Sha256", hash)
	if err := v4.NewSigner().SignHTTP(t.Context(), aws.Credentials{AccessKeyID: signingMetadataAccessKey, SecretAccessKey: signingMetadataSecretKey}, r, hash, "s3", signingMetadataRegion, time.Now().UTC()); err != nil {
		t.Fatalf("sign request: %v", err)
	}
	if result, _, _, err := f.forwarder.ValidateAndGetCredentials(r); err != nil || result != proxy.AuthValidated {
		t.Fatalf("validate signed request: result=%v err=%v", result, err)
	}
	reader := &conditionBodyReader{Reader: strings.NewReader(body)}
	r.Body = io.NopCloser(reader)
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	return w, reader.n
}

func (f *tieredConditionFixture) seed(t *testing.T, key string) (string, uint64) {
	t.Helper()
	w, _ := f.request(t, http.MethodPut, key, "old body", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("seed PUT = %d: %s", w.Code, w.Body.String())
	}
	meta, version, found, err := f.store.GetMetaWithVersion(t.Context(), signingMetadataBucket, key)
	if err != nil || !found || meta == nil || meta.BodyUpstream {
		t.Fatalf("seed local metadata: found=%v err=%v meta=%+v", found, err, meta)
	}
	return meta.ETag, version
}

func TestTieredPutRepeatedConditions(t *testing.T) {
	for _, flavor := range []string{"signing", "transparent"} {
		t.Run(flavor, func(t *testing.T) {
			f := newTieredConditionFixture(t, flavor == "transparent")
			for _, tc := range []struct {
				name, header, leading string
				weak                  bool
				accept                bool
			}{
				{name: "match later", header: "If-Match", leading: `"different"`, accept: true},
				{name: "none match later", header: "If-None-Match", leading: `"different"`},
				{name: "match empty first", header: "If-Match", accept: true},
				{name: "none match empty first", header: "If-None-Match"},
				{name: "weak match later", header: "If-Match", leading: `"different"`, weak: true},
				{name: "weak none match later", header: "If-None-Match", leading: `"different"`, weak: true},
				{name: "match after quoted comma", header: "If-Match", leading: `"other,tag"`, accept: true},
				{name: "none match after quoted comma", header: "If-None-Match", leading: `"other,tag"`},
			} {
				t.Run(tc.name, func(t *testing.T) {
					for _, form := range []string{"joined", "repeated"} {
						t.Run(form, func(t *testing.T) {
							key := strings.ReplaceAll(tc.name, " ", "-") + "-" + form
							etag, version := f.seed(t, key)
							if tc.weak {
								etag = "W/" + etag
							}
							values := []string{tc.leading, etag}
							if form == "joined" {
								values = []string{strings.Join(values, ",")}
							}
							w, read := f.request(t, http.MethodPut, key, "new body", tc.header, values)
							meta, nextVersion, found, err := f.store.GetMetaWithVersion(t.Context(), signingMetadataBucket, key)
							if err != nil || !found || meta == nil {
								t.Fatalf("read metadata after PUT: found=%v err=%v", found, err)
							}
							get, _ := f.request(t, http.MethodGet, key, "", "", nil)
							wantStatus, wantRead, wantBody := http.StatusPreconditionFailed, 0, "old body"
							if tc.accept {
								wantStatus, wantRead, wantBody = http.StatusOK, len("new body"), "new body"
							}
							if w.Code != wantStatus || read != wantRead || (nextVersion != version) != tc.accept || meta.BodyUpstream || get.Code != http.StatusOK || get.Body.String() != wantBody || f.forwards.Load() != 0 {
								t.Errorf("conditional PUT: status=%d body bytes read=%d metadata changed=%v upstream tier=%v forwards=%d GET=(%d, %q); want status=%d read=%d changed=%v local GET=(200, %q); response=%s", w.Code, read, nextVersion != version, meta.BodyUpstream, f.forwards.Load(), get.Code, get.Body.String(), wantStatus, wantRead, tc.accept, wantBody, w.Body.String())
							}
						})
					}
				})
			}
		})
	}
}

func TestTieredPutRepeatedConditionRouting(t *testing.T) {
	for _, header := range []string{"If-Match", "If-None-Match"} {
		for _, prior := range []string{"over budget local", "upstream marker"} {
			t.Run(header+"/"+prior, func(t *testing.T) {
				f := newTieredConditionFixture(t, true)
				etag, version := f.seed(t, "object")
				if prior == "upstream marker" {
					meta, found, err := f.store.GetMeta(t.Context(), signingMetadataBucket, "object")
					if err != nil || !found {
						t.Fatalf("read marker seed: found=%v err=%v", found, err)
					}
					meta.BodyUpstream = true
					if wrote, err := f.store.PutMetaIfVersion(t.Context(), signingMetadataBucket, "object", meta, 60, version); err != nil || !wrote {
						t.Fatalf("seed marker: wrote=%v err=%v", wrote, err)
					}
					_, version, _, err = f.store.GetMetaWithVersion(t.Context(), signingMetadataBucket, "object")
					if err != nil {
						t.Fatal(err)
					}
				} else {
					f.client.used.Store(1100)
				}
				// An empty first field must not hide a later condition from the
				// disk-shed exemption or the upstream-marker routing decision.
				if header == "If-Match" {
					etag = `"different"`
				}
				w, read := f.request(t, http.MethodPut, "object", "new body", header, []string{"", etag})
				meta, nextVersion, found, err := f.store.GetMetaWithVersion(t.Context(), signingMetadataBucket, "object")
				if err != nil || !found || meta == nil {
					t.Fatalf("read metadata after PUT: found=%v err=%v", found, err)
				}
				wantForwards := int64(0)
				if prior == "upstream marker" {
					wantForwards = 1
				}
				if w.Code != http.StatusPreconditionFailed || nextVersion != version || f.forwards.Load() != wantForwards || (prior == "over budget local" && read != 0) {
					t.Errorf("conditional routing: status=%d read=%d version=%d->%d forwards=%d; want 412, unchanged metadata and %d forwards", w.Code, read, version, nextVersion, f.forwards.Load(), wantForwards)
				}
			})
		}
	}
}

func TestTieredPutRepeatedWildcardExistence(t *testing.T) {
	for _, header := range []string{"If-Match", "If-None-Match"} {
		for _, state := range []string{"live", "missing", "missing body"} {
			t.Run(header+"/"+state, func(t *testing.T) {
				f := newTieredConditionFixture(t, true)
				if state != "missing" {
					f.seed(t, "object")
				}
				meta, version, _, err := f.store.GetMetaWithVersion(t.Context(), signingMetadataBucket, "object")
				if err != nil {
					t.Fatal(err)
				}
				if state == "missing body" {
					if err := f.store.DeleteBody(t.Context(), signingMetadataBucket, "object", meta.BodyDiscriminator()); err != nil {
						t.Fatal(err)
					}
				}
				w, read := f.request(t, http.MethodPut, "object", "new body", header, []string{"", "*"})
				_, nextVersion, found, err := f.store.GetMetaWithVersion(t.Context(), signingMetadataBucket, "object")
				if err != nil {
					t.Fatal(err)
				}
				wantStatus := http.StatusOK
				if header == "If-Match" && state != "live" {
					wantStatus = http.StatusNotFound
				} else if header == "If-None-Match" && state == "live" {
					wantStatus = http.StatusPreconditionFailed
				}
				wantRead := 0
				if wantStatus == http.StatusOK {
					wantRead = len("new body")
				}
				changed := nextVersion != version
				if state == "missing" {
					// Absence tokens need not be stable across reads; publication
					// of a row, not a different absence token, changes this state.
					changed = found
				}
				if w.Code != wantStatus || read != wantRead || changed != (wantStatus == http.StatusOK) {
					t.Errorf("wildcard PUT: status=%d read=%d metadata changed=%v; want status=%d read=%d changed=%v", w.Code, read, changed, wantStatus, wantRead, wantStatus == http.StatusOK)
				}
				get, _ := f.request(t, http.MethodGet, "object", "", "", nil)
				if wantStatus == http.StatusOK && (get.Code != http.StatusOK || get.Body.String() != "new body") {
					t.Errorf("GET after healing PUT = (%d, %q), want (200, new body)", get.Code, get.Body.String())
				}
			})
		}
	}
}

func TestTieredPutRepeatedConditionRacedCommit(t *testing.T) {
	f := newTieredConditionFixture(t, true)
	etag, _ := f.seed(t, "object")
	var racingVersion uint64
	// Run another writer at the metadata CAS boundary, after the conditional
	// request's decision and body stream but before its publication.
	f.client.race = func() {
		w, _ := f.request(t, http.MethodPut, "object", "racing body", "", nil)
		if w.Code != http.StatusOK {
			t.Fatalf("racing PUT = %d: %s", w.Code, w.Body.String())
		}
		_, version, found, err := f.store.GetMetaWithVersion(t.Context(), signingMetadataBucket, "object")
		if err != nil || !found {
			t.Fatalf("racing metadata: found=%v err=%v", found, err)
		}
		racingVersion = version
	}
	w, read := f.request(t, http.MethodPut, "object", "new body", "If-Match", []string{"", etag})
	_, version, found, err := f.store.GetMetaWithVersion(t.Context(), signingMetadataBucket, "object")
	if err != nil || !found {
		t.Fatalf("metadata after raced PUT: found=%v err=%v", found, err)
	}
	get, _ := f.request(t, http.MethodGet, "object", "", "", nil)
	if w.Code != http.StatusPreconditionFailed || read != len("new body") || racingVersion == 0 || version != racingVersion || get.Code != http.StatusOK || get.Body.String() != "racing body" {
		t.Errorf("raced conditional PUT: status=%d read=%d racing version=%d final version=%d GET=(%d, %q); want 412 after streaming, racing metadata and bytes preserved", w.Code, read, racingVersion, version, get.Code, get.Body.String())
	}
}
