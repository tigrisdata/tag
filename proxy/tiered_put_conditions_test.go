package proxy

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
)

// tieredConditionalOrigin evaluates conditions against its own objects, not
// TAG's local tier. It also validates the forwarded caller's signature.
type tieredConditionalOrigin struct {
	mu             sync.Mutex
	objects        map[string]string
	puts           int
	lastConditions http.Header
}

func (o *tieredConditionalOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(payloadHashTestAccessKey, payloadHashTestSecretKey)
	check := r.Clone(r.Context())
	if host := r.Header.Get("X-Tigris-Forwarded-Host"); host != "" {
		check.Host = host
		check.Header.Set("Host", host)
	}
	if _, err := auth.NewRequestValidator(credentials).Validate(check); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	body, exists := o.objects[r.URL.Path]
	etag := fmt.Sprintf(`"%x"`, md5.Sum([]byte(body)))
	switch r.Method {
	case http.MethodPut:
		o.puts++
		o.lastConditions = http.Header{"If-Match": {r.Header.Get("If-Match")}, "If-None-Match": {r.Header.Get("If-None-Match")}}
		if im := r.Header.Get("If-Match"); im != "" && (!exists || (im != "*" && im != etag)) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		if inm := r.Header.Get("If-None-Match"); exists && (inm == "*" || inm == etag) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		o.objects[r.URL.Path] = string(data)
		w.Header().Set("ETag", fmt.Sprintf(`"%x"`, md5.Sum(data)))
	case http.MethodGet:
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", etag)
		_, _ = io.WriteString(w, body)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func TestTieredLargePutLocalConditions(t *testing.T) {
	for _, flavor := range []payloadHashTestFlavor{payloadHashSigningFlavor, payloadHashTransparentFlavor} {
		for _, condition := range []string{"If-None-Match", "If-Match"} {
			origin := &tieredConditionalOrigin{objects: make(map[string]string)}
			fixture := newTieredPayloadHashFixture(t, flavor, origin)
			old := []byte("local only")
			seed := fixture.do(t, http.MethodPut, "object", old, payloadSHA256(old), "")
			if seed.Code != http.StatusOK {
				t.Fatalf("seed local object: %d %s", seed.Code, seed.Body.String())
			}
			prior, version, found, err := fixture.cache.GetMetaWithVersion(context.Background(), payloadHashTestBucket, "object")
			if err != nil || !found || prior == nil || prior.BodyUpstream {
				t.Fatalf("seed metadata: %+v found=%v err=%v", prior, found, err)
			}
			value := "*"
			if condition == "If-Match" {
				value = prior.ETag
			}
			body := []byte(strings.Repeat("replacement", 128))
			result := fixture.do(t, http.MethodPut, "object", body, payloadSHA256(body), "", http.Header{condition: {value}})
			current, currentVersion, found, err := fixture.cache.GetMetaWithVersion(context.Background(), payloadHashTestBucket, "object")
			if err != nil {
				t.Fatalf("read current metadata: %v", err)
			}
			get := fixture.do(t, http.MethodGet, "object", nil, "UNSIGNED-PAYLOAD", "")
			origin.mu.Lock()
			puts := origin.puts
			origin.mu.Unlock()
			if condition == "If-None-Match" {
				if result.Code != http.StatusPreconditionFailed || puts != 0 || !found || current == nil || currentVersion != version || get.Code != http.StatusOK || get.Body.String() != string(old) {
					t.Errorf("conditional rejection (%s) = %d, origin PUTs=%d, version=%d->%d, meta=%+v, GET=%d %q; want 412 without forwarding or changing the local object", flavor, result.Code, puts, version, currentVersion, current, get.Code, get.Body.String())
				}
			} else if result.Code != http.StatusOK || puts != 1 || !found || current == nil || !current.BodyUpstream || get.Code != http.StatusOK || get.Body.String() != string(body) {
				t.Errorf("conditional replacement (%s) = %d %s, origin PUTs=%d, meta=%+v, GET=%d %q; want a readable upstream replacement after matching the local ETag", flavor, result.Code, result.Body.String(), puts, current, get.Code, get.Body.String())
			}
		}
	}
}

// The same precondition semantics apply at either side of the size boundary.
// An upstream marker delegates to the origin, even for a small replacement.
func TestTieredPutConditionRouting(t *testing.T) {
	for _, flavor := range []payloadHashTestFlavor{payloadHashSigningFlavor, payloadHashTransparentFlavor} {
		for _, large := range []bool{false, true} {
			for _, tc := range []struct {
				name, prior, condition, value string
				status                        int
			}{
				{"wrong local match", "local", "If-Match", `"wrong"`, 412},
				{"weak local match", "local", "If-Match", "W/@etag", 412},
				{"local match list", "local", "If-Match", `"other", @etag`, 200},
				{"local wildcard match", "local", "If-Match", "*", 200},
				{"local none match", "local", "If-None-Match", "@etag", 412},
				{"local different none match", "local", "If-None-Match", `"different"`, 200},
				{"absent match", "absent", "If-Match", "*", 404},
				{"absent create", "absent", "If-None-Match", "*", 200},
				{"missing body match", "unservable", "If-Match", "*", 404},
				{"missing body create", "unservable", "If-None-Match", "*", 200},
				{"upstream create", "upstream", "If-None-Match", "*", 412},
				{"upstream match", "upstream", "If-Match", "@etag", 200},
				{"unconditional transition", "local", "", "", 200},
			} {
				t.Run(fmt.Sprintf("%s/large=%v/%s", flavor, large, tc.name), func(t *testing.T) {
					origin := &tieredConditionalOrigin{objects: make(map[string]string)}
					f := newTieredPayloadHashFixture(t, flavor, origin)
					old := []byte("local only")
					if tc.prior == "upstream" {
						old = bytes.Repeat([]byte("u"), 1408)
					}
					if tc.prior != "absent" {
						if w := f.do(t, http.MethodPut, "object", old, payloadSHA256(old), ""); w.Code != 200 {
							t.Fatalf("seed: %d %s", w.Code, w.Body.String())
						}
					}
					prior, version, found, err := f.cache.GetMetaWithVersion(t.Context(), payloadHashTestBucket, "object")
					if err != nil || found != (tc.prior != "absent") {
						t.Fatalf("prior: found=%v err=%v", found, err)
					}
					etag := ""
					if prior != nil {
						etag = prior.ETag
					}
					if tc.prior == "unservable" {
						if err := f.cache.DeleteBody(t.Context(), payloadHashTestBucket, "object", prior.BodyDiscriminator()); err != nil {
							t.Fatal(err)
						}
					}
					origin.mu.Lock()
					putsBefore := origin.puts
					origin.mu.Unlock()
					body := []byte("next")
					if large {
						body = bytes.Repeat([]byte("n"), 1408)
					}
					headers := make(http.Header)
					if tc.condition != "" {
						headers.Set(tc.condition, strings.ReplaceAll(tc.value, "@etag", etag))
					}
					w := f.do(t, http.MethodPut, "object", body, payloadSHA256(body), "", headers)
					if w.Code != tc.status {
						t.Fatalf("PUT = %d %s, want %d", w.Code, w.Body.String(), tc.status)
					}
					current, currentVersion, currentFound, err := f.cache.GetMetaWithVersion(t.Context(), payloadHashTestBucket, "object")
					if err != nil {
						t.Fatal(err)
					}
					forwarded := tc.prior == "upstream" || (large && tc.status == 200)
					origin.mu.Lock()
					puts, conditions := origin.puts-putsBefore, origin.lastConditions.Clone()
					origin.mu.Unlock()
					wantPuts := 0
					if forwarded {
						wantPuts = 1
					}
					if puts != wantPuts {
						t.Fatalf("origin PUTs = %d, want %d", puts, wantPuts)
					}
					if forwarded {
						expected := make(http.Header)
						for _, name := range []string{"If-Match", "If-None-Match"} {
							value := ""
							if tc.prior == "upstream" {
								value = headers.Get(name)
							}
							expected.Set(name, value)
						}
						if !reflect.DeepEqual(conditions, expected) {
							t.Errorf("forwarded conditions = %v, want %v", conditions, expected)
						}
					}
					if tc.status != 200 {
						if (found && currentVersion != version) || currentFound != found || !reflect.DeepEqual(current, prior) {
							t.Errorf("rejected PUT changed metadata: prior=%+v/%d current=%+v/%d", prior, version, current, currentVersion)
						}
					} else if !currentFound || current == nil || current.BodyUpstream != forwarded {
						t.Fatalf("replacement metadata = %+v", current)
					}
					if tc.prior != "unservable" || tc.status == 200 {
						get := f.do(t, http.MethodGet, "object", nil, "UNSIGNED-PAYLOAD", "")
						wantBody, wantStatus := string(old), 200
						if tc.status == 200 {
							wantBody = string(body)
						} else if tc.prior == "absent" {
							wantBody, wantStatus = "", 404
						}
						if get.Code != wantStatus || (wantStatus == 200 && get.Body.String() != wantBody) {
							t.Errorf("GET = %d %q, want %d %q", get.Code, get.Body.String(), wantStatus, wantBody)
						}
					}
				})
			}
		}
	}
}

// Hold the decision-time read while another writer publishes a new local body.
// The forward must not re-read and borrow that writer's publication token.
func TestTieredLargeConditionalPutPreservesRacingLocalWrite(t *testing.T) {
	for _, flavor := range []payloadHashTestFlavor{payloadHashSigningFlavor, payloadHashTransparentFlavor} {
		for _, present := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/present=%v", flavor, present), func(t *testing.T) {
				origin := &tieredConditionalOrigin{objects: make(map[string]string)}
				f := newTieredPayloadHashFixture(t, flavor, origin)
				if present {
					f.do(t, http.MethodPut, "object", []byte("prior"), payloadSHA256([]byte("prior")), "")
				}
				gate := &tieredMetaReadGateClient{CacheClient: f.client.CacheClient}
				f.client.bodyKeyLedger.CacheClient = gate
				observed, release := gate.gateNextMetaRead(cache.MakeMetaKey(payloadHashTestBucket, "object"))
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				t.Cleanup(unblock)
				condition := "If-None-Match"
				if present {
					condition = "If-Match"
				}
				body := bytes.Repeat([]byte("l"), 1408)
				r := f.signedRequest(t, http.MethodPut, "object", body, payloadSHA256(body), "", http.Header{condition: {"*"}})
				done := make(chan error, 1)
				w := httptest.NewRecorder()
				go func() { done <- f.service.HandlePutObject(w, r) }()
				select {
				case snapshot := <-observed:
					if snapshot.err != nil || snapshot.found != present {
						t.Fatalf("snapshot: %+v", snapshot)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("PUT did not reach decision-time read")
				}
				racer := []byte("new local body")
				if w := f.do(t, http.MethodPut, "object", racer, payloadSHA256(racer), ""); w.Code != 200 {
					t.Fatalf("racing write = %d", w.Code)
				}
				prior, version, _, err := f.cache.GetMetaWithVersion(t.Context(), payloadHashTestBucket, "object")
				if err != nil {
					t.Fatal(err)
				}
				unblock()
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("forward did not finish")
				}
				current, currentVersion, found, err := f.cache.GetMetaWithVersion(t.Context(), payloadHashTestBucket, "object")
				get := f.do(t, http.MethodGet, "object", nil, "UNSIGNED-PAYLOAD", "")
				if w.Code != 200 || err != nil || !found || currentVersion != version || !reflect.DeepEqual(prior, current) || get.Code != 200 || get.Body.String() != string(racer) {
					t.Fatalf("racing local write lost: PUT=%d metadata=%+v/%d -> %+v/%d found=%v err=%v GET=%d %q", w.Code, prior, version, current, currentVersion, found, err, get.Code, get.Body.String())
				}
			})
		}
	}
}

type tieredPutReadFailureClient struct {
	cacheclient.CacheClient
	meta, body bool
}

func (c *tieredPutReadFailureClient) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	if c.meta && strings.HasPrefix(key, "meta|") {
		return nil, 0, false, errors.New("metadata unavailable")
	}
	return c.CacheClient.GetWithVersion(ctx, key)
}

func (c *tieredPutReadFailureClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if c.body {
		return errors.New("body probe unavailable")
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

func TestTieredLargeConditionalPutReadFailures(t *testing.T) {
	for _, flavor := range []payloadHashTestFlavor{payloadHashSigningFlavor, payloadHashTransparentFlavor} {
		for _, metaFailure := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/metadata=%v", flavor, metaFailure), func(t *testing.T) {
				origin := &tieredConditionalOrigin{objects: make(map[string]string)}
				f := newTieredPayloadHashFixture(t, flavor, origin)
				old := []byte("local only")
				f.do(t, http.MethodPut, "object", old, payloadSHA256(old), "")
				prior, version, _, err := f.cache.GetMetaWithVersion(t.Context(), payloadHashTestBucket, "object")
				if err != nil {
					t.Fatal(err)
				}
				failure := &tieredPutReadFailureClient{CacheClient: f.client.CacheClient, meta: metaFailure, body: !metaFailure}
				f.client.bodyKeyLedger.CacheClient = failure
				body := bytes.Repeat([]byte("l"), 1408)
				r := f.signedRequest(t, http.MethodPut, "object", body, payloadSHA256(body), "", http.Header{"If-None-Match": {"*"}})
				w := httptest.NewRecorder()
				err = f.service.HandlePutObject(w, r)
				if err == nil {
					t.Fatal("unreadable prior did not return a retryable error")
				}
				failure.meta, failure.body = false, false
				current, currentVersion, found, err := f.cache.GetMetaWithVersion(t.Context(), payloadHashTestBucket, "object")
				get := f.do(t, http.MethodGet, "object", nil, "UNSIGNED-PAYLOAD", "")
				origin.mu.Lock()
				puts := origin.puts
				origin.mu.Unlock()
				if err != nil || !found || currentVersion != version || !reflect.DeepEqual(current, prior) || puts != 0 || get.Code != 200 || get.Body.String() != string(old) {
					t.Fatalf("failed condition lookup changed prior: meta=%+v version=%d origin PUTs=%d GET=%d %q err=%v", current, currentVersion, puts, get.Code, get.Body.String(), err)
				}
			})
		}
	}
}

func TestTieredLargeConditionalPutSignedChunks(t *testing.T) {
	for _, flavor := range []payloadHashTestFlavor{payloadHashSigningFlavor, payloadHashTransparentFlavor} {
		for _, tamper := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/tamper=%v", flavor, tamper), func(t *testing.T) {
				origin := &tieredConditionalOrigin{objects: make(map[string]string)}
				f := newTieredPayloadHashFixture(t, flavor, origin)
				old := []byte("local only")
				f.do(t, http.MethodPut, "object", old, payloadSHA256(old), "")
				prior, version, _, err := f.cache.GetMetaWithVersion(t.Context(), payloadHashTestBucket, "object")
				if err != nil {
					t.Fatal(err)
				}
				body := bytes.Repeat([]byte("l"), 1408)
				r := f.signedRequest(t, http.MethodPut, "object", nil, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "", http.Header{"If-Match": {prior.ETag}, "Content-Encoding": {"aws-chunked"}, "X-Amz-Decoded-Content-Length": {fmt.Sprint(len(body))}})
				info, err := auth.ParseAuthInfo(r)
				if err != nil {
					t.Fatal(err)
				}
				seed, err := hex.DecodeString(info.Signature)
				if err != nil {
					t.Fatal(err)
				}
				stream := v4.NewStreamSigner(aws.Credentials{AccessKeyID: payloadHashTestAccessKey, SecretAccessKey: payloadHashTestSecretKey}, "s3", payloadHashTestRegion, seed)
				var wire bytes.Buffer
				for _, chunk := range [][]byte{body, nil} {
					signature, err := stream.GetSignature(t.Context(), nil, chunk, f.signingTime)
					if err != nil {
						t.Fatal(err)
					}
					fmt.Fprintf(&wire, "%x;chunk-signature=%x\r\n", len(chunk), signature)
					wire.Write(chunk)
					wire.WriteString("\r\n")
				}
				framed := wire.Bytes()
				if tamper {
					framed = flipByte(t, framed, body)
				}
				r.Body = io.NopCloser(bytes.NewReader(framed))
				r.ContentLength = int64(len(framed))
				headers := r.Header.Clone()
				w := httptest.NewRecorder()
				err = f.service.HandlePutObject(w, r)
				current, currentVersion, found, readErr := f.cache.GetMetaWithVersion(t.Context(), payloadHashTestBucket, "object")
				if readErr != nil || !found || current == nil {
					t.Fatalf("metadata after stream: %+v %v", current, readErr)
				}
				if !reflect.DeepEqual(r.Header, headers) {
					t.Error("forwarder mutated the signed client headers")
				}
				if tamper {
					if authErr, ok := IsAuthError(err); !ok || authErr.Code != ErrCodeSignatureMismatch {
						t.Fatalf("tampered signed chunk error = %v, want signature mismatch", err)
					}
					if currentVersion != version || !reflect.DeepEqual(current, prior) {
						t.Fatal("failed stream replaced local metadata")
					}
				} else if err != nil || w.Code != 200 || !current.BodyUpstream {
					t.Fatalf("valid stream: err=%v status=%d metadata=%+v", err, w.Code, current)
				}
				get := f.do(t, http.MethodGet, "object", nil, "UNSIGNED-PAYLOAD", "")
				want := body
				if tamper {
					want = old
				}
				if get.Code != 200 || get.Body.String() != string(want) {
					t.Fatalf("GET after stream = %d %q, want %q", get.Code, get.Body.String(), want)
				}
			})
		}
	}
}

func TestTieredLargeConditionalPutPresigned(t *testing.T) {
	for _, flavor := range []payloadHashTestFlavor{payloadHashSigningFlavor, payloadHashTransparentFlavor} {
		t.Run(string(flavor), func(t *testing.T) {
			origin := &tieredConditionalOrigin{objects: make(map[string]string)}
			f := newTieredPayloadHashFixture(t, flavor, origin)
			old := []byte("local only")
			if w := f.do(t, http.MethodPut, "object", old, payloadSHA256(old), ""); w.Code != 200 {
				t.Fatalf("seed: %d", w.Code)
			}
			body := bytes.Repeat([]byte("l"), 1408)
			r := httptest.NewRequest(http.MethodPut, "http://s3.test/"+payloadHashTestBucket+"/object?X-Amz-Expires=60", bytes.NewReader(body))
			r.Header.Set("If-Match", "*")
			r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			r.ContentLength = -1
			uri, _, err := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableHeaderHoisting = true }).PresignHTTP(t.Context(), aws.Credentials{AccessKeyID: payloadHashTestAccessKey, SecretAccessKey: payloadHashTestSecretKey}, r, "UNSIGNED-PAYLOAD", "s3", payloadHashTestRegion, f.signingTime)
			if err != nil {
				t.Fatal(err)
			}
			r.URL, err = url.Parse(uri)
			if err != nil {
				t.Fatal(err)
			}
			r.ContentLength = int64(len(body))
			originalURL, originalHeaders := r.URL.String(), r.Header.Clone()
			w := httptest.NewRecorder()
			if err := f.service.HandlePutObject(w, r); err != nil {
				t.Fatal(err)
			}
			get := f.do(t, http.MethodGet, "object", nil, "UNSIGNED-PAYLOAD", "")
			if w.Code != 200 || get.Code != 200 || get.Body.String() != string(body) {
				t.Fatalf("presigned replacement: PUT=%d %s GET=%d %q", w.Code, w.Body.String(), get.Code, get.Body.String())
			}
			if r.URL.String() != originalURL || !reflect.DeepEqual(r.Header, originalHeaders) {
				t.Fatal("forwarder mutated the presigned caller request")
			}
		})
	}
}
