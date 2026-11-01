package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"reflect"
	"strconv"
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
		data, err := readConditionalOriginBody(r)
		if err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, auth.ErrChunkSignatureMismatch) {
				status = http.StatusForbidden
			}
			http.Error(w, err.Error(), status)
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

// trailerSignature is the spec-literal trailer MAC. Chunk signatures below use
// the AWS SDK; neither the producer nor origin uses TAG's body verifier.
func trailerSignature(previous, signingTime, canonical string) string {
	mac := func(key []byte, text string) []byte {
		h := hmac.New(sha256.New, key)
		h.Write([]byte(text))
		return h.Sum(nil)
	}
	date := signingTime[:8]
	key := mac([]byte("AWS4"+payloadHashTestSecretKey), date)
	key = mac(key, payloadHashTestRegion)
	key = mac(key, "s3")
	key = mac(key, "aws4_request")
	sum := sha256.Sum256([]byte(canonical))
	text := strings.Join([]string{"AWS4-HMAC-SHA256-TRAILER", signingTime, date + "/" + payloadHashTestRegion + "/s3/aws4_request", previous, hex.EncodeToString(sum[:])}, "\n")
	return hex.EncodeToString(mac(key, text))
}

func readConditionalOriginBody(r *http.Request) ([]byte, error) {
	marker := r.Header.Get("X-Amz-Content-Sha256")
	if !IsStreamingPayload(marker) {
		return io.ReadAll(r.Body)
	}
	reader := bufio.NewReader(r.Body)
	info, err := auth.ParseAuthInfo(r)
	if err != nil {
		return nil, err
	}
	seed, err := hex.DecodeString(info.Signature)
	if err != nil {
		return nil, err
	}
	signingTime, err := time.Parse(auth.TimeFormat, r.Header.Get("X-Amz-Date"))
	if err != nil {
		return nil, err
	}
	signer := v4.NewStreamSigner(aws.Credentials{AccessKeyID: payloadHashTestAccessKey, SecretAccessKey: payloadHashTestSecretKey}, "s3", payloadHashTestRegion, seed)
	var data bytes.Buffer
	previous := info.Signature
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, extension, _ := strings.Cut(strings.TrimSpace(line), ";chunk-signature=")
		n, err := strconv.ParseInt(size, 16, 64)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid chunk size %q", size)
		}
		chunk := make([]byte, n)
		if _, err := io.ReadFull(reader, chunk); err != nil {
			return nil, err
		}
		if n > 0 || marker == StreamingPayloadHash {
			end, err := reader.ReadString('\n')
			if err != nil || end != "\r\n" {
				return nil, fmt.Errorf("invalid chunk end: %q %v", end, err)
			}
		}
		if marker != StreamingUnsignedTrailerHash {
			signature, err := signer.GetSignature(r.Context(), nil, chunk, signingTime)
			if err != nil || hex.EncodeToString(signature) != extension {
				return nil, fmt.Errorf("invalid original chunk signature: %w", auth.ErrChunkSignatureMismatch)
			}
			previous = extension
		}
		data.Write(chunk)
		if n == 0 {
			break
		}
	}
	if value := r.Header.Get("X-Amz-Decoded-Content-Length"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n != data.Len() {
			return nil, fmt.Errorf("wrong decoded length")
		}
	}
	if marker != StreamingPayloadHash {
		trailers, err := textproto.NewReader(reader).ReadMIMEHeader()
		if err != nil {
			return nil, err
		}
		checksum := trailers.Get("X-Amz-Checksum-Sha256")
		if marker == StreamingPayloadHash+"-TRAILER" {
			expected := trailerSignature(previous, signingTime.Format(auth.TimeFormat), "x-amz-checksum-sha256:"+checksum+"\n")
			if trailers.Get("X-Amz-Trailer-Signature") != expected {
				return nil, fmt.Errorf("invalid original trailer signature: %w", auth.ErrChunkSignatureMismatch)
			}
		}
		sum := sha256.Sum256(data.Bytes())
		if checksum != base64.StdEncoding.EncodeToString(sum[:]) {
			return nil, fmt.Errorf("checksum mismatch")
		}
	}
	return data.Bytes(), nil
}

func TestTieredLargeConditionalPutSignedChunks(t *testing.T) {
	for _, flavor := range []payloadHashTestFlavor{payloadHashSigningFlavor, payloadHashTransparentFlavor} {
		for _, tc := range []struct {
			marker, fault                                  string
			unknown, absent, empty, upstream, large, split bool
		}{
			{marker: StreamingPayloadHash},
			{marker: StreamingPayloadHash, fault: "data"},
			{marker: StreamingPayloadHash + "-TRAILER"},
			{marker: StreamingPayloadHash + "-TRAILER", upstream: true},
			{marker: StreamingPayloadHash + "-TRAILER", upstream: true, unknown: true},
			{marker: StreamingPayloadHash + "-TRAILER", unknown: true},
			{marker: StreamingPayloadHash + "-TRAILER", absent: true},
			{marker: StreamingPayloadHash + "-TRAILER", large: true},
			{marker: StreamingPayloadHash + "-TRAILER", split: true},
			{marker: StreamingPayloadHash + "-TRAILER", unknown: true, empty: true},
			{marker: StreamingPayloadHash + "-TRAILER", fault: "data"},
			{marker: StreamingPayloadHash + "-TRAILER", fault: "data", unknown: true, absent: true},
			{marker: StreamingPayloadHash + "-TRAILER", fault: "terminal"},
			{marker: StreamingPayloadHash + "-TRAILER", fault: "trailer"},
			{marker: StreamingPayloadHash + "-TRAILER", fault: "missing trailer"},
			{marker: StreamingPayloadHash + "-TRAILER", fault: "checksum"},
			{marker: StreamingPayloadHash + "-TRAILER", fault: "short declaration"},
			{marker: StreamingPayloadHash + "-TRAILER", fault: "long declaration"},
			{marker: StreamingPayloadHash + "-TRAILER", fault: "negative terminal"},
			{marker: StreamingPayloadHash + "-TRAILER", fault: "negative length"},
			{marker: StreamingPayloadHash, fault: "malformed length"},
			{marker: StreamingUnsignedTrailerHash},
			{marker: StreamingUnsignedTrailerHash, unknown: true},
			{marker: StreamingUnsignedTrailerHash, fault: "checksum"},
			{marker: StreamingUnsignedTrailerHash, fault: "checksum", unknown: true},
		} {
			t.Run(fmt.Sprintf("%s/%s/%s/unknown=%v/absent=%v/empty=%v/upstream=%v/large=%v/split=%v", flavor, tc.marker, tc.fault, tc.unknown, tc.absent, tc.empty, tc.upstream, tc.large, tc.split), func(t *testing.T) {
				origin := &tieredConditionalOrigin{objects: make(map[string]string)}
				f := newTieredPayloadHashFixture(t, flavor, origin)
				old := []byte("local only")
				if tc.upstream {
					old = bytes.Repeat([]byte("u"), 2048)
				}
				if !tc.absent {
					if w := f.do(t, http.MethodPut, "object", old, payloadSHA256(old), ""); w.Code != 200 {
						t.Fatalf("seed: %d", w.Code)
					}
				}
				prior, version, found, err := f.cache.GetMetaWithVersion(t.Context(), payloadHashTestBucket, "object")
				if err != nil || found == tc.absent {
					t.Fatalf("prior: %+v found=%v err=%v", prior, found, err)
				}
				body := bytes.Repeat([]byte("l"), 1408)
				if tc.large {
					body = bytes.Repeat([]byte("l"), 3*len((authenticatedTrailerReader{}).buffer)+17)
				}
				if tc.empty {
					body = nil
				}
				headers := http.Header{"Content-Encoding": {"aws-chunked"}}
				if tc.absent {
					headers.Set("If-None-Match", "*")
				} else {
					headers.Set("If-Match", prior.ETag)
				}
				if tc.marker != StreamingPayloadHash {
					headers.Set("X-Amz-Trailer", "x-amz-checksum-sha256")
				}
				n := len(body)
				if tc.fault == "short declaration" {
					n--
				}
				if tc.fault == "long declaration" {
					n++
				}
				if !tc.unknown {
					headers.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(n))
					if tc.fault == "negative length" {
						headers.Set("X-Amz-Decoded-Content-Length", "-1")
					}
					if tc.fault == "malformed length" {
						headers.Set("X-Amz-Decoded-Content-Length", "invalid")
					}
				}
				r := f.signedRequest(t, http.MethodPut, "object", nil, tc.marker, "", headers)
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
				var previous string
				chunks := [][]byte{body, nil}
				if tc.split {
					chunks = [][]byte{body[:len(body)/2], body[len(body)/2:], nil}
				}
				if tc.empty {
					chunks = [][]byte{nil}
				}
				for _, chunk := range chunks {
					signature, err := stream.GetSignature(t.Context(), nil, chunk, f.signingTime)
					if err != nil {
						t.Fatal(err)
					}
					previous = hex.EncodeToString(signature)
					if len(chunk) == 0 && tc.fault == "terminal" {
						previous = strings.Repeat("0", 64)
					}
					size := fmt.Sprintf("%x", len(chunk))
					if len(chunk) == 0 && tc.fault == "negative terminal" {
						size = "-1"
					}
					wire.WriteString(size)
					if tc.marker != StreamingUnsignedTrailerHash {
						fmt.Fprintf(&wire, ";chunk-signature=%s", previous)
					}
					wire.WriteString("\r\n")
					wire.Write(chunk)
					if len(chunk) > 0 || tc.marker == StreamingPayloadHash {
						wire.WriteString("\r\n")
					}
				}
				if tc.marker != StreamingPayloadHash && tc.fault != "missing trailer" {
					sum := sha256.Sum256(body)
					if tc.fault == "checksum" {
						sum[0] ^= 1
					}
					checksum := base64.StdEncoding.EncodeToString(sum[:])
					fmt.Fprintf(&wire, "x-amz-checksum-sha256:%s\r\n", checksum)
					if tc.marker != StreamingUnsignedTrailerHash {
						signature := trailerSignature(previous, f.signingTime.UTC().Format(auth.TimeFormat), "x-amz-checksum-sha256:"+checksum+"\n")
						if tc.fault == "trailer" {
							signature = strings.Repeat("0", 64)
						}
						fmt.Fprintf(&wire, "x-amz-trailer-signature:%s\r\n", signature)
					}
					wire.WriteString("\r\n")
				}
				framed := wire.Bytes()
				if tc.fault == "data" {
					framed = flipByte(t, framed, body)
				}
				r.Body = io.NopCloser(bytes.NewReader(framed))
				r.ContentLength = int64(len(framed))
				clientHeaders, clientURL := r.Header.Clone(), r.URL.String()
				w := httptest.NewRecorder()
				err = f.service.HandlePutObject(w, r)
				current, currentVersion, currentFound, readErr := f.cache.GetMetaWithVersion(t.Context(), payloadHashTestBucket, "object")
				if readErr != nil {
					t.Fatal(readErr)
				}
				if !reflect.DeepEqual(r.Header, clientHeaders) || r.URL.String() != clientURL {
					t.Fatal("forwarder mutated the signed client request")
				}
				if tc.fault != "" {
					if tc.fault != "checksum" {
						if e, ok := IsAuthError(err); !ok || e.Code != ErrCodeSignatureMismatch {
							t.Fatalf("invalid stream: error=%v status=%d; want signature mismatch", err, w.Code)
						}
					} else if err != nil || w.Code != 400 {
						t.Fatalf("bad checksum: error=%v status=%d; want origin rejection", err, w.Code)
					}
					if currentFound != found || (found && currentVersion != version) || !reflect.DeepEqual(current, prior) {
						t.Fatalf("failed stream replaced local metadata: %+v/%d -> %+v/%d", prior, version, current, currentVersion)
					}
					origin.mu.Lock()
					_, committed := origin.objects["/"+payloadHashTestBucket+"/object"]
					origin.mu.Unlock()
					if committed {
						t.Fatal("origin committed an invalid upload")
					}
				} else if err != nil || w.Code != 200 || !currentFound || !current.BodyUpstream {
					t.Fatalf("valid stream: error=%v status=%d metadata=%+v", err, w.Code, current)
				}
				get := f.do(t, http.MethodGet, "object", nil, "UNSIGNED-PAYLOAD", "")
				want, wantStatus := body, 200
				if tc.fault != "" {
					want = old
					if tc.absent {
						want = nil
						wantStatus = 404
					}
				}
				if get.Code != wantStatus || (wantStatus == 200 && get.Body.String() != string(want)) {
					t.Fatalf("GET after stream=%d %q, want %d %q", get.Code, get.Body.String(), wantStatus, want)
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

func TestTieredConditionalBodyClosesInbound(t *testing.T) {
	for _, marker := range []string{StreamingPayloadHash, StreamingPayloadHash + "-TRAILER"} {
		t.Run(marker, func(t *testing.T) {
			f := newTieredPayloadHashFixture(t, payloadHashSigningFlavor)
			r := f.signedRequest(t, http.MethodPut, "object", nil, marker, "", http.Header{"If-None-Match": {"*"}, "X-Amz-Decoded-Content-Length": {"1408"}, "X-Amz-Trailer": {"x-amz-checksum-sha256"}})
			credentials := auth.NewCredentialStore()
			credentials.AddCredential(payloadHashTestAccessKey, payloadHashTestSecretKey)
			vr, err := auth.NewRequestValidator(credentials).Validate(r)
			if err != nil {
				t.Fatal(err)
			}
			body := &countingReadCloser{Reader: strings.NewReader("")}
			r.Body = body
			prepared, err := preparePutWithoutConditions(t.Context(), r, vr)
			if err != nil {
				t.Fatal(err)
			}
			if err := prepared.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if !body.closed {
				t.Fatal("closing the forwarded body did not close the inbound body")
			}
		})
	}
}
