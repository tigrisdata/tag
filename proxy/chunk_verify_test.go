package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/tigrisdata/tag/auth"
)

const (
	chunkVerifyAccessKey = "AKIAIOSFODNN7EXAMPLE"
	chunkVerifySecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	chunkVerifyRegion    = "us-east-1"
)

// upstreamRecord is what the fake Tigris observed for one forwarded request.
type upstreamRecord struct {
	path        string
	body        []byte
	bodyErr     error
	contentLen  int64
	header      http.Header
	validateErr error
}

// complete reports whether upstream received every declared body byte.
func (u upstreamRecord) complete() bool {
	return u.bodyErr == nil && int64(len(u.body)) == u.contentLen
}

// chunkVerifyUpstream fakes Tigris for the signing forwarder: it validates
// the re-signed request with the same credentials TAG re-signs with, reads the
// body, and answers 200 only to a validly signed, complete request. Every
// handler run is recorded, attributed by request path (unique per subtest),
// and announced on changed, so a test can wait for the handler of a request
// whose client side already returned: a forward that aborts mid-body comes
// back before upstream has finished reading — or even started — and a record
// inspected before then is simply absent, which would let the central
// promise (upstream never holds a complete unauthenticated body) go
// unchecked.
type chunkVerifyUpstream struct {
	*httptest.Server
	mu      sync.Mutex
	records []upstreamRecord
	changed chan struct{} // closed and replaced on every new record
}

func newChunkVerifyUpstream(t *testing.T) *chunkVerifyUpstream {
	t.Helper()
	store := auth.NewCredentialStore()
	store.AddCredential(chunkVerifyAccessKey, chunkVerifySecretKey)
	validator := auth.NewRequestValidator(store)

	u := &chunkVerifyUpstream{changed: make(chan struct{})}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := upstreamRecord{path: r.URL.Path, contentLen: r.ContentLength, header: r.Header.Clone()}
		_, rec.validateErr = validator.ValidateRequest(r)
		rec.body, rec.bodyErr = io.ReadAll(r.Body)
		u.mu.Lock()
		u.records = append(u.records, rec)
		close(u.changed)
		u.changed = make(chan struct{})
		u.mu.Unlock()
		switch {
		case rec.validateErr != nil:
			http.Error(w, rec.validateErr.Error(), http.StatusForbidden)
		case !rec.complete():
			http.Error(w, "incomplete body", http.StatusBadRequest)
		default:
			w.Header().Set("ETag", `"upstream-etag"`)
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(u.Close)
	return u
}

// recorded returns the handler runs recorded so far for path, without waiting.
func (u *chunkVerifyUpstream) recorded(path string) []upstreamRecord {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.recordedLocked(path)
}

func (u *chunkVerifyUpstream) recordedLocked(path string) []upstreamRecord {
	var out []upstreamRecord
	for _, rec := range u.records {
		if rec.path == path {
			out = append(out, rec)
		}
	}
	return out
}

// await blocks until at least n handler runs have been recorded for path and
// returns them. A forwarded request always reaches the handler — the
// transport flushes the headers before it reads the body, so even a body
// that fails on its first byte has been announced upstream — but the handler
// may finish well after the client side returned.
func (u *chunkVerifyUpstream) await(t *testing.T, path string, n int) []upstreamRecord {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		u.mu.Lock()
		recs := u.recordedLocked(path)
		changed := u.changed
		u.mu.Unlock()
		if len(recs) >= n {
			return recs
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf("upstream recorded %d requests for %s, want %d", len(recs), path, n)
		}
	}
}

// streamFixture is a header-signed streaming PUT produced by the AWS SDK v2
// signer, plus the material to sign chunk chains for it with the SDK's own
// stream signer (an oracle independent of TAG's implementation).
type streamFixture struct {
	t           testing.TB
	url         string
	header      http.Header
	signingTime time.Time
	seed        []byte
	creds       aws.Credentials
}

func newStreamFixture(t testing.TB, endpoint, path, marker string, decodedLen int) *streamFixture {
	t.Helper()
	f := &streamFixture{
		t:           t,
		url:         endpoint + path,
		signingTime: time.Now().UTC().Truncate(time.Second),
		creds:       aws.Credentials{AccessKeyID: chunkVerifyAccessKey, SecretAccessKey: chunkVerifySecretKey},
	}
	req, err := http.NewRequest(http.MethodPut, f.url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Amz-Content-Sha256", marker)
	req.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(decodedLen))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("Content-Type", "application/octet-stream")
	if err := v4.NewSigner().SignHTTP(t.Context(), f.creds, req, marker, "s3", chunkVerifyRegion, f.signingTime); err != nil {
		t.Fatalf("SignHTTP: %v", err)
	}
	info, err := auth.ParseAuthInfo(req)
	if err != nil {
		t.Fatalf("ParseAuthInfo: %v", err)
	}
	if f.seed, err = hex.DecodeString(info.Signature); err != nil {
		t.Fatalf("decode seed signature: %v", err)
	}
	f.header = req.Header.Clone()
	return f
}

// chain returns a chunk signer that continues the request's chain with the
// given credentials — the client's, or an attacker's for a replayed header.
func (f *streamFixture) chain(creds aws.Credentials) func([]byte) string {
	signer := v4.NewStreamSigner(creds, "s3", chunkVerifyRegion, f.seed)
	return func(chunk []byte) string {
		sig, err := signer.GetSignature(f.t.Context(), nil, chunk, f.signingTime)
		if err != nil {
			f.t.Fatalf("GetSignature: %v", err)
		}
		return hex.EncodeToString(sig)
	}
}

// wire frames chunks as a signed aws-chunked body, signing each with sign.
func (f *streamFixture) wire(chunks [][]byte, sign func([]byte) string) []byte {
	var b bytes.Buffer
	for _, c := range chunks {
		fmt.Fprintf(&b, "%x;chunk-signature=%s\r\n", len(c), sign(c))
		b.Write(c)
		b.WriteString("\r\n")
	}
	fmt.Fprintf(&b, "0;chunk-signature=%s\r\n\r\n", sign(nil))
	return b.Bytes()
}

// request is the header-signed PUT carrying wire as its body: the same
// headers every time, so a tampered wire IS a replay of a captured request.
func (f *streamFixture) request(wire []byte) *http.Request {
	req, err := http.NewRequestWithContext(f.t.Context(), http.MethodPut, f.url, bytes.NewReader(wire))
	if err != nil {
		f.t.Fatalf("new request: %v", err)
	}
	req.Header = f.header.Clone()
	req.ContentLength = int64(len(wire))
	return req
}

// flipByte returns a copy of wire with the byte at the first occurrence of
// marker altered.
func flipByte(t *testing.T, wire []byte, marker []byte) []byte {
	t.Helper()
	i := bytes.Index(wire, marker)
	if i < 0 {
		t.Fatalf("marker %q not in wire", marker)
	}
	out := bytes.Clone(wire)
	out[i] ^= 1
	return out
}

// flipSignature returns a copy of wire with the n-th chunk signature altered.
func flipSignature(t *testing.T, wire []byte, n int) []byte {
	t.Helper()
	out := bytes.Clone(wire)
	pos := 0
	for i := 0; i <= n; i++ {
		j := bytes.Index(out[pos:], []byte(";chunk-signature="))
		if j < 0 {
			t.Fatalf("chunk signature %d not in wire", n)
		}
		pos += j + len(";chunk-signature=")
	}
	if out[pos] == '0' {
		out[pos] = '1'
	} else {
		out[pos] = '0'
	}
	return out
}

func concat(chunks [][]byte) []byte {
	return bytes.Join(chunks, nil)
}

// forwardCall drives one signing-forwarder entry point and reports the status
// it delivered to the client; tee receives what ForwardTeeingBody teed.
type forwardCall func(f RequestForwarder, r *http.Request, tee *bytes.Buffer) (int, error)

var forwardCalls = map[string]forwardCall{
	"Forward": func(f RequestForwarder, r *http.Request, _ *bytes.Buffer) (int, error) {
		// statusRecorder reports 0 when no status was ever written (a bare
		// httptest recorder would default to 200).
		w := &statusRecorder{ResponseWriter: httptest.NewRecorder()}
		err := f.Forward(r.Context(), w, r)
		return w.status, err
	},
	"ForwardWithCapture": func(f RequestForwarder, r *http.Request, _ *bytes.Buffer) (int, error) {
		capture, err := f.ForwardWithCapture(r.Context(), httptest.NewRecorder(), r)
		if capture == nil {
			return 0, err
		}
		return capture.StatusCode, err
	},
	"ForwardTeeingBody": func(f RequestForwarder, r *http.Request, tee *bytes.Buffer) (int, error) {
		status, _, _, _, err := f.(bodyTeeingForwarder).ForwardTeeingBody(r.Context(), httptest.NewRecorder(), r, tee)
		return status, err
	},
	"DoRequestWithCreds": func(f RequestForwarder, r *http.Request, _ *bytes.Buffer) (int, error) {
		resp, err := f.DoRequestWithCreds(r.Context(), r, chunkVerifyAccessKey, chunkVerifySecretKey)
		if resp == nil {
			return 0, err
		}
		resp.Body.Close()
		return resp.StatusCode, err
	},
}

func newChunkVerifyForwarder(upstream string) RequestForwarder {
	store := auth.NewCredentialStore()
	store.AddCredential(chunkVerifyAccessKey, chunkVerifySecretKey)
	return NewForwarder(store, upstream, chunkVerifyRegion, 1, nil, nil)
}

// chunkVerifyChunks spans more than one bufio read (first chunk > 64 KiB) and
// ends in a short chunk, so hashing across Reads and the withheld last byte
// are both exercised.
func chunkVerifyChunks(t *testing.T) [][]byte {
	t.Helper()
	big := make([]byte, 70_000)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	return [][]byte{big, []byte("second chunk"), []byte("tail!")}
}

// A correctly chunk-signed stream passes every signing-forwarder entry point:
// upstream receives exactly the decoded bytes under an exact Content-Length,
// and the re-signed request validates upstream — which it does only because
// X-Amz-Decoded-Content-Length and the aws-chunked token are left out of the
// signed header set, not deleted after signing.
func TestSigningForwarderVerifiedStreamForwardsDecodedBody(t *testing.T) {
	upstream := newChunkVerifyUpstream(t)
	fwd := newChunkVerifyForwarder(upstream.URL)
	chunks := chunkVerifyChunks(t)
	want := concat(chunks)

	for name, call := range forwardCalls {
		t.Run(name, func(t *testing.T) {
			path := "/bucket/" + name
			f := newStreamFixture(t, upstream.URL, path, StreamingPayloadHash, len(want))
			var tee bytes.Buffer
			status, err := call(fwd, f.request(f.wire(chunks, f.chain(f.creds))), &tee)
			if err != nil || status != http.StatusOK {
				t.Fatalf("status = %d, err = %v, want 200", status, err)
			}
			recs := upstream.await(t, path, 1)
			if len(recs) != 1 {
				t.Fatalf("upstream requests = %d, want 1", len(recs))
			}
			rec := recs[0]
			if rec.validateErr != nil {
				t.Fatalf("upstream signature validation: %v", rec.validateErr)
			}
			if !rec.complete() || !bytes.Equal(rec.body, want) {
				t.Fatalf("upstream body: %d bytes (err %v), want %d exact", len(rec.body), rec.bodyErr, len(want))
			}
			if rec.contentLen != int64(len(want)) {
				t.Fatalf("upstream Content-Length = %d, want %d", rec.contentLen, len(want))
			}
			if v := rec.header.Get("X-Amz-Decoded-Content-Length"); v != "" {
				t.Fatalf("X-Amz-Decoded-Content-Length forwarded as %q", v)
			}
			if v := rec.header.Get("Content-Encoding"); v != "" {
				t.Fatalf("Content-Encoding forwarded as %q", v)
			}
			if a := rec.header.Get("Authorization"); strings.Contains(a, "x-amz-decoded-content-length") || a == "" {
				t.Fatalf("forwarded Authorization signs the stripped header: %q", a)
			}
			if name == "ForwardTeeingBody" && !bytes.Equal(tee.Bytes(), want) {
				t.Fatalf("tee holds %d bytes, want the %d decoded bytes", tee.Len(), len(want))
			}
		})
	}
}

// oversizedFrame frames data as ONE chunk claiming two bytes more than it
// carries, under an arbitrary signature, and no terminal frame: the replay
// that needs no signing at all. A decoder bounding frames only by their own
// claimed size would hand every byte of data to a consumer satisfied by
// exactly len(data) — upstream under an exact Content-Length — before the
// frame's signature was ever checked.
func oversizedFrame(data []byte) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "%x;chunk-signature=%s\r\n", len(data)+2, strings.Repeat("0", 64))
	b.Write(data)
	b.WriteString("\r\n")
	return b.Bytes()
}

// Every way a captured header can be replayed with a body it did not sign is
// refused before the body completes: upstream never receives all the declared
// bytes (so cannot commit), nothing answers 2xx, and the client gets a
// signature error.
func TestSigningForwarderRejectsUnverifiedStream(t *testing.T) {
	upstream := newChunkVerifyUpstream(t)
	fwd := newChunkVerifyForwarder(upstream.URL)
	chunks := chunkVerifyChunks(t)
	want := concat(chunks)
	attacker := aws.Credentials{AccessKeyID: chunkVerifyAccessKey, SecretAccessKey: "not-the-secret"}

	cases := map[string]func(f *streamFixture) []byte{
		"replayed header, one frame claiming more than the declared length": func(f *streamFixture) []byte {
			return oversizedFrame(want)
		},
		"frames stop short of the declared length": func(f *streamFixture) []byte {
			// Honestly signed, but the signed header declared len(want):
			// the chain verified some other body.
			return f.wire(chunks[:len(chunks)-1], f.chain(f.creds))
		},
		"tampered middle chunk body": func(f *streamFixture) []byte {
			return flipByte(t, f.wire(chunks, f.chain(f.creds)), []byte("second chunk"))
		},
		"tampered last chunk body": func(f *streamFixture) []byte {
			// The dangerous one: with the last byte delivered, the body
			// would be complete against the exact upstream Content-Length.
			return flipByte(t, f.wire(chunks, f.chain(f.creds)), []byte("tail!"))
		},
		"tampered chunk signature": func(f *streamFixture) []byte {
			return flipSignature(t, f.wire(chunks, f.chain(f.creds)), 1)
		},
		"tampered terminal signature": func(f *streamFixture) []byte {
			return flipSignature(t, f.wire(chunks, f.chain(f.creds)), len(chunks))
		},
		"replayed header, body signed with another key": func(f *streamFixture) []byte {
			return f.wire([][]byte{[]byte(strings.Repeat("x", len(want)))}, f.chain(attacker))
		},
		"replayed header, unsigned chunks": func(f *streamFixture) []byte {
			var b bytes.Buffer
			fmt.Fprintf(&b, "%x\r\n%s\r\n0\r\n\r\n", len(want), strings.Repeat("y", len(want)))
			return b.Bytes()
		},
	}

	seq := 0
	for name, call := range forwardCalls {
		for tamper, body := range cases {
			seq++
			path := fmt.Sprintf("/bucket/%s-%d", name, seq)
			t.Run(name+"/"+tamper, func(t *testing.T) {
				f := newStreamFixture(t, upstream.URL, path, StreamingPayloadHash, len(want))
				var tee bytes.Buffer
				status, err := call(fwd, f.request(body(f)), &tee)
				if err == nil || status == http.StatusOK {
					t.Fatalf("status = %d, err = %v, want a signature error", status, err)
				}
				if !errors.Is(err, auth.ErrChunkSignatureMismatch) {
					t.Fatalf("err = %v, want ErrChunkSignatureMismatch", err)
				}
				if authErr, ok := IsAuthError(err); !ok || authErr.Code != ErrCodeSignatureMismatch {
					t.Fatalf("err = %v, want AuthError with ErrCodeSignatureMismatch", err)
				}
				// The forward aborted the upstream request mid-body; wait for
				// upstream's view of it rather than inspecting an empty record set.
				for _, rec := range upstream.await(t, path, 1) {
					if rec.complete() {
						t.Fatalf("upstream received a complete %d-byte body", len(rec.body))
					}
				}
				if tee.Len() >= len(want) {
					t.Fatalf("tee holds %d bytes, a complete body", tee.Len())
				}
			})
		}
	}
}

// An empty signed stream is the one body the forwarder never streams: a zero
// decoded length goes upstream as http.NoBody. Its terminal frame — the only
// thing binding the empty body to the request — is verified anyway, on every
// entry point: a correct one forwards as an empty PUT that validates
// upstream; a tampered one is refused before any request is made upstream.
func TestSigningForwarderVerifiesEmptyStream(t *testing.T) {
	upstream := newChunkVerifyUpstream(t)
	fwd := newChunkVerifyForwarder(upstream.URL)

	for name, call := range forwardCalls {
		t.Run(name+"/verified", func(t *testing.T) {
			path := "/bucket/empty-" + name
			f := newStreamFixture(t, upstream.URL, path, StreamingPayloadHash, 0)
			var tee bytes.Buffer
			status, err := call(fwd, f.request(f.wire(nil, f.chain(f.creds))), &tee)
			if err != nil || status != http.StatusOK {
				t.Fatalf("status = %d, err = %v, want 200", status, err)
			}
			recs := upstream.await(t, path, 1)
			if len(recs) != 1 || recs[0].validateErr != nil || !recs[0].complete() || recs[0].contentLen != 0 {
				t.Fatalf("upstream records = %+v, want one validly signed, complete, empty PUT", recs)
			}
			if tee.Len() != 0 {
				t.Fatalf("tee holds %d bytes for an empty body", tee.Len())
			}
		})
		t.Run(name+"/tampered terminal signature", func(t *testing.T) {
			path := "/bucket/empty-bad-" + name
			f := newStreamFixture(t, upstream.URL, path, StreamingPayloadHash, 0)
			var tee bytes.Buffer
			status, err := call(fwd, f.request(flipSignature(t, f.wire(nil, f.chain(f.creds)), 0)), &tee)
			if err == nil || status == http.StatusOK {
				t.Fatalf("status = %d, err = %v, want a signature error", status, err)
			}
			if !errors.Is(err, auth.ErrChunkSignatureMismatch) {
				t.Fatalf("err = %v, want ErrChunkSignatureMismatch", err)
			}
			if authErr, ok := IsAuthError(err); !ok || authErr.Code != ErrCodeSignatureMismatch {
				t.Fatalf("err = %v, want AuthError with ErrCodeSignatureMismatch", err)
			}
			// Refused before forwarding: the path was never requested
			// upstream, so there is no late handler to wait for.
			if recs := upstream.recorded(path); len(recs) != 0 {
				t.Fatalf("upstream received %d requests for an unverified empty stream", len(recs))
			}
		})
	}
}

// STREAMING-UNSIGNED-PAYLOAD-TRAILER carries no chunk signatures; it keeps
// decoding exactly as before, with nothing to verify.
func TestSigningForwarderUnsignedTrailerStreamUntouched(t *testing.T) {
	upstream := newChunkVerifyUpstream(t)
	fwd := newChunkVerifyForwarder(upstream.URL)
	f := newStreamFixture(t, upstream.URL, "/bucket/unsigned", StreamingUnsignedTrailerHash, 4)
	wire := []byte("4\r\ntest\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n")

	w := httptest.NewRecorder()
	if err := fwd.Forward(t.Context(), w, f.request(wire)); err != nil || w.Code != http.StatusOK {
		t.Fatalf("status = %d, err = %v, want 200", w.Code, err)
	}
	recs := upstream.await(t, "/bucket/unsigned", 1)
	if len(recs) != 1 || !recs[0].complete() || string(recs[0].body) != "test" {
		t.Fatalf("upstream records = %+v, want one complete %q body", recs, "test")
	}
}

// The tiered local tier decodes the body itself: a verified stream commits
// the decoded object, an unverified one answers SignatureDoesNotMatch and
// commits nothing — no metadata, no staged body.
func TestTieredLocalPutVerifiesStreamChunks(t *testing.T) {
	upstream := newChunkVerifyUpstream(t)

	fwd := newChunkVerifyForwarder(upstream.URL)
	svc, c := newTieredTestService(fwd, 1<<20)
	chunks := [][]byte{[]byte("hello, "), []byte("local tier")}
	want := concat(chunks)

	f := newStreamFixture(t, upstream.URL, "/bucket/good", StreamingPayloadHash, len(want))
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, f.request(f.wire(chunks, f.chain(f.creds)))); err != nil || w.Code != http.StatusOK {
		t.Fatalf("verified PUT: status = %d, err = %v, want 200", w.Code, err)
	}
	meta, found, err := c.GetMeta(context.Background(), "bucket", "good")
	if err != nil || !found || meta.ContentLength != int64(len(want)) {
		t.Fatalf("verified PUT meta = (%+v, %v, %v), want committed with length %d", meta, found, err, len(want))
	}

	bad := newStreamFixture(t, upstream.URL, "/bucket/bad", StreamingPayloadHash, len(want))
	w = httptest.NewRecorder()
	if err := svc.HandlePutObject(w, bad.request(flipByte(t, bad.wire(chunks, bad.chain(bad.creds)), []byte("local tier")))); err != nil {
		t.Fatalf("tampered PUT: %v", err)
	}
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "SignatureDoesNotMatch") {
		t.Fatalf("tampered PUT = %d %q, want 403 SignatureDoesNotMatch", w.Code, w.Body.String())
	}
	if _, found, _ := c.GetMeta(context.Background(), "bucket", "bad"); found {
		t.Fatal("tampered PUT committed metadata")
	}
}

// The local tier verifies an empty signed stream too: the store reads the
// decoder to EOF even for a zero declared size, and that EOF exists only
// after the terminal frame verified. A correct terminal frame commits an
// empty object; a tampered one answers SignatureDoesNotMatch and commits
// nothing.
func TestTieredLocalPutVerifiesEmptyStream(t *testing.T) {
	upstream := newChunkVerifyUpstream(t)
	fwd := newChunkVerifyForwarder(upstream.URL)
	svc, c := newTieredTestService(fwd, 1<<20)

	f := newStreamFixture(t, upstream.URL, "/bucket/empty", StreamingPayloadHash, 0)
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, f.request(f.wire(nil, f.chain(f.creds)))); err != nil || w.Code != http.StatusOK {
		t.Fatalf("verified empty PUT: status = %d, err = %v, want 200", w.Code, err)
	}
	meta, found, err := c.GetMeta(context.Background(), "bucket", "empty")
	if err != nil || !found || meta.ContentLength != 0 {
		t.Fatalf("verified empty PUT meta = (%+v, %v, %v), want committed with length 0", meta, found, err)
	}

	bad := newStreamFixture(t, upstream.URL, "/bucket/empty-bad", StreamingPayloadHash, 0)
	w = httptest.NewRecorder()
	if err := svc.HandlePutObject(w, bad.request(flipSignature(t, bad.wire(nil, bad.chain(bad.creds)), 0))); err != nil {
		t.Fatalf("tampered empty PUT: %v", err)
	}
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "SignatureDoesNotMatch") {
		t.Fatalf("tampered empty PUT = %d %q, want 403 SignatureDoesNotMatch", w.Code, w.Body.String())
	}
	if _, found, _ := c.GetMeta(context.Background(), "bucket", "empty-bad"); found {
		t.Fatal("tampered empty PUT committed metadata")
	}
	if recs := upstream.recorded("/bucket/empty-bad"); len(recs) != 0 {
		t.Fatalf("local-tier PUT reached upstream %d times", len(recs))
	}
}

// Reader-level contract: a chunk-signature failure (or absence) is sticky and
// the failed chunk's last byte is never delivered.
func TestVerifyingChunkedReaderWithholdsUnverifiedChunk(t *testing.T) {
	f := newStreamFixture(t, "http://s3.example.com", "/bucket/key", StreamingPayloadHash, 9)
	store := auth.NewCredentialStore()
	store.AddCredential(chunkVerifyAccessKey, chunkVerifySecretKey)
	verify := func() *auth.ChunkVerifier {
		vr, err := auth.NewRequestValidator(store).Validate(f.request(nil))
		if err != nil {
			t.Fatalf("Validate: %v", err)
		}
		return vr.ChunkVerifier()
	}
	chunks := [][]byte{[]byte("hello"), []byte("wrld")}

	got, err := io.ReadAll(newVerifyingAWSChunkedReader(bytes.NewReader(f.wire(chunks, f.chain(f.creds))), verify(), 9))
	if err != nil || string(got) != "hellowrld" {
		t.Fatalf("verified stream = %q, %v", got, err)
	}

	r := newVerifyingAWSChunkedReader(bytes.NewReader(flipByte(t, f.wire(chunks, f.chain(f.creds)), []byte("wrld"))), verify(), 9)
	got, err = io.ReadAll(r)
	if !errors.Is(err, auth.ErrChunkSignatureMismatch) || len(got) != len("hellowrld")-1 {
		t.Fatalf("tampered stream = %d bytes, %v; want 8 bytes and ErrChunkSignatureMismatch", len(got), err)
	}
	if n, err2 := r.Read(make([]byte, 8)); n != 0 || !errors.Is(err2, auth.ErrChunkSignatureMismatch) {
		t.Fatalf("read after failure = %d, %v; want sticky failure", n, err2)
	}

	got, err = io.ReadAll(newVerifyingAWSChunkedReader(strings.NewReader("5\r\nhello\r\n0\r\n\r\n"), verify(), 9))
	if !errors.Is(err, auth.ErrChunkSignatureMismatch) || len(got) != 0 {
		t.Fatalf("unsigned chunk under verifier = %q, %v; want nothing and ErrChunkSignatureMismatch", got, err)
	}
}

// Reader-level contract: with a declared decoded length, the frames are bound
// to it. A frame claiming more than the declaration has left is refused
// before any of its bytes are delivered; a frame claiming exactly what is
// left is fine; frames that stop short fail at the terminal frame, the last
// delivered chunk's final byte still withheld. Without a declaration the
// per-chunk rules alone apply.
func TestVerifyingChunkedReaderBoundsDecodedLength(t *testing.T) {
	const declared = 9
	f := newStreamFixture(t, "http://s3.example.com", "/bucket/key", StreamingPayloadHash, declared)
	store := auth.NewCredentialStore()
	store.AddCredential(chunkVerifyAccessKey, chunkVerifySecretKey)
	verify := func() *auth.ChunkVerifier {
		vr, err := auth.NewRequestValidator(store).Validate(f.request(nil))
		if err != nil {
			t.Fatalf("Validate: %v", err)
		}
		return vr.ChunkVerifier()
	}
	body := []byte("hellowrld")

	// One frame claiming declared+2 with declared bytes behind it: nothing
	// is delivered, so no consumer can hold the declared body.
	got, err := io.ReadAll(newVerifyingAWSChunkedReader(bytes.NewReader(oversizedFrame(body)), verify(), declared))
	if !errors.Is(err, auth.ErrChunkSignatureMismatch) || len(got) != 0 {
		t.Fatalf("oversized frame = %d bytes, %v; want 0 bytes and ErrChunkSignatureMismatch", len(got), err)
	}

	// A later frame overrunning what is left is refused the same way; the
	// previous chunk's withheld byte stays withheld.
	over := f.wire([][]byte{[]byte("hello"), []byte("wrld!")}, f.chain(f.creds))
	got, err = io.ReadAll(newVerifyingAWSChunkedReader(bytes.NewReader(over), verify(), declared))
	if !errors.Is(err, auth.ErrChunkSignatureMismatch) || string(got) != "hell" {
		t.Fatalf("overrunning second frame = %q, %v; want %q and ErrChunkSignatureMismatch", got, err, "hell")
	}

	// A frame claiming exactly what is left completes normally.
	got, err = io.ReadAll(newVerifyingAWSChunkedReader(bytes.NewReader(f.wire([][]byte{body}, f.chain(f.creds))), verify(), declared))
	if err != nil || string(got) != string(body) {
		t.Fatalf("exact frame = %q, %v; want %q", got, err, body)
	}

	// Frames summing below the declaration, honestly signed: refused at the
	// terminal frame, with the last chunk's final byte never delivered.
	short := f.wire([][]byte{[]byte("hello")}, f.chain(f.creds))
	got, err = io.ReadAll(newVerifyingAWSChunkedReader(bytes.NewReader(short), verify(), declared))
	if !errors.Is(err, auth.ErrChunkSignatureMismatch) || string(got) != "hell" {
		t.Fatalf("short stream = %q, %v; want %q and ErrChunkSignatureMismatch", got, err, "hell")
	}

	// No declaration: the same short stream is a verified 5-byte body.
	got, err = io.ReadAll(newVerifyingAWSChunkedReader(bytes.NewReader(short), verify(), -1))
	if err != nil || string(got) != "hello" {
		t.Fatalf("undeclared length = %q, %v; want %q", got, err, "hello")
	}
}

// headersForForwardedSigning drops the aws-chunked framing headers from the
// set the upstream signature is computed over, without touching the client's.
func TestHeadersForForwardedSigning(t *testing.T) {
	client := http.Header{
		"X-Amz-Decoded-Content-Length": {"10"},
		"Content-Encoding":             {"aws-chunked,gzip"},
		"X-Amz-Meta-Keep":              {"yes"},
	}
	got := headersForForwardedSigning(client, true)
	if got.Get("X-Amz-Decoded-Content-Length") != "" || got.Get("Content-Encoding") != "gzip" || got.Get("X-Amz-Meta-Keep") != "yes" {
		t.Fatalf("chunked headers = %v", got)
	}
	if client.Get("X-Amz-Decoded-Content-Length") != "10" || client.Get("Content-Encoding") != "aws-chunked,gzip" {
		t.Fatalf("client headers mutated: %v", client)
	}
	only := http.Header{"Content-Encoding": {"aws-chunked"}}
	if got := headersForForwardedSigning(only, true); got.Get("Content-Encoding") != "" || len(got["Content-Encoding"]) != 0 {
		t.Fatalf("lone aws-chunked token kept: %v", got)
	}
	if got := headersForForwardedSigning(client, false); got.Get("X-Amz-Decoded-Content-Length") != "10" {
		t.Fatalf("non-chunked headers altered: %v", got)
	}
}
