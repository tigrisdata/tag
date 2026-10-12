package proxy

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tigrisdata/tag/auth"
)

const (
	signedStreamTestAccessKey = "AKIAIOSFODNN7EXAMPLE"
	signedStreamTestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

type signedStreamObservation struct {
	body          []byte
	contentHash   string
	contentLength int64
	contentCoding string
	decodedLength string
}

type signedStreamSeed struct {
	endpoint        string
	path            string
	headers         http.Header
	host            string
	seedSignature   string
	signingKey      []byte
	signingTime     string
	credentialScope string
}

type signedStreamWire struct {
	wire                   []byte
	data                   []byte
	dataSignatureStart     []int
	dataSignatureExtension []int
	terminalSignatureStart int
	terminalSignatureExt   int
}

// TestSigningForwarderSignedStreamIntegrity checks the public signing-mode
// forwarder against independently generated SigV4 chunk chains. The local
// upstream only records requests; it does not authenticate or reject them.
func TestSigningForwarderSignedStreamIntegrity(t *testing.T) {
	var (
		dispatched atomic.Int32
		mu         sync.Mutex
		seen       []signedStreamObservation
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dispatched.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		seen = append(seen, signedStreamObservation{
			body:          body,
			contentHash:   r.Header.Get("X-Amz-Content-Sha256"),
			contentLength: r.ContentLength,
			contentCoding: r.Header.Get("Content-Encoding"),
			decodedLength: r.Header.Get("X-Amz-Decoded-Content-Length"),
		})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
	forwarder := NewForwarder(credentials, upstream.URL, "us-east-1", 1, nil, nil)
	// Keep the integrity assertion independent of the host's available temp space.
	useSignedStreamStageBudget(t, newSignedStreamStageBudget(func(string) (signedStreamStageSpace, error) {
		return signedStreamStageSpace{availableBytes: 1 << 30, blockSize: 1}, nil
	}))
	chunks := [][]byte{[]byte("first chunk "), []byte("second chunk")}
	decodedLength := len(chunks[0]) + len(chunks[1])
	seed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, decodedLength, true)
	validWire := makeSignedStreamWire(t, seed, chunks...)

	methods := []struct {
		name string
		call func(*http.Request) (int, error)
	}{
		{
			name: "Forward",
			call: func(r *http.Request) (int, error) {
				w := httptest.NewRecorder()
				err := forwarder.Forward(r.Context(), w, r)
				return w.Code, err
			},
		},
		{
			name: "ForwardWithCapture",
			call: func(r *http.Request) (int, error) {
				capture, err := forwarder.ForwardWithCapture(r.Context(), httptest.NewRecorder(), r)
				if capture == nil {
					return 0, err
				}
				return capture.StatusCode, err
			},
		},
		{
			name: "ForwardTeeingBody",
			call: func(r *http.Request) (int, error) {
				tee, ok := forwarder.(interface {
					ForwardTeeingBody(context.Context, http.ResponseWriter, *http.Request, io.Writer) (int, http.Header, string, string, error)
				})
				if !ok {
					return 0, fmt.Errorf("signing forwarder does not implement ForwardTeeingBody")
				}
				status, _, _, _, err := tee.ForwardTeeingBody(r.Context(), httptest.NewRecorder(), r, io.Discard)
				return status, err
			},
		},
	}

	for _, method := range methods {
		t.Run(method.name, func(t *testing.T) {
			assertSignedStreamForwarded(t, method.call, seed, validWire.wire, validWire.data, &dispatched, &mu, &seen)

			invalid := []struct {
				name string
				wire []byte
			}{
				{name: "same-length payload mutation", wire: mutateStreamPayload(t, validWire)},
				{name: "missing data signature", wire: removeStreamSignature(t, validWire, validWire.dataSignatureExtension[0], validWire.dataSignatureStart[0])},
				{name: "changed later data signature", wire: changeStreamSignature(t, validWire.wire, validWire.dataSignatureStart[1])},
				{name: "changed terminal signature", wire: changeStreamSignature(t, validWire.wire, validWire.terminalSignatureStart)},
				{name: "missing terminal signature", wire: removeStreamSignature(t, validWire, validWire.terminalSignatureExt, validWire.terminalSignatureStart)},
			}
			for _, tc := range invalid {
				t.Run(tc.name, func(t *testing.T) {
					assertSignedStreamRejectedBeforeDispatch(t, method.call, seed, tc.wire, &dispatched)
				})
			}

			emptySeed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, 0, true)
			emptyWire := makeSignedStreamWire(t, emptySeed)
			t.Run("valid zero-byte terminal signature", func(t *testing.T) {
				assertSignedStreamForwarded(t, method.call, emptySeed, emptyWire.wire, nil, &dispatched, &mu, &seen)
			})
			badEmptyTerminal := changeStreamSignature(t, emptyWire.wire, emptyWire.terminalSignatureStart)
			t.Run("invalid zero-byte terminal signature", func(t *testing.T) {
				assertSignedStreamRejectedBeforeDispatch(t, method.call, emptySeed, badEmptyTerminal, &dispatched)
			})
		})
	}

	// Compatibility controls: ordinary signed payloads and the separately
	// supported unsigned-trailer stream keep their existing forwarding behavior.
	payload := []byte("compatibility payload")
	nonStreamingHash := sha256Hex(payload)
	nonStreamingSeed := newSignedStreamSeed(t, upstream.URL, nonStreamingHash, -1, false)
	assertCompatibilityStreamForwarded(t, methods[0].call, nonStreamingSeed, payload, nonStreamingHash, payload, &dispatched, &mu, &seen)

	unsignedSeed := newSignedStreamSeed(t, upstream.URL, StreamingUnsignedTrailerHash, len(payload), true)
	unsignedWire := makeUnsignedStreamWire(payload)
	assertCompatibilityStreamForwarded(t, methods[0].call, unsignedSeed, unsignedWire, "UNSIGNED-PAYLOAD", payload, &dispatched, &mu, &seen)
}

func TestSigningForwarderDecodedStreamOutgoingSignature(t *testing.T) {
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
	upstreamValidator := auth.NewRequestValidator(credentials)
	var (
		dispatched atomic.Int32
		mu         sync.Mutex
		seen       []signedStreamObservation
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dispatched.Add(1)
		if _, err := upstreamValidator.ValidateRequest(r); err != nil {
			http.Error(w, fmt.Sprintf("outbound SigV4 validation failed: %v", err), http.StatusForbidden)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		seen = append(seen, signedStreamObservation{
			body:          body,
			contentHash:   r.Header.Get("X-Amz-Content-Sha256"),
			contentLength: r.ContentLength,
			contentCoding: r.Header.Get("Content-Encoding"),
			decodedLength: r.Header.Get("X-Amz-Decoded-Content-Length"),
		})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	forwarder := NewForwarder(credentials, upstream.URL, "us-east-1", 1, nil, nil)
	useSignedStreamStageBudget(t, newSignedStreamStageBudget(func(string) (signedStreamStageSpace, error) {
		return signedStreamStageSpace{availableBytes: 1 << 20, blockSize: 1}, nil
	}))
	payload := []byte("payload signed for the decoded upstream request")
	seed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, len(payload), true)
	stream := makeSignedStreamWire(t, seed, payload)
	methods := []struct {
		name string
		call func(*http.Request) (int, error)
	}{
		{
			name: "Forward",
			call: func(r *http.Request) (int, error) {
				w := httptest.NewRecorder()
				err := forwarder.Forward(r.Context(), w, r)
				return w.Code, err
			},
		},
		{
			name: "ForwardWithCapture",
			call: func(r *http.Request) (int, error) {
				capture, err := forwarder.ForwardWithCapture(r.Context(), httptest.NewRecorder(), r)
				if capture == nil {
					return 0, err
				}
				return capture.StatusCode, err
			},
		},
		{
			name: "ForwardTeeingBody",
			call: func(r *http.Request) (int, error) {
				tee, ok := forwarder.(interface {
					ForwardTeeingBody(context.Context, http.ResponseWriter, *http.Request, io.Writer) (int, http.Header, string, string, error)
				})
				if !ok {
					return 0, fmt.Errorf("signing forwarder does not implement ForwardTeeingBody")
				}
				status, _, _, _, err := tee.ForwardTeeingBody(r.Context(), httptest.NewRecorder(), r, io.Discard)
				return status, err
			},
		},
	}
	for _, method := range methods {
		t.Run(method.name, func(t *testing.T) {
			before := dispatched.Load()
			seenBefore := observationCount(&mu, &seen)
			status, err := method.call(seed.request(t, stream.wire))
			if err != nil {
				t.Fatalf("forward decoded signed stream: %v", err)
			}
			if status != http.StatusOK || dispatched.Load() != before+1 {
				t.Fatalf("forwarded status=%d dispatches=%d; want one SigV4-accepted request", status, dispatched.Load()-before)
			}
			got, ok := observationAt(&mu, &seen, seenBefore)
			if !ok {
				t.Fatalf("upstream dispatch %d produced no accepted request observation", seenBefore)
			}
			if !bytes.Equal(got.body, payload) || got.contentHash != "UNSIGNED-PAYLOAD" || got.contentLength != int64(len(payload)) || got.contentCoding != "" || got.decodedLength != "" {
				t.Fatalf("upstream accepted a different decoded request: %+v want bytes=%q", got, payload)
			}
		})
	}
}

func newSignedStreamSeed(t *testing.T, endpoint, bodyHash string, decodedLength int, chunked bool) signedStreamSeed {
	t.Helper()

	path := "/bucket/object"
	headers := make(http.Header)
	if chunked {
		headers.Set("Content-Encoding", "aws-chunked")
		if decodedLength >= 0 {
			headers.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(decodedLength))
		}
	}
	signed, err := auth.NewRequestSigner(endpoint, "us-east-1").SignRequest(
		t.Context(), http.MethodPut, path, nil, bodyHash,
		signedStreamTestAccessKey, signedStreamTestSecretKey, headers,
	)
	if err != nil {
		t.Fatalf("sign request header: %v", err)
	}
	info, err := auth.ParseAuthInfo(signed)
	if err != nil {
		t.Fatalf("parse generated request signature: %v", err)
	}
	requestTime, err := auth.ParseHTTPDate(signed.Header.Get("X-Amz-Date"))
	if err != nil {
		t.Fatalf("parse generated request date: %v", err)
	}

	return signedStreamSeed{
		endpoint:        endpoint,
		path:            path,
		headers:         signed.Header.Clone(),
		host:            signed.Host,
		seedSignature:   info.Signature,
		signingKey:      deriveSignedStreamTestKey(signedStreamTestSecretKey, info.Date, info.Region),
		signingTime:     requestTime.UTC().Format(auth.TimeFormat),
		credentialScope: info.Date + "/" + info.Region + "/s3/aws4_request",
	}
}

func (seed signedStreamSeed) request(t *testing.T, wire []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, seed.endpoint+seed.path, bytes.NewReader(wire))
	if err != nil {
		t.Fatalf("create signed-stream request: %v", err)
	}
	req.Header = seed.headers.Clone()
	req.Host = seed.host
	req.ContentLength = int64(len(wire))
	return req
}

func makeSignedStreamWire(t *testing.T, seed signedStreamSeed, chunks ...[]byte) signedStreamWire {
	t.Helper()

	var wire signedStreamWire
	var encoded bytes.Buffer
	wire.dataSignatureStart = make([]int, 0, len(chunks))
	wire.dataSignatureExtension = make([]int, 0, len(chunks))
	previous := seed.seedSignature
	for _, chunk := range chunks {
		if len(chunk) == 0 {
			t.Fatal("test chunk must be non-empty")
		}
		signature := signedStreamTestChunkSignature(seed.signingKey, seed.signingTime, seed.credentialScope, previous, chunk)
		fmt.Fprintf(&encoded, "%x", len(chunk))
		wire.dataSignatureExtension = append(wire.dataSignatureExtension, encoded.Len())
		encoded.WriteString(";chunk-signature=")
		wire.dataSignatureStart = append(wire.dataSignatureStart, encoded.Len())
		encoded.WriteString(signature)
		encoded.WriteString("\r\n")
		wire.data = append(wire.data, chunk...)
		encoded.Write(chunk)
		encoded.WriteString("\r\n")
		previous = signature
	}

	terminal := signedStreamTestChunkSignature(seed.signingKey, seed.signingTime, seed.credentialScope, previous, nil)
	encoded.WriteString("0")
	wire.terminalSignatureExt = encoded.Len()
	encoded.WriteString(";chunk-signature=")
	wire.terminalSignatureStart = encoded.Len()
	encoded.WriteString(terminal)
	encoded.WriteString("\r\n\r\n")
	wire.wire = bytes.Clone(encoded.Bytes())
	return wire
}

func signedStreamTestChunkSignature(signingKey []byte, signingTime, scope, previous string, chunk []byte) string {
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		signingTime,
		scope,
		previous,
		sha256Hex(nil),
		sha256Hex(chunk),
	}, "\n")
	mac := hmac.New(sha256.New, signingKey)
	_, _ = mac.Write([]byte(stringToSign))
	return hex.EncodeToString(mac.Sum(nil))
}

func deriveSignedStreamTestKey(secret, date, region string) []byte {
	kDate := signedStreamTestHMAC([]byte("AWS4"+secret), []byte(date))
	kRegion := signedStreamTestHMAC(kDate, []byte(region))
	kService := signedStreamTestHMAC(kRegion, []byte("s3"))
	return signedStreamTestHMAC(kService, []byte("aws4_request"))
}

func signedStreamTestHMAC(key, message []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(message)
	return mac.Sum(nil)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func mutateStreamPayload(t *testing.T, stream signedStreamWire) []byte {
	t.Helper()
	if len(stream.dataSignatureStart) < 2 {
		t.Fatal("test stream needs two data chunks")
	}
	wire := bytes.Clone(stream.wire)
	dataStart := bytes.Index(wire, []byte("second chunk"))
	if dataStart < 0 {
		t.Fatal("test stream payload not found")
	}
	wire[dataStart] ^= 1
	return wire
}

func changeStreamSignature(t *testing.T, wire []byte, signatureStart int) []byte {
	t.Helper()
	changed := bytes.Clone(wire)
	if signatureStart < 0 || signatureStart+64 > len(changed) {
		t.Fatalf("invalid test signature offset %d", signatureStart)
	}
	if changed[signatureStart] == '0' {
		changed[signatureStart] = '1'
	} else {
		changed[signatureStart] = '0'
	}
	return changed
}

func removeStreamSignature(t *testing.T, stream signedStreamWire, extensionStart, signatureStart int) []byte {
	t.Helper()
	end := signatureStart + 64
	if extensionStart < 0 || signatureStart < extensionStart || end > len(stream.wire) {
		t.Fatalf("invalid test signature span %d..%d", extensionStart, end)
	}
	changed := make([]byte, 0, len(stream.wire)-(end-extensionStart))
	changed = append(changed, stream.wire[:extensionStart]...)
	changed = append(changed, stream.wire[end:]...)
	return changed
}

func assertSignedStreamForwarded(t *testing.T, call func(*http.Request) (int, error), seed signedStreamSeed, wire, wantBody []byte, dispatched *atomic.Int32, mu *sync.Mutex, seen *[]signedStreamObservation) {
	t.Helper()
	before := dispatched.Load()
	seenBefore := observationCount(mu, seen)
	status, err := call(seed.request(t, wire))
	if err != nil {
		if authErr, ok := IsAuthError(err); ok && authErr.Code == ErrCodeSignatureMismatch {
			signedStreamViolation(t, "valid signed stream was rejected as an authentication failure")
		} else {
			t.Fatalf("valid stream setup or forwarding failed: %v", err)
		}
	}
	if status != http.StatusOK || dispatched.Load() != before+1 {
		signedStreamViolation(t, "valid stream was not dispatched successfully: status=%d dispatches=%d", status, dispatched.Load()-before)
		return
	}
	got, ok := observationAt(mu, seen, seenBefore)
	if !ok {
		t.Fatalf("upstream dispatch %d produced no complete observation", seenBefore)
	}
	if !bytes.Equal(got.body, wantBody) || got.contentHash != "UNSIGNED-PAYLOAD" || got.contentCoding != "" || got.decodedLength != "" || got.contentLength != int64(len(wantBody)) {
		signedStreamViolation(t, "valid stream was not re-signed as the exact decoded payload: observation=%+v want bytes=%q", got, wantBody)
	}
}

func assertSignedStreamRejectedBeforeDispatch(t *testing.T, call func(*http.Request) (int, error), seed signedStreamSeed, wire []byte, dispatched *atomic.Int32) {
	t.Helper()
	before := dispatched.Load()
	_, err := call(seed.request(t, wire))
	after := dispatched.Load()
	authErr, isAuthErr := IsAuthError(err)
	if err != nil && !isAuthErr && after == before {
		t.Fatalf("invalid signed stream was not conclusively rejected by payload authentication: %v", err)
	}
	if err == nil || !isAuthErr || authErr.Code != ErrCodeSignatureMismatch || after != before {
		signedStreamViolation(t, "invalid signed stream was not rejected as a signature mismatch before upstream dispatch: err=%v auth=%v dispatches=%d", err, isAuthErr, after-before)
	}
}

func assertCompatibilityStreamForwarded(t *testing.T, call func(*http.Request) (int, error), seed signedStreamSeed, wire []byte, outboundHash string, payload []byte, dispatched *atomic.Int32, mu *sync.Mutex, seen *[]signedStreamObservation) {
	t.Helper()
	before := dispatched.Load()
	seenBefore := observationCount(mu, seen)
	status, err := call(seed.request(t, wire))
	if err != nil {
		t.Fatalf("compatibility control failed: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("compatibility control status = %d, want %d", status, http.StatusOK)
	}
	if dispatched.Load() != before+1 {
		t.Fatalf("compatibility control dispatched %d times, want exactly once", dispatched.Load()-before)
	}
	got, ok := observationAt(mu, seen, seenBefore)
	if !ok {
		t.Fatalf("compatibility control dispatch %d produced no complete observation", seenBefore)
	}
	if !bytes.Equal(got.body, payload) || got.contentHash != outboundHash {
		t.Fatalf("compatibility control changed body or payload hash: got body=%q hash=%q, want body=%q hash=%q", got.body, got.contentHash, payload, outboundHash)
	}
}

func observationCount(mu *sync.Mutex, seen *[]signedStreamObservation) int {
	mu.Lock()
	defer mu.Unlock()
	return len(*seen)
}

func observationAt(mu *sync.Mutex, seen *[]signedStreamObservation, index int) (signedStreamObservation, bool) {
	mu.Lock()
	defer mu.Unlock()
	if index < 0 || index >= len(*seen) {
		return signedStreamObservation{}, false
	}
	return (*seen)[index], true
}

func signedStreamViolation(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Errorf(format, args...)
}

func makeUnsignedStreamWire(payload []byte) []byte {
	var wire bytes.Buffer
	fmt.Fprintf(&wire, "%x\r\n", len(payload))
	wire.Write(payload)
	wire.WriteString("\r\n0\r\n\r\n")
	return wire.Bytes()
}
