package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

const (
	signingForwarderBenchmarkAccessKey = "AKIAIOSFODNN7EXAMPLE"
	signingForwarderBenchmarkSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	// This is a GetObjectTagging request. The server routes tagging requests to
	// Service.HandlePassthrough, unlike a basic object GET. Its escaped key and
	// version ID exercise a longer ordinary object target.
	signingForwarderBenchmarkGetPath = "/benchmark-bucket/object%2Fwith space+and%25%2Fkey-" +
		"000000000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000" +
		"?tagging&versionId=one%2Ftwo%20with%25marker-" +
		"000000000000000000000000000000000000000000000000000000000000"

	// This is a GetObjectTagging request with query keys and values that need
	// SigV4 percent encoding. It exercises validation and re-signing through
	// Service.HandlePassthrough without an upstream network round trip.
	signingForwarderBenchmarkEscapedQueryPath = "/benchmark-bucket/object%2Fwith space+and%25" +
		"?tagging&prefix%2Fwith%20space=one%2Ftwo&prefix%2Fwith%20space=a%20b" +
		"&prefix%2Fwith%20space=100%25&x%2By=&x%2By=%E6%97%A5%E6%9C%AC"

	// This is an UploadPart request, which is also routed to
	// Service.HandlePassthrough through handleObjectWithQuery. Its escaped key
	// and opaque upload ID are deliberately longer than the tagging request.
	signingForwarderBenchmarkUploadPartPath = "/benchmark-bucket/multipart%2F2025%2F03%2Fcustomer%20data%2F" +
		"partition%3Dnorth-america%2Fpart+with%25escapes%2Fobject-key-" +
		"000000000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000" +
		"?uploadId=benchmark%2Fupload%20id%25-with-a-longer-opaque-upload-token-" +
		"000000000000000000000000000000000000000000000000000000000000&partNumber=9999"
	signingForwarderBenchmarkUploadPartBody = "benchmark upload part body with escaped-key coverage"
)

type signingForwarderBenchmarkTransport struct {
	expectedBody     string
	expectedBodyHash string
}

type signingForwarderBenchmarkBodyValidator struct {
	expected string
	offset   int
}

func (v *signingForwarderBenchmarkBodyValidator) Write(p []byte) (int, error) {
	if len(p) > len(v.expected)-v.offset {
		return 0, fmt.Errorf("forwarded body is longer than expected")
	}
	for i := range p {
		if p[i] != v.expected[v.offset+i] {
			return 0, fmt.Errorf("forwarded body differs at byte %d", v.offset+i)
		}
	}
	v.offset += len(p)
	return len(p), nil
}

func (t signingForwarderBenchmarkTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if t.expectedBodyHash != "" && request.Header.Get("X-Amz-Content-Sha256") != t.expectedBodyHash {
		return nil, fmt.Errorf("forwarded body hash = %q, want %q", request.Header.Get("X-Amz-Content-Sha256"), t.expectedBodyHash)
	}
	if request.Body != nil {
		validator := signingForwarderBenchmarkBodyValidator{expected: t.expectedBody}
		var buf [256]byte
		var n int64
		for {
			readN, readErr := request.Body.Read(buf[:])
			if readN > 0 {
				written, writeErr := validator.Write(buf[:readN])
				n += int64(written)
				if writeErr != nil {
					return nil, writeErr
				}
				if written != readN {
					return nil, io.ErrShortWrite
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return nil, readErr
			}
		}
		if err := request.Body.Close(); err != nil {
			return nil, err
		}
		if validator.offset != len(t.expectedBody) {
			return nil, fmt.Errorf("forwarded body length = %d, want %d", validator.offset, len(t.expectedBody))
		}
		if request.ContentLength >= 0 && n != request.ContentLength {
			return nil, fmt.Errorf("forwarded body length = %d, want %d", n, request.ContentLength)
		}
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       http.NoBody,
	}, nil
}

type signingForwarderBenchmarkResponseWriter struct {
	header http.Header
}

func (w *signingForwarderBenchmarkResponseWriter) Header() http.Header {
	return w.header
}

func (*signingForwarderBenchmarkResponseWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (*signingForwarderBenchmarkResponseWriter) WriteHeader(int) {}

// signingForwarderBenchmarkBody is reset by its owning benchmark worker before
// each request. Its no-op Close lets the controlled transport model a consumed
// request body without allocating a new fixture reader in the timed operation.
type signingForwarderBenchmarkBody struct {
	reader *strings.Reader
}

func newSigningForwarderBenchmarkBody(body string) *signingForwarderBenchmarkBody {
	return &signingForwarderBenchmarkBody{reader: strings.NewReader(body)}
}

func (b *signingForwarderBenchmarkBody) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (*signingForwarderBenchmarkBody) Close() error {
	return nil
}

func (b *signingForwarderBenchmarkBody) Reset(body string) {
	b.reader.Reset(body)
}

func newSigningPassthroughBenchmark(b testing.TB, method, path, body string, headers http.Header) (*Service, *http.Request) {
	b.Helper()

	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	store := auth.NewCredentialStore()
	store.AddCredential(signingForwarderBenchmarkAccessKey, signingForwarderBenchmarkSecretKey)

	var requestBody io.Reader
	bodyHash := ""
	if body != "" {
		requestBody = strings.NewReader(body)
		sum := sha256.Sum256([]byte(body))
		bodyHash = hex.EncodeToString(sum[:])
	}

	incomingSigner := auth.NewRequestSigner("https://client.example.com", "us-east-1")
	incomingRequest, err := incomingSigner.SignRequest(
		context.Background(),
		method,
		path,
		requestBody,
		bodyHash,
		signingForwarderBenchmarkAccessKey,
		signingForwarderBenchmarkSecretKey,
		headers,
	)
	if err != nil {
		b.Fatalf("sign incoming request: %v", err)
	}

	base := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
	base.httpClient = &http.Client{Transport: signingForwarderBenchmarkTransport{
		expectedBody:     body,
		expectedBodyHash: bodyHash,
	}}
	forwarder := &signingForwarder{
		baseForwarder: base,
		credStore:     store,
		validator:     auth.NewRequestValidator(store),
	}
	return NewService(forwarder, cache.NewDisabledCache(), config.NewDefault()), incomingRequest
}

// BenchmarkServiceHandlePassthroughSigning measures the signing-mode
// passthrough handler for an object-tagging request. The controlled transport
// keeps upstream network latency out of the request-signing workload.
func BenchmarkServiceHandlePassthroughSigning(b *testing.B) {
	service, incomingRequest := newSigningPassthroughBenchmark(
		b,
		http.MethodGet,
		signingForwarderBenchmarkGetPath,
		"",
		http.Header{"X-Amz-Expected-Bucket-Owner": {"123456789012"}},
	)
	req := incomingRequest.Clone(context.Background())
	writer := signingForwarderBenchmarkResponseWriter{header: make(http.Header)}
	b.ReportAllocs()

	for b.Loop() {
		if err := service.HandlePassthrough(&writer, req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkServiceHandlePassthroughSigningEscapedQuery measures a signed
// passthrough request whose canonical query has reserved keys and values.
func BenchmarkServiceHandlePassthroughSigningEscapedQuery(b *testing.B) {
	service, incomingRequest := newSigningPassthroughBenchmark(
		b,
		http.MethodGet,
		signingForwarderBenchmarkEscapedQueryPath,
		"",
		http.Header{"X-Amz-Expected-Bucket-Owner": {"123456789012"}},
	)
	req := incomingRequest.Clone(context.Background())
	writer := signingForwarderBenchmarkResponseWriter{header: make(http.Header)}
	b.ReportAllocs()

	for b.Loop() {
		if err := service.HandlePassthrough(&writer, req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkServiceHandlePassthroughSigningUploadPartParallel measures a
// body-bearing UploadPart request with one signing passthrough worker per
// GOMAXPROCS worker.
func BenchmarkServiceHandlePassthroughSigningUploadPartParallel(b *testing.B) {
	service, incomingRequest := newSigningPassthroughBenchmark(
		b,
		http.MethodPut,
		signingForwarderBenchmarkUploadPartPath,
		signingForwarderBenchmarkUploadPartBody,
		http.Header{
			"Content-Type":                {"application/octet-stream"},
			"X-Amz-Expected-Bucket-Owner": {"123456789012"},
		},
	)
	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		req := incomingRequest.Clone(context.Background())
		body := newSigningForwarderBenchmarkBody(signingForwarderBenchmarkUploadPartBody)
		req.Body = body
		writer := signingForwarderBenchmarkResponseWriter{header: make(http.Header)}
		for pb.Next() {
			body.Reset(signingForwarderBenchmarkUploadPartBody)
			if err := service.HandlePassthrough(&writer, req); err != nil {
				b.Fatal(err)
			}
		}
	})
}
