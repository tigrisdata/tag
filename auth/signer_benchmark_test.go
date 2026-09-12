package auth

import (
	"context"
	"net/http"
	"testing"
)

const signerBenchmarkPath = "/benchmark-bucket/object%2Fwith space+and%25?prefix=one%2Ftwo&marker=a%20b&x=100%25"

var signerBenchmarkHeaders = http.Header{
	"Range":                 {"bytes=0-1048575"},
	"X-Amz-Meta-Request-Id": {"benchmark-request"},
}

func newSignerBenchmark() *RequestSigner {
	return NewRequestSigner("https://upstream.example.com", "us-east-1")
}

var signerBenchmarkCopyableNonSigningHeaders = []struct {
	key   string
	value string
}{
	{"Content-Encoding", "gzip"},
	{"Content-Disposition", "attachment"},
	{"Content-Language", "en"},
	{"Cache-Control", "no-cache"},
	{"Expires", "Wed, 21 Oct 2015 07:28:00 GMT"},
	{"Content-Md5", "Q2hlY2sgSW50ZWdyaXR5IQ=="},
	{"Range", "bytes=0-1048575"},
	{"If-Match", "\"benchmark-etag\""},
	{"If-None-Match", "\"other-benchmark-etag\""},
	{"Tigris-Force-Delete", "true"},
}

var signerBenchmarkCopiedHeaderCases = []struct {
	name  string
	count int
}{
	{"copied_headers_0", 0},
	{"copied_headers_1", 1},
	{"copied_headers_4", 4},
	{"copied_headers_10", 10},
}

const (
	signerBenchmarkHeaderDate       = "20260101T000000Z"
	signerBenchmarkCanonicalHeaders = "host:upstream.example.com\n" +
		"x-amz-content-sha256:" + emptyBodyHash + "\n" +
		"x-amz-date:" + signerBenchmarkHeaderDate + "\n" +
		"x-amz-meta-request-id:benchmark-request\n"
	signerBenchmarkSignedHeaders = "host;x-amz-content-sha256;x-amz-date;x-amz-meta-request-id"
)

func newSignerBenchmarkHeaders(b testing.TB, copiedHeaderCount int) http.Header {
	b.Helper()

	headers := http.Header{
		"X-Amz-Meta-Request-Id": {"benchmark-request"},
	}
	for _, header := range signerBenchmarkCopyableNonSigningHeaders[:copiedHeaderCount] {
		if !shouldCopyHeader(header.key) {
			b.Fatalf("header %q is not copied by SignRequest", header.key)
		}
		headers.Set(header.key, header.value)
	}
	return headers
}

func newSignerBenchmarkCanonicalHeadersRequest(b testing.TB, copiedHeaderCount int) *http.Request {
	b.Helper()

	req, err := http.NewRequest(http.MethodGet, "https://upstream.example.com/benchmark", nil)
	if err != nil {
		b.Fatal(err)
	}
	req.Header = newSignerBenchmarkHeaders(b, copiedHeaderCount)
	req.Header.Set("X-Amz-Date", signerBenchmarkHeaderDate)
	req.Header.Set("X-Amz-Content-Sha256", emptyBodyHash)
	req.Header.Set("Host", req.URL.Host)
	return req
}

func BenchmarkRequestSignerBuildCanonicalHeadersCopiedHeaders(b *testing.B) {
	signer := newSignerBenchmark()

	for _, tc := range signerBenchmarkCopiedHeaderCases {
		b.Run(tc.name, func(b *testing.B) {
			req := newSignerBenchmarkCanonicalHeadersRequest(b, tc.count)
			canonicalHeaders, signedHeaders := signer.buildCanonicalHeaders(req)
			if canonicalHeaders != signerBenchmarkCanonicalHeaders {
				b.Fatalf("canonical headers = %q, want %q", canonicalHeaders, signerBenchmarkCanonicalHeaders)
			}
			if signedHeaders != signerBenchmarkSignedHeaders {
				b.Fatalf("signed headers = %q, want %q", signedHeaders, signerBenchmarkSignedHeaders)
			}

			b.ReportAllocs()
			for b.Loop() {
				signer.buildCanonicalHeaders(req)
			}
		})
	}
}

func BenchmarkRequestSignerSignRequestCopiedHeaders(b *testing.B) {
	signer := newSignerBenchmark()
	ctx := context.Background()

	for _, tc := range signerBenchmarkCopiedHeaderCases {
		b.Run(tc.name, func(b *testing.B) {
			headers := newSignerBenchmarkHeaders(b, tc.count)
			req, err := signer.SignRequest(
				ctx,
				http.MethodGet,
				signerBenchmarkPath,
				nil,
				"",
				requestSignerTestAccessKey,
				requestSignerTestSecretKey,
				headers,
			)
			if err != nil {
				b.Fatal(err)
			}
			if got := req.Header.Get("Authorization"); got == "" {
				b.Fatal("Authorization header is missing")
			}

			b.ReportAllocs()
			for b.Loop() {
				if _, err := signer.SignRequest(
					ctx,
					http.MethodGet,
					signerBenchmarkPath,
					nil,
					"",
					requestSignerTestAccessKey,
					requestSignerTestSecretKey,
					headers,
				); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRequestValidatorBuildCanonicalQueryStringReservedBytes(b *testing.B) {
	validator := NewRequestValidator(NewCredentialStore())
	query := newReservedCanonicalQueryValues()
	if got := validator.buildCanonicalQueryString(newReservedCanonicalQueryValues()); got != reservedCanonicalQueryString {
		b.Fatalf("buildCanonicalQueryString() = %q, want %q", got, reservedCanonicalQueryString)
	}

	b.ReportAllocs()
	for b.Loop() {
		validator.buildCanonicalQueryString(query)
	}
}

func BenchmarkRequestSignerSignRequest(b *testing.B) {
	signer := newSignerBenchmark()
	ctx := context.Background()
	headers := signerBenchmarkHeaders.Clone()
	b.ReportAllocs()

	for b.Loop() {
		if _, err := signer.SignRequest(
			ctx,
			http.MethodGet,
			signerBenchmarkPath,
			nil,
			"",
			requestSignerTestAccessKey,
			requestSignerTestSecretKey,
			headers,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRequestSignerSignRequestParallel(b *testing.B) {
	signer := newSignerBenchmark()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		headers := signerBenchmarkHeaders.Clone()
		for pb.Next() {
			if _, err := signer.SignRequest(
				ctx,
				http.MethodGet,
				signerBenchmarkPath,
				nil,
				"",
				requestSignerTestAccessKey,
				requestSignerTestSecretKey,
				headers,
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}
