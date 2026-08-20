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
