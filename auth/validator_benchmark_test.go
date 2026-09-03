package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	validatorBenchmarkAccessKey = "AKIAIOSFODNN7EXAMPLE"
	validatorBenchmarkSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

func newValidatorBenchmarkHeaders(t testing.TB, metadataHeaders, metadataBytes int) http.Header {
	t.Helper()

	if metadataHeaders == 0 && metadataBytes != 0 {
		t.Fatalf("metadataBytes = %d with no metadata headers", metadataBytes)
	}
	if metadataHeaders > 0 && metadataBytes%metadataHeaders != 0 {
		t.Fatalf("metadataBytes = %d is not divisible by metadataHeaders = %d", metadataBytes, metadataHeaders)
	}

	headers := make(http.Header, metadataHeaders)
	if metadataHeaders > 0 {
		value := strings.Repeat("m", metadataBytes/metadataHeaders)
		for i := 0; i < metadataHeaders; i++ {
			headers.Set(fmt.Sprintf("X-Amz-Meta-Benchmark-%02d", i), value)
		}
	}
	return headers
}

func newValidatorBenchmarkRequest(t testing.TB, metadataHeaders, metadataBytes int) (*RequestValidator, *http.Request) {
	t.Helper()

	headers := newValidatorBenchmarkHeaders(t, metadataHeaders, metadataBytes)

	body := "benchmark request body"
	bodyHashSum := sha256.Sum256([]byte(body))
	bodyHash := hex.EncodeToString(bodyHashSum[:])
	req, err := NewRequestSigner("https://s3.amazonaws.com", "us-east-1").SignRequest(
		context.Background(),
		http.MethodPut,
		"/benchmark-bucket/metadata-object",
		strings.NewReader(body),
		bodyHash,
		validatorBenchmarkAccessKey,
		validatorBenchmarkSecretKey,
		headers,
	)
	if err != nil {
		t.Fatalf("SignRequest() error = %v", err)
	}

	authInfo, err := ParseAuthInfo(req)
	if err != nil {
		t.Fatalf("ParseAuthInfo() error = %v", err)
	}
	keys := NewDerivedKeyStore(time.Hour)
	keys.Store(
		validatorBenchmarkAccessKey,
		authInfo.Date,
		authInfo.Region,
		deriveSigningKey(validatorBenchmarkSecretKey, authInfo.Date, authInfo.Region),
	)
	validator := NewRequestValidator(keys)
	if _, err := validator.ValidateRequest(req); err != nil {
		t.Fatalf("ValidateRequest() error = %v", err)
	}

	return validator, req
}

func newPresignedValidatorBenchmarkRequest(t testing.TB, metadataHeaders, metadataBytes int) (*RequestValidator, *http.Request) {
	t.Helper()

	headers := newValidatorBenchmarkHeaders(t, metadataHeaders, metadataBytes)
	headers.Set("X-Amz-Content-Sha256", unsignedPayload)
	requestTime := time.Now().UTC()
	req, err := http.NewRequest(http.MethodPut, "https://s3.amazonaws.com/benchmark-bucket/metadata-object", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header = headers

	signedHeaders := make([]string, 0, len(headers)+1)
	signedHeaders = append(signedHeaders, "host")
	for name := range headers {
		signedHeaders = append(signedHeaders, strings.ToLower(name))
	}
	sort.Strings(signedHeaders)

	query := req.URL.Query()
	query.Set("X-Amz-Algorithm", algorithm)
	query.Set("X-Amz-Credential", validatorBenchmarkAccessKey+"/"+requestTime.Format(shortTimeFormat)+"/us-east-1/"+service+"/"+terminationString)
	query.Set("X-Amz-Date", requestTime.Format(TimeFormat))
	query.Set("X-Amz-Expires", "900")
	query.Set("X-Amz-SignedHeaders", strings.Join(signedHeaders, ";"))
	req.URL.RawQuery = query.Encode()

	authInfo, err := ParseAuthInfo(req)
	if err != nil {
		t.Fatalf("ParseAuthInfo() error = %v", err)
	}
	signingKey := deriveSigningKey(validatorBenchmarkSecretKey, authInfo.Date, authInfo.Region)
	keys := NewDerivedKeyStore(time.Hour)
	keys.Store(validatorBenchmarkAccessKey, authInfo.Date, authInfo.Region, signingKey)
	validator := NewRequestValidator(keys)

	query.Set("X-Amz-Signature", validator.computePresignedSignature(req, authInfo, signingKey, unsignedPayload, requestTime))
	req.URL.RawQuery = query.Encode()
	if _, err := validator.ValidateRequest(req); err != nil {
		t.Fatalf("ValidateRequest() error = %v", err)
	}

	return validator, req
}

func TestRequestValidatorCanonicalRequests(t *testing.T) {
	validator := NewRequestValidator(NewCredentialStore())

	t.Run("header based", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPut, "https://s3.example.test/bucket/object?z=last&a=first", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header["X-Amz-Meta-Labels"] = []string{"  alpha  ", "\tbeta\t"}
		req.Header.Set("X-Amz-Date", "20260211T055514Z")

		got := validator.buildCanonicalRequest(
			req,
			[]string{"host", "x-amz-meta-labels", "x-amz-date"},
			"payload-hash",
		)
		want := "PUT\n" +
			"/bucket/object\n" +
			"a=first&z=last\n" +
			"host:s3.example.test\n" +
			"x-amz-meta-labels:alpha,beta\n" +
			"x-amz-date:20260211T055514Z\n" +
			"\n" +
			"host;x-amz-meta-labels;x-amz-date\n" +
			"payload-hash"
		if got != want {
			t.Errorf("buildCanonicalRequest() = %q, want %q", got, want)
		}
		if values := req.Header.Values("X-Amz-Meta-Labels"); len(values) != 2 || values[0] != "alpha" || values[1] != "beta" {
			t.Errorf("X-Amz-Meta-Labels after canonicalization = %q, want [alpha beta]", values)
		}
	})

	t.Run("presigned", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "https://s3.example.test/bucket/presigned?z=last&X-Amz-Signature=signature&a=first", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header["X-Amz-Meta-Labels"] = []string{"  alpha  ", "\tbeta\t"}

		got := validator.buildCanonicalRequestPresigned(
			req,
			[]string{"host", "x-amz-meta-labels"},
			unsignedPayload,
		)
		want := "GET\n" +
			"/bucket/presigned\n" +
			"a=first&z=last\n" +
			"host:s3.example.test\n" +
			"x-amz-meta-labels:alpha,beta\n" +
			"\n" +
			"host;x-amz-meta-labels\n" +
			unsignedPayload
		if got != want {
			t.Errorf("buildCanonicalRequestPresigned() = %q, want %q", got, want)
		}
		if got := req.URL.Query().Get("X-Amz-Signature"); got != "signature" {
			t.Errorf("X-Amz-Signature after canonicalization = %q, want signature", got)
		}
		if values := req.Header.Values("X-Amz-Meta-Labels"); len(values) != 2 || values[0] != "alpha" || values[1] != "beta" {
			t.Errorf("X-Amz-Meta-Labels after canonicalization = %q, want [alpha beta]", values)
		}
	})
}

func TestRequestValidatorValidateRequestWithMetadataHeaders(t *testing.T) {
	validator, req := newValidatorBenchmarkRequest(t, 16, 2048)
	if accessKey, err := validator.ValidateRequest(req); err != nil {
		t.Errorf("ValidateRequest() error = %v", err)
	} else if accessKey != validatorBenchmarkAccessKey {
		t.Errorf("ValidateRequest() accessKey = %q, want %q", accessKey, validatorBenchmarkAccessKey)
	}
}

func BenchmarkRequestValidatorValidateSignedPUT(b *testing.B) {
	cases := []struct {
		name            string
		metadataHeaders int
		metadataBytes   int
	}{
		{name: "small", metadataHeaders: 0, metadataBytes: 0},
		{name: "metadata_2KiB_one_header", metadataHeaders: 1, metadataBytes: 2048},
		{name: "metadata_2KiB_sixteen_headers", metadataHeaders: 16, metadataBytes: 2048},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			validator, req := newValidatorBenchmarkRequest(b, tc.metadataHeaders, tc.metadataBytes)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := validator.ValidateRequest(req); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestRequestValidatorValidatePresignedRequestWithMetadataHeaders(t *testing.T) {
	validator, req := newPresignedValidatorBenchmarkRequest(t, 16, 2048)
	if accessKey, err := validator.ValidateRequest(req); err != nil {
		t.Errorf("ValidateRequest() error = %v", err)
	} else if accessKey != validatorBenchmarkAccessKey {
		t.Errorf("ValidateRequest() accessKey = %q, want %q", accessKey, validatorBenchmarkAccessKey)
	}
}

func BenchmarkRequestValidatorValidatePresignedPUT(b *testing.B) {
	cases := []struct {
		name            string
		metadataHeaders int
		metadataBytes   int
	}{
		{name: "small", metadataHeaders: 0, metadataBytes: 0},
		{name: "metadata_2KiB_one_header", metadataHeaders: 1, metadataBytes: 2048},
		{name: "metadata_2KiB_sixteen_headers", metadataHeaders: 16, metadataBytes: 2048},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			validator, req := newPresignedValidatorBenchmarkRequest(b, tc.metadataHeaders, tc.metadataBytes)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := validator.ValidateRequest(req); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
