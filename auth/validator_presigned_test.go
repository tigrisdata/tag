package auth

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	presignedValidatorTestAccessKey = "AKIAIOSFODNN7EXAMPLE"
	presignedValidatorTestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	presignedValidatorTestRegion    = "us-east-1"
)

func newSDKPresignedRequestForValidatorTest(t *testing.T, method string, signingTime time.Time, expiry time.Duration) *http.Request {
	t.Helper()

	req, err := http.NewRequest(method, "http://s3.example.com/test-bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}
	query := req.URL.Query()
	query.Set("X-Amz-Expires", strconv.FormatInt(int64(expiry/time.Second), 10))
	req.URL.RawQuery = query.Encode()

	signedURI, signedHeaders, err := v4.NewSigner().PresignHTTP(
		t.Context(),
		aws.Credentials{
			AccessKeyID:     presignedValidatorTestAccessKey,
			SecretAccessKey: presignedValidatorTestSecretKey,
		},
		req,
		"UNSIGNED-PAYLOAD",
		"s3",
		presignedValidatorTestRegion,
		signingTime,
	)
	if err != nil {
		t.Fatal(err)
	}

	req.URL, err = url.Parse(signedURI)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = req.URL.Host
	for name, values := range signedHeaders {
		if strings.EqualFold(name, "host") {
			continue
		}
		req.Header[name] = append([]string(nil), values...)
	}
	return req
}

func clonePresignedRequestForValidatorTest(req *http.Request) *http.Request {
	clone := req.Clone(context.Background())
	clone.Header = req.Header.Clone()
	urlCopy := *req.URL
	clone.URL = &urlCopy
	return clone
}

func TestRequestValidator_SDKPresignedGETWithoutContentHash(t *testing.T) {
	credentials := NewCredentialStore()
	credentials.AddCredential(presignedValidatorTestAccessKey, presignedValidatorTestSecretKey)
	validator := NewRequestValidator(credentials)

	now := time.Now().UTC().Truncate(time.Second)
	urlOnly := newSDKPresignedRequestForValidatorTest(t, http.MethodGet, now, 5*time.Minute)
	if got := urlOnly.URL.Query().Get("X-Amz-SignedHeaders"); got != "host" {
		t.Fatalf("SDK signed headers = %q, want host", got)
	}
	if got := urlOnly.Header.Get("X-Amz-Content-Sha256"); got != "" {
		t.Fatalf("SDK presigned GET unexpectedly has X-Amz-Content-Sha256 = %q", got)
	}
	if got := urlOnly.Header.Get("Authorization"); got != "" {
		t.Fatalf("SDK presigned GET unexpectedly has Authorization = %q", got)
	}

	if accessKey, err := validator.ValidateRequest(urlOnly); err != nil || accessKey != presignedValidatorTestAccessKey {
		t.Fatalf("URL-only ValidateRequest() = (%q, %v), want (%q, nil)", accessKey, err, presignedValidatorTestAccessKey)
	}

	extraHeader := clonePresignedRequestForValidatorTest(urlOnly)
	extraHeader.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	if accessKey, err := validator.ValidateRequest(extraHeader); err != nil || accessKey != presignedValidatorTestAccessKey {
		t.Fatalf("same URL with unsigned payload-hash header ValidateRequest() = (%q, %v), want (%q, nil)", accessKey, err, presignedValidatorTestAccessKey)
	}

	t.Run("tampered signature remains rejected without the hash header", func(t *testing.T) {
		request := clonePresignedRequestForValidatorTest(urlOnly)
		query := request.URL.Query()
		signature := query.Get("X-Amz-Signature")
		if signature == "" {
			t.Fatal("SDK presigner omitted X-Amz-Signature")
		}
		if signature[0] == '0' {
			signature = "1" + signature[1:]
		} else {
			signature = "0" + signature[1:]
		}
		query.Set("X-Amz-Signature", signature)
		request.URL.RawQuery = query.Encode()

		if _, err := validator.ValidateRequest(request); !errors.Is(err, ErrSignatureMismatch) {
			t.Fatalf("tampered URL-only ValidateRequest() error = %v, want %v", err, ErrSignatureMismatch)
		}

		withHash := clonePresignedRequestForValidatorTest(request)
		withHash.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		if _, err := validator.ValidateRequest(withHash); !errors.Is(err, ErrSignatureMismatch) {
			t.Fatalf("tampered URL with payload-hash header ValidateRequest() error = %v, want %v", err, ErrSignatureMismatch)
		}
	})

	t.Run("ordinary expiry remains enforced without the hash header", func(t *testing.T) {
		request := newSDKPresignedRequestForValidatorTest(t, http.MethodGet, now.Add(-2*time.Minute), time.Second)
		if _, err := validator.ValidateRequest(request); !errors.Is(err, ErrExpiredRequest) {
			t.Fatalf("expired URL-only ValidateRequest() error = %v, want %v", err, ErrExpiredRequest)
		}

		withHash := clonePresignedRequestForValidatorTest(request)
		withHash.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		if _, err := validator.ValidateRequest(withHash); !errors.Is(err, ErrExpiredRequest) {
			t.Fatalf("expired URL with payload-hash header ValidateRequest() error = %v, want %v", err, ErrExpiredRequest)
		}
	})

	t.Run("non-GET query authentication still requires the hash", func(t *testing.T) {
		request := newSDKPresignedRequestForValidatorTest(t, http.MethodPut, now, 5*time.Minute)
		if _, err := validator.ValidateRequest(request); !errors.Is(err, ErrMissingContentHash) {
			t.Fatalf("presigned PUT ValidateRequest() error = %v, want %v", err, ErrMissingContentHash)
		}
	})

	t.Run("header authentication still requires the hash", func(t *testing.T) {
		request, err := NewRequestSigner("http://s3.example.com", presignedValidatorTestRegion).SignRequest(
			context.Background(),
			http.MethodGet,
			"/test-bucket/object",
			nil,
			"",
			presignedValidatorTestAccessKey,
			presignedValidatorTestSecretKey,
			http.Header{},
		)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Del("X-Amz-Content-Sha256")
		if _, err := validator.ValidateRequest(request); !errors.Is(err, ErrMissingContentHash) {
			t.Fatalf("header-auth GET ValidateRequest() error = %v, want %v", err, ErrMissingContentHash)
		}
	})
}
