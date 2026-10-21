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
	return newSDKPresignedRequestWithRawExpires(t, method, signingTime, []string{strconv.FormatInt(int64(expiry/time.Second), 10)})
}

// newSDKPresignedRequestWithRawExpires presigns with the SDK over whatever
// X-Amz-Expires values are given (nil omits the parameter), so malformed
// lifetimes reach the validator inside an otherwise well-formed URL.
func newSDKPresignedRequestWithRawExpires(t *testing.T, method string, signingTime time.Time, expires []string) *http.Request {
	t.Helper()

	req, err := http.NewRequest(method, "http://s3.example.com/test-bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}
	query := req.URL.Query()
	for _, value := range expires {
		query.Add("X-Amz-Expires", value)
	}
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

func TestRequestValidator_PresignedLifetime(t *testing.T) {
	credentials := NewCredentialStore()
	credentials.AddCredential(presignedValidatorTestAccessKey, presignedValidatorTestSecretKey)
	validator := NewRequestValidator(credentials)

	// Dates are offsets from the time the table is built. The validator's own
	// clock only moves forward from here, so every case is deterministic: a
	// case meant to pass sits well inside its window, and a case meant to fail
	// sits well outside it. The exactly-15-minutes-ahead case is accepted
	// because the skew the validator measures is at most 15m minus the test's
	// own elapsed time, never more.
	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name    string
		date    time.Duration // X-Amz-Date relative to now
		expires []string      // X-Amz-Expires values; nil omits the parameter
		wantErr error         // nil means the request validates
	}{
		{name: "missing expires", date: 0, expires: nil, wantErr: ErrInvalidExpires},
		{name: "duplicate expires", date: 0, expires: []string{"300", "300"}, wantErr: ErrInvalidExpires},
		{name: "non-integer expires", date: 0, expires: []string{"5m"}, wantErr: ErrInvalidExpires},
		{name: "empty expires", date: 0, expires: []string{""}, wantErr: ErrInvalidExpires},
		{name: "zero expires", date: 0, expires: []string{"0"}, wantErr: ErrInvalidExpires},
		{name: "negative expires", date: 0, expires: []string{"-1"}, wantErr: ErrInvalidExpires},
		{name: "expires one past the cap", date: 0, expires: []string{"604801"}, wantErr: ErrInvalidExpires},
		{name: "expires at int64 max", date: 0, expires: []string{"9223372036854775807"}, wantErr: ErrInvalidExpires},
		{name: "expires beyond int64", date: 0, expires: []string{"9223372036854775808"}, wantErr: ErrInvalidExpires},
		{name: "expired", date: -2 * time.Minute, expires: []string{"1"}, wantErr: ErrExpiredRequest},
		{name: "expired long-lived url", date: -8 * 24 * time.Hour, expires: []string{"604800"}, wantErr: ErrExpiredRequest},
		{name: "dated beyond future skew", date: 16 * time.Minute, expires: []string{"300"}, wantErr: ErrExpiredRequest},
		{name: "dated far in the future with maximum expires", date: 24 * time.Hour, expires: []string{"604800"}, wantErr: ErrExpiredRequest},
		{name: "minimum expires", date: time.Minute, expires: []string{"1"}, wantErr: nil},
		{name: "maximum expires", date: 0, expires: []string{"604800"}, wantErr: nil},
		{name: "dated exactly at the future skew limit", date: maxRequestAge, expires: []string{"300"}, wantErr: nil},
		{name: "typical sdk url", date: 0, expires: []string{"300"}, wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := newSDKPresignedRequestWithRawExpires(t, http.MethodGet, now.Add(tt.date), tt.expires)
			accessKey, err := validator.ValidateRequest(request)
			if tt.wantErr == nil {
				if err != nil || accessKey != presignedValidatorTestAccessKey {
					t.Fatalf("ValidateRequest() = (%q, %v), want (%q, nil)", accessKey, err, presignedValidatorTestAccessKey)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateRequest() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
