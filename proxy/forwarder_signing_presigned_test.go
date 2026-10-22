package proxy

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/tigrisdata/tag/auth"
)

func TestSigningForwarderRetainsPayloadHashRequirementForPresignedGET(t *testing.T) {
	const (
		accessKey = "AKIAIOSFODNN7EXAMPLE"
		secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		region    = "us-east-1"
	)

	store := auth.NewCredentialStore()
	store.AddCredential(accessKey, secretKey)
	req, err := http.NewRequest(http.MethodGet, "http://s3.example.com/private-bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}
	query := req.URL.Query()
	query.Set("X-Amz-Expires", strconv.FormatInt(int64(5*time.Minute/time.Second), 10))
	req.URL.RawQuery = query.Encode()
	signedURI, signedHeaders, err := v4.NewSigner().PresignHTTP(
		t.Context(),
		aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey},
		req,
		"UNSIGNED-PAYLOAD",
		"s3",
		region,
		time.Now().UTC().Truncate(time.Second),
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
	if got := req.URL.Query().Get("X-Amz-SignedHeaders"); got != "host" {
		t.Fatalf("SDK signed headers = %q, want host", got)
	}
	if got := req.Header.Get("X-Amz-Content-Sha256"); got != "" {
		t.Fatalf("SDK presigned GET unexpectedly has X-Amz-Content-Sha256 = %q", got)
	}

	withHash := req.Clone(t.Context())
	withHash.Header = req.Header.Clone()
	withHash.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	if access, err := auth.NewRequestValidator(store).ValidateRequest(withHash); err != nil || access != accessKey {
		t.Fatalf("same SDK URL with unsigned payload-hash header ValidateRequest() = (%q, %v), want (%q, nil)", access, err, accessKey)
	}

	forwarder := NewForwarder(store, "http://upstream.example.com", region, 1, nil, nil)
	result, _, _, err := forwarder.ValidateAndGetCredentials(req)
	if result != AuthNotValidated {
		t.Fatalf("signing-mode ValidateAndGetCredentials() result = %v, want AuthNotValidated", result)
	}
	if !errors.Is(err, auth.ErrMissingContentHash) {
		t.Fatalf("signing-mode ValidateAndGetCredentials() error = %v, want %v", err, auth.ErrMissingContentHash)
	}
}
