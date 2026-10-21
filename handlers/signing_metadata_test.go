package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

const (
	signingMetadataAccessKey = "AKIAIOSFODNN7EXAMPLE"
	signingMetadataSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	signingMetadataRegion    = "us-east-1"
	signingMetadataHeader    = "X-Amz-Meta-Label"
	signingMetadataBucket    = "signed-metadata-bucket"
	signingMetadataBody      = "metadata request body"
)

type signingMetadataObject struct {
	values []string
	body   []byte
}

type signingMetadataUpstream struct {
	requests atomic.Int32
	mu       sync.RWMutex
	objects  map[string]signingMetadataObject
}

func newSigningMetadataUpstream() *signingMetadataUpstream {
	return &signingMetadataUpstream{objects: make(map[string]signingMetadataObject)}
}

func (u *signingMetadataUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.requests.Add(1)
	if err := verifySigningMetadataRequest(r); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	key, ok := strings.CutPrefix(r.URL.Path, "/"+signingMetadataBucket+"/")
	if !ok || key == "" {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if got, want := r.Header.Get("X-Amz-Content-Sha256"), sha256Hex(body); got != want {
			http.Error(w, fmt.Sprintf("payload hash %q does not match body hash %q", got, want), http.StatusBadRequest)
			return
		}

		u.mu.Lock()
		u.objects[key] = signingMetadataObject{
			values: append([]string(nil), r.Header.Values(signingMetadataHeader)...),
			body:   append([]byte(nil), body...),
		}
		u.mu.Unlock()
		w.Header().Set("ETag", `"signed-metadata-etag"`)
		w.WriteHeader(http.StatusOK)

	case http.MethodHead:
		u.mu.RLock()
		object, found := u.objects[key]
		u.mu.RUnlock()
		if !found {
			http.NotFound(w, r)
			return
		}
		for _, value := range object.values {
			w.Header().Add(signingMetadataHeader, value)
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(object.body)))
		w.Header().Set("ETag", `"signed-metadata-etag"`)
		w.WriteHeader(http.StatusOK)

	default:
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
	}
}

// TestSigningModePutPreservesCanonicalMetadataSpacing checks the HTTP PUT route
// with an AWS SDK signer and an independently SDK-verified upstream. A signed
// HEAD verifies that canonicalization did not rewrite the stored metadata.
func TestSigningModePutPreservesCanonicalMetadataSpacing(t *testing.T) {
	upstreamState := newSigningMetadataUpstream()
	upstream := httptest.NewServer(upstreamState)
	t.Cleanup(upstream.Close)

	cfg := config.NewDefault()
	cfg.Mode = config.ModeSigning
	cfg.Upstream.TransparentProxy = nil
	cfg.Upstream.Endpoint = upstream.URL
	cfg.Upstream.Region = signingMetadataRegion
	cfg.Cache.SetEnabled(false)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate signing-mode config: %v", err)
	}

	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signingMetadataAccessKey, signingMetadataSecretKey)
	forwarder := proxy.NewForwarder(
		credentials,
		cfg.Upstream.Endpoint,
		cfg.Upstream.Region,
		cfg.Upstream.MaxIdleConnsPerHost,
		nil,
		nil,
	)
	service := proxy.NewService(forwarder, cache.NewDisabledCache(), cfg)
	gateway := httptest.NewServer(NewServer(service, "127.0.0.1", 0, false, 0).Router())
	t.Cleanup(gateway.Close)

	client := gateway.Client()
	signer := v4.NewSigner(func(options *v4.SignerOptions) {
		options.DisableURIPathEscaping = true
	})
	testCases := []struct {
		name   string
		values []string
	}{
		{name: "ordinary space", values: []string{"alpha beta"}},
		{name: "ordinary multiple values", values: []string{"alpha beta", "gamma delta"}},
		{name: "repeated spaces in one value", values: []string{"alpha  beta"}},
		{name: "repeated spaces in multiple values", values: []string{"alpha  beta", "gamma   delta"}},
		{name: "single interior tab", values: []string{"alpha\tbeta"}},
		{name: "repeated interior tabs", values: []string{"alpha\t\tbeta"}},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := fmt.Sprintf("object-%d", i)
			put := newSigningMetadataRequest(t, signer, http.MethodPut, gateway.URL, key, tc.values, []byte(signingMetadataBody))
			putResponse, err := client.Do(put)
			if err != nil {
				t.Fatalf("PUT request: %v", err)
			}
			putBody, readErr := io.ReadAll(putResponse.Body)
			closeErr := putResponse.Body.Close()
			if readErr != nil {
				t.Fatalf("read PUT response: %v", readErr)
			}
			if closeErr != nil {
				t.Fatalf("close PUT response: %v", closeErr)
			}
			if putResponse.StatusCode != http.StatusOK {
				t.Fatalf("PUT status = %d, want %d; response: %s", putResponse.StatusCode, http.StatusOK, putBody)
			}

			head := newSigningMetadataRequest(t, signer, http.MethodHead, gateway.URL, key, nil, nil)
			headResponse, err := client.Do(head)
			if err != nil {
				t.Fatalf("HEAD request: %v", err)
			}
			headBody, readErr := io.ReadAll(headResponse.Body)
			closeErr = headResponse.Body.Close()
			if readErr != nil {
				t.Fatalf("read HEAD response: %v", readErr)
			}
			if closeErr != nil {
				t.Fatalf("close HEAD response: %v", closeErr)
			}
			if headResponse.StatusCode != http.StatusOK {
				t.Fatalf("HEAD status = %d, want %d; response: %s", headResponse.StatusCode, http.StatusOK, headBody)
			}
			if got := headResponse.Header.Values(signingMetadataHeader); !reflect.DeepEqual(got, tc.values) {
				t.Errorf("stored metadata = %#v, want %#v", got, tc.values)
			}
		})
	}

	t.Run("tampered metadata is rejected", func(t *testing.T) {
		before := upstreamState.requests.Load()
		req := newSigningMetadataRequest(t, signer, http.MethodPut, gateway.URL, "tampered-metadata", []string{"alpha  beta"}, []byte(signingMetadataBody))
		req.Header.Set(signingMetadataHeader, "alpha  gamma")
		assertSigningMetadataRejection(t, client, req)
		if got := upstreamState.requests.Load(); got != before {
			t.Errorf("upstream requests = %d, want %d after tampered metadata", got, before)
		}
	})

	t.Run("tampered signature is rejected", func(t *testing.T) {
		before := upstreamState.requests.Load()
		req := newSigningMetadataRequest(t, signer, http.MethodPut, gateway.URL, "tampered-signature", []string{"alpha  beta"}, []byte(signingMetadataBody))
		authorization := req.Header.Get("Authorization")
		marker := "Signature="
		index := strings.LastIndex(authorization, marker)
		if index < 0 || index+len(marker) == len(authorization) {
			t.Fatalf("malformed generated authorization header: %q", authorization)
		}
		signatureIndex := index + len(marker)
		replacement := byte('0')
		if authorization[signatureIndex] == replacement {
			replacement = '1'
		}
		req.Header.Set("Authorization", authorization[:signatureIndex]+string(replacement)+authorization[signatureIndex+1:])
		assertSigningMetadataRejection(t, client, req)
		if got := upstreamState.requests.Load(); got != before {
			t.Errorf("upstream requests = %d, want %d after tampered signature", got, before)
		}
	})
}

func newSigningMetadataRequest(t *testing.T, signer *v4.Signer, method, endpoint, key string, metadata []string, body []byte) *http.Request {
	t.Helper()

	var requestBody io.Reader
	if body != nil {
		requestBody = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(
		t.Context(),
		method,
		endpoint+"/"+signingMetadataBucket+"/"+key,
		requestBody,
	)
	if err != nil {
		t.Fatalf("create %s request: %v", method, err)
	}
	for _, value := range metadata {
		request.Header.Add(signingMetadataHeader, value)
	}
	payloadHash := sha256Hex(body)
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if err := signer.SignHTTP(
		t.Context(),
		aws.Credentials{AccessKeyID: signingMetadataAccessKey, SecretAccessKey: signingMetadataSecretKey},
		request,
		payloadHash,
		"s3",
		signingMetadataRegion,
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("sign %s request: %v", method, err)
	}
	return request
}

func assertSigningMetadataRejection(t *testing.T, client *http.Client, request *http.Request) {
	t.Helper()

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("send tampered request: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read tampered response: %v", err)
	}
	if response.StatusCode != http.StatusForbidden {
		t.Errorf("tampered request status = %d, want %d; response: %s", response.StatusCode, http.StatusForbidden, body)
	}
}

func verifySigningMetadataRequest(request *http.Request) error {
	authorization := request.Header.Get("Authorization")
	signedHeaders, err := signedHeadersFromAuthorization(authorization)
	if err != nil {
		return err
	}

	verificationRequest := request.Clone(request.Context())
	verificationRequest.Header = make(http.Header, len(signedHeaders))
	for _, name := range signedHeaders {
		if name == "host" {
			continue
		}
		values := request.Header.Values(name)
		if len(values) == 0 {
			return fmt.Errorf("signed header %q is missing", name)
		}
		verificationRequest.Header[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
	}
	if !containsHeaderName(signedHeaders, "content-length") {
		verificationRequest.ContentLength = 0
	}

	date, err := time.Parse("20060102T150405Z", request.Header.Get("X-Amz-Date"))
	if err != nil {
		return fmt.Errorf("parse X-Amz-Date: %w", err)
	}
	payloadHash := request.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		return fmt.Errorf("missing X-Amz-Content-Sha256")
	}
	signer := v4.NewSigner(func(options *v4.SignerOptions) {
		options.DisableURIPathEscaping = true
	})
	if err := signer.SignHTTP(
		context.Background(),
		aws.Credentials{AccessKeyID: signingMetadataAccessKey, SecretAccessKey: signingMetadataSecretKey},
		verificationRequest,
		payloadHash,
		"s3",
		signingMetadataRegion,
		date,
	); err != nil {
		return fmt.Errorf("SDK signature check: %w", err)
	}
	if expected := verificationRequest.Header.Get("Authorization"); expected != authorization {
		return fmt.Errorf("signature mismatch: got %q, SDK expected %q", authorization, expected)
	}
	return nil
}

func signedHeadersFromAuthorization(authorization string) ([]string, error) {
	for _, part := range strings.Split(authorization, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && name == "SignedHeaders" {
			return strings.Split(value, ";"), nil
		}
	}
	return nil, fmt.Errorf("authorization has no SignedHeaders field")
}

func containsHeaderName(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
