package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	requestSignerTestAccessKey = "AKIAIOSFODNN7EXAMPLE"
	requestSignerTestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

// legacySignerRequest reproduces the URL construction SignRequest used before
// caching the parsed endpoint. It keeps the compatibility assertions below
// independent from the optimized request construction.
func legacySignerRequest(t testing.TB, ctx context.Context, method, endpoint, path string, body io.Reader) *http.Request {
	t.Helper()

	baseURL, err := url.Parse(strings.TrimSuffix(endpoint, "/"))
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}

	pathPart := path
	queryPart := ""
	if idx := strings.Index(path, "?"); idx != -1 {
		pathPart = path[:idx]
		queryPart = path[idx+1:]
	}
	baseURL.Path = pathPart
	baseURL.RawQuery = queryPart

	req, err := http.NewRequestWithContext(ctx, method, baseURL.String(), body)
	if err != nil {
		t.Fatalf("create legacy request: %v", err)
	}
	return req
}

func TestRequestSignerSignRequestMatchesLegacyURLAndSignature(t *testing.T) {
	const (
		endpoint = "https://upstream.example.com"
		path     = "/benchmark-bucket/object%2Fwith space+and%25?prefix=one%2Ftwo&marker=a%20b&x=100%25#raw-fragment"
		body     = "request body"
	)

	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "request context")
	headers := http.Header{
		"Content-Type":          {"application/octet-stream"},
		"X-Amz-Meta-Request-Id": {"test-request"},
	}
	bodyHashSum := sha256.Sum256([]byte(body))
	bodyHash := hex.EncodeToString(bodyHashSum[:])

	signer := NewRequestSigner(endpoint, "us-east-1")
	got, err := signer.SignRequest(
		ctx,
		http.MethodPut,
		path,
		strings.NewReader(body),
		bodyHash,
		requestSignerTestAccessKey,
		requestSignerTestSecretKey,
		headers,
	)
	if err != nil {
		t.Fatalf("SignRequest() error = %v", err)
	}

	want := legacySignerRequest(t, ctx, http.MethodPut, endpoint, path, strings.NewReader(body))
	for key, values := range headers {
		if shouldCopyHeader(key) {
			want.Header[key] = append([]string(nil), values...)
		}
	}
	want.Header.Set("X-Amz-Date", got.Header.Get("X-Amz-Date"))
	want.Header.Set("X-Amz-Content-Sha256", bodyHash)
	want.Header.Set("Host", want.URL.Host)
	signingTime, err := time.Parse(TimeFormat, got.Header.Get("X-Amz-Date"))
	if err != nil {
		t.Fatalf("parse signing time: %v", err)
	}
	if err := signer.signHTTP(want, requestSignerTestAccessKey, requestSignerTestSecretKey, bodyHash, signingTime); err != nil {
		t.Fatalf("sign legacy request: %v", err)
	}

	if gotURL, wantURL := got.URL.String(), want.URL.String(); gotURL != wantURL {
		t.Errorf("URL = %q, want legacy URL %q", gotURL, wantURL)
	}
	if got.Host != want.Host {
		t.Errorf("Host = %q, want legacy host %q", got.Host, want.Host)
	}
	if gotAuthorization, wantAuthorization := got.Header.Get("Authorization"), want.Header.Get("Authorization"); gotAuthorization != wantAuthorization {
		t.Errorf("Authorization = %q, want legacy signature %q", gotAuthorization, wantAuthorization)
	}
	if got.Context().Value(contextKey{}) != "request context" {
		t.Error("request context was not preserved")
	}
	if got.ContentLength != int64(len(body)) {
		t.Errorf("ContentLength = %d, want %d", got.ContentLength, len(body))
	}
	if got.GetBody == nil {
		t.Fatal("GetBody is nil")
	}
	gotBody, err := got.GetBody()
	if err != nil {
		t.Fatalf("GetBody() error = %v", err)
	}
	defer gotBody.Close()
	gotBodyBytes, err := io.ReadAll(gotBody)
	if err != nil {
		t.Fatalf("read GetBody: %v", err)
	}
	if string(gotBodyBytes) != body {
		t.Errorf("GetBody() = %q, want %q", gotBodyBytes, body)
	}

	store := NewCredentialStore()
	store.AddCredential(requestSignerTestAccessKey, requestSignerTestSecretKey)
	if _, err := NewRequestValidator(store).ValidateRequest(got); err != nil {
		t.Errorf("ValidateRequest() error = %v", err)
	}
}

func TestRequestSignerSignRequestMatchesLegacyURLFields(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		path     string
	}{
		{
			name:     "empty port",
			endpoint: "https://upstream.example.com:",
			path:     "/bucket/object%2Fwith space?prefix=one%2Ftwo",
		},
		{
			name:     "escaped endpoint path",
			endpoint: "https://upstream.example.com/%2F",
			path:     "/bucket/object%2Fwith space?prefix=one%2Ftwo",
		},
		{
			name:     "endpoint force query with request query",
			endpoint: "https://upstream.example.com/?",
			path:     "/bucket/object?prefix=one%2Ftwo",
		},
		{
			name:     "literal fragment in query",
			endpoint: "https://upstream.example.com",
			path:     "/bucket/object?prefix=one#fragment",
		},
		{
			name:     "multiple literal fragments in query",
			endpoint: "https://upstream.example.com",
			path:     "/bucket/object?prefix=one#first#second",
		},
		{
			name:     "literal fragment with escaped endpoint path",
			endpoint: "https://upstream.example.com/%2F",
			path:     "/bucket/object?prefix=one#fragment",
		},
		{
			name:     "literal fragment with endpoint fragment",
			endpoint: "https://upstream.example.com#endpoint-fragment",
			path:     "/bucket/object?prefix=one#request-fragment",
		},
		{
			name:     "endpoint force query without request query",
			endpoint: "https://upstream.example.com/?",
			path:     "/bucket/object?",
		},
		{
			name:     "relative request path",
			endpoint: "https://upstream.example.com",
			path:     "bucket/object%2Fwith space?prefix=one%2Ftwo",
		},
		{
			name:     "raw fragment request target",
			endpoint: "https://upstream.example.com",
			path:     "#fragment-only",
		},
		{
			name:     "opaque endpoint",
			endpoint: "http:opaque",
			path:     "/bucket/object?prefix=one%2Ftwo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewRequestSigner(tt.endpoint, "us-east-1").SignRequest(
				t.Context(),
				http.MethodGet,
				tt.path,
				nil,
				"",
				requestSignerTestAccessKey,
				requestSignerTestSecretKey,
				nil,
			)
			if err != nil {
				t.Fatalf("SignRequest() error = %v", err)
			}

			want := legacySignerRequest(t, t.Context(), http.MethodGet, tt.endpoint, tt.path, nil)
			if !reflect.DeepEqual(got.URL, want.URL) {
				t.Errorf("URL = %#v, want legacy URL %#v", got.URL, want.URL)
			}
			if got.Host != want.Host {
				t.Errorf("Host = %q, want legacy host %q", got.Host, want.Host)
			}
		})
	}
}

func TestRequestSignerSignRequestRawQueryFragmentSemantics(t *testing.T) {
	signer := NewRequestSigner("https://upstream.example.com", "us-east-1")
	tests := []struct {
		name       string
		path       string
		requestURI string
		rawQuery   string
		fragment   string
	}{
		{
			name:       "literal fragment",
			path:       "/bucket/key?x=a#b",
			requestURI: "/bucket/key?x=a",
			rawQuery:   "x=a",
			fragment:   "b",
		},
		{
			name:       "escaped fragment",
			path:       "/bucket/key?x=a%23b",
			requestURI: "/bucket/key?x=a%23b",
			rawQuery:   "x=a%23b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := signer.SignRequest(
				t.Context(),
				http.MethodGet,
				tt.path,
				nil,
				"",
				requestSignerTestAccessKey,
				requestSignerTestSecretKey,
				nil,
			)
			if err != nil {
				t.Fatalf("SignRequest() error = %v", err)
			}
			if got := req.URL.RequestURI(); got != tt.requestURI {
				t.Errorf("RequestURI() = %q, want %q", got, tt.requestURI)
			}
			if got := req.URL.RawQuery; got != tt.rawQuery {
				t.Errorf("RawQuery = %q, want %q", got, tt.rawQuery)
			}
			if got := req.URL.Fragment; got != tt.fragment {
				t.Errorf("Fragment = %q, want %q", got, tt.fragment)
			}
		})
	}
}

func TestRequestSignerSignRequestMalformedEndpointMatchesLegacyError(t *testing.T) {
	const endpoint = "https://[::1"

	_, legacyErr := url.Parse(strings.TrimSuffix(endpoint, "/"))
	if legacyErr == nil {
		t.Fatal("url.Parse() error = nil, want error")
	}
	wantErr := fmt.Errorf("failed to parse endpoint: %w", legacyErr)

	_, gotErr := NewRequestSigner(endpoint, "us-east-1").SignRequest(
		t.Context(),
		http.MethodGet,
		"/bucket/key",
		nil,
		"",
		requestSignerTestAccessKey,
		requestSignerTestSecretKey,
		nil,
	)
	if gotErr == nil {
		t.Fatal("SignRequest() error = nil, want error")
	}
	if gotErr.Error() != wantErr.Error() {
		t.Errorf("SignRequest() error = %q, want legacy error %q", gotErr, wantErr)
	}
}

func TestRequestSignerSignRequestCopiesEndpointTemplate(t *testing.T) {
	const endpoint = "https://upstream.example.com"

	signer := NewRequestSigner(endpoint, "us-east-1")
	paths := []string{
		"/bucket/first%25?prefix=one%2Ftwo",
		"/bucket/second value?marker=a%20b",
	}

	requests := make([]*http.Request, len(paths))
	for i, path := range paths {
		req, err := signer.SignRequest(
			t.Context(),
			http.MethodGet,
			path,
			nil,
			"",
			requestSignerTestAccessKey,
			requestSignerTestSecretKey,
			nil,
		)
		if err != nil {
			t.Fatalf("SignRequest(%q) error = %v", path, err)
		}
		requests[i] = req
	}

	wantURLs := make([]string, len(paths))
	for i, path := range paths {
		want := legacySignerRequest(t, t.Context(), http.MethodGet, endpoint, path, nil)
		wantURLs[i] = want.URL.String()
		if gotURL := requests[i].URL.String(); gotURL != wantURLs[i] {
			t.Errorf("request %d URL = %q, want %q", i, gotURL, wantURLs[i])
		}
	}

	const workers = 16
	errs := make(chan error, workers)
	var ready, done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	start := make(chan struct{})
	for i := range workers {
		go func(i int) {
			defer done.Done()
			ready.Done()
			<-start
			pathIndex := i % len(paths)
			req, err := signer.SignRequest(
				context.Background(),
				http.MethodGet,
				paths[pathIndex],
				nil,
				"",
				requestSignerTestAccessKey,
				requestSignerTestSecretKey,
				nil,
			)
			if err != nil {
				errs <- err
				return
			}
			if req.URL.String() != wantURLs[pathIndex] {
				errs <- fmt.Errorf("URL = %q, want %q", req.URL, wantURLs[pathIndex])
			}
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
