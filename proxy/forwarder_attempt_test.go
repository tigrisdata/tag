package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tigrisdata/tag/auth"
)

func TestForwardWithCaptureAttemptedReportsDoBoundary(t *testing.T) {
	t.Run("transparent-build-error-is-not-attempted", func(t *testing.T) {
		var calls int
		base := newBaseForwarder("http://%", "us-east-1", 1)
		base.httpClient = &http.Client{Transport: forwarderTestTransport(func(*http.Request) (*http.Response, error) {
			calls++
			return nil, errors.New("unexpected HTTP client call")
		})}
		forwarder := &transparentForwarder{
			baseForwarder:    base,
			proxySigner:      auth.NewProxySigner("test-access-key", "test-secret-key"),
			upstreamEndpoint: "http://%",
		}
		capture, attempted, err := forwarder.forwardWithCaptureAttempted(context.Background(), httptest.NewRecorder(), newTransparentRequestForTest(t))
		if err == nil || attempted || capture != nil || calls != 0 {
			t.Fatalf("capture=%v attempted=%t err=%v HTTP calls=%d; want local error before Do", capture, attempted, err, calls)
		}
	})

	t.Run("transparent-transport-error-is-attempted", func(t *testing.T) {
		transportErr := errors.New("round trip failed")
		forwarder := newTransparentForwarderForTest(forwarderTestTransport(func(*http.Request) (*http.Response, error) {
			return nil, transportErr
		}))
		capture, attempted, err := forwarder.forwardWithCaptureAttempted(context.Background(), httptest.NewRecorder(), newTransparentRequestForTest(t))
		if capture != nil || !attempted || !errors.Is(err, transportErr) {
			t.Fatalf("capture=%v attempted=%t err=%v; want attempted transport error %v", capture, attempted, err, transportErr)
		}
	})

	t.Run("signing-validation-error-is-not-attempted", func(t *testing.T) {
		var calls int
		store := auth.NewCredentialStore()
		base := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
		base.httpClient = &http.Client{Transport: forwarderTestTransport(func(*http.Request) (*http.Response, error) {
			calls++
			return nil, errors.New("unexpected HTTP client call")
		})}
		forwarder := &signingForwarder{
			baseForwarder: base,
			credStore:     store,
			validator:     auth.NewRequestValidator(store),
		}
		r := httptest.NewRequest(http.MethodPost, "https://client.example.com/bulk-bucket?delete", strings.NewReader(`<Delete/>`))
		capture, attempted, err := forwarder.forwardWithCaptureAttempted(context.Background(), httptest.NewRecorder(), r)
		if err == nil || attempted || capture != nil || calls != 0 {
			t.Fatalf("capture=%v attempted=%t err=%v HTTP calls=%d; want validation error before Do", capture, attempted, err, calls)
		}
	})

	t.Run("signing-transport-error-is-attempted", func(t *testing.T) {
		const accessKey = "AKIAIOSFODNN7EXAMPLE"
		const secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		body := `<Delete><Object><Key>key</Key></Object></Delete>`
		hash := sha256.Sum256([]byte(body))
		request, err := auth.NewRequestSigner("https://client.example.com", "us-east-1").SignRequest(
			context.Background(),
			http.MethodPost,
			"/bulk-bucket?delete",
			strings.NewReader(body),
			hex.EncodeToString(hash[:]),
			accessKey,
			secretKey,
			http.Header{},
		)
		if err != nil {
			t.Fatalf("sign test request: %v", err)
		}

		transportErr := errors.New("round trip failed")
		store := auth.NewCredentialStore()
		store.AddCredential(accessKey, secretKey)
		base := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
		base.httpClient = &http.Client{Transport: forwarderTestTransport(func(*http.Request) (*http.Response, error) {
			return nil, transportErr
		})}
		forwarder := &signingForwarder{
			baseForwarder: base,
			credStore:     store,
			validator:     auth.NewRequestValidator(store),
		}
		capture, attempted, err := forwarder.forwardWithCaptureAttempted(context.Background(), httptest.NewRecorder(), request)
		if capture != nil || !attempted || !errors.Is(err, transportErr) {
			t.Fatalf("capture=%v attempted=%t err=%v; want attempted transport error %v", capture, attempted, err, transportErr)
		}
	})
}
