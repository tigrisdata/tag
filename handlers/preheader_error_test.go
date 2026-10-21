package handlers

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
	"github.com/tigrisdata/tag/s3err"
)

type preHeaderHandlerRoundTripper func(*http.Request) (*http.Response, error)

func (f preHeaderHandlerRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type preHeaderHandlerForwarder struct {
	proxy.RequestForwarder
	client *http.Client
}

func (f *preHeaderHandlerForwarder) ValidateAndGetCredentials(*http.Request) (proxy.AuthResult, string, string, error) {
	return proxy.AuthValidated, "access", "secret", nil
}

func (f *preHeaderHandlerForwarder) DoRequestWithCreds(ctx context.Context, r *http.Request, _, _ string) (*http.Response, error) {
	upstreamRequest, err := http.NewRequestWithContext(ctx, r.Method, "https://upstream.example.com"+r.URL.RequestURI(), nil)
	if err != nil {
		return nil, err
	}
	return f.client.Do(upstreamRequest)
}

func TestGetObjectPreHeaderTransportFailureUsesS3ErrorResponse(t *testing.T) {
	transportErr := errors.New("injected upstream transport failure")
	var transportCalls atomic.Int32
	forwarder := &preHeaderHandlerForwarder{client: &http.Client{
		Transport: preHeaderHandlerRoundTripper(func(*http.Request) (*http.Response, error) {
			transportCalls.Add(1)
			return nil, transportErr
		}),
	}}
	service := proxy.NewService(forwarder, cache.NewDisabledCache(), config.NewDefault())
	router := NewServer(service, "127.0.0.1", 0, false, 0).Router()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/bucket/object", nil).WithContext(ctx)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		router.ServeHTTP(response, request)
		close(done)
	}()

	returnedWithoutCancellation := true
	select {
	case <-done:
	case <-time.After(time.Second):
		returnedWithoutCancellation = false
		cancel()
		<-done
	}
	if !returnedWithoutCancellation {
		t.Error("full GET did not return the upstream failure while its request context remained live")
	}
	if returnedWithoutCancellation && ctx.Err() != nil {
		t.Errorf("request context ended before the handler returned: %v", ctx.Err())
	}
	if got := transportCalls.Load(); got != 1 {
		t.Errorf("upstream transport calls = %d, want 1", got)
	}
	if response.Code != http.StatusInternalServerError {
		t.Errorf("HTTP status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
	if got := response.Header().Get("Content-Type"); got != "application/xml" {
		t.Errorf("Content-Type = %q, want application/xml", got)
	}
	if got := response.Header().Get(proxy.XCacheHeader); got != "" {
		t.Errorf("%s = %q, want no cache status on an uncommitted upstream error", proxy.XCacheHeader, got)
	}
	var body s3err.ErrorResponse
	if err := xml.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode S3 error response %q: %v", response.Body.String(), err)
	}
	if body.Code != s3err.ErrInternalError {
		t.Errorf("S3 error code = %q, want %q", body.Code, s3err.ErrInternalError)
	}
}
