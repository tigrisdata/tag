package handlers

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

const (
	routedChunkExtentAccessKey = "AKIAIOSFODNN7EXAMPLE"
	routedChunkExtentSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	routedChunkExtentRegion    = "us-east-1"

	routedSignedChunkHash   = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	routedUnsignedChunkHash = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
)

type routedChunkExtentObservation struct {
	body          []byte
	bodyReadErr   error
	contentLength int64
}

type routedChunkExtentFixture struct {
	gateway         *httptest.Server
	client          *http.Client
	upstreamStarted atomic.Int64
	upstreamResults chan routedChunkExtentObservation
}

func newRoutedChunkExtentFixture(tb testing.TB) *routedChunkExtentFixture {
	tb.Helper()

	fixture := &routedChunkExtentFixture{
		upstreamResults: make(chan routedChunkExtentObservation, 8),
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixture.upstreamStarted.Add(1)
		body, err := io.ReadAll(r.Body)
		fixture.upstreamResults <- routedChunkExtentObservation{
			body:          body,
			bodyReadErr:   err,
			contentLength: r.ContentLength,
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "stored")
	}))
	tb.Cleanup(upstream.Close)

	cfg := config.NewDefault()
	cfg.Mode = config.ModeSigning
	cfg.Upstream.Endpoint = upstream.URL
	cfg.Upstream.Region = routedChunkExtentRegion
	cfg.Cache.WarmOnWrite = false
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(routedChunkExtentAccessKey, routedChunkExtentSecretKey)
	forwarder := proxy.NewForwarder(
		credentials,
		cfg.Upstream.Endpoint,
		cfg.Upstream.Region,
		1,
		nil,
		nil,
	)
	service := proxy.NewService(forwarder, cache.NewDisabledCache(), cfg)
	fixture.gateway = httptest.NewServer(NewServer(service, "127.0.0.1", 0, false, 0).Router())
	tb.Cleanup(fixture.gateway.Close)

	fixture.client = &http.Client{Timeout: 5 * time.Second}
	tb.Cleanup(fixture.client.CloseIdleConnections)
	return fixture
}

type routedChunkExtentResponse struct {
	status int
	body   []byte
	err    error
}

func (f *routedChunkExtentFixture) put(tb testing.TB, hash, decodedLength, wireBody string, includeDecodedLength bool) routedChunkExtentResponse {
	tb.Helper()

	headers := make(http.Header)
	headers.Set("Content-Encoding", "aws-chunked")
	headers.Set("X-Amz-Content-Sha256", hash)
	if includeDecodedLength {
		headers.Set("X-Amz-Decoded-Content-Length", decodedLength)
	}
	request, err := auth.NewRequestSigner(f.gateway.URL, routedChunkExtentRegion).SignRequest(
		context.Background(),
		http.MethodPut,
		"/bucket/object",
		strings.NewReader(wireBody),
		hash,
		routedChunkExtentAccessKey,
		routedChunkExtentSecretKey,
		headers,
	)
	if err != nil {
		return routedChunkExtentResponse{err: err}
	}

	response, err := f.client.Do(request)
	if err != nil {
		return routedChunkExtentResponse{err: err}
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return routedChunkExtentResponse{status: response.StatusCode, body: body, err: err}
}

func (f *routedChunkExtentFixture) awaitUpstream(t *testing.T, startedBefore int64, timeout time.Duration) (routedChunkExtentObservation, bool) {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-f.upstreamResults:
		return result, true
	case <-timer.C:
		if f.upstreamStarted.Load() != startedBefore {
			t.Fatalf("upstream request started but did not finish within %s", timeout)
		}
		return routedChunkExtentObservation{}, false
	}
}

func TestRoutedAWSChunkedDecodedExtent(t *testing.T) {
	fixture := newRoutedChunkExtentFixture(t)

	overruns := []struct {
		name                 string
		hash                 string
		decodedLength        string
		decodedLengthValue   int64
		wireBody             string
		allowedForwardedBody []byte
	}{
		{
			name:               "signed single frame exceeds positive extent",
			hash:               routedSignedChunkHash,
			decodedLength:      "3",
			decodedLengthValue: 3,
			wireBody:           "8;chunk-signature=sig\r\nabc",
		},
		{
			name:                 "signed complete frame exceeds positive extent",
			hash:                 routedSignedChunkHash,
			decodedLength:        "3",
			decodedLengthValue:   3,
			wireBody:             "8;chunk-signature=sig\r\nabcdefgh\r\n0;chunk-signature=end\r\n\r\n",
			allowedForwardedBody: nil,
		},
		{
			name:                 "unsigned later frame exceeds remaining extent",
			hash:                 routedUnsignedChunkHash,
			decodedLength:        "8",
			decodedLengthValue:   8,
			wireBody:             "4\r\nkeep\r\n5\r\nFAIL",
			allowedForwardedBody: []byte("keep"),
		},
		{
			name:                 "unsigned complete later frame exceeds remaining extent",
			hash:                 routedUnsignedChunkHash,
			decodedLength:        "8",
			decodedLengthValue:   8,
			wireBody:             "4\r\nkeep\r\n5\r\nFAIL!\r\n0\r\n\r\n",
			allowedForwardedBody: []byte("keep"),
		},
		{
			name:               "zero extent rejects data frame",
			hash:               routedSignedChunkHash,
			decodedLength:      "0",
			decodedLengthValue: 0,
			wireBody:           "1;chunk-signature=sig\r\nz\r\n0;chunk-signature=end\r\n\r\n",
		},
	}
	for _, test := range overruns {
		t.Run(test.name, func(t *testing.T) {
			startedBefore := fixture.upstreamStarted.Load()
			response := fixture.put(t, test.hash, test.decodedLength, test.wireBody, true)
			successful := response.err == nil && response.status >= http.StatusOK && response.status < http.StatusMultipleChoices

			upstream, observed := fixture.awaitUpstream(t, startedBefore, 250*time.Millisecond)
			if successful {
				t.Errorf("overlong AWS chunk was reported successful: status=%d body=%q", response.status, response.body)
			}
			if !observed {
				t.Logf("client status=%d client error=%v; upstream observed=false", response.status, response.err)
				return
			}
			complete := upstream.bodyReadErr == nil
			t.Logf("client status=%d client error=%v; upstream content-length=%d body=%q complete=%t read-error=%v", response.status, response.err, upstream.contentLength, upstream.body, complete, upstream.bodyReadErr)
			if upstream.contentLength != test.decodedLengthValue {
				t.Errorf("upstream content length = %d, want declared decoded length %d", upstream.contentLength, test.decodedLengthValue)
			}
			if !bytes.HasPrefix(test.allowedForwardedBody, upstream.body) {
				t.Errorf("upstream received bytes from the offending frame: body=%q allowed-prefix=%q", upstream.body, test.allowedForwardedBody)
			}
		})
	}

	valid := []struct {
		name          string
		hash          string
		decodedLength string
		wireBody      string
		wantBody      string
	}{
		{
			name:          "signed equal-size frame",
			hash:          routedSignedChunkHash,
			decodedLength: "3",
			wireBody:      "3;chunk-signature=sig\r\nabc\r\n0;chunk-signature=end\r\n\r\n",
			wantBody:      "abc",
		},
		{
			name:          "unsigned smaller multi-frame total",
			hash:          routedUnsignedChunkHash,
			decodedLength: "5",
			wireBody:      "2\r\nab\r\n3\r\ncde\r\n0\r\n\r\n",
			wantBody:      "abcde",
		},
		{
			name:          "signed empty upload",
			hash:          routedSignedChunkHash,
			decodedLength: "0",
			wireBody:      "0;chunk-signature=end\r\n\r\n",
		},
		{
			name:          "unsigned empty upload",
			hash:          routedUnsignedChunkHash,
			decodedLength: "0",
			wireBody:      "0\r\n\r\n",
		},
	}
	for _, test := range valid {
		t.Run(test.name, func(t *testing.T) {
			startedBefore := fixture.upstreamStarted.Load()
			response := fixture.put(t, test.hash, test.decodedLength, test.wireBody, true)
			if response.err != nil {
				t.Fatalf("PUT failed: %v", response.err)
			}
			if response.status != http.StatusOK {
				t.Fatalf("PUT status = %d, want %d; body=%q", response.status, http.StatusOK, response.body)
			}

			upstream, observed := fixture.awaitUpstream(t, startedBefore, 3*time.Second)
			if !observed {
				t.Fatal("upstream did not receive the valid PUT")
			}
			if upstream.bodyReadErr != nil {
				t.Fatalf("upstream body read failed: %v", upstream.bodyReadErr)
			}
			if got := string(upstream.body); got != test.wantBody {
				t.Fatalf("upstream body = %q, want %q", got, test.wantBody)
			}
		})
	}

	t.Run("missing decoded length is rejected before forwarding", func(t *testing.T) {
		startedBefore := fixture.upstreamStarted.Load()
		response := fixture.put(t, routedSignedChunkHash, "", "3;chunk-signature=sig\r\nabc\r\n0;chunk-signature=end\r\n\r\n", false)
		if response.err != nil {
			t.Fatalf("PUT failed: %v", response.err)
		}
		if response.status != http.StatusLengthRequired {
			t.Fatalf("PUT status = %d, want %d; body=%q", response.status, http.StatusLengthRequired, response.body)
		}
		if got := fixture.upstreamStarted.Load(); got != startedBefore {
			t.Fatalf("missing decoded length reached upstream: requests=%d before=%d", got, startedBefore)
		}
	})
}
