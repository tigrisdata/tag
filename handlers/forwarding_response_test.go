package handlers

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

const (
	forwardingTestAccessKey = "AKIAIOSFODNN7EXAMPLE"
	forwardingTestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	forwardingTestRegion    = "us-east-1"
)

func newSigningListBucketsRouter(t *testing.T, upstreamURL string) http.Handler {
	t.Helper()

	credentials := auth.NewCredentialStore()
	credentials.AddCredential(forwardingTestAccessKey, forwardingTestSecretKey)
	forwarder := proxy.NewForwarder(credentials, upstreamURL, forwardingTestRegion, 1, nil, nil)
	cfg := config.NewDefault()
	cfg.Mode = config.ModeSigning
	service := proxy.NewService(forwarder, nil, cfg)
	return NewServer(service, "127.0.0.1", 0, false, 0).Router()
}

func newSigningListBucketsGateway(t *testing.T, upstreamURL string) *httptest.Server {
	t.Helper()
	gateway := httptest.NewServer(newSigningListBucketsRouter(t, upstreamURL))
	t.Cleanup(gateway.Close)
	return gateway
}

func newSigningListBucketsTLSGateway(t *testing.T, upstreamURL string) *httptest.Server {
	t.Helper()
	gateway := httptest.NewTLSServer(newSigningListBucketsRouter(t, upstreamURL))
	t.Cleanup(gateway.Close)
	return gateway
}

func newSigningListBucketsHTTP2Gateway(t *testing.T, upstreamURL string) (*httptest.Server, <-chan string) {
	t.Helper()
	protoSeen := make(chan string, 1)
	router := newSigningListBucketsRouter(t, upstreamURL)
	gateway := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protoSeen <- r.Proto
		router.ServeHTTP(w, r)
	}))
	gateway.EnableHTTP2 = true
	gateway.StartTLS()
	t.Cleanup(gateway.Close)
	return gateway, protoSeen
}

func signedListBucketsRequest(t *testing.T, gatewayURL string) *http.Request {
	t.Helper()

	request, err := auth.NewRequestSigner(gatewayURL, forwardingTestRegion).SignRequest(
		context.Background(), http.MethodGet, "/", nil, "", forwardingTestAccessKey, forwardingTestSecretKey, nil,
	)
	if err != nil {
		t.Fatalf("sign ListBuckets request: %v", err)
	}
	return request
}

type truncatedListBucketsUpstream struct {
	server *httptest.Server
	calls  atomic.Int32
	result chan error
	prefix []byte
}

func newTruncatedListBucketsUpstream(t *testing.T, prefix []byte) *truncatedListBucketsUpstream {
	t.Helper()
	upstream := &truncatedListBucketsUpstream{
		result: make(chan error, 1),
		prefix: prefix,
	}
	upstream.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.calls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/" || r.Header.Get("Authorization") == "" {
			upstream.result <- fmt.Errorf("unexpected forwarded request: %s %s", r.Method, r.URL.Path)
			return
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			upstream.result <- errors.New("upstream response writer does not support hijacking")
			return
		}
		conn, rw, err := hijacker.Hijack()
		if err != nil {
			upstream.result <- fmt.Errorf("hijack upstream connection: %w", err)
			return
		}
		_, writeErr := fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n%x\r\n", len(prefix))
		if writeErr == nil {
			_, writeErr = rw.Write(prefix)
		}
		if writeErr == nil {
			_, writeErr = io.WriteString(rw, "\r\n")
		}
		if writeErr == nil {
			writeErr = rw.Flush()
		}
		_ = conn.Close()
		upstream.result <- writeErr
	}))
	t.Cleanup(upstream.server.Close)
	return upstream
}

func (u *truncatedListBucketsUpstream) check(t *testing.T) {
	t.Helper()
	if got := u.calls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
	select {
	case err := <-u.result:
		if err != nil {
			t.Fatalf("write truncated upstream response: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upstream fixture did not finish writing the truncated response")
	}
}

func sendSignedHTTP10ListBuckets(t *testing.T, gatewayURL string, request *http.Request) (*http.Response, net.Conn, error) {
	t.Helper()
	address := strings.TrimPrefix(strings.TrimPrefix(gatewayURL, "http://"), "https://")
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, nil, err
	}
	if strings.HasPrefix(gatewayURL, "https://") {
		tlsConn := tls.Client(conn, &tls.Config{
			InsecureSkipVerify: true, // httptest server uses a self-signed certificate.
			NextProtos:         []string{"http/1.1"},
		})
		if err := tlsConn.Handshake(); err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
		conn = tlsConn
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	headers := request.Header.Clone()
	headers.Set("Host", request.URL.Host)
	headers.Set("Connection", "close")
	if _, err := fmt.Fprintf(conn, "%s %s HTTP/1.0\r\n", request.Method, request.URL.RequestURI()); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if err := headers.Write(conn); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if _, err := io.WriteString(conn, "\r\n"); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), request)
	return response, conn, err
}

func isClientAbortTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var networkErr net.Error
	return errors.As(err, &networkErr) && networkErr.Timeout()
}

func assertTruncatedListBucketsResponse(t *testing.T, response *http.Response, responseErr error, prefix []byte, upstream *truncatedListBucketsUpstream) {
	t.Helper()
	if responseErr != nil {
		upstream.check(t)
		if isClientAbortTimeout(responseErr) {
			t.Fatalf("response abort timed out: %v", responseErr)
		}
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("ListBuckets status = %d, want upstream status %d", response.StatusCode, http.StatusOK)
	}
	upstream.check(t)

	body, readErr := io.ReadAll(response.Body)
	if isClientAbortTimeout(readErr) {
		t.Fatalf("response body abort timed out: %v", readErr)
	}
	if readErr == nil {
		t.Errorf("truncated upstream body completed as a clean HTTP success: body bytes = %d", len(body))
	}
	if bytes.Contains(body, []byte("<Error>")) {
		t.Errorf("S3 error XML was appended after the response was committed: %q", body)
	}
	if !bytes.HasPrefix(prefix, body) {
		t.Errorf("downstream received bytes outside the upstream prefix: %d", len(body))
	}
}

func TestRoutedListBucketsTruncatedUpstreamResponseAborts(t *testing.T) {
	t.Run("HTTP/1.1", func(t *testing.T) {
		upstream := newTruncatedListBucketsUpstream(t, []byte("abc"))
		gateway := newSigningListBucketsGateway(t, upstream.server.URL)
		response, err := gateway.Client().Do(signedListBucketsRequest(t, gateway.URL))
		assertTruncatedListBucketsResponse(t, response, err, upstream.prefix, upstream)
	})

	for _, tc := range []struct {
		name string
		tls  bool
	}{
		{name: "HTTP/1.0"},
		{name: "HTTP/1.0 TLS", tls: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := bytes.Repeat([]byte("a"), 2<<20)
			upstream := newTruncatedListBucketsUpstream(t, prefix)
			var gateway *httptest.Server
			if tc.tls {
				gateway = newSigningListBucketsTLSGateway(t, upstream.server.URL)
			} else {
				gateway = newSigningListBucketsGateway(t, upstream.server.URL)
			}
			request := signedListBucketsRequest(t, gateway.URL)
			response, conn, err := sendSignedHTTP10ListBuckets(t, gateway.URL, request)
			if conn != nil {
				defer conn.Close()
			}
			assertTruncatedListBucketsResponse(t, response, err, prefix, upstream)
		})
	}

	t.Run("HTTP/2", func(t *testing.T) {
		upstream := newTruncatedListBucketsUpstream(t, []byte("abc"))
		gateway, protoSeen := newSigningListBucketsHTTP2Gateway(t, upstream.server.URL)
		response, err := gateway.Client().Do(signedListBucketsRequest(t, gateway.URL))
		select {
		case proto := <-protoSeen:
			if proto != "HTTP/2.0" {
				t.Fatalf("handler saw protocol %q, want HTTP/2.0", proto)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("HTTP/2 request did not reach the handler")
		}
		assertTruncatedListBucketsResponse(t, response, err, upstream.prefix, upstream)
	})
}

func TestRoutedListBucketsCompleteResponsesRemainComplete(t *testing.T) {
	body := "complete bucket list"
	for _, tc := range []struct {
		name        string
		knownLength bool
		http10      bool
	}{
		{name: "known length HTTP/1.1", knownLength: true},
		{name: "unknown length HTTP/1.1"},
		{name: "unknown length HTTP/1.0", http10: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var upstreamCalls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamCalls.Add(1)
				if tc.knownLength {
					w.Header().Set("Content-Type", "application/xml")
					w.Header().Set("Content-Length", fmt.Sprint(len(body)))
					w.WriteHeader(http.StatusOK)
					_, _ = io.WriteString(w, body)
					return
				}

				hijacker, ok := w.(http.Hijacker)
				if !ok {
					return
				}
				conn, rw, err := hijacker.Hijack()
				if err != nil {
					return
				}
				_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n%x\r\n%s\r\n0\r\n\r\n", len(body), body)
				_ = rw.Flush()
				_ = conn.Close()
			}))
			t.Cleanup(upstream.Close)
			gateway := newSigningListBucketsGateway(t, upstream.URL)

			request := signedListBucketsRequest(t, gateway.URL)
			var response *http.Response
			var err error
			if tc.http10 {
				var conn net.Conn
				response, conn, err = sendSignedHTTP10ListBuckets(t, gateway.URL, request)
				if conn != nil {
					defer conn.Close()
				}
			} else {
				response, err = gateway.Client().Do(request)
			}
			if err != nil {
				t.Fatalf("ListBuckets request: %v", err)
			}
			defer response.Body.Close()
			gotBody, readErr := io.ReadAll(response.Body)
			if readErr != nil {
				t.Fatalf("read complete ListBuckets response: %v", readErr)
			}
			if response.StatusCode != http.StatusOK {
				t.Errorf("ListBuckets status = %d, want %d", response.StatusCode, http.StatusOK)
			}
			if string(gotBody) != body {
				t.Errorf("ListBuckets body = %q, want %q", gotBody, body)
			}
			if tc.knownLength && response.ContentLength != int64(len(body)) {
				t.Errorf("ContentLength = %d, want %d", response.ContentLength, len(body))
			}
			if got := response.Header.Get("Content-Type"); got != "application/xml" {
				t.Errorf("Content-Type = %q, want application/xml", got)
			}
			if got := upstreamCalls.Load(); got != 1 {
				t.Errorf("upstream calls = %d, want 1", got)
			}
		})
	}
}

func TestRoutedListBucketsAuthFailureBeforeCommitmentRemainsS3Error(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	gateway := newSigningListBucketsGateway(t, upstream.URL)

	request := signedListBucketsRequest(t, gateway.URL)
	authorization := request.Header.Get("Authorization")
	last := authorization[len(authorization)-1]
	if last == '0' {
		authorization = authorization[:len(authorization)-1] + "1"
	} else {
		authorization = authorization[:len(authorization)-1] + "0"
	}
	request.Header.Set("Authorization", authorization)

	response, err := gateway.Client().Do(request)
	if err != nil {
		t.Fatalf("ListBuckets auth error request: %v", err)
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		t.Fatalf("read auth error response: %v", readErr)
	}
	if response.StatusCode != http.StatusForbidden {
		t.Errorf("auth failure status = %d, want %d", response.StatusCode, http.StatusForbidden)
	}
	if !bytes.Contains(body, []byte("SignatureDoesNotMatch")) {
		t.Errorf("auth failure body = %q, want SignatureDoesNotMatch S3 XML", body)
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Errorf("upstream calls after local auth rejection = %d, want 0", got)
	}
}
