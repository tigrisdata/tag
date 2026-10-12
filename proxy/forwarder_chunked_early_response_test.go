package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tigrisdata/tag/auth"
)

type chunkExtentBodyGate struct {
	reader    *strings.Reader
	stopAfter int64
	read      int64
	blocked   chan struct{}
	release   <-chan struct{}
	once      sync.Once
}

type testReadDeadlineResponseWriter struct {
	*httptest.ResponseRecorder
}

func (*testReadDeadlineResponseWriter) SetReadDeadline(time.Time) error { return nil }

type deadlineBlockingReader struct {
	mu       sync.Mutex
	deadline time.Time
}

func (r *deadlineBlockingReader) SetReadDeadline(deadline time.Time) error {
	r.mu.Lock()
	r.deadline = deadline
	r.mu.Unlock()
	return nil
}

func (r *deadlineBlockingReader) Read([]byte) (int, error) {
	r.mu.Lock()
	deadline := r.deadline
	r.mu.Unlock()
	if deadline.IsZero() {
		return 0, errors.New("request-body read started without a deadline")
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	<-timer.C
	return 0, os.ErrDeadlineExceeded
}

type validationDeadlineResponseWriter struct {
	*httptest.ResponseRecorder
	reader *deadlineBlockingReader
}

func (w *validationDeadlineResponseWriter) SetReadDeadline(deadline time.Time) error {
	return w.reader.SetReadDeadline(deadline)
}

func (r *chunkExtentBodyGate) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.read >= r.stopAfter {
		r.once.Do(func() { close(r.blocked) })
		<-r.release
	} else if int64(len(p)) > r.stopAfter-r.read {
		p = p[:r.stopAfter-r.read]
	}
	n, err := r.reader.Read(p)
	r.read += int64(n)
	return n, err
}

func TestRequestBodyWriteTrackerValidatesWithoutTeeingUnreadFrames(t *testing.T) {
	for _, tc := range []struct {
		name       string
		wire       string
		wantExtent bool
	}{
		{name: "fitting frame", wire: "4\r\nkeep\r\n4\r\nPASS\r\n0\r\n\r\n"},
		{name: "oversized frame", wire: "4\r\nkeep\r\n5\r\nFAIL!\r\n0\r\n\r\n", wantExtent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := newAWSChunkedReader(strings.NewReader(tc.wire))
			reader.decodedRemaining = 8
			decodedBody := &awsChunkedReadCloser{reader: reader}
			var tee strings.Builder
			request := &http.Request{Body: io.NopCloser(io.TeeReader(decodedBody, &tee))}
			tracker := trackRequestBodyWrite(request, decodedBody)

			prefix := make([]byte, 4)
			n, err := request.Body.Read(prefix)
			if err != nil || n != 4 || string(prefix) != "keep" {
				t.Fatalf("transport read n=%d body=%q err=%v; want keep", n, prefix[:n], err)
			}
			tracker.writeResult <- nil // deterministic WroteRequest ownership handoff
			if err := tracker.waitForWrite(context.Background(), time.Second); err != nil {
				t.Fatalf("wait for request writer: %v", err)
			}
			err = tracker.validateDecodedExtent()
			if tc.wantExtent != errors.Is(err, errAWSChunkExceedsDecodedLength) {
				t.Fatalf("post-writer validation error = %v, extent violation=%t", err, tc.wantExtent)
			}
			if got := tee.String(); got != "keep" {
				t.Fatalf("validation added unread source bytes to the upstream tee: %q", got)
			}
		})
	}
}

func TestSuccessfulChunkedValidationReadUsesDeadline(t *testing.T) {
	const timeout = 25 * time.Millisecond
	source := &deadlineBlockingReader{}
	decoded := &awsChunkedReadCloser{reader: newAWSChunkedReader(source)}
	decoded.reader.decodedRemaining = 1
	request := &http.Request{Body: decoded}
	tracker := trackRequestBodyWrite(request, decoded)
	tracker.writeResult <- nil
	writer := &validationDeadlineResponseWriter{
		ResponseRecorder: httptest.NewRecorder(),
		reader:           source,
	}
	forwarder := &baseForwarder{httpClient: &http.Client{Timeout: timeout}}

	started := time.Now()
	err := forwarder.validateSuccessfulChunkedRequest(writer, request, http.StatusOK)
	if !isRequestBodyReadTimeout(err) {
		t.Fatalf("validation error = %v, want a request-body timeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("request-body validation took %s, want it bounded by %s", elapsed, timeout)
	}
}

func TestSigningForwarderChunkedExtentWithEarlyHTTP2Response(t *testing.T) {
	const (
		accessKey = "AKIAIOSFODNN7EXAMPLE"
		secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		region    = "us-east-1"
		chunkSize = 8 * 1024
	)
	chunkHeader := fmt.Sprintf("%x\r\n", chunkSize)
	wireBody := chunkHeader + strings.Repeat("A", chunkSize) + "\r\n5\r\nFAIL!\r\n0\r\n\r\n"
	decodedLength := int64(chunkSize + 4)

	protocol := make(chan int, 1)
	upstreamPrefix := make(chan []byte, 1)
	allowResponse := make(chan struct{})
	var allowOnce sync.Once
	defer allowOnce.Do(func() { close(allowResponse) })
	responseSent := make(chan struct{})
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protocol <- r.ProtoMajor
		var prefix [1]byte
		n, err := io.ReadFull(r.Body, prefix[:])
		if err != nil {
			t.Errorf("read first upstream byte: n=%d err=%v", n, err)
		}
		upstreamPrefix <- append([]byte(nil), prefix[:n]...)
		<-allowResponse
		w.Header().Set("Content-Length", "6")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		_, _ = io.WriteString(w, "stored")
		close(responseSent)
	}))
	upstream.EnableHTTP2 = true
	upstream.StartTLS()
	defer upstream.Close()

	credentials := auth.NewCredentialStore()
	credentials.AddCredential(accessKey, secretKey)
	base := newBaseForwarder(upstream.URL, region, 1)
	base.httpClient = upstream.Client()
	forwarder := &signingForwarder{
		baseForwarder: base,
		credStore:     credentials,
		validator:     auth.NewRequestValidator(credentials),
	}

	bodyBlocked := make(chan struct{})
	releaseBody := make(chan struct{})
	var releaseBodyOnce sync.Once
	defer releaseBodyOnce.Do(func() { close(releaseBody) })
	requestBody := &chunkExtentBodyGate{
		reader:    strings.NewReader(wireBody),
		stopAfter: int64(len(chunkHeader) + chunkSize/2),
		blocked:   bodyBlocked,
		release:   releaseBody,
	}
	headers := make(http.Header)
	headers.Set("Content-Encoding", "aws-chunked")
	headers.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	headers.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(decodedLength))
	request, err := auth.NewRequestSigner("http://client.example.com", region).SignRequest(
		context.Background(),
		http.MethodPut,
		"/bucket/object",
		requestBody,
		"STREAMING-UNSIGNED-PAYLOAD-TRAILER",
		accessKey,
		secretKey,
		headers,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.ContentLength = int64(len(wireBody))
	if _, err := forwarder.validator.ValidateRequest(request); err != nil {
		t.Fatalf("incoming SigV4 request did not validate: %v", err)
	}

	writer := &testReadDeadlineResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	recorder := &statusRecorder{ResponseWriter: writer}
	forwardDone := make(chan error, 1)
	go func() {
		forwardDone <- forwarder.Forward(context.Background(), recorder, request)
	}()
	select {
	case <-bodyBlocked:
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP/2 forwarder did not reach the controlled body-read gate")
	}
	select {
	case version := <-protocol:
		if version != 2 {
			t.Fatalf("upstream protocol major = %d, want HTTP/2", version)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP/2 upstream did not receive the request")
	}
	select {
	case prefix := <-upstreamPrefix:
		if string(prefix) != "A" {
			t.Fatalf("upstream prefix = %q, want one valid-frame byte", prefix)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP/2 upstream did not read its prefix")
	}
	allowOnce.Do(func() { close(allowResponse) })
	select {
	case <-responseSent:
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP/2 upstream did not send its early 2xx response")
	}
	releaseBodyOnce.Do(func() { close(releaseBody) })

	select {
	case err := <-forwardDone:
		if err == nil {
			t.Fatalf("signing forwarder returned success after an early HTTP/2 response; response body=%q", writer.Body.String())
		}
		if writer.Body.Len() != 0 {
			t.Fatalf("signing forwarder exposed upstream success body %q after validation failed", writer.Body.String())
		}
		if recorder.status != 0 {
			t.Fatalf("client response status was recorded as %d despite validation failure", recorder.status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP/2 request body validation did not finish")
	}
}
