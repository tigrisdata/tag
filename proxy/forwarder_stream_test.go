package proxy

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tigrisdata/tag/auth"
)

type flushTestResponseWriter struct {
	mu      sync.Mutex
	header  http.Header
	status  int
	body    bytes.Buffer
	flushes chan struct{}
}

func newFlushTestResponseWriter() *flushTestResponseWriter {
	return &flushTestResponseWriter{
		header:  make(http.Header),
		flushes: make(chan struct{}, 8),
	}
}

func (w *flushTestResponseWriter) Header() http.Header {
	return w.header
}

func (w *flushTestResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.status = status
}

func (w *flushTestResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

func (w *flushTestResponseWriter) Flush() {
	w.flushes <- struct{}{}
}

func (w *flushTestResponseWriter) bodyString() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.String()
}

type noFlushTestResponseWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

type forwardingFlushTestResponseWriter struct {
	http.ResponseWriter
	flushes int
}

func (w *forwardingFlushTestResponseWriter) Flush() { w.flushes++ }

func (w *forwardingFlushTestResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *noFlushTestResponseWriter) Header() http.Header { return w.header }

func (w *noFlushTestResponseWriter) WriteHeader(status int) { w.status = status }

func (w *noFlushTestResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

type gatedResponseBody struct {
	firstRead    chan struct{}
	releaseFirst chan struct{}
	releaseTail  chan struct{}
	reads        int
}

func (b *gatedResponseBody) Read(p []byte) (int, error) {
	switch b.reads {
	case 0:
		b.reads++
		close(b.firstRead)
		<-b.releaseFirst
		return copy(p, "first-fragment"), nil
	case 1:
		b.reads++
		<-b.releaseTail
		return copy(p, "tail"), nil
	default:
		return 0, io.EOF
	}
}

func (*gatedResponseBody) Close() error { return nil }

func newTestForwarder(body io.ReadCloser, headers http.Header) baseForwarder {
	return newTestForwarderWithLength(body, headers, -1)
}

func newTestForwarderWithLength(body io.ReadCloser, headers http.Header, contentLength int64) baseForwarder {
	forwarder := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
	forwarder.httpClient = &http.Client{Transport: forwarderTestTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusAccepted,
			Header:        headers,
			Body:          body,
			ContentLength: contentLength,
		}, nil
	})}
	return forwarder
}

func TestExecuteAndStreamFlushesHeadersAndFirstFragment(t *testing.T) {
	body := &gatedResponseBody{
		firstRead:    make(chan struct{}),
		releaseFirst: make(chan struct{}),
		releaseTail:  make(chan struct{}),
	}
	var releaseFirstOnce sync.Once
	var releaseTailOnce sync.Once
	releaseFirstNow := func() { releaseFirstOnce.Do(func() { close(body.releaseFirst) }) }
	releaseTailNow := func() { releaseTailOnce.Do(func() { close(body.releaseTail) }) }
	defer func() {
		releaseFirstNow()
		releaseTailNow()
	}()
	forwarder := newTestForwarder(body, http.Header{
		"Content-Type": {"application/octet-stream"},
		"X-Upstream":   {"present"},
	})
	writer := newFlushTestResponseWriter()
	request, err := http.NewRequest(http.MethodGet, "https://upstream.example.com/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- forwarder.executeAndStream(writer, request, 0, nil)
	}()

	select {
	case <-writer.flushes:
	case <-time.After(time.Second):
		t.Fatal("response headers were not flushed")
	}
	if got := writer.header.Get("X-Upstream"); got != "present" {
		t.Fatalf("header = %q, want present", got)
	}
	if got := writer.bodyString(); got != "" {
		t.Fatalf("body before first fragment = %q, want empty", got)
	}

	releaseFirstNow()
	select {
	case <-writer.flushes:
	case <-time.After(time.Second):
		t.Fatal("first body fragment was not flushed")
	}
	if got := writer.bodyString(); got != "first-fragment" {
		t.Fatalf("body before tail release = %q, want first-fragment", got)
	}

	releaseTailNow()
	if err := <-done; err != nil {
		t.Fatalf("executeAndStream: %v", err)
	}
	if got := writer.bodyString(); got != "first-fragmenttail" {
		t.Fatalf("body = %q, want first-fragmenttail", got)
	}
}

func TestExecuteAndStreamDoesNotFlushRequestWithBody(t *testing.T) {
	writer := newFlushTestResponseWriter()
	forwarder := newTestForwarder(io.NopCloser(strings.NewReader("response")), http.Header{})
	request, err := http.NewRequest(http.MethodPut, "https://upstream.example.com/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := forwarder.executeAndStream(writer, request, 1, nil); err != nil {
		t.Fatalf("executeAndStream: %v", err)
	}
	select {
	case <-writer.flushes:
		t.Fatal("body-bearing request unexpectedly flushed response")
	default:
	}
	if got := writer.bodyString(); got != "response" {
		t.Fatalf("body = %q, want response", got)
	}
}

func TestExecuteAndStreamSkipsFlushForKnownLengthResponse(t *testing.T) {
	writer := newFlushTestResponseWriter()
	forwarder := newTestForwarderWithLength(io.NopCloser(strings.NewReader("response")), http.Header{}, int64(len("response")))
	request, err := http.NewRequest(http.MethodGet, "https://upstream.example.com/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := forwarder.executeAndStream(writer, request, 0, nil); err != nil {
		t.Fatalf("executeAndStream: %v", err)
	}
	select {
	case <-writer.flushes:
		t.Fatal("known-length response unexpectedly flushed")
	default:
	}
	if got := writer.bodyString(); got != "response" {
		t.Fatalf("body = %q, want response", got)
	}
}

func TestExecuteAndStreamSkipsFlushWithoutContentType(t *testing.T) {
	writer := newFlushTestResponseWriter()
	forwarder := newTestForwarder(io.NopCloser(strings.NewReader("response")), http.Header{"X-Upstream": {"present"}})
	request, err := http.NewRequest(http.MethodGet, "https://upstream.example.com/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := forwarder.executeAndStream(writer, request, 0, nil); err != nil {
		t.Fatalf("executeAndStream: %v", err)
	}
	select {
	case <-writer.flushes:
		t.Fatal("response without Content-Type unexpectedly flushed")
	default:
	}
	if got := writer.bodyString(); got != "response" {
		t.Fatalf("body = %q, want response", got)
	}
}

func TestExecuteAndStreamPreservesWrappedNoFlusherResponseWriter(t *testing.T) {
	underlying := &noFlushTestResponseWriter{header: make(http.Header)}
	wrapped := &forwardingFlushTestResponseWriter{ResponseWriter: underlying}
	writer := &statusRecorder{ResponseWriter: wrapped}
	forwarder := newTestForwarder(io.NopCloser(strings.NewReader("response")), http.Header{})
	request, err := http.NewRequest(http.MethodGet, "https://upstream.example.com/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := forwarder.executeAndStream(writer, request, 0, nil); err != nil {
		t.Fatalf("executeAndStream: %v", err)
	}
	if wrapped.flushes != 0 {
		t.Fatalf("flush calls through no-flusher wrapper = %d, want 0", wrapped.flushes)
	}
	if got := underlying.body.String(); got != "response" {
		t.Fatalf("body = %q, want response", got)
	}
}

func TestExecuteAndStreamPreservesNoFlusherResponseWriter(t *testing.T) {
	writer := &noFlushTestResponseWriter{header: make(http.Header)}
	forwarder := newTestForwarder(io.NopCloser(strings.NewReader("response")), http.Header{"X-Upstream": {"present"}})
	request, err := http.NewRequest(http.MethodGet, "https://upstream.example.com/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := forwarder.executeAndStream(writer, request, 0, nil); err != nil {
		t.Fatalf("executeAndStream: %v", err)
	}
	if writer.status != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", writer.status, http.StatusAccepted)
	}
	if got := writer.header.Get("X-Upstream"); got != "present" {
		t.Fatalf("header = %q, want present", got)
	}
	if got := writer.body.String(); got != "response" {
		t.Fatalf("body = %q, want response", got)
	}
}

func TestPacedFlushWriterCoalescesLaterWrites(t *testing.T) {
	writer := newFlushTestResponseWriter()
	paced := newPacedFlushWriter(writer, writer)

	if _, err := paced.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := paced.Write([]byte("second")); err != nil {
		t.Fatal(err)
	}
	if _, err := paced.Write([]byte("third")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-writer.flushes:
	case <-time.After(time.Second):
		t.Fatal("first write was not flushed")
	}
	select {
	case <-writer.flushes:
		t.Fatal("later small writes flushed individually")
	default:
	}

	select {
	case <-writer.flushes:
	case <-time.After(forwarderFlushInterval + time.Second):
		t.Fatal("later small writes were not flushed on the cadence")
	}
	paced.stop()
}

func TestPacedFlushWriterFlushesPendingOnStop(t *testing.T) {
	writer := newFlushTestResponseWriter()
	paced := newPacedFlushWriter(writer, writer)

	if _, err := paced.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := paced.Write([]byte("pending")); err != nil {
		t.Fatal(err)
	}
	<-writer.flushes
	paced.stop()
	select {
	case <-writer.flushes:
	case <-time.After(time.Second):
		t.Fatal("pending body was not flushed when the stream stopped")
	}
}

func TestExecuteAndStreamReturnsTransportError(t *testing.T) {
	wantErr := errors.New("upstream unavailable")
	forwarder := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
	forwarder.httpClient = &http.Client{Transport: forwarderTestTransport(func(*http.Request) (*http.Response, error) {
		return nil, wantErr
	})}
	writer := newFlushTestResponseWriter()
	request, err := http.NewRequest(http.MethodGet, "https://upstream.example.com/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := forwarder.executeAndStream(writer, request, 0, nil); !errors.Is(err, wantErr) {
		t.Fatalf("executeAndStream error = %v, want %v", err, wantErr)
	}
}

func TestPassthroughPacedResponseReachesClientBeforeTail(t *testing.T) {
	releaseFirst := make(chan struct{})
	releaseTail := make(chan struct{})
	var releaseFirstOnce sync.Once
	var releaseTailOnce sync.Once
	releaseFirstNow := func() { releaseFirstOnce.Do(func() { close(releaseFirst) }) }
	releaseTailNow := func() { releaseTailOnce.Do(func() { close(releaseTail) }) }
	upstreamReady := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Stream-Test", "yes")
		w.WriteHeader(http.StatusAccepted)
		w.(http.Flusher).Flush()
		close(upstreamReady)
		<-releaseFirst
		_, _ = w.Write([]byte("fragment"))
		w.(http.Flusher).Flush()
		<-releaseTail
		_, _ = w.Write([]byte("tail"))
	}))
	forwarder := NewForwarder(nil, upstream.URL, "us-east-1", 1, auth.NewProxySigner("test-access-key", "test-secret-key"), nil)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := forwarder.Forward(r.Context(), w, r); err != nil {
			t.Errorf("Forward: %v", err)
		}
	}))
	defer func() {
		releaseFirstNow()
		releaseTailNow()
		proxy.Close()
		upstream.Close()
	}()

	response := make(chan *http.Response, 1)
	errorsCh := make(chan error, 1)
	go func() {
		resp, err := proxy.Client().Get(proxy.URL + "/bucket/object")
		if err != nil {
			errorsCh <- err
			return
		}
		response <- resp
	}()

	select {
	case <-upstreamReady:
	case <-time.After(time.Second):
		t.Fatal("upstream did not reach its first pause")
	}

	var resp *http.Response
	select {
	case resp = <-response:
	case err := <-errorsCh:
		t.Fatal(err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("client headers waited for the upstream tail")
	}
	defer resp.Body.Close()
	if got := resp.StatusCode; got != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", got, http.StatusAccepted)
	}
	if got := resp.Header.Get("X-Stream-Test"); got != "yes" {
		t.Fatalf("X-Stream-Test = %q, want yes", got)
	}

	releaseFirstNow()
	fragment := make([]byte, len("fragment"))
	if _, err := io.ReadFull(resp.Body, fragment); err != nil {
		t.Fatalf("read first fragment: %v", err)
	}
	if string(fragment) != "fragment" {
		t.Fatalf("first fragment = %q, want fragment", fragment)
	}

	releaseTailNow()
	if rest, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("read tail: %v", err)
	} else if string(rest) != "tail" {
		t.Fatalf("tail = %q, want tail", rest)
	}
}
