package proxy

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
)

type forwarderTestTransport func(*http.Request) (*http.Response, error)

func TestNewBaseForwarderUsesBoundedIdlePool(t *testing.T) {
	for _, test := range []struct {
		name           string
		configuredPool int
		wantPool       int
	}{
		{name: "default", configuredPool: 0, wantPool: config.DefaultMaxIdleConnsPerHost},
		{name: "operator cap", configuredPool: 64, wantPool: 64},
	} {
		t.Run(test.name, func(t *testing.T) {
			forwarder := newBaseForwarder("https://upstream.example.com", "us-east-1", test.configuredPool)
			transport, ok := forwarder.httpClient.Transport.(*http.Transport)
			if !ok {
				t.Fatalf("transport type = %T, want *http.Transport", forwarder.httpClient.Transport)
			}
			if transport.MaxIdleConns != test.wantPool {
				t.Errorf("MaxIdleConns = %d, want %d", transport.MaxIdleConns, test.wantPool)
			}
			if transport.MaxIdleConnsPerHost != test.wantPool {
				t.Errorf("MaxIdleConnsPerHost = %d, want %d", transport.MaxIdleConnsPerHost, test.wantPool)
			}
			if transport.IdleConnTimeout != 90*time.Second {
				t.Errorf("IdleConnTimeout = %v, want 90s", transport.IdleConnTimeout)
			}
		})
	}
}

func (f forwarderTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type failedResponseBody struct {
	reader *strings.Reader
	err    error
	closed bool
}

func (b *failedResponseBody) Read(p []byte) (int, error) {
	if b.reader.Len() > 0 {
		return b.reader.Read(p)
	}
	return 0, b.err
}

func (b *failedResponseBody) Close() error {
	b.closed = true
	return nil
}

func TestExecuteAndStreamReturningMetaReturnsBodyReadError(t *testing.T) {
	readErr := errors.New("upstream response body failed")
	body := &failedResponseBody{reader: strings.NewReader("prefix"), err: readErr}
	headers := make(http.Header)
	headers.Set("ETag", `"upstream-etag"`)
	forwarder := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
	forwarder.httpClient = &http.Client{Transport: forwarderTestTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: headers, Body: body}, nil
	})}
	request, err := http.NewRequest(http.MethodGet, "https://upstream.example.com/bucket", nil)
	if err != nil {
		t.Fatal(err)
	}
	writer := httptest.NewRecorder()

	status, gotHeaders, err := forwarder.executeAndStreamReturningMeta(writer, request, 0, nil)
	if !errors.Is(err, readErr) {
		t.Fatalf("executeAndStreamReturningMeta error = %v, want %v", err, readErr)
	}
	if status != http.StatusOK {
		t.Errorf("status = %d, want %d", status, http.StatusOK)
	}
	if gotHeaders.Get("ETag") != `"upstream-etag"` {
		t.Errorf("captured ETag = %q, want upstream ETag", gotHeaders.Get("ETag"))
	}
	if writer.Body.String() != "prefix" {
		t.Errorf("client body = %q, want received prefix", writer.Body.String())
	}
	if !body.closed {
		t.Error("upstream response body was not closed")
	}
}

func TestExecuteAndStreamReturningMetaOwnsHeaders(t *testing.T) {
	upstreamHeaders := make(http.Header)
	upstreamHeaders.Set("ETag", `"upstream-etag"`)
	upstreamHeaders["X-Upstream-Metadata"] = []string{"first", "second"}

	forwarder := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
	forwarder.httpClient = &http.Client{Transport: forwarderTestTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     upstreamHeaders,
			Body:       io.NopCloser(strings.NewReader("response body")),
		}, nil
	})}

	writer := httptest.NewRecorder()
	request, err := http.NewRequest(http.MethodPut, "https://upstream.example.com/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}
	status, headers, err := forwarder.executeAndStreamReturningMeta(writer, request, 0, nil)
	if err != nil {
		t.Fatalf("executeAndStreamReturningMeta: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}
	if writer.Body.String() != "response body" {
		t.Errorf("client body = %q, want response body", writer.Body.String())
	}
	if writer.Header().Get("ETag") != `"upstream-etag"` {
		t.Errorf("client ETag = %q, want upstream ETag", writer.Header().Get("ETag"))
	}
	if headers.Get("ETag") != `"upstream-etag"` {
		t.Fatalf("captured ETag = %q, want upstream ETag", headers.Get("ETag"))
	}
	if got := headers.Values("X-Upstream-Metadata"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("captured metadata = %q, want [first second]", got)
	}

	upstreamHeaders.Set("ETag", `"changed-upstream-etag"`)
	upstreamHeaders["X-Upstream-Metadata"][0] = "changed"
	if headers.Get("ETag") != `"upstream-etag"` {
		t.Errorf("captured ETag changed with upstream map: %q", headers.Get("ETag"))
	}
	if got := headers.Values("X-Upstream-Metadata"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Errorf("captured metadata changed with upstream slice: %q", got)
	}

	headers.Set("ETag", `"changed-captured-etag"`)
	if upstreamHeaders.Get("ETag") != `"changed-upstream-etag"` {
		t.Errorf("upstream ETag changed with captured map: %q", upstreamHeaders.Get("ETag"))
	}
}

type partialResponseWriter struct {
	header        http.Header
	statusCode    int
	bytesAccepted int
}

func (w *partialResponseWriter) Header() http.Header { return w.header }

func (w *partialResponseWriter) WriteHeader(statusCode int) {
	if w.statusCode == 0 {
		w.statusCode = statusCode
	}
}

func (w *partialResponseWriter) Write(body []byte) (int, error) {
	accepted := 3
	if accepted > len(body) {
		accepted = len(body)
	}
	w.bytesAccepted += accepted
	return accepted, io.ErrClosedPipe
}

func TestDeleteObjectsResponseStagerCountsClientAcceptedBytes(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 10*1024)
	headers := make(http.Header)
	headers.Set("Content-Type", "application/xml")
	forwarder := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
	forwarder.httpClient = &http.Client{Transport: forwarderTestTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        headers.Clone(),
			Body:          io.NopCloser(bytes.NewReader(body)),
			ContentLength: int64(len(body)),
		}, nil
	})}

	outBytes := metrics.BytesTransferred.WithLabelValues("out")
	before := testutil.ToFloat64(outBytes)
	stager := newDeleteObjectsResponseStager()
	request, err := http.NewRequest(http.MethodPost, "https://upstream.example.com/bucket?delete", nil)
	if err != nil {
		t.Fatalf("create upstream request: %v", err)
	}
	capture, err := forwarder.executeAndCapture(stager, request, 0, nil)
	if err != nil {
		t.Fatalf("executeAndCapture: %v", err)
	}
	if len(capture.Body) != len(body) {
		t.Fatalf("captured bytes = %d, want %d", len(capture.Body), len(body))
	}
	if got := testutil.ToFloat64(outBytes) - before; got != 0 {
		t.Fatalf("outbound bytes counted before client commit = %v, want 0", got)
	}

	w := &partialResponseWriter{header: make(http.Header)}
	stager.commit(w, capture)
	if w.statusCode != http.StatusOK || w.bytesAccepted != 3 {
		t.Fatalf("client write status/bytes = %d/%d, want 200/3", w.statusCode, w.bytesAccepted)
	}
	if got := testutil.ToFloat64(outBytes) - before; got != 3 {
		t.Fatalf("outbound bytes = %v, want the 3 bytes accepted by the client writer", got)
	}

	fullBefore := testutil.ToFloat64(outBytes)
	fullStager := newDeleteObjectsResponseStager()
	fullRequest, err := http.NewRequest(http.MethodPost, "https://upstream.example.com/bucket?delete", nil)
	if err != nil {
		t.Fatalf("create full-write upstream request: %v", err)
	}
	fullCapture, err := forwarder.executeAndCapture(fullStager, fullRequest, 0, nil)
	if err != nil {
		t.Fatalf("executeAndCapture for full write: %v", err)
	}
	if got := testutil.ToFloat64(outBytes) - fullBefore; got != 0 {
		t.Fatalf("outbound bytes counted before full client commit = %v, want 0", got)
	}
	fullWriter := httptest.NewRecorder()
	fullStager.commit(fullWriter, fullCapture)
	if fullWriter.Code != http.StatusOK || !bytes.Equal(fullWriter.Body.Bytes(), body) {
		t.Fatalf("full client response = status %d and %d bytes", fullWriter.Code, fullWriter.Body.Len())
	}
	if got := testutil.ToFloat64(outBytes) - fullBefore; got != float64(len(body)) {
		t.Fatalf("outbound bytes after full client write = %v, want %d", got, len(body))
	}

	regularBefore := testutil.ToFloat64(outBytes)
	regularRequest, err := http.NewRequest(http.MethodPost, "https://upstream.example.com/bucket?delete", nil)
	if err != nil {
		t.Fatalf("create regular-write request: %v", err)
	}
	regularWriter := httptest.NewRecorder()
	regularCapture, err := forwarder.executeAndCapture(regularWriter, regularRequest, 0, nil)
	if err != nil {
		t.Fatalf("executeAndCapture for regular writer: %v", err)
	}
	if regularWriter.Code != http.StatusOK || !bytes.Equal(regularWriter.Body.Bytes(), body) || !bytes.Equal(regularCapture.Body, body) {
		t.Fatalf("regular client response = status %d, %d body bytes, %d captured bytes", regularWriter.Code, regularWriter.Body.Len(), len(regularCapture.Body))
	}
	if got := testutil.ToFloat64(outBytes) - regularBefore; got != float64(len(body)) {
		t.Fatalf("outbound bytes for regular writer = %v, want %d", got, len(body))
	}
}
