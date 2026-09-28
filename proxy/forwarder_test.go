package proxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/tigrisdata/tag/metrics"
)

type forwarderTestTransport func(*http.Request) (*http.Response, error)

func (f forwarderTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
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
}
