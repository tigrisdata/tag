package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
