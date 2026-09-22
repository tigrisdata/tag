package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tigrisdata/tag/auth"
)

type forwarderTestTransport func(*http.Request) (*http.Response, error)

func (f forwarderTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestConditionalForwarderPreservesRequestAndSignature(t *testing.T) {
	const (
		accessKey = "AKIAIOSFODNN7EXAMPLE"
		secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		bucket    = "escaped-bucket"
		key       = "object/with space+and%25?literal"
	)
	lastModified := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC).Unix()
	tests := []struct {
		name         string
		method       string
		etag         string
		lastModified int64
		rangeHeader  string
	}{
		{name: "get none", method: http.MethodGet},
		{name: "get all", method: http.MethodGet, etag: `W/"etag with space"`, lastModified: lastModified, rangeHeader: "bytes=7-19"},
		{name: "head etag", method: http.MethodHead, etag: `"etag"`},
		{name: "head date", method: http.MethodHead, lastModified: lastModified},
		{name: "head all", method: http.MethodHead, etag: `W/"etag with space"`, lastModified: lastModified},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured *http.Request
			forwarder := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
			forwarder.httpClient = &http.Client{Transport: forwarderTestTransport(func(req *http.Request) (*http.Response, error) {
				captured = req
				return &http.Response{
					StatusCode: http.StatusNotModified,
					Header:     make(http.Header),
					Body:       http.NoBody,
				}, nil
			})}

			var resp *http.Response
			var err error
			if tt.method == http.MethodHead {
				resp, err = forwarder.DoConditionalHeadRequest(t.Context(), bucket, key, accessKey, secretKey, tt.etag, tt.lastModified)
			} else {
				resp, err = forwarder.DoConditionalGetRequest(t.Context(), bucket, key, accessKey, secretKey, tt.etag, tt.lastModified, tt.rangeHeader)
			}
			if err != nil {
				t.Fatalf("conditional request: %v", err)
			}
			if err := resp.Body.Close(); err != nil {
				t.Fatalf("response body close: %v", err)
			}
			if captured == nil {
				t.Fatal("transport did not capture a request")
			}
			if captured.Method != tt.method {
				t.Errorf("method = %q, want %q", captured.Method, tt.method)
			}
			if captured.URL.Path != "/"+bucket+"/"+key || captured.URL.RawQuery != "" {
				t.Errorf("target = %q?%s, want literal object path", captured.URL.Path, captured.URL.RawQuery)
			}
			if got := captured.Header.Get("Range"); got != tt.rangeHeader {
				t.Errorf("Range = %q, want %q", got, tt.rangeHeader)
			}
			if got := captured.Header.Get("If-None-Match"); got != tt.etag {
				t.Errorf("If-None-Match = %q, want %q", got, tt.etag)
			}
			wantDate := ""
			if tt.lastModified > 0 {
				wantDate = time.Unix(tt.lastModified, 0).UTC().Format(http.TimeFormat)
			}
			if got := captured.Header.Get("If-Modified-Since"); got != wantDate {
				t.Errorf("If-Modified-Since = %q, want %q", got, wantDate)
			}
			if got := captured.Header.Get("Authorization"); !strings.Contains(got, "SignedHeaders=host;x-amz-content-sha256;x-amz-date") {
				t.Errorf("Authorization = %q, want fixed signed headers", got)
			}

			store := auth.NewCredentialStore()
			store.AddCredential(accessKey, secretKey)
			if _, err := auth.NewRequestValidator(store).ValidateRequest(captured); err != nil {
				t.Errorf("ValidateRequest() error = %v", err)
			}
		})
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
