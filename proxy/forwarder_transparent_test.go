package proxy

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tigrisdata/tag/auth"
)

func TestBuildTransparentRequest_StreamingPayload(t *testing.T) {
	ps := auth.NewProxySigner("test-access-key", "test-secret-key")
	fwd := &transparentForwarder{
		baseForwarder:    newBaseForwarder("https://upstream.example.com", "us-east-1", 10),
		proxySigner:      ps,
		upstreamEndpoint: "https://upstream.example.com",
	}

	t.Run("adds Content-Encoding aws-chunked when missing", func(t *testing.T) {
		// minio-go sends STREAMING-AWS4-HMAC-SHA256-PAYLOAD but omits Content-Encoding
		chunkedBody := "4;chunk-signature=abc123\r\ntest\r\n0;chunk-signature=def456\r\n\r\n"

		req, _ := http.NewRequest(http.MethodPut, "http://localhost:8080/bucket/key", strings.NewReader(chunkedBody))
		req.ContentLength = int64(len(chunkedBody))
		req.Header.Set("X-Amz-Content-Sha256", StreamingPayloadHash)
		req.Header.Set("X-Amz-Decoded-Content-Length", "4")

		fwdReq, restore, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}
		defer restore()

		if ce := fwdReq.Header.Get("Content-Encoding"); ce != "aws-chunked" {
			t.Errorf("Content-Encoding = %q, want %q", ce, "aws-chunked")
		}

		// Body and ContentLength should be preserved (wire size, not decoded)
		if fwdReq.ContentLength != int64(len(chunkedBody)) {
			t.Errorf("ContentLength = %d, want %d", fwdReq.ContentLength, len(chunkedBody))
		}
	})

	t.Run("adds Content-Encoding for unsigned streaming payload", func(t *testing.T) {
		chunkedBody := "5\r\nhello\r\n0\r\n\r\n"

		req, _ := http.NewRequest(http.MethodPut, "http://localhost:8080/bucket/key", strings.NewReader(chunkedBody))
		req.ContentLength = int64(len(chunkedBody))
		req.Header.Set("X-Amz-Content-Sha256", StreamingUnsignedTrailerHash)
		req.Header.Set("X-Amz-Decoded-Content-Length", "5")

		fwdReq, restore, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}
		defer restore()

		if ce := fwdReq.Header.Get("Content-Encoding"); ce != "aws-chunked" {
			t.Errorf("Content-Encoding = %q, want %q", ce, "aws-chunked")
		}
	})

	t.Run("preserves existing Content-Encoding aws-chunked", func(t *testing.T) {
		chunkedBody := "4;chunk-signature=sig\r\ntest\r\n0;chunk-signature=sig\r\n\r\n"

		req, _ := http.NewRequest(http.MethodPut, "http://localhost:8080/bucket/key", strings.NewReader(chunkedBody))
		req.ContentLength = int64(len(chunkedBody))
		req.Header.Set("X-Amz-Content-Sha256", StreamingPayloadHash)
		req.Header.Set("X-Amz-Decoded-Content-Length", "4")
		req.Header.Set("Content-Encoding", "aws-chunked")

		fwdReq, restore, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}
		defer restore()

		// Should not duplicate aws-chunked
		if ce := fwdReq.Header.Get("Content-Encoding"); ce != "aws-chunked" {
			t.Errorf("Content-Encoding = %q, want %q", ce, "aws-chunked")
		}
	})

	t.Run("preserves combined Content-Encoding with aws-chunked", func(t *testing.T) {
		chunkedBody := "4;chunk-signature=sig\r\ntest\r\n0;chunk-signature=sig\r\n\r\n"

		req, _ := http.NewRequest(http.MethodPut, "http://localhost:8080/bucket/key", strings.NewReader(chunkedBody))
		req.ContentLength = int64(len(chunkedBody))
		req.Header.Set("X-Amz-Content-Sha256", StreamingPayloadHash)
		req.Header.Set("X-Amz-Decoded-Content-Length", "4")
		req.Header.Set("Content-Encoding", "aws-chunked,gzip")

		fwdReq, restore, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}
		defer restore()

		// Should not modify when aws-chunked already present
		if ce := fwdReq.Header.Get("Content-Encoding"); ce != "aws-chunked,gzip" {
			t.Errorf("Content-Encoding = %q, want %q", ce, "aws-chunked,gzip")
		}
	})

	t.Run("prepends aws-chunked to existing Content-Encoding", func(t *testing.T) {
		chunkedBody := "4;chunk-signature=sig\r\ntest\r\n0;chunk-signature=sig\r\n\r\n"

		req, _ := http.NewRequest(http.MethodPut, "http://localhost:8080/bucket/key", strings.NewReader(chunkedBody))
		req.ContentLength = int64(len(chunkedBody))
		req.Header.Set("X-Amz-Content-Sha256", StreamingPayloadHash)
		req.Header.Set("X-Amz-Decoded-Content-Length", "4")
		req.Header.Set("Content-Encoding", "gzip")

		fwdReq, restore, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}
		defer restore()

		if ce := fwdReq.Header.Get("Content-Encoding"); ce != "aws-chunked,gzip" {
			t.Errorf("Content-Encoding = %q, want %q", ce, "aws-chunked,gzip")
		}
	})

	t.Run("signed headers preserved", func(t *testing.T) {
		chunkedBody := "4;chunk-signature=abc\r\ntest\r\n0;chunk-signature=def\r\n\r\n"

		req, _ := http.NewRequest(http.MethodPut, "http://localhost:8080/bucket/key", strings.NewReader(chunkedBody))
		req.ContentLength = int64(len(chunkedBody))
		req.Header.Set("X-Amz-Content-Sha256", StreamingPayloadHash)
		req.Header.Set("X-Amz-Decoded-Content-Length", "4")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=key/date/region/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length,Signature=sig")

		fwdReq, restore, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}
		defer restore()

		if got := fwdReq.Header.Get("X-Amz-Content-Sha256"); got != StreamingPayloadHash {
			t.Errorf("X-Amz-Content-Sha256 = %q, want %q", got, StreamingPayloadHash)
		}
		if got := fwdReq.Header.Get("X-Amz-Decoded-Content-Length"); got != "4" {
			t.Errorf("X-Amz-Decoded-Content-Length = %q, want %q", got, "4")
		}
		if got := fwdReq.Header.Get("Authorization"); got == "" {
			t.Error("Authorization header missing")
		}
	})

	t.Run("non-streaming payload no Content-Encoding added", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPut, "http://localhost:8080/bucket/key", strings.NewReader("data"))
		req.ContentLength = 4
		req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

		fwdReq, restore, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}
		defer restore()

		if ce := fwdReq.Header.Get("Content-Encoding"); ce != "" {
			t.Errorf("Content-Encoding = %q, want empty for non-streaming", ce)
		}
	})

	t.Run("skips mutation when content-encoding is in SignedHeaders", func(t *testing.T) {
		chunkedBody := "4;chunk-signature=sig\r\ntest\r\n0;chunk-signature=sig\r\n\r\n"

		req, _ := http.NewRequest(http.MethodPut, "http://localhost:8080/bucket/key", strings.NewReader(chunkedBody))
		req.ContentLength = int64(len(chunkedBody))
		req.Header.Set("X-Amz-Content-Sha256", StreamingPayloadHash)
		req.Header.Set("X-Amz-Decoded-Content-Length", "4")
		req.Header.Set("Content-Encoding", "gzip")
		// content-encoding IS in SignedHeaders — must not be mutated
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=key/20260214/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length,Signature=sig")

		fwdReq, restore, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}
		defer restore()

		// Content-Encoding should be preserved as-is (no aws-chunked prepended)
		if ce := fwdReq.Header.Get("Content-Encoding"); ce != "gzip" {
			t.Errorf("Content-Encoding = %q, want %q (should not be mutated when signed)", ce, "gzip")
		}
	})

	t.Run("case-insensitive aws-chunked detection", func(t *testing.T) {
		chunkedBody := "4;chunk-signature=sig\r\ntest\r\n0;chunk-signature=sig\r\n\r\n"

		req, _ := http.NewRequest(http.MethodPut, "http://localhost:8080/bucket/key", strings.NewReader(chunkedBody))
		req.ContentLength = int64(len(chunkedBody))
		req.Header.Set("X-Amz-Content-Sha256", StreamingPayloadHash)
		req.Header.Set("X-Amz-Decoded-Content-Length", "4")
		req.Header.Set("Content-Encoding", "AWS-CHUNKED")

		fwdReq, restore, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}
		defer restore()

		// Should not duplicate — case-insensitive match should detect AWS-CHUNKED
		if ce := fwdReq.Header.Get("Content-Encoding"); ce != "AWS-CHUNKED" {
			t.Errorf("Content-Encoding = %q, want %q (should not duplicate)", ce, "AWS-CHUNKED")
		}
	})
}

func TestBuildTransparentRequest_DateHandling(t *testing.T) {
	ps := auth.NewProxySigner("test-access-key", "test-secret-key")
	fwd := &transparentForwarder{
		baseForwarder:    newBaseForwarder("https://upstream.example.com", "us-east-1", 10),
		proxySigner:      ps,
		upstreamEndpoint: "https://upstream.example.com",
	}

	tests := []struct {
		name          string
		dateHeader    string
		amzDateHeader string
		wantDate      string // expected Date header ("" means absent)
		wantAmzDate   bool   // whether X-Amz-Date should be present
	}{
		{
			name:          "both Date and X-Amz-Date present",
			dateHeader:    "Wed, 11 Feb 2026 05:55:14 GMT",
			amzDateHeader: "20260211T055514Z",
			wantDate:      "Wed, 11 Feb 2026 05:55:14 GMT",
			wantAmzDate:   true,
		},
		{
			name:          "only Date present - synthesize X-Amz-Date",
			dateHeader:    "Wed, 11 Feb 2026 05:55:14 -0000",
			amzDateHeader: "",
			wantDate:      "Wed, 11 Feb 2026 05:55:14 -0000",
			wantAmzDate:   true,
		},
		{
			name:          "only X-Amz-Date present",
			dateHeader:    "",
			amzDateHeader: "20260211T055514Z",
			wantDate:      "",
			wantAmzDate:   true,
		},
		{
			name:          "neither present",
			dateHeader:    "",
			amzDateHeader: "",
			wantDate:      "",
			wantAmzDate:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPut, "http://localhost:8080/bucket/key", nil)
			if tt.dateHeader != "" {
				req.Header.Set("Date", tt.dateHeader)
			}
			if tt.amzDateHeader != "" {
				req.Header.Set("X-Amz-Date", tt.amzDateHeader)
			}

			fwdReq, restore, err := fwd.buildTransparentRequest(t.Context(), req)
			if err != nil {
				t.Fatalf("buildTransparentRequest() error = %v", err)
			}
			defer restore()

			// Check Date header preserved
			gotDate := fwdReq.Header.Get("Date")
			if gotDate != tt.wantDate {
				t.Errorf("Date header = %q, want %q", gotDate, tt.wantDate)
			}

			// Check X-Amz-Date header
			gotAmzDate := fwdReq.Header.Get("X-Amz-Date")
			if tt.wantAmzDate && gotAmzDate == "" {
				t.Error("X-Amz-Date header is missing, want present")
			}
			if !tt.wantAmzDate && gotAmzDate != "" {
				t.Errorf("X-Amz-Date header = %q, want absent", gotAmzDate)
			}

			// When X-Amz-Date was explicitly set, it should be preserved as-is
			if tt.amzDateHeader != "" && gotAmzDate != tt.amzDateHeader {
				t.Errorf("X-Amz-Date header = %q, want %q (should be preserved)", gotAmzDate, tt.amzDateHeader)
			}

			// When synthesized from Date, verify ISO 8601 format
			if tt.amzDateHeader == "" && tt.dateHeader != "" && gotAmzDate != "" {
				if len(gotAmzDate) != 16 || gotAmzDate[8] != 'T' || gotAmzDate[15] != 'Z' {
					t.Errorf("Synthesized X-Amz-Date = %q, want ISO 8601 format (20060102T150405Z)", gotAmzDate)
				}
			}
		})
	}
}

func TestBuildTransparentRequest_BorrowsAndRestoresHeaders(t *testing.T) {
	fwd := &transparentForwarder{
		baseForwarder:    newBaseForwarder("https://upstream.example.com", "us-east-1", 10),
		proxySigner:      auth.NewProxySigner("test-access-key", "test-secret-key"),
		upstreamEndpoint: "https://upstream.example.com",
	}
	req, err := http.NewRequest(http.MethodPut, "http://localhost:8080/bucket/key", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "client.example.com"
	req.Header = http.Header{
		"Authorization":             {"client-auth"},
		"Date":                      {"Wed, 11 Feb 2026 05:55:14 GMT"},
		"X-Amz-Content-Sha256":      {StreamingPayloadHash},
		"Content-Encoding":          {"gzip"},
		"X-Tigris-Forwarded-Host":   {"client-forwarded-host"},
		"X-Tigris-Proxy-Access-Key": {"client-access-key"},
		"X-Tigris-Proxy-Timestamp":  {"client-timestamp"},
		"X-Tigris-Proxy-Signature":  {"client-signature"},
		"X-Unrelated":               {"one", "two"},
	}
	original := req.Header.Clone()
	unrelatedValues := req.Header["X-Unrelated"]
	authorizationValues := req.Header["Authorization"]

	fwdReq, restore, err := fwd.buildTransparentRequest(t.Context(), req)
	if err != nil {
		t.Fatalf("buildTransparentRequest() error = %v", err)
	}

	if got := fwdReq.Header.Get("Authorization"); got != "client-auth" {
		t.Errorf("Authorization = %q, want client-auth", got)
	}
	if &fwdReq.Header["Authorization"][0] != &authorizationValues[0] {
		t.Error("Authorization value slice was copied")
	}
	if &fwdReq.Header["X-Unrelated"][0] != &unrelatedValues[0] {
		t.Error("unrelated header value slice was copied")
	}
	fwdReq.Header.Set("X-Borrowed-Map", "visible-on-original")
	if got := req.Header.Get("X-Borrowed-Map"); got != "visible-on-original" {
		t.Errorf("borrowed header map is not shared, got %q", got)
	}
	delete(fwdReq.Header, "X-Borrowed-Map")

	if got := fwdReq.Header.Get("X-Amz-Date"); got != "20260211T055514Z" {
		t.Errorf("X-Amz-Date = %q, want synthesized date", got)
	}
	if got := fwdReq.Header.Get("Content-Encoding"); got != "aws-chunked,gzip" {
		t.Errorf("Content-Encoding = %q, want aws-chunked,gzip", got)
	}
	for _, header := range []string{
		"X-Tigris-Forwarded-Host",
		"X-Tigris-Proxy-Access-Key",
		"X-Tigris-Proxy-Timestamp",
		"X-Tigris-Proxy-Signature",
	} {
		if got := fwdReq.Header.Get(header); got == original.Get(header) {
			t.Errorf("%s was not overwritten for upstream request", header)
		}
	}

	restore()
	if !reflect.DeepEqual(req.Header, original) {
		t.Errorf("headers after restore = %#v, want %#v", req.Header, original)
	}

	// The restore handle is one-shot. A later caller change must not be undone
	// by a second invocation.
	req.Header.Set("X-Tigris-Proxy-Access-Key", "changed-after-restore")
	restore()
	if got := req.Header.Get("X-Tigris-Proxy-Access-Key"); got != "changed-after-restore" {
		t.Errorf("second restore changed a later header value to %q", got)
	}
}

type transparentForwarderTestBody struct {
	request        *http.Request
	closeErr       error
	closeCalls     int
	headersAtClose http.Header
}

func (b *transparentForwarderTestBody) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (b *transparentForwarderTestBody) Close() error {
	b.closeCalls++
	b.headersAtClose = b.request.Header.Clone()
	return b.closeErr
}

type transparentForwarderTestTransport struct {
	body *transparentForwarderTestBody
	err  error
}

func (t *transparentForwarderTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.err != nil {
		return nil, t.err
	}
	t.body.request = r
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       t.body,
	}, nil
}

func newTransparentForwarderForTest(transport http.RoundTripper) *transparentForwarder {
	base := newBaseForwarder("https://upstream.example.com", "us-east-1", 10)
	base.httpClient = &http.Client{Transport: transport}
	fwd := &transparentForwarder{
		baseForwarder:    base,
		proxySigner:      auth.NewProxySigner("test-access-key", "test-secret-key"),
		upstreamEndpoint: "https://upstream.example.com",
	}
	fwd.initInterceptor()
	return fwd
}

func newTransparentRequestForTest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://localhost:8080/bucket/key", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "client.example.com"
	req.Header.Set("Authorization", "client-auth")
	req.Header.Set("X-Tigris-Forwarded-Host", "client-forwarded-host")
	req.Header.Set("X-Tigris-Proxy-Access-Key", "client-access-key")
	req.Header.Set("X-Tigris-Proxy-Timestamp", "client-timestamp")
	req.Header.Set("X-Tigris-Proxy-Signature", "client-signature")
	return req
}

func TestTransparentForwarderDoRequestWithCredsRestoresOnBodyClose(t *testing.T) {
	closeErr := errors.New("close failed")
	body := &transparentForwarderTestBody{closeErr: closeErr}
	fwd := newTransparentForwarderForTest(&transparentForwarderTestTransport{body: body})
	req := newTransparentRequestForTest(t)
	original := req.Header.Clone()

	resp, err := fwd.DoRequestWithCreds(t.Context(), req, "", "")
	if err != nil {
		t.Fatalf("DoRequestWithCreds() error = %v", err)
	}
	if got := req.Header.Get("X-Tigris-Proxy-Access-Key"); got == original.Get("X-Tigris-Proxy-Access-Key") {
		t.Error("proxy header was restored before response body close")
	}
	if _, err := resp.Body.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Errorf("response Read() error = %v, want EOF", err)
	}
	if err := resp.Body.Close(); !errors.Is(err, closeErr) {
		t.Errorf("response Body.Close() error = %v, want %v", err, closeErr)
	}
	if body.closeCalls != 1 {
		t.Errorf("underlying body Close calls = %d, want 1", body.closeCalls)
	}
	if got := body.headersAtClose.Get("X-Tigris-Proxy-Access-Key"); got == original.Get("X-Tigris-Proxy-Access-Key") {
		t.Error("underlying body did not observe borrowed proxy header before restore")
	}
	if !reflect.DeepEqual(req.Header, original) {
		t.Errorf("headers after response close = %#v, want %#v", req.Header, original)
	}

	// Closing again must not close the transport body or restore over a later
	// caller change.
	req.Header.Set("X-Tigris-Proxy-Access-Key", "changed-after-close")
	if err := resp.Body.Close(); !errors.Is(err, closeErr) {
		t.Errorf("second Body.Close() error = %v, want %v", err, closeErr)
	}
	if body.closeCalls != 1 {
		t.Errorf("underlying body Close calls after second close = %d, want 1", body.closeCalls)
	}
	if got := req.Header.Get("X-Tigris-Proxy-Access-Key"); got != "changed-after-close" {
		t.Errorf("second Body.Close() changed header to %q", got)
	}
}

func TestTransparentForwarderRestoresOnTransportError(t *testing.T) {
	transportErr := errors.New("transport failed")
	fwd := newTransparentForwarderForTest(&transparentForwarderTestTransport{err: transportErr})
	req := newTransparentRequestForTest(t)
	original := req.Header.Clone()

	resp, err := fwd.DoRequestWithCreds(t.Context(), req, "", "")
	if resp != nil {
		t.Errorf("DoRequestWithCreds() response = %#v, want nil", resp)
	}
	if err == nil || !strings.Contains(err.Error(), transportErr.Error()) {
		t.Fatalf("DoRequestWithCreds() error = %v, want %v", err, transportErr)
	}
	if !reflect.DeepEqual(req.Header, original) {
		t.Errorf("headers after transport error = %#v, want %#v", req.Header, original)
	}
}

func TestTransparentForwarderSynchronousPathsRestoreHeaders(t *testing.T) {
	tests := []struct {
		name string
		call func(*transparentForwarder, http.ResponseWriter, *http.Request) error
	}{
		{
			name: "Forward",
			call: func(fwd *transparentForwarder, w http.ResponseWriter, r *http.Request) error {
				return fwd.Forward(r.Context(), w, r)
			},
		},
		{
			name: "ForwardWithCapture",
			call: func(fwd *transparentForwarder, w http.ResponseWriter, r *http.Request) error {
				_, err := fwd.ForwardWithCapture(r.Context(), w, r)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := &transparentForwarderTestBody{}
			fwd := newTransparentForwarderForTest(&transparentForwarderTestTransport{body: body})
			req := newTransparentRequestForTest(t)
			original := req.Header.Clone()

			if err := tt.call(fwd, httptest.NewRecorder(), req); err != nil {
				t.Fatalf("forwarding error = %v", err)
			}
			if body.closeCalls != 1 {
				t.Errorf("underlying body Close calls = %d, want 1", body.closeCalls)
			}
			if !reflect.DeepEqual(req.Header, original) {
				t.Errorf("headers after forwarding = %#v, want %#v", req.Header, original)
			}
		})
	}
}
