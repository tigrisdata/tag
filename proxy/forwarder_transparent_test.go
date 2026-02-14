package proxy

import (
	"net/http"
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

		fwdReq, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}

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

		fwdReq, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}

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

		fwdReq, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}

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

		fwdReq, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}

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

		fwdReq, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}

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

		fwdReq, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}

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

		fwdReq, err := fwd.buildTransparentRequest(t.Context(), req)
		if err != nil {
			t.Fatalf("buildTransparentRequest() error = %v", err)
		}

		if ce := fwdReq.Header.Get("Content-Encoding"); ce != "" {
			t.Errorf("Content-Encoding = %q, want empty for non-streaming", ce)
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

			fwdReq, err := fwd.buildTransparentRequest(t.Context(), req)
			if err != nil {
				t.Fatalf("buildTransparentRequest() error = %v", err)
			}

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
