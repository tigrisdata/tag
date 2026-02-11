package proxy

import (
	"net/http"
	"testing"

	"github.com/tigrisdata/tag/auth"
)

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
