package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExtractAccessKey_Header(t *testing.T) {
	tests := []struct {
		name        string
		authHeader  string
		wantKey     string
		wantErr     bool
		errContains string
	}{
		{
			name:       "valid SigV4 header",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024",
			wantKey:    "AKIAIOSFODNN7EXAMPLE",
			wantErr:    false,
		},
		{
			name:        "unsupported auth scheme",
			authHeader:  "Basic dXNlcjpwYXNz",
			wantErr:     true,
			errContains: "unsupported",
		},
		{
			name:        "malformed header",
			authHeader:  "AWS4-HMAC-SHA256 InvalidFormat",
			wantErr:     true,
			errContains: "invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			req.Header.Set("Authorization", tt.authHeader)

			key, err := ExtractAccessKey(req)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ExtractAccessKey() error = nil, want error")
				}
				return
			}

			if err != nil {
				t.Errorf("ExtractAccessKey() error = %v, want nil", err)
				return
			}

			if key != tt.wantKey {
				t.Errorf("ExtractAccessKey() = %q, want %q", key, tt.wantKey)
			}
		})
	}
}

func TestExtractAccessKey_QueryString(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantKey string
		wantErr bool
	}{
		{
			name:    "valid presigned URL",
			url:     "/test-bucket/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaa",
			wantKey: "AKIAIOSFODNN7EXAMPLE",
			wantErr: false,
		},
		{
			name:    "missing credential",
			url:     "/test-bucket/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			// No Authorization header

			key, err := ExtractAccessKey(req)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ExtractAccessKey() error = nil, want error")
				}
				return
			}

			if err != nil {
				t.Errorf("ExtractAccessKey() error = %v, want nil", err)
				return
			}

			if key != tt.wantKey {
				t.Errorf("ExtractAccessKey() = %q, want %q", key, tt.wantKey)
			}
		})
	}
}

func TestParseAuthInfo_Header(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024")

	info, err := ParseAuthInfo(req)
	if err != nil {
		t.Fatalf("ParseAuthInfo() error = %v", err)
	}

	if info.AccessKey != "AKIAIOSFODNN7EXAMPLE" {
		t.Errorf("AccessKey = %q, want %q", info.AccessKey, "AKIAIOSFODNN7EXAMPLE")
	}

	if info.IsPresigned {
		t.Error("IsPresigned = true, want false")
	}

	if info.Region != "us-east-1" {
		t.Errorf("Region = %q, want %q", info.Region, "us-east-1")
	}

	if info.Date != "20130524" {
		t.Errorf("Date = %q, want %q", info.Date, "20130524")
	}

	if info.Signature != "fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024" {
		t.Errorf("Signature = %q, want %q", info.Signature, "fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024")
	}

	expectedHeaders := []string{"host", "range", "x-amz-date"}
	if len(info.SignedHeaders) != len(expectedHeaders) {
		t.Errorf("SignedHeaders length = %d, want %d", len(info.SignedHeaders), len(expectedHeaders))
	}
}

func TestParseAuthInfo_PresignedURL(t *testing.T) {
	url := "/test-bucket/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaa"
	req := httptest.NewRequest(http.MethodGet, url, nil)

	info, err := ParseAuthInfo(req)
	if err != nil {
		t.Fatalf("ParseAuthInfo() error = %v", err)
	}

	if info.AccessKey != "AKIAIOSFODNN7EXAMPLE" {
		t.Errorf("AccessKey = %q, want %q", info.AccessKey, "AKIAIOSFODNN7EXAMPLE")
	}

	if !info.IsPresigned {
		t.Error("IsPresigned = false, want true")
	}

	if info.Region != "us-east-1" {
		t.Errorf("Region = %q, want %q", info.Region, "us-east-1")
	}

	if info.Signature != "aaaaaaa" {
		t.Errorf("Signature = %q, want %q", info.Signature, "aaaaaaa")
	}
}

func TestIsPresignedRequest(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{
			name:     "presigned URL",
			url:      "/bucket/key?X-Amz-Credential=ABC/20230101/us-east-1/s3/aws4_request",
			expected: true,
		},
		{
			name:     "regular URL",
			url:      "/bucket/key",
			expected: false,
		},
		{
			name:     "URL with other query params",
			url:      "/bucket?list-type=2&prefix=test",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			result := IsPresignedRequest(req)

			if result != tt.expected {
				t.Errorf("IsPresignedRequest() = %v, want %v", result, tt.expected)
			}
		})
	}
}
