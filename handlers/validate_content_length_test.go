package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestValidateContentLength_Regular(t *testing.T) {
	tests := []struct {
		name          string
		contentLength string
		wantValid     bool
	}{
		{name: "valid", contentLength: "100", wantValid: true},
		{name: "zero", contentLength: "0", wantValid: true},
		{name: "missing", contentLength: "", wantValid: false},
		{name: "negative", contentLength: "-1", wantValid: false},
		{name: "non-numeric", contentLength: "abc", wantValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", nil)
			if tt.contentLength != "" {
				req.Header.Set("Content-Length", tt.contentLength)
			}
			w := httptest.NewRecorder()

			got := validateContentLength(w, req)
			if got != tt.wantValid {
				t.Errorf("validateContentLength() = %v, want %v", got, tt.wantValid)
			}
		})
	}
}

func TestValidateContentLength_Chunked(t *testing.T) {
	tests := []struct {
		name       string
		bodyHash   string // X-Amz-Content-Sha256 value
		decodedLen string // X-Amz-Decoded-Content-Length value; empty = omit header
		wantValid  bool
	}{
		{name: "valid length", bodyHash: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", decodedLen: "1048576", wantValid: true},
		{name: "zero byte upload", bodyHash: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", decodedLen: "0", wantValid: true},
		{name: "missing header", bodyHash: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", decodedLen: "", wantValid: false},
		{name: "negative value", bodyHash: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", decodedLen: "-5", wantValid: false},
		{name: "non-numeric", bodyHash: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", decodedLen: "abc", wantValid: false},
		{name: "float value", bodyHash: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", decodedLen: "1.5", wantValid: false},
		{name: "unsigned trailer valid", bodyHash: "STREAMING-UNSIGNED-PAYLOAD-TRAILER", decodedLen: "512", wantValid: true},
		{name: "unsigned trailer zero", bodyHash: "STREAMING-UNSIGNED-PAYLOAD-TRAILER", decodedLen: "0", wantValid: true},
		{name: "unsigned trailer missing", bodyHash: "STREAMING-UNSIGNED-PAYLOAD-TRAILER", decodedLen: "", wantValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", nil)
			req.Header.Set("X-Amz-Content-Sha256", tt.bodyHash)
			req.Header.Set("Content-Length", "5000") // wire size, should be ignored
			if tt.decodedLen != "" {
				req.Header.Set("X-Amz-Decoded-Content-Length", tt.decodedLen)
			}
			w := httptest.NewRecorder()

			got := validateContentLength(w, req)
			if got != tt.wantValid {
				t.Errorf("validateContentLength() = %v, want %v", got, tt.wantValid)
			}
		})
	}
}
