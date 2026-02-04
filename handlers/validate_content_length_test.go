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
		name         string
		decodedLen   string // X-Amz-Decoded-Content-Length value; empty = omit header
		wantValid    bool
	}{
		{name: "valid length", decodedLen: "1048576", wantValid: true},
		{name: "zero byte upload", decodedLen: "0", wantValid: true},
		{name: "missing header", decodedLen: "", wantValid: false},
		{name: "negative value", decodedLen: "-5", wantValid: false},
		{name: "non-numeric", decodedLen: "abc", wantValid: false},
		{name: "float value", decodedLen: "1.5", wantValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", nil)
			req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
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
