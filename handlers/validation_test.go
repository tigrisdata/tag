package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

func TestValidateBucketName(t *testing.T) {
	tests := []struct {
		name       string
		bucketName string
		wantValid  bool
	}{
		// Valid bucket names
		{name: "valid simple", bucketName: "mybucket", wantValid: true},
		{name: "valid with numbers", bucketName: "bucket123", wantValid: true},
		{name: "valid with hyphens", bucketName: "my-bucket-name", wantValid: true},
		{name: "valid with dots", bucketName: "my.bucket.name", wantValid: true},
		{name: "valid minimum length", bucketName: "abc", wantValid: true},
		{name: "valid 63 chars", bucketName: "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz1", wantValid: true},

		// Invalid: empty
		{name: "empty bucket name", bucketName: "", wantValid: false},

		// Invalid: length
		{name: "too short 1 char", bucketName: "a", wantValid: false},
		{name: "too short 2 chars", bucketName: "ab", wantValid: false},
		{name: "too long 64 chars", bucketName: "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz12", wantValid: false},

		// Invalid: characters
		{name: "uppercase letters", bucketName: "MyBucket", wantValid: false},
		{name: "underscore", bucketName: "my_bucket", wantValid: false},
		{name: "special characters", bucketName: "my@bucket", wantValid: false},

		// Invalid: start/end
		{name: "starts with hyphen", bucketName: "-mybucket", wantValid: false},
		{name: "ends with hyphen", bucketName: "mybucket-", wantValid: false},
		{name: "starts with dot", bucketName: ".mybucket", wantValid: false},
		{name: "ends with dot", bucketName: "mybucket.", wantValid: false},

		// Invalid: patterns
		{name: "consecutive dots", bucketName: "my..bucket", wantValid: false},
		{name: "dot-dash pattern", bucketName: "my.-bucket", wantValid: false},
		{name: "dash-dot pattern", bucketName: "my-.bucket", wantValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a request with the bucket name in the path
			req := httptest.NewRequest(http.MethodGet, "/"+tt.bucketName, nil)

			// Set up mux vars to simulate gorilla/mux routing
			req = mux.SetURLVars(req, map[string]string{"bucket": tt.bucketName})

			// Create response recorder
			w := httptest.NewRecorder()

			// Call validateBucketName
			got := validateBucketName(w, req)

			if got != tt.wantValid {
				t.Errorf("validateBucketName() = %v, want %v", got, tt.wantValid)
			}

			// If invalid, check that an error response was written
			if !tt.wantValid && tt.bucketName != "" {
				if w.Code != http.StatusBadRequest {
					t.Errorf("HTTP status = %d, want %d for invalid bucket", w.Code, http.StatusBadRequest)
				}
			}
		})
	}
}
