package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

// TestBucketRoutesMatchTrailingSlash verifies that bucket-level routes match
// paths with and without trailing slashes. S3 clients like warp send
// bucket-level requests with trailing slashes (e.g., "GET /bucket/?location=").
func TestBucketRoutesMatchTrailingSlash(t *testing.T) {
	// Build a router with the same structure as setupRouter but with simple test handlers.
	r := mux.NewRouter()
	r.SkipClean(true)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Register routes the same way setupRouter does (both /{bucket} and /{bucket}/)
	for _, prefix := range []string{"/{bucket}", "/{bucket}/"} {
		r.HandleFunc(prefix, handler).Queries("location", "").Methods("GET")
		r.HandleFunc(prefix, handler).Queries("uploads", "").Methods("GET")
		r.HandleFunc(prefix, handler).Queries("list-type", "2").Methods("GET")
		r.HandleFunc(prefix, handler).Methods("GET", "HEAD", "PUT", "DELETE")
	}

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		// Without trailing slash
		{name: "GET bucket location", method: "GET", path: "/mybucket?location=", wantStatus: 200},
		{name: "HEAD bucket", method: "HEAD", path: "/mybucket", wantStatus: 200},
		{name: "PUT bucket", method: "PUT", path: "/mybucket", wantStatus: 200},
		{name: "DELETE bucket", method: "DELETE", path: "/mybucket", wantStatus: 200},
		{name: "GET bucket uploads", method: "GET", path: "/mybucket?uploads=", wantStatus: 200},
		{name: "GET bucket list-type=2", method: "GET", path: "/mybucket?list-type=2", wantStatus: 200},

		// With trailing slash
		{name: "GET bucket location trailing slash", method: "GET", path: "/mybucket/?location=", wantStatus: 200},
		{name: "HEAD bucket trailing slash", method: "HEAD", path: "/mybucket/", wantStatus: 200},
		{name: "PUT bucket trailing slash", method: "PUT", path: "/mybucket/", wantStatus: 200},
		{name: "DELETE bucket trailing slash", method: "DELETE", path: "/mybucket/", wantStatus: 200},
		{name: "GET bucket uploads trailing slash", method: "GET", path: "/mybucket/?uploads=", wantStatus: 200},
		{name: "GET bucket list-type=2 trailing slash", method: "GET", path: "/mybucket/?list-type=2", wantStatus: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "http://localhost"+tt.path, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tt.wantStatus)
			}
		})
	}
}

// TestBucketTrailingSlashPreservesVars verifies that mux.Vars["bucket"] is
// set correctly for both /{bucket} and /{bucket}/ route patterns.
func TestBucketTrailingSlashPreservesVars(t *testing.T) {
	r := mux.NewRouter()
	r.SkipClean(true)

	var capturedBucket string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBucket = mux.Vars(r)["bucket"]
		w.WriteHeader(http.StatusOK)
	})

	for _, prefix := range []string{"/{bucket}", "/{bucket}/"} {
		r.HandleFunc(prefix, handler).Methods("GET", "HEAD", "PUT")
	}

	tests := []struct {
		name       string
		path       string
		wantBucket string
	}{
		{name: "without trailing slash", path: "/warp", wantBucket: "warp"},
		{name: "with trailing slash", path: "/warp/", wantBucket: "warp"},
		{name: "long bucket name", path: "/my-test-bucket/", wantBucket: "my-test-bucket"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capturedBucket = ""
			req := httptest.NewRequest(http.MethodGet, "http://localhost"+tt.path, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if capturedBucket != tt.wantBucket {
				t.Errorf("bucket = %q, want %q", capturedBucket, tt.wantBucket)
			}
		})
	}
}

// TestBucketTrailingSlashPreservesURLPath verifies that r.URL.Path is NOT
// modified when using duplicate routes (important for SigV4 validation).
func TestBucketTrailingSlashPreservesURLPath(t *testing.T) {
	r := mux.NewRouter()
	r.SkipClean(true)

	var capturedPath string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})

	for _, prefix := range []string{"/{bucket}", "/{bucket}/"} {
		r.HandleFunc(prefix, handler).Methods("GET", "HEAD", "PUT")
	}

	tests := []struct {
		name     string
		path     string
		wantPath string
	}{
		{name: "path without slash preserved", path: "/warp", wantPath: "/warp"},
		{name: "path with slash preserved", path: "/warp/", wantPath: "/warp/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capturedPath = ""
			req := httptest.NewRequest(http.MethodGet, "http://localhost"+tt.path, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if capturedPath != tt.wantPath {
				t.Errorf("path = %q, want %q", capturedPath, tt.wantPath)
			}
		})
	}
}
