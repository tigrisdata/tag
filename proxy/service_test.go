package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestParseBucketKey(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantBucket string
		wantKey    string
	}{
		{
			name:       "bucket and key",
			path:       "/my-bucket/path/to/object.txt",
			wantBucket: "my-bucket",
			wantKey:    "path/to/object.txt",
		},
		{
			name:       "bucket only",
			path:       "/my-bucket",
			wantBucket: "my-bucket",
			wantKey:    "",
		},
		{
			name:       "bucket with trailing slash",
			path:       "/my-bucket/",
			wantBucket: "my-bucket",
			wantKey:    "",
		},
		{
			name:       "nested key path",
			path:       "/bucket/a/b/c/d/file.txt",
			wantBucket: "bucket",
			wantKey:    "a/b/c/d/file.txt",
		},
		{
			name:       "root path",
			path:       "/",
			wantBucket: "",
			wantKey:    "",
		},
		{
			name:       "empty path",
			path:       "",
			wantBucket: "",
			wantKey:    "",
		},
		{
			name:       "key with special characters",
			path:       "/bucket/file with spaces.txt",
			wantBucket: "bucket",
			wantKey:    "file with spaces.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request with a base URL, then set the path directly.
			// httptest.NewRequest has issues with empty URLs and special characters.
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.URL = &url.URL{Path: tt.path}

			bucket, key := ParseBucketKey(req)

			if bucket != tt.wantBucket {
				t.Errorf("ParseBucketKey() bucket = %q, want %q", bucket, tt.wantBucket)
			}
			if key != tt.wantKey {
				t.Errorf("ParseBucketKey() key = %q, want %q", key, tt.wantKey)
			}
		})
	}
}

func TestShouldSkipCache(t *testing.T) {
	tests := []struct {
		name         string
		cacheControl string
		want         bool
	}{
		{
			name:         "no cache-control header",
			cacheControl: "",
			want:         false,
		},
		{
			name:         "no-cache directive",
			cacheControl: "no-cache",
			want:         true,
		},
		{
			name:         "max-age=0",
			cacheControl: "max-age=0",
			want:         true,
		},
		{
			name:         "max-age with other directives",
			cacheControl: "max-age=0, must-revalidate",
			want:         true,
		},
		{
			name:         "normal max-age",
			cacheControl: "max-age=3600",
			want:         false,
		},
		{
			name:         "private",
			cacheControl: "private",
			want:         false,
		},
		{
			name:         "no-store",
			cacheControl: "no-store",
			want:         false,
		},
		{
			name:         "no-cache with no-store",
			cacheControl: "no-cache, no-store",
			want:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
			if tt.cacheControl != "" {
				req.Header.Set("Cache-Control", tt.cacheControl)
			}

			got := shouldSkipCache(req)
			if got != tt.want {
				t.Errorf("shouldSkipCache() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResponseCapture_ContentLength(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want int64
	}{
		{
			name: "empty body",
			body: nil,
			want: 0,
		},
		{
			name: "non-empty body",
			body: []byte("test content"),
			want: 12,
		},
		{
			name: "large body",
			body: make([]byte, 1024),
			want: 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capture := &ResponseCapture{
				Body: tt.body,
			}

			got := capture.ContentLength()
			if got != tt.want {
				t.Errorf("ContentLength() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestCopyHeaders(t *testing.T) {
	src := http.Header{
		"Content-Type":   []string{"application/json"},
		"X-Custom":       []string{"value1", "value2"},
		"Content-Length": []string{"100"},
	}

	dst := http.Header{}
	copyHeaders(dst, src)

	// Check all headers were copied
	if dst.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q, want %q", dst.Get("Content-Type"), "application/json")
	}

	if len(dst["X-Custom"]) != 2 {
		t.Errorf("X-Custom values count = %d, want 2", len(dst["X-Custom"]))
	}

	if dst.Get("Content-Length") != "100" {
		t.Errorf("Content-Length = %q, want %q", dst.Get("Content-Length"), "100")
	}
}
