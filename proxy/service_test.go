package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/tigrisdata/tag/cache"
)

func TestParseBucketKey(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		host       string
		presigned  bool
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
		{
			name:       "Tigris virtual host",
			path:       "/tagcheck/small.bin",
			host:       "example-bucket.t3.tigrisfiles.io",
			presigned:  true,
			wantBucket: "example-bucket",
			wantKey:    "tagcheck/small.bin",
		},
		{
			// A TAG endpoint can be served under a name of this shape, so the host
			// alone does not identify a bucket: the request stays path-style.
			name:       "non-Tigris host stays path-style",
			path:       "/bucket/tagcheck/small.bin",
			host:       "tag.s3.example.com",
			presigned:  true,
			wantBucket: "bucket",
			wantKey:    "tagcheck/small.bin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request with a base URL, then set the path directly.
			// httptest.NewRequest has issues with empty URLs and special characters.
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.URL = &url.URL{Path: tt.path}
			req.Host = tt.host
			if tt.presigned {
				req.URL.RawQuery = "X-Amz-Credential=key%2F20260717%2Fauto%2Fs3%2Faws4_request"
			}

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

func TestShouldForceRevalidate(t *testing.T) {
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
			name:         "max-age=0 with must-revalidate",
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

			got := shouldForceRevalidate(req)
			if got != tt.want {
				t.Errorf("shouldForceRevalidate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShouldBypassCache(t *testing.T) {
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
			want:         false,
		},
		{
			name:         "no-store",
			cacheControl: "no-store",
			want:         true,
		},
		{
			name:         "no-store with no-cache",
			cacheControl: "no-cache, no-store",
			want:         true,
		},
		{
			name:         "normal max-age",
			cacheControl: "max-age=3600",
			want:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
			if tt.cacheControl != "" {
				req.Header.Set("Cache-Control", tt.cacheControl)
			}

			got := shouldBypassCache(req)
			if got != tt.want {
				t.Errorf("shouldBypassCache() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsCacheEligiblePresignedRequest(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		target  string
		headers http.Header
		want    bool
	}{
		{
			name:   "standard GET",
			method: http.MethodGet,
			target: "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260715T080000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=abc&x-id=GetObject",
			want:   true,
		},
		{
			name:   "standard HEAD",
			method: http.MethodHead,
			target: "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260715T080000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=abc&x-id=HeadObject",
			want:   true,
		},
		{
			name:   "GET with checksum mode",
			method: http.MethodGet,
			target: "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Checksum-Mode=ENABLED&X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260715T080000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=abc&x-id=GetObject",
			want:   true,
		},
		{
			name:   "HEAD with checksum mode",
			method: http.MethodHead,
			target: "/bucket/key?X-Amz-Checksum-Mode=ENABLED&X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request",
			want:   true,
		},
		{
			name:   "PUT with checksum mode",
			method: http.MethodPut,
			target: "/bucket/key?X-Amz-Checksum-Mode=ENABLED&X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request",
			want:   false,
		},
		{
			name:   "invalid checksum mode",
			method: http.MethodGet,
			target: "/bucket/key?X-Amz-Checksum-Mode=DISABLED&X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request",
			want:   false,
		},
		{
			name:   "semantic query",
			method: http.MethodGet,
			target: "/bucket/key?X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request&response-content-type=text%2Fplain",
			want:   false,
		},
		{
			name:   "version id",
			method: http.MethodGet,
			target: "/bucket/key?X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request&versionId=abc",
			want:   false,
		},
		{
			name:   "part number",
			method: http.MethodGet,
			target: "/bucket/key?X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request&partNumber=1",
			want:   false,
		},
		{
			name:   "temporary credentials",
			method: http.MethodGet,
			target: "/bucket/key?X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Security-Token=token",
			want:   false,
		},
		{
			name:    "conditional header",
			method:  http.MethodGet,
			target:  "/bucket/key?X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request",
			headers: http.Header{"If-None-Match": {`"etag"`}},
			want:    false,
		},
		{
			name:    "forced revalidation",
			method:  http.MethodGet,
			target:  "/bucket/key?X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request",
			headers: http.Header{"Cache-Control": {"no-cache"}},
			want:    false,
		},
		{
			name:    "no-store",
			method:  http.MethodGet,
			target:  "/bucket/key?X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request",
			headers: http.Header{"Cache-Control": {"no-store"}},
			want:    false,
		},
		{
			name:    "mixed-case cache control",
			method:  http.MethodGet,
			target:  "/bucket/key?X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request",
			headers: http.Header{"Cache-Control": {"Max-Age=3600"}},
			want:    false,
		},
		{
			name:    "browser Origin header",
			method:  http.MethodGet,
			target:  "/bucket/key?X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request",
			headers: http.Header{"Origin": {"https://app.example.com"}},
			want:    false,
		},
		{
			name:    "HEAD range",
			method:  http.MethodHead,
			target:  "/bucket/key?X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request",
			headers: http.Header{"Range": {"bytes=0-9"}},
			want:    false,
		},
		{
			name:    "S3 control header",
			method:  http.MethodGet,
			target:  "/bucket/key?X-Amz-Credential=key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request",
			headers: http.Header{"X-Amz-Expected-Bucket-Owner": {"owner"}},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.target, nil)
			req.Header = tt.headers
			if got := isCacheEligiblePresignedRequest(req); got != tt.want {
				t.Errorf("isCacheEligiblePresignedRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCachedMetaSatisfiesChecksumRequest(t *testing.T) {
	checksumRequest := httptest.NewRequest(
		http.MethodGet,
		"/bucket/key?X-Amz-Checksum-Mode=ENABLED",
		nil,
	)
	plainRequest := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	withoutChecksum := &cache.CachedObjectMeta{ETag: `"etag"`}
	withChecksum := &cache.CachedObjectMeta{
		ETag: `"etag"`,
		ChecksumHeaders: map[string]string{
			"x-amz-checksum-crc32c": "checksum-value",
		},
	}
	fetchedWithChecksumMode := &cache.CachedObjectMeta{
		ETag:         `"etag"`,
		ChecksumMode: true,
	}

	if cachedMetaSatisfiesRequest(checksumRequest, withoutChecksum) {
		t.Error("checksum request accepted cached metadata without a checksum")
	}
	if !cachedMetaSatisfiesRequest(checksumRequest, withChecksum) {
		t.Error("checksum request rejected cached metadata with a checksum")
	}
	if !cachedMetaSatisfiesRequest(checksumRequest, fetchedWithChecksumMode) {
		t.Error("checksum request rejected metadata fetched in checksum mode")
	}
	if !cachedMetaSatisfiesRequest(plainRequest, withoutChecksum) {
		t.Error("plain request rejected otherwise valid cached metadata")
	}
}

func TestHideUnrequestedChecksums(t *testing.T) {
	checksumRequest := httptest.NewRequest(
		http.MethodGet,
		"/bucket/key?X-Amz-Checksum-Mode=ENABLED",
		nil,
	)
	headerRequest := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	headerRequest.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
	plainRequest := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	newMeta := func() *cache.CachedObjectMeta {
		return &cache.CachedObjectMeta{
			ETag: `"etag"`,
			ChecksumHeaders: map[string]string{
				"x-amz-checksum-crc32c": "checksum-value",
			},
		}
	}

	kept := newMeta()
	hideUnrequestedChecksums(checksumRequest, kept)
	if len(kept.ChecksumHeaders) != 1 {
		t.Errorf("ChecksumHeaders = %#v, want the checksum kept for a checksum-mode request", kept.ChecksumHeaders)
	}

	keptForHeader := newMeta()
	hideUnrequestedChecksums(headerRequest, keptForHeader)
	if len(keptForHeader.ChecksumHeaders) != 1 {
		t.Errorf("ChecksumHeaders = %#v, want the checksum kept for a header-signed checksum-mode request",
			keptForHeader.ChecksumHeaders)
	}

	dropped := newMeta()
	hideUnrequestedChecksums(plainRequest, dropped)
	if len(dropped.ChecksumHeaders) != 0 {
		t.Errorf("ChecksumHeaders = %#v, want no checksum for a request that did not ask", dropped.ChecksumHeaders)
	}

	hideUnrequestedChecksums(plainRequest, nil) // must not panic
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

func TestCopyHeaders_MetadataLowercase(t *testing.T) {
	// Create headers using Set() to ensure proper canonical form
	// This simulates how Go's HTTP library stores headers from real responses
	src := http.Header{}
	src.Set("X-Amz-Meta-Custom-Key", "custom-value")
	src.Set("X-Amz-Meta-Another", "another-value")
	src.Set("Content-Type", "application/octet-stream")
	src.Set("ETag", `"abc123"`)

	dst := http.Header{}
	copyHeaders(dst, src)

	// Metadata headers should be stored with lowercase keys
	if _, ok := dst["x-amz-meta-custom-key"]; !ok {
		t.Error("x-amz-meta-custom-key should be stored with lowercase key")
	}
	// Check canonical key is NOT used (it would be "X-Amz-Meta-Custom-Key")
	if _, ok := dst["X-Amz-Meta-Custom-Key"]; ok {
		t.Error("X-Amz-Meta-Custom-Key should NOT be stored with canonical key")
	}

	if _, ok := dst["x-amz-meta-another"]; !ok {
		t.Error("x-amz-meta-another should be stored with lowercase key")
	}

	// Non-metadata headers should retain canonical form and be accessible via Get
	if dst.Get("Content-Type") != "application/octet-stream" {
		t.Errorf("Content-Type = %q, want %q", dst.Get("Content-Type"), "application/octet-stream")
	}
	if dst.Get("ETag") != `"abc123"` {
		t.Errorf("ETag = %q, want %q", dst.Get("ETag"), `"abc123"`)
	}

	// Verify metadata values are correct
	if dst["x-amz-meta-custom-key"][0] != "custom-value" {
		t.Errorf("x-amz-meta-custom-key value = %q, want %q", dst["x-amz-meta-custom-key"][0], "custom-value")
	}
}
