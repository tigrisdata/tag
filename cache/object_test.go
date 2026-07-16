package cache

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMetaFromHTTPHeaders(t *testing.T) {
	// Use http.Header.Set() to ensure canonical header names are used.
	headers := make(http.Header)
	headers.Set("ETag", `"abc123"`)
	headers.Set("Content-Type", "application/json")
	headers.Set("Content-Length", "1024")
	headers.Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
	headers.Set("Cache-Control", "max-age=3600")
	headers.Set("X-Amz-Meta-Custom", "custom-value")
	headers.Set("X-Amz-Storage-Class", "STANDARD")
	headers.Set("X-Amz-Acl", "public-read")

	obj := MetaFromHTTPHeaders("test-bucket", "test-key", http.StatusOK, headers)

	if obj.Bucket != "test-bucket" {
		t.Errorf("Bucket = %q, want %q", obj.Bucket, "test-bucket")
	}

	if obj.Key != "test-key" {
		t.Errorf("Key = %q, want %q", obj.Key, "test-key")
	}

	if obj.ETag != `"abc123"` {
		t.Errorf("ETag = %q, want %q", obj.ETag, `"abc123"`)
	}

	if obj.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want %q", obj.ContentType, "application/json")
	}

	if obj.ContentLength != 1024 {
		t.Errorf("ContentLength = %d, want %d", obj.ContentLength, 1024)
	}

	if obj.CacheControl != "max-age=3600" {
		t.Errorf("CacheControl = %q, want %q", obj.CacheControl, "max-age=3600")
	}

	if obj.StorageClass != "STANDARD" {
		t.Errorf("StorageClass = %q, want %q", obj.StorageClass, "STANDARD")
	}

	if obj.ACL != "public-read" {
		t.Errorf("ACL = %q, want %q", obj.ACL, "public-read")
	}

	if obj.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", obj.StatusCode, http.StatusOK)
	}
}

func TestCachedObjectMeta_WriteHeaders(t *testing.T) {
	// Use Unix timestamp for LastModified
	lastModified := time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC).Unix()

	obj := &CachedObjectMeta{
		Key:           "test-key",
		Bucket:        "test-bucket",
		ETag:          `"abc123"`,
		ContentType:   "application/json",
		ContentLength: 1024,
		LastModified:  lastModified,
		CacheControl:  "max-age=3600",
		StorageClass:  "STANDARD",
		UserMetadata: map[string]string{
			"X-Amz-Meta-Custom": "custom-value",
		},
		StatusCode: http.StatusOK,
	}

	w := httptest.NewRecorder()
	obj.WriteHeaders(w)

	if w.Header().Get("ETag") != `"abc123"` {
		t.Errorf("ETag header = %q, want %q", w.Header().Get("ETag"), `"abc123"`)
	}

	if w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type header = %q, want %q", w.Header().Get("Content-Type"), "application/json")
	}

	if w.Header().Get("Content-Length") != "1024" {
		t.Errorf("Content-Length header = %q, want %q", w.Header().Get("Content-Length"), "1024")
	}

	if w.Header().Get("Cache-Control") != "max-age=3600" {
		t.Errorf("Cache-Control header = %q, want %q", w.Header().Get("Cache-Control"), "max-age=3600")
	}

	if w.Header().Get("X-Amz-Meta-Custom") != "custom-value" {
		t.Errorf("X-Amz-Meta-Custom header = %q, want %q", w.Header().Get("X-Amz-Meta-Custom"), "custom-value")
	}
}

func TestCachedObjectMeta_IsCacheable(t *testing.T) {
	tests := []struct {
		name         string
		etag         string
		cacheControl string
		contentLen   int64
		maxSize      int64
		expected     bool
	}{
		{
			name:         "cacheable object",
			etag:         `"abc"`,
			cacheControl: "max-age=3600",
			contentLen:   1024,
			maxSize:      1024 * 1024,
			expected:     true,
		},
		{
			name:         "no etag",
			etag:         "",
			cacheControl: "max-age=3600",
			contentLen:   1024,
			maxSize:      1024 * 1024,
			expected:     false,
		},
		{
			name:         "no-store",
			etag:         `"abc"`,
			cacheControl: "no-store",
			contentLen:   1024,
			maxSize:      1024 * 1024,
			expected:     false,
		},
		{
			name:         "private",
			etag:         `"abc"`,
			cacheControl: "private",
			contentLen:   1024,
			maxSize:      1024 * 1024,
			expected:     false,
		},
		{
			name:         "too large",
			etag:         `"abc"`,
			cacheControl: "max-age=3600",
			contentLen:   10 * 1024 * 1024,
			maxSize:      1024 * 1024,
			expected:     false,
		},
		{
			name:         "no cache control",
			etag:         `"abc"`,
			cacheControl: "",
			contentLen:   1024,
			maxSize:      1024 * 1024,
			expected:     true,
		},
		{
			name:         "public cache control",
			etag:         `"abc"`,
			cacheControl: "public, max-age=3600",
			contentLen:   1024,
			maxSize:      1024 * 1024,
			expected:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := &CachedObjectMeta{
				ETag:          tt.etag,
				CacheControl:  tt.cacheControl,
				ContentLength: tt.contentLen,
			}

			result := obj.IsCacheable(tt.maxSize)
			if result != tt.expected {
				t.Errorf("IsCacheable(%d) = %v, want %v", tt.maxSize, result, tt.expected)
			}
		})
	}
}

func TestMakeMetaKey(t *testing.T) {
	expected := "meta|my-bucket|path/to/object.txt"
	result := MakeMetaKey("my-bucket", "path/to/object.txt")
	if result != expected {
		t.Errorf("MakeMetaKey() = %q, want %q", result, expected)
	}
}

func TestMakeBodyKey(t *testing.T) {
	// No ETag falls back to the unversioned key.
	expected := "body|my-bucket|path/to/object.txt"
	result := MakeBodyKey("my-bucket", "path/to/object.txt", "")
	if result != expected {
		t.Errorf("MakeBodyKey() = %q, want %q", result, expected)
	}
	// With an ETag the body key is version-qualified (quotes/weak prefix stripped).
	if got := MakeBodyKey("my-bucket", "path/to/object.txt", `"abc123"`); got != "body|my-bucket|path/to/object.txt|abc123" {
		t.Errorf("MakeBodyKey(etag) = %q, want %q", got, "body|my-bucket|path/to/object.txt|abc123")
	}
}

func TestMetaFromHTTPHeaders_InvalidContentLength(t *testing.T) {
	headers := http.Header{
		"Content-Length": []string{"not-a-number"},
	}

	obj := MetaFromHTTPHeaders("bucket", "key", http.StatusOK, headers)

	// Should default to 0 for invalid content length
	if obj.ContentLength != 0 {
		t.Errorf("ContentLength = %d, want 0 for invalid value", obj.ContentLength)
	}
}

func TestMetaFromHTTPHeaders_InvalidLastModified(t *testing.T) {
	headers := http.Header{
		"Last-Modified": []string{"invalid-date"},
	}

	obj := MetaFromHTTPHeaders("bucket", "key", http.StatusOK, headers)

	// Should default to 0 for invalid date (Unix timestamp)
	if obj.LastModified != 0 {
		t.Errorf("LastModified = %d, want 0 for invalid value", obj.LastModified)
	}
}

func TestMetaFromHTTPHeaders_MultipleMetadataHeaders(t *testing.T) {
	headers := http.Header{
		"X-Amz-Meta-Key1": []string{"value1"},
		"X-Amz-Meta-Key2": []string{"value2"},
		"X-Amz-Meta-Key3": []string{"value3"},
	}

	obj := MetaFromHTTPHeaders("bucket", "key", http.StatusOK, headers)

	if len(obj.UserMetadata) != 3 {
		t.Errorf("UserMetadata count = %d, want 3", len(obj.UserMetadata))
	}
}

func TestMetaFromHTTPHeaders_MetadataLowercaseKeys(t *testing.T) {
	// Go's http.Header canonicalizes keys (e.g., x-amz-meta-foo -> X-Amz-Meta-Foo)
	// Our code should store metadata with lowercase keys per S3 convention
	headers := http.Header{
		"X-Amz-Meta-Custom-Key": []string{"custom-value"},
		"X-Amz-Meta-Another":    []string{"another-value"},
	}

	obj := MetaFromHTTPHeaders("bucket", "key", http.StatusOK, headers)

	// Keys should be stored lowercase
	if _, ok := obj.UserMetadata["x-amz-meta-custom-key"]; !ok {
		t.Error("UserMetadata should store key as lowercase 'x-amz-meta-custom-key'")
	}
	if _, ok := obj.UserMetadata["X-Amz-Meta-Custom-Key"]; ok {
		t.Error("UserMetadata should NOT store key as canonical 'X-Amz-Meta-Custom-Key'")
	}

	// Values should be preserved
	if obj.UserMetadata["x-amz-meta-custom-key"] != "custom-value" {
		t.Errorf("UserMetadata[x-amz-meta-custom-key] = %q, want %q",
			obj.UserMetadata["x-amz-meta-custom-key"], "custom-value")
	}
	if obj.UserMetadata["x-amz-meta-another"] != "another-value" {
		t.Errorf("UserMetadata[x-amz-meta-another] = %q, want %q",
			obj.UserMetadata["x-amz-meta-another"], "another-value")
	}
}

func TestCachedObjectMeta_WriteHeaders_MetadataLowercase(t *testing.T) {
	meta := &CachedObjectMeta{
		Key:         "test-key",
		Bucket:      "test-bucket",
		ContentType: "application/octet-stream",
		UserMetadata: map[string]string{
			"x-amz-meta-custom": "custom-value",
			"x-amz-meta-foo":    "foo-value",
		},
		StatusCode: http.StatusOK,
	}

	w := httptest.NewRecorder()
	meta.WriteHeaders(w)

	// Metadata headers should be written with lowercase keys
	// http.Header.Get is case-insensitive, so it will find headers regardless of case
	if w.Header().Get("x-amz-meta-custom") != "custom-value" {
		t.Errorf("x-amz-meta-custom = %q, want %q", w.Header().Get("x-amz-meta-custom"), "custom-value")
	}
	if w.Header().Get("x-amz-meta-foo") != "foo-value" {
		t.Errorf("x-amz-meta-foo = %q, want %q", w.Header().Get("x-amz-meta-foo"), "foo-value")
	}
}

func TestCachedObjectMeta_MatchesETag(t *testing.T) {
	tests := []struct {
		name     string
		objETag  string
		reqETag  string
		expected bool
	}{
		{
			name:     "exact match with quotes",
			objETag:  `"abc123"`,
			reqETag:  `"abc123"`,
			expected: true,
		},
		{
			name:     "match without quotes",
			objETag:  `"abc123"`,
			reqETag:  `abc123`,
			expected: true,
		},
		{
			name:     "wildcard",
			objETag:  `"abc123"`,
			reqETag:  `*`,
			expected: true,
		},
		{
			name:     "no match",
			objETag:  `"abc123"`,
			reqETag:  `"def456"`,
			expected: false,
		},
		{
			name:     "empty request etag",
			objETag:  `"abc123"`,
			reqETag:  ``,
			expected: false,
		},
		{
			name:     "empty object etag",
			objETag:  ``,
			reqETag:  `"abc123"`,
			expected: false,
		},
		{
			name:     "weak validator match",
			objETag:  `"abc123"`,
			reqETag:  `W/"abc123"`,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := &CachedObjectMeta{ETag: tt.objETag}
			result := obj.MatchesETag(tt.reqETag)
			if result != tt.expected {
				t.Errorf("MatchesETag(%q) = %v, want %v", tt.reqETag, result, tt.expected)
			}
		})
	}
}

func TestCachedObjectMeta_IsModifiedSince(t *testing.T) {
	objTime := time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC)
	obj := &CachedObjectMeta{LastModified: objTime.Unix()}

	tests := []struct {
		name     string
		since    time.Time
		expected bool
	}{
		{
			name:     "modified after",
			since:    time.Date(2023, 6, 15, 9, 0, 0, 0, time.UTC),
			expected: true,
		},
		{
			name:     "not modified before",
			since:    time.Date(2023, 6, 15, 11, 0, 0, 0, time.UTC),
			expected: false,
		},
		{
			name:     "same time",
			since:    objTime,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := obj.IsModifiedSince(tt.since)
			if result != tt.expected {
				t.Errorf("IsModifiedSince(%v) = %v, want %v", tt.since, result, tt.expected)
			}
		})
	}
}

func TestCachedObjectMeta_EncodeDecodeMeta(t *testing.T) {
	original := &CachedObjectMeta{
		Key:           "test-key",
		Bucket:        "test-bucket",
		ETag:          `"abc123"`,
		ContentType:   "application/json",
		ContentLength: 1024,
		LastModified:  time.Now().Unix(),
		CacheControl:  "max-age=3600",
		StorageClass:  "STANDARD",
		UserMetadata: map[string]string{
			"X-Amz-Meta-Custom": "value",
		},
		StatusCode: http.StatusOK,
	}

	// Encode
	data, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	// Decode
	decoded, err := DecodeMeta(data)
	if err != nil {
		t.Fatalf("DecodeMeta() error = %v", err)
	}

	// Compare
	if decoded.Key != original.Key {
		t.Errorf("Key = %q, want %q", decoded.Key, original.Key)
	}
	if decoded.Bucket != original.Bucket {
		t.Errorf("Bucket = %q, want %q", decoded.Bucket, original.Bucket)
	}
	if decoded.ETag != original.ETag {
		t.Errorf("ETag = %q, want %q", decoded.ETag, original.ETag)
	}
	if decoded.ContentType != original.ContentType {
		t.Errorf("ContentType = %q, want %q", decoded.ContentType, original.ContentType)
	}
	if decoded.ContentLength != original.ContentLength {
		t.Errorf("ContentLength = %d, want %d", decoded.ContentLength, original.ContentLength)
	}
	if decoded.StatusCode != original.StatusCode {
		t.Errorf("StatusCode = %d, want %d", decoded.StatusCode, original.StatusCode)
	}
}

func TestCachedObjectMeta_IsPublicRead(t *testing.T) {
	tests := []struct {
		name     string
		acl      string
		expected bool
	}{
		{name: "public-read", acl: "public-read", expected: true},
		{name: "public-read-write", acl: "public-read-write", expected: true},
		{name: "private", acl: "private", expected: false},
		{name: "empty", acl: "", expected: false},
		{name: "authenticated-read", acl: "authenticated-read", expected: false},
		{name: "bucket-owner-full-control", acl: "bucket-owner-full-control", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := &CachedObjectMeta{ACL: tt.acl}
			if got := meta.IsPublicRead(); got != tt.expected {
				t.Errorf("IsPublicRead() = %v, want %v for ACL %q", got, tt.expected, tt.acl)
			}
		})
	}
}

func TestMetaFromHTTPHeaders_ACL(t *testing.T) {
	headers := make(http.Header)
	headers.Set("X-Amz-Acl", "private")

	meta := MetaFromHTTPHeaders("bucket", "key", http.StatusOK, headers)
	if meta.ACL != "private" {
		t.Errorf("ACL = %q, want %q", meta.ACL, "private")
	}
}

func TestMetaFromHTTPHeaders_NoACL(t *testing.T) {
	headers := make(http.Header)

	meta := MetaFromHTTPHeaders("bucket", "key", http.StatusOK, headers)
	if meta.ACL != "" {
		t.Errorf("ACL = %q, want empty string", meta.ACL)
	}
}

func TestCachedObjectMeta_WriteHeaders_ACL(t *testing.T) {
	meta := &CachedObjectMeta{
		ACL:        "public-read",
		StatusCode: http.StatusOK,
	}

	w := httptest.NewRecorder()
	meta.WriteHeaders(w)

	if got := w.Header().Get("X-Amz-Acl"); got != "public-read" {
		t.Errorf("X-Amz-Acl header = %q, want %q", got, "public-read")
	}
}

func TestCachedObjectMeta_WriteHeaders_NoACL(t *testing.T) {
	meta := &CachedObjectMeta{
		StatusCode: http.StatusOK,
	}

	w := httptest.NewRecorder()
	meta.WriteHeaders(w)

	if got := w.Header().Get("X-Amz-Acl"); got != "" {
		t.Errorf("X-Amz-Acl header = %q, want empty", got)
	}
}

func TestCachedObjectMeta_ACL_EncodeDecode(t *testing.T) {
	original := &CachedObjectMeta{
		Key:    "key",
		Bucket: "bucket",
		ACL:    "public-read",
	}

	data, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	decoded, err := DecodeMeta(data)
	if err != nil {
		t.Fatalf("DecodeMeta() error = %v", err)
	}

	if decoded.ACL != original.ACL {
		t.Errorf("ACL = %q, want %q", decoded.ACL, original.ACL)
	}
}
