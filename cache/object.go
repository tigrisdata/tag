// Package cache provides cache storage and object types for TAG.
package cache

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// Cache key prefixes for separate metadata and body storage
	metaKeyPrefix = "meta|"
	bodyKeyPrefix = "body|"
)

// CachedObjectMeta represents cached S3 object metadata.
// This is stored separately from the body to support:
// - HEAD requests from cache (metadata only)
// - Conditional requests (If-None-Match, If-Modified-Since)
// - Proper response headers on cache hits
type CachedObjectMeta struct {
	Key           string            `json:"key"`
	Bucket        string            `json:"bucket"`
	ETag          string            `json:"etag,omitempty"`
	ContentType   string            `json:"content_type,omitempty"`
	ContentLength int64             `json:"content_length"`
	LastModified  int64             `json:"last_modified"` // Unix timestamp (seconds)
	CacheControl  string            `json:"cache_control,omitempty"`
	StorageClass  string            `json:"storage_class,omitempty"`
	UserMetadata  map[string]string `json:"user_metadata,omitempty"` // x-amz-meta-*
	StatusCode    int               `json:"status_code"`             // Original HTTP status (200, etc.)
}

// MetaFromHTTPHeaders builds CachedObjectMeta from S3 response headers.
func MetaFromHTTPHeaders(bucket, key string, statusCode int, headers http.Header) *CachedObjectMeta {
	meta := &CachedObjectMeta{
		Key:          key,
		Bucket:       bucket,
		StatusCode:   statusCode,
		ETag:         headers.Get("ETag"),
		ContentType:  headers.Get("Content-Type"),
		CacheControl: headers.Get("Cache-Control"),
		StorageClass: headers.Get("x-amz-storage-class"),
		UserMetadata: make(map[string]string),
	}

	// Parse Content-Length
	if cl := headers.Get("Content-Length"); cl != "" {
		meta.ContentLength, _ = strconv.ParseInt(cl, 10, 64)
	}

	// Parse Last-Modified to Unix timestamp
	if lm := headers.Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			meta.LastModified = t.Unix()
		}
	}

	// Extract user metadata (x-amz-meta-*)
	for k, v := range headers {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-meta-") && len(v) > 0 {
			meta.UserMetadata[k] = v[0]
		}
	}

	return meta
}

// WriteHeaders writes object metadata to response headers.
func (m *CachedObjectMeta) WriteHeaders(w http.ResponseWriter) {
	if m.ETag != "" {
		w.Header().Set("ETag", m.ETag)
	}
	if m.ContentType != "" {
		w.Header().Set("Content-Type", m.ContentType)
	}
	if m.ContentLength > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(m.ContentLength, 10))
	}
	if m.LastModified > 0 {
		t := time.Unix(m.LastModified, 0).UTC()
		w.Header().Set("Last-Modified", t.Format(http.TimeFormat))
	}
	if m.CacheControl != "" {
		w.Header().Set("Cache-Control", m.CacheControl)
	}
	if m.StorageClass != "" {
		w.Header().Set("x-amz-storage-class", m.StorageClass)
	}
	for k, v := range m.UserMetadata {
		w.Header().Set(k, v)
	}
}

// IsCacheable returns true if the object should be cached based on headers.
func (m *CachedObjectMeta) IsCacheable(maxSize int64) bool {
	// Don't cache if Cache-Control says not to
	cc := strings.ToLower(m.CacheControl)
	if strings.Contains(cc, "no-store") || strings.Contains(cc, "private") {
		return false
	}

	// Don't cache objects larger than threshold
	if m.ContentLength > maxSize {
		return false
	}

	return true
}

// MatchesETag returns true if the given etag matches this object's ETag.
// Used for If-None-Match conditional requests.
func (m *CachedObjectMeta) MatchesETag(etag string) bool {
	if etag == "" || m.ETag == "" {
		return false
	}
	// Handle "*" wildcard
	if etag == "*" {
		return true
	}
	// Compare ETags (strip quotes if present for comparison)
	return normalizeETag(etag) == normalizeETag(m.ETag)
}

// IsModifiedSince returns true if the object was modified after the given time.
// Used for If-Modified-Since conditional requests.
func (m *CachedObjectMeta) IsModifiedSince(since time.Time) bool {
	if m.LastModified == 0 {
		return true // Unknown last modified, consider modified
	}
	objTime := time.Unix(m.LastModified, 0)
	// HTTP dates are only accurate to the second
	return objTime.After(since.Truncate(time.Second))
}

// normalizeETag strips quotes from ETag for comparison.
func normalizeETag(etag string) string {
	etag = strings.TrimPrefix(etag, "W/") // Remove weak validator prefix
	etag = strings.Trim(etag, "\"")
	return etag
}

// Encode serializes metadata to JSON bytes for cache storage.
func (m *CachedObjectMeta) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeMeta deserializes JSON bytes to CachedObjectMeta.
func DecodeMeta(data []byte) (*CachedObjectMeta, error) {
	var meta CachedObjectMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// MakeMetaKey creates the cache key for object metadata.
func MakeMetaKey(bucket, key string) string {
	return metaKeyPrefix + bucket + "|" + key
}

// MakeBodyKey creates the cache key for object body.
func MakeBodyKey(bucket, key string) string {
	return bodyKeyPrefix + bucket + "|" + key
}
