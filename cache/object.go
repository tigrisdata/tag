// Package cache provides cache storage and object types for TAG.
package cache

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	json "github.com/goccy/go-json"
)

const (
	// Cache key prefixes for separate metadata and body storage
	metaKeyPrefix = "meta|"
	bodyKeyPrefix = "body|"
	tombKeyPrefix = "tomb|"
)

// CachedObjectMeta represents cached S3 object metadata.
// This is stored separately from the body to support:
// - HEAD requests from cache (metadata only)
// - Conditional requests (If-None-Match, If-Modified-Since)
// - Proper response headers on cache hits
type CachedObjectMeta struct {
	Key                  string            `json:"key"`
	Bucket               string            `json:"bucket"`
	ETag                 string            `json:"etag,omitempty"`
	ContentType          string            `json:"content_type,omitempty"`
	ContentLength        int64             `json:"content_length"`
	LastModified         int64             `json:"last_modified"` // Unix timestamp (seconds)
	CacheControl         string            `json:"cache_control,omitempty"`
	StorageClass         string            `json:"storage_class,omitempty"`
	ACL                  string            `json:"acl,omitempty"`                    // X-Amz-Acl canned ACL (e.g., "public-read")
	ContentEncoding      string            `json:"content_encoding,omitempty"`       // Content-Encoding (e.g., "gzip")
	ContentDisposition   string            `json:"content_disposition,omitempty"`    // Content-Disposition (e.g., "attachment; filename=...")
	ContentLanguage      string            `json:"content_language,omitempty"`       // Content-Language
	Expires              string            `json:"expires,omitempty"`                // Expires header (raw HTTP-date string)
	ServerSideEncryption string            `json:"server_side_encryption,omitempty"` // x-amz-server-side-encryption
	VersionID            string            `json:"version_id,omitempty"`             // x-amz-version-id
	PartsCount           string            `json:"parts_count,omitempty"`            // x-amz-mp-parts-count
	UserMetadata         map[string]string `json:"user_metadata,omitempty"`          // x-amz-meta-*
	StatusCode           int               `json:"status_code"`                      // Original HTTP status (200, etc.)
}

// MetaFromHTTPHeaders builds CachedObjectMeta from S3 response headers.
func MetaFromHTTPHeaders(bucket, key string, statusCode int, headers http.Header) *CachedObjectMeta {
	meta := &CachedObjectMeta{
		Key:                  key,
		Bucket:               bucket,
		StatusCode:           statusCode,
		ETag:                 headers.Get("ETag"),
		ContentType:          headers.Get("Content-Type"),
		CacheControl:         headers.Get("Cache-Control"),
		StorageClass:         headers.Get("x-amz-storage-class"),
		ACL:                  headers.Get("X-Amz-Acl"),
		ContentEncoding:      headers.Get("Content-Encoding"),
		ContentDisposition:   headers.Get("Content-Disposition"),
		ContentLanguage:      headers.Get("Content-Language"),
		Expires:              headers.Get("Expires"),
		ServerSideEncryption: headers.Get("x-amz-server-side-encryption"),
		VersionID:            headers.Get("x-amz-version-id"),
		PartsCount:           headers.Get("x-amz-mp-parts-count"),
		UserMetadata:         make(map[string]string),
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
	// Store with lowercase key to match S3 convention
	for k, v := range headers {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-meta-") && len(v) > 0 {
			meta.UserMetadata[lk] = v[0]
		}
	}

	return meta
}

// WriteHeaderOption customizes headers written by WriteHeaders.
type WriteHeaderOption func(http.Header)

// WithRangeHeaders overrides Content-Length for the range size and adds
// Content-Range and Accept-Ranges headers for 206 Partial Content responses.
func WithRangeHeaders(start, end, totalSize int64) WriteHeaderOption {
	return func(h http.Header) {
		contentLength := end - start + 1
		h.Set("Content-Length", strconv.FormatInt(contentLength, 10))
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
		h.Set("Accept-Ranges", "bytes")
	}
}

// WriteHeaders writes object metadata to response headers.
// Options are applied after standard headers, allowing overrides (e.g., WithRangeHeaders).
func (m *CachedObjectMeta) WriteHeaders(w http.ResponseWriter, opts ...WriteHeaderOption) {
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
	if m.ACL != "" {
		w.Header().Set("X-Amz-Acl", m.ACL)
	}
	if m.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", m.ContentEncoding)
	}
	if m.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", m.ContentDisposition)
	}
	if m.ContentLanguage != "" {
		w.Header().Set("Content-Language", m.ContentLanguage)
	}
	if m.Expires != "" {
		w.Header().Set("Expires", m.Expires)
	}
	if m.ServerSideEncryption != "" {
		w.Header().Set("x-amz-server-side-encryption", m.ServerSideEncryption)
	}
	if m.VersionID != "" {
		w.Header().Set("x-amz-version-id", m.VersionID)
	}
	if m.PartsCount != "" {
		w.Header().Set("x-amz-mp-parts-count", m.PartsCount)
	}
	// Write user metadata with lowercase keys per S3 convention
	for k, v := range m.UserMetadata {
		lk := strings.ToLower(k)
		w.Header().Set(lk, v)
	}

	// Apply options (may override headers like Content-Length for range responses)
	for _, opt := range opts {
		opt(w.Header())
	}
}

// IsPublicRead returns true if the object's ACL allows anonymous read access.
func (m *CachedObjectMeta) IsPublicRead() bool {
	return m.ACL == "public-read" || m.ACL == "public-read-write"
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

// MakeBodyKey creates the cache key for an object body. Bodies are addressed by
// the object's ETag ("body|bucket|key|<etag>") so a served metadata entry always
// maps to the exact body version it describes: a concurrent overwrite writes a
// new meta plus a new body key and never clobbers the version an in-flight reader
// resolved. Objects with no ETag fall back to the unversioned key (there is no
// version discriminator available for them).
func MakeBodyKey(bucket, key, etag string) string {
	if etag == "" {
		return bodyKeyPrefix + bucket + "|" + key
	}
	return bodyKeyPrefix + bucket + "|" + key + "|" + normalizeETag(etag)
}

// bodyKeyCandidates returns the body keys to try when reading, in priority order.
// For a versioned (non-empty ETag) object it appends the legacy unversioned key as
// a fallback: bodies written by a pre-versioning build are still on disk at the
// unversioned key after an upgrade, while their metadata already carries an ETag.
// Without this fallback, the first Range GET for such an object after upgrade would
// resolve the versioned key, miss, and stream a truncated body under a committed 206
// for the entire cache-migration window.
func bodyKeyCandidates(bucket, key, etag string) []string {
	versioned := MakeBodyKey(bucket, key, etag)
	if etag == "" {
		return []string{versioned}
	}
	return []string{versioned, MakeBodyKey(bucket, key, "")}
}

// MakeTombstoneKey creates the cache key for invalidation tombstones.
func MakeTombstoneKey(bucket, key string) string {
	return tombKeyPrefix + bucket + "|" + key
}
