// Package cache provides cache storage and object types for TAG.
package cache

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	json "github.com/goccy/go-json"
)

const (
	// Cache key prefixes for separate metadata and body storage
	metaKeyPrefix  = "meta|"
	bodyKeyPrefix  = "body|"
	blockKeyPrefix = "blk|"
	tombKeyPrefix  = "tomb|"
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
	// BodyUpstream marks a tiered-mode entry whose body lives upstream (the large
	// tier): this metadata is authoritative for existence, HEAD, and conditionals,
	// while GET bodies are forwarded. False means the body is stored locally.
	BodyUpstream bool `json:"body_upstream,omitempty"`
	// BlockSize records the block granularity for a block-mode entry (RFC 0001). 0 means
	// the body is stored as a single whole blob (MakeBodyKey); >0 means the body is stored
	// as fixed-size blocks (MakeBlockKey) of this size. Captured at populate time so an
	// entry keeps its block layout even if the block_size config later changes.
	BlockSize int64 `json:"block_size,omitempty"`
	// BlocksComplete records that every block of a block-mode entry was present when this
	// meta was written (a full-stream split, or a promotion after a successful full
	// assembly). Full-object serves use it to skip the per-block existence probe pass and
	// stream optimistically — a block evicted since is recovered by an inline fetch. It is a
	// hint, not an invariant: false (including on entries written before the field existed)
	// only means the probe-first path is used.
	BlocksComplete bool `json:"blocks_complete,omitempty"`
	// BodyRef, when set, is the body key discriminator for this entry —
	// a per-write unique id minted by the LOCAL-STORE ENGINE, which streams
	// the body into the cache before its MD5 (the ETag) is known and so
	// cannot key the body by content. Null on every proxy-written entry:
	// proxy populates learn the ETag from upstream headers before the body
	// streams and keep content-addressed keys (and their convergent
	// double-write dedup). This is a WRITER-OWNERSHIP invariant, not a
	// migration state — readers resolve through BodyDiscriminator and never
	// care who wrote the entry.
	BodyRef string `json:"body_ref,omitempty"`
	// ContentLengthKnown records that ContentLength carries a real declared or
	// measured length — including a genuine 0 for an empty object. Encode sets
	// it automatically from ContentLength >= 0; DecodeMeta treats a row
	// WITHOUT it whose ContentLength is 0 as UNKNOWN (-1): rows persisted
	// before the -1 sentinel existed stored 0 for both "empty" and "unknown"
	// (no Content-Length on the upstream response), and re-reading that
	// ambiguous 0 as an affirmative zero length would serve non-empty cached
	// objects as empty. Those legacy rows revert to their pre-upgrade
	// omit-the-header behavior until TTL replaces them.
	ContentLengthKnown bool `json:"content_length_known,omitempty"`
	// CachedAt is the Unix time (seconds) this meta was built from a live upstream response.
	// Rewrites of an existing meta that do NOT consult upstream (the blocks-complete
	// promotion) use it to compute the entry's remaining TTL so they never extend its
	// lifetime — re-stamping a full TTL would reset the staleness clock and let the meta
	// outlive its blocks. 0 (entries written before the field existed) means the age is
	// unknown and no lifetime-sensitive rewrite is allowed.
	CachedAt int64 `json:"cached_at,omitempty"`
}

// MetaFromHTTPHeaders builds CachedObjectMeta from S3 response headers.
func MetaFromHTTPHeaders(bucket, key string, statusCode int, headers http.Header) *CachedObjectMeta {
	meta := &CachedObjectMeta{
		Key:                  key,
		Bucket:               bucket,
		StatusCode:           statusCode,
		CachedAt:             time.Now().Unix(),
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

	// Parse Content-Length. Absent means UNKNOWN (a chunked upstream
	// response), recorded as -1 — never 0, which is a real length (an empty
	// object) that WriteHeaders must advertise. Producers that know the exact
	// body length (the local-store engine, block populates) overwrite this
	// with the measured value.
	if cl := headers.Get("Content-Length"); cl != "" {
		meta.ContentLength, _ = strconv.ParseInt(cl, 10, 64)
	} else {
		meta.ContentLength = -1
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
	if m.ContentLength >= 0 {
		// Zero included: S3 sends Content-Length: 0 for an empty object, and a HEAD
		// without it makes clients read the length as unknown rather than zero.
		// Negative means genuinely unknown (a chunked upstream response,
		// recorded as -1 by MetaFromHTTPHeaders) and the header is omitted —
		// an affirmative 0 there would make clients read a non-empty object
		// as zero bytes.
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
		// Written via the map directly so the wire name stays lowercase, as S3
		// sends it. Header.Set would canonicalize to X-Amz-Meta-..., and botocore
		// derives its Metadata keys from the case-preserved wire name — canonical
		// casing turns the user's key "meta1" into "Meta1" on the client.
		lk := strings.ToLower(k)
		w.Header()[lk] = []string{v}
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
	// Don't cache objects without an ETag. Bodies are addressed by ETag so each
	// version gets an immutable key; without one, all versions would share a single
	// unversioned body key that a concurrent overwrite could clobber in place,
	// truncating an in-flight reader. Objects Tigris serves always carry an ETag, so
	// this only excludes rare ETag-less responses (e.g. some non-Tigris upstreams in
	// signing mode), which are forwarded uncached rather than cached unsafely.
	if m.ETag == "" {
		return false
	}

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

// MatchesETagHeader evaluates a full If-Match / If-None-Match header value
// against the object's ETag. RFC 7232 allows a comma-separated LIST of
// entity-tags ("e1", "e2"), and matching the header as one opaque tag makes a
// list that contains the live ETag never match — fatal on the authoritative
// paths (origin-less and tiered), where there is no upstream to answer
// correctly instead: legal conditional writes 412 and valid 304s are lost.
// Splitting on ',' is exact here because ETags are quoted strings whose only
// legal inner characters exclude ',' (RFC 7232 §2.3).
//
// strong selects the comparison function (RFC 7232 §2.3.2): If-Match REQUIRES
// strong comparison — a W/ weak validator never matches, so a conditional
// overwrite gated on one is refused — while If-None-Match uses weak
// comparison, where W/"x" and "x" match.
func (m *CachedObjectMeta) MatchesETagHeader(header string, strong bool) bool {
	if header == "" || m.ETag == "" {
		return false
	}
	if header == "*" {
		return true
	}
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" {
			return true
		}
		if candidate == "" {
			continue
		}
		if strong && (strings.HasPrefix(candidate, "W/") || strings.HasPrefix(m.ETag, "W/")) {
			continue
		}
		if normalizeETag(candidate) == normalizeETag(m.ETag) {
			return true
		}
	}
	return false
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

// normalizeETag strips the weak prefix and quotes from an ETag for comparison.
// This is a weak comparison (W/"abc" matches "abc"), used for conditional requests.
func normalizeETag(etag string) string {
	etag = strings.TrimPrefix(etag, "W/") // Remove weak validator prefix
	etag = strings.Trim(etag, "\"")
	return etag
}

// etagKeyComponent normalizes an ETag for use in a body cache key while PRESERVING
// the weak/strong distinction. A weak validator (W/"abc") only asserts semantic
// equivalence, so it can label different bytes than the strong validator "abc";
// collapsing them (as normalizeETag does) would map both to one body key, letting a
// later populate clobber the other version's bytes under an in-flight reader. Quotes
// are stripped (they are only delimiters) but the weak marker is kept.
func etagKeyComponent(etag string) string {
	weak := strings.HasPrefix(etag, "W/")
	etag = strings.Trim(strings.TrimPrefix(etag, "W/"), "\"")
	if weak {
		return "W/" + etag
	}
	return etag
}

// Encode serializes metadata to JSON bytes for cache storage. It stamps
// ContentLengthKnown from the length itself, so every producer — engine
// writes, markers, block finalizes — gets the disambiguation without
// having to remember the field exists.
func (m *CachedObjectMeta) Encode() ([]byte, error) {
	m.ContentLengthKnown = m.ContentLength >= 0
	return json.Marshal(m)
}

// DecodeMeta deserializes JSON bytes to CachedObjectMeta. A row without
// ContentLengthKnown whose ContentLength is 0 predates the -1 unknown
// sentinel, where 0 was ambiguous between "empty" and "unknown" — it decodes
// as unknown so a non-empty cached object is never served as empty.
func DecodeMeta(data []byte) (*CachedObjectMeta, error) {
	var meta CachedObjectMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	if !meta.ContentLengthKnown && meta.ContentLength == 0 {
		meta.ContentLength = -1
	}
	return &meta, nil
}

// MakeTombstoneKey creates the cache key for legacy invalidation tombstones
// (legacyCoordinator only; CAS mode writes none).
func MakeTombstoneKey(bucket, key string) string {
	return "tomb|" + bucket + "|" + key
}

// MakeMetaKey creates the cache key for object metadata.
func MakeMetaKey(bucket, key string) string {
	return metaKeyPrefix + bucket + "|" + key
}

// MakeBodyKey creates the cache key for an object body. Bodies are addressed by
// the object's ETag ("body|bucket|key|<etag>") so a served metadata entry always
// maps to the exact body version it describes: a concurrent overwrite writes a
// new meta plus a new body key and never clobbers the version an in-flight reader
// resolved. The ETag is normalized with etagKeyComponent, which keeps the
// weak/strong distinction so different validators never collide on one key. Objects
// with no ETag fall back to the unversioned key (no version discriminator exists).
// BodyDiscriminator returns the string this entry's body key is derived
// from: BodyRef when set (engine-written entries, whose bodies are keyed by
// a per-write id), else the ETag (proxy-written entries, content-addressed).
// Every body read, probe, or targeted delete for an entry must resolve
// through this — deriving from the ETag directly serves the wrong (absent)
// key for engine entries.
func (m *CachedObjectMeta) BodyDiscriminator() string {
	if m.BodyRef != "" {
		return m.BodyRef
	}
	return m.ETag
}

// NewBodyRef mints a per-write body key discriminator for entries whose
// ETag is not known until the body has fully streamed (the local-store
// engine's PUT). Uniqueness is what matters; the timestamp fallback keeps a
// usable id even if the entropy source fails.
func NewBodyRef() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "ref-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

func MakeBodyKey(bucket, key, etag string) string {
	if etag == "" {
		return bodyKeyPrefix + bucket + "|" + key
	}
	return bodyKeyPrefix + bucket + "|" + key + "|" + etagKeyComponent(etag)
}

// MakeBlockKey creates the cache key for a single block of a block-mode object body
// ("blk|bucket|key|<etag>|<blockSize>|<blockIdx>"). Blocks are ETag-scoped exactly like
// whole bodies (MakeBodyKey), so a concurrent overwrite writes new block keys under the new
// ETag and never clobbers the version an in-flight reader resolved; stale blocks age out by
// TTL. The blockSize is part of the key so blocks written under one block_size can never be
// resolved by a meta captured under a different block_size (e.g. after the config changes and
// the entry is re-established for an unchanged ETag) — that would read a block at the wrong
// offsets. blockIdx is the zero-based index of the block within the object. An ETag-less
// object is not block-cached (no version discriminator); callers must pass a non-empty etag.
func MakeBlockKey(bucket, key, etag string, blockSize, blockIdx int64) string {
	return blockKeyPrefix + bucket + "|" + key + "|" + etagKeyComponent(etag) + "|" +
		strconv.FormatInt(blockSize, 10) + "|" + strconv.FormatInt(blockIdx, 10)
}
