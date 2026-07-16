package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

// ErrNotFound indicates the key was not found in the cache.
var ErrNotFound = errors.New("not found in cache")

// ErrCacheDisabled indicates the cache is disabled.
var ErrCacheDisabled = errors.New("cache is disabled")

// Cache wraps ocache client for TAG.
type Cache struct {
	client     cacheclient.CacheClient
	defaultTTL int64 // seconds
	enabled    bool
	closed     bool
}

// NewCacheWithClient creates a cache with an injected client.
// This allows tests to use an in-memory cache implementation like cacheclient.NewMemoryCache().
func NewCacheWithClient(client cacheclient.CacheClient, cfg *config.CacheConfig) *Cache {
	ttl := int64(config.DefaultCacheTTL.Seconds())
	enabled := true // Default to enabled
	if cfg != nil {
		if cfg.TTL > 0 {
			ttl = int64(cfg.TTL.Seconds())
		}
		enabled = cfg.IsEnabled()
	}
	return &Cache{
		client:     client,
		defaultTTL: ttl,
		enabled:    enabled,
	}
}

// NewDisabledCache creates a cache that is disabled.
// All operations return successfully with "not found" or nil results.
func NewDisabledCache() *Cache {
	return &Cache{
		enabled: false,
	}
}

// IsEnabled returns true if the cache is enabled.
func (c *Cache) IsEnabled() bool {
	return c.enabled && !c.closed
}

// ============================================================================
// Two-Key Pattern: Metadata and Body stored separately
// ============================================================================

// PutWithMeta stores object metadata and body in separate cache entries.
// This follows the gateway's LiteCache pattern for proper S3 caching.
// IMPORTANT: Body is written BEFORE metadata to ensure metadata presence
// guarantees body availability. This prevents race conditions where a reader
// finds metadata but body hasn't been written yet.
// Body lifecycle note: bodies are addressed by ETag and are never deleted
// synchronously (not on overwrite, not on invalidation). Each version is an
// immutable entry that ages out via TTL, so a reader that resolved a given
// metadata version always finds its exact body — no delete-during-read can
// truncate an in-flight response. Invalidation removes only the metadata (plus a
// tombstone), which is enough to make subsequent reads miss and refetch.
func (c *Cache) PutWithMeta(ctx context.Context, bucket, key string, meta *CachedObjectMeta, body []byte, ttl int) error {
	if !c.IsEnabled() {
		return nil
	}

	// Objects without an ETag are not cached: bodies are addressed by ETag, and an
	// ETag-less object would share a single unversioned body key that a concurrent
	// overwrite could clobber in place. Callers gate on IsCacheable (which also
	// excludes empty ETags); this is a defensive backstop for direct callers.
	if meta.ETag == "" {
		return nil
	}

	if ttl == 0 {
		ttl = int(c.defaultTTL)
	}

	metaKey := MakeMetaKey(bucket, key)
	bodyKey := MakeBodyKey(bucket, key, meta.ETag)

	// Encode metadata as JSON
	metaBytes, err := meta.Encode()
	if err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta encode error")
		return err
	}

	// Store body FIRST (can be empty for zero-byte objects)
	// This ensures metadata presence guarantees body availability
	if err := c.client.Put(ctx, bodyKey, body, int64(ttl)); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache body put error")
		return err
	}

	// Store metadata AFTER body is complete
	if err := c.client.Put(ctx, metaKey, metaBytes, int64(ttl)); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta put error")
		// Leave the versioned body to age out via TTL rather than deleting it
		// synchronously: a concurrent populate of the same ETag could have a reader
		// streaming this exact body key, and deleting it would truncate that reader.
		// Without a visible meta entry the orphaned body is unreachable until it expires.
		return err
	}

	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Int("ttl", ttl).
		Int("meta_size", len(metaBytes)).
		Int("body_size", len(body)).
		Msg("Cached object with metadata")
	return nil
}

// PutWithMetaStreamTombstoneAware is like PutWithMetaStream but checks for
// tombstones after body streaming and before writing metadata. If a tombstone
// exists that's newer than writeStartTime, the metadata write is skipped and
// the orphaned body is cleaned up. Checking after body stream (rather than
// before) closes the TOCTOU window where an invalidation during streaming
// could be missed, causing resurrected metadata without a body.
func (c *Cache) PutWithMetaStreamTombstoneAware(
	ctx context.Context,
	bucket, key string,
	meta *CachedObjectMeta,
	body io.Reader,
	ttl int,
	writeStartTime int64, // Unix nano timestamp when write started
) error {
	if !c.IsEnabled() {
		return nil
	}

	if ttl == 0 {
		ttl = int(c.defaultTTL)
	}

	metaKey := MakeMetaKey(bucket, key)
	bodyKey := MakeBodyKey(bucket, key, meta.ETag)

	// Encode metadata first (fail fast if encoding fails)
	metaBytes, err := meta.Encode()
	if err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta encode error")
		return err
	}

	// Stream body to cache
	if err := c.client.PutStream(ctx, bodyKey, body, int64(ttl)); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache body put error")
		return err
	}

	// Check tombstone AFTER body stream, right before meta write.
	// This closes the TOCTOU window: if the key was invalidated during body streaming
	// (e.g., a PUT/DELETE arrived while we were writing), we skip the meta write.
	// Without this, a slow body stream could allow meta to be written after a
	// concurrent invalidation deletes meta+body, resurrecting stale metadata.
	tombTs := c.GetTombstoneTimestamp(ctx, bucket, key)
	if tombTs >= writeStartTime {
		log.Debug().Str("bucket", bucket).Str("key", key).
			Int64("tombstone_ts", tombTs).
			Int64("write_start", writeStartTime).
			Msg("Skipping meta write - tombstone detected after body stream")
		// The just-written versioned body is left to age out via TTL rather than
		// deleted synchronously: a concurrent populate of the same ETag could have
		// a reader streaming this exact body key, and deleting it would truncate
		// that reader. Without a visible meta entry the orphaned body is unreachable
		// and harmless until it expires.
		return nil
	}

	// Write metadata AFTER body (makes entry visible)
	if err := c.client.Put(ctx, metaKey, metaBytes, int64(ttl)); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta put error")
		// Same rationale as the tombstone branch: leave the versioned body to TTL
		// rather than risk truncating a concurrent same-version reader.
		return err
	}

	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Int("ttl", ttl).
		Int("meta_size", len(metaBytes)).
		Msg("Cached object with metadata (streamed, tombstone-aware)")
	return nil
}

// GetMeta retrieves only object metadata from cache (no body).
// Use this for HEAD requests to avoid fetching the body.
func (c *Cache) GetMeta(ctx context.Context, bucket, key string) (*CachedObjectMeta, bool, error) {
	if !c.IsEnabled() {
		return nil, false, nil
	}

	metaKey := MakeMetaKey(bucket, key)

	// Get metadata
	metaBytes, err := c.client.Get(ctx, metaKey)
	if err != nil {
		if isNotFoundError(err) {
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache miss (meta only)")
			return nil, false, nil
		}
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta get error")
		return nil, false, err
	}

	if metaBytes == nil {
		return nil, false, nil
	}

	// Decode metadata
	meta, err := DecodeMeta(metaBytes)
	if err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta decode error")
		return nil, false, err
	}

	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Int("meta_size", len(metaBytes)).
		Msg("Cache hit (meta only)")
	return meta, true, nil
}

// GetBodyStream streams the cached object body directly to the provided writer.
// This avoids buffering the entire object in memory, which is critical for large objects.
// Use this after GetMeta(), passing the meta's ETag so the body read resolves to
// the exact version the metadata describes. Returns ErrNotFound if the body for
// that version is not in cache.
func (c *Cache) GetBodyStream(ctx context.Context, bucket, key, etag string, w io.Writer) error {
	if !c.IsEnabled() {
		return ErrCacheDisabled
	}

	bodyKey := MakeBodyKey(bucket, key, etag)

	// Stream body directly to writer - no intermediate buffer
	err := c.client.GetStream(ctx, bodyKey, w)
	if err != nil {
		if isNotFoundError(err) {
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache miss (body stream)")
			return ErrNotFound
		}
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache body stream error")
		return err
	}

	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Msg("Cache hit (body streamed)")
	return nil
}

// DeleteWithMeta removes both metadata and body from cache.
// Writes a tombstone first to prevent in-flight cache writes from completing.
func (c *Cache) DeleteWithMeta(ctx context.Context, bucket, key string) error {
	if !c.IsEnabled() {
		return nil
	}

	// Write tombstone FIRST - prevents in-flight writes from completing
	if err := c.WriteTombstone(ctx, bucket, key); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).
			Msg("Failed to write tombstone (continuing with delete)")
	}

	metaKey := MakeMetaKey(bucket, key)

	// Delete only the metadata. That is sufficient to make subsequent reads miss
	// (a read resolves the body from meta.ETag, so with meta gone there is no body
	// lookup), and the tombstone above blocks any in-flight repopulation. The
	// versioned body is intentionally left to age out via TTL rather than deleted
	// synchronously — deleting it could truncate an in-flight reader still
	// streaming that exact version.
	if err := c.client.Delete(ctx, metaKey); err != nil && !isNotFoundError(err) {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta delete error")
	}

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("Invalidated cache metadata (body ages out via TTL)")
	return nil
}

// Delete removes an object from the cache.
func (c *Cache) Delete(ctx context.Context, bucket, key string) error {
	if !c.IsEnabled() {
		return nil
	}

	return c.DeleteWithMeta(ctx, bucket, key)
}

// ============================================================================
// Range request support
// ============================================================================

// GetRangeStream retrieves a byte range from the cached object body.
// Uses ocache's GetRangeStream for efficient partial reads from disk.
// start and end are inclusive byte positions (HTTP Range semantics).
// Pass the meta's ETag so the range resolves to the exact cached version.
// Returns ErrNotFound if the object is not in cache.
func (c *Cache) GetRangeStream(ctx context.Context, bucket, key, etag string, start, end int64, w io.Writer) error {
	if !c.IsEnabled() {
		return ErrCacheDisabled
	}

	bodyKey := MakeBodyKey(bucket, key, etag)

	// Handle ocache quirk: reading byte 0 alone requires reading 2 bytes
	// and discarding the last byte
	if start == 0 && end == 0 {
		// Single byte at position 0 - need to read 2 bytes and discard last
		var buf bytes.Buffer
		err := c.client.GetRangeStream(ctx, bodyKey, 0, 1, &buf)
		if err != nil {
			if isNotFoundError(err) {
				log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache miss (range)")
				return ErrNotFound
			}
			return err
		}
		// Write only the first byte
		if buf.Len() > 0 {
			_, err = w.Write(buf.Bytes()[:1])
		}
		return err
	}

	// ocache now uses inclusive end (same as HTTP Range semantics)
	err := c.client.GetRangeStream(ctx, bodyKey, start, end, w)
	if err != nil {
		if isNotFoundError(err) {
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache miss (range)")
			return ErrNotFound
		}
		log.Debug().Err(err).
			Str("bucket", bucket).
			Str("key", key).
			Int64("start", start).
			Int64("end", end).
			Msg("Cache range get error")
		return err
	}

	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Int64("start", start).
		Int64("end", end).
		Int64("length", end-start+1).
		Msg("Cache hit (range)")
	return nil
}

// ============================================================================
// Tombstone methods for cache invalidation
// ============================================================================

// tombstoneTTL is the TTL for tombstone entries (60 seconds).
// Tombstones only need to outlive in-flight cache writes.
const tombstoneTTL = 60

// WriteTombstone writes an invalidation marker for a key.
// The value is the timestamp as 8 bytes (int64 big-endian).
// This is used to prevent stale cache writes from completing after invalidation.
func (c *Cache) WriteTombstone(ctx context.Context, bucket, key string) error {
	if !c.IsEnabled() {
		return nil
	}
	tombKey := MakeTombstoneKey(bucket, key)
	ts := time.Now().UnixNano()
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(ts))
	return c.client.Put(ctx, tombKey, data, tombstoneTTL)
}

// GetTombstoneTimestamp retrieves the tombstone timestamp for a key.
// Returns 0 if no tombstone exists.
func (c *Cache) GetTombstoneTimestamp(ctx context.Context, bucket, key string) int64 {
	if !c.IsEnabled() {
		return 0
	}
	tombKey := MakeTombstoneKey(bucket, key)
	data, err := c.client.Get(ctx, tombKey)
	if err != nil || len(data) != 8 {
		return 0 // No tombstone or invalid data
	}
	return int64(binary.BigEndian.Uint64(data))
}

// ============================================================================
// Utility methods
// ============================================================================

// Has checks if an object exists in the cache.
func (c *Cache) Has(ctx context.Context, bucket, key string) bool {
	if !c.IsEnabled() {
		return false
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Check for metadata key
	metaKey := MakeMetaKey(bucket, key)
	metaBytes, err := c.client.Get(ctx, metaKey)
	return err == nil && metaBytes != nil
}

// ListKeys returns all keys matching the prefix.
func (c *Cache) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	if !c.IsEnabled() {
		return nil, ErrCacheDisabled
	}

	return c.client.List(ctx, prefix)
}

// Close shuts down the cache client.
func (c *Cache) Close() error {
	if c.closed || !c.enabled {
		return nil
	}

	log.Info().Msg("Closing cache client")
	c.closed = true
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// IsClosed returns true if the cache is closed.
func (c *Cache) IsClosed() bool {
	return c.closed
}

// GetConnectedNodes returns the list of ocache nodes this client is connected to.
func (c *Cache) GetConnectedNodes() []string {
	if !c.IsEnabled() || c.client == nil {
		return nil
	}
	return c.client.GetConnectedNodes()
}

// GetMode returns the connection mode (cluster or simple).
func (c *Cache) GetMode() string {
	if !c.IsEnabled() || c.client == nil {
		return "disabled"
	}
	return string(c.client.GetMode())
}

// isNotFoundError checks if the error indicates a cache miss.
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errStr == "key not found" ||
		errStr == "not found" ||
		strings.Contains(errStr, "NotFound") ||
		strings.Contains(errStr, "not found")
}
