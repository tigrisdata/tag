package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
)

// localityChecker is an optional CacheClient capability: it reports whether a
// key is owned by the local node (a read is served from local storage) or must
// be pulled from a peer over gRPC. The embedded ocache cluster client
// implements it; clients that cannot tell simply do not, in which case serve
// locality is left unrecorded.
type localityChecker interface {
	IsLocal(key string) bool
}

// ErrNotFound indicates the key was not found in the cache.
var ErrNotFound = errors.New("not found in cache")

// ErrCacheDisabled indicates the cache is disabled.
var ErrCacheDisabled = errors.New("cache is disabled")

// Cache wraps ocache client for TAG.
type Cache struct {
	client       cacheclient.CacheClient
	defaultTTL   int64 // seconds
	tombstoneTTL int64 // seconds; must outlive the longest racing cache-populate
	enabled      bool
	closed       bool
}

// NewCacheWithClient creates a cache with an injected client.
// This allows tests to use an in-memory cache implementation like cacheclient.NewMemoryCache().
func NewCacheWithClient(client cacheclient.CacheClient, cfg *config.CacheConfig) *Cache {
	ttl := int64(config.DefaultCacheTTL.Seconds())
	enabled := true // Default to enabled
	var sizeThreshold int64
	if cfg != nil {
		if cfg.TTL > 0 {
			ttl = int64(cfg.TTL.Seconds())
		}
		enabled = cfg.IsEnabled()
		sizeThreshold = cfg.SizeThreshold
	}
	return &Cache{
		client:       client,
		defaultTTL:   ttl,
		tombstoneTTL: TombstoneTTLSeconds(sizeThreshold),
		enabled:      enabled,
	}
}

// NewDisabledCache creates a cache that is disabled.
// All operations return successfully with "not found" or nil results.
func NewDisabledCache() *Cache {
	return &Cache{
		enabled:      false,
		tombstoneTTL: MinTombstoneTTLSeconds,
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
//
// Returns wrote=true only when the metadata was actually written (the entry is now
// visible). It is false when the write was skipped without error — the object was not
// cacheable (no ETag) or a newer tombstone superseded it — so callers can distinguish a
// no-op from a real write (e.g. for metrics or a fallback).
func (c *Cache) PutWithMetaStreamTombstoneAware(
	ctx context.Context,
	bucket, key string,
	meta *CachedObjectMeta,
	body io.Reader,
	ttl int,
	writeStartTime int64, // Unix nano timestamp when write started
) (wrote bool, err error) {
	if !c.IsEnabled() {
		return false, nil
	}

	// Objects without an ETag are not cached: they would share a single
	// unversioned body key (MakeBodyKey(..., "")) that a concurrent overwrite could
	// clobber in place, truncating an in-flight reader — the hazard ETag-versioned
	// bodies exist to prevent. Callers gate on IsCacheable (which excludes empty
	// ETags) before building the stream; this is a backstop matching PutWithMeta.
	// Drain the body first so the producer side of the pipe never blocks.
	if meta.ETag == "" {
		_, _ = io.Copy(io.Discard, body)
		return false, nil
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
		return false, err
	}

	// Stream body to cache
	if err := c.client.PutStream(ctx, bodyKey, body, int64(ttl)); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache body put error")
		return false, err
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
		return false, nil
	}

	// Write metadata AFTER body (makes entry visible)
	if err := c.client.Put(ctx, metaKey, metaBytes, int64(ttl)); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta put error")
		// Same rationale as the tombstone branch: leave the versioned body to TTL
		// rather than risk truncating a concurrent same-version reader.
		return false, err
	}

	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Int("ttl", ttl).
		Int("meta_size", len(metaBytes)).
		Msg("Cached object with metadata (streamed, tombstone-aware)")
	return true, nil
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

	c.recordServeLocality(bodyKey)
	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Msg("Cache hit (body streamed)")
	return nil
}

// DeleteWithMeta removes both metadata and body from cache.
// Writes a tombstone first to prevent in-flight cache writes from completing.
//
// Both steps are attempted even if the first fails (best-effort invalidation), but a
// genuine backend failure of either is returned so callers don't report a successful
// invalidation while stale metadata is still readable. A not-found metadata delete is
// success — the entry is already gone.
func (c *Cache) DeleteWithMeta(ctx context.Context, bucket, key string) error {
	if !c.IsEnabled() {
		return nil
	}

	var errs []error

	// Write tombstone FIRST - prevents in-flight writes from completing. A failure
	// here leaves the invalidation incomplete (an in-flight populate could resurrect
	// the entry), so it is a real failure — but still continue to the meta delete.
	if err := c.WriteTombstone(ctx, bucket, key); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).
			Msg("Failed to write tombstone (continuing with delete)")
		errs = append(errs, fmt.Errorf("write tombstone: %w", err))
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
		errs = append(errs, fmt.Errorf("delete meta: %w", err))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
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

// recordServeLocality records whether a successful body read for bodyKey was
// satisfied from local storage or pulled from a peer over gRPC. It is a no-op
// when the underlying client cannot report key ownership (e.g. non-cluster
// clients or an ocache version without IsLocal), so the metric stays honest
// rather than reporting a guessed locality.
func (c *Cache) recordServeLocality(bodyKey string) {
	lc, ok := c.client.(localityChecker)
	if !ok {
		return
	}
	if lc.IsLocal(bodyKey) {
		metrics.RecordCacheServeLocality(metrics.LocalityLocal)
	} else {
		metrics.RecordCacheServeLocality(metrics.LocalityRemote)
	}
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
	return c.getRangeStreamByKey(ctx, MakeBodyKey(bucket, key, etag), bucket, key, start, end, w)
}

// GetBlockRangeStream streams an inclusive block-LOCAL byte range [start,end] of a single
// block of a block-mode object to w. blockIdx identifies the block; start and end are
// offsets WITHIN the block (0 = first byte of the block), not within the object. Pass the
// meta's ETag so the block resolves to the exact cached version. Returns ErrNotFound if the
// block is not in cache. See RFC 0001.
func (c *Cache) GetBlockRangeStream(ctx context.Context, bucket, key, etag string, blockSize, blockIdx, start, end int64, w io.Writer) error {
	if !c.IsEnabled() {
		return ErrCacheDisabled
	}
	return c.getRangeStreamByKey(ctx, MakeBlockKey(bucket, key, etag, blockSize, blockIdx), bucket, key, start, end, w)
}

// countingWriter wraps an io.Writer and counts the bytes written through it, so a range read
// can distinguish an absent key (embedded backend returns nil + zero bytes) from a real read.
type countingWriter struct {
	w       io.Writer
	written int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.written += int64(n)
	return n, err
}

// getRangeStreamByKey streams an inclusive byte range [start,end] of the blob at cacheKey to
// w, mapping ocache's not-found to ErrNotFound and handling ocache's read-byte-0 quirk.
// bucket/key are used for logging only.
func (c *Cache) getRangeStreamByKey(ctx context.Context, cacheKey, bucket, key string, start, end int64, w io.Writer) error {
	// Handle ocache quirk: reading byte 0 alone requires reading 2 bytes
	// and discarding the last byte
	if start == 0 && end == 0 {
		// Single byte at position 0 - need to read 2 bytes and discard last
		var buf bytes.Buffer
		err := c.client.GetRangeStream(ctx, cacheKey, 0, 1, &buf)
		if err != nil {
			if isNotFoundError(err) {
				log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache miss (range)")
				return ErrNotFound
			}
			return err
		}
		// Absent-key gap: the embedded (RocksDB) backend returns a nil error with ZERO bytes for
		// a missing key on a range read, rather than the not-found the whole-blob GetStream path
		// surfaces. A present block always has a byte at offset 0, so zero bytes here means the
		// key is absent — map it to ErrNotFound. Without this a presence probe (BlockExistsErr,
		// which reads [0,0]) treats a never-stored block as present, so fetchOneBlock skips the
		// fetch (the block is never stored) yet the block-mode meta is still written, and a later
		// serve streams an empty body. See RFC 0001.
		if buf.Len() == 0 {
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache miss (range)")
			return ErrNotFound
		}
		c.recordServeLocality(cacheKey)
		// Write only the first byte
		_, err = w.Write(buf.Bytes()[:1])
		return err
	}

	// ocache now uses inclusive end (same as HTTP Range semantics). Count bytes written so the
	// absent-key gap (embedded returns nil + zero bytes rather than not-found) is mapped to
	// ErrNotFound below: the requested range [start,end] is inclusive and non-empty here, and an
	// in-bounds read of a present block always yields at least one byte, so zero bytes with no
	// error means the key is absent — not a legitimately empty read.
	cw := &countingWriter{w: w}
	err := c.client.GetRangeStream(ctx, cacheKey, start, end, cw)
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
	if cw.written == 0 {
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache miss (range)")
		return ErrNotFound
	}

	c.recordServeLocality(cacheKey)
	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Int64("start", start).
		Int64("end", end).
		Int64("length", end-start+1).
		Msg("Cache hit (range)")
	return nil
}

// BlockExists reports whether the given block of a block-mode object is present in cache.
// It probes the block's first byte (quirk-safe, cheap), so a not-found or any read error
// returns false — the caller then (re)fetches the block. See RFC 0001.
func (c *Cache) BlockExists(ctx context.Context, bucket, key, etag string, blockSize, blockIdx int64) bool {
	present, _ := c.BlockExistsErr(ctx, bucket, key, etag, blockSize, blockIdx)
	return present
}

// BlockExistsErr reports whether a block is present, distinguishing genuine absence (present=false,
// err=nil) from a transient probe failure (present=false, err!=nil) such as a canceled context or a
// cluster gRPC error. Callers that make invalidation/amplification decisions from block presence
// must NOT treat a transient probe error as "absent" — doing so could, e.g., delete a still-valid
// entry when a network blip makes present blocks look missing. BlockExists (bool) collapses both to
// false and is only safe where a probe error is equivalent to absent (a plain cache miss).
func (c *Cache) BlockExistsErr(ctx context.Context, bucket, key, etag string, blockSize, blockIdx int64) (present bool, err error) {
	if !c.IsEnabled() || etag == "" {
		return false, nil
	}
	e := c.getRangeStreamByKey(ctx, MakeBlockKey(bucket, key, etag, blockSize, blockIdx), bucket, key, 0, 0, io.Discard)
	if e == nil {
		return true, nil
	}
	if errors.Is(e, ErrNotFound) {
		return false, nil // genuinely absent
	}
	return false, e // transient failure — not proof of absence
}

// PutBlockStream writes a single block of a block-mode object to cache. Blocks are
// ETag-scoped (MakeBlockKey) exactly like whole bodies, and — like bodies — are never
// deleted on invalidation; they age out by TTL. The block-mode meta (written tombstone-
// aware via PutMetaTombstoneAware) is the visibility gate, and reads only resolve blocks
// after a meta hit, so a block written for a since-deleted object is unreachable and
// harmless. An empty etag is not block-cached (no version discriminator). See RFC 0001.
func (c *Cache) PutBlockStream(ctx context.Context, bucket, key, etag string, blockSize, blockIdx int64, r io.Reader, ttl int) error {
	if !c.IsEnabled() || etag == "" {
		_, _ = io.Copy(io.Discard, r) // drain so a pipe producer never blocks
		return nil
	}
	if ttl == 0 {
		ttl = int(c.defaultTTL)
	}
	blockKey := MakeBlockKey(bucket, key, etag, blockSize, blockIdx)
	if err := c.client.PutStream(ctx, blockKey, r, int64(ttl)); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Int64("block", blockIdx).Msg("Cache block put error")
		return err
	}
	return nil
}

// PutMetaTombstoneAware writes only the object metadata (no body), gated on the tombstone
// like PutWithMetaStreamTombstoneAware: if an invalidation landed at or after writeStartTime
// the write is skipped and wrote=false is returned. It is the visibility gate for a block-
// mode entry — callers write the touched blocks first, then this meta last. See RFC 0001.
func (c *Cache) PutMetaTombstoneAware(
	ctx context.Context,
	bucket, key string,
	meta *CachedObjectMeta,
	ttl int,
	writeStartTime int64,
) (wrote bool, err error) {
	if !c.IsEnabled() {
		return false, nil
	}
	if meta.ETag == "" {
		return false, nil
	}
	if ttl == 0 {
		ttl = int(c.defaultTTL)
	}
	metaKey := MakeMetaKey(bucket, key)
	metaBytes, err := meta.Encode()
	if err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta encode error")
		return false, err
	}
	// Tombstone gate right before the (visibility-granting) meta write: if the key was
	// invalidated at or after our write start, skip so we don't resurrect stale metadata.
	tombTs := c.GetTombstoneTimestamp(ctx, bucket, key)
	if tombTs >= writeStartTime {
		log.Debug().Str("bucket", bucket).Str("key", key).
			Int64("tombstone_ts", tombTs).Int64("write_start", writeStartTime).
			Msg("Skipping block-mode meta write - tombstone detected")
		return false, nil
	}
	if err := c.client.Put(ctx, metaKey, metaBytes, int64(ttl)); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta put error")
		return false, err
	}
	return true, nil
}

// ============================================================================
// Tombstone methods for cache invalidation
// ============================================================================

const (
	// MinTombstoneTTLSeconds is the floor for how long an invalidation tombstone
	// lives (10 minutes). It comfortably exceeds a small object's populate.
	MinTombstoneTTLSeconds = 600

	// The constants below mirror the proxy's cache-populate timeouts so the TTL can
	// model the same window. They are kept honest by
	// TestTombstoneTTLCoversPopulateWindow in the proxy package, which sweeps
	// thresholds and fails if either side drifts.

	// tombstoneWriteThroughput mirrors the conservative streaming-write throughput
	// the proxy assumes when sizing a cache-populate timeout (proxy:
	// minCacheWriteThroughput, 5 MB/s).
	tombstoneWriteThroughput = 5 * 1024 * 1024

	// tombstoneMinWriteSeconds mirrors the proxy's floor on that write timeout
	// (proxy: cacheWriteTimeout, 60s).
	tombstoneMinWriteSeconds = 60

	// tombstoneFetchSeconds mirrors the upstream fetch that precedes the write
	// (proxy: backgroundFetchTimeout, 5m).
	tombstoneFetchSeconds = 300

	// tombstoneMarginSeconds is slack on top of the modeled window.
	tombstoneMarginSeconds = 300
)

// TombstoneTTLSeconds returns how long an invalidation tombstone must live for a
// given cache size threshold.
//
// A tombstone's whole job is to outlive any cache-populate that could race it: a
// populate is only compared against the tombstone immediately before its metadata
// write, so if the tombstone expires first the guard reads zero and the racing
// (stale) write proceeds — silently resurrecting invalidated content.
//
// The longest populate is bounded by the upstream fetch PLUS the streaming write of
// the largest cacheable object, so this is derived from sizeThreshold rather than
// fixed: raising cache.size_threshold must not silently reintroduce that race.
//
// The derivation mirrors that window's shape — write + fetch + margin — rather than
// scaling the write time by a factor. That matters: a multiplicative approximation
// (e.g. 2 x write) grows on a different curve than the additive window and crosses
// it, collapsing the margin to zero where write time approaches the fetch bound
// (~1.5 GiB) and silently reopening the race. Modeling the same shape keeps a
// constant margin at every threshold.
func TombstoneTTLSeconds(sizeThreshold int64) int64 {
	write := int64(tombstoneMinWriteSeconds)
	if sizeThreshold > 0 {
		if w := sizeThreshold / tombstoneWriteThroughput; w > write {
			write = w
		}
	}
	return max(write+tombstoneFetchSeconds+tombstoneMarginSeconds, MinTombstoneTTLSeconds)
}

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
	return c.client.Put(ctx, tombKey, data, c.tombstoneTTL)
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
