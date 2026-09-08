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
	"github.com/tigrisdata/ocache/coordinator"
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

// blockBytePutter is an optional CacheClient capability for a fully staged block.
// A client returns handled=false when its deployment-specific byte path cannot
// safely accept the request; Cache then uses the normal CacheClient path.
type blockBytePutter interface {
	PutBlockBytes(ctx context.Context, key string, data []byte, ttlSeconds int64) (handled bool, err error)
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

// VersionAny requests a version-BUMPING unconditional meta write: last-write-
// wins like a plain Put, but implemented as a CAS loop so the row stays
// version-stamped. This upholds the package invariant that NO meta key is ever
// written with a plain Put — a plain Put resets the row to the storage layer's
// legacy version, which blinds DeleteIfETag's version guard for every
// subsequent conditional operation on the key.
const VersionAny = ^uint64(0)

// putMetaVersioned writes metaBytes to metaKey under a version precondition.
// expected == VersionAny preserves plain-Put semantics (unconditional,
// last-write-wins) via a bounded read-CAS loop; any other value — including 0,
// put-if-absent — is a strict precondition, and a lost race returns
// (false, nil): the newer entry wins.
func (c *Cache) putMetaVersioned(ctx context.Context, bucket, key, metaKey string, metaBytes []byte, ttl int64, expected uint64) (bool, error) {
	if expected != VersionAny {
		if _, err := c.client.PutIfVersion(ctx, metaKey, metaBytes, ttl, expected); err != nil {
			if _, mismatch := cacheclient.IsVersionMismatch(err); mismatch {
				// Debug + metric, per the repo's log policy: the counter is the
				// rollout-visibility signal — a lost precondition replaces what
				// was previously a SILENT lost update, so a low rate here is
				// the feature working, and growth means a precondition was
				// chosen wrong for its path.
				metrics.RecordCacheOperation("meta_put", "precondition_lost")
				log.Debug().Str("bucket", bucket).Str("key", key).Uint64("expected", expected).
					Msg("Skipping meta write - version precondition lost to a newer write")
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	// Unconditional: retry the CAS against whatever is current. Contention on a
	// single object's metadata is bounded by the populate paths racing it, so
	// the loop terminating early is a pathology worth surfacing, not masking.
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		_, version, found, err := c.client.GetWithVersion(ctx, metaKey)
		if err != nil && !isNotFoundError(err) {
			return false, err
		}
		_ = found // absent reads carry a usable token since ocache v1.13.0
		_, err = c.client.PutIfVersion(ctx, metaKey, metaBytes, ttl, version)
		if err == nil {
			return true, nil
		}
		if _, mismatch := cacheclient.IsVersionMismatch(err); !mismatch {
			return false, err
		}
	}
	return false, fmt.Errorf("meta write for %s/%s lost %d consecutive version races", bucket, key, 8)
}

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

	// Store metadata AFTER body is complete. Version-bumping unconditional:
	// same last-write-wins semantics as before, but the row stays stamped.
	if _, err := c.putMetaVersioned(ctx, bucket, key, metaKey, metaBytes, int64(ttl), VersionAny); err != nil {
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
// The expected version is the precondition of the meta write (see
// putMetaVersioned): 0 for a populate decided while the entry was absent,
// the observed version for a read-modify-write, VersionAny to preserve
// last-write-wins. The tombstone check is retained alongside it — tombstones
// still order writers that carry no useful precondition (VersionAny), and
// belt-and-braces costs one point read.
func (c *Cache) PutWithMetaStreamTombstoneAware(
	ctx context.Context,
	bucket, key string,
	meta *CachedObjectMeta,
	body io.Reader,
	ttl int,
	writeStartTime int64, // Unix nano timestamp when write started
	expected uint64, // version precondition for the meta write
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

	// Write metadata AFTER body (makes entry visible), under the version
	// precondition. A lost precondition leaves the newer entry in place and the
	// just-written body to TTL, like the tombstone branch.
	wrote, err = c.putMetaVersioned(ctx, bucket, key, metaKey, metaBytes, int64(ttl), expected)
	if err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta put error")
		// Same rationale as the tombstone branch: leave the versioned body to TTL
		// rather than risk truncating a concurrent same-version reader.
		return false, err
	}
	if !wrote {
		return false, nil
	}

	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Int("ttl", ttl).
		Int("meta_size", len(metaBytes)).
		Msg("Cached object with metadata (streamed, tombstone-aware)")
	return true, nil
}

// GetMetaWithVersion is GetMeta plus the meta row's CAS version — the value a
// read-modify-write passes back as its precondition. Version 0 means absent.
func (c *Cache) GetMetaWithVersion(ctx context.Context, bucket, key string) (*CachedObjectMeta, uint64, bool, error) {
	if !c.IsEnabled() {
		return nil, 0, false, nil
	}
	metaBytes, version, found, err := c.client.GetWithVersion(ctx, MakeMetaKey(bucket, key))
	if err != nil {
		if isNotFoundError(err) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}
	if !found || metaBytes == nil {
		// Since ocache v1.13.0 (fenced deletes, #267) an absent read carries a
		// nonzero token — the fence of the CAS delete that removed the key, or
		// a fresh "absent as of now" stamp. Passing it through lets a populate
		// that observed absence be ORDERED against a later fenced delete;
		// squashing it to 0 would silently opt the caller back into legacy
		// unordered put-if-absent. Callers test found, never version==0.
		return nil, version, false, nil
	}
	meta, err := DecodeMeta(metaBytes)
	if err != nil {
		return nil, 0, false, err
	}
	return meta, version, true, nil
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

	// Delete only the metadata. That is sufficient to make subsequent reads miss
	// (a read resolves the body from meta.ETag, so with meta gone there is no body
	// lookup), and the tombstone above blocks any in-flight repopulation. The
	// versioned body is intentionally left to age out via TTL rather than deleted
	// synchronously — deleting it could truncate an in-flight reader still
	// streaming that exact version.
	//
	// FENCED (ocache v1.13.0, #267): the delete goes through the CAS op family
	// so it leaves a fence, ordering token-carrying populates that observed
	// pre-delete state. The tombstone above covers the writers that don't carry
	// tokens yet; it is retained for this one overlap release and retired next.
	if err := c.deleteMetaFenced(ctx, bucket, key); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta delete error")
		errs = append(errs, fmt.Errorf("delete meta: %w", err))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("Invalidated cache metadata (body ages out via TTL)")
	return nil
}

// deleteMetaFenced is the unconditional FENCED delete of a meta key: it must
// remove whatever is live and leave a fence either way, so that a populate
// that observed pre-delete state — including pre-delete ABSENCE — loses its
// commit. DeleteIfVersion(key, 0) fences an absent/dead key directly; against
// a live key it reports the current version, and the retry deletes exactly
// that observed version. Bounded: each retry means a concurrent writer just
// committed, and sustained contention on one meta key is a pathology worth
// surfacing, not masking.
func (c *Cache) deleteMetaFenced(ctx context.Context, bucket, key string) error {
	metaKey := MakeMetaKey(bucket, key)
	expected := uint64(0)
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := c.client.DeleteIfVersion(ctx, metaKey, expected)
		if err == nil {
			return nil
		}
		if mm, mismatch := cacheclient.IsVersionMismatch(err); mismatch {
			expected = mm.CurrentVersion
			continue
		}
		if isNotFoundError(err) {
			return nil
		}
		return err
	}
	return fmt.Errorf("fenced meta delete for %s/%s lost %d consecutive version races", bucket, key, 8)
}

// Delete removes an object from the cache.
func (c *Cache) Delete(ctx context.Context, bucket, key string) error {
	if !c.IsEnabled() {
		return nil
	}

	return c.DeleteWithMeta(ctx, bucket, key)
}

// DeleteIfETag invalidates the object's metadata only while it still carries
// the given ETag, using the store's per-key CAS (ocache #254): the version is
// read together with the metadata, and the delete is conditioned on it.
// Returns (false, nil) when the entry is already gone, carries a different
// ETag, or was replaced by a VERSION-STAMPED write between the read and the
// delete — the newer entry wins in each case.
//
// Scope of the guard: exact against version-stamped writers (CAS deletes, and
// populates once they carry version preconditions). A plain Put resets a row
// to the legacy version (storage EffectiveRowVersion semantics), so between
// today's plain-put populates the version adds nothing and the protection
// equals the previous compare-then-delete — the same read→delete window as
// before, never wider. Versioning the populate paths closes it.
//
// The tombstone is still written on the match path (same rationale as
// DeleteWithMeta: it blocks in-flight stamp-based populates, which CAS on this
// key does not order). On a lost CAS the tombstone may have been written
// spuriously; that can suppress one racing populate — churn, bounded by the
// tombstone TTL — but never removes data, which is strictly milder than the
// unguarded delete this replaces.
func (c *Cache) DeleteIfETag(ctx context.Context, bucket, key, staleETag string) (bool, error) {
	if !c.IsEnabled() {
		return false, nil
	}
	metaKey := MakeMetaKey(bucket, key)

	metaBytes, version, found, err := c.client.GetWithVersion(ctx, metaKey)
	if err != nil {
		if isNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	if !found || metaBytes == nil {
		return false, nil
	}
	meta, err := DecodeMeta(metaBytes)
	if err != nil {
		// Undecodable metadata is not the version the caller observed; leave it
		// for the read path's own decode-error handling.
		return false, err
	}
	if meta.ETag != staleETag {
		return false, nil
	}

	var errs []error
	if terr := c.WriteTombstone(ctx, bucket, key); terr != nil {
		log.Debug().Err(terr).Str("bucket", bucket).Str("key", key).
			Msg("Failed to write tombstone (continuing with guarded delete)")
		errs = append(errs, fmt.Errorf("write tombstone: %w", terr))
	}
	if derr := c.client.DeleteIfVersion(ctx, metaKey, version); derr != nil {
		if _, mismatch := cacheclient.IsVersionMismatch(derr); mismatch {
			// Replaced (or removed) between the read and the delete: the newer
			// state wins, exactly what the guard exists for.
			return false, nil
		}
		errs = append(errs, fmt.Errorf("guarded meta delete: %w", derr))
		return false, errors.Join(errs...)
	}
	// The versioned body is intentionally left to age out via TTL, as in
	// DeleteWithMeta.
	log.Debug().Str("bucket", bucket).Str("key", key).Msg("Invalidated cache metadata (ETag-guarded)")
	return true, errors.Join(errs...)
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

// IsBlockLocal reports whether the given block is owned by this node. known is
// false when the underlying client cannot report ownership, in which case
// callers must keep their existing synchronous write behavior.
func (c *Cache) IsBlockLocal(bucket, key, etag string, blockSize, blockIdx int64) (local, known bool) {
	lc, ok := c.client.(localityChecker)
	if !ok {
		return false, false
	}
	return lc.IsLocal(MakeBlockKey(bucket, key, etag, blockSize, blockIdx)), true
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

// unaryPutRequestSize returns the encoded size of ocache's v1.9.0 PutRequest.
// PutRequest has string key = 1, int64 ttl_seconds = 2, and bytes data = 3. Keeping
// this check allocation-free matters on the staged-block path, while the exact size
// keeps an oversized custom block_size on PutStream instead of exceeding gRPC's limit.
func unaryPutRequestSize(key string, data []byte, ttlSeconds int64) int64 {
	var size int64
	if key != "" {
		size += 1 + int64(protoVarintSize(uint64(len(key)))) + int64(len(key))
	}
	if ttlSeconds != 0 {
		size += 1 + int64(protoVarintSize(uint64(ttlSeconds)))
	}
	if len(data) != 0 {
		size += 1 + int64(protoVarintSize(uint64(len(data)))) + int64(len(data))
	}
	return size
}

func protoVarintSize(v uint64) int {
	size := 1
	for v >= 0x80 {
		v >>= 7
		size++
	}
	return size
}

func canPutBlockUnaryForLimit(key string, data []byte, ttlSeconds, limit int64) bool {
	return unaryPutRequestSize(key, data, ttlSeconds) <= limit
}

func canPutBlockUnary(key string, data []byte, ttlSeconds int64) bool {
	// TAG uses embedded's default coordinator router. Bound the unary request by
	// both its send limit and ocache's client limit so either pinned setting can
	// become the limiting side without changing this path.
	limit := int64(min(cacheclient.MaxMessageSize, coordinator.MaxMessageSize))
	return canPutBlockUnaryForLimit(key, data, ttlSeconds, limit)
}

// PutBlock writes a fully validated block to cache. Clients that explicitly
// support a unary byte write receive it when the request fits ocache's configured
// message limit; all other clients retain the streaming path.
func (c *Cache) PutBlock(ctx context.Context, bucket, key, etag string, blockSize, blockIdx int64, data []byte, ttl int) error {
	if !c.IsEnabled() || etag == "" {
		return nil
	}
	if ttl == 0 {
		ttl = int(c.defaultTTL)
	}
	blockKey := MakeBlockKey(bucket, key, etag, blockSize, blockIdx)

	if canPutBlockUnary(blockKey, data, int64(ttl)) {
		if putter, ok := c.client.(blockBytePutter); ok {
			handled, err := putter.PutBlockBytes(ctx, blockKey, data, int64(ttl))
			if handled || err != nil {
				if err != nil {
					log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Int64("block", blockIdx).Msg("Cache block put error")
				}
				return err
			}
		}
	}

	if err := c.client.PutStream(ctx, blockKey, bytes.NewReader(data), int64(ttl)); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Int64("block", blockIdx).Msg("Cache block put error")
		return err
	}
	return nil
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
// The expected version is the meta write's precondition (see putMetaVersioned).
func (c *Cache) PutMetaTombstoneAware(
	ctx context.Context,
	bucket, key string,
	meta *CachedObjectMeta,
	ttl int,
	writeStartTime int64,
	expected uint64,
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
	return c.putMetaVersioned(ctx, bucket, key, metaKey, metaBytes, int64(ttl), expected)
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
