package cache

import (
	"bytes"
	"context"
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
	client     cacheclient.CacheClient
	defaultTTL int64 // seconds
	enabled    bool
	closed     bool // coord is the meta-key ordering strategy: legacy tombstones (default)
	// or CAS fences/versions. Selected once at construction; see coordinator.go.
	coord metaCoordinator
}

// NewCacheWithClient creates a cache with an injected client.
// This allows tests to use an in-memory cache implementation like cacheclient.NewMemoryCache().
func NewCacheWithClient(client cacheclient.CacheClient, cfg *config.CacheConfig) *Cache {
	ttl := int64(config.DefaultCacheTTL.Seconds())
	enabled := true // Default to enabled
	legacy := true  // Default to the legacy coordinator (see coordinator.go)
	var sizeThreshold int64
	if cfg != nil {
		if cfg.TTL > 0 {
			ttl = int64(cfg.TTL.Seconds())
		}
		enabled = cfg.IsEnabled()
		legacy = cfg.IsLegacyCoordination()
		sizeThreshold = cfg.SizeThreshold
	}
	var coord metaCoordinator
	if legacy {
		coord = &legacyCoordinator{client: client, tombstoneTTL: TombstoneTTLSeconds(sizeThreshold)}
	} else {
		coord = &casCoordinator{client: client}
	}
	return &Cache{
		client:     client,
		defaultTTL: ttl,
		enabled:    enabled,
		coord:      coord,
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

// VersionAny requests a version-BUMPING unconditional meta write: last-write-
// wins like a plain Put, but implemented as a CAS loop so the row stays
// version-stamped. This upholds the package invariant that NO meta key is ever
// written with a plain Put — a plain Put resets the row to the storage layer's
// legacy version, which blinds DeleteIfETag's version guard for every
// subsequent conditional operation on the key.
const VersionAny = ^uint64(0)

// putMetaVersioned commits the meta bytes through the selected coordinator
// (see coordinator.go for both mechanisms' semantics).
func (c *Cache) putMetaVersioned(ctx context.Context, bucket, key, metaKey string, metaBytes []byte, ttl int64, expected uint64) (bool, error) {
	return c.coord.putMeta(ctx, bucket, key, metaKey, metaBytes, ttl, expected)
}
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

// PutWithMetaStreamIfVersion is like PutWithMetaStream but commits the
// metadata under the caller's decision-time version precondition (see
// putMetaVersioned): body first, then the meta write that makes the entry
// visible — refused atomically if the entry changed (including a fenced
// delete) since the caller's token was read.
//
// The expected version is the meta write's precondition: a decision-time
// token (live version or absence token) for an ordered populate, VersionAny
// for last-write-wins. 0 is unordered put-if-absent under the CAS
// coordinator but is REFUSED by the legacy coordinator (nothing to order
// against) — production callers always carry a nonzero token.
//
// Returns wrote=true only when the metadata was actually written (the entry
// is now visible). It is false when the write was skipped without error — the
// object was not cacheable (no ETag) or the precondition was lost — so
// callers can distinguish a no-op from a real write.
func (c *Cache) PutWithMetaStreamIfVersion(
	ctx context.Context,
	bucket, key string,
	meta *CachedObjectMeta,
	body io.Reader,
	ttl int,
	expected uint64, // decision-time version precondition for the meta write
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

	// No tombstone gate: the version precondition below IS the invalidation
	// guard — a fenced delete (or any write) landing after the caller's
	// decision-time token makes this commit lose atomically. On a lost commit
	// the just-written versioned body is left to age out via TTL rather than
	// deleted synchronously: a concurrent populate of the same ETag could have
	// a reader streaming this exact body key, and deleting it would truncate
	// that reader. Without a visible meta entry the orphaned body is
	// unreachable and harmless until it expires.

	// Write metadata AFTER body (makes entry visible), under the version
	// precondition. A lost precondition leaves the newer entry in place and the
	// just-written body to TTL.
	wrote, err = c.putMetaVersioned(ctx, bucket, key, metaKey, metaBytes, int64(ttl), expected)
	if err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache meta put error")
		// Same rationale as the lost-precondition branch: leave the versioned body to TTL
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
		Msg("Cached object with metadata (streamed, version-preconditioned)")
	return true, nil
}

// GetMetaWithVersion is GetMeta plus the decision-time TOKEN a subsequent
// commit must carry (semantics per coordinator: a store version in CAS mode,
// a wall-clock stamp in legacy mode). Absent entries return found=false with
// a valid token; callers test found, never token==0.
func (c *Cache) GetMetaWithVersion(ctx context.Context, bucket, key string) (*CachedObjectMeta, uint64, bool, error) {
	if !c.IsEnabled() {
		return nil, 0, false, nil
	}
	return c.coord.getMetaWithVersion(ctx, bucket, key)
}
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
// PutBodyStream streams a whole-object body into the cache under the given
// discriminator (an ETag for content-addressed entries, a NewBodyRef id for
// the engine's streamed writes, where the ETag is unknown until EOF). Body
// only — the entry becomes visible when its metadata commits; a body whose
// meta never commits is an orphan that ages out by TTL, the same lifecycle
// as a displaced version.
func (c *Cache) PutBodyStream(ctx context.Context, bucket, key, discriminator string, r io.Reader, ttl int64) error {
	if !c.IsEnabled() {
		_, _ = io.Copy(io.Discard, r) // drain so a pipe producer never blocks
		return nil
	}
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	return c.client.PutStream(ctx, MakeBodyKey(bucket, key, discriminator), r, ttl)
}

// DeleteBody removes one staged or orphaned body by its discriminator. Used
// by the engine's PUT abort paths (overrun, digest mismatch, refused
// conditional) to reclaim a body whose metadata will never commit; TTL is
// the backstop when this best-effort delete fails.
func (c *Cache) DeleteBody(ctx context.Context, bucket, key, discriminator string) error {
	if !c.IsEnabled() {
		return nil
	}
	return c.client.Delete(ctx, MakeBodyKey(bucket, key, discriminator))
}

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

// DeleteWithMeta removes both metadata and body from cache via the fenced CAS
// delete: the fence it leaves is what stops an in-flight populate that
// observed pre-delete state (including pre-delete absence) from completing.
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

	// Delete only the metadata. That is sufficient to make subsequent reads miss
	// (a read resolves the body from meta.ETag, so with meta gone there is no body
	// lookup), and the fence left by the CAS delete blocks any in-flight
	// token-carrying repopulation. The
	// versioned body is intentionally left to age out via TTL rather than deleted
	// synchronously — deleting it could truncate an in-flight reader still
	// streaming that exact version.
	//
	// FENCED (ocache v1.13.0, #267): the delete goes through the CAS op family
	// so it leaves a fence, ordering every token-carrying populate that
	// observed pre-delete state — the sole invalidation mechanism.
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

// deleteMetaFenced routes the unconditional invalidation through the selected
// coordinator (fenced CAS delete, or legacy tombstone + plain delete).
func (c *Cache) deleteMetaFenced(ctx context.Context, bucket, key string) error {
	return c.coord.deleteMeta(ctx, bucket, key)
}
func (c *Cache) Delete(ctx context.Context, bucket, key string) error {
	if !c.IsEnabled() {
		return nil
	}

	return c.DeleteWithMeta(ctx, bucket, key)
}

// DeleteMetaIfVersion invalidates the object's metadata only while it still
// carries the observed version token (from GetMetaWithVersion): the guard is
// the exact snapshot the caller examined, so an entry replaced in between —
// even by identical content reusing the same MD5 ETag — refuses the delete.
// Returns (false, nil) when replaced or absent. CAS-coordinator strength; the
// legacy coordinator refuses (see coordinator.go).
func (c *Cache) DeleteMetaIfVersion(ctx context.Context, bucket, key string, version uint64) (bool, error) {
	if !c.IsEnabled() {
		return false, nil
	}
	return c.coord.deleteMetaIfVersion(ctx, bucket, key, version)
}

// DeleteIfETag invalidates the object's metadata only while it still carries
// the given ETag, through the selected coordinator: an atomic versioned CAS
// in CAS mode; the pre-CAS narrowed compare-then-delete in legacy mode.
// Returns (false, nil) when the entry is absent, different, or replaced.
func (c *Cache) DeleteIfETag(ctx context.Context, bucket, key, staleETag string) (bool, error) {
	if !c.IsEnabled() {
		return false, nil
	}
	deleted, err := c.coord.deleteMetaIfETag(ctx, bucket, key, staleETag)
	if err == nil && deleted {
		// The versioned body is intentionally left to age out via TTL, as in
		// DeleteWithMeta.
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Invalidated cache metadata (ETag-guarded)")
	}
	return deleted, err
}
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

// firstByteWriter accepts the complete range response while retaining only its
// first byte. Range backends may write in multiple chunks, and must receive the
// full count back so streaming implementations do not report a short write.
type firstByteWriter struct {
	first   [1]byte
	written int
}

func (w *firstByteWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if w.written == 0 {
		w.first[0] = p[0]
	}
	w.written += len(p)
	return len(p), nil
}

// getRangeStreamByKey streams an inclusive byte range [start,end] of the blob at cacheKey to
// w, mapping ocache's not-found to ErrNotFound and handling ocache's read-byte-0 quirk.
// bucket/key are used for logging only.
func (c *Cache) getRangeStreamByKey(ctx context.Context, cacheKey, bucket, key string, start, end int64, w io.Writer) error {
	// Handle ocache quirk: reading byte 0 alone requires reading 2 bytes
	// and discarding the last byte
	if start == 0 && end == 0 {
		// Single byte at position 0 - need to read 2 bytes and discard last.
		// Keep only the first byte while reporting each backend write in full.
		var first firstByteWriter
		err := c.client.GetRangeStream(ctx, cacheKey, 0, 1, &first)
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
		if first.written == 0 {
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache miss (range)")
			return ErrNotFound
		}
		c.recordServeLocality(cacheKey)
		// Write only the first byte
		_, err = w.Write(first.first[:])
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

// BodyExistsErr reports whether a whole-object body is present, with the same
// error contract as BlockExistsErr: genuine absence is (false, nil), a transient
// probe failure is (false, err). Same first-byte probe, on the body key.
func (c *Cache) BodyExistsErr(ctx context.Context, bucket, key, etag string) (present bool, err error) {
	if !c.IsEnabled() || etag == "" {
		return false, nil
	}
	e := c.getRangeStreamByKey(ctx, MakeBodyKey(bucket, key, etag), bucket, key, 0, 0, io.Discard)
	if e == nil {
		return true, nil
	}
	if errors.Is(e, ErrNotFound) {
		return false, nil // genuinely absent
	}
	return false, e // transient failure — not proof of absence
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
// deleted on invalidation; they age out by TTL. The block-mode meta (written version-
// preconditioned via PutMetaIfVersion) is the visibility gate, and reads only resolve blocks
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

// PutMetaIfVersion writes only the object metadata (no body), committed under
// the caller's decision-time version precondition (see putMetaVersioned). It
// is the visibility gate for a block-mode entry — callers write the touched
// blocks first, then this meta last. See RFC 0001.
func (c *Cache) PutMetaIfVersion(
	ctx context.Context,
	bucket, key string,
	meta *CachedObjectMeta,
	ttl int,
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
	return c.putMetaVersioned(ctx, bucket, key, metaKey, metaBytes, int64(ttl), expected)
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
