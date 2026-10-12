package cache

// Meta-coordination strategy. The cache stores object metadata under one
// mutable key per object, and everything that orders writers of that key —
// invalidation, populate preconditions, guarded deletes — lives behind ONE
// seam with two implementations, selected once at construction (mirroring the
// proxy's RequestForwarder strategy pattern):
//
//   - legacyCoordinator (cache.legacy_coordination: true, the DEFAULT):
//     preserves the v1.20 eight-byte tombstones and plain metadata Put/Delete
//     so older peers keep their existing wire protocol. New TAG readers and
//     writers also use a separate CAS generation sidecar: metadata rows carry
//     its decision-time version, and a read rejects a late plain Put after the
//     sidecar advances. Older readers do not consult that sidecar. If its CAS
//     RPC is unavailable on an old cache owner, new TAG readers fail closed.
//
//   - casCoordinator (legacy_coordination: false): ocache v1.13.0's fenced
//     CAS deletes and per-key versions carry all ordering; no tombstones
//     exist. Enable only when every node in the cluster runs a CAS-capable
//     release (standalone nodes qualify trivially).
//
// The caller-facing contract is identical either way: read a decision-time
// token with getMetaWithVersion before fetching, commit with putMeta under it.
// CAS mode uses the metadata-key store version. Legacy mode carries both the
// sidecar version and the v1.20 tombstone timestamp in the opaque token. The
// proxy layer remains mode-blind.
//
// Flipping legacy → CAS on a live cluster: set the flag and rolling-restart.
// Nodes in different modes during that restart window run different ordering
// mechanisms (a CAS delete writes no tombstone for a legacy populate to see,
// and vice versa), the same bounded, TTL-convergent exposure as any
// mixed-mechanism window — keep the flip's rolling restart brisk.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/metrics"
)

// metaCoordinator is the ordering seam for the mutable meta key.
type metaCoordinator interface {
	// getMeta is the current-reader path. The legacy coordinator validates its
	// sidecar generation before returning metadata; CAS reads the metadata key.
	getMeta(ctx context.Context, bucket, key string) (*CachedObjectMeta, bool, error)
	// getMetaWithVersion reads metadata plus the complete decision-time token.
	getMetaWithVersion(ctx context.Context, bucket, key string) (*CachedObjectMeta, MetaVersionToken, bool, error)
	// prepareMetaToken resolves an unconditional write token before body or
	// upstream work when the selected coordinator needs a decision-time fence.
	prepareMetaToken(ctx context.Context, bucket, key string, expected MetaVersionToken) (MetaVersionToken, error)
	// putMeta commits metaBytes under the caller's decision-time token.
	// Unconditional tokens are used only for last-write-wins seeding. Returns
	// wrote=false without error when the precondition was lost.
	putMeta(ctx context.Context, bucket, key, metaKey string, metaBytes []byte, ttl int64, expected MetaVersionToken) (bool, error)
	// deleteMeta is the unconditional invalidation of the meta key.
	deleteMeta(ctx context.Context, bucket, key string) error
	// deleteMetaIfETag invalidates only while the entry still carries
	// staleETag. Returns (false, nil) when absent, different, or replaced.
	deleteMetaIfETag(ctx context.Context, bucket, key, staleETag string) (bool, error)
	// deleteMetaIfVersion invalidates only while the entry still carries the
	// caller's observed version token — a check-then-delete whose guard is
	// exactly the check's snapshot, unlike deleteMetaIfETag's content ETag
	// (which identical bytes can reuse across entries and tiers). CAS-
	// coordinator strength: legacy coordination stores no versions to compare
	// and refuses with (false, nil).
	deleteMetaIfVersion(ctx context.Context, bucket, key string, expected MetaVersionToken) (bool, error)
}

// ============================================================================
// CAS coordinator — fences and versions, no tombstones (ocache v1.13.0, #267)
// ============================================================================

type casCoordinator struct {
	client cacheclient.CacheClient
}

func (c *casCoordinator) getMeta(ctx context.Context, bucket, key string) (*CachedObjectMeta, bool, error) {
	metaBytes, err := c.client.Get(ctx, MakeMetaKey(bucket, key))
	if err != nil {
		if isNotFoundError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if metaBytes == nil {
		return nil, false, nil
	}
	meta, err := DecodeMeta(metaBytes)
	if err != nil {
		return nil, false, err
	}
	return meta, true, nil
}

func (c *casCoordinator) getMetaWithVersion(ctx context.Context, bucket, key string) (*CachedObjectMeta, MetaVersionToken, bool, error) {
	metaBytes, version, found, err := c.client.GetWithVersion(ctx, MakeMetaKey(bucket, key))
	if err != nil {
		if isNotFoundError(err) {
			// Defensive: a v1.13.0 client reports absence as found=false with a
			// token, not an error. Pass through whatever version accompanied
			// the error rather than squashing to 0 — 0 would opt the caller
			// into unordered put-if-absent that a fence exists to refuse.
			return nil, MetaVersionToken{version: version}, false, nil
		}
		return nil, MetaVersionToken{}, false, err
	}
	if !found || metaBytes == nil {
		// Absence carries a nonzero token (fence stamp or fresh observation);
		// it orders the caller's commit against a later fenced delete.
		return nil, MetaVersionToken{version: version}, false, nil
	}
	meta, err := DecodeMeta(metaBytes)
	if err != nil {
		return nil, MetaVersionToken{}, false, err
	}
	return meta, MetaVersionToken{version: version}, true, nil
}

func (c *casCoordinator) prepareMetaToken(_ context.Context, _, _ string, expected MetaVersionToken) (MetaVersionToken, error) {
	return expected, nil
}

func (c *casCoordinator) putMeta(ctx context.Context, bucket, key, metaKey string, metaBytes []byte, ttl int64, expected MetaVersionToken) (bool, error) {
	if !expected.unconditional {
		if _, err := c.client.PutIfVersion(ctx, metaKey, metaBytes, ttl, expected.version); err != nil {
			if _, mismatch := cacheclient.IsVersionMismatch(err); mismatch {
				// Debug + metric, per the repo's log policy: a lost
				// precondition replaces what was previously a SILENT lost
				// update, so a low rate is the feature working; growth means a
				// precondition was chosen wrong for its path.
				metrics.RecordCacheOperation("meta_put", "precondition_lost")
				log.Debug().Str("bucket", bucket).Str("key", key).Uint64("expected", expected.version).
					Msg("Skipping meta write - version precondition lost to a newer write")
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	// Unconditional: retry the CAS against whatever is current. Contention on
	// one object's metadata is bounded by the populate paths racing it, so the
	// loop terminating early is a pathology worth surfacing, not masking.
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		_, version, _, err := c.client.GetWithVersion(ctx, metaKey)
		if err != nil && !isNotFoundError(err) {
			return false, err
		}
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

// deleteMeta is the unconditional FENCED delete: it must remove whatever is
// live and leave a fence either way, so a populate that observed pre-delete
// state — including pre-delete ABSENCE — loses its commit. DeleteIfVersion(0)
// fences an absent/dead key directly; against a live key it reports the
// current version, and the retry deletes exactly that observed version.
// Invalidation must not abandon a key to racing writers: each mismatch means
// exactly one more committed write to out-delete, so the loop always makes
// progress; it is bounded by the caller's context and a generous ceiling, and
// genuine exhaustion is surfaced as an error the callers record.
func (c *casCoordinator) deleteMeta(ctx context.Context, bucket, key string) error {
	metaKey := MakeMetaKey(bucket, key)
	expected := uint64(0)
	const maxAttempts = 64
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("fenced meta delete for %s/%s: %w", bucket, key, err)
		}
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
	return fmt.Errorf("fenced meta delete for %s/%s lost %d consecutive version races", bucket, key, 64)
}

func (c *casCoordinator) deleteMetaIfETag(ctx context.Context, bucket, key, staleETag string) (bool, error) {
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
		// Undecodable metadata is not the version the caller observed; leave
		// it for the read path's own decode-error handling.
		return false, err
	}
	if meta.ETag != staleETag {
		return false, nil
	}
	if derr := c.client.DeleteIfVersion(ctx, metaKey, version); derr != nil {
		if _, mismatch := cacheclient.IsVersionMismatch(derr); mismatch {
			// Replaced (or removed) between the read and the delete: the newer
			// state wins, exactly what the guard exists for.
			return false, nil
		}
		return false, fmt.Errorf("guarded meta delete: %w", derr)
	}
	return true, nil
}

func (c *casCoordinator) deleteMetaIfVersion(ctx context.Context, bucket, key string, expected MetaVersionToken) (bool, error) {
	if derr := c.client.DeleteIfVersion(ctx, MakeMetaKey(bucket, key), expected.version); derr != nil {
		if _, mismatch := cacheclient.IsVersionMismatch(derr); mismatch {
			// Replaced (or removed) since the caller's read: the newer state
			// wins — exactly what the guard exists for.
			return false, nil
		}
		if isNotFoundError(derr) {
			return false, nil
		}
		return false, fmt.Errorf("version-guarded meta delete: %w", derr)
	}
	return true, nil
}

// ============================================================================
// Legacy coordinator — v1.20 metadata/tombstone protocol plus the
// current-reader generation sidecar
// ============================================================================

const (
	// MinTombstoneTTLSeconds is the floor for how long an invalidation
	// tombstone lives (10 minutes). It comfortably exceeds a small object's
	// populate.
	MinTombstoneTTLSeconds = 600

	// The constants below mirror the proxy's cache-populate timeouts so the
	// TTL can model the same window (see TombstoneTTLSeconds).
	tombstoneWriteThroughput = 5 * 1024 * 1024
	tombstoneMinWriteSeconds = 60
	tombstoneFetchSeconds    = 300
	tombstoneMarginSeconds   = 300
)

// TombstoneTTLSeconds returns how long an invalidation tombstone must live for
// a given cache size threshold: it must outlive any populate that could race
// it (the upstream fetch plus the streaming write of the largest cacheable
// object), modeled additively — write + fetch + margin — so the margin stays
// constant at every threshold.
func TombstoneTTLSeconds(sizeThreshold int64) int64 {
	write := int64(tombstoneMinWriteSeconds)
	if sizeThreshold > 0 {
		if w := sizeThreshold / tombstoneWriteThroughput; w > write {
			write = w
		}
	}
	return max(write+tombstoneFetchSeconds+tombstoneMarginSeconds, MinTombstoneTTLSeconds)
}

type legacyCoordinator struct {
	client        cacheclient.CacheClient
	tombstoneTTL  int64 // seconds; must outlive the longest racing cache-populate
	generationTTL int64 // seconds; default cache TTL plus populate margin
}

const legacyGenerationMarker byte = 1

// generationToken reads the separate CAS sidecar used to tag legacy metadata.
// Unsupported or failed CAS reads fail closed.
func (c *legacyCoordinator) generationToken(ctx context.Context, bucket, key string) (uint64, bool, error) {
	marker, version, found, err := c.client.GetWithVersion(ctx, makeGenerationKey(bucket, key))
	if err != nil && !isNotFoundError(err) {
		return 0, false, fmt.Errorf("read legacy generation for %s/%s: %w", bucket, key, err)
	}
	if found && (len(marker) != 1 || marker[0] != legacyGenerationMarker) {
		return 0, false, fmt.Errorf("invalid legacy generation marker for %s/%s", bucket, key)
	}
	return version, found, nil
}

// ensureDecisionGeneration creates a marker at token-capture time when a key
// has none. The populate token carries its version, so eviction before commit
// leaves no matching generation and makes the delayed writer fail closed.
func (c *legacyCoordinator) ensureDecisionGeneration(ctx context.Context, bucket, key string) (uint64, error) {
	generationKey := makeGenerationKey(bucket, key)
	const maxAttempts = 64
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("initialize legacy generation for %s/%s: %w", bucket, key, err)
		}
		version, found, err := c.generationToken(ctx, bucket, key)
		if err != nil {
			return 0, err
		}
		if found {
			return version, nil
		}
		version, err = c.client.PutIfVersion(ctx, generationKey, []byte{legacyGenerationMarker}, c.generationTTL, version)
		if err == nil {
			return version, nil
		}
		if _, mismatch := cacheclient.IsVersionMismatch(err); !mismatch {
			return 0, fmt.Errorf("initialize legacy generation for %s/%s: %w", bucket, key, err)
		}
	}
	return 0, fmt.Errorf("initialize legacy generation for %s/%s lost %d consecutive version races", bucket, key, maxAttempts)
}

// advanceGeneration atomically moves the sidecar version before a legacy
// invalidation. The metadata key itself remains the plain v1.20 key so older
// peers can still read and write it; current readers use the sidecar version to
// reject late plain metadata puts.
func (c *legacyCoordinator) advanceGeneration(ctx context.Context, bucket, key string) error {
	generationKey := makeGenerationKey(bucket, key)
	const maxAttempts = 64
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("advance legacy generation for %s/%s: %w", bucket, key, err)
		}
		version, _, err := c.generationToken(ctx, bucket, key)
		if err != nil {
			return err
		}
		if _, err := c.client.PutIfVersion(ctx, generationKey, []byte{legacyGenerationMarker}, c.generationTTL, version); err == nil {
			return nil
		} else if _, mismatch := cacheclient.IsVersionMismatch(err); !mismatch {
			return fmt.Errorf("write legacy generation for %s/%s: %w", bucket, key, err)
		}
	}
	return fmt.Errorf("advance legacy generation for %s/%s lost %d consecutive version races", bucket, key, maxAttempts)
}

// getMetaWithVersion reads the metadata snapshot and captures the sidecar
// version plus v1.20 tombstone decision time. If no sidecar exists, it creates a
// decision marker before returning the token. Later marker eviction then makes
// the commit fail closed rather than looking like a fresh absence.
func (c *legacyCoordinator) getMetaWithVersion(ctx context.Context, bucket, key string) (*CachedObjectMeta, MetaVersionToken, bool, error) {
	// Stamp before every read. An older v1.20 peer can write a tombstone without
	// advancing this generation sidecar; the decision-time check must still see
	// that invalidation if it lands while we read the metadata and generation.
	decisionTime := time.Now().UnixNano()
	if _, err := c.tombstoneTimestamp(ctx, bucket, key); err != nil {
		return nil, MetaVersionToken{}, false, err
	}
	metaBytes, metaErr := c.client.Get(ctx, MakeMetaKey(bucket, key))
	if metaErr != nil && !isNotFoundError(metaErr) {
		return nil, MetaVersionToken{}, false, metaErr
	}
	var meta *CachedObjectMeta
	if metaErr == nil && metaBytes != nil {
		var err error
		meta, err = DecodeMeta(metaBytes)
		if err != nil {
			return nil, MetaVersionToken{}, false, err
		}
	}
	tokenVersion, foundGeneration, err := c.generationToken(ctx, bucket, key)
	if err != nil {
		return nil, MetaVersionToken{}, false, err
	}
	if !foundGeneration {
		// Keep a decision marker until this populate commits or its bounded
		// generation TTL expires. If the disk-cap cleaner evicts it after a
		// later invalidation, putMeta sees that the captured marker vanished and
		// refuses the old writer instead of recreating a fresh generation.
		tokenVersion, err = c.ensureDecisionGeneration(ctx, bucket, key)
		if err != nil {
			return nil, MetaVersionToken{}, false, err
		}
	}
	token := MetaVersionToken{
		version:      tokenVersion,
		decisionTime: decisionTime,
	}
	if meta == nil || meta.cacheGeneration != tokenVersion {
		return nil, token, false, nil
	}
	return meta, token, true, nil
}

func (c *legacyCoordinator) getMeta(ctx context.Context, bucket, key string) (*CachedObjectMeta, bool, error) {
	metaBytes, err := c.client.Get(ctx, MakeMetaKey(bucket, key))
	if err != nil {
		if isNotFoundError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if metaBytes == nil {
		return nil, false, nil
	}
	meta, err := DecodeMeta(metaBytes)
	if err != nil {
		return nil, false, err
	}
	generation, found, err := c.generationToken(ctx, bucket, key)
	if err != nil {
		return nil, false, err
	}
	if !found || meta.cacheGeneration != generation {
		return nil, false, nil
	}
	return meta, true, nil
}

func (c *legacyCoordinator) prepareMetaToken(ctx context.Context, bucket, key string, expected MetaVersionToken) (MetaVersionToken, error) {
	if !expected.unconditional {
		return expected, nil
	}
	_, token, _, err := c.getMetaWithVersion(ctx, bucket, key)
	return token, err
}

func (c *legacyCoordinator) putMeta(ctx context.Context, bucket, key, metaKey string, metaBytes []byte, ttl int64, expected MetaVersionToken) (bool, error) {
	if expected.decisionTime <= 0 {
		// No decision-time token to order against. Refuse rather than publish
		// metadata that current readers cannot validate.
		metrics.RecordCacheOperation("meta_put", "precondition_lost")
		return false, nil
	}
	// Retain the v1.20 decision-time tombstone check for mixed-version peers.
	// The sidecar is a separate guard for current readers because the metadata
	// key still uses a plain compatibility Put.
	tombstone, err := c.tombstoneTimestamp(ctx, bucket, key)
	if err != nil {
		return false, err
	}
	if tombstone >= expected.decisionTime {
		metrics.RecordCacheOperation("meta_put", "precondition_lost")
		log.Debug().Str("bucket", bucket).Str("key", key).Int64("tombstone_ts", tombstone).
			Int64("write_start", expected.decisionTime).Msg("Skipping meta write - tombstone detected")
		return false, nil
	}
	current, found, err := c.generationToken(ctx, bucket, key)
	if err != nil {
		return false, err
	}
	if !found || current != expected.version {
		// A decision token captured from an existing sidecar cannot be reused
		// after that marker expires or is evicted; fail closed rather than
		// reviving an old writer under a fresh generation.
		metrics.RecordCacheOperation("meta_put", "precondition_lost")
		return false, nil
	}
	generation := current
	meta, err := DecodeMeta(metaBytes)
	if err != nil {
		return false, err
	}
	meta.cacheGeneration = generation
	metaBytes, err = meta.Encode()
	if err != nil {
		return false, err
	}
	if err := c.client.Put(ctx, metaKey, metaBytes, ttl); err != nil {
		return false, err
	}
	return true, nil
}

// deleteMeta advances the generation sidecar for current readers, then uses
// v1.20's tombstone-first, plain metadata delete for older peers. All three
// operations are attempted even if one fails, and a genuine failure is
// returned so callers don't report successful invalidation while stale
// metadata may remain readable.
func (c *legacyCoordinator) deleteMeta(ctx context.Context, bucket, key string) error {
	var errs []error
	if err := c.advanceGeneration(ctx, bucket, key); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).
			Msg("Failed to advance cache generation (continuing with legacy invalidation)")
		errs = append(errs, fmt.Errorf("advance generation: %w", err))
	}
	if err := c.writeTombstone(ctx, bucket, key); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).
			Msg("Failed to write tombstone (continuing with delete)")
		errs = append(errs, fmt.Errorf("write tombstone: %w", err))
	}
	if err := c.client.Delete(ctx, MakeMetaKey(bucket, key)); err != nil && !isNotFoundError(err) {
		errs = append(errs, fmt.Errorf("delete meta: %w", err))
	}
	return errors.Join(errs...)
}

// deleteMetaIfETag is the pre-CAS compare-then-delete: it narrows the window
// ("deletes only the version we just observed as stale") rather than closing
// it — the accepted legacy semantics. A replacement landing between the
// compare and the delete is removed too; that degrades to v1.20's
// unconditional delete at these call sites — a spurious miss and refetch,
// never stale data. Closing the window takes CAS; that IS the other mode.
func (c *legacyCoordinator) deleteMetaIfETag(ctx context.Context, bucket, key, staleETag string) (bool, error) {
	meta, found, err := c.getMeta(ctx, bucket, key)
	if err != nil {
		return false, err
	}
	if !found || meta == nil {
		return false, nil
	}
	if meta.ETag != staleETag {
		return false, nil
	}
	if err := c.deleteMeta(ctx, bucket, key); err != nil {
		return false, err
	}
	return true, nil
}

func (c *legacyCoordinator) writeTombstone(ctx context.Context, bucket, key string) error {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(time.Now().UnixNano()))
	return c.client.Put(ctx, MakeTombstoneKey(bucket, key), data, c.tombstoneTTL)
}

func (c *legacyCoordinator) tombstoneTimestamp(ctx context.Context, bucket, key string) (int64, error) {
	data, err := c.client.Get(ctx, MakeTombstoneKey(bucket, key))
	if err != nil {
		if isNotFoundError(err) {
			return 0, nil
		}
		return 0, err
	}
	if len(data) != 8 {
		return 0, nil
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}

// deleteMetaIfVersion is a CAS-strength identity delete: legacy tokens carry a
// sidecar version and tombstone timestamp, but the plain metadata key has no
// per-row CAS version to compare. The only safe answer is to refuse. Its sole
// caller (the tiered cleanup repair)
// runs under CAS coordination by construction — tiered mode rejects legacy.
func (c *legacyCoordinator) deleteMetaIfVersion(ctx context.Context, bucket, key string, _ MetaVersionToken) (bool, error) {
	return false, nil
}
