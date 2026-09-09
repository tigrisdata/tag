package cache

// Meta-coordination strategy. The cache stores object metadata under one
// mutable key per object, and everything that orders writers of that key —
// invalidation, populate preconditions, guarded deletes — lives behind ONE
// seam with two implementations, selected once at construction (mirroring the
// proxy's RequestForwarder strategy pattern):
//
//   - legacyCoordinator (cache.legacy_coordination: true, the DEFAULT):
//     the pre-CAS mechanism, byte-faithful to v1.20 — timestamp tombstones
//     written before deletes, checked before meta commits, plain Put/Delete on
//     the meta key. Rolling upgrades from v1.20 are homogeneous under it: a
//     mixed cluster runs one mechanism, so there is no cross-version ordering
//     gap to reason about.
//
//   - casCoordinator (legacy_coordination: false): ocache v1.13.0's fenced
//     CAS deletes and per-key versions carry all ordering; no tombstones
//     exist. Enable only when every node in the cluster runs a CAS-capable
//     release (standalone nodes qualify trivially).
//
// The caller-facing contract is identical either way: read a decision-time
// TOKEN with getMetaWithVersion before fetching, commit with putMeta under it.
// In CAS mode the token is a store version (absence included); in legacy mode
// it is the wall-clock stamp the old writeStartTime discipline used — which is
// why the proxy layer is mode-blind and contains no conditionals.
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
	// getMetaWithVersion reads the metadata and the decision-time token a
	// subsequent putMeta must carry. Absent entries return found=false with a
	// still-valid token; callers test found, never token==0.
	getMetaWithVersion(ctx context.Context, bucket, key string) (*CachedObjectMeta, uint64, bool, error)
	// putMeta commits metaBytes under the caller's decision-time token.
	// expected==VersionAny is the deliberately unordered last-write-wins used
	// by tests and simple seeding. Returns wrote=false without error when the
	// precondition was lost — the newer state wins.
	putMeta(ctx context.Context, bucket, key, metaKey string, metaBytes []byte, ttl int64, expected uint64) (bool, error)
	// deleteMeta is the unconditional invalidation of the meta key.
	deleteMeta(ctx context.Context, bucket, key string) error
	// deleteMetaIfETag invalidates only while the entry still carries
	// staleETag. Returns (false, nil) when absent, different, or replaced.
	deleteMetaIfETag(ctx context.Context, bucket, key, staleETag string) (bool, error)
}

// ============================================================================
// CAS coordinator — fences and versions, no tombstones (ocache v1.13.0, #267)
// ============================================================================

type casCoordinator struct {
	client cacheclient.CacheClient
}

func (c *casCoordinator) getMetaWithVersion(ctx context.Context, bucket, key string) (*CachedObjectMeta, uint64, bool, error) {
	metaBytes, version, found, err := c.client.GetWithVersion(ctx, MakeMetaKey(bucket, key))
	if err != nil {
		if isNotFoundError(err) {
			// Defensive: a v1.13.0 client reports absence as found=false with a
			// token, not an error. Pass through whatever version accompanied
			// the error rather than squashing to 0 — 0 would opt the caller
			// into unordered put-if-absent that a fence exists to refuse.
			return nil, version, false, nil
		}
		return nil, 0, false, err
	}
	if !found || metaBytes == nil {
		// Absence carries a nonzero token (fence stamp or fresh observation);
		// it orders the caller's commit against a later fenced delete.
		return nil, version, false, nil
	}
	meta, err := DecodeMeta(metaBytes)
	if err != nil {
		return nil, 0, false, err
	}
	return meta, version, true, nil
}

func (c *casCoordinator) putMeta(ctx context.Context, bucket, key, metaKey string, metaBytes []byte, ttl int64, expected uint64) (bool, error) {
	if expected != VersionAny {
		if _, err := c.client.PutIfVersion(ctx, metaKey, metaBytes, ttl, expected); err != nil {
			if _, mismatch := cacheclient.IsVersionMismatch(err); mismatch {
				// Debug + metric, per the repo's log policy: a lost
				// precondition replaces what was previously a SILENT lost
				// update, so a low rate is the feature working; growth means a
				// precondition was chosen wrong for its path.
				metrics.RecordCacheOperation("meta_put", "precondition_lost")
				log.Debug().Str("bucket", bucket).Str("key", key).Uint64("expected", expected).
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

// ============================================================================
// Legacy coordinator — timestamp tombstones, byte-faithful to v1.20
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
	client       cacheclient.CacheClient
	tombstoneTTL int64 // seconds; must outlive the longest racing cache-populate
}

// getMetaWithVersion reads via the plain API and hands out the wall-clock
// stamp the old writeStartTime discipline used as the decision-time token:
// callers capture it before fetching, and putMeta refuses the commit if an
// invalidation tombstone postdates it — the pre-CAS ordering, unchanged.
func (c *legacyCoordinator) getMetaWithVersion(ctx context.Context, bucket, key string) (*CachedObjectMeta, uint64, bool, error) {
	stamp := uint64(time.Now().UnixNano())
	metaBytes, err := c.client.Get(ctx, MakeMetaKey(bucket, key))
	if err != nil {
		if isNotFoundError(err) {
			return nil, stamp, false, nil
		}
		return nil, 0, false, err
	}
	if metaBytes == nil {
		return nil, stamp, false, nil
	}
	meta, err := DecodeMeta(metaBytes)
	if err != nil {
		return nil, 0, false, err
	}
	return meta, stamp, true, nil
}

func (c *legacyCoordinator) putMeta(ctx context.Context, bucket, key, metaKey string, metaBytes []byte, ttl int64, expected uint64) (bool, error) {
	if expected != VersionAny {
		// Tombstone gate right before the visibility-granting meta write: an
		// invalidation at or after the caller's decision-time stamp blocks the
		// commit, so a populate cannot resurrect deleted metadata.
		if ts := c.tombstoneTimestamp(ctx, bucket, key); ts >= int64(expected) {
			metrics.RecordCacheOperation("meta_put", "precondition_lost")
			log.Debug().Str("bucket", bucket).Str("key", key).
				Int64("tombstone_ts", ts).Uint64("write_start", expected).
				Msg("Skipping meta write - tombstone detected")
			return false, nil
		}
	}
	if err := c.client.Put(ctx, metaKey, metaBytes, ttl); err != nil {
		return false, err
	}
	return true, nil
}

// deleteMeta is v1.20's invalidation, unchanged: tombstone FIRST (it blocks
// in-flight populates from completing), then the plain metadata delete. Both
// steps are attempted even if the first fails, and a genuine failure of
// either is returned so callers don't report a successful invalidation while
// stale metadata is still readable.
func (c *legacyCoordinator) deleteMeta(ctx context.Context, bucket, key string) error {
	var errs []error
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
	metaBytes, err := c.client.Get(ctx, MakeMetaKey(bucket, key))
	if err != nil {
		if isNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	if metaBytes == nil {
		return false, nil
	}
	meta, err := DecodeMeta(metaBytes)
	if err != nil {
		return false, err
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
	ts := time.Now().UnixNano()
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(ts))
	return c.client.Put(ctx, MakeTombstoneKey(bucket, key), data, c.tombstoneTTL)
}

func (c *legacyCoordinator) tombstoneTimestamp(ctx context.Context, bucket, key string) int64 {
	data, err := c.client.Get(ctx, MakeTombstoneKey(bucket, key))
	if err != nil || len(data) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data))
}
