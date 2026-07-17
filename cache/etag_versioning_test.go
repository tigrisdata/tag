package cache

import (
	"bytes"
	"context"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

func newVersioningTestCache(t *testing.T) (*Cache, cacheclient.CacheClient) {
	t.Helper()
	mem := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	return NewCacheWithClient(mem, &cfg.Cache), mem
}

// The core guarantee: metadata for one version always resolves to that version's
// body, never a concurrently-written different version's body (no torn pair).
func TestETagVersionedBody_MetaResolvesToItsOwnBody(t *testing.T) {
	c, mem := newVersioningTestCache(t)
	ctx := context.Background()
	bucket, key := "b", "k"

	meta1 := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"v1"`, ContentLength: 2, StatusCode: 200}
	if err := c.PutWithMeta(ctx, bucket, key, meta1, []byte("v1"), 60); err != nil {
		t.Fatalf("PutWithMeta v1: %v", err)
	}

	// A concurrent writer stores a different version's body at its own key,
	// without touching v1's body.
	if err := mem.Put(ctx, MakeBodyKey(bucket, key, `"v2"`), []byte("VERSION-TWO"), 60); err != nil {
		t.Fatalf("seed v2 body: %v", err)
	}

	// A reader holding meta_v1 must resolve to body_v1 — never the v2 body.
	var buf bytes.Buffer
	if err := c.GetBodyStream(ctx, bucket, key, meta1.ETag, &buf); err != nil {
		t.Fatalf("GetBodyStream v1: %v", err)
	}
	if buf.String() != "v1" {
		t.Errorf("meta_v1 resolved to %q, want %q (torn pair!)", buf.String(), "v1")
	}
}

// Bodies are never deleted synchronously: after an overwrite, the superseded
// version's body still exists (it ages out via TTL), so an in-flight reader that
// resolved the old meta can still stream it without truncation.
func TestETagVersionedBody_VersionsCoexistUntilTTL(t *testing.T) {
	c, _ := newVersioningTestCache(t)
	ctx := context.Background()
	bucket, key := "b", "k"

	meta1 := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"v1"`, ContentLength: 2, StatusCode: 200}
	_ = c.PutWithMeta(ctx, bucket, key, meta1, []byte("v1"), 60)
	meta2 := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"v2"`, ContentLength: 2, StatusCode: 200}
	_ = c.PutWithMeta(ctx, bucket, key, meta2, []byte("v2"), 60)

	// Both versions' bodies remain readable — the old one is not evicted.
	var buf bytes.Buffer
	if err := c.GetBodyStream(ctx, bucket, key, `"v1"`, &buf); err != nil || buf.String() != "v1" {
		t.Errorf("v1 body after overwrite: err=%v body=%q, want it to still exist", err, buf.String())
	}
	buf.Reset()
	if err := c.GetBodyStream(ctx, bucket, key, `"v2"`, &buf); err != nil || buf.String() != "v2" {
		t.Errorf("v2 body: err=%v body=%q", err, buf.String())
	}
}

// Invalidation removes only metadata; the body is left to age out via TTL so it
// cannot truncate an in-flight reader still streaming that version.
func TestETagVersionedBody_DeleteRemovesMetaNotBody(t *testing.T) {
	c, _ := newVersioningTestCache(t)
	ctx := context.Background()
	bucket, key := "b", "k"

	meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"v1"`, ContentLength: 2, StatusCode: 200}
	_ = c.PutWithMeta(ctx, bucket, key, meta, []byte("v1"), 60)

	if err := c.DeleteWithMeta(ctx, bucket, key); err != nil {
		t.Fatalf("DeleteWithMeta: %v", err)
	}

	// Meta is gone -> reads miss.
	if _, ok, _ := c.GetMeta(ctx, bucket, key); ok {
		t.Error("meta still present after DeleteWithMeta")
	}
	// Body still resolvable by its ETag (ages out via TTL) — a reader that
	// resolved this version before the invalidation is not truncated.
	var buf bytes.Buffer
	if err := c.GetBodyStream(ctx, bucket, key, `"v1"`, &buf); err != nil || buf.String() != "v1" {
		t.Errorf("body after DeleteWithMeta: err=%v body=%q, want it to survive", err, buf.String())
	}
}

// A versioned (non-empty ETag) read must NEVER serve bytes from the legacy
// unversioned key. Overwrites and invalidation leave that legacy entry in place
// until TTL, so falling back to it could return older, unrelated bytes under
// metadata that describes a newer version. A versioned-key miss must surface as
// ErrNotFound so the caller refetches from upstream instead of serving stale data.
func TestETagVersionedBody_VersionedMissDoesNotServeLegacyBody(t *testing.T) {
	c, mem := newVersioningTestCache(t)
	ctx := context.Background()
	bucket, key := "b", "k"

	// Seed a stale legacy unversioned body, with no matching versioned body.
	if err := mem.Put(ctx, MakeBodyKey(bucket, key, ""), []byte("STALE-LEGACY"), 60); err != nil {
		t.Fatalf("seed legacy body: %v", err)
	}

	var buf bytes.Buffer
	if err := c.GetBodyStream(ctx, bucket, key, `"v1"`, &buf); err != ErrNotFound {
		t.Errorf("GetBodyStream returned err=%v body=%q, want ErrNotFound (no legacy fallback)", err, buf.String())
	}
	buf.Reset()
	if err := c.GetRangeStream(ctx, bucket, key, `"v1"`, 0, 5, &buf); err != ErrNotFound {
		t.Errorf("GetRangeStream returned err=%v body=%q, want ErrNotFound (no legacy fallback)", err, buf.String())
	}
}

// The streaming write path (the primary populate path) must also refuse empty-ETag
// objects, and must drain the body so the producer side of the pipe doesn't block.
func TestETagVersionedBody_EmptyETagNotCachedViaStream(t *testing.T) {
	c, mem := newVersioningTestCache(t)
	ctx := context.Background()
	bucket, key := "b", "k"

	body := bytes.NewReader([]byte("body-bytes"))
	meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: "", ContentLength: 10, StatusCode: 200}
	if err := c.PutWithMetaStreamTombstoneAware(ctx, bucket, key, meta, body, 60, 1); err != nil {
		t.Fatalf("PutWithMetaStreamTombstoneAware: %v", err)
	}
	if _, found, _ := c.GetMeta(ctx, bucket, key); found {
		t.Error("empty-ETag object should not be cached via the streaming path (meta present)")
	}
	if _, err := mem.Get(ctx, MakeBodyKey(bucket, key, "")); err == nil {
		t.Error("empty-ETag object should not be cached via the streaming path (body present)")
	}
	if body.Len() != 0 {
		t.Errorf("body not drained: %d bytes remain (producer pipe would block)", body.Len())
	}
}

// Objects with no ETag are not cached at all: there is no version discriminator,
// so caching them at a single unversioned key would reintroduce the in-place
// overwrite / truncation hazard the ETag versioning was introduced to prevent.
func TestETagVersionedBody_EmptyETagIsNotCached(t *testing.T) {
	c, mem := newVersioningTestCache(t)
	ctx := context.Background()
	bucket, key := "b", "k"

	meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: "", ContentLength: 4, StatusCode: 200}
	if err := c.PutWithMeta(ctx, bucket, key, meta, []byte("body"), 60); err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}
	// Neither meta nor body should have been written.
	if _, found, _ := c.GetMeta(ctx, bucket, key); found {
		t.Error("empty-ETag object should not be cached (meta present)")
	}
	if _, err := mem.Get(ctx, MakeBodyKey(bucket, key, "")); err == nil {
		t.Error("empty-ETag object should not be cached (body present)")
	}
}

// A tombstone must outlive any cache-populate that could race it: the populate is
// only compared against the tombstone right before its metadata write, so an
// expired tombstone reads as zero and lets the stale write through. The TTL is
// therefore derived from the size threshold, not fixed — raising size_threshold
// must not silently shorten the guard relative to the write it has to outlive.
func TestTombstoneTTLSeconds(t *testing.T) {
	// Small/unset thresholds still get the floor.
	if got := TombstoneTTLSeconds(0); got != MinTombstoneTTLSeconds {
		t.Errorf("TombstoneTTLSeconds(0) = %d, want floor %d", got, MinTombstoneTTLSeconds)
	}
	if got := TombstoneTTLSeconds(1024); got != MinTombstoneTTLSeconds {
		t.Errorf("TombstoneTTLSeconds(1KiB) = %d, want floor %d", got, MinTombstoneTTLSeconds)
	}
	// The 60s that shipped before was far shorter than a large object's write.
	if MinTombstoneTTLSeconds <= 60 {
		t.Errorf("floor %d is not meaningfully above the old 60s tombstone TTL", MinTombstoneTTLSeconds)
	}
	// A large threshold scales the TTL past the floor.
	big := int64(10 * 1024 * 1024 * 1024) // 10 GiB
	if got := TombstoneTTLSeconds(big); got <= MinTombstoneTTLSeconds {
		t.Errorf("TombstoneTTLSeconds(10GiB) = %d, want > floor %d (must scale with size_threshold)", got, MinTombstoneTTLSeconds)
	}
	// Monotonic in the threshold.
	if TombstoneTTLSeconds(big) <= TombstoneTTLSeconds(big/4) {
		t.Error("TombstoneTTLSeconds must be non-decreasing in size_threshold")
	}
}
