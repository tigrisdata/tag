package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

func newLegacyTestCache() (*Cache, *cacheclient.MemoryCache) {
	cfg := config.NewDefault() // legacy coordination IS the default
	mem := cacheclient.NewMemoryCache()
	return NewCacheWithClient(mem, &cfg.Cache), mem
}

// The default configuration selects the legacy coordinator: a fresh install or
// an untouched upgrade must run the v1.20-compatible mechanism.
func TestCoordinator_DefaultIsLegacy(t *testing.T) {
	cfg := config.NewDefault()
	if !cfg.Cache.IsLegacyCoordination() {
		t.Fatal("default configuration did not select legacy coordination")
	}
	c, _ := newLegacyTestCache()
	if _, ok := c.coord.(*legacyCoordinator); !ok {
		t.Fatalf("default coordinator is %T, want *legacyCoordinator", c.coord)
	}
	cfg.Cache.SetLegacyCoordination(false)
	c2 := NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	if _, ok := c2.coord.(*casCoordinator); !ok {
		t.Fatalf("legacy_coordination=false coordinator is %T, want *casCoordinator", c2.coord)
	}
}

// Legacy mode reproduces the v1.20 ordering contract: a populate whose
// decision-time stamp predates an invalidation tombstone must not commit.
func TestLegacyCoordinator_TombstoneBlocksStalePopulate(t *testing.T) {
	c, mem := newLegacyTestCache()
	ctx := context.Background()

	// Populate decides (token = stamp) before the invalidation...
	_, tok, found, err := c.GetMetaWithVersion(ctx, "b", "k")
	if err != nil || found {
		t.Fatalf("decision read: found=%v err=%v", found, err)
	}
	if tok.decisionTime <= 0 {
		t.Fatal("legacy decision token must carry a positive tombstone timestamp")
	}

	// ...the invalidation lands (tombstone + delete)...
	if err := c.DeleteWithMeta(ctx, "b", "k"); err != nil {
		t.Fatalf("DeleteWithMeta: %v", err)
	}
	if data, err := mem.Get(ctx, MakeTombstoneKey("b", "k")); err != nil || len(data) != 8 {
		t.Fatalf("legacy invalidation left no tombstone: err=%v len=%d", err, len(data))
	}

	// ...and the stale commit must be refused.
	meta := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"stale"`, StatusCode: 200}
	wrote, err := c.PutMetaIfVersion(ctx, "b", "k", meta, 60, tok)
	if err != nil {
		t.Fatalf("PutMetaIfVersion: %v", err)
	}
	if wrote {
		t.Fatal("stale populate committed over a newer tombstone")
	}

	// A populate deciding AFTER the invalidation commits normally.
	_, tok2, _, _ := c.GetMetaWithVersion(ctx, "b", "k")
	fresh := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"fresh"`, StatusCode: 200}
	if wrote, err := c.PutMetaIfVersion(ctx, "b", "k", fresh, 60, tok2); err != nil || !wrote {
		t.Fatalf("post-invalidation populate = (%v, %v), want (true, nil)", wrote, err)
	}
}

// Concurrent misses share the marker created at token capture. A first populate's
// publish generation is reusable by the second; only an invalidation advances it.
func TestLegacyConcurrentFirstPopulatesSharePublishGeneration(t *testing.T) {
	ctx := context.Background()
	c, _ := newLegacyTestCache()
	const bucket, key = "b", "parallel-first-populates"

	_, firstToken, firstFound, err := c.GetMetaWithVersion(ctx, bucket, key)
	if err != nil || firstFound {
		t.Fatalf("first decision=(%+v, found=%t, err=%v), want decision-time publish marker", firstToken, firstFound, err)
	}
	_, secondToken, secondFound, err := c.GetMetaWithVersion(ctx, bucket, key)
	if err != nil || secondFound || secondToken.version != firstToken.version {
		t.Fatalf("second decision=(%+v, found=%t, err=%v), want the shared publish marker", secondToken, secondFound, err)
	}

	first := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"first"`, StatusCode: http.StatusOK}
	if wrote, err := c.PutMetaIfVersion(ctx, bucket, key, first, 60, firstToken); err != nil || !wrote {
		t.Fatalf("first populate=(%t, %v), want success", wrote, err)
	}
	second := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"second"`, StatusCode: http.StatusOK}
	if wrote, err := c.PutMetaIfVersion(ctx, bucket, key, second, 60, secondToken); err != nil || !wrote {
		t.Fatalf("concurrent populate=(%t, %v), want legacy last-writer success", wrote, err)
	}
	meta, found, err := c.GetMeta(ctx, bucket, key)
	if err != nil || !found || meta.ETag != second.ETag {
		t.Fatalf("final legacy metadata=(%+v, found=%t, err=%v), want the later populate", meta, found, err)
	}
}

// Legacy commits retain the v1.20 timestamp guard in addition to the current
// reader sidecar, so a tombstone from an older peer still blocks stale writers.
func TestLegacyCoordinatorOldPeerTombstoneBlocksRefill(t *testing.T) {
	ctx := context.Background()
	c, store := newLegacyTestCache()
	const bucket, key = "b", "old-peer"

	if err := c.DeleteWithMeta(ctx, bucket, key); err != nil {
		t.Fatalf("current-code first invalidation: %v", err)
	}
	_, token, found, err := c.GetMetaWithVersion(ctx, bucket, key)
	if err != nil || found || token.decisionTime <= 0 {
		t.Fatalf("decision token=(%+v, found=%t, err=%v), want present generation and a timestamp", token, found, err)
	}

	// A v1.20 peer has no generation-sidecar operation. It orders itself only
	// through the shared eight-byte tombstone and plain metadata delete.
	newerTombstone := uint64(token.decisionTime + 1)
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, newerTombstone)
	if err := store.Put(ctx, MakeTombstoneKey(bucket, key), encoded, c.defaultTTL); err != nil {
		t.Fatalf("old-peer tombstone write: %v", err)
	}
	if err := store.Delete(ctx, MakeMetaKey(bucket, key)); err != nil && !isNotFoundError(err) {
		t.Fatalf("old-peer metadata delete: %v", err)
	}

	stale := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"stale"`, StatusCode: http.StatusOK}
	if wrote, err := c.PutMetaIfVersion(ctx, bucket, key, stale, 60, token); err != nil || wrote {
		t.Fatalf("late legacy refill=(wrote=%t, err=%v), want tombstone rejection", wrote, err)
	}
	if _, found, err := c.GetMeta(ctx, bucket, key); err != nil || found {
		t.Fatalf("current reader found stale metadata=%t err=%v", found, err)
	}
}

// Legacy DeleteIfETag is the pre-CAS compare-then-delete: removes a matching
// entry (leaving a tombstone), spares a mismatched one.
func TestLegacyCoordinator_DeleteIfETag(t *testing.T) {
	c, _ := newLegacyTestCache()
	ctx := context.Background()

	meta := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v1"`, ContentLength: 2, StatusCode: 200}
	if err := c.PutWithMeta(ctx, "b", "k", meta, []byte("v1"), 60); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if deleted, err := c.DeleteIfETag(ctx, "b", "k", `"other"`); err != nil || deleted {
		t.Fatalf("mismatched ETag: deleted=%v err=%v, want (false, nil)", deleted, err)
	}
	if deleted, err := c.DeleteIfETag(ctx, "b", "k", `"v1"`); err != nil || !deleted {
		t.Fatalf("matching ETag: deleted=%v err=%v, want (true, nil)", deleted, err)
	}
	if _, found, _ := c.GetMeta(ctx, "b", "k"); found {
		t.Fatal("entry survived the guarded delete")
	}
}

// Legacy mode keeps plain metadata and tombstone operations for v1.20
// compatibility. Its CAS operations are isolated to the separate generation
// sidecar so new readers can reject a late plain metadata Put.
type generationSidecarCASClient struct {
	cacheclient.CacheClient
}

func (c *generationSidecarCASClient) checkGenerationKey(key string) {
	if !strings.HasPrefix(key, "meta-gen|") {
		panic("CAS operation reached a legacy metadata or tombstone key: " + key)
	}
}

func (c *generationSidecarCASClient) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	c.checkGenerationKey(key)
	return c.CacheClient.GetWithVersion(ctx, key)
}
func (c *generationSidecarCASClient) PutIfVersion(ctx context.Context, key string, data []byte, ttl int64, expected uint64) (uint64, error) {
	c.checkGenerationKey(key)
	return c.CacheClient.PutIfVersion(ctx, key, data, ttl, expected)
}
func (c *generationSidecarCASClient) DeleteIfVersion(ctx context.Context, key string, expected uint64) error {
	c.checkGenerationKey(key)
	return c.CacheClient.DeleteIfVersion(ctx, key, expected)
}

func TestLegacyCoordinatorCASIsIsolatedToGenerationKey(t *testing.T) {
	cfg := config.NewDefault()
	c := NewCacheWithClient(&generationSidecarCASClient{CacheClient: cacheclient.NewMemoryCache()}, &cfg.Cache)
	ctx := context.Background()

	meta := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v1"`, ContentLength: 2, StatusCode: 200}
	_, tok, _, err := c.GetMetaWithVersion(ctx, "b", "k")
	if err != nil {
		t.Fatalf("legacy decision read: %v", err)
	}
	if wrote, err := c.PutMetaIfVersion(ctx, "b", "k", meta, 60, tok); err != nil || !wrote {
		t.Fatalf("legacy put: (%v, %v)", wrote, err)
	}
	if _, _, found, err := c.GetMetaWithVersion(ctx, "b", "k"); err != nil || !found {
		t.Fatalf("legacy read: found=%t err=%v", found, err)
	}
	if _, err := c.DeleteIfETag(ctx, "b", "k", `"v1"`); err != nil {
		t.Fatalf("legacy guarded delete: %v", err)
	}
	if err := c.DeleteWithMeta(ctx, "b", "k"); err != nil {
		t.Fatalf("legacy delete: %v", err)
	}
}

// expected==0 carries no decision-time token, so the legacy coordinator must
// refuse it outright — even on a virgin key with no tombstone anywhere.
func TestLegacyCoordinator_ZeroExpectedRefused(t *testing.T) {
	c, mem := newLegacyTestCache()
	ctx := context.Background()

	meta := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v1"`, StatusCode: 200}
	wrote, err := c.PutMetaIfVersion(ctx, "b", "k", meta, 60, MetaVersionToken{})
	if err != nil {
		t.Fatalf("PutMetaIfVersion(expected=0): %v", err)
	}
	if wrote {
		t.Fatal("legacy coordinator accepted an unordered expected=0 write")
	}
	if data, err := mem.Get(ctx, MakeMetaKey("b", "k")); err == nil && data != nil {
		t.Fatal("refused write still landed in the store")
	}
}

// A refill on a second legacy coordinator carries the shared sidecar
// generation, not its local wall clock. A later invalidation advances that
// generation and makes the old metadata write invisible to current readers.
func TestLegacyCoordinator_GenerationSidecarBlocksCrossCoordinatorRefill(t *testing.T) {
	ctx := context.Background()
	shared := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	deleter := NewCacheWithClient(shared, &cfg.Cache)
	refiller := NewCacheWithClient(shared, &cfg.Cache)

	old := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"old"`, ContentLength: 3, StatusCode: http.StatusOK}
	if err := refiller.PutWithMeta(ctx, "b", "k", old, []byte("old"), 60); err != nil {
		t.Fatalf("seed old object: %v", err)
	}
	if err := deleter.DeleteWithMeta(ctx, "b", "k"); err != nil {
		t.Fatalf("first DeleteWithMeta: %v", err)
	}
	_, staleToken, found, err := refiller.GetMetaWithVersion(ctx, "b", "k")
	if err != nil || found {
		t.Fatalf("cross-coordinator decision read: found=%v err=%v", found, err)
	}
	if err := deleter.DeleteWithMeta(ctx, "b", "k"); err != nil {
		t.Fatalf("second DeleteWithMeta: %v", err)
	}

	stale := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"stale"`, StatusCode: http.StatusOK}
	if wrote, err := refiller.PutMetaIfVersion(ctx, "b", "k", stale, 60, staleToken); err != nil || wrote {
		t.Fatalf("stale PutMetaIfVersion=(%t,%v), want generation precondition loss", wrote, err)
	}
	if _, found, err := refiller.GetMeta(ctx, "b", "k"); err != nil || found {
		t.Fatalf("current reader found stale metadata=%t err=%v", found, err)
	}

	_, freshToken, _, err := refiller.GetMetaWithVersion(ctx, "b", "k")
	if err != nil {
		t.Fatalf("fresh decision read: %v", err)
	}
	fresh := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"fresh"`, StatusCode: http.StatusOK}
	if wrote, err := refiller.PutMetaIfVersion(ctx, "b", "k", fresh, 60, freshToken); err != nil || !wrote {
		t.Fatalf("fresh PutMetaIfVersion=(%t,%v), want successful generation commit", wrote, err)
	}
}

type legacyTombstoneReadFailureClient struct {
	cacheclient.CacheClient
	tombstoneKey string
	err          error
	fail         bool
}

func (c *legacyTombstoneReadFailureClient) Get(ctx context.Context, key string) ([]byte, error) {
	if c.fail && key == c.tombstoneKey {
		return nil, c.err
	}
	return c.CacheClient.Get(ctx, key)
}

func TestLegacyCoordinatorTombstoneReadFailuresFailClosed(t *testing.T) {
	ctx := context.Background()
	const bucket, key = "b", "k"
	backendErr := errors.New("tombstone store unavailable")

	t.Run("decision token", func(t *testing.T) {
		client := &legacyTombstoneReadFailureClient{
			CacheClient:  cacheclient.NewMemoryCache(),
			tombstoneKey: MakeTombstoneKey(bucket, key),
			err:          backendErr,
			fail:         true,
		}
		cfg := config.NewDefault()
		c := NewCacheWithClient(client, &cfg.Cache)
		if meta, token, found, err := c.GetMetaWithVersion(ctx, bucket, key); !errors.Is(err, backendErr) || meta != nil || token != (MetaVersionToken{}) || found {
			t.Fatalf("GetMetaWithVersion = (%v, %+v, %v, %v), want no token and tombstone read error", meta, token, found, err)
		}
	})

	t.Run("meta commit", func(t *testing.T) {
		client := &legacyTombstoneReadFailureClient{
			CacheClient:  cacheclient.NewMemoryCache(),
			tombstoneKey: MakeTombstoneKey(bucket, key),
			err:          backendErr,
		}
		cfg := config.NewDefault()
		c := NewCacheWithClient(client, &cfg.Cache)
		_, token, found, err := c.GetMetaWithVersion(ctx, bucket, key)
		if err != nil || found {
			t.Fatalf("decision read: found=%v err=%v", found, err)
		}
		client.fail = true
		meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"stale"`, StatusCode: http.StatusOK}
		wrote, err := c.PutMetaIfVersion(ctx, bucket, key, meta, 60, token)
		if !errors.Is(err, backendErr) || wrote {
			t.Fatalf("PutMetaIfVersion = (%v, %v), want the tombstone read error without a write", wrote, err)
		}
		if _, found, err := c.GetMeta(ctx, bucket, key); err != nil || found {
			t.Fatalf("metadata found=%v err=%v after refused write", found, err)
		}
	})

}

type generationCASUnavailableClient struct {
	cacheclient.CacheClient
	err error
}

func (c *generationCASUnavailableClient) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	if strings.HasPrefix(key, "meta-gen|") {
		return nil, 0, false, c.err
	}
	return c.CacheClient.GetWithVersion(ctx, key)
}

func (c *generationCASUnavailableClient) PutIfVersion(ctx context.Context, key string, data []byte, ttl int64, expected uint64) (uint64, error) {
	if strings.HasPrefix(key, "meta-gen|") {
		return 0, c.err
	}
	return c.CacheClient.PutIfVersion(ctx, key, data, ttl, expected)
}

func TestLegacyGenerationSidecarUnavailableFailsClosed(t *testing.T) {
	ctx := context.Background()
	const bucket, key = "b", "k"
	backendErr := errors.New("generation sidecar CAS unsupported")
	store := cacheclient.NewMemoryCache()
	oldMeta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"old"`, ContentLength: 3, StatusCode: http.StatusOK}
	oldBytes, err := oldMeta.Encode() // v1.20-style metadata has no generation field
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, MakeMetaKey(bucket, key), oldBytes, 3600); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, MakeBodyKey(bucket, key, oldMeta.ETag), []byte("old"), 3600); err != nil {
		t.Fatal(err)
	}

	cfg := config.NewDefault()
	client := &generationCASUnavailableClient{CacheClient: store, err: backendErr}
	c := NewCacheWithClient(client, &cfg.Cache)
	if meta, found, err := c.GetMeta(ctx, bucket, key); !errors.Is(err, backendErr) || meta != nil || found {
		t.Fatalf("GetMeta = (%v, %v, %v), want fail-closed sidecar error", meta, found, err)
	}
	if meta, token, found, err := c.GetMetaWithVersion(ctx, bucket, key); !errors.Is(err, backendErr) || meta != nil || token != (MetaVersionToken{}) || found {
		t.Fatalf("GetMetaWithVersion = (%v, %+v, %v, %v), want no token on sidecar error", meta, token, found, err)
	}
	if err := c.DeleteWithMeta(ctx, bucket, key); !errors.Is(err, backendErr) {
		t.Fatalf("DeleteWithMeta error=%v, want sidecar invalidation error", err)
	}
	if _, err := store.Get(ctx, MakeMetaKey(bucket, key)); !isNotFoundError(err) {
		t.Fatalf("legacy metadata delete did not continue after sidecar error: err=%v", err)
	}
}

func TestLegacyGenerationSidecarRejectsUntaggedPlainPut(t *testing.T) {
	c, store := newLegacyTestCache()
	ctx := context.Background()
	const bucket, key = "b", "untagged"

	if err := c.DeleteWithMeta(ctx, bucket, key); err != nil {
		t.Fatalf("advance generation: %v", err)
	}
	old := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"old"`, ContentLength: 3, StatusCode: http.StatusOK}
	oldBytes, err := old.Encode() // an older writer knows only the legacy meta key
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, MakeMetaKey(bucket, key), oldBytes, 3600); err != nil {
		t.Fatalf("legacy plain metadata Put: %v", err)
	}
	if meta, found, err := c.GetMeta(ctx, bucket, key); err != nil || found || meta != nil {
		t.Fatalf("new TAG reader accepted untagged metadata: (%v, found=%t, err=%v)", meta, found, err)
	}

	_, token, found, err := c.GetMetaWithVersion(ctx, bucket, key)
	if err != nil || found {
		t.Fatalf("new decision read: found=%t err=%v", found, err)
	}
	fresh := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"fresh"`, ContentLength: 5, StatusCode: http.StatusOK}
	if wrote, err := c.PutMetaIfVersion(ctx, bucket, key, fresh, 60, token); err != nil || !wrote {
		t.Fatalf("new tagged PutMetaIfVersion=(%t,%v), want success", wrote, err)
	}
	if got, found, err := c.GetMeta(ctx, bucket, key); err != nil || !found || got.ETag != fresh.ETag {
		t.Fatalf("tagged metadata read=(%v, found=%t, err=%v), want fresh entry", got, found, err)
	}
}

type generationCASReadBarrier struct {
	cacheclient.CacheClient
	generationKey string
	armed         atomic.Bool
	reads         atomic.Int32
	bothRead      chan struct{}
	release       chan struct{}
}

func (c *generationCASReadBarrier) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	data, version, found, err := c.CacheClient.GetWithVersion(ctx, key)
	if key == c.generationKey && c.armed.Load() {
		read := c.reads.Add(1)
		if read <= 2 {
			if read == 2 {
				close(c.bothRead)
			}
			<-c.release
		}
	}
	return data, version, found, err
}

func TestLegacyGenerationSidecarConcurrentAdvances(t *testing.T) {
	ctx := context.Background()
	const bucket, key = "b", "concurrent"
	shared := &generationCASReadBarrier{
		CacheClient:   cacheclient.NewMemoryCache(),
		generationKey: makeGenerationKey(bucket, key),
		bothRead:      make(chan struct{}),
		release:       make(chan struct{}),
	}
	cfg := config.NewDefault()
	first := NewCacheWithClient(shared, &cfg.Cache)
	second := NewCacheWithClient(shared, &cfg.Cache)
	old := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"old"`, ContentLength: 3, StatusCode: http.StatusOK}
	if err := first.PutWithMeta(ctx, bucket, key, old, []byte("old"), 60); err != nil {
		t.Fatalf("seed old metadata: %v", err)
	}
	_, staleToken, found, err := first.GetMetaWithVersion(ctx, bucket, key)
	if err != nil || !found {
		t.Fatalf("pre-delete read: found=%t err=%v", found, err)
	}

	shared.armed.Store(true)
	deleteResults := make(chan error, 2)
	go func() { deleteResults <- first.DeleteWithMeta(ctx, bucket, key) }()
	go func() { deleteResults <- second.DeleteWithMeta(ctx, bucket, key) }()
	select {
	case <-shared.bothRead:
	case <-time.After(5 * time.Second):
		t.Fatal("both legacy coordinators did not read the same generation")
	}
	close(shared.release)
	for i := 0; i < 2; i++ {
		if err := <-deleteResults; err != nil {
			t.Fatalf("concurrent DeleteWithMeta: %v", err)
		}
	}

	if wrote, err := first.PutMetaIfVersion(ctx, bucket, key, old, 60, staleToken); err != nil || wrote {
		t.Fatalf("pre-delete metadata Put=(%t,%v), want generation precondition loss", wrote, err)
	}
	if meta, found, err := first.GetMeta(ctx, bucket, key); err != nil || found {
		t.Fatalf("stale metadata after concurrent generation advances=(%v, found=%t, err=%v)", meta, found, err)
	}
}

func TestLegacyGenerationSidecarEvictionFailsClosed(t *testing.T) {
	c, mem := newLegacyTestCache()
	ctx := context.Background()
	const bucket, key = "b", "evicted-generation"

	meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"old"`, ContentLength: 3, StatusCode: http.StatusOK}
	if err := c.PutWithMeta(ctx, bucket, key, meta, []byte("old"), 60); err != nil {
		t.Fatalf("seed tagged metadata: %v", err)
	}
	_, staleToken, found, err := c.GetMetaWithVersion(ctx, bucket, key)
	if err != nil || !found {
		t.Fatalf("read tagged metadata: found=%t err=%v", found, err)
	}
	if err := mem.Delete(ctx, makeGenerationKey(bucket, key)); err != nil {
		t.Fatalf("evict generation sidecar: %v", err)
	}
	if _, found, err := c.GetMeta(ctx, bucket, key); err != nil || found {
		t.Fatalf("reader found metadata without its sidecar: found=%t err=%v", found, err)
	}
	if wrote, err := c.PutMetaIfVersion(ctx, bucket, key, meta, 60, staleToken); err != nil || wrote {
		t.Fatalf("writer with evicted generation=(%t,%v), want fail closed", wrote, err)
	}
}

func TestLegacyGenerationSidecarTreatsPreTagMetadataAsMiss(t *testing.T) {
	ctx := context.Background()
	const bucket, key = "b", "pre-tagged"
	store := cacheclient.NewMemoryCache()
	legacyMeta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"old"`, ContentLength: 3, StatusCode: http.StatusOK}
	data, err := legacyMeta.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, MakeMetaKey(bucket, key), data, 60); err != nil {
		t.Fatal(err)
	}

	cfg := config.NewDefault()
	current := NewCacheWithClient(store, &cfg.Cache)
	if meta, found, err := current.GetMeta(ctx, bucket, key); err != nil || found || meta != nil {
		t.Fatalf("untagged metadata read=(%v, found=%t, err=%v), want miss", meta, found, err)
	}
}

func TestLegacyGenerationDecisionInitializesSidecar(t *testing.T) {
	c, store := newLegacyTestCache()
	ctx := context.Background()
	_, token, found, err := c.GetMetaWithVersion(ctx, "b", "uncacheable")
	if err != nil || found || token.decisionTime <= 0 {
		t.Fatalf("decision read=(token=%+v, found=%t, err=%v), want a present decision marker and metadata miss", token, found, err)
	}
	marker, err := store.Get(ctx, makeGenerationKey("b", "uncacheable"))
	if err != nil || !bytes.Equal(marker, []byte{legacyGenerationMarker}) {
		t.Fatalf("decision marker=%v err=%v, want an initialized publish marker", marker, err)
	}
}
