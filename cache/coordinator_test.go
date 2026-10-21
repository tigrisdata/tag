package cache

import (
	"context"
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
	if tok == 0 {
		t.Fatal("legacy decision token must be a nonzero stamp")
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

// Legacy mode must never touch the CAS API: a client whose CAS methods panic
// proves the separation is total.
type noCASClient struct {
	cacheclient.CacheClient
}

func (n *noCASClient) GetWithVersion(context.Context, string) ([]byte, uint64, bool, error) {
	panic("CAS op reached a legacy coordinator")
}
func (n *noCASClient) PutIfVersion(context.Context, string, []byte, int64, uint64) (uint64, error) {
	panic("CAS op reached a legacy coordinator")
}
func (n *noCASClient) DeleteIfVersion(context.Context, string, uint64) error {
	panic("CAS op reached a legacy coordinator")
}

func TestLegacyCoordinator_NeverCallsCASAPI(t *testing.T) {
	cfg := config.NewDefault()
	c := NewCacheWithClient(&noCASClient{CacheClient: cacheclient.NewMemoryCache()}, &cfg.Cache)
	ctx := context.Background()

	meta := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v1"`, ContentLength: 2, StatusCode: 200}
	_, tok, _, _ := c.GetMetaWithVersion(ctx, "b", "k")
	if wrote, err := c.PutMetaIfVersion(ctx, "b", "k", meta, 60, tok); err != nil || !wrote {
		t.Fatalf("legacy put: (%v, %v)", wrote, err)
	}
	if _, _, found, _ := c.GetMetaWithVersion(ctx, "b", "k"); !found {
		t.Fatal("legacy read: not found")
	}
	if _, err := c.DeleteIfETag(ctx, "b", "k", `"v1"`); err != nil {
		t.Fatalf("legacy guarded delete: %v", err)
	}
	if err := c.DeleteWithMeta(ctx, "b", "k"); err != nil {
		t.Fatalf("legacy delete: %v", err)
	}
	_ = time.Now()
}

// expected==0 carries no decision-time token, so the legacy coordinator must
// refuse it outright — even on a virgin key with no tombstone anywhere.
func TestLegacyCoordinator_ZeroExpectedRefused(t *testing.T) {
	c, mem := newLegacyTestCache()
	ctx := context.Background()

	meta := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v1"`, StatusCode: 200}
	wrote, err := c.PutMetaIfVersion(ctx, "b", "k", meta, 60, 0)
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
