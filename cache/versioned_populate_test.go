package cache

import (
	"context"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

func newVersionedTestCache() (*Cache, *cacheclient.MemoryCache) {
	cfg := config.NewDefault()
	mem := cacheclient.NewMemoryCache()
	return NewCacheWithClient(mem, &cfg.Cache), mem
}

// A put-if-absent populate (expected 0) must refuse when an entry exists: the
// racer that established it fetched the same-or-newer state and wins.
func TestPutMetaTombstoneAware_PutIfAbsentRefusesExisting(t *testing.T) {
	c, _ := newVersionedTestCache()
	ctx := context.Background()

	existing := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"racer"`, StatusCode: 200}
	if wrote, err := c.PutMetaTombstoneAware(ctx, "b", "k", existing, 60, time.Now().UnixNano(), 0); err != nil || !wrote {
		t.Fatalf("seed via put-if-absent = (%v, %v), want (true, nil)", wrote, err)
	}

	late := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"late"`, StatusCode: 200}
	wrote, err := c.PutMetaTombstoneAware(ctx, "b", "k", late, 60, time.Now().UnixNano(), 0)
	if err != nil {
		t.Fatalf("PutMetaTombstoneAware: %v", err)
	}
	if wrote {
		t.Fatal("put-if-absent overwrote an existing entry")
	}
	meta, found, _ := c.GetMeta(ctx, "b", "k")
	if !found || meta.ETag != `"racer"` {
		t.Fatalf("racer's entry disturbed: %+v", meta)
	}
}

// A strict-version write must refuse when the entry moved past the snapshot.
func TestPutMetaTombstoneAware_StaleVersionRefused(t *testing.T) {
	c, _ := newVersionedTestCache()
	ctx := context.Background()

	v1 := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v1"`, StatusCode: 200}
	if wrote, err := c.PutMetaTombstoneAware(ctx, "b", "k", v1, 60, time.Now().UnixNano(), 0); err != nil || !wrote {
		t.Fatalf("seed: (%v, %v)", wrote, err)
	}
	_, version, found, err := c.GetMetaWithVersion(ctx, "b", "k")
	if err != nil || !found {
		t.Fatalf("GetMetaWithVersion: found=%v err=%v", found, err)
	}

	// The entry moves on (an unconditional refresh)...
	v2 := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v2"`, StatusCode: 200}
	if wrote, err := c.PutMetaTombstoneAware(ctx, "b", "k", v2, 60, time.Now().UnixNano(), VersionAny); err != nil || !wrote {
		t.Fatalf("refresh: (%v, %v)", wrote, err)
	}

	// ...and the stale snapshot's write must lose.
	stale := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v1-promoted"`, StatusCode: 200}
	wrote, err := c.PutMetaTombstoneAware(ctx, "b", "k", stale, 60, time.Now().UnixNano(), version)
	if err != nil {
		t.Fatalf("stale write: %v", err)
	}
	if wrote {
		t.Fatal("stale-version write clobbered a newer entry")
	}
	meta, _, _, _ := c.GetMetaWithVersion(ctx, "b", "k")
	if meta == nil || meta.ETag != `"v2"` {
		t.Fatalf("newer entry disturbed: %+v", meta)
	}
}

// VersionAny preserves last-write-wins but keeps the row version-stamped, so a
// subsequent guarded operation still distinguishes it from the snapshot a
// racer holds.
func TestVersionAny_BumpsVersion(t *testing.T) {
	c, mem := newVersionedTestCache()
	ctx := context.Background()

	v1 := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v1"`, StatusCode: 200}
	if wrote, err := c.PutMetaTombstoneAware(ctx, "b", "k", v1, 60, time.Now().UnixNano(), VersionAny); err != nil || !wrote {
		t.Fatalf("first: (%v, %v)", wrote, err)
	}
	_, ver1, _, _ := mem.GetWithVersion(ctx, MakeMetaKey("b", "k"))

	v2 := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v2"`, StatusCode: 200}
	if wrote, err := c.PutMetaTombstoneAware(ctx, "b", "k", v2, 60, time.Now().UnixNano(), VersionAny); err != nil || !wrote {
		t.Fatalf("second: (%v, %v)", wrote, err)
	}
	_, ver2, _, _ := mem.GetWithVersion(ctx, MakeMetaKey("b", "k"))

	if ver2 <= ver1 {
		t.Fatalf("VersionAny write did not bump the version: %d -> %d", ver1, ver2)
	}
	if meta, _, _ := c.GetMeta(ctx, "b", "k"); meta == nil || meta.ETag != `"v2"` {
		t.Fatal("last-write-wins semantics broken")
	}
}
