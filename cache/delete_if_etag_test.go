package cache

import (
	"context"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

func newETagTestCache() *Cache {
	cfg := config.NewDefault()
	return NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
}

func seedEntry(t *testing.T, c *Cache, bucket, key, etag, body string) {
	t.Helper()
	meta := &CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          etag,
		ContentLength: int64(len(body)),
		StatusCode:    200,
	}
	if err := c.PutWithMeta(context.Background(), bucket, key, meta, []byte(body), 0); err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}
}

// The guard's positive edge: the entry still carries the observed ETag, so the
// guarded delete removes it and subsequent reads miss.
func TestDeleteIfETag_RemovesMatchingEntry(t *testing.T) {
	c := newETagTestCache()
	ctx := context.Background()
	seedEntry(t, c, "b", "k", `"v1"`, "body-v1")

	deleted, err := c.DeleteIfETag(ctx, "b", "k", `"v1"`)
	if err != nil {
		t.Fatalf("DeleteIfETag: %v", err)
	}
	if !deleted {
		t.Fatal("deleted = false, want true for a matching ETag")
	}
	if _, found, _ := c.GetMeta(ctx, "b", "k"); found {
		t.Fatal("meta still present after guarded delete")
	}
}

// The guard's negative edge: a newer version replaced the observed one, and
// the guarded delete must leave it untouched.
func TestDeleteIfETag_SparesReplacedEntry(t *testing.T) {
	c := newETagTestCache()
	ctx := context.Background()
	seedEntry(t, c, "b", "k", `"v2"`, "body-v2")

	deleted, err := c.DeleteIfETag(ctx, "b", "k", `"v1"`)
	if err != nil {
		t.Fatalf("DeleteIfETag: %v", err)
	}
	if deleted {
		t.Fatal("deleted = true, want false: the entry carries a newer ETag")
	}
	meta, found, _ := c.GetMeta(ctx, "b", "k")
	if !found || meta == nil || meta.ETag != `"v2"` {
		t.Fatalf("newer entry disturbed: found=%v meta=%+v", found, meta)
	}
}

// Absent entries are a no-op, not an error.
func TestDeleteIfETag_AbsentIsNoop(t *testing.T) {
	c := newETagTestCache()
	deleted, err := c.DeleteIfETag(context.Background(), "b", "missing", `"v1"`)
	if err != nil {
		t.Fatalf("DeleteIfETag: %v", err)
	}
	if deleted {
		t.Fatal("deleted = true for an absent entry")
	}
}

// The match path writes a tombstone (like DeleteWithMeta), so an in-flight
// stamp-based populate that began before the guarded delete is still blocked.
func TestDeleteIfETag_WritesTombstoneOnMatch(t *testing.T) {
	c := newETagTestCache()
	ctx := context.Background()
	seedEntry(t, c, "b", "k", `"v1"`, "body-v1")

	// A populate "decides" before the delete...
	populateStart := time.Now().UnixNano()

	if deleted, err := c.DeleteIfETag(ctx, "b", "k", `"v1"`); err != nil || !deleted {
		t.Fatalf("DeleteIfETag: deleted=%v err=%v", deleted, err)
	}

	// ...and its tombstone-aware meta write must now be refused.
	meta := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v1"`, StatusCode: 200}
	wrote, err := c.PutMetaTombstoneAware(ctx, "b", "k", meta, 60, populateStart)
	if err != nil {
		t.Fatalf("PutMetaTombstoneAware: %v", err)
	}
	if wrote {
		t.Fatal("stale populate resurrected the entry - guarded delete wrote no tombstone")
	}
}
