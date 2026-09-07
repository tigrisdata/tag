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

// raceOnReadClient injects a concurrent replacement at the worst instant: the
// versioned read has returned (ETag comparison will pass against the stale
// snapshot), and the replacement lands before the delete runs.
type raceOnReadClient struct {
	cacheclient.CacheClient
	raced   bool
	replace func()
}

func (r *raceOnReadClient) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	data, version, found, err := r.CacheClient.GetWithVersion(ctx, key)
	if !r.raced && found && r.replace != nil {
		r.raced = true
		r.replace()
	}
	return data, version, found, err
}

// The guard's central guarantee, exercised in the exact window it protects: a
// version-stamped replacement landing between the versioned read and the
// delete must survive. The old compare-then-delete would have compared against
// the stale snapshot, passed, and deleted the new entry; only the version
// condition on the delete itself catches this.
func TestDeleteIfETag_VersionedReplacementInsideWindowSurvives(t *testing.T) {
	mem := cacheclient.NewMemoryCache()
	wrapper := &raceOnReadClient{CacheClient: mem}
	cfg := config.NewDefault()
	c := NewCacheWithClient(wrapper, &cfg.Cache)
	ctx := context.Background()

	seedEntry(t, c, "b", "k", `"v1"`, "body-v1")
	wrapper.replace = func() {
		// A CAS writer replaces the entry: the version bumps past the snapshot
		// the delete is holding. Plain-written rows read as the legacy version
		// (1), so that is the expectation the racer swaps against.
		v2 := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v2"`, StatusCode: 200}
		encoded, err := v2.Encode()
		if err != nil {
			t.Errorf("encode v2: %v", err)
			return
		}
		if _, err := mem.PutIfVersion(ctx, MakeMetaKey("b", "k"), encoded, 0, 1); err != nil {
			t.Errorf("versioned replacement: %v", err)
		}
	}

	deleted, err := c.DeleteIfETag(ctx, "b", "k", `"v1"`)
	if err != nil {
		t.Fatalf("DeleteIfETag: %v", err)
	}
	if !wrapper.raced {
		t.Fatal("test wiring: the replacement was never injected")
	}
	if deleted {
		t.Fatal("deleted = true: the guarded delete claimed a win over a newer entry")
	}
	meta, found, _ := c.GetMeta(ctx, "b", "k")
	if !found || meta == nil || meta.ETag != `"v2"` {
		t.Fatalf("versioned replacement inside the CAS window was deleted: found=%v meta=%+v", found, meta)
	}
}

// KNOWN LIMITATION, pinned as a tripwire: a PLAIN-put replacement inside the
// window is still deleted, because a plain Put resets the row to the legacy
// version (storage EffectiveRowVersion semantics) — version equality carries
// no information between plain writes. Today every populate path writes meta
// with a plain Put, so for those racers this guard equals the previous
// compare-then-delete: the same window as before, never wider. Moving the
// populate paths to version-stamped writes closes it; when that lands, this
// test MUST flip to assert the replacement survives.
func TestDeleteIfETag_PlainReplacementInsideWindowIsStillDeleted(t *testing.T) {
	wrapper := &raceOnReadClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	c := NewCacheWithClient(wrapper, &cfg.Cache)
	ctx := context.Background()

	seedEntry(t, c, "b", "k", `"v1"`, "body-v1")
	wrapper.replace = func() {
		seedEntry(t, c, "b", "k", `"v2"`, "body-v2")
	}

	deleted, err := c.DeleteIfETag(ctx, "b", "k", `"v1"`)
	if err != nil {
		t.Fatalf("DeleteIfETag: %v", err)
	}
	if !wrapper.raced {
		t.Fatal("test wiring: the replacement was never injected")
	}
	if !deleted {
		t.Fatal("plain-put replacement survived the window: the populate paths " +
			"now stamp versions - flip this test to assert survival")
	}
}
