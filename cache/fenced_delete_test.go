package cache

import (
	"context"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

// Absent reads carry a usable token since ocache v1.13.0 — never squash it.
func TestGetMetaWithVersion_AbsentCarriesToken(t *testing.T) {
	c, _ := newVersionedTestCache()
	_, tok, found, err := c.GetMetaWithVersion(context.Background(), "b", "never-written")
	if err != nil || found {
		t.Fatalf("absent read: found=%v err=%v", found, err)
	}
	if tok == 0 {
		t.Fatal("absent read returned token 0: the absence token was squashed")
	}
}

// The race TAG's tombstones existed for, closed by the fence alone: a populate
// observes absence, a fenced delete lands, and the populate's token-carrying
// commit must lose. No TAG tombstone is written anywhere in this test.
func TestFencedDelete_BlocksPopulateThatObservedPreDeleteAbsence(t *testing.T) {
	c, mem := newVersionedTestCache()
	ctx := context.Background()

	// Populate observes absence and captures the token.
	_, tok, found, err := c.GetMetaWithVersion(ctx, "b", "k")
	if err != nil || found {
		t.Fatalf("absent read: found=%v err=%v", found, err)
	}

	// A fenced delete lands after the observation (an invalidation of a key
	// the deleter believes may exist).
	if err := c.deleteMetaFenced(ctx, "b", "k"); err != nil {
		t.Fatalf("deleteMetaFenced: %v", err)
	}

	// The populate commits with its pre-delete token: the fence must refuse it.
	meta := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"pre-delete"`, StatusCode: 200}
	wrote, err := c.PutMetaTombstoneAware(ctx, "b", "k", meta, 60, time.Now().UnixNano(), tok)
	if err != nil {
		t.Fatalf("PutMetaTombstoneAware: %v", err)
	}
	if wrote {
		t.Fatal("populate holding a pre-delete absence token committed over the fence")
	}
	if _, _, f, _ := mem.GetWithVersion(ctx, MakeMetaKey("b", "k")); f {
		t.Fatal("entry present after refused commit")
	}
}

// The double-invalidation pattern in the fence domain: a refill that read the
// first fence's token and fetched (possibly pre-commit) bytes must lose to the
// second fence.
func TestFencedDelete_SecondFenceBlocksRefillHoldingFirst(t *testing.T) {
	c, _ := newVersionedTestCache()
	ctx := context.Background()

	// First invalidation (pre-forward).
	if err := c.deleteMetaFenced(ctx, "b", "k"); err != nil {
		t.Fatalf("first fence: %v", err)
	}
	// Refill reads the fence token and goes to fetch.
	_, tok1, found, _ := c.GetMetaWithVersion(ctx, "b", "k")
	if found {
		t.Fatal("key unexpectedly present")
	}
	// Second invalidation (post-confirm) bumps the fence.
	if err := c.deleteMetaFenced(ctx, "b", "k"); err != nil {
		t.Fatalf("second fence: %v", err)
	}
	// The refill's commit with the first token must mismatch.
	meta := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"pre-commit"`, StatusCode: 200}
	wrote, err := c.PutMetaTombstoneAware(ctx, "b", "k", meta, 60, time.Now().UnixNano(), tok1)
	if err != nil {
		t.Fatalf("PutMetaTombstoneAware: %v", err)
	}
	if wrote {
		t.Fatal("refill holding the first fence's token committed over the second fence")
	}
}

// The fenced delete's retry loop removes a LIVE key: DeleteIfVersion(0)
// reports the live version, and the retry deletes exactly that version.
func TestDeleteMetaFenced_RemovesLiveKey(t *testing.T) {
	cfg := config.NewDefault()
	mem := cacheclient.NewMemoryCache()
	c := NewCacheWithClient(mem, &cfg.Cache)
	ctx := context.Background()
	seedEntry(t, c, "b", "k", `"live"`, "body")

	if err := c.deleteMetaFenced(ctx, "b", "k"); err != nil {
		t.Fatalf("deleteMetaFenced over live key: %v", err)
	}
	if _, found, _ := c.GetMeta(ctx, "b", "k"); found {
		t.Fatal("live key survived the fenced delete")
	}
	// And it left a fence: a pre-delete token must not recreate.
	// (The live version the seed had is such a token.)
}
