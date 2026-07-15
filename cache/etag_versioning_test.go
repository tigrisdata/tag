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

// Objects with no ETag fall back to the unversioned body key and round-trip.
func TestETagVersionedBody_EmptyETagFallsBackToFixedKey(t *testing.T) {
	c, mem := newVersioningTestCache(t)
	ctx := context.Background()
	bucket, key := "b", "k"

	meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: "", ContentLength: 4, StatusCode: 200}
	if err := c.PutWithMeta(ctx, bucket, key, meta, []byte("body"), 60); err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}
	if _, err := mem.Get(ctx, MakeBodyKey(bucket, key, "")); err != nil {
		t.Errorf("empty-ETag body not stored at fixed key: %v", err)
	}
	var buf bytes.Buffer
	if err := c.GetBodyStream(ctx, bucket, key, "", &buf); err != nil || buf.String() != "body" {
		t.Errorf("read empty-ETag body: err=%v body=%q", err, buf.String())
	}
}
