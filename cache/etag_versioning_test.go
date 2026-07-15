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

// Overwriting a key with a new ETag eagerly evicts the superseded body.
func TestETagVersionedBody_EagerEvictionOnOverwrite(t *testing.T) {
	c, _ := newVersioningTestCache(t)
	ctx := context.Background()
	bucket, key := "b", "k"

	meta1 := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"v1"`, ContentLength: 2, StatusCode: 200}
	_ = c.PutWithMeta(ctx, bucket, key, meta1, []byte("v1"), 60)
	meta2 := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"v2"`, ContentLength: 2, StatusCode: 200}
	_ = c.PutWithMeta(ctx, bucket, key, meta2, []byte("v2"), 60)

	// Superseded v1 body is gone.
	var buf bytes.Buffer
	if err := c.GetBodyStream(ctx, bucket, key, `"v1"`, &buf); err != ErrNotFound {
		t.Errorf("v1 body after overwrite: err=%v, want ErrNotFound (evicted)", err)
	}
	// Current v2 body is served.
	buf.Reset()
	if err := c.GetBodyStream(ctx, bucket, key, `"v2"`, &buf); err != nil || buf.String() != "v2" {
		t.Errorf("v2 body: err=%v body=%q", err, buf.String())
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
