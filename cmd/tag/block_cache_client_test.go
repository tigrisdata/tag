package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/tigrisdata/ocache/embedded"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

func TestEmbeddedBlockCacheClientRangeStreamOutcome(t *testing.T) {
	cfg := config.NewDefault()
	cfg.Cache.DiskPath = t.TempDir()
	cfg.Cache.SetBlockCachingEnabled(false)
	embeddedCache, err := embedded.New(&embedded.Config{DiskPath: cfg.Cache.DiskPath, TTL: cfg.Cache.TTL})
	if err != nil {
		t.Fatalf("create embedded cache: %v", err)
	}
	t.Cleanup(func() { _ = embeddedCache.Close() })

	client := newEmbeddedBlockCacheClient(embeddedCache)
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	ctx := context.Background()
	bucket, key, etag := "bucket", "key", `"v1"`

	var missing bytes.Buffer
	found, written, err := client.GetRangeStreamOutcome(ctx, cache.MakeBodyKey(bucket, "missing", etag), 1, 3, &missing)
	if err != nil || found || written != 0 || missing.Len() != 0 {
		t.Fatalf("missing outcome = (found=%t, written=%d, body=%q, err=%v), want (false, 0, empty, nil)", found, written, missing.String(), err)
	}
	if err := store.GetRangeStream(ctx, bucket, "missing", etag, 1, 3, &missing); !errors.Is(err, cache.ErrNotFound) || missing.Len() != 0 {
		t.Fatalf("cache missing read = (body=%q, err=%v), want empty ErrNotFound", missing.String(), err)
	}

	body := []byte("abcdef")
	meta := &cache.CachedObjectMeta{
		Bucket: bucket, Key: key, ETag: etag,
		ContentType: "text/plain", ContentLength: int64(len(body)), StatusCode: http.StatusOK,
	}
	if err := store.PutWithMeta(ctx, bucket, key, meta, body, 0); err != nil {
		t.Fatalf("seed embedded body: %v", err)
	}

	var direct bytes.Buffer
	found, written, err = client.GetRangeStreamOutcome(ctx, cache.MakeBodyKey(bucket, key, etag), 1, 3, &direct)
	if err != nil || !found || written != 3 || direct.String() != "bcd" {
		t.Fatalf("direct outcome = (found=%t, written=%d, body=%q, err=%v), want (true, 3, %q, nil)", found, written, direct.String(), err, "bcd")
	}

	var rangeBody bytes.Buffer
	if err := store.GetRangeStream(ctx, bucket, key, etag, 1, 3, &rangeBody); err != nil || rangeBody.String() != "bcd" {
		t.Fatalf("Cache.GetRangeStream = (body=%q, err=%v), want %q, nil", rangeBody.String(), err, "bcd")
	}
	rangeBody.Reset()
	if err := store.GetRangeStream(ctx, bucket, key, etag, int64(len(body)), int64(len(body))+1, &rangeBody); !errors.Is(err, cache.ErrNotFound) || rangeBody.Len() != 0 {
		t.Fatalf("empty successful read = (body=%q, err=%v), want empty ErrNotFound", rangeBody.String(), err)
	}
	rangeBody.Reset()
	if err := store.GetRangeStream(ctx, bucket, key, etag, 0, 0, &rangeBody); err != nil || rangeBody.String() != "a" {
		t.Fatalf("position-zero range = (body=%q, err=%v), want %q, nil", rangeBody.String(), err, "a")
	}

	blockSize := int64(len(body))
	blockKey := cache.MakeBlockKey(bucket, key, etag, blockSize, 0)
	if err := client.Put(ctx, blockKey, body, 60); err != nil {
		t.Fatalf("seed embedded block: %v", err)
	}
	rangeBody.Reset()
	if err := store.GetBlockRangeStream(ctx, bucket, key, etag, blockSize, 0, 1, 3, &rangeBody); err != nil || rangeBody.String() != "bcd" {
		t.Fatalf("Cache.GetBlockRangeStream = (body=%q, err=%v), want %q, nil", rangeBody.String(), err, "bcd")
	}
}
