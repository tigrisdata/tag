package cache

import (
	"context"
	"net/http"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

func TestLegacyFenceRefusesLateCommitAfterMarkerEviction(t *testing.T) {
	ctx := context.Background()
	const bucket, key = "eviction-bucket", "deleted-key"
	store := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	cache := NewCacheWithClient(store, &cfg.Cache)
	old := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"old"`, StatusCode: http.StatusOK}
	if err := cache.PutWithMeta(ctx, bucket, key, old, []byte("old body"), 60); err != nil {
		t.Fatalf("seed old metadata: %v", err)
	}

	if err := cache.DeleteWithMeta(ctx, bucket, key); err != nil {
		t.Fatalf("first invalidation: %v", err)
	}
	evict := func(key string) {
		t.Helper()
		if err := store.Delete(ctx, key); err != nil && !isNotFoundError(err) {
			t.Fatalf("simulate disk-cap eviction of %q: %v", key, err)
		}
	}
	generationKey := "meta-gen|" + bucket + "|" + key
	evict(generationKey)
	evict(MakeTombstoneKey(bucket, key))

	// The GET takes its decision after the first successful invalidation, even
	// though cap eviction removed both ordinary cache fence keys.
	_, token, found, err := cache.GetMetaWithVersion(ctx, bucket, key)
	if err != nil || found {
		t.Fatalf("post-first-fence decision=(found=%t, err=%v), want metadata miss", found, err)
	}

	if err := cache.DeleteWithMeta(ctx, bucket, key); err != nil {
		t.Fatalf("second invalidation: %v", err)
	}
	evict(generationKey)
	evict(MakeTombstoneKey(bucket, key))

	stale := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"old-refill"`, StatusCode: http.StatusOK}
	wrote, err := cache.PutMetaIfVersion(ctx, bucket, key, stale, 60, token)
	if err != nil {
		t.Fatalf("late old metadata commit: %v", err)
	}
	if wrote {
		t.Fatal("late old metadata survived sidecar eviction")
	}
	if meta, found, err := cache.GetMeta(ctx, bucket, key); err != nil || found || meta != nil {
		t.Fatalf("current read=(%+v, found=%t, err=%v), want a miss", meta, found, err)
	}
}
