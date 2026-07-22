package cache

import (
	"context"
	"errors"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

// flakyClient wraps a real client and can inject failures into the two backend
// operations DeleteWithMeta performs: Put (tombstone) and Delete (metadata).
type flakyClient struct {
	cacheclient.CacheClient
	putErr    error // returned by Put (tombstone write) when non-nil
	deleteErr error // returned by Delete (metadata delete) when non-nil
}

func (f *flakyClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	if f.putErr != nil {
		return f.putErr
	}
	return f.CacheClient.Put(ctx, key, data, ttlSeconds)
}

func (f *flakyClient) Delete(ctx context.Context, key string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return f.CacheClient.Delete(ctx, key)
}

func newCacheWithClientForTest(t *testing.T, client cacheclient.CacheClient) *Cache {
	t.Helper()
	cfg := config.NewDefault()
	return NewCacheWithClient(client, &cfg.Cache)
}

// A backend failure of either step must be reported so a caller cannot record a
// successful invalidation while stale metadata is still readable.
func TestDeleteWithMeta_PropagatesBackendFailures(t *testing.T) {
	backendDown := errors.New("backend unavailable")

	t.Run("tombstone write fails", func(t *testing.T) {
		c := newCacheWithClientForTest(t, &flakyClient{CacheClient: cacheclient.NewMemoryCache(), putErr: backendDown})
		if err := c.DeleteWithMeta(context.Background(), "b", "k"); err == nil {
			t.Error("DeleteWithMeta returned nil when the tombstone write failed — invalidation was not actually complete")
		}
	})

	t.Run("metadata delete fails", func(t *testing.T) {
		c := newCacheWithClientForTest(t, &flakyClient{CacheClient: cacheclient.NewMemoryCache(), deleteErr: backendDown})
		if err := c.DeleteWithMeta(context.Background(), "b", "k"); err == nil {
			t.Error("DeleteWithMeta returned nil when the metadata delete failed — stale metadata may remain readable")
		}
	})
}

// A healthy invalidation of a present entry succeeds and actually removes the meta.
func TestDeleteWithMeta_SuccessReturnsNil(t *testing.T) {
	c := newCacheWithClientForTest(t, cacheclient.NewMemoryCache())
	ctx := context.Background()

	meta := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v1"`, ContentLength: 2, StatusCode: 200}
	if err := c.PutWithMeta(ctx, "b", "k", meta, []byte("v1"), 60); err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}
	if err := c.DeleteWithMeta(ctx, "b", "k"); err != nil {
		t.Errorf("DeleteWithMeta of a healthy entry returned %v, want nil", err)
	}
	if _, found, _ := c.GetMeta(ctx, "b", "k"); found {
		t.Error("metadata still present after a successful DeleteWithMeta")
	}
}

// Deleting metadata that is already gone is a successful invalidation, not a failure:
// the goal state (no cached meta) already holds.
func TestDeleteWithMeta_NotFoundIsSuccess(t *testing.T) {
	c := newCacheWithClientForTest(t, &flakyClient{
		CacheClient: cacheclient.NewMemoryCache(),
		deleteErr:   errors.New("key not found"),
	})
	if err := c.DeleteWithMeta(context.Background(), "b", "missing"); err != nil {
		t.Errorf("DeleteWithMeta returned %v for an already-absent entry, want nil (not-found is success)", err)
	}
}
