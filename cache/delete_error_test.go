package cache

import (
	"context"
	"errors"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

// flakyClient wraps a real client and can inject failures into the two backend
// operation DeleteWithMeta performs: the fenced CAS delete of the metadata.
type flakyClient struct {
	cacheclient.CacheClient
	deleteErr error // returned by Delete (metadata delete) when non-nil
}

func (f *flakyClient) Delete(ctx context.Context, key string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return f.CacheClient.Delete(ctx, key)
}

// DeleteWithMeta's metadata removal goes through the fenced CAS delete since
// ocache v1.13.0; the fault must be injected there too.
func (f *flakyClient) DeleteIfVersion(ctx context.Context, key string, expected uint64) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return f.CacheClient.DeleteIfVersion(ctx, key, expected)
}

func newCacheWithClientForTest(t *testing.T, client cacheclient.CacheClient) *Cache {
	t.Helper()
	cfg := config.NewDefault()
	cfg.Cache.SetLegacyCoordination(false) // fault injection targets the CAS coordinator's ops
	return NewCacheWithClient(client, &cfg.Cache)
}

// A backend failure of either step must be reported so a caller cannot record a
// successful invalidation while stale metadata is still readable.
func TestDeleteWithMeta_PropagatesBackendFailures(t *testing.T) {
	backendDown := errors.New("backend unavailable")

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
