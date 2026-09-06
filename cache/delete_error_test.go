package cache

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

// flakyClient wraps a real client and can inject failures into the two backend
// operations DeleteWithMeta performs: Put (tombstone) and Delete (metadata).
type flakyClient struct {
	cacheclient.CacheClient
	putErr    error // returned by Put (tombstone write) when non-nil
	deleteErr error // returned by Delete (metadata delete) when non-nil
	getErr    error // returned by the next Get when non-nil
}

func (f *flakyClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	if f.putErr != nil {
		return f.putErr
	}
	return f.CacheClient.Put(ctx, key, data, ttlSeconds)
}

func (f *flakyClient) Get(ctx context.Context, key string) ([]byte, error) {
	if f.getErr != nil {
		err := f.getErr
		f.getErr = nil
		return nil, err
	}
	return f.CacheClient.Get(ctx, key)
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

// A tombstone order already issued by this Cache must remain the fence without
// another backend read. This keeps invalidations for unrelated keys independent
// of tombstone-read latency or failure.
func TestDeleteWithOrder_RetainsLocalFenceWithoutTombstoneRead(t *testing.T) {
	ctx := context.Background()
	backendDown := errors.New("tombstone read unavailable")
	client := &flakyClient{CacheClient: cacheclient.NewMemoryCache()}
	c := newCacheWithClientForTest(t, client)

	meta := &CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"v1"`, ContentLength: 3, StatusCode: 200}
	if err := c.PutWithMeta(ctx, "b", "k", meta, []byte("old"), 60); err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}
	if err := c.WriteTombstoneWithOrder(ctx, "b", "k", 9); err != nil {
		t.Fatalf("seed tombstone: %v", err)
	}

	writeStart := time.Now().UnixNano()
	client.getErr = backendDown
	if err := c.DeleteWithOrder(ctx, "b", "k", 3); err != nil {
		t.Fatalf("DeleteWithOrder: %v", err)
	}
	// The injected read failure is still armed because WriteTombstoneWithOrder
	// uses the local order table. Clear it before the read-side assertions.
	client.getErr = nil
	if _, found, err := c.GetMeta(ctx, "b", "k"); err != nil || found {
		t.Fatalf("metadata after invalidation: found=%v err=%v", found, err)
	}
	if got := c.GetTombstoneOrder(ctx, "b", "k"); got != 9 {
		t.Fatalf("replacement tombstone order = %d, want 9", got)
	}

	wrote, err := c.PutWithMetaStreamTombstoneAware(
		ctx,
		"b",
		"k",
		&CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"stale"`, ContentLength: 3, StatusCode: 200},
		bytes.NewReader([]byte("old")),
		60,
		writeStart,
	)
	if err != nil {
		t.Fatalf("stale populate: %v", err)
	}
	if wrote {
		t.Fatal("stale populate bypassed the replacement tombstone fence")
	}
	if _, found, _ := c.GetMeta(ctx, "b", "k"); found {
		t.Fatal("stale populate published metadata after invalidation")
	}
}

func TestWriteTombstoneWithOrder_EvictedOrderReadsDurableFence(t *testing.T) {
	ctx := context.Background()
	client := cacheclient.NewMemoryCache()
	c := newCacheWithClientForTest(t, client)
	const bucket = "eviction-bucket"

	if err := c.WriteTombstoneWithOrder(ctx, bucket, "retained", 9); err != nil {
		t.Fatalf("seed tombstone: %v", err)
	}
	for i := 0; i < tombstoneOrderCacheCapacity; i++ {
		if err := c.WriteTombstoneWithOrder(ctx, bucket, "other-"+strconv.Itoa(i), uint64(i+1)); err != nil {
			t.Fatalf("fill tombstone order cache at %d: %v", i, err)
		}
	}
	if err := c.WriteTombstoneWithOrder(ctx, bucket, "retained", 3); err != nil {
		t.Fatalf("rewrite evicted tombstone: %v", err)
	}
	if got := c.GetTombstoneOrder(ctx, bucket, "retained"); got != 9 {
		t.Fatalf("evicted tombstone order = %d, want 9", got)
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
