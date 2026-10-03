package cache

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	cacheclient "github.com/tigrisdata/ocache/client"
	ocachecoordinator "github.com/tigrisdata/ocache/coordinator"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
)

type tombstoneGetOverrideClient struct {
	cacheclient.CacheClient
	markerKey string
	data      []byte
	err       error
	override  bool
}

func (c *tombstoneGetOverrideClient) Get(ctx context.Context, key string) ([]byte, error) {
	if c.override && key == c.markerKey {
		return c.data, c.err
	}
	return c.CacheClient.Get(ctx, key)
}

func TestLegacyCoordinator_TombstoneReadErrorAndMalformedMarkerRefuseMetaWrite(t *testing.T) {
	const bucket, key = "b", "k"
	markerKey := MakeTombstoneKey(bucket, key)
	backendErr := errors.New("cache read unavailable")
	routingErr := ocachecoordinator.NewNodeNotFoundError("missing-node", markerKey)
	tests := []struct {
		name          string
		data          []byte
		err           error
		override      bool
		wantCause     error
		wantErrorText string
	}{
		{
			name:      "backend read failure",
			err:       backendErr,
			override:  true,
			wantCause: backendErr,
		},
		{
			name:      "routing error containing not found",
			err:       routingErr,
			override:  true,
			wantCause: ocachecoordinator.ErrNodeNotFound,
		},
		{
			name:          "empty marker is malformed",
			data:          make([]byte, 0),
			override:      true,
			wantErrorText: "malformed tombstone",
		},
		{
			name:          "short marker is malformed",
			data:          []byte{1, 2, 3},
			override:      true,
			wantErrorText: "malformed tombstone",
		},
		{
			name:          "long marker is malformed",
			data:          make([]byte, 9),
			override:      true,
			wantErrorText: "malformed tombstone",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.NewDefault()
			cfg.Cache.SetLegacyCoordination(true)
			client := &tombstoneGetOverrideClient{
				CacheClient: cacheclient.NewMemoryCache(),
				markerKey:   markerKey,
				data:        tc.data,
				err:         tc.err,
				override:    tc.override,
			}
			c := NewCacheWithClient(client, &cfg.Cache)
			ctx := context.Background()

			old, expected, found, err := c.GetMetaWithVersion(ctx, bucket, key)
			if err != nil || found || old != nil || expected == 0 {
				t.Fatalf("decision token = (meta=%+v expected=%d found=%v err=%v), want absent metadata and a nonzero token", old, expected, found, err)
			}

			meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"stale"`, StatusCode: 200}
			metaPutErrorsBefore := testutil.ToFloat64(metrics.CacheOperations.WithLabelValues("meta_put", "error"))
			wrote, writeErr := c.PutMetaIfVersion(ctx, bucket, key, meta, 60, expected)
			if got := testutil.ToFloat64(metrics.CacheOperations.WithLabelValues("meta_put", "error")); got != metaPutErrorsBefore+1 {
				t.Errorf("meta_put/error count = %v, want one additional error outcome", got)
			}
			if wrote {
				t.Fatal("metadata was published despite an unreadable or malformed tombstone")
			}
			if writeErr == nil {
				t.Fatal("PutMetaIfVersion returned nil error for an unreadable or malformed tombstone")
			}
			if tc.wantCause != nil && !errors.Is(writeErr, tc.wantCause) {
				t.Fatalf("PutMetaIfVersion error %v does not preserve cause %v", writeErr, tc.wantCause)
			}
			if tc.wantErrorText != "" && !strings.Contains(writeErr.Error(), tc.wantErrorText) {
				t.Fatalf("PutMetaIfVersion error = %v, want text %q", writeErr, tc.wantErrorText)
			}
			if _, found, err := c.GetMeta(ctx, bucket, key); err != nil || found {
				t.Fatalf("metadata after refused write: found=%v err=%v, want absent", found, err)
			}
		})
	}
}

func TestLegacyCoordinator_TombstoneAbsenceStillAllowsMetaWarm(t *testing.T) {
	const bucket, key = "b", "k"
	for _, tc := range []struct {
		name     string
		override bool
	}{
		{name: "typed not found from cache client"},
		{name: "embedded nil result", override: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.NewDefault()
			cfg.Cache.SetLegacyCoordination(true)
			client := &tombstoneGetOverrideClient{
				CacheClient: cacheclient.NewMemoryCache(),
				markerKey:   MakeTombstoneKey(bucket, key),
				override:    tc.override,
			}
			c := NewCacheWithClient(client, &cfg.Cache)
			ctx := context.Background()

			_, expected, found, err := c.GetMetaWithVersion(ctx, bucket, key)
			if err != nil || found || expected == 0 {
				t.Fatalf("decision token = (expected=%d found=%v err=%v), want an absent key and nonzero token", expected, found, err)
			}

			meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"current"`, StatusCode: 200}
			metaPutErrorsBefore := testutil.ToFloat64(metrics.CacheOperations.WithLabelValues("meta_put", "error"))
			wrote, err := c.PutMetaIfVersion(ctx, bucket, key, meta, 60, expected)
			if got := testutil.ToFloat64(metrics.CacheOperations.WithLabelValues("meta_put", "error")); got != metaPutErrorsBefore {
				t.Errorf("meta_put/error count = %v, want no additional error outcome for a genuinely absent tombstone", got)
			}
			if err != nil || !wrote {
				t.Fatalf("PutMetaIfVersion on absent tombstone = (%v, %v), want (true, nil)", wrote, err)
			}
			got, found, err := c.GetMeta(ctx, bucket, key)
			if err != nil || !found || got == nil || got.ETag != meta.ETag {
				t.Fatalf("metadata after warm = (%+v, %v, %v), want current ETag", got, found, err)
			}
		})
	}
}
