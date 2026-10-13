package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

type blockPresenceTestClient struct {
	cacheclient.CacheClient
	result     []bool
	batchErr   error
	batchKeys  []string
	pageSizes  []int
	probeCalls int
	localKeys  []string
}

func (c *blockPresenceTestClient) BlockPresence(_ context.Context, keys []string) ([]bool, error) {
	start := len(c.batchKeys)
	c.batchKeys = append(c.batchKeys, keys...)
	c.pageSizes = append(c.pageSizes, len(keys))
	if c.batchErr != nil {
		return nil, c.batchErr
	}
	end := start + len(keys)
	if end > len(c.result) {
		return append([]bool(nil), c.result[start:]...), nil
	}
	return append([]bool(nil), c.result[start:end]...), nil
}

func (c *blockPresenceTestClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if strings.HasPrefix(key, blockKeyPrefix) && start == 0 && end == 1 {
		c.probeCalls++
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

func (c *blockPresenceTestClient) IsLocal(key string) bool {
	c.localKeys = append(c.localKeys, key)
	return true
}

func newBlockPresenceTestCache(client cacheclient.CacheClient) *Cache {
	cfg := config.NewDefault()
	return NewCacheWithClient(client, &cfg.Cache)
}

func TestBlockExistsBatchErr_OptionalClientPreservesOrderAndLocality(t *testing.T) {
	client := &blockPresenceTestClient{
		CacheClient: cacheclient.NewMemoryCache(),
		result:      []bool{false, true, true},
	}
	c := newBlockPresenceTestCache(client)

	got, err := c.BlockExistsBatchErr(context.Background(), "b", "k", `"v1"`, 4, []int64{3, 1, 2})
	if err != nil {
		t.Fatalf("BlockExistsBatchErr: %v", err)
	}
	if want := []bool{false, true, true}; !reflect.DeepEqual(got, want) {
		t.Fatalf("presence = %v, want %v", got, want)
	}
	wantKeys := []string{
		MakeBlockKey("b", "k", `"v1"`, 4, 3),
		MakeBlockKey("b", "k", `"v1"`, 4, 1),
		MakeBlockKey("b", "k", `"v1"`, 4, 2),
	}
	if !reflect.DeepEqual(client.batchKeys, wantKeys) {
		t.Fatalf("batch keys = %v, want %v", client.batchKeys, wantKeys)
	}
	if !reflect.DeepEqual(client.localKeys, wantKeys[1:]) {
		t.Fatalf("locality checks = %v, want present keys %v", client.localKeys, wantKeys[1:])
	}
	if client.probeCalls != 0 {
		t.Fatalf("batch path opened %d single-key streams, want 0", client.probeCalls)
	}
}

func TestBlockExistsBatchErr_FallsBackForMissingCapabilityAndUnsupportedPeer(t *testing.T) {
	for _, tc := range []struct {
		name      string
		batchErr  error
		wantBatch int
	}{
		{name: "no capability"},
		{name: "old peer", batchErr: fmt.Errorf("peer response: %w", ErrBlockPresenceUnsupported), wantBatch: 3},
		{name: "owner changed", batchErr: fmt.Errorf("peer response: %w", ErrBlockPresenceTopologyChanged), wantBatch: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := cacheclient.NewMemoryCache()
			presentKey := MakeBlockKey("b", "k", `"v1"`, 4, 0)
			if err := mem.Put(context.Background(), presentKey, []byte("AB"), 0); err != nil {
				t.Fatal(err)
			}

			var wrapped cacheclient.CacheClient = mem
			var probeClient *blockPresenceTestClient
			if tc.batchErr != nil {
				probeClient = &blockPresenceTestClient{CacheClient: mem, batchErr: tc.batchErr}
				wrapped = probeClient
			}
			c := newBlockPresenceTestCache(wrapped)
			got, err := c.BlockExistsBatchErr(context.Background(), "b", "k", `"v1"`, 4, []int64{2, 0, 1})
			if err != nil {
				t.Fatalf("BlockExistsBatchErr: %v", err)
			}
			if want := []bool{false, true, false}; !reflect.DeepEqual(got, want) {
				t.Fatalf("presence = %v, want %v", got, want)
			}
			if probeClient != nil {
				if probeClient.probeCalls != 3 {
					t.Fatalf("fallback single-key probes = %d, want 3", probeClient.probeCalls)
				}
				if got := len(probeClient.batchKeys); got != tc.wantBatch {
					t.Fatalf("batch key count = %d, want %d", got, tc.wantBatch)
				}
			}
		})
	}
}

func TestBlockExistsBatchErr_PropagatesRealBatchFailure(t *testing.T) {
	failure := errors.New("owner storage unavailable")
	client := &blockPresenceTestClient{
		CacheClient: cacheclient.NewMemoryCache(),
		batchErr:    failure,
	}
	c := newBlockPresenceTestCache(client)

	_, err := c.BlockExistsBatchErr(context.Background(), "b", "k", `"v1"`, 4, []int64{0, 1})
	if !errors.Is(err, failure) {
		t.Fatalf("BlockExistsBatchErr error = %v, want %v", err, failure)
	}
	if client.probeCalls != 0 {
		t.Fatalf("real batch failure fell back to %d single-key probes", client.probeCalls)
	}
}

func TestBlockExistsBatchErr_PagesOversizedInput(t *testing.T) {
	client := &blockPresenceTestClient{
		CacheClient: cacheclient.NewMemoryCache(),
		result:      make([]bool, maxBlockPresenceBatchSize+1),
	}
	for i := range client.result {
		client.result[i] = true
	}
	c := newBlockPresenceTestCache(client)
	indices := make([]int64, len(client.result))
	for i := range indices {
		indices[i] = int64(i)
	}

	got, err := c.BlockExistsBatchErr(context.Background(), "b", "k", `"v1"`, 4, indices)
	if err != nil {
		t.Fatalf("BlockExistsBatchErr: %v", err)
	}
	if len(got) != len(indices) {
		t.Fatalf("presence result count = %d, want %d", len(got), len(indices))
	}
	for i, present := range got {
		if !present {
			t.Errorf("presence[%d] = false, want true", i)
		}
	}
	if want := []int{maxBlockPresenceBatchSize, 1}; !reflect.DeepEqual(client.pageSizes, want) {
		t.Fatalf("batch page sizes = %v, want %v", client.pageSizes, want)
	}
}

func TestBlockExistsBatchErr_RejectsMalformedBatchResult(t *testing.T) {
	client := &blockPresenceTestClient{
		CacheClient: cacheclient.NewMemoryCache(),
		result:      []bool{true},
	}
	c := newBlockPresenceTestCache(client)

	_, err := c.BlockExistsBatchErr(context.Background(), "b", "k", `"v1"`, 4, []int64{0, 1})
	if err == nil {
		t.Fatal("BlockExistsBatchErr accepted a short result")
	}
	if client.probeCalls != 0 {
		t.Fatalf("malformed result fell back to %d single-key probes", client.probeCalls)
	}
}
