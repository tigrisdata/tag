package main

import (
	"context"

	"github.com/tigrisdata/ocache/embedded"
	"github.com/tigrisdata/tag/cache"
)

// embeddedBlockCacheClient keeps embedded.Client's normal CacheClient behavior
// while providing a byte-slice write for blocks whose owner is remote. Local
// writes retain the embedded streaming path because they already write directly
// to local storage.
type embeddedBlockCacheClient struct {
	*embedded.Client
}

func newEmbeddedBlockCacheClient(client *embedded.Client) *embeddedBlockCacheClient {
	return &embeddedBlockCacheClient{Client: client}
}

// PutBlockBytes writes a fully staged block to its remote owner in one unary
// request. Cache calls this only after the request-size guard has accepted the
// block. Returning handled=false leaves local writes on embedded.Client's
// existing path.
func (c *embeddedBlockCacheClient) PutBlockBytes(ctx context.Context, key string, data []byte, ttlSeconds int64) (handled bool, err error) {
	return cache.PutRemoteBlockBytes(ctx, c.Operations(), key, data, ttlSeconds)
}

// IsOverBudget tells the cache layer that a PutStreamSheddable error (promoted
// from embedded.Client) is the owning node's over-budget refusal, which the
// cache reports as cache.ErrPopulateShed. See cache.OverBudgetClassifier.
func (c *embeddedBlockCacheClient) IsOverBudget(err error) bool {
	return embedded.IsOverBudget(err)
}

// DiskUsageBytes reports the engine's tracked local store size (the same
// total behind ocache_disk_usage_ratio), so the proxy can shed populates when
// eviction has fallen behind the disk cap. See cache.DiskUsageReporter.
func (c *embeddedBlockCacheClient) DiskUsageBytes() int64 {
	if s := c.Storage(); s != nil {
		return s.TotalSize()
	}
	return 0
}
