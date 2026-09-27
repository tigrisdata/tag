package cache

import (
	"context"
	"errors"
	"fmt"
)

// ErrBlockPresenceUnsupported signals that a peer does not implement the optional
// block-presence exchange. Cache falls back to its single-key probe for that page.
var ErrBlockPresenceUnsupported = errors.New("block presence exchange unsupported")

// ErrBlockPresenceTopologyChanged signals that the owner or ring view changed
// during a batched check. Cache discards the whole page and retries it through the
// normal per-key routing path.
var ErrBlockPresenceTopologyChanged = errors.New("block presence topology changed")

const maxBlockPresenceBatchSize = 32

// blockPresenceClient is an optional CacheClient capability. The result order
// must match keys; errors are never absence. It deliberately does not extend the
// required ocache CacheClient interface.
type blockPresenceClient interface {
	BlockPresence(ctx context.Context, keys []string) ([]bool, error)
}

// BlockExistsBatchErr reports presence for blockIdxs in their original order,
// processing at most maxBlockPresenceBatchSize keys per optional exchange.
// Clients without the optional batch capability, and peers that explicitly do
// not support it, use BlockExistsErr one key at a time. A real batch error is
// returned unchanged so a routing or storage failure cannot become a miss.
func (c *Cache) BlockExistsBatchErr(ctx context.Context, bucket, key, etag string, blockSize int64, blockIdxs []int64) ([]bool, error) {
	present := make([]bool, len(blockIdxs))
	if len(blockIdxs) == 0 || !c.IsEnabled() || etag == "" {
		return present, nil
	}

	for start := 0; start < len(blockIdxs); start += maxBlockPresenceBatchSize {
		end := min(start+maxBlockPresenceBatchSize, len(blockIdxs))
		page, err := c.blockExistsPageErr(ctx, bucket, key, etag, blockSize, blockIdxs[start:end])
		if err != nil {
			return nil, err
		}
		copy(present[start:end], page)
	}
	return present, nil
}

func (c *Cache) blockExistsPageErr(ctx context.Context, bucket, key, etag string, blockSize int64, blockIdxs []int64) ([]bool, error) {
	present := make([]bool, len(blockIdxs))
	keys := make([]string, len(blockIdxs))
	for i, idx := range blockIdxs {
		keys[i] = MakeBlockKey(bucket, key, etag, blockSize, idx)
	}

	if batcher, ok := c.client.(blockPresenceClient); ok {
		batch, err := batcher.BlockPresence(ctx, keys)
		if err == nil {
			if len(batch) != len(keys) {
				return nil, fmt.Errorf("block presence returned %d results for %d keys", len(batch), len(keys))
			}
			for i, found := range batch {
				present[i] = found
				if found {
					c.recordServeLocality(keys[i])
				}
			}
			return present, nil
		}
		if !errors.Is(err, ErrBlockPresenceUnsupported) && !errors.Is(err, ErrBlockPresenceTopologyChanged) {
			return nil, err
		}
	}

	for i, idx := range blockIdxs {
		found, err := c.BlockExistsErr(ctx, bucket, key, etag, blockSize, idx)
		if err != nil {
			return nil, err
		}
		present[i] = found
	}
	return present, nil
}
