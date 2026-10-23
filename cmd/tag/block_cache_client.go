package main

import (
	"context"
	"io"

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

// GetRangeStreamOutcome returns OCache's presence result and the bytes copied to
// the caller. Operations.GetRangeStream discards io.Copy's count, which Cache
// uses to preserve exact-range validation. Repeating its Get, Close, and Copy
// sequence exposes that count without wrapping the caller's writer.
func (c *embeddedBlockCacheClient) GetRangeStreamOutcome(ctx context.Context, key string, start, end int64, w io.Writer) (found bool, written int64, err error) {
	reader, found, err := c.Operations().Get(ctx, key, start, end)
	if err != nil || !found {
		return found, 0, err
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}
	written, err = io.Copy(w, reader)
	return found, written, err
}
