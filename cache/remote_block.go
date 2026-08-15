package cache

import (
	"context"
	"fmt"

	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
)

// RemoteBlockRouter is the embedded-cache routing surface used by
// PutRemoteBlockBytes. It is satisfied by ocache's operations layer.
type RemoteBlockRouter interface {
	IsLocal(key string) bool
	GetLocalNodeID() string
	Route(key string) (pb.CacheServiceClient, error)
}

// PutRemoteBlockBytes writes a fully staged block to its remote owner in one
// unary request. It returns handled=false for local or oversized requests so
// callers can retain their streaming write path.
func PutRemoteBlockBytes(ctx context.Context, router RemoteBlockRouter, key string, data []byte, ttlSeconds int64) (handled bool, err error) {
	if !canPutBlockUnary(key, data, ttlSeconds) || router.IsLocal(key) {
		return false, nil
	}

	ctx, err = coordinator.IncrementHopCount(ctx, router.GetLocalNodeID())
	if err != nil {
		return true, err
	}
	client, err := router.Route(key)
	if err != nil {
		return true, err
	}
	resp, err := client.PutObject(ctx, &pb.PutRequest{
		Key:        key,
		Data:       data,
		TtlSeconds: ttlSeconds,
	})
	if err != nil {
		return true, err
	}
	if resp != nil && !resp.Success {
		return true, fmt.Errorf("put failed: %s", resp.Error)
	}
	return true, nil
}
