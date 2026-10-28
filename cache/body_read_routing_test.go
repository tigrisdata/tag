package cache

import (
	"context"
	"errors"
	"io"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	ocachecoordinator "github.com/tigrisdata/ocache/coordinator"
	"github.com/tigrisdata/tag/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type bodyRoutingErrorClient struct {
	cacheclient.CacheClient
	err error
}

func (c *bodyRoutingErrorClient) GetStream(context.Context, string, io.Writer) error {
	return c.err
}

func (c *bodyRoutingErrorClient) GetRangeStream(context.Context, string, int64, int64, io.Writer) error {
	return c.err
}

func TestBodyReadUnavailableWithNotFoundTextIsNotAbsence(t *testing.T) {
	ctx := context.Background()
	routingErr := status.Error(codes.Unavailable, "routing error: node not found in ring")
	client := &bodyRoutingErrorClient{CacheClient: cacheclient.NewMemoryCache(), err: routingErr}
	cfg := config.NewDefault()
	store := NewCacheWithClient(client, &cfg.Cache)

	if err := store.GetBodyStream(ctx, "bucket", "object", `"etag"`, io.Discard); status.Code(err) != codes.Unavailable || errors.Is(err, ErrNotFound) {
		t.Errorf("GetBodyStream error = %v, want the Unavailable routing error rather than ErrNotFound", err)
	}
	if err := store.GetRangeStream(ctx, "bucket", "object", `"etag"`, 0, 1, io.Discard); status.Code(err) != codes.Unavailable || errors.Is(err, ErrNotFound) {
		t.Errorf("GetRangeStream error = %v, want the Unavailable routing error rather than ErrNotFound", err)
	}
	if present, err := store.BodyExistsErr(ctx, "bucket", "object", `"etag"`); present || status.Code(err) != codes.Unavailable || errors.Is(err, ErrNotFound) {
		t.Errorf("BodyExistsErr = (%v, %v), want an Unavailable error rather than absence", present, err)
	}
}

func TestBodyReadNotFoundPreservesOwnerRoutingError(t *testing.T) {
	ctx := context.Background()
	routeErr := ocachecoordinator.NewNodeNotFoundError("owner", "body|bucket/object/etag")
	client := &bodyRoutingErrorClient{
		CacheClient: cacheclient.NewMemoryCache(),
		err:         routeErr,
	}
	cfg := config.NewDefault()
	store := NewCacheWithClient(client, &cfg.Cache)

	if err := store.GetBodyStream(ctx, "bucket", "object", `"etag"`, io.Discard); !errors.Is(err, ErrNotFound) || !errors.Is(err, ocachecoordinator.ErrNodeNotFound) {
		t.Errorf("GetBodyStream routing error = %v, want the legacy miss match and the routing cause", err)
	}
	if err := store.GetRangeStream(ctx, "bucket", "object", `"etag"`, 0, 1, io.Discard); !errors.Is(err, ErrNotFound) || !errors.Is(err, ocachecoordinator.ErrNodeNotFound) {
		t.Errorf("GetRangeStream routing error = %v, want the legacy miss match and the routing cause", err)
	}
	if present, err := store.BodyExistsErr(ctx, "bucket", "object", `"etag"`); present || !errors.Is(err, ocachecoordinator.ErrNodeNotFound) {
		t.Errorf("BodyExistsErr = (%v, %v), want a routing error rather than an absence result", present, err)
	}
}
