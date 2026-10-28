package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	ocachecoordinator "github.com/tigrisdata/ocache/coordinator"
	ocachestorageerrors "github.com/tigrisdata/ocache/storage/errors"
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

func TestBodyReadRetryExhaustedOwnerRoutingErrorIsNotAbsence(t *testing.T) {
	ctx := context.Background()
	routeErr := ocachecoordinator.NewMaxRetriesExceededError(
		"owner",
		"body|bucket/object/etag",
		1,
		ocachecoordinator.NewNodeNotFoundError("owner", "body|bucket/object/etag"),
	)
	client := &bodyRoutingErrorClient{
		CacheClient: cacheclient.NewMemoryCache(),
		err:         routeErr,
	}
	cfg := config.NewDefault()
	store := NewCacheWithClient(client, &cfg.Cache)

	for _, tc := range []struct {
		name string
		read func() error
	}{
		{
			name: "GetBodyStream",
			read: func() error { return store.GetBodyStream(ctx, "bucket", "object", `"etag"`, io.Discard) },
		},
		{
			name: "GetRangeStream",
			read: func() error { return store.GetRangeStream(ctx, "bucket", "object", `"etag"`, 0, 1, io.Discard) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.read()
			var got *ocachecoordinator.RouterError
			if !errors.Is(err, ErrNotFound) || !errors.As(err, &got) || !errors.Is(got, ocachecoordinator.ErrMaxRetriesExceeded) || !errors.Is(got.Cause, ocachecoordinator.ErrNodeNotFound) {
				t.Errorf("read error = %v, want the legacy ErrNotFound match and the retry-exhausted routing cause", err)
			}
		})
	}

	if present, err := store.BodyExistsErr(ctx, "bucket", "object", `"etag"`); present || err == nil {
		t.Errorf("BodyExistsErr = (%v, %v), want the routing error rather than absence", present, err)
	} else {
		var got *ocachecoordinator.RouterError
		if !errors.Is(err, ErrNotFound) || !errors.As(err, &got) || !errors.Is(got, ocachecoordinator.ErrMaxRetriesExceeded) || !errors.Is(got.Cause, ocachecoordinator.ErrNodeNotFound) {
			t.Errorf("BodyExistsErr error = %v, want the legacy ErrNotFound match and the retry-exhausted routing cause", err)
		}
	}
}

func TestBodyReadTypedIOKeyContainingNotFoundIsNotAbsence(t *testing.T) {
	ctx := context.Background()
	key := MakeBodyKey("bucket", "not found", `"etag"`)
	readErr := ocachestorageerrors.NewIOError("Get", key, errors.New("disk read failed"))
	client := &bodyRoutingErrorClient{CacheClient: cacheclient.NewMemoryCache(), err: readErr}
	cfg := config.NewDefault()
	store := NewCacheWithClient(client, &cfg.Cache)

	for _, tc := range []struct {
		name string
		read func() error
	}{
		{
			name: "GetBodyStream",
			read: func() error { return store.GetBodyStream(ctx, "bucket", "not found", `"etag"`, io.Discard) },
		},
		{
			name: "GetRangeStream",
			read: func() error { return store.GetRangeStream(ctx, "bucket", "not found", `"etag"`, 0, 1, io.Discard) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.read()
			var storageErr *ocachestorageerrors.StorageError
			if errors.Is(err, ErrNotFound) || !errors.As(err, &storageErr) || storageErr.GetType() != ocachestorageerrors.TypeIO {
				t.Errorf("read error = %v, want typed I/O rather than ErrNotFound", err)
			}
		})
	}

	if present, err := store.BodyExistsErr(ctx, "bucket", "not found", `"etag"`); present || err == nil {
		t.Errorf("BodyExistsErr = (%v, %v), want typed I/O rather than absence", present, err)
	} else {
		var storageErr *ocachestorageerrors.StorageError
		if errors.Is(err, ErrNotFound) || !errors.As(err, &storageErr) || storageErr.GetType() != ocachestorageerrors.TypeIO {
			t.Errorf("BodyExistsErr error = %v, want typed I/O rather than ErrNotFound", err)
		}
	}
}

func TestBodyReadTextNotFoundKeepsLegacyMissAndMarksUnconfirmed(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "ordinary I/O text", err: errors.New("disk read failed: object not found")},
		{name: "wrapped cancellation text", err: fmt.Errorf("disk read failed: object not found: %w", context.Canceled)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &bodyRoutingErrorClient{CacheClient: cacheclient.NewMemoryCache(), err: tc.err}
			cfg := config.NewDefault()
			store := NewCacheWithClient(client, &cfg.Cache)

			for _, read := range []struct {
				name string
				call func() error
			}{
				{name: "GetBodyStream", call: func() error { return store.GetBodyStream(ctx, "bucket", "object", `"etag"`, io.Discard) }},
				{name: "GetRangeStream", call: func() error { return store.GetRangeStream(ctx, "bucket", "object", `"etag"`, 0, 1, io.Discard) }},
			} {
				t.Run(read.name, func(t *testing.T) {
					err := read.call()
					if !errors.Is(err, ErrNotFound) || !errors.Is(err, ErrInferredNotFound) {
						t.Errorf("read error = %v, want the legacy miss match and the unconfirmed marker", err)
					}
					if errors.Is(err, context.Canceled) {
						t.Errorf("read error = %v, must not expose the wrapped cancellation cause", err)
					}
				})
			}

			if present, err := store.BodyExistsErr(ctx, "bucket", "object", `"etag"`); present || err == nil || !errors.Is(err, ErrInferredNotFound) {
				t.Errorf("BodyExistsErr = (%v, %v), want an unconfirmed-not-found error", present, err)
			}
			if present, err := store.BlockExistsErr(ctx, "bucket", "object", `"etag"`, 4, 0); present || err != nil {
				t.Errorf("BlockExistsErr = (%v, %v), want a legacy block miss", present, err)
			}
		})
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
