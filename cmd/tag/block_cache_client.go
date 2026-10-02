package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/ocache/embedded"
	"github.com/tigrisdata/tag/cache"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// embeddedBlockCacheClient keeps embedded.Client's normal CacheClient behavior
// while providing the byte-slice write and optional grouped block-presence paths.
type embeddedBlockCacheClient struct {
	*embedded.Client

	dialOptions    []grpc.DialOption
	lifecycleMu    sync.Mutex
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
	activeCalls    sync.WaitGroup
	closed         bool
	closeOnce      sync.Once
	closeErr       error
}

type blockPresenceOwnerGroup struct {
	owner     string
	address   string
	local     bool
	keys      []string
	positions []int
}

func newEmbeddedBlockCacheClient(client *embedded.Client, dialOptions ...grpc.DialOption) *embeddedBlockCacheClient {
	options := append(cacheclient.DefaultDialOptions(), dialOptions...)
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	return &embeddedBlockCacheClient{
		Client:         client,
		dialOptions:    options,
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
	}
}

// PutBlockBytes writes a fully staged block to its remote owner in one unary
// request. Cache calls this only after the request-size guard has accepted the
// block. Returning handled=false leaves local writes on embedded.Client's
// existing path.
func (c *embeddedBlockCacheClient) PutBlockBytes(ctx context.Context, key string, data []byte, ttlSeconds int64) (handled bool, err error) {
	return cache.PutRemoteBlockBytes(ctx, c.Operations(), key, data, ttlSeconds)
}

// BlockPresence checks one bounded range-preflight page. Remote keys are grouped
// by their current owner and sent in one compact exchange per owner; local keys
// use the same owner-local byte-zero read without opening a gRPC stream.
func (c *embeddedBlockCacheClient) BlockPresence(ctx context.Context, keys []string) ([]bool, error) {
	if len(keys) == 0 || len(keys) > blockPresenceMaxKeys {
		return nil, fmt.Errorf("invalid block presence key count %d", len(keys))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	coord := c.Coordinator()
	var epoch uint64
	var localID string
	if coord != nil {
		epoch = coord.GetEpoch()
		localID = coord.GetLocalNodeID()
	}
	groups := make(map[string]*blockPresenceOwnerGroup)
	owners := make([]string, len(keys))
	addresses := make([]string, len(keys))
	for i, key := range keys {
		owner := ""
		address := ""
		local := true
		if coord != nil {
			node, err := coord.GetNodeForKey(key)
			if err != nil {
				return nil, fmt.Errorf("resolve block owner for %q: %w", key, err)
			}
			if node == nil || node.ID == "" {
				return nil, fmt.Errorf("resolve block owner for %q: no owner", key)
			}
			owner = node.ID
			address = node.ListenAddress
			local = owner == localID
			if !local && address == "" {
				return nil, fmt.Errorf("block owner %q has no gRPC address", owner)
			}
		}
		owners[i] = owner
		addresses[i] = address
		group := groups[owner]
		if group == nil {
			group = &blockPresenceOwnerGroup{owner: owner, address: address, local: local}
			groups[owner] = group
		} else if group.address != address || group.local != local {
			return nil, cache.ErrBlockPresenceTopologyChanged
		}
		group.keys = append(group.keys, key)
		group.positions = append(group.positions, i)
	}
	if coord != nil && coord.GetEpoch() != epoch {
		return nil, cache.ErrBlockPresenceTopologyChanged
	}

	ownerIDs := make([]string, 0, len(groups))
	for owner := range groups {
		ownerIDs = append(ownerIDs, owner)
	}
	sort.Strings(ownerIDs)

	present := make([]bool, len(keys))
	var mu sync.Mutex
	var retryErr, fatalErr error
	group, groupCtx := errgroup.WithContext(ctx)
	for _, owner := range ownerIDs {
		ownerGroup := groups[owner]
		group.Go(func() error {
			var found []bool
			var err error
			if ownerGroup.local {
				found, err = embeddedBlockPresence(groupCtx, c.Client, ownerGroup.owner, epoch, ownerGroup.keys)
			} else {
				found, err = c.remoteBlockPresence(groupCtx, ownerGroup.owner, ownerGroup.address, epoch, ownerGroup.keys)
			}
			if err == nil && len(found) != len(ownerGroup.positions) {
				err = fmt.Errorf("block presence returned %d results for %d keys", len(found), len(ownerGroup.positions))
			}
			if err != nil {
				mu.Lock()
				switch {
				case errors.Is(err, cache.ErrBlockPresenceUnsupported), errors.Is(err, cache.ErrBlockPresenceTopologyChanged):
					if retryErr == nil || errors.Is(err, cache.ErrBlockPresenceTopologyChanged) {
						retryErr = err
					}
				case isSiblingBlockPresenceCancellation(ctx, groupCtx, err):
					// A sibling's failure canceled this exchange; the original error is
					// recorded by the owner that caused the cancellation.
				default:
					if fatalErr == nil {
						fatalErr = err
					}
				}
				mu.Unlock()
				return err
			}
			for i, value := range found {
				present[ownerGroup.positions[i]] = value
			}
			return nil
		})
	}
	_ = group.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fatalErr != nil {
		return nil, fatalErr
	}
	if retryErr != nil {
		return nil, retryErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if coord != nil {
		if coord.GetEpoch() != epoch {
			return nil, cache.ErrBlockPresenceTopologyChanged
		}
		for i, key := range keys {
			node, err := coord.GetNodeForKey(key)
			if err != nil {
				return nil, fmt.Errorf("recheck block owner for %q: %w", key, err)
			}
			if node == nil || node.ID != owners[i] || node.ListenAddress != addresses[i] {
				return nil, cache.ErrBlockPresenceTopologyChanged
			}
		}
	}
	return present, nil
}

func (c *embeddedBlockCacheClient) remoteBlockPresence(ctx context.Context, owner, address string, epoch uint64, keys []string) ([]bool, error) {
	request, err := encodeBlockPresenceRequest(owner, epoch, keys)
	if err != nil {
		if errors.Is(err, errBlockPresenceRequestTooLarge) {
			return nil, cache.ErrBlockPresenceUnsupported
		}
		return nil, err
	}
	rpcCtx, finishCall, err := c.beginPresenceCall(ctx)
	if err != nil {
		return nil, err
	}
	defer finishCall()
	// Keep this ClientConn scoped to one owner exchange. OCache's topology epoch
	// omits addresses, so retaining owner channels would require a full-ring scan
	// on the Range path to retire an unused owner's old address.
	conn, err := grpc.NewClient(address, c.dialOptions...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	stream, err := conn.NewStream(rpcCtx, &grpc.StreamDesc{
		StreamName:    "Check",
		ClientStreams: true,
		ServerStreams: true,
	}, blockPresenceMethod)
	if err != nil {
		return nil, blockPresenceRPCError(rpcCtx, err)
	}
	if err := stream.SendMsg(request); err != nil {
		// A server-side Unimplemented/FailedPrecondition can arrive before SendMsg
		// finishes. grpc-go reports io.EOF here and exposes the status on RecvMsg.
		if !errors.Is(err, io.EOF) {
			return nil, blockPresenceRPCError(rpcCtx, err)
		}
		return receiveBlockPresenceResponse(rpcCtx, stream, epoch, len(keys))
	}
	if err := stream.CloseSend(); err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, blockPresenceRPCError(rpcCtx, err)
		}
		return receiveBlockPresenceResponse(rpcCtx, stream, epoch, len(keys))
	}
	return receiveBlockPresenceResponse(rpcCtx, stream, epoch, len(keys))
}

func receiveBlockPresenceResponse(ctx context.Context, stream grpc.ClientStream, epoch uint64, keyCount int) ([]bool, error) {
	var response structpb.Struct
	if err := stream.RecvMsg(&response); err != nil {
		return nil, blockPresenceRPCError(ctx, err)
	}
	var extra structpb.Struct
	if err := stream.RecvMsg(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, blockPresenceRPCError(ctx, err)
		}
		return nil, errors.New("block presence returned more than one response")
	}
	return decodeBlockPresenceResponse(&response, epoch, keyCount)
}

func isSiblingBlockPresenceCancellation(ctx, groupCtx context.Context, err error) bool {
	return ctx.Err() == nil && groupCtx.Err() != nil &&
		(errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled)
}

func blockPresenceRPCError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	switch status.Code(err) {
	case codes.Unimplemented:
		return fmt.Errorf("%w: %v", cache.ErrBlockPresenceUnsupported, err)
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %v", cache.ErrBlockPresenceTopologyChanged, err)
	default:
		return err
	}
}

// beginPresenceCall keeps wrapper shutdown authoritative while a one-owner
// exchange owns its temporary ClientConn. Add happens under lifecycleMu so Close
// cannot begin waiting before the final call has registered.
func (c *embeddedBlockCacheClient) beginPresenceCall(parent context.Context) (context.Context, func(), error) {
	c.lifecycleMu.Lock()
	if c.closed {
		c.lifecycleMu.Unlock()
		return nil, nil, errors.New("embedded cache client is closed")
	}
	c.activeCalls.Add(1)
	rpcCtx, cancel := context.WithCancel(parent)
	stopShutdown := context.AfterFunc(c.shutdownCtx, cancel)
	c.lifecycleMu.Unlock()
	return rpcCtx, func() {
		stopShutdown()
		cancel()
		c.activeCalls.Done()
	}, nil
}

// Close cancels and joins active presence exchanges before closing the embedded
// cache. Each exchange closes its own ClientConn when it returns.
func (c *embeddedBlockCacheClient) Close() error {
	c.closeOnce.Do(func() {
		c.lifecycleMu.Lock()
		c.closed = true
		c.lifecycleMu.Unlock()
		c.shutdownCancel()
		c.activeCalls.Wait()
		if c.Client != nil {
			c.closeErr = c.Client.Close()
		}
	})
	return c.closeErr
}
