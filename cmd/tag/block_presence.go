package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/tigrisdata/ocache/coordinator/ring"
	"github.com/tigrisdata/ocache/embedded"
	"github.com/tigrisdata/tag/cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// blockPresenceMethod is a private bidi peer exchange carried by OCache's
// existing gRPC listener. Its request is {owner, decimal epoch, keys}; its
// response is {decimal epoch, found flags in key order}.
const (
	blockPresenceMethod      = "/tag.cache.v1.BlockPresence/Check"
	blockPresenceMaxKeys     = 32
	blockPresenceMaxKeyBytes = 64 << 10
)

var errBlockPresenceRequestTooLarge = errors.New("block presence request is too large")

type blockPresenceServer struct {
	client *embedded.Client
}

// serverOption adds the versioned private peer method to OCache's existing
// listener. Other unknown methods keep the gRPC Unimplemented response.
func (s *blockPresenceServer) serverOption() grpc.ServerOption {
	return grpc.UnknownServiceHandler(s.serve)
}

func (s *blockPresenceServer) serve(_ any, stream grpc.ServerStream) error {
	method, ok := grpc.MethodFromServerStream(stream)
	if !ok || method != blockPresenceMethod {
		return status.Error(codes.Unimplemented, "unsupported cache method")
	}
	if s.client == nil {
		return status.Error(codes.Unavailable, "embedded cache is not ready")
	}

	var message structpb.Struct
	if err := stream.RecvMsg(&message); err != nil {
		return err
	}
	owner, epoch, keys, err := decodeBlockPresenceRequest(&message)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	coord := s.client.Coordinator()
	if coord == nil || coord.GetLocalNodeID() != owner || coord.GetEpoch() != epoch {
		return status.Error(codes.FailedPrecondition, "block presence owner changed")
	}

	found, err := embeddedBlockPresence(stream.Context(), s.client, owner, epoch, keys)
	if err != nil {
		if errors.Is(err, cache.ErrBlockPresenceTopologyChanged) {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
		if ctxErr := stream.Context().Err(); ctxErr != nil {
			return status.FromContextError(ctxErr).Err()
		}
		return status.Errorf(codes.Internal, "block presence read: %v", err)
	}
	response, err := encodeBlockPresenceResponse(found, epoch)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return stream.SendMsg(response)
}

func decodeBlockPresenceRequest(message *structpb.Struct) (owner string, epoch uint64, keys []string, err error) {
	if message == nil {
		return "", 0, nil, errors.New("missing block presence request")
	}
	owner = message.GetFields()["owner"].GetStringValue()
	if owner == "" {
		return "", 0, nil, errors.New("missing block presence owner")
	}
	epochText := message.GetFields()["epoch"].GetStringValue()
	if epochText == "" {
		return "", 0, nil, errors.New("missing block presence epoch")
	}
	epoch, err = strconv.ParseUint(epochText, 10, 64)
	if err != nil {
		return "", 0, nil, fmt.Errorf("invalid block presence epoch: %w", err)
	}
	list := message.GetFields()["keys"].GetListValue()
	if list == nil || len(list.Values) == 0 || len(list.Values) > blockPresenceMaxKeys {
		return "", 0, nil, fmt.Errorf("block presence key count must be between 1 and %d", blockPresenceMaxKeys)
	}
	keys = make([]string, len(list.Values))
	keyBytes := 0
	for i, value := range list.Values {
		if value == nil {
			return "", 0, nil, fmt.Errorf("block presence key %d is missing", i)
		}
		key, ok := value.Kind.(*structpb.Value_StringValue)
		if !ok || key.StringValue == "" {
			return "", 0, nil, fmt.Errorf("block presence key %d is invalid", i)
		}
		keys[i] = key.StringValue
		keyBytes += len(key.StringValue)
		if keyBytes > blockPresenceMaxKeyBytes {
			return "", 0, nil, errBlockPresenceRequestTooLarge
		}
	}
	return owner, epoch, keys, nil
}

func encodeBlockPresenceRequest(owner string, epoch uint64, keys []string) (*structpb.Struct, error) {
	if owner == "" || len(keys) == 0 || len(keys) > blockPresenceMaxKeys {
		return nil, fmt.Errorf("invalid block presence request: owner=%q keys=%d", owner, len(keys))
	}
	keyValues := make([]any, len(keys))
	keyBytes := 0
	for i, key := range keys {
		if key == "" {
			return nil, fmt.Errorf("block presence key %d is empty", i)
		}
		if !utf8.ValidString(key) {
			// The protobuf transport uses UTF-8 strings; preserve legacy handling for other keys.
			return nil, cache.ErrBlockPresenceUnsupported
		}
		keyBytes += len(key)
		if keyBytes > blockPresenceMaxKeyBytes {
			return nil, errBlockPresenceRequestTooLarge
		}
		keyValues[i] = key
	}
	return structpb.NewStruct(map[string]any{
		"owner": owner,
		"epoch": strconv.FormatUint(epoch, 10),
		"keys":  keyValues,
	})
}

func encodeBlockPresenceResponse(found []bool, epoch uint64) (*structpb.Struct, error) {
	if len(found) == 0 || len(found) > blockPresenceMaxKeys {
		return nil, fmt.Errorf("invalid block presence response count %d", len(found))
	}
	values := make([]any, len(found))
	for i, present := range found {
		values[i] = present
	}
	return structpb.NewStruct(map[string]any{
		"epoch": strconv.FormatUint(epoch, 10),
		"found": values,
	})
}

func decodeBlockPresenceResponse(message *structpb.Struct, expectedEpoch uint64, expectedCount int) ([]bool, error) {
	if message == nil {
		return nil, errors.New("missing block presence response")
	}
	epochText := message.GetFields()["epoch"].GetStringValue()
	epoch, err := strconv.ParseUint(epochText, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid block presence response epoch: %w", err)
	}
	if epoch != expectedEpoch {
		return nil, cache.ErrBlockPresenceTopologyChanged
	}
	list := message.GetFields()["found"].GetListValue()
	if list == nil || len(list.Values) != expectedCount || len(list.Values) > blockPresenceMaxKeys {
		return nil, fmt.Errorf("invalid block presence result count")
	}
	found := make([]bool, len(list.Values))
	for i, value := range list.Values {
		if value == nil {
			return nil, fmt.Errorf("block presence result %d is missing", i)
		}
		result, ok := value.Kind.(*structpb.Value_BoolValue)
		if !ok {
			return nil, fmt.Errorf("block presence result %d is not boolean", i)
		}
		found[i] = result.BoolValue
	}
	return found, nil
}

// embeddedBlockPresence reads only the same byte-zero probe range as
// BlockExistsErr, on the node that currently owns every key. A short range with
// at least one byte is present; an empty range is absent; storage errors remain
// errors. Owner checks make a stale ring a retry, not a miss.
func embeddedBlockPresence(ctx context.Context, client *embedded.Client, owner string, epoch uint64, keys []string) ([]bool, error) {
	if len(keys) == 0 || len(keys) > blockPresenceMaxKeys {
		return nil, fmt.Errorf("invalid block presence key count %d", len(keys))
	}
	coord := client.Coordinator()
	if coord != nil {
		if coord.GetLocalNodeID() != owner || coord.GetEpoch() != epoch {
			return nil, cache.ErrBlockPresenceTopologyChanged
		}
	} else if owner != "" || epoch != 0 {
		return nil, cache.ErrBlockPresenceTopologyChanged
	}

	found := make([]bool, len(keys))
	for i, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if coord != nil {
			if err := checkBlockPresenceOwner(coord, key, owner); err != nil {
				return nil, err
			}
		}
		reader, exists, err := client.Operations().GetLocal(ctx, key, 0, 1)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		if reader == nil {
			return nil, fmt.Errorf("block %q returned a nil reader", key)
		}
		n, readErr := io.Copy(io.Discard, &io.LimitedReader{R: reader, N: 2})
		if closer, ok := reader.(io.Closer); ok {
			_ = closer.Close()
		}
		if readErr != nil {
			return nil, readErr
		}
		found[i] = n > 0
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if coord != nil {
		if coord.GetEpoch() != epoch {
			return nil, cache.ErrBlockPresenceTopologyChanged
		}
		for _, key := range keys {
			if err := checkBlockPresenceOwner(coord, key, owner); err != nil {
				return nil, err
			}
		}
	}
	return found, nil
}

func checkBlockPresenceOwner(coord blockPresenceCoordinator, key, owner string) error {
	node, err := coord.GetNodeForKey(key)
	if err != nil {
		return fmt.Errorf("resolve block owner for %q: %w", key, err)
	}
	if node.ID != owner {
		return cache.ErrBlockPresenceTopologyChanged
	}
	return nil
}

// blockPresenceCoordinator is the small, source-owned part of the embedded
// coordinator used by both the peer handler and its caller.
type blockPresenceCoordinator interface {
	GetEpoch() uint64
	GetLocalNodeID() string
	GetNodeForKey(string) (*ring.NodeInfo, error)
}
