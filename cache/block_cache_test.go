package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/tag/config"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func newBlockTestCache(t *testing.T) *Cache {
	t.Helper()
	mem := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	return NewCacheWithClient(mem, &cfg.Cache)
}

type recordingBlockClient struct {
	cacheclient.CacheClient
	putCalls       int
	streamPutCalls int
	bytePutCalls   int
	lastTTL        int64
	putErr         error
	handleBytes    bool
}

func (c *recordingBlockClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	c.putCalls++
	c.lastTTL = ttlSeconds
	if c.putErr != nil {
		return c.putErr
	}
	return c.CacheClient.Put(ctx, key, data, ttlSeconds)
}

func (c *recordingBlockClient) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	c.streamPutCalls++
	c.lastTTL = ttlSeconds
	return c.CacheClient.PutStream(ctx, key, r, ttlSeconds)
}

func (c *recordingBlockClient) PutBlockBytes(ctx context.Context, key string, data []byte, ttlSeconds int64) (bool, error) {
	c.bytePutCalls++
	if !c.handleBytes {
		return false, nil
	}
	c.lastTTL = ttlSeconds
	if c.putErr != nil {
		return true, c.putErr
	}
	return true, c.CacheClient.Put(ctx, key, data, ttlSeconds)
}

// BlockExistsErr distinguishes a present block (true, nil) from a genuinely-absent one
// (false, nil). Both must report err=nil so callers don't mistake a plain cache miss for a
// transient probe failure (the transient case, present=false with err!=nil, is what lets callers
// avoid counting a network blip as a missing block).
func TestBlockExistsErr_PresentVsAbsentAreNotErrors(t *testing.T) {
	c := newBlockTestCache(t)
	ctx := context.Background()
	bucket, key, etag := "b", "k", `"v1"`

	if err := c.PutBlockStream(ctx, bucket, key, etag, 4, 0, strings.NewReader("AAAA"), 60); err != nil {
		t.Fatalf("PutBlockStream: %v", err)
	}
	if present, err := c.BlockExistsErr(ctx, bucket, key, etag, 4, 0); !present || err != nil {
		t.Errorf("present block: got (present=%v, err=%v), want (true, nil)", present, err)
	}
	if present, err := c.BlockExistsErr(ctx, bucket, key, etag, 4, 1); present || err != nil {
		t.Errorf("absent block: got (present=%v, err=%v), want (false, nil)", present, err)
	}
}

// Blocks round-trip: write a few blocks, probe existence, and read block-local sub-ranges.
func TestBlockRoundTrip(t *testing.T) {
	c := newBlockTestCache(t)
	ctx := context.Background()
	bucket, key, etag := "b", "k", `"v1"`

	// Three 4-byte blocks of the object "AAAABBBBCC" (last block short: 2 bytes).
	blocks := []string{"AAAA", "BBBB", "CC"}
	for i, b := range blocks {
		if err := c.PutBlockStream(ctx, bucket, key, etag, 4, int64(i), strings.NewReader(b), 60); err != nil {
			t.Fatalf("PutBlockStream block %d: %v", i, err)
		}
	}

	// Existence: written blocks present, an unwritten block absent.
	for i := range blocks {
		if !c.BlockExists(ctx, bucket, key, etag, 4, int64(i)) {
			t.Errorf("BlockExists(block %d) = false, want true", i)
		}
	}
	if c.BlockExists(ctx, bucket, key, etag, 4, 3) {
		t.Error("BlockExists(block 3) = true, want false (never written)")
	}
	// A different ETag must not resolve these blocks.
	if c.BlockExists(ctx, bucket, key, `"v2"`, 4, 0) {
		t.Error("BlockExists with wrong etag = true, want false")
	}

	// Read block-local sub-ranges.
	cases := []struct {
		blockIdx, start, end int64
		want                 string
	}{
		{0, 0, 3, "AAAA"}, // whole first block
		{0, 0, 0, "A"},    // byte-0 quirk path
		{1, 1, 2, "BB"},   // interior of second block
		{2, 0, 1, "CC"},   // whole short last block
	}
	for _, tc := range cases {
		var buf bytes.Buffer
		if err := c.GetBlockRangeStream(ctx, bucket, key, etag, 4, tc.blockIdx, tc.start, tc.end, &buf); err != nil {
			t.Fatalf("GetBlockRangeStream(block %d, %d-%d): %v", tc.blockIdx, tc.start, tc.end, err)
		}
		if buf.String() != tc.want {
			t.Errorf("GetBlockRangeStream(block %d, %d-%d) = %q, want %q", tc.blockIdx, tc.start, tc.end, buf.String(), tc.want)
		}
	}

	// A missing block reads as ErrNotFound.
	if err := c.GetBlockRangeStream(ctx, bucket, key, etag, 4, 3, 0, 1, &bytes.Buffer{}); err != ErrNotFound {
		t.Errorf("GetBlockRangeStream(missing block) err = %v, want ErrNotFound", err)
	}
}

// A block of exactly 1 byte (the last block when contentLength % blockSize == 1) must probe
// and read correctly through the byte-0 quirk path, which requests [0,1]: ocache returns the
// single available byte (memory clamps the end; embedded reads it then EOFs) rather than
// erroring, so presence probes and single-byte reads don't spuriously miss.
func TestBlockRoundTrip_OneByteBlock(t *testing.T) {
	c := newBlockTestCache(t)
	ctx := context.Background()
	bucket, key, etag := "b", "k", `"v1"`
	if err := c.PutBlockStream(ctx, bucket, key, etag, 4, 5, strings.NewReader("Z"), 60); err != nil {
		t.Fatalf("PutBlockStream: %v", err)
	}
	if !c.BlockExists(ctx, bucket, key, etag, 4, 5) {
		t.Error("BlockExists(1-byte block) = false, want true")
	}
	var buf bytes.Buffer
	if err := c.GetBlockRangeStream(ctx, bucket, key, etag, 4, 5, 0, 0, &buf); err != nil {
		t.Fatalf("GetBlockRangeStream(1-byte block, 0-0): %v", err)
	}
	if buf.String() != "Z" {
		t.Errorf("GetBlockRangeStream(1-byte block) = %q, want Z", buf.String())
	}
}

// An empty ETag is not block-cached (no version discriminator).
func TestPutBlockStream_EmptyETagNotCached(t *testing.T) {
	c := newBlockTestCache(t)
	ctx := context.Background()
	if err := c.PutBlockStream(ctx, "b", "k", "", 4, 0, strings.NewReader("data"), 60); err != nil {
		t.Fatalf("PutBlockStream(empty etag): %v", err)
	}
	if c.BlockExists(ctx, "b", "k", "", 4, 0) {
		t.Error("BlockExists(empty etag) = true, want false (empty etag not cached)")
	}
}

func TestPutBlock_FallsBackToStreamAndPreservesBlockKeyAndTTL(t *testing.T) {
	ctx := context.Background()
	mem := cacheclient.NewMemoryCache()
	client := &recordingBlockClient{CacheClient: mem}
	cfg := config.NewDefault()
	c := NewCacheWithClient(client, &cfg.Cache)

	body := []byte("data")
	if err := c.PutBlock(ctx, "b", "k", `"v1"`, 4, 2, body, 37); err != nil {
		t.Fatalf("PutBlock: %v", err)
	}
	if client.putCalls != 0 || client.streamPutCalls != 1 {
		t.Fatalf("write calls = Put:%d PutStream:%d, want Put:0 PutStream:1", client.putCalls, client.streamPutCalls)
	}
	if client.lastTTL != 37 {
		t.Errorf("TTL = %d, want 37", client.lastTTL)
	}

	var got bytes.Buffer
	if err := c.GetBlockRangeStream(ctx, "b", "k", `"v1"`, 4, 2, 0, int64(len(body)-1), &got); err != nil {
		t.Fatalf("GetBlockRangeStream: %v", err)
	}
	if !bytes.Equal(got.Bytes(), body) {
		t.Errorf("stored body = %q, want %q", got.Bytes(), body)
	}
}

func TestPutBlock_UsesSpecializedBytePath(t *testing.T) {
	ctx := context.Background()
	mem := cacheclient.NewMemoryCache()
	client := &recordingBlockClient{CacheClient: mem, handleBytes: true}
	cfg := config.NewDefault()
	c := NewCacheWithClient(client, &cfg.Cache)
	body := []byte("data")

	if err := c.PutBlock(ctx, "b", "k", `"v1"`, 4, 0, body, 0); err != nil {
		t.Fatalf("PutBlock: %v", err)
	}
	if client.bytePutCalls != 1 || client.putCalls != 0 || client.streamPutCalls != 0 {
		t.Fatalf("write calls = PutBlockBytes:%d Put:%d PutStream:%d, want 1:0:0", client.bytePutCalls, client.putCalls, client.streamPutCalls)
	}
	if want := int64(cfg.Cache.TTL.Seconds()); client.lastTTL != want {
		t.Errorf("TTL = %d, want default %d", client.lastTTL, want)
	}

	var got bytes.Buffer
	if err := c.GetBlockRangeStream(ctx, "b", "k", `"v1"`, 4, 0, 0, int64(len(body)-1), &got); err != nil {
		t.Fatalf("GetBlockRangeStream: %v", err)
	}
	if !bytes.Equal(got.Bytes(), body) {
		t.Errorf("stored body = %q, want %q", got.Bytes(), body)
	}
}

func TestPutBlock_LargerBlocksUseSpecializedBytePath(t *testing.T) {
	client := &recordingBlockClient{CacheClient: cacheclient.NewMemoryCache(), handleBytes: true}
	cfg := config.NewDefault()
	c := NewCacheWithClient(client, &cfg.Cache)
	body := make([]byte, config.DefaultCacheBlockSize+1)

	if err := c.PutBlock(context.Background(), "b", "k", `"v1"`, int64(len(body)), 0, body, 60); err != nil {
		t.Fatalf("PutBlock: %v", err)
	}
	if client.bytePutCalls != 1 || client.streamPutCalls != 0 {
		t.Fatalf("write calls = PutBlockBytes:%d PutStream:%d, want 1:0", client.bytePutCalls, client.streamPutCalls)
	}
}

func TestPutBlock_PropagatesBytePathError(t *testing.T) {
	want := errors.New("injected put failure")
	client := &recordingBlockClient{CacheClient: cacheclient.NewMemoryCache(), putErr: want, handleBytes: true}
	cfg := config.NewDefault()
	c := NewCacheWithClient(client, &cfg.Cache)

	if err := c.PutBlock(context.Background(), "b", "k", `"v1"`, 4, 0, []byte("data"), 60); !errors.Is(err, want) {
		t.Fatalf("PutBlock error = %v, want %v", err, want)
	}
	if client.bytePutCalls != 1 || client.putCalls != 0 || client.streamPutCalls != 0 {
		t.Fatalf("write calls = PutBlockBytes:%d Put:%d PutStream:%d, want 1:0:0", client.bytePutCalls, client.putCalls, client.streamPutCalls)
	}
}

func TestPutBlock_EmptyETagAndDisabledCacheDoNothing(t *testing.T) {
	client := &recordingBlockClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	c := NewCacheWithClient(client, &cfg.Cache)
	if err := c.PutBlock(context.Background(), "b", "k", "", 4, 0, []byte("data"), 60); err != nil {
		t.Fatalf("PutBlock(empty etag): %v", err)
	}
	if client.putCalls != 0 || client.streamPutCalls != 0 || client.bytePutCalls != 0 {
		t.Fatalf("empty ETag wrote cache: PutBlockBytes:%d Put:%d PutStream:%d", client.bytePutCalls, client.putCalls, client.streamPutCalls)
	}

	disabled := NewDisabledCache()
	if err := disabled.PutBlock(context.Background(), "b", "k", `"v1"`, 4, 0, []byte("data"), 60); err != nil {
		t.Fatalf("PutBlock(disabled): %v", err)
	}
}

func TestCanPutBlockUnaryUsesExactRequestLimit(t *testing.T) {
	key := "blk|bucket|key|etag|4|0"
	data := []byte("data")
	ttl := int64(60)
	size := unaryPutRequestSize(key, data, ttl)
	if want := int64(proto.Size(&pb.PutRequest{Key: key, Data: data, TtlSeconds: ttl})); size != want {
		t.Fatalf("unaryPutRequestSize = %d, want protobuf size %d", size, want)
	}
	if !canPutBlockUnaryForLimit(key, data, ttl, size) {
		t.Errorf("request of size %d rejected at its exact limit", size)
	}
	if canPutBlockUnaryForLimit(key, data, ttl, size-1) {
		t.Errorf("request of size %d accepted above limit %d", size, size-1)
	}
}

func TestCanPutBlockUnaryAcceptsLargerConfiguredBlocks(t *testing.T) {
	key := "blk|bucket|key|etag|4|0"
	if !canPutBlockUnary(key, make([]byte, 2*config.DefaultCacheBlockSize), 60) {
		t.Fatal("larger configured block did not use unary path")
	}
}

type fakeRemoteBlockRouter struct {
	local    bool
	nodeID   string
	client   pb.CacheServiceClient
	routeErr error
	routes   int
}

func (r *fakeRemoteBlockRouter) IsLocal(string) bool { return r.local }

func (r *fakeRemoteBlockRouter) GetLocalNodeID() string { return r.nodeID }

func (r *fakeRemoteBlockRouter) Route(string) (pb.CacheServiceClient, error) {
	r.routes++
	return r.client, r.routeErr
}

type recordingRemoteBlockRPCClient struct {
	pb.CacheServiceClient
	request  *pb.PutRequest
	response *pb.PutResponse
	err      error
}

func (c *recordingRemoteBlockRPCClient) PutObject(_ context.Context, req *pb.PutRequest, _ ...grpc.CallOption) (*pb.PutResponse, error) {
	c.request = req
	return c.response, c.err
}

func TestPutRemoteBlockBytesUsesUnaryRequest(t *testing.T) {
	rpc := &recordingRemoteBlockRPCClient{response: &pb.PutResponse{Success: true}}
	router := &fakeRemoteBlockRouter{nodeID: "node-a", client: rpc}
	body := []byte("block data")

	handled, err := PutRemoteBlockBytes(context.Background(), router, "block-key", body, 42)
	if err != nil || !handled {
		t.Fatalf("PutRemoteBlockBytes = (handled=%t, err=%v), want (true, nil)", handled, err)
	}
	if router.routes != 1 {
		t.Fatalf("Route calls = %d, want 1", router.routes)
	}
	if rpc.request == nil {
		t.Fatal("PutObject was not called")
	}
	if rpc.request.Key != "block-key" || rpc.request.TtlSeconds != 42 || !bytes.Equal(rpc.request.Data, body) {
		t.Errorf("PutObject request = %+v, want key, TTL, and data preserved", rpc.request)
	}
}

func TestPutRemoteBlockBytesLeavesLocalWriteOnExistingPath(t *testing.T) {
	router := &fakeRemoteBlockRouter{local: true, nodeID: "node-a"}

	handled, err := PutRemoteBlockBytes(context.Background(), router, "block-key", []byte("block data"), 42)
	if err != nil || handled {
		t.Fatalf("PutRemoteBlockBytes = (handled=%t, err=%v), want (false, nil)", handled, err)
	}
	if router.routes != 0 {
		t.Errorf("Route calls = %d, want 0 for local key", router.routes)
	}
}

func TestPutRemoteBlockBytesPropagatesRouteAndRemoteErrors(t *testing.T) {
	routeErr := errors.New("route failed")
	router := &fakeRemoteBlockRouter{nodeID: "node-a", routeErr: routeErr}
	if handled, err := PutRemoteBlockBytes(context.Background(), router, "block-key", []byte("block data"), 42); !handled || !errors.Is(err, routeErr) {
		t.Fatalf("route failure = (handled=%t, err=%v), want (true, %v)", handled, err, routeErr)
	}

	remoteErr := errors.New("remote failed")
	rpc := &recordingRemoteBlockRPCClient{err: remoteErr}
	router = &fakeRemoteBlockRouter{nodeID: "node-a", client: rpc}
	if handled, err := PutRemoteBlockBytes(context.Background(), router, "block-key", []byte("block data"), 42); !handled || !errors.Is(err, remoteErr) {
		t.Fatalf("remote failure = (handled=%t, err=%v), want (true, %v)", handled, err, remoteErr)
	}
}

func TestPutRemoteBlockBytesPropagatesRejectedWrite(t *testing.T) {
	rpc := &recordingRemoteBlockRPCClient{response: &pb.PutResponse{Success: false, Error: "write rejected"}}
	router := &fakeRemoteBlockRouter{nodeID: "node-a", client: rpc}

	handled, err := PutRemoteBlockBytes(context.Background(), router, "block-key", []byte("block data"), 42)
	if !handled || err == nil || err.Error() != "put failed: write rejected" {
		t.Fatalf("rejected write = (handled=%t, err=%v), want (true, put failed: write rejected)", handled, err)
	}
}

// PutMetaTombstoneAware writes meta, and skips the write when a newer tombstone exists.
func TestPutMetaTombstoneAware(t *testing.T) {
	c := newBlockTestCache(t)
	ctx := context.Background()
	bucket, key := "b", "k"
	meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"v1"`, ContentLength: 100, StatusCode: 200, BlockSize: 4}

	// No tombstone → meta is written.
	wrote, err := c.PutMetaTombstoneAware(ctx, bucket, key, meta, 60, time.Now().UnixNano())
	if err != nil || !wrote {
		t.Fatalf("PutMetaTombstoneAware = (%v, %v), want (true, nil)", wrote, err)
	}
	got, found, _ := c.GetMeta(ctx, bucket, key)
	if !found {
		t.Fatal("GetMeta after write: not found, want found")
	}
	if got.BlockSize != 4 {
		t.Fatalf("GetMeta after write: blockSize=%d, want 4", got.BlockSize)
	}

	// A tombstone newer than the write start → meta write skipped.
	writeStart := time.Now().UnixNano()
	time.Sleep(time.Millisecond)
	c.WriteTombstone(ctx, bucket, key)
	wrote, err = c.PutMetaTombstoneAware(ctx, bucket, key, meta, 60, writeStart)
	if err != nil {
		t.Fatalf("PutMetaTombstoneAware (tombstoned) err = %v", err)
	}
	if wrote {
		t.Error("PutMetaTombstoneAware wrote meta despite a newer tombstone")
	}
}
