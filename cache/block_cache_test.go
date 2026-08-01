package cache

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

func newBlockTestCache(t *testing.T) *Cache {
	t.Helper()
	mem := cacheclient.NewMemoryCache()
	cfg := config.NewDefault()
	return NewCacheWithClient(mem, &cfg.Cache)
}

// Blocks round-trip: write a few blocks, probe existence, and read block-local sub-ranges.
func TestBlockRoundTrip(t *testing.T) {
	c := newBlockTestCache(t)
	ctx := context.Background()
	bucket, key, etag := "b", "k", `"v1"`

	// Three 4-byte blocks of the object "AAAABBBBCC" (last block short: 2 bytes).
	blocks := []string{"AAAA", "BBBB", "CC"}
	for i, b := range blocks {
		if err := c.PutBlockStream(ctx, bucket, key, etag, int64(i), strings.NewReader(b), 60); err != nil {
			t.Fatalf("PutBlockStream block %d: %v", i, err)
		}
	}

	// Existence: written blocks present, an unwritten block absent.
	for i := range blocks {
		if !c.BlockExists(ctx, bucket, key, etag, int64(i)) {
			t.Errorf("BlockExists(block %d) = false, want true", i)
		}
	}
	if c.BlockExists(ctx, bucket, key, etag, 3) {
		t.Error("BlockExists(block 3) = true, want false (never written)")
	}
	// A different ETag must not resolve these blocks.
	if c.BlockExists(ctx, bucket, key, `"v2"`, 0) {
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
		if err := c.GetBlockRangeStream(ctx, bucket, key, etag, tc.blockIdx, tc.start, tc.end, &buf); err != nil {
			t.Fatalf("GetBlockRangeStream(block %d, %d-%d): %v", tc.blockIdx, tc.start, tc.end, err)
		}
		if buf.String() != tc.want {
			t.Errorf("GetBlockRangeStream(block %d, %d-%d) = %q, want %q", tc.blockIdx, tc.start, tc.end, buf.String(), tc.want)
		}
	}

	// A missing block reads as ErrNotFound.
	if err := c.GetBlockRangeStream(ctx, bucket, key, etag, 3, 0, 1, &bytes.Buffer{}); err != ErrNotFound {
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
	if err := c.PutBlockStream(ctx, bucket, key, etag, 5, strings.NewReader("Z"), 60); err != nil {
		t.Fatalf("PutBlockStream: %v", err)
	}
	if !c.BlockExists(ctx, bucket, key, etag, 5) {
		t.Error("BlockExists(1-byte block) = false, want true")
	}
	var buf bytes.Buffer
	if err := c.GetBlockRangeStream(ctx, bucket, key, etag, 5, 0, 0, &buf); err != nil {
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
	if err := c.PutBlockStream(ctx, "b", "k", "", 0, strings.NewReader("data"), 60); err != nil {
		t.Fatalf("PutBlockStream(empty etag): %v", err)
	}
	if c.BlockExists(ctx, "b", "k", "", 0) {
		t.Error("BlockExists(empty etag) = true, want false (empty etag not cached)")
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
