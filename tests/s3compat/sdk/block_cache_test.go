package sdk

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

// blockSizeForTests returns the block size TAG is configured with for this run, read from
// TAG_TEST_BLOCK_SIZE (the same variable the Makefile passes to s3-test-local-blocks). It
// defaults to 64 KiB — the e2e block size — so object sizes here straddle the actual block
// boundary. In default (whole-object) mode the value is just a sizing hint: the correctness
// assertions still hold, they simply don't exercise the block paths.
func blockSizeForTests() int {
	if v := os.Getenv("TAG_TEST_BLOCK_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 65536
}

// waitForCacheHitBody polls a full GET until TAG serves it from cache (X-Cache: HIT), then
// returns the served body. Because population is asynchronous, MISS/REVALIDATED responses are
// retried. Every served body is verified to equal want on the spot: the block-serve bug this
// guards against returned X-Cache HIT with a truncated/empty body, so an early HIT with the
// wrong length must fail here, not be masked by a later correct read.
func waitForCacheHitBody(t *testing.T, bucket, key string, want []byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastCache string
	for time.Now().Before(deadline) {
		resp, err := globalEnv.DoRawGet(bucket, key)
		if err != nil {
			t.Fatalf("%s/%s: raw GET failed: %v", bucket, key, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s/%s: full GET status %d, want 200", bucket, key, resp.StatusCode)
		}
		// A served response — cache HIT or an upstream MISS — must always carry the full body.
		if !bytes.Equal(body, want) {
			t.Fatalf("%s/%s: full GET (X-Cache=%s) returned %d bytes, want %d (block serve truncation)",
				bucket, key, resp.Header.Get("X-Cache"), len(body), len(want))
		}
		lastCache = resp.Header.Get("X-Cache")
		if lastCache == "HIT" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s/%s: object never served from cache (last X-Cache=%s) within %v", bucket, key, lastCache, timeout)
}

// getRangeExpectHit issues a warm Range GET and verifies it is served from cache (206, X-Cache:
// HIT) with exactly the requested bytes. This is the path the warp block-mode benchmark broke:
// a range assembled from cached blocks that streamed 0 bytes under a committed Content-Length.
func getRangeExpectHit(t *testing.T, bucket, key string, start, end int64, want []byte) {
	t.Helper()
	resp, err := globalEnv.DoRawGetWithHeaders(bucket, key, map[string]string{
		"Range": fmt.Sprintf("bytes=%d-%d", start, end),
	})
	if err != nil {
		t.Fatalf("%s/%s range %d-%d: GET failed: %v", bucket, key, start, end, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("%s/%s range %d-%d: status %d, want 206", bucket, key, start, end, resp.StatusCode)
	}
	if xc := resp.Header.Get("X-Cache"); xc != "HIT" {
		t.Errorf("%s/%s range %d-%d: X-Cache=%q, want HIT (range not served from cache)", bucket, key, start, end, xc)
	}
	if !bytes.Equal(body, want) {
		t.Fatalf("%s/%s range %d-%d: got %d bytes, want %d (block range serve truncation/corruption)",
			bucket, key, start, end, len(body), len(want))
	}
}

// TestSDK_BlockCache_FullGET_SizeRegimes exercises full-object GETs across every size regime
// relative to the block size, reading each object twice (cold populate, then a warm cache hit)
// and verifying the served bytes exactly. It then reads block-spanning sub-ranges from the warm
// entry. In block mode (make s3-test-local-blocks) the >= block-size cases are stored and served
// as fixed-size blocks, so this covers: a sub-block whole-cached object, an object of exactly one
// block, one block plus a partial tail, exactly two blocks, several blocks with a partial tail,
// and a many-block object. The warp benchmark regression (warm block reads returning 0 bytes) is
// caught by the body-length assertions in waitForCacheHitBody / getRangeExpectHit.
func TestSDK_BlockCache_FullGET_SizeRegimes(t *testing.T) {
	bs := blockSizeForTests()
	cases := []struct {
		name string
		size int
	}{
		{"sub-block", bs / 2},                    // < 1 block: whole-cached
		{"exact-one-block", bs},                  // == 1 block
		{"one-block-plus-partial", bs + bs/2},    // > 1 block, < 2 blocks (partial 2nd block)
		{"exact-two-blocks", bs * 2},             // == 2 blocks
		{"multi-block-partial-tail", bs*3 + 111}, // 4 blocks, last partial
		{"many-blocks", bs*8 + 7},                // 9 blocks, last partial
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bucket, err := globalEnv.CreateTestBucket("blk-" + tc.name)
			if err != nil {
				t.Fatalf("Failed to create test bucket: %v", err)
			}
			key := "obj"
			data := make([]byte, tc.size)
			if _, err := rand.Read(data); err != nil {
				t.Fatalf("Failed to generate random data: %v", err)
			}
			if err := globalEnv.PutTestObject(bucket, key, data); err != nil {
				t.Fatalf("Failed to put test object: %v", err)
			}

			// Cold full GET (populates the cache) must return the full object.
			cold, err := globalEnv.DoGet(bucket, key)
			if err != nil {
				t.Fatalf("cold full GET failed: %v", err)
			}
			if !bytes.Equal(cold, data) {
				t.Fatalf("cold full GET: got %d bytes, want %d", len(cold), len(data))
			}

			// Warm full GET served from cache (block-assembled when >= block size) must match.
			waitForCacheHitBody(t, bucket, key, data, 10*time.Second)

			// Warm sub-range reads from the (now fully populated) entry.
			last := int64(tc.size - 1)
			// Small range inside the first block.
			end0 := min(int64(31), last)
			getRangeExpectHit(t, bucket, key, 0, end0, data[0:end0+1])
			// Tail of the object (crosses into the last, possibly partial, block).
			tailStart := max(last-16, 0)
			getRangeExpectHit(t, bucket, key, tailStart, last, data[tailStart:last+1])
			// Cross a block boundary and cover a full interior block when the object is big enough.
			if tc.size > bs {
				bStart := int64(bs - 16)
				bEnd := min(int64(bs+16), last)
				getRangeExpectHit(t, bucket, key, bStart, bEnd, data[bStart:bEnd+1])
			}
			if tc.size >= 3*bs {
				// An entire interior block [bs, 2*bs-1] — the aligned multi-block assembly case.
				getRangeExpectHit(t, bucket, key, int64(bs), int64(2*bs-1), data[bs:2*bs])
			}
		})
	}
}

// waitForRangeCacheHit polls a Range GET until it is served from cache (206, X-Cache: HIT) and
// verifies every served response (cache HIT or upstream MISS) carries exactly the requested bytes.
// The block-mode bug returned X-Cache HIT with a 0-byte body once the (empty) block-mode meta was
// populated, so a HIT with the wrong length fails here.
func waitForRangeCacheHit(t *testing.T, bucket, key string, start, end int64, want []byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastCache string
	for time.Now().Before(deadline) {
		resp, err := globalEnv.DoRawGetWithHeaders(bucket, key, map[string]string{
			"Range": fmt.Sprintf("bytes=%d-%d", start, end),
		})
		if err != nil {
			t.Fatalf("%s/%s range %d-%d: GET failed: %v", bucket, key, start, end, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("%s/%s range %d-%d: status %d, want 206", bucket, key, start, end, resp.StatusCode)
		}
		if !bytes.Equal(body, want) {
			t.Fatalf("%s/%s range %d-%d (X-Cache=%s): got %d bytes, want %d (block range serve truncation)",
				bucket, key, start, end, resp.Header.Get("X-Cache"), len(body), len(want))
		}
		lastCache = resp.Header.Get("X-Cache")
		if lastCache == "HIT" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s/%s range %d-%d: never served from cache (last X-Cache=%s) within %v", bucket, key, start, end, lastCache, timeout)
}

// TestSDK_BlockCache_RangeFirst_MultiBlock is the direct regression for the warp block-mode
// failure. Unlike the full-GET path (which populates every block via putBlocksFromStream, a
// block-split of the whole body), the FIRST access here is a Range GET — so blocks are populated
// through the range path (triggerBlockModePopulate -> fetchOneBlock), which probes block presence
// before fetching. When that presence probe wrongly reported an absent block as present, the block
// was never stored yet the block-mode meta was, and the warm Range GET streamed 0 bytes under a
// committed Content-Length. Each range is read cold (populates its covering blocks) then warm
// (served from those blocks) with the served bytes verified exactly.
func TestSDK_BlockCache_RangeFirst_MultiBlock(t *testing.T) {
	bs := blockSizeForTests()
	size := bs*4 + 123 // 5 blocks, last partial

	bucket, err := globalEnv.CreateTestBucket("blk-rangefirst")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}
	key := "obj"
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}
	if err := globalEnv.PutTestObject(bucket, key, data); err != nil {
		t.Fatalf("Failed to put test object: %v", err)
	}
	// Deliberately NO full GET here — the first access to every block is via a Range, so
	// population goes through the range path, not the whole-object block split.

	ranges := []struct {
		name       string
		start, end int64
	}{
		{"interior-aligned-block", int64(bs), int64(2*bs - 1)},    // exactly one interior block
		{"cross-block-boundary", int64(bs - 32), int64(bs + 32)},  // spans blocks 0 and 1
		{"sub-block", 100, 199},                                   // inside block 0
		{"spans-three-blocks", int64(bs - 8), int64(3*bs + 8)},    // touches blocks 0..3
		{"tail-partial-block", int64(size - 50), int64(size - 1)}, // last, partial block
	}
	for _, r := range ranges {
		t.Run(r.name, func(t *testing.T) {
			waitForRangeCacheHit(t, bucket, key, r.start, r.end, data[r.start:r.end+1], 15*time.Second)
		})
	}
}
