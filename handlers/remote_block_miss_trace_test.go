package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/tigrisdata/tag/cache"
)

// TestRemoteBlockMissWaterfall exercises the ordinary GET router path while a
// remote cache owner is deliberately stalled. It records the full timed
// waterfall and proves that the 206 completes before the remote put does.
func TestRemoteBlockMissWaterfall(t *testing.T) {
	fixture := newRemoteBlockMissBenchmarkFixture(t)
	const (
		bucket = "benchmark"
		key    = "waterfall"
	)
	meta := fixture.seedMeta(t, bucket, key)
	fixture.beginMiss()
	releasePut := fixture.owner.blockPuts()
	defer releasePut()
	fixture.trace.reset()

	type result struct {
		status      int
		cacheStatus string
		body        []byte
		err         error
	}
	results := make(chan result, 1)
	go func() {
		status, cacheStatus, body, err := fixture.getRange(context.Background(), bucket, key)
		results <- result{status: status, cacheStatus: cacheStatus, body: body, err: err}
	}()

	fixture.owner.waitForPutStart(t)
	select {
	case got := <-results:
		if got.err != nil {
			t.Fatalf("GET through handlers.Server = %v", got.err)
		}
		fixture.requireResponse(t, got.status, got.cacheStatus, got.body)
	case <-time.After(2 * time.Second):
		t.Fatal("GET waited for the stalled remote cache write")
	}

	releasePut()
	fixture.owner.waitForPut(t)
	fixture.remoteClient.waitForPutReturn(t)
	blockKey := cache.MakeBlockKey(bucket, key, meta.ETag, meta.BlockSize, 0)
	if !fixture.owner.hasBlock(blockKey, fixture.body) {
		t.Fatal("remote owner was not populated after the detached write completed")
	}
	fixture.trace.requireOrder(t, "cache-miss-probe", "upstream-fetch", "response-complete", "remote-cache-put")
	if rereads := fixture.trace.postFetchRereads(); rereads != 0 {
		t.Fatalf("post-fetch cache rereads = %d, want 0", rereads)
	}
	events, stageSum, cacheReads := fixture.trace.snapshot()
	if cacheReads != 1 || len(events) == 0 || stageSum <= 0 {
		t.Fatalf("trace = (cache reads=%d, events=%d, stage sum=%s), want one miss probe and timed stages", cacheReads, len(events), stageSum)
	}
	t.Logf("remote range-miss waterfall: %s; summed stage latency: %s", fixture.trace.String(), stageSum)
}
