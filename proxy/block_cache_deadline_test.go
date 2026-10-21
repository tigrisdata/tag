package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

const blockFetchTestBlockSize = int64(4)

type blockFetchDeadlineEvent struct {
	key string
	at  time.Time
}

type blockProbeRecordingClient struct {
	cacheclient.CacheClient
	mu     sync.Mutex
	events []blockFetchDeadlineEvent
}

func (c *blockProbeRecordingClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if strings.HasPrefix(key, "blk|") {
		c.mu.Lock()
		c.events = append(c.events, blockFetchDeadlineEvent{key: key, at: time.Now()})
		c.mu.Unlock()
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

func (c *blockProbeRecordingClient) snapshot() []blockFetchDeadlineEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]blockFetchDeadlineEvent(nil), c.events...)
}

type gatedBlockFetchForwarder struct {
	*blockMockForwarder
	started    chan blockFetchDeadlineEvent
	release    chan struct{}
	staleStart int64
	staleETag  string
	stale      bool
	mu         sync.Mutex
	events     []blockFetchDeadlineEvent
	active     int
}

func (f *gatedBlockFetchForwarder) DoConditionalGetRequest(ctx context.Context, bucket, key, accessKey, secretKey, etag string, lastModified int64, rangeHeader string) (*http.Response, error) {
	startText, _, _ := strings.Cut(strings.TrimPrefix(rangeHeader, "bytes="), "-")
	start, _ := strconv.ParseInt(startText, 10, 64)
	event := blockFetchDeadlineEvent{key: strconv.FormatInt(start, 10), at: time.Now()}
	f.mu.Lock()
	f.events = append(f.events, event)
	f.active++
	f.mu.Unlock()
	f.started <- event
	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()

	if f.stale && start == f.staleStart {
		resp, err := f.blockMockForwarder.DoConditionalGetRequest(ctx, bucket, key, accessKey, secretKey, etag, lastModified, rangeHeader)
		if resp != nil {
			resp.Header.Set("ETag", f.staleETag)
		}
		return resp, err
	}
	<-f.release
	return f.blockMockForwarder.DoConditionalGetRequest(ctx, bucket, key, accessKey, secretKey, etag, lastModified, rangeHeader)
}

func (f *gatedBlockFetchForwarder) snapshot() []blockFetchDeadlineEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]blockFetchDeadlineEvent(nil), f.events...)
}

func (f *gatedBlockFetchForwarder) activeRequests() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active
}

type blockFetchAdmissionSnapshot struct {
	at        time.Time
	states    []string
	probes    []blockFetchDeadlineEvent
	gets      []blockFetchDeadlineEvent
	slots     int
	bytesUsed int64
}

func snapshotBlockFetchAdmission(svc *Service, probes *blockProbeRecordingClient, forwarder *gatedBlockFetchForwarder) blockFetchAdmissionSnapshot {
	svc.blockFetchMu.Lock()
	states := make([]string, 0, len(svc.blockFetches))
	for key := range svc.blockFetches {
		states = append(states, key)
	}
	svc.blockFetchMu.Unlock()

	var bytesUsed int64
	if svc.populateBudget != nil {
		svc.populateBudget.mu.Lock()
		bytesUsed = svc.populateBudget.total - svc.populateBudget.remaining
		svc.populateBudget.mu.Unlock()
	}
	return blockFetchAdmissionSnapshot{
		at:        time.Now(),
		states:    states,
		probes:    probes.snapshot(),
		gets:      forwarder.snapshot(),
		slots:     len(svc.cacheSemaphore),
		bytesUsed: bytesUsed,
	}
}

func waitForBlockFetchCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", description)
		}
	}
}

func waitForBlockFetchStart(t *testing.T, forwarder *gatedBlockFetchForwarder, count int, ctx context.Context) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-forwarder.started:
		case <-ctx.Done():
			t.Fatalf("context was canceled before the first %d block GETs started", count)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for block GET %d of %d", i+1, count)
		}
	}
}

func waitForBlockFetchDrain(t *testing.T, svc *Service, forwarder *gatedBlockFetchForwarder) {
	t.Helper()
	waitForBlockFetchCondition(t, "detached block fetches to release their states and populate slots", func() bool {
		svc.blockFetchMu.Lock()
		states := len(svc.blockFetches)
		svc.blockFetchMu.Unlock()
		return states == 0 && len(svc.cacheSemaphore) == 0 && forwarder.activeRequests() == 0
	})
}

type blockFetchTaskCheckContext struct {
	context.Context
	mu           sync.Mutex
	checks       int
	checkReached chan struct{}
	resumeCheck  chan struct{}
}

func (c *blockFetchTaskCheckContext) Err() error {
	c.mu.Lock()
	c.checks++
	check := c.checks
	c.mu.Unlock()
	if check == 2 {
		// Capture the task-entry result before the test cancels the parent, then
		// pause the task between that check and blockFetchMu admission.
		err := c.Context.Err()
		close(c.checkReached)
		<-c.resumeCheck
		return err
	}
	return c.Context.Err()
}

func summarizeLateBlockFetchEvents(events []blockFetchDeadlineEvent, cutoff time.Time) (int, time.Time, time.Time) {
	count := 0
	var first, last time.Time
	for _, event := range events {
		if !event.at.After(cutoff) {
			continue
		}
		if count == 0 {
			first = event.at
		}
		last = event.at
		count++
	}
	return count, first, last
}

// TestFetchBlocksToCacheStopsAdmissionAfterCancellation verifies that a canceled
// parent stops a populate batch from admitting queued blocks while already-admitted
// block fetches remain detached and can complete.
func TestFetchBlocksToCacheStopsAdmissionAfterCancellation(t *testing.T) {
	const (
		blockSize  = int64(4)
		blockCount = 32
	)
	object := []byte(strings.Repeat("x", int(blockSize)*blockCount))
	mock := newBlockMock(object, `"v1"`)
	forwarder := &gatedBlockFetchForwarder{
		blockMockForwarder: mock,
		started:            make(chan blockFetchDeadlineEvent, blockCount),
		release:            make(chan struct{}),
	}
	probeClient := &blockProbeRecordingClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = int64(len(object))
	cfg.Cache.MaxConcurrentWrites = blockCount * 2
	cfg.Cache.MaxPopulateMemoryBytes = 1 << 20
	store := cache.NewCacheWithClient(probeClient, &cfg.Cache)
	svc := NewService(forwarder, store, cfg)
	meta := &cache.CachedObjectMeta{
		Bucket:             "deadline-bucket",
		Key:                "deadline-key",
		ETag:               `"v1"`,
		ContentLength:      int64(len(object)),
		BlockSize:          blockSize,
		ContentLengthKnown: true,
		StatusCode:         http.StatusOK,
	}
	blocks := make([]int64, blockCount)
	for i := range blocks {
		blocks[i] = int64(i)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- svc.fetchBlocksToCache(ctx, meta.Bucket, meta.Key, "access", "secret", meta, blocks)
	}()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(forwarder.release) }) }
	t.Cleanup(func() {
		cancel()
		release()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fetchBlocksToCache did not return during cleanup")
		}
		waitForBlockFetchDrain(t, svc, forwarder)
	})

	waitForBlockFetchStart(t, forwarder, maxConcurrentBlockFetches, ctx)
	before := snapshotBlockFetchAdmission(svc, probeClient, forwarder)
	if got := len(before.states); got != maxConcurrentBlockFetches {
		t.Fatalf("first wave created %d block states, want %d", got, maxConcurrentBlockFetches)
	}
	if got := len(before.probes); got != maxConcurrentBlockFetches {
		t.Fatalf("first wave made %d block cache probes, want %d", got, maxConcurrentBlockFetches)
	}
	if got := len(before.gets); got != maxConcurrentBlockFetches {
		t.Fatalf("first wave started %d upstream GETs, want %d", got, maxConcurrentBlockFetches)
	}
	if before.slots != maxConcurrentBlockFetches || before.bytesUsed != int64(maxConcurrentBlockFetches)*blockSize {
		t.Fatalf("first wave populate permits = (slots=%d bytes=%d), want (%d slots, %d bytes)", before.slots, before.bytesUsed, maxConcurrentBlockFetches, int64(maxConcurrentBlockFetches)*blockSize)
	}

	// Start cancellation only after the first wave is synchronized at upstream.
	// This keeps the admission check independent of scheduler speed.
	canceledAt := time.Now()
	if !before.at.Before(canceledAt) {
		t.Fatalf("first wave was not observed before cancellation: snapshot=%s canceled_at=%s", before.at, canceledAt)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fetchBlocksToCache did not drain its admitted tasks after cancellation")
	}
	fetchErr := <-result
	afterStates := snapshotBlockFetchAdmission(svc, probeClient, forwarder)
	stateCount := len(afterStates.states)
	waitForBlockFetchCondition(t, "each admitted block state to reach its cache probe, permit, and GET", func() bool {
		snapshot := snapshotBlockFetchAdmission(svc, probeClient, forwarder)
		return len(snapshot.probes) == stateCount && len(snapshot.gets) == stateCount && snapshot.slots == stateCount
	})
	after := snapshotBlockFetchAdmission(svc, probeClient, forwarder)

	stateSet := make(map[string]struct{}, len(before.states))
	for _, key := range before.states {
		stateSet[key] = struct{}{}
	}
	newStates := 0
	for _, key := range after.states {
		if _, found := stateSet[key]; !found {
			newStates++
		}
	}
	lateProbes, firstLateProbe, lastLateProbe := summarizeLateBlockFetchEvents(after.probes, canceledAt)
	lateGets, firstLateGet, lastLateGet := summarizeLateBlockFetchEvents(after.gets, canceledAt)
	if !errors.Is(fetchErr, context.Canceled) || newStates != 0 || len(after.probes) != len(before.probes) || len(after.gets) != len(before.gets) || after.slots != before.slots || after.bytesUsed != before.bytesUsed || lateProbes != 0 || lateGets != 0 {
		t.Errorf("post-cancellation block admission: canceled_at=%s before@%s(states=%d probes=%d GETs=%d slots=%d bytes=%d) after@%s(states=%d probes=%d GETs=%d slots=%d bytes=%d) new_states=%d late_probes=%d[%s..%s] late_GETs=%d[%s..%s] result=%v", canceledAt, before.at, len(before.states), len(before.probes), len(before.gets), before.slots, before.bytesUsed, after.at, len(after.states), len(after.probes), len(after.gets), after.slots, after.bytesUsed, newStates, lateProbes, firstLateProbe, lastLateProbe, lateGets, firstLateGet, lastLateGet, fetchErr)
	}

	// The four states admitted before cancellation are intentionally detached from the
	// batch context; after upstream is released they still finish their cache writes.
	release()
	waitForBlockFetchDrain(t, svc, forwarder)
	for i := 0; i < maxConcurrentBlockFetches; i++ {
		if !store.BlockExists(context.Background(), meta.Bucket, meta.Key, meta.ETag, blockSize, int64(i)) {
			t.Errorf("pre-cancellation admitted block %d did not complete its detached cache write", i)
		}
	}
}

// TestFetchBlocksToCacheRejectsCancellationAfterTaskCheck prevents a task that
// passed its context check before cancellation from admitting state after waiting
// between that check and blockFetchMu.
func TestFetchBlocksToCacheRejectsCancellationAfterTaskCheck(t *testing.T) {
	blockSize := blockFetchTestBlockSize
	object := []byte("data")
	mock := newBlockMock(object, `"v1"`)
	forwarder := &gatedBlockFetchForwarder{
		blockMockForwarder: mock,
		started:            make(chan blockFetchDeadlineEvent, 1),
		release:            make(chan struct{}),
	}
	probeClient := &blockProbeRecordingClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = int64(len(object))
	cfg.Cache.MaxConcurrentWrites = 2
	cfg.Cache.MaxPopulateMemoryBytes = 1 << 20
	store := cache.NewCacheWithClient(probeClient, &cfg.Cache)
	svc := NewService(forwarder, store, cfg)
	meta := &cache.CachedObjectMeta{
		Bucket:             "deadline-bucket",
		Key:                "admission-race-key",
		ETag:               `"v1"`,
		ContentLength:      int64(len(object)),
		BlockSize:          blockSize,
		ContentLengthKnown: true,
		StatusCode:         http.StatusOK,
	}

	parent, cancel := context.WithCancel(context.Background())
	ctx := &blockFetchTaskCheckContext{
		Context:      parent,
		checkReached: make(chan struct{}),
		resumeCheck:  make(chan struct{}),
	}
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- svc.fetchBlocksToCache(ctx, meta.Bucket, meta.Key, "access", "secret", meta, []int64{0})
	}()
	var releaseOnce, resumeOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(forwarder.release) }) }
	resume := func() { resumeOnce.Do(func() { close(ctx.resumeCheck) }) }
	t.Cleanup(func() {
		cancel()
		resume()
		release()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fetchBlocksToCache did not return during cleanup")
		}
		waitForBlockFetchDrain(t, svc, forwarder)
	})

	select {
	case <-ctx.checkReached:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not reach its context check")
	}
	cancel()
	resume()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fetchBlocksToCache did not return after cancellation")
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Errorf("canceled task result = %v, want %v", err, context.Canceled)
	}

	snapshot := snapshotBlockFetchAdmission(svc, probeClient, forwarder)
	if len(snapshot.states) != 0 || len(snapshot.probes) != 0 || len(snapshot.gets) != 0 || snapshot.slots != 0 || snapshot.bytesUsed != 0 {
		t.Errorf("canceled task admitted state or populate work: states=%d probes=%d GETs=%d slots=%d bytes=%d", len(snapshot.states), len(snapshot.probes), len(snapshot.gets), snapshot.slots, snapshot.bytesUsed)
	}
}

// TestFetchBlocksToCacheStaleResultWinsCancellation preserves stale-meta
// invalidation when a stale fetch completed before its batch context was canceled.
func TestFetchBlocksToCacheStaleResultWinsCancellation(t *testing.T) {
	const blockCount = 5
	blockSize := blockFetchTestBlockSize
	object := []byte(strings.Repeat("x", int(blockSize)*blockCount))
	mock := newBlockMock(object, `"v1"`)
	forwarder := &gatedBlockFetchForwarder{
		blockMockForwarder: mock,
		started:            make(chan blockFetchDeadlineEvent, blockCount),
		release:            make(chan struct{}),
		staleStart:         0,
		staleETag:          `"v2"`,
		stale:              true,
	}
	probeClient := &blockProbeRecordingClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = int64(len(object))
	cfg.Cache.MaxConcurrentWrites = blockCount * 2
	cfg.Cache.MaxPopulateMemoryBytes = 1 << 20
	store := cache.NewCacheWithClient(probeClient, &cfg.Cache)
	svc := NewService(forwarder, store, cfg)
	meta := &cache.CachedObjectMeta{
		Bucket:             "deadline-bucket",
		Key:                "stale-key",
		ETag:               `"v1"`,
		ContentLength:      int64(len(object)),
		BlockSize:          blockSize,
		ContentLengthKnown: true,
		StatusCode:         http.StatusOK,
	}
	if wrote, err := store.PutMetaIfVersion(context.Background(), meta.Bucket, meta.Key, meta, 60, cache.VersionAny); err != nil || !wrote {
		t.Fatalf("seed stale metadata: wrote=%t err=%v", wrote, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- svc.fetchBlocksToCache(ctx, meta.Bucket, meta.Key, "access", "secret", meta, []int64{0, 1, 2, 3, 4})
	}()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(forwarder.release) }) }
	t.Cleanup(func() {
		cancel()
		release()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fetchBlocksToCache did not return during cleanup")
		}
		waitForBlockFetchDrain(t, svc, forwarder)
	})

	waitForBlockFetchCondition(t, "the fifth block GET after the first block reports stale", func() bool {
		for _, event := range forwarder.snapshot() {
			if event.key == "16" {
				return true
			}
		}
		return false
	})
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fetchBlocksToCache did not drain after cancellation")
	}
	if err := <-result; !errors.Is(err, errBlockETagMismatch) {
		t.Errorf("stale result after cancellation = %v, want %v", err, errBlockETagMismatch)
	}
	if _, found, err := store.GetMeta(context.Background(), meta.Bucket, meta.Key); err != nil || found {
		t.Errorf("stale metadata after cancellation = (found=%t, err=%v), want absent", found, err)
	}

	release()
	waitForBlockFetchDrain(t, svc, forwarder)
}
