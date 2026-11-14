package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

const (
	assembledRangeBlockSize = int64(4 << 20)
	smallRangeSize          = int64(64 << 10)
	rangeStagingTestBudget  = int64(32 << 20)
)

type rangeWriteObservation struct {
	capacity int
	status   int
}

type rangeStagingResponseWriter struct {
	header      http.Header
	status      int
	started     chan<- rangeWriteObservation
	release     <-chan struct{}
	writeErr    error
	captureBody bool
	body        []byte
}

func (w *rangeStagingResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *rangeStagingResponseWriter) WriteHeader(status int) { w.status = status }

func (w *rangeStagingResponseWriter) Write(p []byte) (int, error) {
	if w.started != nil {
		w.started <- rangeWriteObservation{capacity: cap(p), status: w.status}
	}
	if w.release != nil {
		<-w.release
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if w.captureBody {
		w.body = append(w.body, p...)
	}
	return len(p), nil
}

// startRangeStagingRequest releases and joins the request before earlier pool cleanup runs,
// even if the test fails while the response is held inside Write.
func startRangeStagingRequest(t *testing.T, svc *Service, w *rangeStagingResponseWriter, r *http.Request) func() error {
	t.Helper()
	release := make(chan struct{})
	w.release = release
	done := make(chan struct{})
	var err error
	finish := func() error {
		select {
		case <-release:
		default:
			close(release)
		}
		<-done
		return err
	}
	t.Cleanup(func() { finish() })
	go func() {
		err = svc.HandleGetObject(w, r)
		close(done)
	}()
	return finish
}

func newRangeStagingService(tb testing.TB, budget int64) (*Service, *cache.Cache, *cache.CachedObjectMeta, []byte) {
	tb.Helper()
	body := make([]byte, assembledRangeBlockSize)
	for i := range body {
		body[i] = byte(i * 31)
	}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = assembledRangeBlockSize
	cfg.Cache.SizeThreshold = assembledRangeBlockSize * 2
	cfg.Cache.MaxPopulateMemoryBytes = budget
	store := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	mock := newBlockMock(body, `"staging-capacity"`)
	svc := NewService(mock, store, cfg)
	meta := &cache.CachedObjectMeta{
		Bucket:        wowBucket,
		Key:           "range-staging-capacity",
		ETag:          `"staging-capacity"`,
		ContentLength: assembledRangeBlockSize,
		StatusCode:    http.StatusOK,
		BlockSize:     assembledRangeBlockSize,
	}
	if wrote, err := store.PutMetaIfVersion(context.Background(), meta.Bucket, meta.Key, meta, 60, cache.VersionAny); err != nil || !wrote {
		tb.Fatalf("seed metadata = (wrote=%t, err=%v)", wrote, err)
	}
	if err := store.PutBlock(context.Background(), meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0, body, 60); err != nil {
		tb.Fatalf("seed block: %v", err)
	}
	return svc, store, meta, body
}

func TestBlockCache_AssembledRangeCapacityMatchesStagingReservation(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	oldGC := debug.SetGCPercent(-1)
	oldPoolLimit := maxPooledBlockBufBytes.Load()
	blockBufPool = sync.Pool{}
	maxPooledBlockBufBytes.Store(4 << 20)
	t.Cleanup(func() {
		blockBufPool = sync.Pool{}
		maxPooledBlockBufBytes.Store(oldPoolLimit)
		debug.SetGCPercent(oldGC)
		runtime.GOMAXPROCS(oldProcs)
	})

	svc, _, meta, body := newRangeStagingService(t, rangeStagingTestBudget)
	blockRange := fmt.Sprintf("bytes=0-%d", assembledRangeBlockSize-1)
	smallRange := fmt.Sprintf("bytes=0-%d", smallRangeSize-1)

	// A full-block range returns a 4 MiB buffer to the shared pool before the small ranges.
	fullResponse := &rangeStagingResponseWriter{}
	if err := svc.HandleGetObject(fullResponse, blockGet(meta.Bucket, meta.Key, blockRange)); err != nil {
		t.Fatalf("full-block range: %v", err)
	}
	if fullResponse.status != http.StatusPartialContent {
		t.Fatalf("full-block status = %d, want 206", fullResponse.status)
	}

	entered := make(chan rangeWriteObservation, 2)
	waitForWrite := func() rangeWriteObservation {
		t.Helper()
		select {
		case observation := <-entered:
			if observation.status != http.StatusPartialContent {
				t.Fatalf("held small range status = %d, want 206", observation.status)
			}
			return observation
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for small range write")
			return rangeWriteObservation{}
		}
	}
	finishFirst := startRangeStagingRequest(t, svc, &rangeStagingResponseWriter{started: entered}, blockGet(meta.Bucket, meta.Key, smallRange))
	first := waitForWrite()
	_, inUse := stagingBudgetSnapshot(svc.populateBudget)
	if int64(first.capacity) > inUse {
		t.Errorf("active assembled range backing capacity is not covered by its staging reservation: capacity=%d reserved=%d", first.capacity, inUse)
	}

	// Another service raises the process-wide pool cap while the first response is still held.
	largerConfig := config.NewDefault()
	largerConfig.Cache.SetBlockCachingEnabled(true)
	largerConfig.Cache.BlockSize = 2 * assembledRangeBlockSize
	largerConfig.Cache.SizeThreshold = 4 * assembledRangeBlockSize
	largerConfig.Cache.MaxPopulateMemoryBytes = rangeStagingTestBudget
	largerStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &largerConfig.Cache)
	_ = NewService(newBlockMock(body, `"larger-block"`), largerStore, largerConfig)
	if got := maxPooledBlockBufBytes.Load(); got != 2*assembledRangeBlockSize {
		t.Fatalf("shared block-buffer limit = %d, want %d", got, 2*assembledRangeBlockSize)
	}

	finishSecond := startRangeStagingRequest(t, svc, &rangeStagingResponseWriter{started: entered}, blockGet(meta.Bucket, meta.Key, smallRange))
	second := waitForWrite()
	activeCapacity := int64(first.capacity + second.capacity)
	_, inUse = stagingBudgetSnapshot(svc.populateBudget)
	if activeCapacity > inUse {
		t.Errorf("active assembled range backing capacity is not covered by its staging reservation: capacity=%d reserved=%d", activeCapacity, inUse)
	}

	if err := finishFirst(); err != nil {
		t.Fatalf("small range: %v", err)
	}
	if err := finishSecond(); err != nil {
		t.Fatalf("small range: %v", err)
	}

	writeErr := errors.New("range response write failed")
	errorEntered := make(chan rangeWriteObservation, 1)
	errorWriter := &rangeStagingResponseWriter{started: errorEntered, writeErr: writeErr}
	finishError := startRangeStagingRequest(t, svc, errorWriter, blockGet(meta.Bucket, meta.Key, smallRange))
	var errorObservation rangeWriteObservation
	select {
	case errorObservation = <-errorEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for failing range write")
	}
	if errorObservation.status != http.StatusPartialContent {
		t.Fatalf("held failing range status = %d, want 206", errorObservation.status)
	}
	_, inUse = stagingBudgetSnapshot(svc.populateBudget)
	if int64(errorObservation.capacity) > inUse {
		t.Errorf("active assembled range backing capacity is not covered by its staging reservation: capacity=%d reserved=%d on write error", errorObservation.capacity, inUse)
	}
	if err := finishError(); !errors.Is(err, writeErr) {
		t.Fatalf("write error = %v, want %v", err, writeErr)
	}
}

func TestBlockCache_AssembledRangeWriteRestoresStaging(t *testing.T) {
	blockBufPool = sync.Pool{}
	t.Cleanup(func() { blockBufPool = sync.Pool{} })
	body := make([]byte, assembledRangeBlockSize)
	for i := range body {
		body[i] = byte(i * 31)
	}
	mock := newBlockMock(body, `"staging-capacity"`)
	svc, store, client := newCountingBlockServiceWithBudget(t, mock, assembledRangeBlockSize, rangeStagingTestBudget)
	meta := &cache.CachedObjectMeta{
		Bucket:        wowBucket,
		Key:           "range-staging-capacity",
		ETag:          `"staging-capacity"`,
		ContentLength: assembledRangeBlockSize,
		StatusCode:    http.StatusOK,
		BlockSize:     assembledRangeBlockSize,
	}
	if wrote, err := store.PutMetaIfVersion(context.Background(), meta.Bucket, meta.Key, meta, 60, cache.VersionAny); err != nil || !wrote {
		t.Fatalf("seed metadata = (wrote=%t, err=%v)", wrote, err)
	}
	if err := store.PutBlock(context.Background(), meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0, body, 60); err != nil {
		t.Fatalf("seed block: %v", err)
	}
	smallRange := fmt.Sprintf("bytes=0-%d", smallRangeSize-1)
	client.presenceReads.Store(0)
	client.payloadReads.Store(0)

	w := httptest.NewRecorder()
	if err := svc.HandleGetObject(w, blockGet(meta.Bucket, meta.Key, smallRange)); err != nil {
		t.Fatalf("small range: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Header().Get("Content-Range") != fmt.Sprintf("bytes 0-%d/%d", smallRangeSize-1, assembledRangeBlockSize) || w.Header().Get("X-Cache") != XCacheHit || !bytes.Equal(w.Body.Bytes(), body[:smallRangeSize]) {
		t.Fatalf("small range response = (status=%d, content-range=%q, cache=%q, bytes=%d, exact=%t), want exact 206/HIT", w.Code, w.Header().Get("Content-Range"), w.Header().Get("X-Cache"), w.Body.Len(), bytes.Equal(w.Body.Bytes(), body[:smallRangeSize]))
	}
	if got := client.presenceReads.Load(); got != 0 {
		t.Fatalf("warm assembled range presence reads = %d, want 0", got)
	}
	if got := client.payloadReads.Load(); got != 1 {
		t.Fatalf("warm assembled range payload reads = %d, want 1", got)
	}
	remaining, inUse := stagingBudgetSnapshot(svc.populateBudget)
	if remaining != rangeStagingTestBudget || inUse != 0 {
		t.Fatalf("staging budget after successful write = (remaining=%d, inUse=%d), want (%d, 0)", remaining, inUse, rangeStagingTestBudget)
	}

	writeErr := errors.New("range response write failed")
	errorWriter := &rangeStagingResponseWriter{writeErr: writeErr}
	if err := svc.HandleGetObject(errorWriter, blockGet(meta.Bucket, meta.Key, smallRange)); !errors.Is(err, writeErr) {
		t.Fatalf("write error = %v, want %v", err, writeErr)
	}
	if errorWriter.status != http.StatusPartialContent {
		t.Fatalf("failing write status = %d, want 206", errorWriter.status)
	}
	remaining, inUse = stagingBudgetSnapshot(svc.populateBudget)
	if remaining != rangeStagingTestBudget || inUse != 0 {
		t.Fatalf("staging budget after failed write = (remaining=%d, inUse=%d), want (%d, 0)", remaining, inUse, rangeStagingTestBudget)
	}
}

func TestBlockCache_AssembledRangeFitsSmallStagingBudget(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	oldGC := debug.SetGCPercent(-1)
	blockBufPool = sync.Pool{}
	t.Cleanup(func() {
		blockBufPool = sync.Pool{}
		debug.SetGCPercent(oldGC)
		runtime.GOMAXPROCS(oldProcs)
	})
	body := make([]byte, assembledRangeBlockSize)
	mock := newBlockMock(body, `"staging-capacity"`)
	svc, store, client := newCountingBlockServiceWithBudget(t, mock, assembledRangeBlockSize, assembledRangeBlockSize)
	meta := &cache.CachedObjectMeta{
		Bucket:        wowBucket,
		Key:           "range-staging-exact-capacity",
		ETag:          `"staging-capacity"`,
		ContentLength: assembledRangeBlockSize,
		StatusCode:    http.StatusOK,
		BlockSize:     assembledRangeBlockSize,
	}
	if wrote, err := store.PutMetaIfVersion(context.Background(), meta.Bucket, meta.Key, meta, 60, cache.VersionAny); err != nil || !wrote {
		t.Fatalf("seed metadata = (wrote=%t, err=%v)", wrote, err)
	}
	if err := store.PutBlock(context.Background(), meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0, body, 60); err != nil {
		t.Fatalf("seed block: %v", err)
	}
	large := make([]byte, assembledRangeBlockSize)
	blockBufPool.Put(&large)

	entered := make(chan rangeWriteObservation, 1)
	writer := &rangeStagingResponseWriter{started: entered, captureBody: true}
	rangeHeader := fmt.Sprintf("bytes=0-%d", smallRangeSize-1)
	finish := startRangeStagingRequest(t, svc, writer, blockGet(meta.Bucket, meta.Key, rangeHeader))
	var observation rangeWriteObservation
	select {
	case observation = <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for exact-capacity range write")
	}
	if observation.status != http.StatusPartialContent || int64(observation.capacity) != smallRangeSize {
		t.Fatalf("exact-capacity write = (status=%d, capacity=%d), want (206, %d)", observation.status, observation.capacity, smallRangeSize)
	}
	_, inUse := stagingBudgetSnapshot(svc.populateBudget)
	if inUse != smallRangeSize {
		t.Fatalf("exact-capacity stagingInUse = %d, want %d", inUse, smallRangeSize)
	}
	if got := client.presenceReads.Load(); got != 0 {
		t.Fatalf("exact-capacity presence reads = %d, want 0", got)
	}
	if got := client.payloadReads.Load(); got != 1 {
		t.Fatalf("exact-capacity payload reads = %d, want 1", got)
	}
	if err := finish(); err != nil {
		t.Fatalf("exact-capacity range: %v", err)
	}
	if writer.status != http.StatusPartialContent || !bytes.Equal(writer.body, body[:smallRangeSize]) {
		t.Fatalf("exact-capacity response = (status=%d, bytes=%d, exact=%t), want exact 206 body", writer.status, len(writer.body), bytes.Equal(writer.body, body[:smallRangeSize]))
	}
	remaining, inUse := stagingBudgetSnapshot(svc.populateBudget)
	if remaining != assembledRangeBlockSize || inUse != 0 {
		t.Fatalf("staging budget after exact-capacity serve = (remaining=%d, inUse=%d), want (%d, 0)", remaining, inUse, assembledRangeBlockSize)
	}
}

func TestBlockCache_AssembledRangeCapacityDeclineUsesProbePath(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	oldGC := debug.SetGCPercent(-1)
	blockBufPool = sync.Pool{}
	t.Cleanup(func() {
		blockBufPool = sync.Pool{}
		debug.SetGCPercent(oldGC)
		runtime.GOMAXPROCS(oldProcs)
	})
	body := make([]byte, assembledRangeBlockSize)
	mock := newBlockMock(body, `"staging-capacity"`)
	svc, store, client := newCountingBlockServiceWithBudget(t, mock, assembledRangeBlockSize, smallRangeSize)
	meta := &cache.CachedObjectMeta{
		Bucket:        wowBucket,
		Key:           "range-staging-capacity-decline",
		ETag:          `"staging-capacity"`,
		ContentLength: assembledRangeBlockSize,
		StatusCode:    http.StatusOK,
		BlockSize:     assembledRangeBlockSize,
	}
	if wrote, err := store.PutMetaIfVersion(context.Background(), meta.Bucket, meta.Key, meta, 60, cache.VersionAny); err != nil || !wrote {
		t.Fatalf("seed metadata = (wrote=%t, err=%v)", wrote, err)
	}
	if err := store.PutBlock(context.Background(), meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, 0, body, 60); err != nil {
		t.Fatalf("seed block: %v", err)
	}

	large := make([]byte, assembledRangeBlockSize)
	blockBufPool.Put(&large)

	w := httptest.NewRecorder()
	rangeHeader := fmt.Sprintf("bytes=0-%d", smallRangeSize-1)
	if err := svc.HandleGetObject(w, blockGet(meta.Bucket, meta.Key, rangeHeader)); err != nil {
		t.Fatalf("probe-first range: %v", err)
	}
	if w.Code != http.StatusPartialContent || w.Body.Len() != int(smallRangeSize) || !bytes.Equal(w.Body.Bytes(), body[:smallRangeSize]) || w.Header().Get("X-Cache") != XCacheHit {
		t.Fatalf("probe-first response = (status=%d, bytes=%d, exact=%t, cache=%q), want exact 206/HIT", w.Code, w.Body.Len(), bytes.Equal(w.Body.Bytes(), body[:smallRangeSize]), w.Header().Get("X-Cache"))
	}
	if got := client.presenceReads.Load(); got != 1 {
		t.Fatalf("probe-first presence reads = %d, want 1", got)
	}
	if got := client.payloadReads.Load(); got != 1 {
		t.Fatalf("probe-first payload reads = %d, want 1", got)
	}
	remaining, inUse := stagingBudgetSnapshot(svc.populateBudget)
	if remaining != smallRangeSize || inUse != 0 {
		t.Fatalf("staging budget after probe-first response = (remaining=%d, inUse=%d), want (%d, 0)", remaining, inUse, smallRangeSize)
	}
}

func stagingBudgetSnapshot(b *byteBudget) (remaining, inUse int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remaining, b.stagingInUse
}
