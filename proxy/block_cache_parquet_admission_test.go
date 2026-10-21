package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
)

const (
	admissionTestBlockSize = 1024
	admissionTestBlocks    = 6
	admissionTestFooterLen = 2644
)

type admissionProbeCacheClient struct {
	cacheclient.CacheClient

	gate    <-chan struct{}
	started chan<- string

	mu           sync.Mutex
	calls        int
	active       int
	peak         int
	trailerKey   string
	trailerStart int64
	trailerEnd   int64
	trailerErr   error
	trailerReads int
}

func (c *admissionProbeCacheClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	c.mu.Lock()
	c.calls++
	trailerRead := key == c.trailerKey && start == c.trailerStart && end == c.trailerEnd
	trailerErr := c.trailerErr
	if trailerRead {
		c.trailerReads++
	}
	c.mu.Unlock()
	if trailerRead && trailerErr != nil {
		return trailerErr
	}

	// Cache.BlockExists probes [0,0], which Cache adapts to [0,1] for the
	// ocache client's byte-zero range quirk. Hold that remote-like probe until
	// the test releases the gate.
	if start != 0 || end != 1 {
		return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
	}

	c.mu.Lock()
	c.active++
	if c.active > c.peak {
		c.peak = c.active
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
	}()

	c.started <- key
	select {
	case <-c.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

// admissionFooterForwarder supplies a valid suffix trailer for write warming
// and delegates aligned block ranges to the existing Parquet test forwarder.
type admissionFooterForwarder struct {
	parquetFooterForwarder
}

func (f *admissionFooterForwarder) DoConditionalGetRequest(ctx context.Context, bucket, key, accessKey, secretKey, etag string, lastModified int64, rangeHeader string) (*http.Response, error) {
	if rangeHeader != fmt.Sprintf("bytes=-%d", parquetTrailerSize) {
		return f.parquetFooterForwarder.DoConditionalGetRequest(ctx, bucket, key, accessKey, secretKey, etag, lastModified, rangeHeader)
	}

	total := int64(len(f.body))
	start := total - parquetTrailerSize
	header := make(http.Header)
	header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, total-1, total))
	header.Set("ETag", f.etag)
	return &http.Response{
		StatusCode:    http.StatusPartialContent,
		ContentLength: parquetTrailerSize,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(f.body[start:])),
	}, nil
}

type parquetAdmissionFixture struct {
	service  *Service
	store    *cache.Cache
	client   *admissionProbeCacheClient
	body     []byte
	gate     chan struct{}
	started  chan string
	gateOnce sync.Once
}

func (f *parquetAdmissionFixture) release() {
	f.gateOnce.Do(func() { close(f.gate) })
}

func newParquetAdmissionFixture(t *testing.T) *parquetAdmissionFixture {
	t.Helper()

	contentLength := admissionTestBlockSize * admissionTestBlocks
	body := make([]byte, contentLength)
	for i := range body {
		body[i] = byte(i)
	}
	binary.LittleEndian.PutUint32(body[contentLength-parquetTrailerSize:], admissionTestFooterLen)
	copy(body[contentLength-4:], parquetMagic)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = admissionTestBlockSize
	cfg.Cache.ParquetOptimization = true

	base := cacheclient.NewMemoryCache()
	gate := make(chan struct{})
	started := make(chan string, 256)
	client := &admissionProbeCacheClient{CacheClient: base, gate: gate, started: started}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	forwarder := &admissionFooterForwarder{
		parquetFooterForwarder: parquetFooterForwarder{body: body, etag: `"v1"`},
	}
	return &parquetAdmissionFixture{
		service: NewService(forwarder, store, cfg),
		store:   store,
		client:  client,
		body:    body,
		gate:    gate,
		started: started,
	}
}

func (f *parquetAdmissionFixture) seedObject(t *testing.T, key string) *cache.CachedObjectMeta {
	t.Helper()
	meta := &cache.CachedObjectMeta{
		Bucket:        "b",
		Key:           key,
		ETag:          `"v1"`,
		ContentLength: int64(len(f.body)),
		StatusCode:    http.StatusOK,
		BlockSize:     admissionTestBlockSize,
	}
	if wrote, err := f.store.PutMetaIfVersion(context.Background(), "b", key, meta, 3600, cache.VersionAny); err != nil || !wrote {
		t.Fatalf("seed meta for %s = (wrote=%t, err=%v)", key, wrote, err)
	}
	tail := int64(admissionTestBlocks - 1)
	if err := f.store.PutBlock(context.Background(), "b", key, meta.ETag, admissionTestBlockSize, tail, f.body[tail*admissionTestBlockSize:], 3600); err != nil {
		t.Fatalf("seed tail block for %s: %v", key, err)
	}
	return meta
}

func (f *parquetAdmissionFixture) serveTail(t *testing.T, key string) *httptest.ResponseRecorder {
	t.Helper()
	contentLength := int64(len(f.body))
	rangeHeader := fmt.Sprintf("bytes=%d-%d", contentLength-parquetTrailerSize, contentLength-1)
	req := httptest.NewRequest(http.MethodGet, "/b/"+key, nil)
	req.Header.Set("Range", rangeHeader)
	rec := httptest.NewRecorder()
	if err := f.service.HandleGetObject(rec, req); err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("GET %s status = %d, want 206", key, rec.Code)
	}
	if got, want := rec.Body.Bytes(), f.body[len(f.body)-parquetTrailerSize:]; !bytes.Equal(got, want) {
		t.Fatalf("GET %s body = %x, want trailer %x", key, got, want)
	}
	return rec
}

func countBackgroundFetchesWithPrefix(s *Service, prefix string) int {
	count := 0
	s.activeBackgroundFetches.Range(func(k, _ any) bool {
		if key, ok := k.(string); ok && len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			count++
		}
		return true
	})
	return count
}

func waitForAdmissionProbeStarts(t *testing.T, started <-chan string, count int) []string {
	t.Helper()
	keys := make([]string, 0, count)
	for len(keys) < count {
		select {
		case key := <-started:
			keys = append(keys, key)
		case <-time.After(5 * time.Second):
			t.Fatalf("got %d of %d remote footer probes", len(keys), count)
		}
	}
	return keys
}

func waitForBackgroundFetches(t *testing.T, s *Service) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		active := 0
		s.activeBackgroundFetches.Range(func(_, _ any) bool {
			active++
			return true
		})
		slots := len(s.parquetFooterPrefetchSlots)
		if active == 0 && slots == 0 {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("background footer work did not finish: markers=%d read_slots=%d", active, slots)
		}
	}
}

func drainAdmissionProbeStarts(started <-chan string) {
	for {
		select {
		case <-started:
		default:
			return
		}
	}
}

func TestMaybePrefetchParquetFooter_GlobalLimitShedsAndRetriesWithoutCooldown(t *testing.T) {
	f := newParquetAdmissionFixture(t)
	f.service.parquetFooterPrefetchSlots = make(chan struct{}, 1)
	t.Cleanup(f.release)
	f.seedObject(t, "a.parquet")
	f.seedObject(t, "b.parquet")

	f.serveTail(t, "a.parquet")
	waitForAdmissionProbeStarts(t, f.started, 1)
	f.serveTail(t, "b.parquet") // the foreground 206 must not wait for the occupied slot

	if got := countBackgroundFetchesWithPrefix(f.service, "pq:"); got != 1 {
		t.Fatalf("active read-prefetch markers = %d, want 1", got)
	}
	f.client.mu.Lock()
	active, peak := f.client.active, f.client.peak
	f.client.mu.Unlock()
	if active != 1 || peak != 1 {
		t.Fatalf("blocked remote probes active/peak = %d/%d, want 1/1", active, peak)
	}
	if _, recent := f.service.recentFooterWork.Get("b/b.parquet/\"v1\""); recent {
		t.Fatal("declined version was cooled down instead of remaining retryable")
	}

	f.release()
	waitForBackgroundFetches(t, f.service)
	if got := len(f.service.parquetFooterPrefetchSlots); got != 0 {
		t.Fatalf("read-prefetch slots after completion = %d, want 0", got)
	}
	drainAdmissionProbeStarts(f.started)

	f.serveTail(t, "b.parquet")
	waitForAdmissionProbeStarts(t, f.started, 1)
	waitForBackgroundFetches(t, f.service)
	if _, recent := f.service.recentFooterWork.Get("b/b.parquet/\"v1\""); !recent {
		t.Fatal("retried version did not complete and start its cooldown")
	}
}

func TestMaybePrefetchParquetFooter_DuplicateReleasesAdmissionSlot(t *testing.T) {
	f := newParquetAdmissionFixture(t)
	f.service.parquetFooterPrefetchSlots = make(chan struct{}, 2)
	t.Cleanup(f.release)
	f.seedObject(t, "a.parquet")
	f.seedObject(t, "b.parquet")

	f.serveTail(t, "a.parquet")
	waitForAdmissionProbeStarts(t, f.started, 1)
	f.serveTail(t, "a.parquet") // coalesces into the active version
	if got := countBackgroundFetchesWithPrefix(f.service, "pq:"); got != 1 {
		t.Fatalf("same-version duplicate changed active markers to %d, want 1", got)
	}
	f.serveTail(t, "b.parquet") // requires the duplicate to have returned its spare permit
	waitForAdmissionProbeStarts(t, f.started, 1)
	if got := countBackgroundFetchesWithPrefix(f.service, "pq:"); got != 2 {
		t.Fatalf("active read-prefetch markers after distinct trigger = %d, want 2", got)
	}
	f.client.mu.Lock()
	active, peak := f.client.active, f.client.peak
	f.client.mu.Unlock()
	if active != 2 || peak != 2 {
		t.Fatalf("blocked remote probes active/peak = %d/%d, want 2/2", active, peak)
	}

	f.release()
	waitForBackgroundFetches(t, f.service)
	if got := len(f.service.parquetFooterPrefetchSlots); got != 0 {
		t.Fatalf("read-prefetch slots after completion = %d, want 0", got)
	}
}

func TestMaybePrefetchParquetFooter_TransientTrailerReadRemainsRetryable(t *testing.T) {
	f := newParquetAdmissionFixture(t)
	meta := &cache.CachedObjectMeta{
		Bucket:        "b",
		Key:           "streamed.parquet",
		ETag:          `"v1"`,
		BlockSize:     admissionTestBlockSize,
		ContentLength: int64(len(f.body)),
	}
	tail := int64(admissionTestBlocks - 1)
	if err := f.store.PutBlock(context.Background(), "b", meta.Key, meta.ETag, meta.BlockSize, tail, f.body[tail*meta.BlockSize:], 3600); err != nil {
		t.Fatal(err)
	}
	trailerKey := cache.MakeBlockKey("b", meta.Key, meta.ETag, meta.BlockSize, tail)
	f.client.mu.Lock()
	f.client.trailerKey = trailerKey
	f.client.trailerStart = meta.BlockSize - parquetTrailerSize
	f.client.trailerEnd = meta.BlockSize - 1
	f.client.trailerErr = errors.New("temporary remote cache read failure")
	f.client.mu.Unlock()

	f.service.maybePrefetchParquetFooter(
		"b", meta.Key, "access", "secret", meta,
		meta.ContentLength-parquetTrailerSize, meta.ContentLength-1, nil,
	)
	waitForBackgroundFetches(t, f.service)
	if _, recent := f.service.recentFooterWork.Get("b/streamed.parquet/\"v1\""); recent {
		t.Fatal("transient trailer-cache error started a cooldown")
	}

	f.release()
	f.client.mu.Lock()
	f.client.trailerErr = nil
	f.client.mu.Unlock()
	f.service.maybePrefetchParquetFooter(
		"b", meta.Key, "access", "secret", meta,
		meta.ContentLength-parquetTrailerSize, meta.ContentLength-1, nil,
	)
	waitForBackgroundFetches(t, f.service)
	if _, recent := f.service.recentFooterWork.Get("b/streamed.parquet/\"v1\""); !recent {
		t.Fatal("successful retry did not start the cooldown")
	}
	f.client.mu.Lock()
	trailerReads := f.client.trailerReads
	f.client.mu.Unlock()
	if trailerReads != 2 {
		t.Fatalf("trailer-cache reads = %d, want failed attempt plus retry", trailerReads)
	}
}

func TestMaybePrefetchParquetFooter_StableInvalidTrailerStartsCooldown(t *testing.T) {
	f := newParquetAdmissionFixture(t)
	meta := &cache.CachedObjectMeta{
		Bucket:        "b",
		Key:           "not-parquet.parquet",
		ETag:          `"v1"`,
		BlockSize:     admissionTestBlockSize,
		ContentLength: int64(len(f.body)),
	}
	trailer := make([]byte, parquetTrailerSize)
	copy(trailer[4:], "NOPE")

	f.service.maybePrefetchParquetFooter(
		"b", meta.Key, "access", "secret", meta,
		meta.ContentLength-parquetTrailerSize, meta.ContentLength-1, trailer,
	)
	waitForBackgroundFetches(t, f.service)
	if _, recent := f.service.recentFooterWork.Get("b/not-parquet.parquet/\"v1\""); !recent {
		t.Fatal("stable non-Parquet trailer did not start the cooldown")
	}
}

func TestMaybePrefetchParquetFooter_StreamedTriggerShedsBeforeTrailerRead(t *testing.T) {
	f := newParquetAdmissionFixture(t)
	f.service.parquetFooterPrefetchSlots = make(chan struct{}, 1)
	f.service.parquetFooterPrefetchSlots <- struct{}{} // model a saturated service
	meta := &cache.CachedObjectMeta{
		ETag:          `"v1"`,
		BlockSize:     admissionTestBlockSize,
		ContentLength: int64(len(f.body)),
	}
	f.client.mu.Lock()
	f.client.calls = 0
	f.client.mu.Unlock()

	shedBefore := testutil.ToFloat64(metrics.CacheParquetFooterPrefetchShed)
	f.service.maybePrefetchParquetFooter(
		"b", "streamed.parquet", "access", "secret", meta,
		meta.ContentLength-parquetTrailerSize, meta.ContentLength-1, nil,
	)
	if got := testutil.ToFloat64(metrics.CacheParquetFooterPrefetchShed) - shedBefore; got != 1 {
		t.Fatalf("footer-prefetch shed count delta = %v, want 1", got)
	}
	f.client.mu.Lock()
	calls := f.client.calls
	f.client.mu.Unlock()
	if calls != 0 {
		t.Fatalf("declined streamed trigger performed %d cache reads before admission", calls)
	}
	if got := countBackgroundFetchesWithPrefix(f.service, "pq:"); got != 0 {
		t.Fatalf("declined streamed trigger installed %d markers, want 0", got)
	}
	if got := len(f.started); got != 0 {
		t.Fatalf("declined streamed trigger started %d remote probes, want 0", got)
	}
}

func TestWarmParquetFooterOnWrite_IgnoresReadPrefetchLimit(t *testing.T) {
	f := newParquetAdmissionFixture(t)
	f.service.parquetFooterPrefetchSlots = make(chan struct{}, 1)
	t.Cleanup(f.release)
	f.seedObject(t, "read.parquet")

	f.serveTail(t, "read.parquet")
	waitForAdmissionProbeStarts(t, f.started, 1)

	f.service.warmParquetFooterOnWrite(httptest.NewRequest(http.MethodPut, "/b/write.parquet", nil), "b", "write.parquet")
	if _, ok := f.service.activeBackgroundFetches.Load("pqw:b/write.parquet"); !ok {
		t.Fatal("write-triggered footer warm was blocked by the read-prefetch limit")
	}
	if got := countBackgroundFetchesWithPrefix(f.service, "pq:"); got != 1 {
		t.Fatalf("read-prefetch markers after write trigger = %d, want 1", got)
	}
	waitForAdmissionProbeStarts(t, f.started, 1)

	f.release()
	waitForBackgroundFetches(t, f.service)
	if _, found, err := f.store.GetMeta(context.Background(), "b", "write.parquet"); err != nil || !found {
		t.Fatalf("write warm after read-limit saturation = (found=%t, err=%v), want metadata", found, err)
	}
}
