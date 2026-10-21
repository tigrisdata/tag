package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

const (
	copyPolicyBucket = "copy-policy-bucket"
	copyPolicyKey    = "destination.parquet"
)

type copyWarmForwarder struct {
	*mockForwarder
	body                 []byte
	responseCacheControl string
	fullCalls            atomic.Int64
	rangeMu              sync.Mutex
	ranges               []string
	rangeStarted         chan string
	fullStarted          chan struct{}
	holdFull             <-chan struct{}
}

func newCopyWarmForwarder(body []byte, responseCacheControl string) *copyWarmForwarder {
	f := &copyWarmForwarder{body: body, responseCacheControl: responseCacheControl}
	f.mockForwarder = &mockForwarder{
		doFullObjectFunc: func(ctx context.Context, _, _, _, _ string) (*http.Response, error) {
			f.fullCalls.Add(1)
			if f.fullStarted != nil {
				f.fullStarted <- struct{}{}
			}
			if f.holdFull != nil {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-f.holdFull:
				}
			}

			headers := make(http.Header)
			headers.Set("Content-Length", strconv.Itoa(len(f.body)))
			headers.Set("ETag", `"copy-warm-etag"`)
			headers.Set("Content-Type", "application/octet-stream")
			if f.responseCacheControl != "" {
				headers.Set("Cache-Control", f.responseCacheControl)
			}
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        headers,
				Body:          io.NopCloser(bytes.NewReader(f.body)),
				ContentLength: int64(len(f.body)),
			}, nil
		},
	}
	return f
}

func (f *copyWarmForwarder) DoConditionalGetRequest(_ context.Context, _, _, _, _, _ string, _ int64, rangeHeader string) (*http.Response, error) {
	f.rangeMu.Lock()
	f.ranges = append(f.ranges, rangeHeader)
	f.rangeMu.Unlock()
	if f.rangeStarted != nil {
		f.rangeStarted <- rangeHeader
	}

	start, end, err := copyWarmRange(rangeHeader, len(f.body))
	if err != nil {
		return nil, err
	}
	part := f.body[start : end+1]
	headers := make(http.Header)
	headers.Set("Content-Length", strconv.Itoa(len(part)))
	headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(f.body)))
	headers.Set("ETag", `"copy-warm-etag"`)
	headers.Set("Content-Type", "application/octet-stream")
	if f.responseCacheControl != "" {
		headers.Set("Cache-Control", f.responseCacheControl)
	}
	return &http.Response{
		StatusCode:    http.StatusPartialContent,
		Header:        headers,
		Body:          io.NopCloser(bytes.NewReader(part)),
		ContentLength: int64(len(part)),
	}, nil
}

func copyWarmRange(header string, size int) (int, int, error) {
	switch header {
	case "bytes=-8":
		if size < 8 {
			return 0, 0, fmt.Errorf("object is too small for %q", header)
		}
		return size - 8, size - 1, nil
	case "bytes=3072-4095":
		if size < 4096 {
			return 0, 0, fmt.Errorf("object is too small for %q", header)
		}
		return 3072, 4095, nil
	default:
		return 0, 0, fmt.Errorf("unexpected Parquet range %q for object size %d", header, size)
	}
}

func (f *copyWarmForwarder) rangeRequests() []string {
	f.rangeMu.Lock()
	defer f.rangeMu.Unlock()
	return append([]string(nil), f.ranges...)
}

func copyWarmBody() []byte {
	return copyWarmBodyWithFooter(4*1024, 512)
}

func copyWarmBodyWithFooter(size int, footerLength uint32) []byte {
	if size <= 8 || uint64(footerLength) > uint64(size-parquetTrailerSize) {
		panic("invalid Parquet body size or footer length")
	}
	body := bytes.Repeat([]byte("x"), size)
	binary.LittleEndian.PutUint32(body[len(body)-parquetTrailerSize:], footerLength)
	copy(body[len(body)-4:], parquetMagic)
	return body
}

func activeCopyWarmCount(svc *Service) int {
	active := 0
	svc.activeBackgroundFetches.Range(func(_, _ any) bool {
		active++
		return true
	})
	return active
}

func waitForCopyWarmWorkers(t testing.TB, svc *Service, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if activeCopyWarmCount(svc) == 0 && (svc.cacheSemaphore == nil || len(svc.cacheSemaphore) == 0) {
			return
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline:
			t.Fatalf("copy warmers did not finish: active=%d populate slots=%d", activeCopyWarmCount(svc), len(svc.cacheSemaphore))
		}
	}
}

type copyWarmWriteCounter struct {
	cacheclient.CacheClient
	mu       sync.Mutex
	metaKey  string
	dataKeys []string
	writes   int
}

func newCopyWarmWriteCounter(client cacheclient.CacheClient, bucket, key string) *copyWarmWriteCounter {
	return &copyWarmWriteCounter{
		CacheClient: client,
		metaKey:     cache.MakeMetaKey(bucket, key),
		dataKeys: []string{
			"body|" + bucket + "|" + key,
			"blk|" + bucket + "|" + key + "|",
		},
	}
}

func (c *copyWarmWriteCounter) recordsPopulateWrite(key string, data []byte) bool {
	for _, prefix := range c.dataKeys {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return key == c.metaKey && len(data) > 0 && data[0] == '{'
}

func (c *copyWarmWriteCounter) record(key string, data []byte) {
	if !c.recordsPopulateWrite(key, data) {
		return
	}
	c.mu.Lock()
	c.writes++
	c.mu.Unlock()
}

func (c *copyWarmWriteCounter) reset() {
	c.mu.Lock()
	c.writes = 0
	c.mu.Unlock()
}

func (c *copyWarmWriteCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

func (c *copyWarmWriteCounter) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	c.record(key, data)
	return c.CacheClient.Put(ctx, key, data, ttlSeconds)
}

func (c *copyWarmWriteCounter) PutIfVersion(ctx context.Context, key string, data []byte, ttlSeconds int64, expected uint64) (uint64, error) {
	c.record(key, data)
	return c.CacheClient.PutIfVersion(ctx, key, data, ttlSeconds, expected)
}

func (c *copyWarmWriteCounter) PutStream(ctx context.Context, key string, body io.Reader, ttlSeconds int64) error {
	if c.recordsPopulateWrite(key, nil) {
		c.record(key, nil)
		return c.CacheClient.PutStream(ctx, key, body, ttlSeconds)
	}
	if key == c.metaKey {
		data, err := io.ReadAll(body)
		if err != nil {
			return err
		}
		c.record(key, data)
		return c.CacheClient.PutStream(ctx, key, bytes.NewReader(data), ttlSeconds)
	}
	return c.CacheClient.PutStream(ctx, key, body, ttlSeconds)
}

func newCopyWarmService(t testing.TB, forwarder RequestForwarder) (*Service, *cache.Cache, *copyWarmWriteCounter) {
	t.Helper()
	cfg := config.NewDefault()
	cfg.Cache.WarmOnWrite = true
	cfg.Cache.ParquetOptimization = true
	cfg.Cache.BlockSize = 1024
	cfg.Cache.SizeThreshold = 1 << 20
	cfg.Cache.SetBlockCachingEnabled(true)

	tracker := newCopyWarmWriteCounter(cacheclient.NewMemoryCache(), copyPolicyBucket, copyPolicyKey)
	store := cache.NewCacheWithClient(tracker, &cfg.Cache)
	return NewService(forwarder, store, cfg), store, tracker
}

func copyObjectRequest(t testing.TB, cacheControls ...string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "/"+copyPolicyBucket+"/"+copyPolicyKey, nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=copy-test")
	r.Header.Set("X-Amz-Copy-Source", "/source-bucket/source-key")
	for _, value := range cacheControls {
		r.Header.Add("Cache-Control", value)
	}
	return r
}

func TestHandleCopyObject_NoStoreSkipsBothWarmersAndKeepsInvalidations(t *testing.T) {
	tests := []struct {
		name          string
		cacheControls []string
	}{
		{name: "no-store", cacheControls: []string{"no-store"}},
		{name: "repeated mixed-case fields", cacheControls: []string{"max-age=60", "No-StOrE"}},
		{name: "private", cacheControls: []string{`private="Set-Cookie"`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			releaseFull := make(chan struct{})
			released := false
			forwarder := newCopyWarmForwarder(copyWarmBody(), "public, max-age=60")
			forwarder.fullStarted = make(chan struct{}, 1)
			forwarder.holdFull = releaseFull
			svc, store, writes := newCopyWarmService(t, forwarder)
			defer func() {
				if !released {
					close(releaseFull)
				}
				waitForCopyWarmWorkers(t, svc, 5*time.Second)
			}()

			var violations []string
			stale := &cache.CachedObjectMeta{
				Bucket:        copyPolicyBucket,
				Key:           copyPolicyKey,
				ETag:          `"stale"`,
				ContentLength: 5,
				StatusCode:    http.StatusOK,
			}
			if err := store.PutWithMeta(context.Background(), copyPolicyBucket, copyPolicyKey, stale, []byte("stale"), 60); err != nil {
				t.Fatalf("seed stale destination: %v", err)
			}
			writes.reset()

			resultBody := []byte(`<CopyObjectResult><ETag>"copied"</ETag></CopyObjectResult>`)
			forwarder.captureFunc = func(_ context.Context, w http.ResponseWriter, _ *http.Request) (*ResponseCapture, error) {
				_, found, err := store.GetMeta(context.Background(), copyPolicyBucket, copyPolicyKey)
				if err != nil {
					t.Fatalf("read pre-forward invalidation state: %v", err)
				}
				if found {
					violations = append(violations, "pre-forward invalidation left stale metadata visible")
				}
				refill := &cache.CachedObjectMeta{
					Bucket:        copyPolicyBucket,
					Key:           copyPolicyKey,
					ETag:          `"racing-refill"`,
					ContentLength: 6,
					StatusCode:    http.StatusOK,
				}
				if err := store.PutWithMeta(context.Background(), copyPolicyBucket, copyPolicyKey, refill, []byte("refill"), 60); err != nil {
					t.Fatalf("inject racing refill: %v", err)
				}
				writes.reset()
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(resultBody)
				return &ResponseCapture{StatusCode: http.StatusOK, Body: resultBody, Complete: true}, nil
			}

			w := httptest.NewRecorder()
			if err := svc.HandleCopyObject(w, copyObjectRequest(t, tt.cacheControls...)); err != nil {
				t.Fatalf("HandleCopyObject: %v", err)
			}
			if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), resultBody) {
				violations = append(violations, fmt.Sprintf("CopyObject response = %d %q, want 200 %q", w.Code, w.Body.Bytes(), resultBody))
			}
			_, found, err := store.GetMeta(context.Background(), copyPolicyBucket, copyPolicyKey)
			if err != nil {
				t.Fatalf("read post-forward cache state: %v", err)
			}
			if found {
				violations = append(violations, "post-success invalidation left a racing refill or cache entry visible")
			}

			activeAtReturn := activeCopyWarmCount(svc)
			slotsAtReturn := len(svc.cacheSemaphore)
			slotsAtFullStart := 0
			if activeAtReturn > 0 {
				select {
				case <-forwarder.fullStarted:
					slotsAtFullStart = len(svc.cacheSemaphore)
				case <-time.After(5 * time.Second):
					violations = append(violations, "active warmer did not reach its full-object request")
				}
			}
			close(releaseFull)
			released = true
			waitForCopyWarmWorkers(t, svc, 5*time.Second)

			if got := forwarder.fullCalls.Load(); got != 0 {
				violations = append(violations, fmt.Sprintf("full-object GETs = %d, want 0", got))
			}
			if got := len(forwarder.rangeRequests()); got != 0 {
				violations = append(violations, fmt.Sprintf("trailer/range requests = %d (%v), want 0", got, forwarder.rangeRequests()))
			}
			if got := writes.count(); got != 0 {
				violations = append(violations, fmt.Sprintf("cache population writes = %d, want 0", got))
			}
			if slotsAtReturn != 0 || slotsAtFullStart != 0 {
				violations = append(violations, fmt.Sprintf("active population slots = %d at return and %d at full GET, want 0", slotsAtReturn, slotsAtFullStart))
			}
			if activeAtReturn != 0 {
				violations = append(violations, fmt.Sprintf("active copy warmers = %d, want 0", activeAtReturn))
			}
			if len(violations) > 0 {
				t.Errorf("%s", strings.Join(violations, "; "))
			}
		})
	}
}

func TestHandleCopyObject_CacheableParquetStillWarms(t *testing.T) {
	releaseFull := make(chan struct{})
	forwarder := newCopyWarmForwarder(copyWarmBody(), "public, max-age=60")
	forwarder.fullStarted = make(chan struct{}, 1)
	forwarder.holdFull = releaseFull
	forwarder.rangeStarted = make(chan string, 16)
	svc, store, writes := newCopyWarmService(t, forwarder)
	defer func() {
		if releaseFull != nil {
			close(releaseFull)
		}
		waitForCopyWarmWorkers(t, svc, 5*time.Second)
	}()

	resultBody := []byte(`<CopyObjectResult><ETag>"copied"</ETag></CopyObjectResult>`)
	forwarder.captureFunc = func(_ context.Context, w http.ResponseWriter, _ *http.Request) (*ResponseCapture, error) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(resultBody)
		return &ResponseCapture{StatusCode: http.StatusOK, Body: resultBody, Complete: true}, nil
	}

	w := httptest.NewRecorder()
	if err := svc.HandleCopyObject(w, copyObjectRequest(t)); err != nil {
		t.Fatalf("HandleCopyObject: %v", err)
	}
	select {
	case <-forwarder.fullStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("cacheable copy did not start its full-object warm")
	}
	if got := len(svc.cacheSemaphore); got == 0 {
		t.Error("full-object warm did not hold a populate slot while its upstream read was blocked")
	}
	select {
	case got := <-forwarder.rangeStarted:
		if got != "bytes=-8" {
			t.Errorf("first Parquet range = %q, want trailer bytes=-8", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cacheable Parquet copy did not start its trailer read")
	}
	select {
	case got := <-forwarder.rangeStarted:
		if got != "bytes=3072-4095" {
			t.Errorf("Parquet footer range = %q, want bytes=3072-4095", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cacheable Parquet copy did not fetch its footer block")
	}

	close(releaseFull)
	releaseFull = nil
	waitForCopyWarmWorkers(t, svc, 5*time.Second)
	if got := forwarder.fullCalls.Load(); got != 1 {
		t.Errorf("full-object GETs = %d, want 1", got)
	}
	if got := forwarder.rangeRequests(); len(got) != 2 || got[0] != "bytes=-8" || got[1] != "bytes=3072-4095" {
		t.Errorf("Parquet range requests = %v, want trailer and footer-block requests", got)
	}
	if got := writes.count(); got == 0 {
		t.Error("cacheable copy did not write populated body, block, or metadata keys")
	}
	if _, found, err := store.GetMeta(context.Background(), copyPolicyBucket, copyPolicyKey); err != nil || !found {
		t.Errorf("cacheable copy metadata: found=%v err=%v, want a populated entry", found, err)
	}
}
