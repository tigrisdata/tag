package handlers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

type lateFillRangeCacheClient struct {
	cacheclient.CacheClient
	targetKey  string
	triggerKey string

	mu        sync.Mutex
	probes    map[string]int
	fillErr   error
	orderErr  error
	fillCount atomic.Int32
	fillOnce  sync.Once
}

func (c *lateFillRangeCacheClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	err := c.CacheClient.GetRangeStream(ctx, key, start, end, w)
	if start != 0 || end != 1 {
		return err
	}
	c.mu.Lock()
	c.probes[key]++
	probesBeforeFill := c.probes[c.targetKey]
	c.mu.Unlock()
	if key == c.triggerKey && err == nil {
		c.fillOnce.Do(func() {
			if probesBeforeFill != 1 {
				c.mu.Lock()
				c.orderErr = errors.New("target absence was not classified before the later scan probe")
				c.mu.Unlock()
			}
			writeDone := make(chan error, 1)
			go func() {
				writeDone <- c.CacheClient.Put(context.Background(), c.targetKey, []byte("ABCD"), 60)
			}()
			select {
			case writeErr := <-writeDone:
				c.mu.Lock()
				c.fillErr = writeErr
				c.mu.Unlock()
				if writeErr == nil {
					c.fillCount.Add(1)
				}
			case <-time.After(2 * time.Second):
				c.mu.Lock()
				c.fillErr = errors.New("late block writer did not complete")
				c.mu.Unlock()
			}
		})
	}
	return err
}

func (c *lateFillRangeCacheClient) probeCount(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.probes[key]
}

func (c *lateFillRangeCacheClient) fillErrors() (error, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fillErr, c.orderErr
}

type lateFillRangeForwarder struct {
	handlerPrefetchForwarder
	alignedGets  atomic.Int32
	fallbackGets atomic.Int32
}

func (f *lateFillRangeForwarder) DoConditionalGetRequest(context.Context, string, string, string, string, string, int64, string) (*http.Response, error) {
	f.alignedGets.Add(1)
	body := "origin unavailable"
	return &http.Response{
		StatusCode:    http.StatusServiceUnavailable,
		Header:        http.Header{"Content-Length": []string{"18"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}, nil
}

func (f *lateFillRangeForwarder) DoRequestWithCreds(context.Context, *http.Request, string, string) (*http.Response, error) {
	f.fallbackGets.Add(1)
	return nil, errors.New("original Range fallback reached the failed origin")
}

func TestHandleObjectLateSameETagFillSurvivesAlignedFetchFailure(t *testing.T) {
	const (
		bucket = "late-fill-bucket"
		key    = "late-fill-key"
		etag   = `"v1"`
	)
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	base := cacheclient.NewMemoryCache()
	block0Key := cache.MakeBlockKey(bucket, key, etag, 4, 0)
	block1Key := cache.MakeBlockKey(bucket, key, etag, 4, 1)
	client := &lateFillRangeCacheClient{
		CacheClient: base,
		targetKey:   block0Key,
		triggerKey:  block1Key,
		probes:      make(map[string]int),
	}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          etag,
		ContentLength: 8,
		StatusCode:    http.StatusOK,
		BlockSize:     4,
	}
	if wrote, err := store.PutMetaIfVersion(context.Background(), bucket, key, meta, 60, cache.VersionAny); err != nil || !wrote {
		t.Fatalf("seed meta = (wrote=%t, err=%v)", wrote, err)
	}
	if err := base.Put(context.Background(), block1Key, []byte("EFGH"), 60); err != nil {
		t.Fatalf("seed second block: %v", err)
	}

	forwarder := &lateFillRangeForwarder{}
	service := proxy.NewService(forwarder, store, cfg)
	gateway := httptest.NewServer(NewServer(service, "127.0.0.1", 0, false, 0).Router())
	defer gateway.Close()
	request, err := http.NewRequest(http.MethodGet, gateway.URL+"/"+bucket+"/"+key, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Range", "bytes=0-7")
	response, err := gateway.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("response body errors = (%v, %v)", readErr, closeErr)
	}
	if response.StatusCode != http.StatusPartialContent || response.Header.Get("X-Cache") != proxy.XCacheHit || string(body) != "ABCDEFGH" {
		t.Fatalf("response = (status=%d, cache=%q, body=%q), want 206 HIT ABCDEFGH", response.StatusCode, response.Header.Get("X-Cache"), body)
	}
	if got := response.Header.Get("ETag"); got != etag {
		t.Errorf("ETag=%q, want %q", got, etag)
	}
	if got := response.Header.Get("Content-Range"); got != "bytes 0-7/8" {
		t.Errorf("Content-Range=%q, want bytes 0-7/8", got)
	}
	if got := client.fillCount.Load(); got != 1 {
		t.Errorf("late same-ETag fills=%d, want 1", got)
	}
	if fillErr, orderErr := client.fillErrors(); fillErr != nil || orderErr != nil {
		t.Errorf("late fill errors = (fill=%v, order=%v)", fillErr, orderErr)
	}
	if got := client.probeCount(block0Key); got != 2 {
		t.Errorf("target byte-zero probes=%d, want initial scan and one failure recovery", got)
	}
	if got := client.probeCount(block1Key); got != 1 {
		t.Errorf("present-block byte-zero probes=%d, want 1", got)
	}
	if got := forwarder.alignedGets.Load(); got != 1 {
		t.Errorf("aligned origin GETs=%d, want 1 failed attempt", got)
	}
	if got := forwarder.fallbackGets.Load(); got != 0 {
		t.Errorf("original Range origin GETs=%d, want 0", got)
	}
}
