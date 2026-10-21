package handlers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

type certifiedRangeHandlerCacheClient struct {
	cacheclient.CacheClient
	presenceReads atomic.Int64
	metaWritten   chan struct{}
}

func (c *certifiedRangeHandlerCacheClient) noteMetaWrite(key string) {
	if strings.HasPrefix(key, "meta|") {
		select {
		case c.metaWritten <- struct{}{}:
		default:
		}
	}
}

func (c *certifiedRangeHandlerCacheClient) Put(ctx context.Context, key string, data []byte, ttl int64) error {
	err := c.CacheClient.Put(ctx, key, data, ttl)
	if err == nil {
		c.noteMetaWrite(key)
	}
	return err
}

func (c *certifiedRangeHandlerCacheClient) PutIfVersion(ctx context.Context, key string, data []byte, ttl int64, expected uint64) (uint64, error) {
	version, err := c.CacheClient.PutIfVersion(ctx, key, data, ttl, expected)
	if err == nil {
		c.noteMetaWrite(key)
	}
	return version, err
}

func (c *certifiedRangeHandlerCacheClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if strings.HasPrefix(key, "blk|") && start == 0 && end == 1 {
		c.presenceReads.Add(1)
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

// This exercises a range-created, non-BlocksComplete entry through Server.handleObject, not a
// direct Service call. Same-span and contained requests must return exact HIT responses without a
// second origin request or the serial byte-zero presence pass.
func TestBlockCacheCertifiedRangeHandlerServesExactBytes(t *testing.T) {
	const (
		bucket    = "benchmark"
		key       = "prefetch"
		etag      = `"v1"`
		blockSize = int64(4)
	)
	body := []byte("ABCDEFGHIJKLMNOP")
	origin := &certifiedRangeHandlerForwarder{body: body, etag: etag}
	client := &certifiedRangeHandlerCacheClient{
		CacheClient: cacheclient.NewMemoryCache(),
		metaWritten: make(chan struct{}, 1),
	}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = int64(len(body))
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	service := proxy.NewService(origin, store, cfg)
	router := NewServer(service, "127.0.0.1", 0, false, 0).Router()

	request := func(rangeHeader string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
		req.Header.Set("Range", rangeHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}

	cold := request("bytes=0-15")
	if cold.Code != http.StatusPartialContent || cold.Body.String() != string(body) {
		t.Fatalf("cold handler range = (%d, %q), want upstream 206 body", cold.Code, cold.Body.String())
	}
	select {
	case <-client.metaWritten:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for range-populated metadata")
	}
	meta, found, err := store.GetMeta(context.Background(), bucket, key)
	if err != nil || !found || meta == nil || meta.RangeBlockSpan == nil || meta.BlocksComplete {
		t.Fatalf("cold handler metadata = (found=%t, meta=%#v, err=%v), want range-created non-BlocksComplete certificate", found, meta, err)
	}
	if meta.RangeBlockSpan.First != 0 || meta.RangeBlockSpan.Last != 3 {
		t.Fatalf("range-created span = %#v, want [0,3]", meta.RangeBlockSpan)
	}
	originCalls := origin.calls.Load()

	for _, tc := range []struct {
		name string
		rng  string
		body string
		cr   string
	}{
		{name: "same span", rng: "bytes=0-15", body: "ABCDEFGHIJKLMNOP", cr: "bytes 0-15/16"},
		{name: "contained span", rng: "bytes=4-11", body: "EFGHIJKL", cr: "bytes 4-11/16"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client.presenceReads.Store(0)
			warm := request(tc.rng)
			if warm.Code != http.StatusPartialContent || warm.Body.String() != tc.body {
				t.Fatalf("handler response = (%d, %q), want (206, %q)", warm.Code, warm.Body.String(), tc.body)
			}
			if got := warm.Header().Get("X-Cache"); got != proxy.XCacheHit {
				t.Fatalf("handler X-Cache = %q, want %q", got, proxy.XCacheHit)
			}
			if got := warm.Header().Get("ETag"); got != etag {
				t.Fatalf("handler ETag = %q, want %q", got, etag)
			}
			if got := warm.Header().Get("Content-Range"); got != tc.cr {
				t.Fatalf("handler Content-Range = %q, want %q", got, tc.cr)
			}
			if got := client.presenceReads.Load(); got != 0 {
				t.Fatalf("handler presence probes = %d, want 0", got)
			}
			if got := origin.calls.Load(); got != originCalls {
				t.Fatalf("warm handler range called the origin %d times, want none", got-originCalls)
			}
		})
	}
}
