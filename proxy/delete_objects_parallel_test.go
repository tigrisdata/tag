package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// Hold metadata deletion after the tombstone write, so overlapping repeated
// entries would be observable even if their tombstone puts did not overlap.
type bulkDeleteGateClient struct {
	cacheclient.CacheClient
	phase        atomic.Int32
	gates        [2]chan struct{}
	mu           sync.Mutex
	active, peak int
	pending      map[string]bool
	calls        [2]map[string]int
	unordered    bool
}

func (c *bulkDeleteGateClient) Put(ctx context.Context, key string, data []byte, ttl int64) error {
	if strings.HasPrefix(key, "tomb|") {
		c.mu.Lock()
		if c.pending[key] {
			c.unordered = true
		}
		c.pending[key] = true
		c.mu.Unlock()
	}
	return c.CacheClient.Put(ctx, key, data, ttl)
}

func (c *bulkDeleteGateClient) hold(ctx context.Context, key string) error {
	phase := c.phase.Load()
	c.mu.Lock()
	c.calls[phase][key]++
	c.active++
	c.peak = max(c.peak, c.active)
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.active--; c.mu.Unlock() }()
	select {
	case <-c.gates[phase]:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *bulkDeleteGateClient) Delete(ctx context.Context, key string) error {
	err := c.hold(ctx, key)
	c.mu.Lock()
	delete(c.pending, strings.Replace(key, "meta|", "tomb|", 1))
	c.mu.Unlock()
	if err != nil {
		return err
	}
	return c.CacheClient.Delete(ctx, key)
}

func (c *bulkDeleteGateClient) DeleteIfVersion(ctx context.Context, key string, expected uint64) error {
	if err := c.hold(ctx, key); err != nil {
		return err
	}
	return c.CacheClient.DeleteIfVersion(ctx, key, expected)
}

func newBulkDeleteGateClient() *bulkDeleteGateClient {
	return &bulkDeleteGateClient{
		CacheClient: cacheclient.NewMemoryCache(),
		gates:       [2]chan struct{}{make(chan struct{}), make(chan struct{})},
		pending:     make(map[string]bool),
		calls:       [2]map[string]int{make(map[string]int), make(map[string]int)},
	}
}

type bulkDeleteLocalGateClient struct{ *bulkDeleteGateClient }

func (*bulkDeleteLocalGateClient) IsLocal(string) bool { return true }

// Both barriers must join every worker, and only distinct keys may overlap.
// A failed key keeps its racing refill; a repeated key with one successful entry
// is fenced once after success, regardless of how many preflight entries it had.
func TestHandleDeleteObjects_ParallelBarriers(t *testing.T) {
	for _, tc := range []struct{ legacy, local bool }{{true, false}, {false, false}, {true, true}, {false, true}} {
		t.Run(fmt.Sprintf("legacy=%t/local=%t", tc.legacy, tc.local), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := config.NewDefault()
				cfg.Cache.SetLegacyCoordination(tc.legacy)
				client := newBulkDeleteGateClient()
				var cacheClient cacheclient.CacheClient = client
				wantActive := bulkDeleteConcurrency
				if tc.local {
					cacheClient = &bulkDeleteLocalGateClient{client}
					wantActive = 1
				}
				store := cache.NewCacheWithClient(cacheClient, &cfg.Cache)
				const keys = 20
				var body strings.Builder
				body.WriteString("<Delete>")
				for i := 0; i < keys; i++ {
					fmt.Fprintf(&body, "<Object><Key>key-%d</Key></Object>", i)
				}
				body.WriteString("<Object><Key>key-0</Key><VersionId>v2</VersionId></Object></Delete>")
				const result = `<DeleteResult><Error><Key>key-0</Key></Error><Error><Key>key-1</Key></Error></DeleteResult>`
				forwarded := false
				forwarder := &mockForwarder{captureFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) (*ResponseCapture, error) {
					forwarded = true
					for i := 0; i < keys; i++ {
						key := fmt.Sprintf("key-%d", i)
						meta := &cache.CachedObjectMeta{ETag: `"old"`, StatusCode: http.StatusOK}
						if err := store.PutWithMeta(context.Background(), "bucket", key, meta, nil, 60); err != nil {
							return nil, err
						}
					}
					client.phase.Store(1)
					w.Header().Set("X-Upstream-Result", "partial")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(result))
					return &ResponseCapture{StatusCode: http.StatusOK, Body: []byte(result), Complete: true}, nil
				}}
				service := NewService(forwarder, store, cfg)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				r := httptest.NewRequest(http.MethodPost, "/bucket?delete", strings.NewReader(body.String())).WithContext(ctx)
				w := httptest.NewRecorder()
				done := make(chan error, 1)
				go func() { done <- service.HandleDeleteObjects(w, r) }()
				synctest.Wait()
				if forwarded || client.active != wantActive {
					t.Fatalf("pre-forward barrier: forwarded=%t active=%d", forwarded, client.active)
				}
				close(client.gates[0])
				synctest.Wait()
				if !forwarded || client.active != wantActive || w.Body.Len() != 0 || len(w.Header()) != 0 {
					t.Fatal("post-success barrier did not hold the response")
				}
				cancel()
				synctest.Wait()
				if client.active != wantActive || w.Body.Len() != 0 {
					t.Fatal("client cancellation abandoned the post-success barrier")
				}
				close(client.gates[1])
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if client.peak != wantActive || client.unordered {
					t.Fatalf("cache scheduling: peak=%d unordered=%t", client.peak, client.unordered)
				}
				if w.Code != http.StatusOK || w.Body.String() != result || w.Header().Get("X-Upstream-Result") != "partial" {
					t.Fatal("upstream response changed")
				}
				for i := 0; i < keys; i++ {
					key := fmt.Sprintf("key-%d", i)
					_, found, err := store.GetMeta(context.Background(), "bucket", key)
					if err != nil || found != (i == 1) {
						t.Fatalf("racing refill %s: found=%t err=%v", key, found, err)
					}
					wantPre := 1
					if i == 0 {
						wantPre++
					}
					if got := client.calls[0][cache.MakeMetaKey("bucket", key)]; got != wantPre {
						t.Errorf("preflight %s: %d attempts, want %d", key, got, wantPre)
					}
					if i == 1 && client.calls[1][cache.MakeMetaKey("bucket", key)] != 0 {
						t.Error("all-failed key was re-invalidated")
					}
				}
			})
		})
	}
}

func TestHandleDeleteObjects_ParallelFenceTimeoutReportsSkippedKeys(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := config.NewDefault()
		client := newBulkDeleteGateClient()
		close(client.gates[0])
		store := cache.NewCacheWithClient(client, &cfg.Cache)
		const keys = 32
		var body strings.Builder
		body.WriteString("<Delete>")
		for i := 0; i < keys; i++ {
			fmt.Fprintf(&body, "<Object><Key>key-%d</Key></Object>", i)
		}
		body.WriteString("</Delete>")
		forwarder := &mockForwarder{captureFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) (*ResponseCapture, error) {
			for i := 0; i < keys; i++ {
				key := fmt.Sprintf("key-%d", i)
				if err := store.PutWithMeta(context.Background(), "bucket", key, &cache.CachedObjectMeta{ETag: `"old"`}, nil, 60); err != nil {
					return nil, err
				}
			}
			client.phase.Store(1)
			return &ResponseCapture{StatusCode: http.StatusOK, Body: []byte("<DeleteResult/>"), Complete: true}, nil
		}}
		logs := &lockedLogBuffer{}
		previous := log.Logger
		log.Logger = zerolog.New(logs).Level(zerolog.WarnLevel)
		defer func() { log.Logger = previous }()
		service := NewService(forwarder, store, cfg)
		start := time.Now()
		w := httptest.NewRecorder()
		err := service.HandleDeleteObjects(w, httptest.NewRequest(http.MethodPost, "/bucket?delete", strings.NewReader(body.String())))
		if err != nil || time.Since(start) != bulkDeleteFenceTimeout || w.Body.String() != "<DeleteResult/>" {
			t.Fatalf("fence timeout: elapsed=%v err=%v body=%q", time.Since(start), err, w.Body.String())
		}
		if len(client.calls[1]) != bulkDeleteConcurrency {
			t.Fatalf("started %d keys, want %d", len(client.calls[1]), bulkDeleteConcurrency)
		}
		for _, fragment := range []string{`"keys":8`, `"skipped_keys":24`, "Bulk-delete cache fence did not converge"} {
			if !strings.Contains(logs.String(), fragment) {
				t.Errorf("missing %q in %s", fragment, logs.String())
			}
		}
		for i := 0; i < keys; i++ {
			if _, found, err := store.GetMeta(context.Background(), "bucket", fmt.Sprintf("key-%d", i)); err != nil || !found {
				t.Fatalf("failed or skipped fence was not observable as a retained refill: found=%t err=%v", found, err)
			}
		}
	})
}
