package proxy

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
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

type deleteObjectsRealGETGate struct {
	cacheclient.CacheClient
	metaKey       string
	generationKey string
	tombstoneKey  string
	armed         atomic.Bool
	tokenOnce     sync.Once
	tokenCaptured chan struct{}
	pauseOnce     sync.Once
	publishPaused chan struct{}
	publishDone   chan struct{}
	publishOnce   sync.Once
	releaseOnce   sync.Once
	release       chan struct{}
}

func newDeleteObjectsRealGETGate(client cacheclient.CacheClient, bucket, key string) *deleteObjectsRealGETGate {
	return &deleteObjectsRealGETGate{
		CacheClient:   client,
		metaKey:       cache.MakeMetaKey(bucket, key),
		generationKey: "meta-gen|" + bucket + "|" + key,
		tombstoneKey:  cache.MakeTombstoneKey(bucket, key),
		tokenCaptured: make(chan struct{}),
		publishPaused: make(chan struct{}),
		publishDone:   make(chan struct{}),
		release:       make(chan struct{}),
	}
}

func (g *deleteObjectsRealGETGate) noteToken(key string) {
	if g.armed.Load() && (key == g.generationKey || key == g.metaKey || key == g.tombstoneKey) {
		g.tokenOnce.Do(func() { close(g.tokenCaptured) })
	}
}

func (g *deleteObjectsRealGETGate) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := g.CacheClient.Get(ctx, key)
	g.noteToken(key)
	return data, err
}

func (g *deleteObjectsRealGETGate) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	data, version, found, err := g.CacheClient.GetWithVersion(ctx, key)
	g.noteToken(key)
	return data, version, found, err
}

func (g *deleteObjectsRealGETGate) waitBeforePublish(key string) {
	if key != g.metaKey || !g.armed.Load() {
		return
	}
	g.pauseOnce.Do(func() {
		close(g.publishPaused)
		<-g.release
	})
}

func (g *deleteObjectsRealGETGate) notePublished(key string) {
	if key == g.metaKey && g.armed.Load() {
		g.publishOnce.Do(func() { close(g.publishDone) })
	}
}

func (g *deleteObjectsRealGETGate) Put(ctx context.Context, key string, value []byte, ttl int64) error {
	g.waitBeforePublish(key)
	err := g.CacheClient.Put(ctx, key, value, ttl)
	g.notePublished(key)
	return err
}

func (g *deleteObjectsRealGETGate) PutIfVersion(ctx context.Context, key string, value []byte, ttl int64, expected uint64) (uint64, error) {
	g.waitBeforePublish(key)
	version, err := g.CacheClient.PutIfVersion(ctx, key, value, ttl, expected)
	g.notePublished(key)
	return version, err
}

func (g *deleteObjectsRealGETGate) releasePublish() {
	g.releaseOnce.Do(func() { close(g.release) })
}

func TestDeleteObjectsLostResponseRealGET(t *testing.T) {
	const bucket, key = "bulk-real-get", "old-key"
	const oldBody = "old body"
	requestBody := `<Delete><Object><Key>` + key + `</Key></Object></Delete>`

	for _, mode := range []struct {
		name   string
		legacy bool
	}{
		{name: "legacy-generation-sidecar", legacy: true},
		{name: "cas-fences", legacy: false},
	} {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			postReceived := make(chan []byte, 1)
			closePost := make(chan struct{})
			postClosed := make(chan struct{})
			getReceived := make(chan struct{}, 4)
			originErrors := make(chan error, 2)
			var deleteApplied atomic.Bool

			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.RawQuery == "delete":
					body, err := io.ReadAll(r.Body)
					if err != nil {
						originErrors <- err
						return
					}
					postReceived <- body
					<-closePost
					deleteApplied.Store(true)
					hijacker, ok := w.(http.Hijacker)
					if !ok {
						originErrors <- errors.New("origin response writer cannot hijack connection")
						return
					}
					conn, _, err := hijacker.Hijack()
					if err != nil {
						originErrors <- err
						return
					}
					_ = conn.Close()
					close(postClosed)
				case r.Method == http.MethodGet && r.URL.Path == "/"+bucket+"/"+key:
					getReceived <- struct{}{}
					if deleteApplied.Load() {
						body := "NoSuchKey"
						w.Header().Set("Content-Length", "9")
						w.WriteHeader(http.StatusNotFound)
						_, _ = io.WriteString(w, body)
						return
					}
					w.Header().Set("Content-Length", "8")
					w.Header().Set("Content-Type", "text/plain")
					w.Header().Set("ETag", `"old"`)
					w.WriteHeader(http.StatusOK)
					_, _ = io.WriteString(w, oldBody)
				default:
					originErrors <- errors.New("unexpected origin request: " + r.Method + " " + r.URL.String())
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer origin.Close()
			defer func() {
				select {
				case <-closePost:
				default:
					close(closePost)
				}
			}()

			cfg := config.NewDefault()
			cfg.Cache.SetLegacyCoordination(mode.legacy)
			gate := newDeleteObjectsRealGETGate(cacheclient.NewMemoryCache(), bucket, key)
			store := cache.NewCacheWithClient(gate, &cfg.Cache)
			seed := &cache.CachedObjectMeta{
				Bucket:        bucket,
				Key:           key,
				ETag:          `"old"`,
				ContentLength: int64(len(oldBody)),
				ContentType:   "text/plain",
				StatusCode:    http.StatusOK,
			}
			if err := store.PutWithMeta(context.Background(), bucket, key, seed, []byte(oldBody), 60); err != nil {
				t.Fatalf("seed old cache entry: %v", err)
			}

			base := newBaseForwarder(origin.URL, "us-east-1", 10)
			base.httpClient = origin.Client()
			forwarder := &transparentForwarder{
				baseForwarder:    base,
				proxySigner:      auth.NewProxySigner("test-access-key", "test-secret-key"),
				upstreamEndpoint: origin.URL,
			}
			forwarder.initInterceptor()
			svc := NewService(forwarder, store, cfg)

			deleteReq := httptest.NewRequest(http.MethodPost, "http://gateway/"+bucket+"?delete", strings.NewReader(requestBody))
			deleteReq.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test")
			deleteDone := make(chan error, 1)
			go func() { deleteDone <- svc.HandleDeleteObjects(httptest.NewRecorder(), deleteReq) }()
			select {
			case body := <-postReceived:
				if string(body) != requestBody {
					t.Fatalf("origin DeleteObjects body=%q, want %q", body, requestBody)
				}
			case err := <-originErrors:
				t.Fatalf("origin setup: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("origin did not receive DeleteObjects after the first invalidation")
			}

			gate.armed.Store(true)
			getReq := httptest.NewRequest(http.MethodGet, "http://gateway/"+bucket+"/"+key, nil)
			getReq.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test")
			getRecorder := httptest.NewRecorder()
			getDone := make(chan error, 1)
			go func() { getDone <- svc.HandleGetObject(getRecorder, getReq) }()
			select {
			case <-gate.tokenCaptured:
			case err := <-originErrors:
				t.Fatalf("origin setup: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("GET did not capture its decision token after the first invalidation")
			}
			select {
			case <-getReceived:
			case err := <-originErrors:
				t.Fatalf("origin GET: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("racing GET did not reach the origin")
			}
			select {
			case <-gate.publishPaused:
			case err := <-originErrors:
				t.Fatalf("origin setup: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("GET did not reach its metadata publish boundary")
			}

			close(closePost)
			select {
			case <-postClosed:
			case err := <-originErrors:
				t.Fatalf("origin response-loss setup: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("origin did not close the DeleteObjects response")
			}
			select {
			case err := <-deleteDone:
				if err == nil || !errors.Is(err, io.EOF) {
					t.Fatalf("HandleDeleteObjects error=%v; want original transport EOF", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("DeleteObjects handler did not return after its error-path invalidation")
			}

			gate.releasePublish()
			select {
			case <-gate.publishDone:
			case <-time.After(5 * time.Second):
				t.Fatal("cache writer did not finish its metadata commit")
			}
			select {
			case err := <-getDone:
				if err != nil {
					t.Fatalf("racing GET error=%v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("racing GET did not finish")
			}
			if getRecorder.Body.String() != oldBody {
				t.Fatalf("racing GET body=%q, want old response snapshot %q", getRecorder.Body.String(), oldBody)
			}

			if _, found, err := store.GetMeta(context.Background(), bucket, key); err != nil {
				t.Fatalf("read metadata after late publish: %v", err)
			} else if found {
				t.Fatal("late old metadata remained readable after the error-path generation fence")
			}
			laterRecorder := httptest.NewRecorder()
			laterReq := httptest.NewRequest(http.MethodGet, "http://gateway/"+bucket+"/"+key, nil)
			laterReq.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test")
			if err := svc.HandleGetObject(laterRecorder, laterReq); err != nil {
				t.Fatalf("later GET error=%v", err)
			}
			if laterRecorder.Header().Get(XCacheHeader) == XCacheHit || laterRecorder.Body.String() == oldBody {
				t.Fatalf("later GET served stale data: X-Cache=%q body=%q", laterRecorder.Header().Get(XCacheHeader), laterRecorder.Body.String())
			}
		})
	}
}

type deleteObjectsInvalidationCounter struct {
	cacheclient.CacheClient
	metaKey       string
	tombstoneKey  string
	legacy        bool
	armed         atomic.Bool
	invalidations atomic.Int32
}

func (c *deleteObjectsInvalidationCounter) Put(ctx context.Context, key string, data []byte, ttl int64) error {
	err := c.CacheClient.Put(ctx, key, data, ttl)
	if c.armed.Load() && c.legacy && key == c.tombstoneKey && err == nil {
		c.invalidations.Add(1)
	}
	return err
}

func (c *deleteObjectsInvalidationCounter) DeleteIfVersion(ctx context.Context, key string, expected uint64) error {
	err := c.CacheClient.DeleteIfVersion(ctx, key, expected)
	if c.armed.Load() && !c.legacy && key == c.metaKey && err == nil {
		c.invalidations.Add(1)
	}
	return err
}

func TestDeleteObjectsPreDispatchControl(t *testing.T) {
	const bucket, key = "pre-dispatch-real", "key"
	for _, mode := range []struct {
		name   string
		legacy bool
	}{
		{name: "legacy-tombstones", legacy: true},
		{name: "cas-fences", legacy: false},
	} {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			cfg := config.NewDefault()
			cfg.Cache.SetLegacyCoordination(mode.legacy)
			counter := &deleteObjectsInvalidationCounter{
				CacheClient:  cacheclient.NewMemoryCache(),
				metaKey:      cache.MakeMetaKey(bucket, key),
				tombstoneKey: cache.MakeTombstoneKey(bucket, key),
				legacy:       mode.legacy,
			}
			store := cache.NewCacheWithClient(counter, &cfg.Cache)
			seedDeleteObjectsCache(t, store, bucket, key)

			base := newBaseForwarder("://invalid", "us-east-1", 10)
			forwarder := &transparentForwarder{
				baseForwarder:    base,
				proxySigner:      auth.NewProxySigner("test-access-key", "test-secret-key"),
				upstreamEndpoint: "://invalid",
			}
			forwarder.initInterceptor()
			svc := NewService(forwarder, store, cfg)
			counter.armed.Store(true)
			request := httptest.NewRequest(http.MethodPost, "http://gateway/"+bucket+"?delete",
				strings.NewReader(`<Delete><Object><Key>`+key+`</Key></Object></Delete>`))
			err := svc.HandleDeleteObjects(httptest.NewRecorder(), request)
			if err == nil || !strings.Contains(err.Error(), "failed to parse endpoint") {
				t.Fatalf("HandleDeleteObjects error=%v; want request-build failure before Do", err)
			}
			if got := counter.invalidations.Load(); got != 1 {
				t.Fatalf("successful per-key invalidations=%d; want only the pre-forward pass", got)
			}
		})
	}
}
