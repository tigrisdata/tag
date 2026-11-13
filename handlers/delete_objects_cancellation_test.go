package handlers

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
	"google.golang.org/grpc"
)

const (
	deleteObjectsPreflightBucket    = "preflight-cancel-bucket"
	deleteObjectsPreflightKey       = "preflight-cancel-key"
	deleteObjectsPreflightSecondKey = "next-preflight-key"
	deleteObjectsPreflightBody      = `<Delete><Object><Key>` + deleteObjectsPreflightKey + `</Key></Object><Object><Key>` + deleteObjectsPreflightSecondKey + `</Key></Object></Delete>`
)

type deleteObjectsPreflightRPCOwner struct {
	pb.UnimplementedCacheServiceServer

	method         string
	key            string
	callToHalt     int32
	calls          atomic.Int32
	operationCalls atomic.Int32

	started        chan struct{}
	rpcReturned    chan struct{}
	cancelObserved chan struct{}
	release        chan struct{}

	startOnce   sync.Once
	returnOnce  sync.Once
	cancelOnce  sync.Once
	releaseOnce sync.Once
}

func newDeleteObjectsPreflightRPCOwner(method, key string, callToHalt int32) *deleteObjectsPreflightRPCOwner {
	return &deleteObjectsPreflightRPCOwner{
		method:         method,
		key:            key,
		callToHalt:     callToHalt,
		started:        make(chan struct{}),
		rpcReturned:    make(chan struct{}),
		cancelObserved: make(chan struct{}),
		release:        make(chan struct{}),
	}
}

func (o *deleteObjectsPreflightRPCOwner) hold(ctx context.Context, method, key string) error {
	if method != o.method {
		return nil
	}
	o.operationCalls.Add(1)
	if key != o.key || o.calls.Add(1) != o.callToHalt {
		return nil
	}
	o.startOnce.Do(func() { close(o.started) })
	defer o.returnOnce.Do(func() { close(o.rpcReturned) })

	select {
	case <-ctx.Done():
		o.cancelOnce.Do(func() { close(o.cancelObserved) })
		return ctx.Err()
	case <-o.release:
		return nil
	}
}

func (o *deleteObjectsPreflightRPCOwner) releaseOwner() {
	o.releaseOnce.Do(func() { close(o.release) })
}

func (o *deleteObjectsPreflightRPCOwner) PutObject(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if err := o.hold(ctx, "put", req.Key); err != nil {
		return nil, err
	}
	return &pb.PutResponse{Success: true}, nil
}

func (o *deleteObjectsPreflightRPCOwner) Delete(_ context.Context, _ *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	return &pb.DeleteResponse{Success: true}, nil
}

func (o *deleteObjectsPreflightRPCOwner) DeleteIfVersion(ctx context.Context, req *pb.DeleteIfVersionRequest) (*pb.DeleteIfVersionResponse, error) {
	if err := o.hold(ctx, "delete-if-version", req.Key); err != nil {
		return nil, err
	}
	return &pb.DeleteIfVersionResponse{Success: true}, nil
}

func (o *deleteObjectsPreflightRPCOwner) GetObjectWithVersion(ctx context.Context, req *pb.GetRequest) (*pb.GetWithVersionResponse, error) {
	if err := o.hold(ctx, "get-with-version", req.Key); err != nil {
		return nil, err
	}
	return &pb.GetWithVersionResponse{Version: 1}, nil
}

type deleteObjectsPreflightContextCapture struct {
	cacheclient.CacheClient
	method   string
	key      string
	contexts chan context.Context
	calls    atomic.Int32
	once     sync.Once
}

func (c *deleteObjectsPreflightContextCapture) record(ctx context.Context, method, key string) {
	if method != c.method {
		return
	}
	c.calls.Add(1)
	if key == c.key {
		c.once.Do(func() { c.contexts <- ctx })
	}
}

func (c *deleteObjectsPreflightContextCapture) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	c.record(ctx, "put", key)
	return c.CacheClient.Put(ctx, key, data, ttlSeconds)
}

func (c *deleteObjectsPreflightContextCapture) DeleteIfVersion(ctx context.Context, key string, expected uint64) error {
	c.record(ctx, "delete-if-version", key)
	return c.CacheClient.DeleteIfVersion(ctx, key, expected)
}

func (c *deleteObjectsPreflightContextCapture) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	c.record(ctx, "get-with-version", key)
	return c.CacheClient.GetWithVersion(ctx, key)
}

type deleteObjectsPreflightForwarder struct {
	proxy.RequestForwarder
	calls atomic.Int32
}

func (f *deleteObjectsPreflightForwarder) ForwardWithCapture(context.Context, http.ResponseWriter, *http.Request) (*proxy.ResponseCapture, error) {
	f.calls.Add(1)
	return nil, context.Canceled
}

type deleteObjectsPreflightHTTPResult struct {
	err error
}

type deleteObjectsPreflightFixture struct {
	gateway          *httptest.Server
	client           *http.Client
	owner            *deleteObjectsPreflightRPCOwner
	cacheClient      *cacheclient.SimpleClient
	grpcServer       *grpc.Server
	listener         net.Listener
	observedClient   *deleteObjectsPreflightContextCapture
	operationContext chan context.Context
	forwarder        *deleteObjectsPreflightForwarder
	handlerDone      chan struct{}
}

func newDeleteObjectsPreflightFixture(t *testing.T, mode string, owner *deleteObjectsPreflightRPCOwner) *deleteObjectsPreflightFixture {
	t.Helper()

	var fixture deleteObjectsPreflightFixture
	fixture.owner = owner
	fixture.handlerDone = make(chan struct{})

	var err error
	fixture.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for cache owner: %v", err)
	}
	fixture.grpcServer = grpc.NewServer()
	pb.RegisterCacheServiceServer(fixture.grpcServer, owner)
	go func() { _ = fixture.grpcServer.Serve(fixture.listener) }()

	fixture.cacheClient, err = cacheclient.NewSimpleClient(&cacheclient.ClientConfig{
		Addrs:              []string{fixture.listener.Addr().String()},
		Mode:               cacheclient.ModeSimple,
		ConnectionPoolSize: 1,
	})
	if err != nil {
		t.Fatalf("create remote cache client: %v", err)
	}

	cfg := config.NewDefault()
	switch mode {
	case "legacy":
		cfg.Cache.SetLegacyCoordination(true)
	case "cas":
		cfg.Cache.SetLegacyCoordination(false)
	case "tiered":
		cfg.Mode = config.ModeTiered
		cfg.Cache.SetBlockCachingEnabled(false)
		cfg.Cache.SizeThreshold = 1024
	default:
		t.Fatalf("unknown DeleteObjects test mode %q", mode)
	}
	if mode == "tiered" {
		if err := cfg.Validate(); err != nil {
			t.Fatalf("validate tiered test configuration: %v", err)
		}
	}

	fixture.observedClient = &deleteObjectsPreflightContextCapture{
		CacheClient: fixture.cacheClient,
		method:      owner.method,
		key:         owner.key,
		contexts:    make(chan context.Context, 1),
	}
	fixture.operationContext = fixture.observedClient.contexts
	cacheStore := cache.NewCacheWithClient(fixture.observedClient, &cfg.Cache)
	fixture.forwarder = &deleteObjectsPreflightForwarder{}
	service := proxy.NewService(fixture.forwarder, cacheStore, cfg)
	router := NewServer(service, "127.0.0.1", 0, false, cfg.Server.MaxInflightRequests).Router()
	fixture.gateway = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(fixture.handlerDone)
		router.ServeHTTP(w, r)
	}))
	fixture.client = &http.Client{}

	t.Cleanup(func() {
		owner.releaseOwner()
		fixture.gateway.Close()
		fixture.client.CloseIdleConnections()
		_ = fixture.cacheClient.Close()
		fixture.grpcServer.Stop()
		_ = fixture.listener.Close()
	})
	return &fixture
}

func (f *deleteObjectsPreflightFixture) startDelete(ctx context.Context) (<-chan deleteObjectsPreflightHTTPResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.gateway.URL+"/"+deleteObjectsPreflightBucket+"?delete", strings.NewReader(deleteObjectsPreflightBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", bulkDeleteFenceAuthorization)
	req.Header.Set("Content-Type", "application/xml")

	done := make(chan deleteObjectsPreflightHTTPResult, 1)
	go func() {
		resp, err := f.client.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		done <- deleteObjectsPreflightHTTPResult{err: err}
	}()
	return done, nil
}

// The registered DeleteObjects route must pass a client cancellation through
// each mode's real cache RPC, finish the handler before owner release, and stop
// before invoking the upstream forwarder.
func TestHandleDeleteObjects_CancelledPreflightReturnsBeforeOwnerRelease(t *testing.T) {
	cases := []struct {
		name     string
		mode     string
		method   string
		cacheKey string
	}{
		{
			name:     "legacy-tombstone-put",
			mode:     "legacy",
			method:   "put",
			cacheKey: cache.MakeTombstoneKey(deleteObjectsPreflightBucket, deleteObjectsPreflightKey),
		},
		{
			name:     "cas-versioned-delete",
			mode:     "cas",
			method:   "delete-if-version",
			cacheKey: cache.MakeMetaKey(deleteObjectsPreflightBucket, deleteObjectsPreflightKey),
		},
		{
			name:     "tiered-prior-version-read",
			mode:     "tiered",
			method:   "get-with-version",
			cacheKey: cache.MakeMetaKey(deleteObjectsPreflightBucket, deleteObjectsPreflightKey),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			owner := newDeleteObjectsPreflightRPCOwner(tc.method, tc.cacheKey, 1)
			fixture := newDeleteObjectsPreflightFixture(t, tc.mode, owner)
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			clientDone, err := fixture.startDelete(requestCtx)
			if err != nil {
				t.Fatalf("start DeleteObjects request: %v", err)
			}
			waitCtx, cancelWait := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancelWait()
			if err := waitBulkDeleteFenceSignal(waitCtx, owner.started, "stalled remote cache RPC"); err != nil {
				t.Fatal(err)
			}

			operationCtx := <-fixture.operationContext
			cancelRequest()
			if operationCtx.Done() == nil {
				t.Errorf("cache preflight context has no cancellation signal")
			} else {
				select {
				case <-operationCtx.Done():
					if !errors.Is(operationCtx.Err(), context.Canceled) {
						t.Errorf("cache preflight context error after client cancellation = %v, want context.Canceled", operationCtx.Err())
					}
				case <-time.After(2 * time.Second):
					t.Errorf("cache preflight context did not observe HTTP request cancellation before owner release")
				}
			}
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			clientDoneCh := clientDone
			rpcDoneCh := (<-chan struct{})(owner.rpcReturned)
			handlerDoneCh := (<-chan struct{})(fixture.handlerDone)
			var clientResult deleteObjectsPreflightHTTPResult
			clientReturned, rpcReturned, handlerReturned := false, false, false
		waitForCancellation:
			for !clientReturned || !rpcReturned || !handlerReturned {
				select {
				case result := <-clientDoneCh:
					clientResult = result
					clientReturned = true
					clientDoneCh = nil
				case <-rpcDoneCh:
					rpcReturned = true
					rpcDoneCh = nil
				case <-handlerDoneCh:
					handlerReturned = true
					handlerDoneCh = nil
				case <-timer.C:
					break waitForCancellation
				}
			}

			if !clientReturned || !rpcReturned || !handlerReturned {
				t.Errorf("canceled DeleteObjects preflight did not return before cache-owner release: client=%t remote_rpc=%t handler=%t", clientReturned, rpcReturned, handlerReturned)
			}
			if clientReturned && !errors.Is(clientResult.err, context.Canceled) {
				t.Errorf("canceled DeleteObjects client call error = %v, want context.Canceled", clientResult.err)
			}
			select {
			case <-owner.cancelObserved:
			default:
				t.Errorf("cache owner did not observe the canceled DeleteObjects request before release")
			}

			owner.releaseOwner()
			if !handlerReturned {
				if err := waitBulkDeleteFenceSignal(waitCtx, fixture.handlerDone, "handler cleanup after releasing the cache owner"); err != nil {
					t.Fatalf("handler did not clean up after owner release: %v", err)
				}
			}
			if got := fixture.observedClient.calls.Load(); got != 1 {
				t.Errorf("canceled DeleteObjects prepared %d matching cache calls, want only the held first-key call", got)
			}
			if got := owner.operationCalls.Load(); got != 1 {
				t.Errorf("canceled DeleteObjects dispatched %d matching remote RPCs, want only the held first-key call", got)
			}
			if got := fixture.forwarder.calls.Load(); got != 0 {
				t.Errorf("canceled DeleteObjects called ForwardWithCapture %d times, want 0", got)
			}
		})
	}
}
