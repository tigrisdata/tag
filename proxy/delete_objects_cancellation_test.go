package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
)

type deleteObjectsTieredPriorGate struct {
	cacheclient.CacheClient
	stalledKey       string
	started          chan struct{}
	operationContext chan context.Context
	release          chan struct{}
	startOnce        sync.Once
	releaseOnce      sync.Once
}

func (c *deleteObjectsTieredPriorGate) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	if key != c.stalledKey {
		return nil, 1, false, nil
	}
	c.startOnce.Do(func() { close(c.started) })
	c.operationContext <- ctx
	select {
	case <-ctx.Done():
		return nil, 0, false, ctx.Err()
	case <-c.release:
		return nil, 1, false, nil
	}
}

func (c *deleteObjectsTieredPriorGate) releaseOwner() {
	c.releaseOnce.Do(func() { close(c.release) })
}

// Cancellation during a later tiered prior lookup must return before owner
// release and run every deferred claim release for keys already prepared.
func TestHandleDeleteObjects_CancelledTieredPreflightReleasesClaims(t *testing.T) {
	const bucket = "tiered-cancel-bucket"
	const firstKey = "first"
	const stalledKey = "stalled"

	gate := &deleteObjectsTieredPriorGate{
		CacheClient:      cacheclient.NewMemoryCache(),
		stalledKey:       cache.MakeMetaKey(bucket, stalledKey),
		started:          make(chan struct{}),
		operationContext: make(chan context.Context, 1),
		release:          make(chan struct{}),
	}
	t.Cleanup(gate.releaseOwner)
	forwardCalls := 0
	forwarder := &mockForwarder{captureFunc: func(_ context.Context, _ http.ResponseWriter, _ *http.Request) (*ResponseCapture, error) {
		forwardCalls++
		return &ResponseCapture{StatusCode: http.StatusOK, Body: []byte(`<DeleteResult/>`), Complete: true}, nil
	}}
	service, _ := newTieredTestServiceWithClient(forwarder, 1024, gate)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requestBody := `<Delete><Object><Key>` + firstKey + `</Key></Object><Object><Key>` + stalledKey + `</Key></Object></Delete>`
	request := httptest.NewRequest(http.MethodPost, "/"+bucket+"?delete", strings.NewReader(requestBody)).WithContext(ctx)
	done := make(chan error, 1)
	go func() { done <- service.HandleDeleteObjects(httptest.NewRecorder(), request) }()

	select {
	case <-gate.started:
	case <-time.After(5 * time.Second):
		t.Fatal("tiered prior lookup did not reach the held cache owner")
	}
	service.retierMu.Lock()
	firstClaimBeforeCancel := service.retierClaims[bucket+"|"+firstKey]
	stalledClaimBeforeCancel := service.retierClaims[bucket+"|"+stalledKey]
	service.retierMu.Unlock()
	if firstClaimBeforeCancel != 1 || stalledClaimBeforeCancel != 1 {
		t.Fatalf("claims while the second prior lookup was held = %d/%d, want 1/1", firstClaimBeforeCancel, stalledClaimBeforeCancel)
	}

	priorCtx := <-gate.operationContext
	cancel()
	if priorCtx.Done() == nil || !errors.Is(priorCtx.Err(), context.Canceled) {
		t.Errorf("tiered prior lookup context did not carry request cancellation: done=%t err=%v", priorCtx.Done() != nil, priorCtx.Err())
	}
	var resultErr error
	returnedBeforeRelease := false
	select {
	case resultErr = <-done:
		returnedBeforeRelease = true
	case <-time.After(2 * time.Second):
	}

	service.retierMu.Lock()
	firstClaimAfterCancel := service.retierClaims[bucket+"|"+firstKey]
	stalledClaimAfterCancel := service.retierClaims[bucket+"|"+stalledKey]
	service.retierMu.Unlock()
	if !returnedBeforeRelease {
		t.Errorf("tiered DeleteObjects did not return before cache-owner release")
	}
	if returnedBeforeRelease && !errors.Is(resultErr, context.Canceled) {
		t.Errorf("canceled tiered DeleteObjects returned error = %v, want context.Canceled", resultErr)
	}
	if returnedBeforeRelease && (firstClaimAfterCancel != 0 || stalledClaimAfterCancel != 0) {
		t.Errorf("canceled tiered DeleteObjects returned while re-tier claims remained registered: %d/%d", firstClaimAfterCancel, stalledClaimAfterCancel)
	}
	if forwardCalls != 0 {
		t.Errorf("canceled tiered DeleteObjects called ForwardWithCapture %d times after request cancellation", forwardCalls)
	}

	gate.releaseOwner()
	if !returnedBeforeRelease {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("handler did not return after releasing the cache owner")
		}
	}
}
