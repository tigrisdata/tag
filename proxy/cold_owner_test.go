package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tigrisdata/tag/proxy/broadcast"
)

// warmForwarder drives fetchAndBroadcast's cold-owner warm branch in isolation.
// The inline upstream fetch (DoRequestWithCreds) returns doRequestErr;
// DoFullObjectRequest — reached only via the deduplicated background warm —
// reports the bucket/key it was asked to fetch on fullObjectFetch.
type warmForwarder struct {
	mockForwarder
	doRequestErr    error
	fullObjectFetch chan [2]string
}

func (m *warmForwarder) DoRequestWithCreds(_ context.Context, _ *http.Request, _, _ string) (*http.Response, error) {
	return nil, m.doRequestErr
}

func (m *warmForwarder) DoFullObjectRequest(_ context.Context, bucket, key, _, _ string) (*http.Response, error) {
	m.fullObjectFetch <- [2]string{bucket, key}
	return nil, errors.New("warm: stop background fetch")
}

// runFetchAndBroadcast drives fetchAndBroadcast with a pre-canceled context (so
// writeChunksToResponse returns promptly) and the given inline-fetch error.
func runFetchAndBroadcast(t *testing.T, doRequestErr error) *warmForwarder {
	t.Helper()
	mock := &warmForwarder{
		doRequestErr:    doRequestErr,
		fullObjectFetch: make(chan [2]string, 1),
	}
	svc, _ := newTestService(mock, true) // empty in-memory cache → GetMeta reports !found

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the cold-owner peer/client deadline has already expired

	bcaster := broadcast.NewBroadcaster(broadcast.DefaultChannelBuffer)
	req := httptest.NewRequest(http.MethodGet, "/cold-bucket/cold-key", nil)
	// Authenticated request → the cold-owner warm uses a signed fetch
	// (DoFullObjectRequest). An anonymous request would warm via an unsigned fetch.
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=access/20260101/us-east-1/s3/aws4_request, Signature=deadbeef")
	w := httptest.NewRecorder()

	_ = svc.fetchAndBroadcast(ctx, w, req, "cold-bucket", "cold-key", "access", "secret", bcaster, time.Now(), "MISS")
	return mock
}

// TestFetchAndBroadcast_WarmsOnContextCancel verifies that when the client
// context is canceled before the inline fetch populates the cache (issue #63
// path A) and the upstream was healthy (the fetch was aborted by cancellation,
// surfacing a context error), fetchAndBroadcast hands warming off to the
// deduplicated background fetcher.
func TestFetchAndBroadcast_WarmsOnContextCancel(t *testing.T) {
	mock := runFetchAndBroadcast(t, context.Canceled)

	select {
	case got := <-mock.fullObjectFetch:
		if got[0] != "cold-bucket" || got[1] != "cold-key" {
			t.Fatalf("background warm fetched %q/%q, want cold-bucket/cold-key", got[0], got[1])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected a background warm fetch after a canceled request, but none occurred")
	}
}

// TestFetchAndBroadcast_NoWarmOnUpstreamError verifies that when the upstream
// itself failed (not the client), fetchAndBroadcast does NOT warm — even though
// the client context is canceled — so a doomed retry never amplifies load
// against an already-unhealthy upstream.
func TestFetchAndBroadcast_NoWarmOnUpstreamError(t *testing.T) {
	mock := runFetchAndBroadcast(t, errors.New("connection refused"))

	select {
	case got := <-mock.fullObjectFetch:
		t.Fatalf("unexpected background warm fetch %q/%q on an upstream failure", got[0], got[1])
	case <-time.After(200 * time.Millisecond):
		// No warm — correct.
	}
}
