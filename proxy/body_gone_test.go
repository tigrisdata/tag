package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestBodyGone pins the denylist contract: only transient failures (where the
// cached body is probably fine and we merely couldn't finish reading it) skip the
// heal. Every other error invalidates the orphaned metadata — the cache reports an
// unusable body in several shapes, and missing one leaves meta outliving its body,
// which forwards every request upstream until the meta TTL expires.
func TestBodyGone(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error is not gone", nil, false},

		// Unusable body — must heal.
		{"body missing", cache.ErrNotFound, true},
		{"wrapped body missing", fmt.Errorf("cache body unavailable: %w", cache.ErrNotFound), true},
		{"empty stream reads as EOF", io.EOF, true},
		{"wrapped empty stream", fmt.Errorf("cache body unavailable: %w", io.EOF), true},
		{"body shorter than metadata claims", status.Error(codes.InvalidArgument, "invalid range"), true},
		{"plain empty-body error", errors.New("cache body empty for b/k"), true},

		// Transient — must NOT evict a still-valid entry.
		{"canceled context", context.Canceled, false},
		{"wrapped canceled context", fmt.Errorf("cache body unavailable: %w", context.Canceled), false},
		{"deadline exceeded", context.DeadlineExceeded, false},
		{"grpc canceled (cluster read)", status.Error(codes.Canceled, "context canceled"), false},
		{"grpc deadline exceeded", status.Error(codes.DeadlineExceeded, "deadline"), false},
		{"grpc peer unavailable", status.Error(codes.Unavailable, "connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bodyGone(tt.err); got != tt.want {
				t.Errorf("bodyGone(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// A metadata entry whose versioned body is present but ZERO bytes is inconsistent.
// ocache surfaces this as an out-of-range read rather than a clean EOF, so it must
// still heal — otherwise the orphaned meta survives and every request re-probes and
// forwards upstream until its TTL expires.
func TestServeRangeFromCache_EmptyBodyReportsGone(t *testing.T) {
	cfg := config.NewDefault()
	memCache := cacheclient.NewMemoryCache()
	c := cache.NewCacheWithClient(memCache, &cfg.Cache)
	svc := NewService(&mockForwarder{}, c, cfg)
	ctx := context.Background()
	bucket, key := "b", "k"

	meta := &cache.CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"e1"`, ContentLength: 100, StatusCode: 200}
	// Seed a zero-byte body at the versioned key while meta claims 100 bytes.
	if err := memCache.Put(ctx, cache.MakeBodyKey(bucket, key, meta.ETag), []byte{}, 60); err != nil {
		t.Fatalf("seed empty body: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	served, err := svc.serveRangeFromCache(ctx, w, r, bucket, key, meta, "bytes=0-9", time.Now())
	if served {
		t.Fatal("an empty body must not be served as a 206")
	}
	if !bodyGone(err) {
		t.Errorf("serveRangeFromCache err = %v, bodyGone = false; want true so the orphaned meta heals", err)
	}
}
