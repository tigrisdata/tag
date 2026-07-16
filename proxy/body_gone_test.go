package proxy

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/tigrisdata/tag/cache"
)

// TestBodyGone verifies only a genuinely missing/inconsistent body invalidates the
// orphaned metadata. Transient failures — most importantly a canceled context from
// a client that disconnected mid-read — must NOT evict a still-valid hot entry.
func TestBodyGone(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error is not gone", nil, false},
		{"body missing", cache.ErrNotFound, true},
		{"wrapped body missing", fmt.Errorf("cache body unavailable: %w", cache.ErrNotFound), true},
		{"empty body", errCacheBodyEmpty, true},
		{"wrapped empty body", fmt.Errorf("cache body empty for b/k: %w", errCacheBodyEmpty), true},
		{"canceled context must not evict", context.Canceled, false},
		{"wrapped canceled context must not evict", fmt.Errorf("cache body unavailable: %w", context.Canceled), false},
		{"deadline exceeded must not evict", context.DeadlineExceeded, false},
		{"transient io error must not evict", errors.New("read tcp: i/o timeout"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bodyGone(tt.err); got != tt.want {
				t.Errorf("bodyGone(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
