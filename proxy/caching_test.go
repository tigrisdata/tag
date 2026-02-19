package proxy

import (
	"testing"
	"time"
)

func TestCacheWriteTimeoutForSize(t *testing.T) {
	tests := []struct {
		name          string
		contentLength int64
		wantTimeout   time.Duration
	}{
		{
			name:          "zero content length returns base timeout",
			contentLength: 0,
			wantTimeout:   cacheWriteTimeout,
		},
		{
			name:          "negative content length returns base timeout",
			contentLength: -1,
			wantTimeout:   cacheWriteTimeout,
		},
		{
			name:          "small object returns base timeout",
			contentLength: 100 * 1024 * 1024, // 100 MB → 20s < 60s
			wantTimeout:   cacheWriteTimeout,
		},
		{
			name:          "512MB object scales up",
			contentLength: 512 * 1024 * 1024, // 512 MB → ~102s
			wantTimeout:   time.Duration(512*1024*1024/minCacheWriteThroughput) * time.Second,
		},
		{
			name:          "1GB object scales up",
			contentLength: 1024 * 1024 * 1024, // 1 GB → ~204s
			wantTimeout:   time.Duration(1024*1024*1024/minCacheWriteThroughput) * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cacheWriteTimeoutForSize(tt.contentLength)
			if got != tt.wantTimeout {
				t.Errorf("cacheWriteTimeoutForSize(%d) = %v, want %v", tt.contentLength, got, tt.wantTimeout)
			}
		})
	}
}
