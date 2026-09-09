package proxy

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestHasNoCacheDirectives(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cacheControls []string
		want          bool
	}{
		{name: "no directives", want: false},
		{name: "no-store", cacheControls: []string{"no-store"}, want: true},
		{name: "private with field list", cacheControls: []string{`private="Set-Cookie"`}, want: true},
		{name: "later repeated directive", cacheControls: []string{"max-age=60", "No-Store"}, want: true},
		{name: "after quoted extension", cacheControls: []string{`foo="value, still", private`}, want: true},
		{name: "extension names", cacheControls: []string{"no-storex, privatex"}, want: false},
		{name: "extension value", cacheControls: []string{"foo=no-store"}, want: false},
		{name: "quoted extension value", cacheControls: []string{`foo="no-store, private"`}, want: false},
		{name: "escaped quote in extension value", cacheControls: []string{`foo="a\", no-store", max-age=60`}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasNoCacheDirectives(tc.cacheControls); got != tc.want {
				t.Errorf("hasNoCacheDirectives(%q) = %t, want %t", tc.cacheControls, got, tc.want)
			}
		})
	}
}

func TestContextBoundReaderDoesNotAcceptCloseAsComplete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pipeReader, pipeWriter := io.Pipe()
	defer pipeWriter.Close()

	readErr := make(chan error, 1)
	go func() {
		_, err := (&contextBoundReader{ctx: ctx, reader: pipeReader}).Read(make([]byte, 1))
		readErr <- err
	}()

	cancel()
	_ = pipeWriter.Close()
	select {
	case err := <-readErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("contextBoundReader.Read() = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("contextBoundReader.Read() remained blocked after cancellation")
	}
}

func TestWaitForCacheWriteHonorsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	release := make(chan struct{})
	cacheErrCh := make(chan error, 1)
	go func() {
		<-release
		cacheErrCh <- nil
	}()

	if err, timedOut := waitForCacheWrite(ctx, cacheErrCh); !timedOut || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForCacheWrite() = (%v, %t), want deadline timeout", err, timedOut)
	}

	// The production channel is buffered so a writer that ignores cancellation can
	// finish without blocking after the fetch has returned.
	close(release)
	select {
	case <-cacheErrCh:
	case <-time.After(time.Second):
		t.Fatal("cache writer did not finish after release")
	}
}

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
