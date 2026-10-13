package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

func TestIsNotFoundError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "key not found",
			err:      &testError{msg: "key not found"},
			expected: true,
		},
		{
			name:     "not found",
			err:      &testError{msg: "not found"},
			expected: true,
		},
		{
			name:     "contains NotFound",
			err:      &testError{msg: "rpc error: code = NotFound desc = key does not exist"},
			expected: true,
		},
		{
			name:     "contains not found lowercase",
			err:      &testError{msg: "cache: key not found in store"},
			expected: true,
		},
		{
			name:     "other error",
			err:      &testError{msg: "connection timeout"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNotFoundError(tt.err)
			if result != tt.expected {
				t.Errorf("isNotFoundError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

// testError is a simple error implementation for testing
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

type bodyReadTestClient struct {
	cacheclient.CacheClient
	stream      func(context.Context, string, io.Writer) error
	rangeStream func(context.Context, string, int64, int64, io.Writer) error
}

func (c *bodyReadTestClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	return c.stream(ctx, key, w)
}

func (c *bodyReadTestClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if c.rangeStream != nil {
		return c.rangeStream(ctx, key, start, end, w)
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

func newBodyReadTestCache(t *testing.T, timeout time.Duration, stream func(context.Context, string, io.Writer) error) *Cache {
	t.Helper()
	cfg := config.NewDefault()
	cfg.Cache.BodyReadIdleTimeout = timeout
	return NewCacheWithClient(&bodyReadTestClient{
		CacheClient: cacheclient.NewMemoryCache(),
		stream:      stream,
	}, &cfg.Cache)
}

func newRangeReadTestCache(t *testing.T, timeout time.Duration, stream func(context.Context, string, int64, int64, io.Writer) error) *Cache {
	t.Helper()
	cfg := config.NewDefault()
	cfg.Cache.BodyReadIdleTimeout = timeout
	return NewCacheWithClient(&bodyReadTestClient{
		CacheClient: cacheclient.NewMemoryCache(),
		rangeStream: stream,
	}, &cfg.Cache)
}

func TestCache_GetBodyStreamCancelsStalledRead(t *testing.T) {
	const idleTimeout = 20 * time.Millisecond
	started := make(chan struct{})
	parent := context.WithValue(context.Background(), "body-read-test", "parent")
	cache := newBodyReadTestCache(t, idleTimeout, func(ctx context.Context, _ string, _ io.Writer) error {
		if got := ctx.Value("body-read-test"); got != "parent" {
			t.Errorf("child context value = %v, want parent value", got)
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	var body bytes.Buffer
	startedAt := time.Now()
	err := cache.GetBodyStream(parent, "bucket", "key", `"etag"`, &body)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetBodyStream() error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(startedAt); elapsed < idleTimeout/2 || elapsed > time.Second {
		t.Fatalf("stalled read returned after %v, want around %v", elapsed, idleTimeout)
	}
	if parent.Err() != nil {
		t.Fatalf("parent context was canceled: %v", parent.Err())
	}
	select {
	case <-started:
	default:
		t.Fatal("cache client was not called")
	}
}

func TestCache_GetBodyStreamRejectsLateWriteAfterIdleCancellation(t *testing.T) {
	const idleTimeout = 20 * time.Millisecond
	cache := newBodyReadTestCache(t, idleTimeout, func(ctx context.Context, _ string, w io.Writer) error {
		<-ctx.Done()
		if _, err := w.Write([]byte("late cache body")); !errors.Is(err, context.Canceled) {
			t.Errorf("late write error = %v, want context.Canceled", err)
		}
		// Deliberately ignore the late write error. The cache wrapper must still
		// report the idle cancellation and leave the destination untouched.
		return nil
	})

	var body bytes.Buffer
	err := cache.GetBodyStream(context.Background(), "bucket", "key", `"etag"`, &body)
	if !errors.Is(err, ErrBodyReadIdleTimeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("GetBodyStream() error = %v, want idle timeout and context.Canceled", err)
	}
	if body.Len() != 0 {
		t.Fatalf("body length = %d, want 0 after late write", body.Len())
	}
}

func TestCache_GetBodyStreamResetsIdleTimeoutOnNonEmptyWrites(t *testing.T) {
	const idleTimeout = 30 * time.Millisecond
	want := []byte("firstsecondthird")
	cache := newBodyReadTestCache(t, idleTimeout, func(_ context.Context, _ string, w io.Writer) error {
		for _, chunk := range [][]byte{[]byte("first"), []byte("second"), []byte("third")} {
			if _, err := w.Write(chunk); err != nil {
				return err
			}
			time.Sleep(idleTimeout / 3)
		}
		return nil
	})

	var body bytes.Buffer
	if err := cache.GetBodyStream(context.Background(), "bucket", "key", `"etag"`, &body); err != nil {
		t.Fatalf("GetBodyStream() error = %v, want nil", err)
	}
	if !bytes.Equal(body.Bytes(), want) {
		t.Fatalf("body = %q, want %q", body.Bytes(), want)
	}
}

func TestCache_GetBodyStreamDoesNotTreatEmptyWritesAsProgress(t *testing.T) {
	const idleTimeout = 20 * time.Millisecond
	cache := newBodyReadTestCache(t, idleTimeout, func(ctx context.Context, _ string, w io.Writer) error {
		if _, err := w.Write(nil); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	})

	var body bytes.Buffer
	if err := cache.GetBodyStream(context.Background(), "bucket", "key", `"etag"`, &body); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetBodyStream() error = %v, want context.Canceled", err)
	}
	if body.Len() != 0 {
		t.Fatalf("body length = %d, want 0", body.Len())
	}
}

type delayedBodyWriter struct {
	bytes.Buffer
	delay          time.Duration
	nonEmptyWrites int
}

func (w *delayedBodyWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		w.nonEmptyWrites++
		if w.nonEmptyWrites == 2 {
			time.Sleep(w.delay)
		}
	}
	return w.Buffer.Write(p)
}

func TestCache_GetBodyStreamDoesNotCancelDuringSlowDestinationWrite(t *testing.T) {
	const idleTimeout = 40 * time.Millisecond
	cache := newBodyReadTestCache(t, idleTimeout, func(ctx context.Context, _ string, w io.Writer) error {
		if _, err := w.Write([]byte("first")); err != nil {
			return err
		}
		time.Sleep(idleTimeout / 4)
		if _, err := w.Write([]byte("second")); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(idleTimeout / 2):
		}
		_, err := w.Write([]byte("third"))
		return err
	})

	var body delayedBodyWriter
	body.delay = 2 * idleTimeout
	if err := cache.GetBodyStream(context.Background(), "bucket", "key", `"etag"`, &body); err != nil {
		t.Fatalf("GetBodyStream() error = %v, want nil", err)
	}
	if got := body.String(); got != "firstsecondthird" {
		t.Fatalf("body = %q, want %q", got, "firstsecondthird")
	}
}

func TestCache_GetRangeStreamsCancelStalledReads(t *testing.T) {
	const idleTimeout = 20 * time.Millisecond
	tests := []struct {
		name string
		read func(*Cache, context.Context, io.Writer) error
	}{
		{
			name: "whole object range",
			read: func(c *Cache, ctx context.Context, w io.Writer) error {
				return c.GetRangeStream(ctx, "bucket", "key", `"etag"`, 1, 4, w)
			},
		},
		{
			name: "byte zero range",
			read: func(c *Cache, ctx context.Context, w io.Writer) error {
				return c.GetRangeStream(ctx, "bucket", "key", `"etag"`, 0, 0, w)
			},
		},
		{
			name: "block range",
			read: func(c *Cache, ctx context.Context, w io.Writer) error {
				return c.GetBlockRangeStream(ctx, "bucket", "key", `"etag"`, 8, 0, 1, 4, w)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			started := make(chan struct{})
			parent := context.WithValue(context.Background(), "range-read-test", "parent")
			c := newRangeReadTestCache(t, idleTimeout, func(ctx context.Context, _ string, _, _ int64, _ io.Writer) error {
				if got := ctx.Value("range-read-test"); got != "parent" {
					t.Errorf("child context value = %v, want parent value", got)
				}
				close(started)
				<-ctx.Done()
				return ctx.Err()
			})

			var body bytes.Buffer
			startedAt := time.Now()
			err := test.read(c, parent, &body)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("range read error = %v, want context.Canceled", err)
			}
			if elapsed := time.Since(startedAt); elapsed < idleTimeout/2 || elapsed > time.Second {
				t.Fatalf("stalled range read returned after %v, want around %v", elapsed, idleTimeout)
			}
			if parent.Err() != nil {
				t.Fatalf("parent context was canceled: %v", parent.Err())
			}
			if body.Len() != 0 {
				t.Fatalf("body length = %d, want 0", body.Len())
			}
			select {
			case <-started:
			default:
				t.Fatal("cache client was not called")
			}
		})
	}
}

func TestCache_GetRangeStreamDoesNotCancelDuringSlowDestinationWrite(t *testing.T) {
	const idleTimeout = 40 * time.Millisecond
	c := newRangeReadTestCache(t, idleTimeout, func(ctx context.Context, _ string, _, _ int64, w io.Writer) error {
		if _, err := w.Write([]byte("first")); err != nil {
			return err
		}
		time.Sleep(idleTimeout / 4)
		if _, err := w.Write([]byte("second")); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(idleTimeout / 2):
		}
		_, err := w.Write([]byte("third"))
		return err
	})

	var body delayedBodyWriter
	body.delay = 2 * idleTimeout
	if err := c.GetRangeStream(context.Background(), "bucket", "key", `"etag"`, 1, 9, &body); err != nil {
		t.Fatalf("GetRangeStream() error = %v, want nil", err)
	}
	if got := body.String(); got != "firstsecondthird" {
		t.Fatalf("body = %q, want %q", got, "firstsecondthird")
	}
}

func TestCache_IsEnabled_Disabled(t *testing.T) {
	cache := &Cache{enabled: false}

	if cache.IsEnabled() {
		t.Error("IsEnabled() = true, want false for disabled cache")
	}
}

func TestCache_IsEnabled_Enabled(t *testing.T) {
	cache := &Cache{enabled: true}

	if !cache.IsEnabled() {
		t.Error("IsEnabled() = false, want true for enabled cache")
	}
}

func TestCache_GetMode_Disabled(t *testing.T) {
	cache := &Cache{enabled: false}

	mode := cache.GetMode()
	if mode != "disabled" {
		t.Errorf("GetMode() = %q, want %q", mode, "disabled")
	}
}

func TestCache_GetConnectedNodes_Disabled(t *testing.T) {
	cache := &Cache{enabled: false}

	nodes := cache.GetConnectedNodes()
	if nodes != nil {
		t.Errorf("GetConnectedNodes() = %v, want nil", nodes)
	}
}

func TestCache_Close_Disabled(t *testing.T) {
	cache := &Cache{enabled: false}

	err := cache.Close()
	if err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

func TestCache_IsClosed(t *testing.T) {
	cache := &Cache{enabled: true, closed: false}

	if cache.IsClosed() {
		t.Error("IsClosed() = true before close")
	}

	cache.closed = true

	if !cache.IsClosed() {
		t.Error("IsClosed() = false after close")
	}
}

func TestCache_DisabledOperationsReturnNil(t *testing.T) {
	cache := &Cache{enabled: false}

	// PutWithMeta should succeed silently
	testMeta := &CachedObjectMeta{Bucket: "bucket", Key: "key"}
	if err := cache.PutWithMeta(t.Context(), "bucket", "key", testMeta, []byte("data"), 60); err != nil {
		t.Errorf("PutWithMeta() error = %v, want nil", err)
	}

	// Delete should succeed silently
	if err := cache.Delete(t.Context(), "bucket", "key"); err != nil {
		t.Errorf("Delete() error = %v, want nil", err)
	}

	// Has should return false
	if cache.Has(t.Context(), "bucket", "key") {
		t.Error("Has() = true, want false")
	}
}
