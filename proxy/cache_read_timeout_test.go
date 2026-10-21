package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

func TestCacheReadTimeoutForSize(t *testing.T) {
	tests := []struct {
		name string
		n    int64
		want time.Duration
	}{
		{name: "probe (zero) returns floor", n: 0, want: cacheReadTimeout},
		{name: "negative returns floor", n: -1, want: cacheReadTimeout},
		{name: "sub-MiB adds nothing", n: 1024*1024 - 1, want: cacheReadTimeout},
		{name: "16 MiB block adds 16s", n: 16 << 20, want: cacheReadTimeout + 16*time.Second},
		{name: "1 GiB caps at the server WriteTimeout", n: 1 << 30, want: maxCacheReadTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cacheReadTimeoutForSize(tt.n); got != tt.want {
				t.Fatalf("cacheReadTimeoutForSize(%d) = %v, want %v", tt.n, got, tt.want)
			}
		})
	}
}

// injectCacheReadTimeout shrinks both per-read deadlines (buffered and client-paced)
// for one test.
func injectCacheReadTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	injectSizedCacheReadTimeout(t, d)
	prevStream := cacheStreamReadTimeout
	cacheStreamReadTimeout = d
	t.Cleanup(func() { cacheStreamReadTimeout = prevStream })
}

// injectSizedCacheReadTimeout shrinks only the size-scaled (buffered-read) deadline,
// leaving the client-paced stream bound at its production value.
func injectSizedCacheReadTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := cacheReadTimeoutForSize
	cacheReadTimeoutForSize = func(int64) time.Duration { return d }
	t.Cleanup(func() { cacheReadTimeoutForSize = prev })
}

// gatedResponseWriter models a slow client: every Write blocks until release is
// closed, and first is closed when the first Write arrives.
type gatedResponseWriter struct {
	http.ResponseWriter
	first   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gatedResponseWriter) Write(p []byte) (int, error) {
	g.once.Do(func() { close(g.first) })
	<-g.release
	return g.ResponseWriter.Write(p)
}

// stalledRead models a peer that never answers: it blocks until ctx is done and
// records the deadline the first read observed, so a test can prove the read was
// bounded by the injected per-read deadline rather than by the request context.
type stalledRead struct {
	mu          sync.Mutex
	calls       int
	hadDeadline bool
	remaining   time.Duration
	err         error
}

func (s *stalledRead) block(ctx context.Context) error {
	now := time.Now()
	deadline, ok := ctx.Deadline()
	<-ctx.Done()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.calls == 0 {
		s.hadDeadline, s.remaining, s.err = ok, deadline.Sub(now), ctx.Err()
	}
	s.calls++
	return ctx.Err()
}

func (s *stalledRead) assertBoundedBy(t *testing.T, injected time.Duration) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.calls == 0 {
		t.Fatal("cache read was never attempted")
	}
	if !s.hadDeadline || s.remaining > injected {
		t.Fatalf("cache read deadline = (set=%t, remaining=%v), want <= injected %v", s.hadDeadline, s.remaining, injected)
	}
	if !errors.Is(s.err, context.DeadlineExceeded) {
		t.Fatalf("cache read ctx error = %v, want context.DeadlineExceeded", s.err)
	}
}

// handleGetObjectBounded runs HandleGetObject and fails fast instead of hanging the
// package if a stalled cache read is not bounded.
func handleGetObjectBounded(t *testing.T, svc *Service, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- svc.HandleGetObject(w, r) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("HandleGetObject() error = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("HandleGetObject did not return: the stalled cache read was not bounded")
	}
}

// A warm whole-body serve whose peer read stalls fails within the injected read
// deadline, is classified as transient (the entry is NOT invalidated), and the
// request falls through to upstream.
func TestHandleGetObject_StalledWholeBodyReadFallsThroughWithoutInvalidating(t *testing.T) {
	const injected = 20 * time.Millisecond
	injectCacheReadTimeout(t, injected)

	body := bytes.Repeat([]byte("cached body"), 16384) // > smallObjectThreshold: streamed path
	stall := &stalledRead{}
	svc, store, meta := newLargeStreamCacheService(t, body, func(ctx context.Context, _ string, _ io.Writer) error {
		return stall.block(ctx)
	})
	upstream := newBlockMock(body, meta.ETag)
	svc.forwarder = upstream

	w := httptest.NewRecorder()
	handleGetObjectBounded(t, svc, w, fullGet(meta.Bucket, meta.Key))

	stall.assertBoundedBy(t, injected)
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), body) {
		t.Fatalf("response = (%d, %d bytes), want (200, %d bytes from upstream)", w.Code, w.Body.Len(), len(body))
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheMiss {
		t.Fatalf("%s = %q, want %q (served upstream after the timed-out cache read)", XCacheHeader, got, XCacheMiss)
	}
	if got := upstream.forwards.Load(); got != 1 {
		t.Fatalf("upstream forwards = %d, want 1", got)
	}
	cur, found, err := store.GetMeta(context.Background(), meta.Bucket, meta.Key)
	if err != nil || !found || cur.ETag != meta.ETag {
		t.Fatalf("entry after timed-out read = (found=%t, etag=%v, err=%v), want the still-valid %s kept", found, cur, err, meta.ETag)
	}
}

// A cold block-mode range whose presence probe stalls fails within the injected read
// deadline, the probe error is not treated as absence (no block fetches, no
// invalidation), and the range is served from upstream.
func TestHandleGetObject_StalledBlockProbeFallsThroughWithoutInvalidating(t *testing.T) {
	const injected = 20 * time.Millisecond
	injectCacheReadTimeout(t, injected)

	body := []byte("ABCDEFGHIJKLMNOP") // 16 bytes → blocks of 4
	upstream := newBlockMock(body, `"v1"`)
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	stall := &stalledRead{}
	client := &testRangeStreamCacheClient{
		CacheClient: cacheclient.NewMemoryCache(),
		rangeStream: func(ctx context.Context, _ string, _, _ int64, _ io.Writer) error { return stall.block(ctx) },
	}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	svc := NewService(upstream, store, cfg)

	// Metadata only: every block is absent, so a multi-block range takes the probe path.
	meta := &cache.CachedObjectMeta{
		Bucket:             wowBucket,
		Key:                wowKey,
		ETag:               `"v1"`,
		ContentLength:      int64(len(body)),
		BlockSize:          4,
		ContentLengthKnown: true,
		StatusCode:         http.StatusOK,
	}
	if wrote, err := store.PutMetaIfVersion(context.Background(), wowBucket, wowKey, meta, 60, cache.VersionAny); err != nil || !wrote {
		t.Fatalf("seed block-mode meta: wrote=%t err=%v", wrote, err)
	}

	w := httptest.NewRecorder()
	handleGetObjectBounded(t, svc, w, blockGet(wowBucket, wowKey, "bytes=0-7"))

	stall.assertBoundedBy(t, injected)
	if w.Code != http.StatusPartialContent || w.Body.String() != "ABCDEFGH" {
		t.Fatalf("response = (%d, %q), want (206, ABCDEFGH from upstream)", w.Code, w.Body.String())
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheMiss {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheMiss)
	}
	if got := upstream.blockGets.Load(); got != 0 {
		t.Fatalf("block fetches = %d, want 0: a timed-out probe must not be read as absence", got)
	}
	if got := upstream.forwards.Load(); got != 1 {
		t.Fatalf("upstream forwards = %d, want 1", got)
	}
	cur, found, err := store.GetMeta(context.Background(), wowBucket, wowKey)
	if err != nil || !found || cur.ETag != meta.ETag {
		t.Fatalf("entry after timed-out probe = (found=%t, etag=%v, err=%v), want the still-valid entry kept", found, cur, err)
	}
}

// A slow-but-progressing client on a large streamed body is paced by the flat
// stream bound (the server's WriteTimeout), never by the size-scaled deadline: with
// the size-scaled value injected far below the client's stall, the whole body still
// arrives and the cache read is never canceled.
func TestServeFromCache_SlowClientIsNotCutOffBySizeScaledDeadline(t *testing.T) {
	const sized = 20 * time.Millisecond
	injectSizedCacheReadTimeout(t, sized) // cacheStreamReadTimeout stays at 5m

	body := bytes.Repeat([]byte("cached body"), 16384) // > smallObjectThreshold: streamed path
	chunks := [][]byte{body[:1], body[1:65536], body[65536:]}
	var (
		mu        sync.Mutex
		remaining time.Duration
		streamErr error
	)
	stream := func(ctx context.Context, _ string, w io.Writer) error {
		deadline, ok := ctx.Deadline()
		mu.Lock()
		if ok {
			remaining = time.Until(deadline)
		}
		mu.Unlock()
		for _, chunk := range chunks {
			// A canceled read context is what a gRPC peer stream would observe.
			if err := ctx.Err(); err != nil {
				mu.Lock()
				streamErr = err
				mu.Unlock()
				return err
			}
			if _, err := w.Write(chunk); err != nil {
				return err
			}
		}
		return nil
	}
	svc, _, meta := newLargeStreamCacheService(t, body, stream)

	rec := httptest.NewRecorder()
	client := &gatedResponseWriter{ResponseWriter: rec, first: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- svc.serveFromCache(context.Background(), client, meta.Bucket, meta.Key, meta, time.Now())
	}()

	// Hold the client's first write well past the injected size-scaled deadline,
	// then let it drain.
	select {
	case <-client.first:
	case <-time.After(10 * time.Second):
		t.Fatal("cache stream never reached the client")
	}
	select {
	case <-time.After(5 * sized):
	case err := <-done:
		t.Fatalf("serveFromCache returned %v while the client was still stalled", err)
	}
	close(client.release)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveFromCache() error = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serveFromCache did not return after the client drained")
	}
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), body) {
		t.Fatalf("response = (%d, %d bytes), want (200, the full %d-byte body)", rec.Code, rec.Body.Len(), len(body))
	}
	mu.Lock()
	defer mu.Unlock()
	if streamErr != nil {
		t.Fatalf("cache stream was canceled: %v", streamErr)
	}
	if remaining <= sized {
		t.Fatalf("streamed read deadline remaining = %v, want the flat stream bound (> injected %v)", remaining, sized)
	}
}
