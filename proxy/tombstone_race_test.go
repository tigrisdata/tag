package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// metaCached polls for up to d, returning true as soon as the key's metadata is
// visible. Populates finish on a background goroutine, so tests poll rather than
// sleep; a negative assertion must exhaust d to be meaningful.
func metaCached(c *cache.Cache, bucket, key string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		if _, found, _ := c.GetMeta(context.Background(), bucket, key); found {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestStreamFromUpstream_TombstoneDuringFetchBlocksPopulate covers issue #97: the
// cache-write timestamp must be stamped BEFORE the upstream request, not after its
// headers arrive.
//
// Upstream takes its read snapshot some time after we send. If an invalidation
// (a racing PUT/DELETE) lands during that round-trip, a timestamp stamped after the
// response is NEWER than the tombstone, the guard `tombTs >= writeStartTime` is
// false, and the pre-invalidation body we just read gets cached — stale. Stamping
// before the request makes our timestamp strictly earlier than the read snapshot,
// so any racing invalidation is provably newer and blocks the write.
func TestStreamFromUpstream_TombstoneDuringFetchBlocksPopulate(t *testing.T) {
	const bucket, key = "b", "k"
	const body = "pre-invalidation-body"

	newResp := func() *http.Response {
		h := http.Header{}
		h.Set("ETag", `"e1"`)
		h.Set("Content-Type", "text/plain")
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        h,
			Body:          io.NopCloser(strings.NewReader(body)),
			ContentLength: int64(len(body)),
		}
	}

	// Control: with no racing invalidation the object must actually be cached —
	// otherwise the negative case below would pass for the wrong reason.
	t.Run("no invalidation - populate succeeds", func(t *testing.T) {
		mock := &mockForwarder{}
		mock.doRequestFunc = func(ctx context.Context, r *http.Request, ak, sk string) (*http.Response, error) {
			return newResp(), nil
		}
		svc, c := newTestService(mock, true)

		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
		if err := svc.HandleGetObject(w, r); err != nil {
			t.Fatalf("HandleGetObject: %v", err)
		}
		if !metaCached(c, bucket, key, 2*time.Second) {
			t.Fatal("control: object should have been cached (test harness is not exercising the populate path)")
		}
	})

	// The race: an invalidation lands while the upstream fetch is in flight.
	t.Run("invalidation during fetch - populate blocked", func(t *testing.T) {
		mock := &mockForwarder{}
		var c *cache.Cache
		mock.doRequestFunc = func(ctx context.Context, r *http.Request, ak, sk string) (*http.Response, error) {
			// A PUT/DELETE lands upstream and invalidates while our GET is in flight.
			// Our writeStartTime is stamped before this call, so this tombstone is newer.
			if err := c.WriteTombstone(context.Background(), bucket, key); err != nil {
				t.Errorf("WriteTombstone: %v", err)
			}
			return newResp(), nil
		}
		var svc *Service
		svc, c = newTestService(mock, true)

		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
		if err := svc.HandleGetObject(w, r); err != nil {
			t.Fatalf("HandleGetObject: %v", err)
		}
		// The client still gets its bytes; only the populate is skipped.
		if got := w.Body.String(); got != body {
			t.Errorf("client body = %q, want %q", got, body)
		}
		if metaCached(c, bucket, key, 300*time.Millisecond) {
			t.Error("stale populate was not blocked — writeStartTime must be stamped before the upstream read")
		}
	})
}

// populateWindow is the longest a populate can run before it checks the tombstone:
// the upstream fetch plus the streaming write of the largest cacheable object.
func populateWindow(threshold int64) time.Duration {
	return cacheWriteTimeoutForSize(threshold) + backgroundFetchTimeout
}

// TestTombstoneTTLCoversPopulateWindow is a cross-package drift guard for issue
// #97[2]: a tombstone must outlive any populate that could race it, or it expires
// mid-write, the guard reads zero, and a stale populate lands.
//
// It SWEEPS thresholds rather than spot-checking a few. The TTL and the window grow
// on independent curves, so they can cross in a narrow band that hand-picked points
// step right over — an earlier multiplicative TTL kept a healthy margin at 64 MiB,
// 1 GiB and 10 GiB while collapsing to zero at ~1.5 GiB, exactly between the
// samples.
func TestTombstoneTTLCoversPopulateWindow(t *testing.T) {
	const (
		step = 16 * 1024 * 1024        // 16 MiB
		max  = 24 * 1024 * 1024 * 1024 // sweep well past any realistic threshold
	)

	worstMargin := time.Duration(1<<62 - 1)
	var worstAt int64
	for size := int64(0); size <= max; size += step {
		ttl := time.Duration(cache.TombstoneTTLSeconds(size)) * time.Second
		window := populateWindow(size)
		if margin := ttl - window; margin < worstMargin {
			worstMargin, worstAt = margin, size
		}
		if ttl <= window {
			t.Fatalf("size_threshold=%d (%.2f GiB): tombstone TTL %v does not outlive the populate window %v — a tombstone can expire mid-write, letting a stale populate through",
				size, float64(size)/(1<<30), ttl, window)
		}
	}
	t.Logf("smallest margin over the sweep: %v at size_threshold=%.2f GiB", worstMargin, float64(worstAt)/(1<<30))

	// Explicitly pin the band where a multiplicative TTL used to collapse, and the
	// configured default.
	for _, size := range []int64{
		config.DefaultCacheSizeThreshold,
		300 * 5 * 1024 * 1024, // write time == the 300s fetch bound: the old crossing point
	} {
		ttl := time.Duration(cache.TombstoneTTLSeconds(size)) * time.Second
		if window := populateWindow(size); ttl <= window {
			t.Errorf("size_threshold=%d: TTL %v <= window %v", size, ttl, window)
		}
	}
}
