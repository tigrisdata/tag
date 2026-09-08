package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// failingDeleteClient fails the next DeleteIfVersion once — simulating the
// transient backend failure that leaves a known-stale row in place after a
// guarded delete.
type failingDeleteClient struct {
	cacheclient.CacheClient
	failNext bool
}

func (f *failingDeleteClient) DeleteIfVersion(ctx context.Context, key string, expected uint64) error {
	if f.failNext {
		f.failNext = false
		return errors.New("transient backend failure")
	}
	return f.CacheClient.DeleteIfVersion(ctx, key, expected)
}

// A successful revalidation must replace known-stale metadata even when the
// preliminary guarded delete fails transiently. Put-if-absent alone would
// refuse against the surviving stale row and keep serving it; the
// revalidation precondition picker overwrites exactly that row instead.
func TestRevalidation200_ReplacesStaleEntryAfterFailedDelete(t *testing.T) {
	newBody := "fresh content"
	mock := &mockForwarder{
		conditionalResp: &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(newBody)),
			Header: http.Header{
				"Content-Type":   []string{"text/plain"},
				"Content-Length": []string{"13"},
				"Etag":           []string{`"new"`},
			},
		},
	}

	wrapped := &failingDeleteClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	c := cache.NewCacheWithClient(wrapped, &cfg.Cache)
	svc := NewService(mock, c, cfg)
	ctx := context.Background()
	bucket, key := "b", "k"

	stale := &cache.CachedObjectMeta{
		Bucket: bucket, Key: key, ETag: `"old"`,
		ContentType: "text/plain", ContentLength: 5, StatusCode: http.StatusOK,
	}
	if err := c.PutWithMeta(ctx, bucket, key, stale, []byte("stale"), 0); err != nil {
		t.Fatalf("seed stale entry: %v", err)
	}

	// The guarded delete inside the revalidation path fails once: the stale
	// row survives it.
	wrapped.failNext = true

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	if err := svc.revalidateAndServe(ctx, w, r, bucket, key, "access", "secret", stale, time.Now()); err != nil {
		t.Fatalf("revalidateAndServe: %v", err)
	}
	if w.Body.String() != newBody {
		t.Fatalf("client body = %q, want the fresh content", w.Body.String())
	}

	meta, found, err := c.GetMeta(ctx, bucket, key)
	if err != nil || !found || meta == nil {
		t.Fatalf("meta after revalidation: found=%v err=%v", found, err)
	}
	if meta.ETag != `"new"` {
		t.Fatalf("meta ETag = %q: known-stale entry survived a successful revalidation", meta.ETag)
	}
}

// The 206 path's repair runs through the BACKGROUND fetch, and must replace
// known-stale metadata even when the preliminary guarded delete failed — the
// same contract as the 200 path, enforced through the trigger's precondition.
func TestRevalidation206_BackgroundFetchReplacesStaleAfterFailedDelete(t *testing.T) {
	freshBody := "fresh content!"
	mock := &mockForwarder{
		conditionalResp: &http.Response{
			StatusCode: http.StatusPartialContent,
			Body:       io.NopCloser(strings.NewReader("fres")),
			Header: http.Header{
				"Content-Type":  []string{"text/plain"},
				"Content-Range": []string{"bytes 0-3/14"},
				"Etag":          []string{`"new"`},
			},
		},
		doFullObjectFunc: func(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error) {
			h := http.Header{}
			h.Set("ETag", `"new"`)
			h.Set("Content-Type", "text/plain")
			h.Set("Content-Length", "14")
			return &http.Response{StatusCode: http.StatusOK, Header: h, ContentLength: 14, Body: io.NopCloser(strings.NewReader(freshBody))}, nil
		},
	}

	wrapped := &failingDeleteClient{CacheClient: cacheclient.NewMemoryCache()}
	cfg := config.NewDefault()
	c := cache.NewCacheWithClient(wrapped, &cfg.Cache)
	svc := NewService(mock, c, cfg)
	ctx := context.Background()
	bucket, key := "b", "k"

	stale := &cache.CachedObjectMeta{
		Bucket: bucket, Key: key, ETag: `"old"`,
		ContentType: "text/plain", ContentLength: 5, StatusCode: http.StatusOK,
	}
	if err := c.PutWithMeta(ctx, bucket, key, stale, []byte("stale"), 0); err != nil {
		t.Fatalf("seed stale entry: %v", err)
	}
	wrapped.failNext = true

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	r.Header.Set("Range", "bytes=0-3")
	r.Header.Set("Cache-Control", "no-cache")
	// A credentialed request, so the background repair uses the signed fetch.
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=access/20260907/auto/s3/aws4_request")
	if err := svc.revalidateAndServe(ctx, w, r, bucket, key, "access", "secret", stale, time.Now()); err != nil {
		t.Fatalf("revalidateAndServe: %v", err)
	}
	if w.Code != http.StatusPartialContent {
		t.Fatalf("client status = %d, want 206", w.Code)
	}

	// The repair is a background fetch: poll until the stale entry is replaced.
	deadline := time.After(3 * time.Second)
	for {
		meta, found, _ := c.GetMeta(ctx, bucket, key)
		if found && meta != nil && meta.ETag == `"new"` {
			return
		}
		select {
		case <-deadline:
			meta, found, _ := c.GetMeta(ctx, bucket, key)
			t.Fatalf("stale entry not replaced by the background fetch: found=%v meta=%+v", found, meta)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
