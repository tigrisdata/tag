package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestColdOwner_RangeWarmsCacheOnClientCancel reproduces issue #63 (path B): a
// byte-range cache miss must still warm the full object even when the client
// cancels mid-stream. Previously the background fetch was triggered only after a
// successful io.Copy, so a client whose deadline was shorter than the cold fetch
// canceled the stream and the warming fetch never fired — a self-sustaining
// cold-miss/cancel loop. The fix triggers the (detached, deduplicated) fetch via
// defer so it runs regardless of the io.Copy outcome.
func TestColdOwner_RangeWarmsCacheOnClientCancel(t *testing.T) {
	const bucket = "cold-bucket"
	const key = "blocks/compacted/01.sst"

	// A multi-MB body ensures TAG is blocked writing to the client (its send
	// buffers fill because the client stops reading) when the client disconnects,
	// so io.Copy fails on the write side deterministically — the cold-owner
	// mid-stream cancel, without relying on context propagation timing.
	body := make([]byte, 2*1024*1024)
	for i := range body {
		body[i] = byte('A' + (i % 26))
	}
	total := len(body)

	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			// Range request: stream the full body in chunks, stopping when the
			// client goes away (write error) or the request is canceled.
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", total-1, total))
			w.Header().Set("Content-Length", strconv.Itoa(total))
			w.WriteHeader(http.StatusPartialContent)
			flusher, _ := w.(http.Flusher)
			for off := 0; off < total; off += 64 * 1024 {
				end := off + 64*1024
				if end > total {
					end = total
				}
				if _, err := w.Write(body[off:end]); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
				if r.Context().Err() != nil {
					return
				}
			}
			return
		}

		// Full-object background fetch (no Range header): return the whole body so
		// the cache warms.
		w.Header().Set("Content-Length", strconv.Itoa(total))
		w.Header().Set("ETag", `"cold-etag"`)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}

	env := NewTestEnvironmentWithCacheHandler(handler)
	defer env.Close()

	// The shared embedded cache persists across tests and process re-runs
	// (-count=N, retries), so evict any stale entry to keep the precondition
	// repeatable.
	_ = env.Cache.DeleteWithMeta(context.Background(), bucket, key)
	require.False(t, env.IsCached(bucket, key), "object must start uncached")

	// Issue a signed range request on a cancelable context, read a little, then
	// cancel — simulating the client walking away before the cold fetch completes.
	ctx, cancel := context.WithCancel(context.Background())
	req, err := env.Signer.SignRequest(ctx, http.MethodGet, "/"+bucket+"/"+key, nil, "", TestAccessKey, TestSecretKey, http.Header{})
	require.NoError(t, err)
	req.Header.Set("Range", "bytes=0-1048575")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	buf := make([]byte, 8)
	_, _ = io.ReadFull(resp.Body, buf)
	cancel()
	_ = resp.Body.Close()

	// Despite the canceled client request, the deferred background fetch must warm
	// the full object so the next request hits and the cold loop terminates.
	require.True(t, env.WaitForCached(bucket, key, 5*time.Second),
		"cache should warm after a canceled range request (issue #63 path B)")

	cachedBody, found := env.GetCachedObject(bucket, key)
	require.True(t, found, "cached body should be retrievable")
	require.Equal(t, body, cachedBody, "cached body should match full object")
}

// TestColdOwner_FullObjectWarmsCacheOnClientCancel reproduces issue #63 (path A):
// a full-object cache miss must finish populating the cache even when the client
// cancels mid-stream. When the client cancels the inline fetch, warming is handed
// off to the deduplicated background fetcher, which completes on a detached
// context. Both the inline GET and the warming GET are full-object (no-Range)
// requests, so the handler simply streams the whole body and tolerates the inline
// request being cut mid-stream.
func TestColdOwner_FullObjectWarmsCacheOnClientCancel(t *testing.T) {
	const bucket = "cold-bucket"
	const key = "blocks/compacted/02-full.sst"

	// Multi-MB body so TAG is blocked writing to the client when it disconnects,
	// cutting the inline fetch deterministically on the write side.
	body := make([]byte, 2*1024*1024)
	for i := range body {
		body[i] = byte('a' + (i % 26))
	}
	total := len(body)

	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(total))
		w.Header().Set("ETag", `"cold-full-etag"`)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for off := 0; off < total; off += 64 * 1024 {
			end := off + 64*1024
			if end > total {
				end = total
			}
			if _, err := w.Write(body[off:end]); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			if r.Context().Err() != nil {
				return
			}
		}
	}

	env := NewTestEnvironmentWithCacheHandler(handler)
	defer env.Close()

	// The shared embedded cache persists across tests and process re-runs
	// (-count=N, retries), so evict any stale entry to keep the precondition
	// repeatable.
	_ = env.Cache.DeleteWithMeta(context.Background(), bucket, key)
	require.False(t, env.IsCached(bucket, key), "object must start uncached")

	ctx, cancel := context.WithCancel(context.Background())
	req, err := env.Signer.SignRequest(ctx, http.MethodGet, "/"+bucket+"/"+key, nil, "", TestAccessKey, TestSecretKey, http.Header{})
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	buf := make([]byte, 8)
	_, _ = io.ReadFull(resp.Body, buf)
	cancel()
	_ = resp.Body.Close()

	// Despite the canceled client request, the background warm must populate the
	// full object so the next request hits and the cold loop terminates.
	require.True(t, env.WaitForCached(bucket, key, 5*time.Second),
		"cache should warm after a canceled full-object request (issue #63 path A)")

	cachedBody, found := env.GetCachedObject(bucket, key)
	require.True(t, found, "cached body should be retrievable")
	require.Equal(t, body, cachedBody, "cached body should match full object")
}
