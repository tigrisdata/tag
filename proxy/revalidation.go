package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
)

// revalidateAndServe sends a conditional GET to upstream and serves the result.
// Supports both full-object and range requests. For range requests, the Range header
// is included in the conditional GET so upstream returns 304 or 206.
// On 304 Not Modified: serves from cached body (or range).
// On 200/206 (changed): streams new body to client and updates cache.
// On error: serves stale data from cache as fallback.
func (s *Service) revalidateAndServe(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key, accessKey, secretKey string,
	meta *cache.CachedObjectMeta,
	start time.Time,
) error {
	metrics.RecordRevalidationTriggered()

	rangeHeader := r.Header.Get("Range")

	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Str("etag", meta.ETag).
		Str("range", rangeHeader).
		Msg("Revalidating cached object with upstream")

	// Send conditional GET to upstream (includes Range header if present).
	// Uses the parent context (not a separate timeout) to avoid truncating body
	// streaming for large objects. The httpClient has its own 5-minute timeout.
	resp, err := s.forwarder.DoConditionalGetRequest(ctx, bucket, key, accessKey, secretKey, meta.ETag, meta.LastModified, rangeHeader)
	if err != nil {
		// Upstream error — serve stale from cache
		log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Revalidation failed, serving stale")
		metrics.RecordRevalidationFailed()
		metrics.RecordRevalidationStaleServed()
		return s.serveStaleFromCache(ctx, w, r, bucket, key, meta, rangeHeader, start)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotModified:
		revalErr := s.handleRevalidation304(ctx, w, r, bucket, key, meta, rangeHeader, start)
		if revalErr == nil {
			return nil
		}
		// Cache body unavailable despite 304 — fall through to upstream.
		// serveFromCache returns errors before committing headers, so we can
		// safely write a new response. Range errors may have partial headers
		// committed (serveRangeFromCache writes headers before streaming), so
		// we return those directly.
		if rangeHeader != "" {
			return revalErr
		}
		log.Warn().Err(revalErr).Str("bucket", bucket).Str("key", key).
			Msg("Revalidation 304 cache body unavailable, fetching from upstream")
		w.Header().Set(XCacheHeader, XCacheMiss)
		forwardErr := s.forwarder.Forward(ctx, w, r)
		status := "success"
		if forwardErr != nil {
			status = "error"
		}
		metrics.RecordRequest("GetObject", status, time.Since(start).Seconds())
		return forwardErr
	case http.StatusOK:
		// Full-object response (no range or upstream ignored range)
		return s.handleRevalidation200(ctx, w, bucket, key, resp, start)
	case http.StatusPartialContent:
		// Range response — object changed, upstream returned only the requested range
		return s.handleRevalidation206Range(ctx, w, r, bucket, key, accessKey, secretKey, resp, start)
	default:
		// Unexpected status (4xx, 5xx) — drain body for connection reuse, serve stale
		io.Copy(io.Discard, resp.Body)
		log.Warn().
			Int("status", resp.StatusCode).
			Str("bucket", bucket).
			Str("key", key).
			Msg("Revalidation got unexpected status, serving stale")
		metrics.RecordRevalidationFailed()
		metrics.RecordRevalidationStaleServed()
		return s.serveStaleFromCache(ctx, w, r, bucket, key, meta, rangeHeader, start)
	}
}

// handleRevalidation304 handles a 304 Not Modified revalidation response.
// Serves from cached body (or range if requested). Does not refresh cache TTL
// because ocache has no TTL-refresh operation and re-writing data is inefficient.
func (s *Service) handleRevalidation304(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
	meta *cache.CachedObjectMeta,
	rangeHeader string,
	start time.Time,
) error {
	metrics.RecordRevalidationNotModified()
	log.Debug().Str("bucket", bucket).Str("key", key).Msg("Revalidation 304 - object unchanged")

	// Serve range or full body from cache
	if rangeHeader != "" {
		return s.serveRangeFromCache(ctx, w, r, bucket, key, meta, rangeHeader, start)
	}
	return s.serveFromCache(ctx, w, bucket, key, meta, start)
}

// handleRevalidation200 handles a 200 OK revalidation response (object changed).
// Streams the new body to the client while simultaneously updating the cache.
func (s *Service) handleRevalidation200(
	ctx context.Context,
	w http.ResponseWriter,
	bucket, key string,
	resp *http.Response,
	start time.Time,
) error {
	metrics.RecordRevalidationUpdated()
	log.Debug().Str("bucket", bucket).Str("key", key).Msg("Revalidation 200 - object changed, streaming new data")

	// Build new metadata from upstream response
	newMeta := cache.MetaFromHTTPHeaders(bucket, key, resp.StatusCode, resp.Header)

	// Check if the new object should be cached
	shouldCache := newMeta.IsCacheable(s.config.Cache.SizeThreshold) &&
		s.cache.IsEnabled() &&
		!s.hasNoCacheHeaders(resp.Header)

	// Always delete stale cache entry when upstream confirms the object changed.
	// Even if the new version is uncacheable (too large, no-store), the old cached
	// version is known-stale and must not be served to future requests.
	s.cache.Delete(context.Background(), bucket, key)

	// Capture writeStartTime after Delete so our own tombstone doesn't block
	// the subsequent cache write. A concurrent DELETE arriving after this point
	// will have a newer tombstone that correctly blocks our write.
	writeStartTime := time.Now().UnixNano()

	// Write response headers to client
	copyHeaders(w.Header(), resp.Header)
	w.Header().Set(XCacheHeader, XCacheRevalidated)
	w.WriteHeader(resp.StatusCode)

	if !shouldCache {
		// Not cacheable — just stream to client
		n, copyErr := io.Copy(w, resp.Body)
		metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))
		status := "success"
		if copyErr != nil {
			status = "error"
		}
		metrics.RecordRequest("GetObject", status, time.Since(start).Seconds())
		return copyErr
	}

	// Stream to client AND cache simultaneously via TeeReader + pipe
	pr, pw := io.Pipe()
	ttl := int(s.config.Cache.TTL.Seconds())

	// Background goroutine: write to cache from pipe reader
	cacheErrCh := make(chan error, 1)
	go func() {
		cacheErr := s.cache.PutWithMetaStreamTombstoneAware(
			context.Background(), bucket, key, newMeta, pr, ttl, writeStartTime,
		)
		if cacheErr != nil {
			log.Warn().Err(cacheErr).Str("bucket", bucket).Str("key", key).Msg("Cache write failed during revalidation update")
			// Drain remaining pipe data so the foreground TeeReader doesn't block.
			// Without this, io.Pipe's zero-buffer causes pw.Write to hang.
			io.Copy(io.Discard, pr)
		}
		cacheErrCh <- cacheErr
	}()

	// Foreground: stream to client via TeeReader (also writes to pipe for cache)
	teeReader := io.TeeReader(resp.Body, pw)
	n, copyErr := io.Copy(w, teeReader)
	metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))

	// Close pipe writer to signal EOF to cache reader
	if copyErr != nil {
		pw.CloseWithError(copyErr)
	} else {
		pw.Close()
	}

	// Wait for cache write to complete (with timeout)
	select {
	case cacheErr := <-cacheErrCh:
		if cacheErr != nil {
			log.Debug().Err(cacheErr).Str("bucket", bucket).Str("key", key).Msg("Cache write error after revalidation")
		}
	case <-time.After(cacheWriteTimeoutForSize(newMeta.ContentLength)):
		log.Warn().Str("bucket", bucket).Str("key", key).Msg("Cache write timeout after revalidation")
	}

	status := "success"
	if copyErr != nil {
		status = "error"
	}
	metrics.RecordRequest("GetObject", status, time.Since(start).Seconds())
	return copyErr
}

// serveFromCache serves an object from the cache body.
// Used as fallback during revalidation (304 or error paths).
func (s *Service) serveFromCache(
	ctx context.Context,
	w http.ResponseWriter,
	bucket, key string,
	meta *cache.CachedObjectMeta,
	start time.Time,
) error {
	// Zero-byte objects: no body to serve
	if meta.ContentLength == 0 {
		metrics.RecordCacheHit()
		meta.WriteHeaders(w)
		w.Header().Set(XCacheHeader, XCacheHit)
		w.WriteHeader(meta.StatusCode)
		metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
		return nil
	}

	// Small objects: buffer and serve
	if meta.ContentLength <= smallObjectThreshold {
		bodyBuf := bufferPool.Get().(*bytes.Buffer)
		bodyBuf.Reset()

		bodyErr := s.cache.GetBodyStream(ctx, bucket, key, meta.ETag, bodyBuf)
		if bodyErr == nil && bodyBuf.Len() > 0 {
			metrics.RecordCacheHit()
			meta.WriteHeaders(w)
			w.Header().Set(XCacheHeader, XCacheHit)
			w.WriteHeader(meta.StatusCode)
			n, _ := w.Write(bodyBuf.Bytes())
			metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))
			metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
			putBuffer(bodyBuf)
			return nil
		}
		putBuffer(bodyBuf)
		// Body unavailable — return error (caller may fall through to upstream)
		if bodyErr != nil {
			return fmt.Errorf("cache body read failed: %w", bodyErr)
		}
		return fmt.Errorf("cache body empty for %s/%s", bucket, key)
	}

	// Large objects: stream via pipe
	pr, pw := io.Pipe()
	go func() {
		err := s.cache.GetBodyStream(ctx, bucket, key, meta.ETag, pw)
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()

	// Read first byte to verify body exists
	firstByte := make([]byte, 1)
	n, readErr := pr.Read(firstByte)
	if readErr != nil {
		pr.Close()
		return fmt.Errorf("cache body unavailable: %w", readErr)
	}

	metrics.RecordCacheHit()
	meta.WriteHeaders(w)
	w.Header().Set(XCacheHeader, XCacheHit)
	w.WriteHeader(meta.StatusCode)

	cw := &countingWriter{w: w}
	cw.Write(firstByte[:n])
	io.Copy(cw, pr)
	pr.Close()

	metrics.BytesTransferred.WithLabelValues("out").Add(float64(cw.written))
	metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
	return nil
}

// handleRevalidation206Range handles a 206 Partial Content revalidation response.
// The object changed and upstream returned only the requested range.
// Streams the range to the client, deletes stale cache, and triggers a background
// full-object fetch to repopulate the cache (same pattern as handleRangeWithBackgroundCache).
func (s *Service) handleRevalidation206Range(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key, accessKey, secretKey string,
	resp *http.Response,
	start time.Time,
) error {
	metrics.RecordRevalidationUpdated()
	log.Debug().Str("bucket", bucket).Str("key", key).Msg("Revalidation 206 - object changed, streaming range")

	// Delete stale cache entry before repopulating
	s.cache.Delete(context.Background(), bucket, key)

	// Determine total object size from Content-Range header
	totalSize := extractTotalSizeFromContentRange(resp.Header.Get("Content-Range"))

	// Stream range response to client
	copyHeaders(w.Header(), resp.Header)
	w.Header().Set(XCacheHeader, XCacheRevalidated)
	w.WriteHeader(resp.StatusCode)

	n, copyErr := io.Copy(w, resp.Body)
	metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))

	status := "success"
	if copyErr != nil {
		status = "error"
	}
	metrics.RecordRequest("GetObject", status, time.Since(start).Seconds())

	// Trigger background full-object fetch to repopulate cache
	if totalSize > 0 &&
		totalSize <= s.config.Cache.SizeThreshold &&
		s.cache.IsEnabled() &&
		accessKey != "" && secretKey != "" {
		s.triggerBackgroundCacheFetch(bucket, key, accessKey, secretKey, hasNoAuthCredentials(r))
	}

	return copyErr
}

// serveStaleFromCache serves stale content from cache, handling both full and range requests.
func (s *Service) serveStaleFromCache(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
	meta *cache.CachedObjectMeta,
	rangeHeader string,
	start time.Time,
) error {
	if rangeHeader != "" {
		return s.serveRangeFromCache(ctx, w, r, bucket, key, meta, rangeHeader, start)
	}
	return s.serveFromCache(ctx, w, bucket, key, meta, start)
}

// revalidateAndServeHead sends a conditional HEAD to upstream for a HEAD request.
// On 304: serves cached headers (no body).
// On 200: serves new headers from upstream and invalidates stale cache.
// On error: serves stale headers from cache.
func (s *Service) revalidateAndServeHead(
	ctx context.Context,
	w http.ResponseWriter,
	bucket, key, accessKey, secretKey string,
	meta *cache.CachedObjectMeta,
	start time.Time,
) error {
	metrics.RecordRevalidationTriggered()

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("Revalidating cached HEAD with upstream (conditional HEAD)")

	resp, err := s.forwarder.DoConditionalHeadRequest(ctx, bucket, key, accessKey, secretKey, meta.ETag, meta.LastModified)

	// Object changed — serve new headers from upstream, invalidate cache
	if err == nil && resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		metrics.RecordRevalidationUpdated()
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("HEAD revalidation 200 - object changed")

		s.cache.Delete(context.Background(), bucket, key)
		io.Copy(io.Discard, resp.Body)

		copyHeaders(w.Header(), resp.Header)
		w.Header().Set(XCacheHeader, XCacheRevalidated)
		w.WriteHeader(resp.StatusCode)
		metrics.RecordRequest("HeadObject", "success", time.Since(start).Seconds())
		return nil
	}

	// 304, error, or unexpected status — record specific metrics, then serve cached headers
	if err != nil {
		log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("HEAD revalidation failed, serving stale")
		metrics.RecordRevalidationFailed()
		metrics.RecordRevalidationStaleServed()
	} else {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotModified {
			metrics.RecordRevalidationNotModified()
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("HEAD revalidation 304 - unchanged")
		} else {
			io.Copy(io.Discard, resp.Body)
			log.Warn().Int("status", resp.StatusCode).Str("bucket", bucket).Str("key", key).Msg("HEAD revalidation unexpected status, serving stale")
			metrics.RecordRevalidationFailed()
			metrics.RecordRevalidationStaleServed()
		}
	}

	metrics.RecordCacheHit()
	meta.WriteHeaders(w)
	w.Header().Set(XCacheHeader, XCacheHit)
	w.WriteHeader(meta.StatusCode)
	metrics.RecordRequest("HeadObject", "success", time.Since(start).Seconds())
	return nil
}

// shouldForceRevalidate checks if the client is requesting cache revalidation.
// Per RFC 7234, Cache-Control: no-cache means "must revalidate with origin before serving".
func shouldForceRevalidate(r *http.Request) bool {
	cc := r.Header.Get("Cache-Control")
	return strings.Contains(cc, "no-cache") || strings.Contains(cc, "max-age=0")
}

// shouldBypassCache checks if the client is requesting full cache bypass.
// Cache-Control: no-store means "do not use or store cached data".
func shouldBypassCache(r *http.Request) bool {
	cc := r.Header.Get("Cache-Control")
	return strings.Contains(cc, "no-store")
}
