package proxy

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

const (
	// X-Cache header constants for indicating cache status.
	XCacheHeader      = "X-Cache"
	XCacheHit         = "HIT"
	XCacheMiss        = "MISS"
	XCacheBypass      = "BYPASS"      // Cache-Control: no-store bypassed cache
	XCacheDisabled    = "DISABLED"    // Cache is disabled
	XCacheRevalidated = "REVALIDATED" // Object revalidated with upstream
)

// Service provides the core caching proxy logic.
type Service struct {
	forwarder               RequestForwarder
	cache                   *cache.Cache
	config                  *config.Config
	cacheSemaphore          chan struct{}      // Bounds concurrent cache-populate operations (nil = unlimited)
	broadcastManager        *broadcast.Manager // For streaming request coalescing
	activeBackgroundFetches sync.Map           // Dedup for background full-object fetches (range caching)
}

// NewService creates a new proxy service.
func NewService(forwarder RequestForwarder, cache *cache.Cache, cfg *config.Config) *Service {
	channelBuf := cfg.Broadcast.ChannelBuffer
	if channelBuf <= 0 {
		channelBuf = broadcast.DefaultChannelBuffer
	}

	// Bound concurrent cache-populate operations. MaxConcurrentWrites is a hard
	// count ceiling; on top of it we cap concurrency so the memory buffered by
	// in-flight populates stays under MaxPopulateMemoryBytes. Each populate buffers
	// up to ~channel_buffer × chunk_size bytes, so a count alone can pin many GB
	// (e.g. 256 × ~64MB ≈ 16GB) under large-object fan-out — the memory budget is
	// what actually prevents that.
	writeLimit := effectiveCacheWriteLimit(cfg)
	var cacheSem chan struct{}
	if writeLimit > 0 {
		cacheSem = make(chan struct{}, writeLimit)
	}

	log.Info().
		Int("max_concurrent_writes", cfg.Cache.MaxConcurrentWrites).
		Int64("max_populate_memory_bytes", cfg.Cache.MaxPopulateMemoryBytes).
		Int("effective_cache_populate_slots", writeLimit).
		Msg("Cache-populate concurrency configured")

	return &Service{
		forwarder:        forwarder,
		cache:            cache,
		config:           cfg,
		cacheSemaphore:   cacheSem,
		broadcastManager: broadcast.NewManager(channelBuf),
	}
}

// effectiveCacheWriteLimit returns the number of concurrent cache-populate slots,
// combining the count ceiling (MaxConcurrentWrites) with the memory budget
// (MaxPopulateMemoryBytes). A non-positive count returns 0 (limiter disabled /
// unbounded), preserving the explicit-disable contract. Otherwise the count is
// reduced so that count × per-populate-buffer-bytes stays within the budget; a
// non-positive budget disables the memory cap. At least one slot is always allowed.
func effectiveCacheWriteLimit(cfg *config.Config) int {
	count := cfg.Cache.MaxConcurrentWrites
	if count <= 0 {
		return 0 // limiter disabled — unbounded (matches prior negative-value semantics)
	}
	budget := cfg.Cache.MaxPopulateMemoryBytes
	if budget <= 0 {
		return count // memory cap disabled
	}

	chunkSize := int64(cfg.Broadcast.ChunkSize)
	if chunkSize <= 0 {
		chunkSize = broadcast.DefaultChunkSize
	}
	channelBuf := int64(cfg.Broadcast.ChannelBuffer)
	if channelBuf <= 0 {
		channelBuf = broadcast.DefaultChannelBuffer
	}
	// Per-populate buffering: the broadcast listener channel (channelBuf chunks)
	// plus the cache-write queue in setupCacheListener (~channelBuf/4 chunks).
	perPopulate := (channelBuf + channelBuf/4) * chunkSize
	if perPopulate <= 0 {
		return count
	}
	memCap := max(int(budget/perPopulate), 1) // always allow at least one populate
	if memCap < count {
		return memCap
	}
	return count
}

// acquireCacheSlot tries to reserve a cache-populate slot without blocking. It
// returns true (and must be paired with releaseCacheSlot) when a slot is
// reserved, or when the limiter is disabled (nil semaphore). It returns false
// when the concurrent-cache-write limit is saturated, in which case the caller
// should skip caching rather than block or spawn unbounded work.
func (s *Service) acquireCacheSlot() bool {
	if s.cacheSemaphore == nil {
		return true
	}
	select {
	case s.cacheSemaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

// releaseCacheSlot releases a slot previously reserved by acquireCacheSlot.
func (s *Service) releaseCacheSlot() {
	if s.cacheSemaphore == nil {
		return
	}
	<-s.cacheSemaphore
}

// statusRecorder wraps http.ResponseWriter to capture the response status code
// written while forwarding an upstream response. Forward() returns nil even when
// upstream responds 4xx/5xx (the response streamed successfully), so mutating
// handlers use this to gate post-forward cache re-invalidation on an actual 2xx —
// otherwise a rejected PUT/DELETE/COPY would still tombstone the destination and
// discard a valid racing refill, causing later reads to miss unnecessarily.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.status = code
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *statusRecorder) Write(b []byte) (int, error) {
	if rec.status == 0 {
		rec.status = http.StatusOK
	}
	return rec.ResponseWriter.Write(b)
}

// Flush delegates to the underlying writer when it supports flushing, so wrapping
// never disables streaming for handlers that rely on http.Flusher.
func (rec *statusRecorder) Flush() {
	if f, ok := rec.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// wroteSuccess reports whether upstream returned a 2xx status.
func (rec *statusRecorder) wroteSuccess() bool {
	return rec.status >= 200 && rec.status < 300
}

// HandlePutObject handles PUT requests for objects.
// Invalidates cache BEFORE forwarding to ensure consistency.
func (s *Service) HandlePutObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	bucket, key := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandlePutObject")

	// Invalidate cache BEFORE forwarding to ensure consistency
	// This prevents stale data from being served if forwarding succeeds but cache invalidation fails
	if s.cache.IsEnabled() {
		s.cache.Delete(context.Background(), bucket, key)
		metrics.RecordCacheOperation("delete", "success")
	}

	// Forward to Tigris, recording the upstream status.
	rec := &statusRecorder{ResponseWriter: w}
	err := s.forwarder.Forward(r.Context(), rec, r)

	// Re-invalidate AFTER upstream confirms the write. A GET that raced the
	// in-flight PUT may have fetched the pre-PUT object and begun re-caching it;
	// this second invalidation writes a tombstone newer than that write's start
	// time, so the tombstone-aware cache write skips the stale repopulation —
	// restoring read-after-write semantics.
	// Gated on a 2xx: a rejected PUT leaves the object unchanged, so re-invalidating
	// would only discard a valid racing refill and cause an unnecessary later miss.
	// The metric is recorded once on the pre-forward invalidation above; this
	// second Delete is the same logical invalidation, so it is not counted again.
	if err == nil && rec.wroteSuccess() && s.cache.IsEnabled() {
		s.cache.Delete(context.Background(), bucket, key)
	}

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("PutObject", status, time.Since(start).Seconds())

	return err
}

// HandleDeleteObject handles DELETE requests for objects.
// Invalidates cache BEFORE forwarding to ensure consistency.
func (s *Service) HandleDeleteObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	bucket, key := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandleDeleteObject")

	// Invalidate cache BEFORE forwarding to ensure consistency
	// This prevents stale data from being served if forwarding succeeds but cache invalidation fails
	if s.cache.IsEnabled() {
		s.cache.Delete(context.Background(), bucket, key)
		metrics.RecordCacheOperation("delete", "success")
	}

	// Forward to upstream, recording the upstream status.
	rec := &statusRecorder{ResponseWriter: w}
	err := s.forwarder.Forward(r.Context(), rec, r)

	// Re-invalidate AFTER upstream confirms the delete, for the same
	// read-after-write reason as HandlePutObject: a GET racing the in-flight
	// DELETE may have re-cached the not-yet-deleted object; this second
	// tombstone blocks that stale repopulation.
	// Gated on a 2xx: a rejected DELETE leaves the object present, so re-invalidating
	// would only discard a valid racing refill and cause an unnecessary later miss.
	// The metric is recorded once on the pre-forward invalidation above; this
	// second Delete is the same logical invalidation, so it is not counted again.
	if err == nil && rec.wroteSuccess() && s.cache.IsEnabled() {
		s.cache.Delete(context.Background(), bucket, key)
	}

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("DeleteObject", status, time.Since(start).Seconds())

	return err
}

// HandleHeadObject handles HEAD requests for objects.
// Serves from cached metadata when available (no body fetch needed).
// Supports cache revalidation via Cache-Control: no-cache/max-age=0.
func (s *Service) HandleHeadObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	ctx := r.Context()
	bucket, key := ParseBucketKey(r)
	forceRevalidate := shouldForceRevalidate(r)
	bypassCache := shouldBypassCache(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandleHeadObject")

	// Validate credentials before serving from cache
	result, accessKey, secretKey, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil {
		metrics.RecordRequest("HeadObject", "auth_error", time.Since(start).Seconds())
		return err
	}

	// Try to serve from cached metadata (no body needed)
	// Anonymous requests can also read from cache if the object's ACL is public-read.
	isAnonymous := isAnonymousRequest(r, result, err)

	if (result == AuthValidated || isAnonymous) && !bypassCache && s.cache.IsEnabled() {
		meta, found, cacheErr := s.cache.GetMeta(ctx, bucket, key)
		cacheHit := cacheErr == nil && found && meta != nil
		if cacheHit && isAnonymous && !meta.IsPublicRead() {
			cacheHit = false
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Skipping HEAD cache for anonymous request - object not public")
		}
		if cacheHit {
			// Client-triggered revalidation: Cache-Control: no-cache/max-age=0
			if forceRevalidate && meta.ETag != "" {
				log.Debug().Str("bucket", bucket).Str("key", key).Msg("HEAD cache hit requires revalidation (client-triggered)")
				return s.revalidateAndServeHead(ctx, w, bucket, key, accessKey, secretKey, meta, start)
			}

			if !forceRevalidate {
				metrics.RecordCacheHit()
				log.Debug().Str("bucket", bucket).Str("key", key).Msg("HEAD served from cache")
				meta.WriteHeaders(w)
				w.Header().Set(XCacheHeader, XCacheHit)
				w.WriteHeader(meta.StatusCode)
				metrics.RecordRequest("HeadObject", "success", time.Since(start).Seconds())
				return nil
			}
			// forceRevalidate but no ETag — fall through to upstream
		}
		metrics.RecordCacheMiss()
	}

	// Cache miss - forward to upstream
	// Set X-Cache header before forwarding (will be included in response)
	if !s.cache.IsEnabled() {
		w.Header().Set(XCacheHeader, XCacheDisabled)
	} else if bypassCache {
		w.Header().Set(XCacheHeader, XCacheBypass)
	} else {
		w.Header().Set(XCacheHeader, XCacheMiss)
	}
	err = s.forwarder.Forward(ctx, w, r)

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("HeadObject", status, time.Since(start).Seconds())
	return err
}

// HandleCopyObject handles copy object requests.
// Invalidates cache BEFORE forwarding to ensure consistency.
func (s *Service) HandleCopyObject(w http.ResponseWriter, r *http.Request) error {
	bucket, key := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandleCopyObject")

	// Invalidate cache for destination object BEFORE forwarding to ensure consistency
	// This prevents stale data from being served if forwarding succeeds but cache invalidation fails
	if s.cache.IsEnabled() {
		s.cache.Delete(context.Background(), bucket, key)
	}

	// Forward to upstream, capturing the response so we can confirm the copy
	// actually succeeded. CopyObject signals failure either with a non-2xx status
	// or — famously — a 200 OK carrying an <Error> body, and Forward would report
	// neither.
	capture, err := s.forwarder.ForwardWithCapture(r.Context(), w, r)

	// Re-invalidate the destination AFTER upstream confirms the copy, for the same
	// read-after-write reason as HandlePutObject: a GET racing the in-flight copy
	// may have re-cached the pre-copy destination object; this second tombstone
	// blocks that stale repopulation.
	// Gated on a confirmed-successful copy: a rejected copy leaves the destination
	// unchanged, so re-invalidating would only discard a valid racing refill.
	if err == nil && copyObjectSucceeded(capture) && s.cache.IsEnabled() {
		s.cache.Delete(context.Background(), bucket, key)
	}

	return err
}

// copyObjectSucceeded reports whether a captured CopyObject response indicates the
// copy actually happened: a 2xx status whose body is not an S3 <Error> document
// (S3 can return 200 OK with an error body for copies that fail mid-operation).
func copyObjectSucceeded(capture *ResponseCapture) bool {
	if capture == nil || capture.StatusCode < 200 || capture.StatusCode >= 300 {
		return false
	}
	return !isS3ErrorBody(capture.Body)
}

// HandlePassthrough handles requests that are passed through without caching.
func (s *Service) HandlePassthrough(w http.ResponseWriter, r *http.Request) error {
	return s.forwarder.Forward(r.Context(), w, r)
}

// HandleCompleteMultipartUpload handles CompleteMultipartUpload with idempotency caching.
// This caches successful completion responses in ocache to support idempotent calls,
// matching tigris-os behavior where a second CompleteMultipartUpload call returns success.
func (s *Service) HandleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request) error {
	bucket, key := ParseBucketKey(r)
	uploadId := r.URL.Query().Get("uploadId")
	ctx := r.Context()

	log.Debug().Str("bucket", bucket).Str("key", key).Str("uploadId", uploadId).Msg("HandleCompleteMultipartUpload")

	// Check ocache first for idempotent completion (works across TAG pods)
	if s.cache.IsEnabled() {
		entry, found, err := s.cache.GetCompletion(ctx, bucket, key, uploadId)
		if err == nil && found {
			log.Debug().Str("uploadId", uploadId).Msg("CompleteMultipartUpload cache hit - returning cached response")
			for k, v := range entry.Headers {
				w.Header().Set(k, v)
			}
			w.WriteHeader(entry.StatusCode)
			_, _ = w.Write(entry.Body)
			return nil
		}
	}

	// Forward to upstream with response capture
	capture, err := s.forwarder.ForwardWithCapture(ctx, w, r)
	if err != nil {
		return err
	}

	// Cache successful completions (2xx status codes) in ocache
	// Only cache if the body was fully captured to avoid storing corrupted responses
	if capture.StatusCode >= 200 && capture.StatusCode < 300 && capture.Complete {
		if cacheErr := s.cache.PutCompletion(ctx, bucket, key, uploadId, capture.StatusCode, capture.Headers, capture.Body); cacheErr != nil {
			log.Debug().Err(cacheErr).Msg("Failed to cache completion response")
			// Don't fail the request if caching fails
		}
	}

	return nil
}

// ParseBucketKey extracts bucket and key from request path.
func ParseBucketKey(r *http.Request) (bucket, key string) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) >= 1 {
		bucket = parts[0]
	}
	if len(parts) >= 2 {
		key = parts[1]
	}
	return
}

// hasNoAuthCredentials returns true if the request carries no SigV4 credentials
// (no Authorization header and no X-Amz-Credential query parameter).
func hasNoAuthCredentials(r *http.Request) bool {
	return r.Header.Get("Authorization") == "" &&
		r.URL.Query().Get("X-Amz-Credential") == ""
}

// isAnonymousRequest returns true if the request has no SigV4 authentication
// and the auth result indicates it was not validated (AuthNotValidated with no error).
func isAnonymousRequest(r *http.Request, result AuthResult, authErr error) bool {
	return authErr == nil && result == AuthNotValidated && hasNoAuthCredentials(r)
}

// extractTotalSizeFromContentRange extracts total size from Content-Range header.
// Header format: "bytes start-end/total" (e.g., "bytes 0-499/1234")
// Returns 0 if header is missing or malformed.
func extractTotalSizeFromContentRange(contentRange string) int64 {
	if contentRange == "" {
		return 0
	}

	// Find the slash separator
	slashIdx := strings.LastIndex(contentRange, "/")
	if slashIdx == -1 {
		return 0
	}

	totalStr := contentRange[slashIdx+1:]
	if totalStr == "*" {
		// Unknown total size
		return 0
	}

	total, err := strconv.ParseInt(totalStr, 10, 64)
	if err != nil {
		return 0
	}
	return total
}

// GetRegion returns the configured upstream region.
func (s *Service) GetRegion() string {
	return s.config.Upstream.Region
}
