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

// writeCacheStatus sets the X-Cache response header and records the matching
// hit/miss counter, keeping the header and tag_cache_{hits,misses}_total in
// lockstep. Call it once per response, where the status is committed — never pair
// it with a separate RecordCacheHit/RecordCacheMiss for the same response.
//
// HIT increments hits; MISS increments misses. REVALIDATED, BYPASS, and DISABLED
// set the header but are neither hits nor misses: REVALIDATED (object changed on
// upstream) is tracked by the tag_revalidations_* metrics, and a bypassed/disabled
// request made no cache decision. Range-from-cache hits additionally call
// RecordRangeFromCacheHit for the specialized counter.
func writeCacheStatus(w http.ResponseWriter, status string) {
	w.Header().Set(XCacheHeader, status)
	switch status {
	case XCacheHit:
		metrics.RecordCacheHit()
	case XCacheMiss:
		metrics.RecordCacheMiss()
	}
}

// cacheMissStatus returns the X-Cache status for a request not served from cache:
// DISABLED when caching is off, BYPASS when the client opted out (Cache-Control:
// no-store), otherwise MISS. Only MISS counts as a cache miss.
func (s *Service) cacheMissStatus(bypassCache bool) string {
	switch {
	case !s.cache.IsEnabled():
		return XCacheDisabled
	case bypassCache:
		return XCacheBypass
	default:
		return XCacheMiss
	}
}

// Service provides the core caching proxy logic.
type Service struct {
	forwarder               RequestForwarder
	cache                   *cache.Cache
	config                  *config.Config
	cacheSemaphore          chan struct{}      // Count ceiling on concurrent cache-populate ops (nil = unlimited)
	populateBudget          *byteBudget        // Byte budget bounding aggregate populate buffering (nil = unlimited)
	perPopulateCap          int64              // Max bytes a single populate can buffer (reservation ceiling)
	broadcastManager        *broadcast.Manager // For streaming request coalescing
	activeBackgroundFetches sync.Map           // Dedup for background full-object fetches (range caching)
}

// NewService creates a new proxy service.
func NewService(forwarder RequestForwarder, cache *cache.Cache, cfg *config.Config) *Service {
	channelBuf := cfg.Broadcast.ChannelBuffer
	if channelBuf <= 0 {
		channelBuf = broadcast.DefaultChannelBuffer
	}

	// Concurrent cache-populate operations are bounded two independent ways, and a
	// populate must pass both:
	//   1. A hard COUNT ceiling (MaxConcurrentWrites) — caps total in-flight populates.
	//   2. A BYTE budget (MaxPopulateMemoryBytes) — caps aggregate buffered memory.
	// The byte budget is what prevents the OOM failure mode: each populate reserves
	// min(object size, per-populate buffer ceiling), so many small objects populate
	// concurrently while a burst of large objects is throttled to keep buffered
	// memory bounded (a byte-unaware count alone can pin many GB — e.g. 256 large
	// populates × ~64–80MB ≈ 16–20GB).
	var cacheSem chan struct{}
	if cfg.Cache.MaxConcurrentWrites > 0 {
		cacheSem = make(chan struct{}, cfg.Cache.MaxConcurrentWrites)
	}

	perPopulateCap := perPopulateBufferBytes(cfg)

	var populateBudget *byteBudget
	if cfg.Cache.MaxPopulateMemoryBytes > 0 {
		populateBudget = newByteBudget(cfg.Cache.MaxPopulateMemoryBytes)
	}

	log.Info().
		Int("max_concurrent_writes", cfg.Cache.MaxConcurrentWrites).
		Int64("max_populate_memory_bytes", cfg.Cache.MaxPopulateMemoryBytes).
		Int64("per_populate_buffer_cap_bytes", perPopulateCap).
		Msg("Cache-populate limits configured")

	return &Service{
		forwarder:        forwarder,
		cache:            cache,
		config:           cfg,
		cacheSemaphore:   cacheSem,
		populateBudget:   populateBudget,
		perPopulateCap:   perPopulateCap,
		broadcastManager: broadcast.NewManager(channelBuf),
	}
}

// perPopulateBufferBytes returns the maximum bytes a single cache-populate can
// buffer: the broadcast listener channel (channel_buffer chunks) plus the
// cache-write queue in setupCacheListener (channel_buffer/4 chunks, floored at 64).
// Each queued chunk retains at least a pooled DefaultChunkSize backing array
// (broadcast.GetChunkBuf pools DefaultChunkSize buffers), so a smaller configured
// chunk_size still holds that much per chunk — charge the larger of the two. This
// is the ceiling a populate ever reserves; smaller objects reserve their actual size.
func perPopulateBufferBytes(cfg *config.Config) int64 {
	chunkSize := int64(cfg.Broadcast.ChunkSize)
	if chunkSize < broadcast.DefaultChunkSize {
		chunkSize = broadcast.DefaultChunkSize
	}
	channelBuf := int64(cfg.Broadcast.ChannelBuffer)
	if channelBuf <= 0 {
		channelBuf = broadcast.DefaultChannelBuffer
	}
	queue := channelBuf / 4
	if queue < 64 {
		queue = 64 // matches setupCacheListener's cacheQueueSize floor
	}
	return (channelBuf + queue) * chunkSize
}

// populateWeight is the byte weight a populate reserves against the memory budget:
// the object's size, capped at the per-populate buffer ceiling (a populate never
// buffers more than the pipeline can hold). Unknown/negative sizes (e.g. chunked
// responses) reserve the full ceiling. It never exceeds the total budget, so an
// object larger than the whole budget can still populate one-at-a-time. Always ≥ 1.
func (s *Service) populateWeight(contentLength int64) int64 {
	w := s.perPopulateCap
	if contentLength > 0 && contentLength < w {
		w = contentLength
	}
	if w < 1 {
		w = 1
	}
	if budget := s.config.Cache.MaxPopulateMemoryBytes; budget > 0 && w > budget {
		w = budget
	}
	return w
}

// byteBudget is a non-blocking weighted semaphore bounding the aggregate bytes
// reserved by concurrent cache-populate operations.
type byteBudget struct {
	mu        sync.Mutex
	remaining int64
}

func newByteBudget(total int64) *byteBudget {
	return &byteBudget{remaining: total}
}

func (b *byteBudget) tryAcquire(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.remaining < n {
		return false
	}
	b.remaining -= n
	return true
}

func (b *byteBudget) release(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.remaining += n
}

// acquireCacheSlot tries to reserve a cache-populate slot without blocking,
// reserving `weight` bytes against the memory budget. It returns true (and must be
// paired with releaseCacheSlot passing the SAME weight) when both the count slot
// and the byte budget are reserved, or when a limiter is disabled (nil). It returns
// false when either the count limit or the memory budget is saturated, in which
// case the caller should skip caching rather than block or spawn unbounded work.
func (s *Service) acquireCacheSlot(weight int64) bool {
	if s.cacheSemaphore != nil {
		select {
		case s.cacheSemaphore <- struct{}{}:
		default:
			return false
		}
	}
	if s.populateBudget != nil && !s.populateBudget.tryAcquire(weight) {
		if s.cacheSemaphore != nil {
			<-s.cacheSemaphore // hand back the count slot we just took
		}
		return false
	}
	return true
}

// releaseCacheSlot releases a slot reserved by acquireCacheSlot. `weight` must
// match the value passed to the paired acquireCacheSlot.
func (s *Service) releaseCacheSlot(weight int64) {
	if s.populateBudget != nil {
		s.populateBudget.release(weight)
	}
	if s.cacheSemaphore != nil {
		<-s.cacheSemaphore
	}
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
	s.invalidateObject(context.Background(), bucket, key)

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
	// Routed through invalidateObject (like the pre-forward call) so a failure of this
	// read-after-write-critical invalidation is recorded and logged, not discarded.
	if err == nil && rec.wroteSuccess() && s.cache.IsEnabled() {
		s.invalidateObject(context.Background(), bucket, key)
		s.warmOnWrite(r, bucket, key)
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
	s.invalidateObject(context.Background(), bucket, key)

	// Forward to upstream, recording the upstream status.
	rec := &statusRecorder{ResponseWriter: w}
	err := s.forwarder.Forward(r.Context(), rec, r)

	// Re-invalidate AFTER upstream confirms the delete, for the same
	// read-after-write reason as HandlePutObject: a GET racing the in-flight
	// DELETE may have re-cached the not-yet-deleted object; this second
	// tombstone blocks that stale repopulation.
	// Gated on a 2xx: a rejected DELETE leaves the object present, so re-invalidating
	// would only discard a valid racing refill and cause an unnecessary later miss.
	// Routed through invalidateObject (like the pre-forward call) so a failure of this
	// read-after-write-critical invalidation is recorded and logged, not discarded.
	if err == nil && rec.wroteSuccess() && s.cache.IsEnabled() {
		s.invalidateObject(context.Background(), bucket, key)
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
				log.Debug().Str("bucket", bucket).Str("key", key).Msg("HEAD served from cache")
				meta.WriteHeaders(w)
				writeCacheStatus(w, XCacheHit)
				w.WriteHeader(meta.StatusCode)
				metrics.RecordRequest("HeadObject", "success", time.Since(start).Seconds())
				return nil
			}
			// forceRevalidate but no ETag — fall through to upstream
		}
	}

	// Cache miss - forward to upstream. Set the X-Cache header (and the matching
	// hit/miss counter) before forwarding; a disabled/bypassed request is neither.
	if !s.cache.IsEnabled() {
		writeCacheStatus(w, XCacheDisabled)
	} else if bypassCache {
		writeCacheStatus(w, XCacheBypass)
	} else {
		writeCacheStatus(w, XCacheMiss)
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
		s.invalidateObject(context.Background(), bucket, key)
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
	if err == nil && s3WriteSucceeded(capture) && s.cache.IsEnabled() {
		s.invalidateObject(context.Background(), bucket, key)
		s.warmOnWrite(r, bucket, key)
	}

	return err
}

// s3WriteSucceeded reports whether a captured mutation response indicates the write
// actually happened: a 2xx status whose body is not an S3 <Error> document. Both
// CopyObject and CompleteMultipartUpload can return 200 OK with an error body for an
// operation that failed mid-flight, so status alone is not enough.
//
// It deliberately does NOT gate on capture.Complete. The two ways to be wrong are
// not symmetric: treating a failed write as successful only re-invalidates an object
// that didn't change (an extra cache miss), while treating a successful write as
// failed skips the invalidation and can leave a racing refill of the OLD object
// cached — serving stale data. So an unconfirmable outcome must be assumed
// successful. An incomplete capture still detects a failure whenever the <Error>
// root element was captured (isS3ErrorBody only reads the first element); only a body
// truncated before that root reads as success, which is the safe side.
func s3WriteSucceeded(capture *ResponseCapture) bool {
	if capture == nil || capture.StatusCode < 200 || capture.StatusCode >= 300 {
		return false
	}
	return !isS3ErrorBody(capture.Body)
}

// invalidateObject removes an object's cached metadata (writing a tombstone) and
// records the true outcome of the attempt. A failed backend invalidation is recorded
// as an error rather than success: a false-green delete metric would hide the very
// read-after-write hazard the invalidation exists to prevent, since the stale entry
// is still in place. It is a no-op when the cache is disabled.
func (s *Service) invalidateObject(ctx context.Context, bucket, key string) {
	if !s.cache.IsEnabled() {
		return
	}
	if err := s.cache.Delete(ctx, bucket, key); err != nil {
		metrics.RecordCacheOperation("delete", "error")
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache invalidation failed")
		return
	}
	metrics.RecordCacheOperation("delete", "success")
}

// warmOnWrite repopulates the cache after a successful write by triggering a
// background full-object fetch, so a read soon after the write hits cache
// (cache-warm-on-write; see cache.warm_on_write). It is best-effort and fully
// detached: triggerBackgroundCacheFetch deduplicates concurrent warms, sheds under
// the populate byte budget, and stamps its own writeStartTime before the GET so an
// invalidation racing the warm is provably newer and blocks it. Safe to call on
// every successful write.
//
// Credentials and public-read handling depend on how the write was authenticated,
// because a write proves nothing about who may READ the object:
//
//   - Authenticated write (anonymous=false): warm with the write's credentials
//     (TAG's own in transparent mode, the client's in signing mode) via a signed
//     fetch, which is never marked public-read. The cached entry serves authenticated
//     reads; a signing-mode write-only client's warm will fail, exactly as its own
//     read would.
//   - Anonymous write (anonymous=true): warm with an UNSIGNED fetch. A successful
//     anonymous write only proves public-WRITE, so we must not infer public-read from
//     it. Instead the anonymous fetch itself is the probe: upstream returns 200 only if
//     the object is genuinely publicly READABLE (then it is cached public-read, so
//     anonymous reads hit) and 403 otherwise (nothing is cached, so a private object in
//     a public-write bucket is never exposed). This mirrors the read path, which
//     likewise caches public-read only after a successful anonymous read.
//
// Best-effort caveat: warms are keyed by bucket/key for dedup, so if any fetch for
// this key is already in flight — a concurrent read-path warm, or the warm from a
// rapid prior write to the same key — this warm coalesces into that one and is
// dropped. When it coalesces into a fetch that predates this write, that fetch's own
// populate is tombstone-blocked (its writeStartTime is older than this write's
// invalidation), so it writes nothing either: the key is simply left absent, not
// left stale. The next read then misses and inline-populates the current object.
// This can never serve a stale object — the same tombstone that blocks the racing
// populate is the read-after-write guard.
func (s *Service) warmOnWrite(r *http.Request, bucket, key string) {
	if !s.config.Cache.WarmOnWrite || !s.cache.IsEnabled() {
		return
	}

	// Anonymous write → anonymous warm (unsigned fetch, public-read learned from the
	// probe). See the doc comment: never infer public-read from a public write.
	if hasNoAuthCredentials(r) {
		metrics.WarmOnWriteTriggered.Inc()
		s.triggerBackgroundCacheFetch(bucket, key, "", "", true /*anonymous*/)
		return
	}

	_, accessKey, secretKey, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil || accessKey == "" || secretKey == "" {
		return
	}
	metrics.WarmOnWriteTriggered.Inc()
	s.triggerBackgroundCacheFetch(bucket, key, accessKey, secretKey, false /*anonymous*/)
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

	// Check ocache first for idempotent completion (works across TAG pods).
	// A replay returns the already-completed response without touching upstream and
	// without changing the object, so it needs no read-cache invalidation — the first
	// completion (below) already invalidated it.
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

	// Invalidate the object's read cache BEFORE forwarding. Completing a multipart
	// upload overwrites the object, so any previously cached version is now stale;
	// like PutObject/DeleteObject/CopyObject, invalidate up front so a forward that
	// succeeds but whose post-invalidation fails can't leave stale data served.
	s.invalidateObject(context.Background(), bucket, key)

	// Forward to upstream with response capture
	capture, err := s.forwarder.ForwardWithCapture(ctx, w, r)
	if err != nil {
		return err
	}

	// Re-invalidate AFTER upstream confirms the completion, for the same
	// read-after-write reason as HandlePutObject: a GET racing the in-flight
	// completion may have re-cached the pre-overwrite object; this second tombstone
	// blocks that stale repopulation. Gated on a confirmed-successful completion
	// (2xx and not a 200-with-<Error> body) so a failed completion, which leaves the
	// object unchanged, doesn't discard a valid racing refill.
	completed := s3WriteSucceeded(capture)
	if completed && s.cache.IsEnabled() {
		s.invalidateObject(context.Background(), bucket, key)
		// Warm-on-write is the only way to make a multipart-completed object hot:
		// TAG never sees its assembled body, so a write-through tee is impossible.
		s.warmOnWrite(r, bucket, key)
	}

	// Cache successful completions in ocache for idempotent replays. Only cache a
	// genuine success (not a 200-with-<Error> body) that was fully captured, so we
	// never replay a corrupted or error response as a successful completion.
	if completed && capture.Complete {
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
