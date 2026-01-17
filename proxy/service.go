package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

const (
	// maxConcurrentCacheWrites limits the number of concurrent background cache operations.
	maxConcurrentCacheWrites = 100

	// X-Cache header constants for indicating cache status.
	XCacheHeader   = "X-Cache"
	XCacheHit      = "HIT"
	XCacheMiss     = "MISS"
	XCacheBypass   = "BYPASS"   // Range requests bypass cache
	XCacheDisabled = "DISABLED" // Cache is disabled
)

// Service provides the core caching proxy logic.
type Service struct {
	forwarder              *Forwarder
	cache                  *cache.Cache
	config                 *config.Config
	cacheSemaphore         chan struct{}      // Limits concurrent cache writes
	broadcastManager       *broadcast.Manager // For streaming request coalescing
	backgroundFetchManager *broadcast.Manager // For background full-object fetches (range caching)
}

// NewService creates a new proxy service.
func NewService(forwarder *Forwarder, cache *cache.Cache, cfg *config.Config) *Service {
	channelBuf := cfg.Broadcast.ChannelBuffer
	if channelBuf <= 0 {
		channelBuf = broadcast.DefaultChannelBuffer
	}

	return &Service{
		forwarder:              forwarder,
		cache:                  cache,
		config:                 cfg,
		cacheSemaphore:         make(chan struct{}, maxConcurrentCacheWrites),
		broadcastManager:       broadcast.NewManager(channelBuf),
		backgroundFetchManager: broadcast.NewManager(channelBuf),
	}
}

// HandleGetObject handles GET requests for objects with cache-first logic.
// Uses streaming broadcast for request coalescing to reduce upstream load.
// Supports conditional requests (If-None-Match, If-Modified-Since).
func (s *Service) HandleGetObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	ctx := r.Context()
	bucket, key := ParseBucketKey(r)
	noCacheRead := shouldSkipCache(r)
	rangeHeader := r.Header.Get("Range")

	// Conditional request headers
	ifNoneMatch := r.Header.Get("If-None-Match")
	ifModifiedSince := r.Header.Get("If-Modified-Since")

	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Bool("no_cache", noCacheRead).
		Str("if_none_match", ifNoneMatch).
		Msg("HandleGetObject")

	// 1. Validate credentials FIRST (before any broadcast operations)
	accessKey, secretKey, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil {
		metrics.RecordRequest("GetObject", "auth_error", time.Since(start).Seconds())
		return err
	}

	// 2. Check cache (fast path) - now also works for range requests!
	// For range requests: check if full object is in cache, then serve range from it.
	if !noCacheRead && s.cache.IsEnabled() {
		meta, bodyReader, found, cacheErr := s.cache.GetWithMeta(ctx, bucket, key)
		if cacheErr == nil && found && meta != nil {
			// Close body reader if we got one (we may serve range instead)
			if bodyReader != nil {
				if closer, ok := bodyReader.(io.Closer); ok {
					defer closer.Close()
				}
			}

			// If this is a Range request and we have the full object cached,
			// serve the range from the cached object
			if rangeHeader != "" {
				log.Debug().Str("bucket", bucket).Str("key", key).Msg("Serving range from cached full object")
				return s.serveRangeFromCache(ctx, w, bucket, key, meta, rangeHeader, start)
			}

			// Check conditional request: If-None-Match
			if ifNoneMatch != "" && meta.MatchesETag(ifNoneMatch) {
				metrics.RecordCacheHit()
				log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache hit - 304 Not Modified")
				w.Header().Set(XCacheHeader, XCacheHit)
				w.WriteHeader(http.StatusNotModified)
				metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
				return nil
			}

			// Check conditional request: If-Modified-Since
			if ifModifiedSince != "" {
				if t, parseErr := http.ParseTime(ifModifiedSince); parseErr == nil {
					if !meta.IsModifiedSince(t) {
						metrics.RecordCacheHit()
						log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache hit - 304 Not Modified (time)")
						w.Header().Set(XCacheHeader, XCacheHit)
						w.WriteHeader(http.StatusNotModified)
						metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
						return nil
					}
				}
			}

			// Serve full response from cache with proper headers
			metrics.RecordCacheHit()
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Serving from cache with metadata")
			meta.WriteHeaders(w)
			w.Header().Set(XCacheHeader, XCacheHit)
			w.WriteHeader(meta.StatusCode)
			if bodyReader != nil {
				if _, copyErr := io.Copy(w, bodyReader); copyErr != nil {
					log.Warn().Err(copyErr).Msg("Failed to copy cached content to response")
				}
			}
			metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
			return nil
		}
		metrics.RecordCacheMiss()
	}

	// 3. Cache miss - handle differently for range requests vs full object requests
	// Range requests: forward immediately + trigger background cache fetch
	if rangeHeader != "" {
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Range request cache miss - forwarding with background cache")
		return s.handleRangeWithBackgroundCache(ctx, w, r, bucket, key, accessKey, secretKey, start)
	}

	// Full object request: use broadcast manager for streaming coalescing
	bcastKey := makeBroadcastKey(bucket, key, rangeHeader)
	broadcaster, isFirstCaller := s.broadcastManager.GetOrCreate(bcastKey)

	// Determine X-Cache header value for this request
	var xCache string
	if !s.cache.IsEnabled() {
		xCache = XCacheDisabled
	} else {
		xCache = XCacheMiss
	}

	// Update active broadcasts metric
	metrics.SetActiveBroadcasts(s.broadcastManager.ActiveCount())

	if isFirstCaller {
		// I'm the fetcher - stream from upstream and broadcast to all listeners
		metrics.RecordBroadcastFetch()
		defer func() {
			s.broadcastManager.Remove(bcastKey)
			metrics.SetActiveBroadcasts(s.broadcastManager.ActiveCount())
		}()
		return s.fetchAndBroadcast(ctx, w, r, bucket, key, accessKey, secretKey, broadcaster, start, xCache)
	}

	// I'm a listener - try to subscribe
	listener := broadcaster.Subscribe()
	if listener == nil {
		// Streaming already started (no late joiners) - start own fetch
		metrics.RecordBroadcastFetch()
		newBroadcaster, _ := s.broadcastManager.GetOrCreate(bcastKey + ":late")
		defer func() {
			s.broadcastManager.Remove(bcastKey + ":late")
			metrics.SetActiveBroadcasts(s.broadcastManager.ActiveCount())
		}()
		return s.fetchAndBroadcast(ctx, w, r, bucket, key, accessKey, secretKey, newBroadcaster, start, xCache)
	}

	// Successfully subscribed - receive streamed chunks
	metrics.RecordBroadcastShared()
	return s.receiveFromBroadcastListener(ctx, w, listener, start, xCache)
}

// makeBroadcastKey creates a unique key for broadcast coalescing.
func makeBroadcastKey(bucket, key, rangeHeader string) string {
	if rangeHeader == "" {
		return bucket + "/" + key
	}
	return bucket + "/" + key + ":" + rangeHeader
}

// fetchAndBroadcast fetches from upstream and broadcasts to all listeners.
// This is the "fetcher" path - only one goroutine executes this per broadcast.
// xCache specifies the X-Cache header value (MISS, BYPASS, DISABLED).
func (s *Service) fetchAndBroadcast(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key, accessKey, secretKey string,
	broadcaster *broadcast.Broadcaster,
	start time.Time,
	xCache string,
) error {
	// Subscribe ourselves as the first listener
	listener := broadcaster.Subscribe()

	// Start the upstream fetch in a goroutine
	go func() {
		err := s.streamFromUpstream(ctx, r, bucket, key, accessKey, secretKey, broadcaster)
		broadcaster.Complete(err)
	}()

	// Receive chunks like any other listener
	err := s.writeChunksToResponse(ctx, w, listener, xCache)

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("GetObject", status, time.Since(start).Seconds())
	return err
}

// streamFromUpstream reads from upstream and broadcasts chunks.
// Cache is added as a listener and receives chunks via io.Pipe (no buffering).
func (s *Service) streamFromUpstream(
	ctx context.Context,
	r *http.Request,
	bucket, key, accessKey, secretKey string,
	broadcaster *broadcast.Broadcaster,
) error {
	// Execute upstream request
	resp, err := s.forwarder.DoRequestWithCreds(ctx, r, accessKey, secretKey)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Determine if we should cache this response
	shouldCache := resp.StatusCode == http.StatusOK &&
		s.cache.IsEnabled() &&
		!s.hasNoCacheHeaders(resp.Header) &&
		s.isWithinSizeThreshold(resp)

	// Set up cache listener if caching (streams directly to cache via pipe)
	var cachePipeWriter *io.PipeWriter
	var cacheErrCh chan error

	if shouldCache {
		cachePipeWriter, cacheErrCh = s.setupCacheListener(ctx, bucket, key, broadcaster)
	}

	// Set headers for all listeners
	broadcaster.SetHeaders(resp.StatusCode, resp.Header)

	// Stream body in chunks, broadcasting to all listeners (including cache)
	chunkSize := s.config.Broadcast.ChunkSize
	if chunkSize <= 0 {
		chunkSize = broadcast.DefaultChunkSize
	}
	buf := make([]byte, chunkSize)

	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			broadcaster.Broadcast(buf[:n])
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			// Error during streaming - the broadcast will signal error to all listeners including cache
			return readErr
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			// Context canceled - the broadcast will signal error to all listeners
			return ctx.Err()
		default:
		}
	}

	// Wait briefly for cache write to complete (the goroutine in setupCacheListener handles closing the pipe)
	// We don't close cachePipeWriter here - the cache listener goroutine closes it when it finishes.
	if cacheErrCh != nil {
		// Wait up to 100ms for cache write to complete, otherwise continue
		select {
		case cacheWriteErr := <-cacheErrCh:
			if cacheWriteErr != nil {
				log.Warn().Err(cacheWriteErr).Str("bucket", bucket).Str("key", key).Msg("Cache write failed")
			}
		case <-time.After(100 * time.Millisecond):
			// Don't block too long waiting for cache - it will complete in background
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache write still in progress, continuing")
		}
	}
	// Ignore unused cachePipeWriter - it's managed by the cache listener goroutine
	_ = cachePipeWriter

	return nil
}

// receiveFromBroadcastListener receives chunks from an existing broadcast.
// This is the "listener" path - waits for fetcher to stream data.
// xCache specifies the X-Cache header value (MISS, BYPASS, DISABLED).
func (s *Service) receiveFromBroadcastListener(
	ctx context.Context,
	w http.ResponseWriter,
	listener *broadcast.Listener,
	start time.Time,
	xCache string,
) error {
	err := s.writeChunksToResponse(ctx, w, listener, xCache)

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("GetObject", status, time.Since(start).Seconds())
	return err
}

// writeChunksToResponse writes received chunks to the HTTP response.
// xCache specifies the X-Cache header value (MISS, BYPASS, DISABLED).
func (s *Service) writeChunksToResponse(
	ctx context.Context,
	w http.ResponseWriter,
	listener *broadcast.Listener,
	xCache string,
) error {
	// Wait for headers first
	status, headers, err := listener.WaitForHeaders(ctx)
	if err != nil {
		return err
	}

	// Write headers to response
	copyHeaders(w.Header(), headers)
	w.Header().Set(XCacheHeader, xCache)
	w.WriteHeader(status)

	// Receive and write chunks
	for chunk := range listener.Chunks() {
		if chunk.Err != nil {
			if chunk.Err == broadcast.ErrSlowConsumer {
				metrics.RecordBroadcastSlowConsumer()
			}
			return chunk.Err
		}

		if len(chunk.Data) > 0 {
			if _, writeErr := w.Write(chunk.Data); writeErr != nil {
				return writeErr
			}
			// Flush if ResponseWriter supports it
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	}

	return nil
}

// setupCacheListener creates a listener that streams chunks directly to cache via io.Pipe.
// This avoids buffering the entire response in memory.
// Stores both metadata (from headers) and body in separate cache entries.
func (s *Service) setupCacheListener(
	ctx context.Context,
	bucket, key string,
	broadcaster *broadcast.Broadcaster,
) (*io.PipeWriter, chan error) {
	listener := broadcaster.Subscribe()
	if listener == nil {
		return nil, nil
	}

	// Create pipe for streaming to cache
	pipeReader, pipeWriter := io.Pipe()
	errCh := make(chan error, 1)

	// Start goroutine to consume chunks, build metadata, and write to cache
	go func() {
		defer close(errCh)

		// Wait for headers to build metadata
		statusCode, headers, err := listener.WaitForHeaders(ctx)
		if err != nil {
			pipeWriter.CloseWithError(err)
			errCh <- err
			return
		}

		// Build metadata from response headers
		meta := cache.MetaFromHTTPHeaders(bucket, key, statusCode, headers)

		// Check if still cacheable based on metadata
		if !meta.IsCacheable(s.config.Cache.SizeThreshold) {
			pipeWriter.CloseWithError(nil)
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Skipping cache - not cacheable")
			return
		}

		// Start a goroutine to stream chunks to the pipe
		go func() {
			for chunk := range listener.Chunks() {
				if chunk.Err != nil {
					pipeWriter.CloseWithError(chunk.Err)
					return
				}
				if len(chunk.Data) > 0 {
					if _, writeErr := pipeWriter.Write(chunk.Data); writeErr != nil {
						pipeWriter.CloseWithError(writeErr)
						return
					}
				}
			}
			pipeWriter.Close()
		}()

		// Use a detached context for cache writes to avoid cancellation when HTTP request completes.
		// The cache write should continue even after the client has received the response.
		cacheCtx, cacheCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cacheCancel()

		// Write to cache with metadata (streaming)
		ttl := int(s.config.Cache.TTL.Seconds())
		cacheErr := s.cache.PutWithMetaStream(cacheCtx, bucket, key, meta, pipeReader, ttl)
		if cacheErr != nil {
			log.Debug().Err(cacheErr).Str("bucket", bucket).Str("key", key).Msg("Cache write with metadata failed")
		}
		errCh <- cacheErr
	}()

	return pipeWriter, errCh
}

// hasNoCacheHeaders checks if response has no-cache directives.
func (s *Service) hasNoCacheHeaders(headers http.Header) bool {
	cc := headers.Get("Cache-Control")
	return strings.Contains(cc, "no-store") || strings.Contains(cc, "private")
}

// isWithinSizeThreshold checks if response is within cache size threshold.
// Uses Content-Length header if available.
func (s *Service) isWithinSizeThreshold(resp *http.Response) bool {
	if resp.ContentLength > 0 {
		return resp.ContentLength <= s.config.Cache.SizeThreshold
	}
	// Unknown size - allow caching (will be handled by cache layer)
	return true
}

// HandlePutObject handles PUT requests for objects.
func (s *Service) HandlePutObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	bucket, key := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandlePutObject")

	// Forward to Tigris
	err := s.forwarder.Forward(r.Context(), w, r)

	// Invalidate cache on success
	if err == nil && s.cache.IsEnabled() {
		s.cache.Delete(context.Background(), bucket, key)
		metrics.RecordCacheOperation("delete", "success")
	}

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("PutObject", status, time.Since(start).Seconds())

	return err
}

// HandleDeleteObject handles DELETE requests for objects.
func (s *Service) HandleDeleteObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	bucket, key := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandleDeleteObject")

	err := s.forwarder.Forward(r.Context(), w, r)

	// Invalidate cache on success
	if err == nil && s.cache.IsEnabled() {
		s.cache.Delete(context.Background(), bucket, key)
		metrics.RecordCacheOperation("delete", "success")
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
func (s *Service) HandleHeadObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	ctx := r.Context()
	bucket, key := ParseBucketKey(r)
	noCacheRead := shouldSkipCache(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandleHeadObject")

	// Try to serve from cached metadata (no body needed)
	if !noCacheRead && s.cache.IsEnabled() {
		meta, found, cacheErr := s.cache.GetMeta(ctx, bucket, key)
		if cacheErr == nil && found && meta != nil {
			metrics.RecordCacheHit()
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("HEAD served from cache")
			meta.WriteHeaders(w)
			w.Header().Set(XCacheHeader, XCacheHit)
			w.WriteHeader(meta.StatusCode)
			metrics.RecordRequest("HeadObject", "success", time.Since(start).Seconds())
			return nil
		}
		metrics.RecordCacheMiss()
	}

	// Cache miss - forward to upstream
	// Set X-Cache header before forwarding (will be included in response)
	if s.cache.IsEnabled() {
		w.Header().Set(XCacheHeader, XCacheMiss)
	} else {
		w.Header().Set(XCacheHeader, XCacheDisabled)
	}
	err := s.forwarder.Forward(ctx, w, r)

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("HeadObject", status, time.Since(start).Seconds())
	return err
}

// HandleCopyObject handles copy object requests.
func (s *Service) HandleCopyObject(w http.ResponseWriter, r *http.Request) error {
	bucket, key := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandleCopyObject")

	err := s.forwarder.Forward(r.Context(), w, r)

	// Invalidate cache for destination object on success
	if err == nil && s.cache.IsEnabled() {
		s.cache.Delete(context.Background(), bucket, key)
	}

	return err
}

// HandlePassthrough handles requests that are passed through without caching.
func (s *Service) HandlePassthrough(w http.ResponseWriter, r *http.Request) error {
	log.Debug().Str("method", r.Method).Str("path", r.URL.Path).Msg("HandlePassthrough")
	return s.forwarder.Forward(r.Context(), w, r)
}

// ============================================================================
// Range Request Caching Support
// ============================================================================

// byteRange represents a parsed byte range.
type byteRange struct {
	start int64
	end   int64
}

// parseRangeHeader parses HTTP Range header.
// Returns list of ranges or error if invalid.
// totalSize is the size of the full object.
func parseRangeHeader(rangeHeader string, totalSize int64) ([]byteRange, error) {
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return nil, errors.New("invalid range header: missing 'bytes=' prefix")
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeSpec, ",")

	var ranges []byteRange
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		dashIdx := strings.Index(part, "-")
		if dashIdx == -1 {
			return nil, errors.New("invalid range: missing dash")
		}

		startStr := part[:dashIdx]
		endStr := part[dashIdx+1:]

		var start, end int64

		if startStr == "" {
			// Suffix range: "-500" means last 500 bytes
			suffixLen, err := strconv.ParseInt(endStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid suffix range: %v", err)
			}
			start = totalSize - suffixLen
			if start < 0 {
				start = 0
			}
			end = totalSize - 1
		} else {
			// Normal range: "0-499" or "500-"
			var err error
			start, err = strconv.ParseInt(startStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid range start: %v", err)
			}

			if endStr == "" {
				// Open-ended: "500-" means from 500 to end
				end = totalSize - 1
			} else {
				end, err = strconv.ParseInt(endStr, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid range end: %v", err)
				}
			}
		}

		// Validate range
		if start < 0 || start >= totalSize {
			return nil, errors.New("range start out of bounds")
		}
		if end >= totalSize {
			end = totalSize - 1
		}
		if start > end {
			return nil, errors.New("invalid range: start > end")
		}

		ranges = append(ranges, byteRange{start: start, end: end})
	}

	return ranges, nil
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

// serveRangeFromCache serves a Range request from the cached full object.
func (s *Service) serveRangeFromCache(
	ctx context.Context,
	w http.ResponseWriter,
	bucket, key string,
	meta *cache.CachedObjectMeta,
	rangeHeader string,
	startTime time.Time,
) error {
	// Parse Range header
	ranges, err := parseRangeHeader(rangeHeader, meta.ContentLength)
	if err != nil || len(ranges) == 0 {
		// Invalid range - return 416 Range Not Satisfiable
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.ContentLength))
		w.Header().Set(XCacheHeader, XCacheHit)
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		metrics.RecordRequest("GetObject", "range_not_satisfiable", time.Since(startTime).Seconds())
		return nil
	}

	// Only support single range (multi-range is complex and rare)
	if len(ranges) > 1 {
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Multi-range not supported from cache")
		// For multi-range, return 416 - we don't support it
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.ContentLength))
		w.Header().Set(XCacheHeader, XCacheHit)
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		metrics.RecordRequest("GetObject", "range_not_satisfiable", time.Since(startTime).Seconds())
		return nil
	}

	rng := ranges[0]
	contentLength := rng.end - rng.start + 1

	// Set response headers
	if meta.ContentType != "" {
		w.Header().Set("Content-Type", meta.ContentType)
	}
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.end, meta.ContentLength))
	if meta.ETag != "" {
		w.Header().Set("ETag", meta.ETag)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set(XCacheHeader, XCacheHit)
	w.WriteHeader(http.StatusPartialContent)

	// Stream range from cache
	if err := s.cache.GetRangeStream(ctx, bucket, key, rng.start, rng.end, w); err != nil {
		log.Warn().Err(err).Str("bucket", bucket).Str("key", key).
			Int64("start", rng.start).Int64("end", rng.end).
			Msg("Failed to stream range from cache")
		// Headers already sent, can't return error to client
		return err
	}

	metrics.RecordRangeFromCacheHit()
	metrics.RecordRequest("GetObject", "success", time.Since(startTime).Seconds())
	return nil
}

// handleRangeWithBackgroundCache handles a Range request on cache miss.
// It forwards the Range request immediately while triggering a background
// fetch of the full object for caching (if within size threshold).
func (s *Service) handleRangeWithBackgroundCache(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key, accessKey, secretKey string,
	startTime time.Time,
) error {
	// Forward the Range request directly to client (low latency)
	resp, err := s.forwarder.DoRequestWithCreds(ctx, r, accessKey, secretKey)
	if err != nil {
		metrics.RecordRequest("GetObject", "error", time.Since(startTime).Seconds())
		return err
	}
	defer resp.Body.Close()

	// Determine total object size from Content-Range header
	// Format: "bytes 0-499/1234" where 1234 is total size
	totalSize := extractTotalSizeFromContentRange(resp.Header.Get("Content-Range"))

	// Trigger background fetch if:
	// - Response is 206 Partial Content (successful range response)
	// - Total size is known and within cache threshold
	// - Cache is enabled
	// - We have valid credentials for the background fetch
	if resp.StatusCode == http.StatusPartialContent &&
		totalSize > 0 &&
		totalSize <= s.config.Cache.SizeThreshold &&
		s.cache.IsEnabled() &&
		accessKey != "" && secretKey != "" {
		s.triggerBackgroundCacheFetch(bucket, key, accessKey, secretKey)
	}

	// Write response headers
	copyHeaders(w.Header(), resp.Header)
	w.Header().Set(XCacheHeader, XCacheMiss)
	w.WriteHeader(resp.StatusCode)

	// Stream Range response body to client
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Warn().Err(err).Msg("Failed to copy range response body")
		return err
	}

	metrics.RecordRequest("GetObject", "success", time.Since(startTime).Seconds())
	return nil
}

// triggerBackgroundCacheFetch starts a background fetch of the full object.
// Uses broadcast manager to coalesce multiple triggers for the same object.
func (s *Service) triggerBackgroundCacheFetch(bucket, key, accessKey, secretKey string) {
	bcastKey := "bg:" + bucket + "/" + key

	broadcaster, isFirst := s.backgroundFetchManager.GetOrCreate(bcastKey)
	if !isFirst {
		// Another background fetch is already in progress
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Background fetch already in progress, coalescing")
		return
	}

	// I'm the first - start the background fetch
	metrics.RecordBackgroundFetchTriggered()
	metrics.SetActiveBackgroundFetches(s.backgroundFetchManager.ActiveCount())

	go func() {
		defer func() {
			s.backgroundFetchManager.Remove(bcastKey)
			metrics.SetActiveBackgroundFetches(s.backgroundFetchManager.ActiveCount())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		err := s.fetchFullObjectToCache(ctx, bucket, key, accessKey, secretKey, broadcaster)
		broadcaster.Complete(err)

		if err != nil {
			log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Background cache fetch failed")
			metrics.RecordBackgroundFetchFailed()
		} else {
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Background cache fetch completed")
			metrics.RecordBackgroundFetchSucceeded()
		}
	}()
}

// fetchFullObjectToCache fetches the full object and caches it.
// This makes a full-object request (no Range header) and streams directly to cache.
func (s *Service) fetchFullObjectToCache(
	ctx context.Context,
	bucket, key, accessKey, secretKey string,
	broadcaster *broadcast.Broadcaster,
) error {
	// Execute full object request (no Range header)
	resp, err := s.forwarder.DoFullObjectRequest(ctx, bucket, key, accessKey, secretKey)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d for background fetch", resp.StatusCode)
	}

	// Check if response is within cache threshold
	if resp.ContentLength > 0 && resp.ContentLength > s.config.Cache.SizeThreshold {
		log.Debug().
			Str("bucket", bucket).
			Str("key", key).
			Int64("size", resp.ContentLength).
			Int64("threshold", s.config.Cache.SizeThreshold).
			Msg("Skipping background cache - object too large")
		return nil
	}

	// Check for no-cache headers
	if s.hasNoCacheHeaders(resp.Header) {
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Skipping background cache - no-cache headers")
		return nil
	}

	// Set up cache listener (streams to cache via pipe)
	cachePipeWriter, cacheErrCh := s.setupCacheListener(ctx, bucket, key, broadcaster)

	// Set headers for the cache listener
	broadcaster.SetHeaders(resp.StatusCode, resp.Header)

	// Stream body to broadcaster (which includes cache listener)
	chunkSize := s.config.Broadcast.ChunkSize
	if chunkSize <= 0 {
		chunkSize = broadcast.DefaultChunkSize
	}
	buf := make([]byte, chunkSize)

	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			broadcaster.Broadcast(buf[:n])
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	// Wait for cache write to complete
	if cacheErrCh != nil {
		select {
		case err := <-cacheErrCh:
			if err != nil {
				log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Background cache write failed")
			}
			return err
		case <-time.After(30 * time.Second):
			log.Warn().Str("bucket", bucket).Str("key", key).Msg("Background cache write timeout")
			return errors.New("cache write timeout")
		}
	}

	_ = cachePipeWriter // managed by cache listener goroutine
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

// shouldSkipCache checks if cache should be skipped for this request.
func shouldSkipCache(r *http.Request) bool {
	cc := r.Header.Get("Cache-Control")
	return strings.Contains(cc, "no-cache") || strings.Contains(cc, "max-age=0")
}
