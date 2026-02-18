package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

// bufferPool provides reusable buffers for small object caching.
// Reduces allocations and GC pressure at high QPS.
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// smallObjectThreshold defines the max size for direct buffered serving.
// Objects at or below this size buffer the body directly instead of using
// io.Pipe + goroutine. This eliminates per-request goroutine spawn, io.Pipe
// allocation, and synchronization overhead for small objects.
const smallObjectThreshold = 64 * 1024 // 64KB

// putBuffer returns a buffer to the pool only if it hasn't grown too large.
// This prevents memory bloat from oversized buffers (e.g., if cached body
// exceeds metadata claims due to corruption/inconsistency).
func putBuffer(buf *bytes.Buffer) {
	if buf.Cap() <= smallObjectThreshold {
		bufferPool.Put(buf)
	}
	// Otherwise let it be garbage collected
}

// countingWriter wraps an io.Writer to count bytes written.
type countingWriter struct {
	w       io.Writer
	written int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.written += int64(n)
	return n, err
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
	result, accessKey, secretKey, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil {
		metrics.RecordRequest("GetObject", "auth_error", time.Since(start).Seconds())
		return err
	}

	// 2. Check cache (fast path) - now also works for range requests!
	// For range requests: check if full object is in cache, then serve range from it.
	// Uses two-phase approach: GetMeta first, then GetBodyStream for direct streaming.
	if result == AuthValidated && !noCacheRead && s.cache.IsEnabled() {
		meta, found, cacheErr := s.cache.GetMeta(ctx, bucket, key)
		if cacheErr == nil && found && meta != nil {
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
				w.Header().Set("ETag", meta.ETag)
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
						w.Header().Set("ETag", meta.ETag)
						w.WriteHeader(http.StatusNotModified)
						metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
						return nil
					}
				}
			}

			// Serve full response from cache with proper headers
			// Handle zero-byte objects directly (no body to verify)
			if meta.ContentLength == 0 {
				metrics.RecordCacheHit()
				log.Debug().Str("bucket", bucket).Str("key", key).Msg("Serving zero-byte object from cache")
				meta.WriteHeaders(w)
				w.Header().Set(XCacheHeader, XCacheHit)
				w.WriteHeader(meta.StatusCode)
				metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
				return nil
			}

			// Small objects: buffer body directly (no goroutine/io.Pipe overhead)
			// Uses GetBodyStream to buffer, reusing metadata from GetMeta above (2 gRPC calls total)
			if meta.ContentLength <= smallObjectThreshold {
				// Get buffer from pool to reduce allocations
				bodyBuf := bufferPool.Get().(*bytes.Buffer)
				bodyBuf.Reset()

				bodyErr := s.cache.GetBodyStream(ctx, bucket, key, bodyBuf)
				// Verify body was actually retrieved: ocache GetStream returns nil
				// for not-found keys (by design), so we must check bytes written.
				if bodyErr == nil && bodyBuf.Len() > 0 {
					metrics.RecordCacheHit()
					log.Debug().Str("bucket", bucket).Str("key", key).Int64("size", meta.ContentLength).Msg("Serving small object from cache (fast path)")
					meta.WriteHeaders(w)
					w.Header().Set(XCacheHeader, XCacheHit)
					w.WriteHeader(meta.StatusCode)
					n, _ := w.Write(bodyBuf.Bytes())
					metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))
					metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
					putBuffer(bodyBuf) // Return buffer to pool
					return nil
				}
				putBuffer(bodyBuf) // Return buffer to pool even on error/miss
				// Fall through to upstream (cache miss handling below)
				if bodyErr != nil {
					log.Debug().Err(bodyErr).Str("bucket", bucket).Str("key", key).Msg("GetBodyStream failed for small object, falling back to upstream")
				} else {
					log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache body empty despite meta hit, falling back to upstream")
				}
			} else {
				// Large objects: use io.Pipe for streaming (avoids buffering entire object)
				pr, pw := io.Pipe()
				go func() {
					err := s.cache.GetBodyStream(ctx, bucket, key, pw)
					if err != nil {
						pw.CloseWithError(err)
					} else {
						pw.Close()
					}
				}()

				// Read first byte to verify body exists before committing headers
				firstByte := make([]byte, 1)
				n, readErr := pr.Read(firstByte)
				if readErr != nil {
					// Body missing or error - close pipe and fall through to upstream
					pr.Close()
					log.Debug().Err(readErr).Str("bucket", bucket).Str("key", key).Msg("Cache body missing, falling back to upstream")
					// Fall through to cache miss handling below
				} else {
					// Body verified - safe to write headers and stream
					metrics.RecordCacheHit()
					log.Debug().Str("bucket", bucket).Str("key", key).Msg("Serving large object from cache (streaming)")
					meta.WriteHeaders(w)
					w.Header().Set(XCacheHeader, XCacheHit)
					w.WriteHeader(meta.StatusCode)

					// Write first byte and stream the rest
					cw := &countingWriter{w: w}
					cw.Write(firstByte[:n])
					io.Copy(cw, pr)
					pr.Close()

					metrics.BytesTransferred.WithLabelValues("out").Add(float64(cw.written))
					metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
					return nil
				}
			}
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
//
// Header commitment is deferred until the first data chunk arrives. This prevents
// committing a 200 OK with Content-Length when the upstream fetch may still fail.
// If an error arrives before any data, the caller can write a proper error response.
func (s *Service) writeChunksToResponse(
	ctx context.Context,
	w http.ResponseWriter,
	listener *broadcast.Listener,
	xCache string,
) error {
	// Wait for headers first
	status, headers, err := listener.WaitForHeaders(ctx)
	if err != nil {
		listener.DrainAndRelease() // Return any buffered pooled chunks
		return err
	}

	var headersWritten bool
	var totalBytesOut int64
	var earlyExit bool
	defer func() {
		// Track bytes out to client, even on error
		if totalBytesOut > 0 {
			metrics.BytesTransferred.WithLabelValues("out").Add(float64(totalBytesOut))
		}
		// Drain remaining pooled chunks on early exit (error, slow consumer, write failure)
		if earlyExit {
			listener.DrainAndRelease()
		}
	}()

	for chunk := range listener.Chunks() {
		if chunk.Err != nil {
			if chunk.Err == broadcast.ErrSlowConsumer {
				metrics.RecordBroadcastSlowConsumer()
			}
			earlyExit = true
			return chunk.Err
		}

		if len(chunk.Data) > 0 {
			if !headersWritten {
				copyHeaders(w.Header(), headers)
				w.Header().Set(XCacheHeader, xCache)
				w.WriteHeader(status)
				headersWritten = true
			}
			n, writeErr := w.Write(chunk.Data)
			totalBytesOut += int64(n)
			chunk.Release() // Return pooled buffer after write copies data
			if writeErr != nil {
				earlyExit = true
				return writeErr
			}
			// Flush if ResponseWriter supports it
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		} else {
			chunk.Release() // Return zero-length pooled buffers
		}
	}

	// Zero-byte response or all-empty chunks: commit headers now
	if !headersWritten {
		copyHeaders(w.Header(), headers)
		w.Header().Set(XCacheHeader, xCache)
		w.WriteHeader(status)
	}

	return nil
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

	// Stream range from cache using counting writer to track actual bytes
	cw := &countingWriter{w: w}
	streamErr := s.cache.GetRangeStream(ctx, bucket, key, rng.start, rng.end, cw)

	// Track bytes out (even on error, some bytes may have been written)
	if cw.written > 0 {
		metrics.BytesTransferred.WithLabelValues("out").Add(float64(cw.written))
	}

	if streamErr != nil {
		log.Warn().Err(streamErr).Str("bucket", bucket).Str("key", key).
			Int64("start", rng.start).Int64("end", rng.end).
			Msg("Failed to stream range from cache")
		// Headers already sent, can't return error to client
		return streamErr
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

	// Write response headers
	copyHeaders(w.Header(), resp.Header)
	w.Header().Set(XCacheHeader, XCacheMiss)
	w.WriteHeader(resp.StatusCode)

	// Stream Range response body to client FIRST (before background fetch)
	// This ensures the client request completes without resource contention
	n, err := io.Copy(w, resp.Body)
	metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))
	if err != nil {
		log.Warn().Err(err).Msg("Failed to copy range response body")
		return err
	}

	metrics.RecordRequest("GetObject", "success", time.Since(startTime).Seconds())

	// Trigger background fetch AFTER streaming completes to avoid resource contention.
	// Conditions:
	// - Response is 206 Partial Content (successful range response)
	// - Total size is known and within cache threshold
	// - Cache is enabled
	// - We have valid credentials for the background fetch
	// - Object is not already cached (prevents redundant fetches when many
	//   parallel range requests each finish after a prior background fetch completes)
	if resp.StatusCode == http.StatusPartialContent &&
		totalSize > 0 &&
		totalSize <= s.config.Cache.SizeThreshold &&
		s.cache.IsEnabled() &&
		accessKey != "" && secretKey != "" {
		if _, found, _ := s.cache.GetMeta(context.Background(), bucket, key); !found {
			s.triggerBackgroundCacheFetch(bucket, key, accessKey, secretKey)
		}
	}

	return nil
}
