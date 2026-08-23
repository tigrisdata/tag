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
	"github.com/tigrisdata/tag/s3err"
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
// Supports client-triggered cache revalidation via Cache-Control: no-cache/max-age=0.
func (s *Service) HandleGetObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	ctx := r.Context()
	bucket, key := ParseBucketKey(r)
	forceRevalidate := s.revalidationRequested(r)
	bypassCache := shouldBypassCache(r)
	rangeHeader := r.Header.Get("Range")

	// Conditional request headers
	ifNoneMatch := r.Header.Get("If-None-Match")
	ifModifiedSince := r.Header.Get("If-Modified-Since")

	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Bool("force_revalidate", forceRevalidate).
		Bool("bypass_cache", bypassCache).
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
	// Anonymous requests can also read from cache if the object's ACL is public-read.
	// Cache-Control: no-store bypasses cache entirely; no-cache/max-age=0 triggers revalidation.
	isAnonymous := isAnonymousRequest(r, result, err)

	if (result == AuthValidated || isAnonymous) && !bypassCache && s.cache.IsEnabled() {
		meta, found, cacheErr := s.cache.GetMeta(ctx, bucket, key)
		cacheHit := cacheErr == nil && found && meta != nil
		// Anonymous requests can only be served from cache if the object's ACL allows it
		if cacheHit && isAnonymous && !meta.IsPublicRead() {
			cacheHit = false
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Skipping cache for anonymous request - object not public")
		}
		if cacheHit {
			// Client-triggered revalidation: Cache-Control: no-cache/max-age=0
			// For range requests, the Range header is included in the conditional GET
			// so upstream returns 304 (serve range from cache) or 206 (stream range to client).
			// revalidateAndServe uses the whole-body serve/populate helpers, which don't
			// understand block-mode entries — so for a block-mode entry (BlockSize>0) fall
			// through to the miss path instead, which re-forwards fresh and re-populates blocks.
			if forceRevalidate && meta.ETag != "" && meta.BlockSize == 0 {
				log.Debug().
					Str("bucket", bucket).
					Str("key", key).
					Msg("Cache hit requires revalidation (client-triggered)")
				return s.revalidateAndServe(ctx, w, r, bucket, key, accessKey, secretKey, meta, start)
			}

			// If the client forced revalidation but there is no whole-body entry to revalidate
			// (no ETag, or a block-mode entry), fall through to the miss path.
			if forceRevalidate {
				// A block-mode entry can't use the conditional-GET revalidate path, and the
				// fall-through's re-populate is gated on GetMeta(!found), so without invalidating
				// here a forced revalidation would serve fresh once but leave the (possibly stale)
				// block-mode entry in place — later normal reads never re-check upstream when their
				// blocks are already cached. Invalidate so the fall-through re-establishes the
				// current version.
				if meta.BlockSize > 0 {
					log.Debug().Str("bucket", bucket).Str("key", key).Msg("Invalidating block-mode entry for client-forced revalidation")
					// Version-guarded: only drop the entry this client is revalidating, never a
					// newer version a concurrent request re-established after an overwrite (an
					// unconditional Delete would wipe that fresh entry and force needless churn).
					s.invalidateStaleBlockMeta(bucket, key, meta.ETag)
				}
				log.Debug().Str("bucket", bucket).Str("key", key).Msg("Force revalidate, falling through to upstream")
				// Fall through to cache miss path below
			} else {
				// Fresh cache hit — serve from cache

				// If this is a Range request and we have the full object cached,
				// serve the range from the cached object
				if rangeHeader != "" {
					// Block-mode entry (RFC 0001): serve the range from its blocks, fetching any
					// missing covering blocks. On a non-serve (budget shed / fetch failure), forward
					// the range with a background (block) populate.
					if meta.BlockSize > 0 {
						served, rangeErr := s.serveRangeFromBlockCache(ctx, w, r, bucket, key, accessKey, secretKey, meta, rangeHeader, start)
						if served {
							return rangeErr
						}
						return s.handleRangeWithBackgroundCache(ctx, w, r, bucket, key, accessKey, secretKey, start, XCacheMiss)
					}
					log.Debug().Str("bucket", bucket).Str("key", key).Msg("Serving range from cached full object")
					served, rangeErr := s.serveRangeFromCache(ctx, w, r, bucket, key, meta, rangeHeader, start)
					if served {
						return rangeErr
					}
					// Only invalidate when the body is genuinely gone: the metadata is then
					// orphaned, and clearing it both heals the entry and unblocks the
					// background re-warm below (gated on GetMeta(!found)) — otherwise every
					// range read for this key forwards upstream until the meta TTL expires
					// (cold-miss loop). A transient failure (e.g. the client disconnected
					// mid-probe) leaves the still-valid entry cached.
					if bodyGone(rangeErr) {
						log.Debug().Str("bucket", bucket).Str("key", key).Msg("Range cache body missing - invalidating orphaned meta and forwarding with background cache")
						s.cache.Delete(context.Background(), bucket, key)
					}
					// Body genuinely gone for an otherwise-cacheable request → a miss.
					return s.handleRangeWithBackgroundCache(ctx, w, r, bucket, key, accessKey, secretKey, start, XCacheMiss)
				}

				// Check conditional request: If-None-Match
				if ifNoneMatch != "" && meta.MatchesETag(ifNoneMatch) {
					log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache hit - 304 Not Modified")
					writeCacheStatus(w, XCacheHit)
					w.Header().Set("ETag", meta.ETag)
					w.WriteHeader(http.StatusNotModified)
					metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
					return nil
				}

				// Check conditional request: If-Modified-Since
				if ifModifiedSince != "" {
					if t, parseErr := http.ParseTime(ifModifiedSince); parseErr == nil {
						if !meta.IsModifiedSince(t) {
							log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache hit - 304 Not Modified (time)")
							writeCacheStatus(w, XCacheHit)
							w.Header().Set("ETag", meta.ETag)
							w.WriteHeader(http.StatusNotModified)
							metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
							return nil
						}
					}
				}

				// Serve full response from cache.
				if meta.BlockSize > 0 {
					// Block-mode entry: assemble the full object from its blocks (RFC 0001),
					// fetching any that are missing. On a non-serve (budget shed / fetch
					// failure) fall through to the miss path rather than serving stale data.
					served, assembleErr := s.serveFullObjectFromBlockCache(ctx, w, bucket, key, accessKey, secretKey, meta, start)
					if served {
						return assembleErr
					}
					// Fall through to cache miss path
				} else if cacheBodyErr := s.serveFromCache(ctx, w, bucket, key, meta, start); cacheBodyErr != nil {
					log.Warn().Err(cacheBodyErr).Str("bucket", bucket).Str("key", key).Msg("Cache body unavailable, falling through to upstream")
					// Invalidate only when the body is genuinely gone: the metadata is then
					// orphaned, and clearing it stops a meta-hit/body-miss loop that repeats
					// the failed cache read on every request before refetching. A transient
					// failure (e.g. the client disconnected mid-read) must not evict a
					// still-valid hot entry. serveFromCache errors before committing
					// headers, so falling through to the miss path below is safe either way.
					if bodyGone(cacheBodyErr) {
						s.cache.Delete(context.Background(), bucket, key)
					}
					// Fall through to cache miss path
				} else {
					return nil
				}
			}
		}
	}

	// 3. Cache miss — determine the X-Cache status once (DISABLED / BYPASS / MISS);
	// the hit/miss counter is recorded where this status is written to the response,
	// so bypassed/disabled requests are not counted as misses.
	xCache := s.cacheMissStatus(bypassCache)

	// With no upstream, a miss is the whole answer. Return it here rather than
	// entering the fetch paths below: both of them (range-with-background-fetch and
	// broadcast coalescing) are built around an upstream response arriving, and a
	// forwarder that cannot produce one leaves the broadcaster waiting on a fetch
	// that never completes — the request hangs instead of 404ing.
	if !s.originPolicy().CanFill() {
		w.Header().Set("X-Cache", xCache)
		metrics.RecordCacheMiss()
		s3err.WriteError(w, r, s3err.ErrNoSuchKey)
		metrics.RecordRequest("GetObject", "success", time.Since(start).Seconds())
		return nil
	}

	// Range requests: forward immediately + trigger background cache fetch
	if rangeHeader != "" {
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Range request cache miss - forwarding with background cache")
		return s.handleRangeWithBackgroundCache(ctx, w, r, bucket, key, accessKey, secretKey, start, xCache)
	}

	// Full object request: use broadcast manager for streaming coalescing
	bcastKey := makeBroadcastKey(bucket, key, rangeHeader)
	broadcaster, isFirstCaller := s.broadcastManager.GetOrCreate(bcastKey)

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

	// If the client's context was canceled before the inline fetch could populate
	// the cache — the cold-owner case, where a peer/client deadline shorter than
	// the cold fetch aborts streamFromUpstream and starves the inline cache write —
	// hand warming off to the deduplicated background fetcher (mirroring
	// handleRangeWithBackgroundCache). Two gates, both required:
	//
	//  1. ctx.Err() != nil — only warm when the client went away. A slow-consumer
	//     drop or any non-cancel failure leaves the inline write streaming to its
	//     own cache listener on the live ctx, so it finalizes on its own; warming
	//     there would race a redundant parallel fetch.
	//  2. the upstream was healthy — wait for the fetch goroutine's recorded
	//     outcome and skip warming on a genuine upstream failure (connection
	//     refused, 5xx, mid-stream drop). Otherwise a sustained upstream outage
	//     would have every timed-out client spawn a doomed background retry,
	//     amplifying load against an already-sick upstream. (A canceled fetch that
	//     was otherwise healthy completes with a context error, which still warms.)
	//
	// The background fetch is detached, deduplicated, and GetMeta(!found)-guarded;
	// fetchFullObjectToCache enforces the size threshold and skips the body download
	// for oversized objects, so a canceled oversized request costs at most one
	// deduplicated header round-trip, never a wasted body transfer.
	if ctx.Err() != nil && s.cache.IsEnabled() && accessKey != "" && secretKey != "" {
		<-broadcaster.Done() // the fetch goroutine above records the upstream outcome
		if upErr := broadcaster.Error(); upErr == nil ||
			errors.Is(upErr, context.Canceled) || errors.Is(upErr, context.DeadlineExceeded) {
			if _, found, _ := s.cache.GetMeta(context.Background(), bucket, key); !found {
				s.triggerBackgroundCacheFetch(bucket, key, accessKey, secretKey, hasNoAuthCredentials(r), priorityReadMiss)
			}
		}
	}

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
	// Stamp the cache-write start BEFORE issuing the upstream request, not after
	// its headers arrive. The tombstone guard compares this against any invalidation
	// that landed while we were fetching, and upstream takes its read snapshot at
	// some point after we send. Stamping after the response would leave the whole
	// upstream round-trip unguarded: a PUT that lands in that window writes a
	// tombstone OLDER than our timestamp, the guard passes, and we cache the
	// pre-PUT body we just read — stale. Stamping first makes the timestamp strictly
	// earlier than the read snapshot, so any racing invalidation is provably newer
	// and blocks the write. The cost is skipping an occasional still-fresh populate
	// (a miss), never serving stale.
	writeStartTime := time.Now().UnixNano()

	// Execute upstream request
	resp, err := s.forwarder.DoRequestWithCreds(ctx, r, accessKey, secretKey)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Determine if we should cache this response. The whole-vs-block decision is made downstream
	// in the shared cache writer (setupCacheListener) by size: a block-eligible object (>=
	// block_size, block caching on) is stored as blocks, everything else whole. A full GET of a
	// block-eligible object therefore block-splits the stream — the same representation a range
	// read would establish (RFC 0001), so there is no whole/block collision to guard against.
	shouldCache := resp.StatusCode == http.StatusOK &&
		s.cache.IsEnabled() &&
		!s.hasNoCacheHeaders(resp.Header) &&
		s.isWithinSizeThreshold(resp)

	// Set up cache listener if caching (streams directly to cache via pipe).
	var cacheErrCh chan error
	if shouldCache {
		// Reserve against the memory budget by the object's actual size (capped at
		// the buffer ceiling), so small objects don't each reserve the worst case.
		weight := s.populateWeight(resp.ContentLength)
		_, cacheErrCh = s.setupCacheListener(ctx, bucket, key, broadcaster, false, weight, writeStartTime)
	}

	// If an anonymous GET succeeded and Tigris didn't set an explicit per-object ACL,
	// the object inherits public access from the bucket. Record this so subsequent
	// anonymous requests can be served from cache.
	if resp.StatusCode == http.StatusOK &&
		hasNoAuthCredentials(r) &&
		resp.Header.Get("X-Amz-Acl") == "" {
		resp.Header.Set("X-Amz-Acl", "public-read")
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

	// A cache listener cannot report completion until broadcaster.Complete closes
	// its chunk channel. fetchAndBroadcast calls Complete after this method returns,
	// so waiting here delays completion without giving the cache writer a chance to
	// finish. Observe an already-ready result, and otherwise log any late error
	// without holding the broadcaster open.
	if cacheErrCh != nil {
		select {
		case cacheWriteErr, ok := <-cacheErrCh:
			if ok && cacheWriteErr != nil {
				log.Warn().Err(cacheWriteErr).Str("bucket", bucket).Str("key", key).Msg("Cache write failed")
			}
		default:
			go func() {
				if cacheWriteErr, ok := <-cacheErrCh; ok && cacheWriteErr != nil {
					log.Warn().Err(cacheWriteErr).Str("bucket", bucket).Str("key", key).Msg("Cache write failed")
				}
			}()
		}
	}
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
				writeCacheStatus(w, xCache)
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
		writeCacheStatus(w, xCache)
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
// writeRangeNotSatisfiable emits the 416 response for a malformed, empty, or multi-range
// request served from a cached entry (a bytes */total Content-Range plus ErrInvalidRange).
// Shared by the whole-body and block-mode range serve paths.
func writeRangeNotSatisfiable(w http.ResponseWriter, r *http.Request, meta *cache.CachedObjectMeta, startTime time.Time) {
	w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.ContentLength))
	writeCacheStatus(w, XCacheHit)
	s3err.WriteError(w, r, s3err.ErrInvalidRange)
	metrics.RecordRequest("GetObject", "range_not_satisfiable", time.Since(startTime).Seconds())
}

// It returns served=true when it has produced a complete client response (a range
// body, or a definitive error response like 416). It returns served=false, without
// touching the response, when the cached body cannot be resolved (e.g. the body was
// independently evicted while its metadata survived, or was written by a
// pre-versioning build under a different key) — the caller then falls through to the
// upstream range path rather than emitting a truncated 206.
func (s *Service) serveRangeFromCache(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
	meta *cache.CachedObjectMeta,
	rangeHeader string,
	startTime time.Time,
) (served bool, err error) {
	// Parse Range header
	ranges, parseErr := parseRangeHeader(rangeHeader, meta.ContentLength)
	if parseErr != nil || len(ranges) == 0 {
		writeRangeNotSatisfiable(w, r, meta, startTime)
		return true, nil
	}

	// Only support single range (multi-range is complex and rare)
	if len(ranges) > 1 {
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Multi-range not supported from cache")
		writeRangeNotSatisfiable(w, r, meta, startTime)
		return true, nil
	}

	rng := ranges[0]

	// Probe the body BEFORE committing 206 status + headers: stream the range
	// through a pipe and read the first byte. If the versioned body is not
	// resolvable, no headers have been sent yet, so we report served=false and let
	// the caller forward to upstream instead of streaming a truncated 206 that the
	// client cannot distinguish from a valid short read.
	pr, pw := io.Pipe()
	go func() {
		streamErr := s.cache.GetRangeStream(ctx, bucket, key, meta.ETag, rng.start, rng.end, pw)
		if streamErr != nil {
			pw.CloseWithError(streamErr)
		} else {
			pw.Close()
		}
	}()

	firstByte := make([]byte, 1)
	n, readErr := pr.Read(firstByte)
	if readErr != nil {
		pr.Close()
		log.Debug().Err(readErr).Str("bucket", bucket).Str("key", key).
			Int64("start", rng.start).Int64("end", rng.end).
			Msg("Range cache body unavailable before headers - falling through to upstream")
		// Report the underlying error, not just served=false: the caller uses it to
		// tell a genuinely-missing body (invalidate the orphaned meta) from a
		// transient failure like a canceled client (leave the entry alone).
		return false, readErr
	}

	meta.WriteHeaders(w, cache.WithRangeHeaders(rng.start, rng.end, meta.ContentLength))
	writeCacheStatus(w, XCacheHit)
	w.WriteHeader(http.StatusPartialContent)

	// Stream range from cache using counting writer to track actual bytes
	cw := &countingWriter{w: w}
	cw.Write(firstByte[:n])
	_, copyErr := io.Copy(cw, pr)
	pr.Close()

	// Track bytes out (even on error, some bytes may have been written)
	if cw.written > 0 {
		metrics.BytesTransferred.WithLabelValues("out").Add(float64(cw.written))
	}

	if copyErr != nil {
		log.Warn().Err(copyErr).Str("bucket", bucket).Str("key", key).
			Int64("start", rng.start).Int64("end", rng.end).
			Msg("Failed to stream range from cache")
		// Headers already sent, can't return error to client
		return true, copyErr
	}

	metrics.RecordRangeFromCacheHit()
	metrics.RecordRequest("GetObject", "success", time.Since(startTime).Seconds())
	return true, nil
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
	xCache string,
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
	_, _, totalSize, _ := parseContentRange(resp.Header.Get("Content-Range"))

	// Decide up front whether this response is cacheable. Conditions:
	// - Response is 206 Partial Content (successful range response)
	// - Total size is known and within cache threshold
	// - Cache is enabled
	// - We have valid credentials for the background fetch
	cacheable := resp.StatusCode == http.StatusPartialContent &&
		totalSize > 0 &&
		totalSize <= s.config.Cache.SizeThreshold &&
		s.cache.IsEnabled() &&
		accessKey != "" && secretKey != ""

	// Trigger the background full-object fetch via defer so it fires AFTER
	// streaming completes regardless of the io.Copy outcome — success OR error.
	// For a cold owner whose client deadline is shorter than the cold fetch,
	// io.Copy fails mid-stream; without this the warming fetch never runs and the
	// same keys are re-fetched on every request (cold-miss/cancel loop).
	//
	// This intentionally also fires on upstream-side io.Copy errors (the range
	// stream closing mid-transfer), not just client disconnects — io.Copy doesn't
	// distinguish the two and warming is the right response to either. The fetch is
	// detached, deduplicated (activeBackgroundFetches), and guarded by
	// GetMeta(!found), so the cost is bounded to at most one background fetch per
	// key and it's a no-op once the object is cached.
	// Objects at or above the block-mode boundary are cached at block granularity (RFC 0001)
	// rather than fetched whole: capture the block-mode meta + the blocks this request touched
	// (from the response headers, before its body is streamed), and populate them in the
	// background. buildBlockMeta/triggerBlockModePopulate never reuse resp.Body — blocks are
	// fetched with fresh aligned range GETs.
	blockEligible := s.isBlockEligibleSize(totalSize) &&
		cacheable &&
		resp.Header.Get("ETag") != "" &&
		!s.hasNoCacheHeaders(resp.Header)

	if blockEligible {
		// If an anonymous range GET succeeded (206) and Tigris didn't set an explicit per-object
		// ACL, the object inherits public access from the bucket — mirror the whole-object path
		// and record public-read so later anonymous reads can be served from the block-mode
		// entry. Without this the entry is stored non-public and every anonymous read is turned
		// away by the IsPublicRead() gate, defeating the cache for anonymous range traffic. The
		// inference is sound: in transparent mode the forward carries the client's (absent) auth,
		// so a 206 means Tigris served an anonymous read.
		meta := s.buildBlockMeta(bucket, key, resp.Header, totalSize)
		// Record public-read on the cached META only — NOT on resp.Header — so this synthetic ACL is
		// never streamed back to the client (its 206 must reflect only what the origin returned;
		// matches the whole-object range path). An anonymous 206 means Tigris served an anonymous
		// read of a bucket-inherited public object, so later anonymous reads can be served from this
		// block-mode entry instead of being turned away by the IsPublicRead() gate.
		if hasNoAuthCredentials(r) && meta.ACL == "" {
			meta.ACL = "public-read"
		}
		if touched := touchedBlocks(r.Header.Get("Range"), totalSize, meta.BlockSize); len(touched) > 0 {
			// Only establish a block-mode entry when none exists (mirrors the whole-object
			// path's GetMeta(!found) dedup). A fall-through after a failed block serve must not
			// rewrite an existing entry's meta: if block_size changed since the entry was
			// written, its cached blocks would be reinterpreted at the new boundaries (index
			// collisions → wrong bytes). Existing entries are filled incrementally by the serve
			// path under their own captured BlockSize instead.
			defer func() {
				if _, found, _ := s.cache.GetMeta(context.Background(), bucket, key); !found {
					s.triggerBlockModePopulate(bucket, key, accessKey, secretKey, meta, touched)
				}
			}()
		}
	} else if cacheable {
		defer func() {
			if _, found, _ := s.cache.GetMeta(context.Background(), bucket, key); !found {
				s.triggerBackgroundCacheFetch(bucket, key, accessKey, secretKey, hasNoAuthCredentials(r), priorityReadMiss)
			}
		}()
	}

	// Write response headers
	copyHeaders(w.Header(), resp.Header)
	writeCacheStatus(w, xCache)
	w.WriteHeader(resp.StatusCode)

	// Stream Range response body to client.
	n, err := io.Copy(w, resp.Body)
	metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))
	if err != nil {
		log.Warn().Err(err).Msg("Failed to copy range response body")
		return err
	}

	metrics.RecordRequest("GetObject", "success", time.Since(startTime).Seconds())

	return nil
}
