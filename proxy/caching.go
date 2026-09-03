package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/proxy/broadcast"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// signalingReader wraps an io.Reader and signals when the first Read() is called.
// This is used to synchronize the cache writer startup with the chunk consumer:
// we wait until the cache reader is actually blocked on Read() before starting
// to write chunks, ensuring the pipe never blocks.
type signalingReader struct {
	r       io.Reader
	ready   chan struct{}
	once    sync.Once
	readErr error // Store any error from signaling
}

// newSignalingReader creates a new signaling reader.
func newSignalingReader(r io.Reader) *signalingReader {
	return &signalingReader{
		r:     r,
		ready: make(chan struct{}),
	}
}

// Read implements io.Reader. On first call, it signals that the reader is ready.
func (s *signalingReader) Read(p []byte) (n int, err error) {
	s.once.Do(func() { close(s.ready) })
	return s.r.Read(p)
}

// Ready returns a channel that is closed when Read() is first called.
func (s *signalingReader) Ready() <-chan struct{} {
	return s.ready
}

const (
	// cacheWriteTimeout is the base timeout for cache writes.
	cacheWriteTimeout = 60 * time.Second

	// backgroundFetchTimeout is the timeout for background fetches.
	backgroundFetchTimeout = 5 * time.Minute

	// minCacheWriteThroughput is the minimum expected cache write speed.
	// Used to compute dynamic timeouts for large objects.
	// 5 MB/s is conservative for local disk writes.
	minCacheWriteThroughput = 5 * 1024 * 1024 // 5 MB/s
)

// errCachePopulateDeclined signals that a background cache populate was
// deliberately skipped because the concurrent-cache-write limit was saturated.
// It is distinct from a fetch failure so callers do not record it as either a
// success or a failure.
var errCachePopulateDeclined = errors.New("cache populate declined: concurrent write limit reached")

// bodyGone reports whether a failed cache-body read should invalidate the metadata
// that pointed at it, letting the orphaned entry heal (and unblocking the
// GetMeta(!found)-gated re-warm).
//
// It is deliberately a denylist of transient errors rather than an allowlist of
// "body missing" ones. The cache signals an unusable body in more ways than can be
// reliably enumerated — not-found, a stream that ends with no bytes (io.EOF), an
// out-of-range read against a body shorter than its metadata claims (ocache returns
// InvalidArgument) — and missing any one of them lets metadata outlive its body, so
// every request re-probes, fails, and forwards upstream until the metadata TTL
// expires (up to 24h). That cold-miss loop is the severe, persistent failure.
//
// The errors that must NOT evict are the transient ones, where the cached body is
// probably fine and we merely could not finish reading it — overwhelmingly a
// canceled context from a client that disconnected mid-stream (in cluster mode the
// read is routed over gRPC, so it can arrive as a status code instead of a plain
// context error), or a briefly unreachable peer. Those leave the entry alone.
// Anything else heals the entry; the worst case is one extra miss on a rare I/O
// error, which repopulates immediately — a far better trade than a 24h loop.
func bodyGone(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
			return false
		}
	}
	return true
}

// cacheWriteTimeoutForSize returns a timeout scaled to contentLength.
// Returns at least cacheWriteTimeout (60s), scaling up for large objects.
func cacheWriteTimeoutForSize(contentLength int64) time.Duration {
	if contentLength <= 0 {
		return cacheWriteTimeout
	}
	sizeBasedTimeout := time.Duration(contentLength/minCacheWriteThroughput) * time.Second
	if sizeBasedTimeout > cacheWriteTimeout {
		return sizeBasedTimeout
	}
	return cacheWriteTimeout
}

// contextBoundReader makes a context-free cache Put observe cancellation at the
// reader boundary. The body is closed separately when the context expires so a
// blocked network read is interrupted; checking again after Read prevents a
// close-induced EOF from being treated as a complete stream.
type contextBoundReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextBoundReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}
	return n, err
}

// waitForCacheWrite waits for a detached cache writer while retaining the
// deadline that bounds the background fetch. The result channel is buffered by
// the caller so a writer that ignores cancellation can still finish later.
func waitForCacheWrite(ctx context.Context, cacheErrCh <-chan error) (error, bool) {
	select {
	case err := <-cacheErrCh:
		return err, false
	case <-ctx.Done():
		return ctx.Err(), true
	}
}

// setupCacheListener creates a listener that streams chunks directly to cache via io.Pipe.
// This avoids buffering the entire response in memory.
// Stores both metadata (from headers) and body in separate cache entries.
// Uses tombstone-aware writes to prevent stale cache after invalidation.
//
// Uses a hybrid signaling reader + intermediate buffer pattern:
// - io.Pipe has zero buffer, so writes block until reads occur
// - We start the cache reader FIRST and wait for it to call Read()
// - Chunks are consumed into an intermediate buffer immediately (non-blocking)
// - A separate goroutine drains the buffer to the pipe after Ready() signals
// - The 4MB intermediate buffer absorbs chunks during cache writer initialization
// - This provides true streaming with O(chunk_size + buffer_size) memory
func (s *Service) setupCacheListener(
	ctx context.Context,
	bucket, key string,
	broadcaster *broadcast.Broadcaster,
	slotHeld bool,
	weight int64,
	writeStartTime int64,
	checksumMode bool,
) (*io.PipeWriter, chan error) {
	// Bound concurrent cache-populate operations. When the limit is saturated,
	// skip caching entirely: the object is still served/forwarded from upstream,
	// we just don't populate the cache this time. This keeps the memory- and
	// I/O-heavy write pipeline (pipe + goroutines + streaming RocksDB write) from
	// growing without bound under load. slotHeld is true when the caller has
	// already reserved a slot (the background path reserves it before the upstream
	// request); once past this point the slot is owned by this function and is
	// released on the no-listener path below or by the populate goroutine. weight is
	// the byte reservation (matching what was/should be acquired) to release.
	if !slotHeld {
		// The inline populate serves a cache miss while streaming from upstream, so
		// it is a read-triggered populate (priorityReadMiss): non-blocking, yields
		// budget headroom to pending warm-on-write.
		if !s.acquireCacheSlot(ctx, weight, priorityReadMiss) {
			metrics.RecordCachePopulateSkipped(metrics.PopulateSourceReadMiss)
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Skipping cache populate - concurrent write limit reached")
			return nil, nil
		}
	}

	listener := broadcaster.Subscribe()
	if listener == nil {
		s.releaseCacheSlot(weight)
		return nil, nil
	}

	// Create pipe for streaming to cache
	pipeReader, pipeWriter := io.Pipe()
	errCh := make(chan error, 1)

	// Start goroutine to consume chunks, build metadata, and write to cache
	go func() {
		defer s.releaseCacheSlot(weight)
		defer close(errCh)

		// Wait for headers to build metadata
		statusCode, headers, err := listener.WaitForHeaders(ctx)
		if err != nil {
			pipeWriter.CloseWithError(err)
			listener.DrainAndRelease() // Return any buffered pooled chunks
			errCh <- err
			return
		}

		// Build metadata from response headers
		meta := cache.MetaFromHTTPHeaders(bucket, key, statusCode, headers)
		meta.ChecksumMode = checksumMode
		// Check if still cacheable based on metadata
		if !meta.IsCacheable(s.config.Cache.SizeThreshold) {
			pipeWriter.CloseWithError(nil)
			listener.DrainAndRelease() // Return any buffered pooled chunks
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Skipping cache - not cacheable")
			return
		}

		// Use a detached context for cache writes to avoid cancellation when HTTP request completes.
		// Scale timeout by content length so large objects have enough time to write.
		cacheCtx, cacheCancel := context.WithTimeout(context.Background(), cacheWriteTimeoutForSize(meta.ContentLength))
		defer cacheCancel()

		ttl := int(s.config.Cache.TTL.Seconds())

		// Wrap pipeReader with signaling reader to know when cache reader is ready
		sigReader := newSignalingReader(pipeReader)

		// Intermediate buffer absorbs chunks while cache writer initializes.
		// Sized as 1/4 of the broadcaster's channel buffer to balance memory savings
		// with sufficient headroom. Total buffering (listener channel + queue) stays
		// well above typical object sizes while reducing per-listener memory.
		cacheQueueSize := s.config.Broadcast.ChannelBuffer / 4
		if cacheQueueSize < 64 {
			cacheQueueSize = 64
		}
		chunkQueue := make(chan []byte, cacheQueueSize)

		// Start cache writer goroutine - will call Read() when ready
		cacheErrCh := make(chan error, 1)
		go func() {
			var cacheErr error
			// Block-eligible objects (>= block_size, block caching on) are stored as fixed-size
			// blocks regardless of how they were fetched — a full-GET read miss or a warm-on-write
			// read-back. The whole-vs-block boundary is size, not access pattern (RFC 0001): both
			// full and range paths converge on one representation per size class. Sub-block objects
			// keep the single whole-body write.
			if s.isBlockEligibleSize(meta.ContentLength) {
				meta.BlockSize = s.config.Cache.BlockSize
				cacheErr = s.putBlocksFromStream(cacheCtx, bucket, key, meta, sigReader, ttl, writeStartTime)
			} else {
				_, cacheErr = s.cache.PutWithMetaStreamTombstoneAware(cacheCtx, bucket, key, meta, sigReader, ttl, writeStartTime)
			}
			if cacheErr != nil {
				log.Debug().Err(cacheErr).Str("bucket", bucket).Str("key", key).Msg("Cache write with metadata failed")
			}
			cacheErrCh <- cacheErr
		}()

		// Pipe writer goroutine: waits for Ready(), then drains queue to pipe
		pipeErrCh := make(chan error, 1)
		go func() {
			// Wait for cache reader to be ready before writing to pipe
			select {
			case <-sigReader.Ready():
				// Reader is ready, safe to write
			case <-cacheCtx.Done():
				pipeErrCh <- cacheCtx.Err()
				// Drain queue to unblock producer, returning pooled buffers
				for chunk := range chunkQueue {
					broadcast.PutChunkBuf(chunk)
				}
				return
			}

			// Drain queue to pipe - blocks on writes, which is fine since reader is ready.
			// Returns pooled buffers after each write.
			var writeErr error
			for chunk := range chunkQueue {
				if _, err := pipeWriter.Write(chunk); err != nil {
					writeErr = err
					broadcast.PutChunkBuf(chunk) // Return current buffer
					// Drain remaining to unblock producer, returning buffers
					for remaining := range chunkQueue {
						broadcast.PutChunkBuf(remaining)
					}
					break
				}
				broadcast.PutChunkBuf(chunk) // Return buffer after successful write
			}
			pipeErrCh <- writeErr
		}()

		// Consume chunks from listener into queue immediately.
		// This runs in parallel with cache writer initialization,
		// with the intermediate buffer absorbing chunks during the startup window.
		var chunkErr error
		var earlyExit bool
	chunkLoop:
		for chunk := range listener.Chunks() {
			if chunk.Err != nil {
				chunkErr = chunk.Err
				earlyExit = true
				break
			}
			if len(chunk.Data) > 0 {
				// Transfer ownership of pooled buffer directly to queue.
				// No copy needed - broadcaster gives each listener its own buffer.
				// The pipe writer goroutine returns buffers to the pool after writing.
				select {
				case chunkQueue <- chunk.Data:
					// Ownership transferred - don't Release()
				case <-cacheCtx.Done():
					broadcast.PutChunkBuf(chunk.Data) // Return unused buffer
					chunkErr = cacheCtx.Err()
					earlyExit = true
					break chunkLoop
				}
			} else {
				chunk.Release() // Return zero-length pooled buffers
			}
		}

		// Drain remaining listener chunks to return pooled buffers on early exit.
		// Runs async since the broadcaster may still be streaming (channel not closed yet).
		if earlyExit {
			listener.DrainAndRelease()
		}

		// Close queue to signal pipe writer to finish
		close(chunkQueue)

		// Wait for pipe writer to finish
		pipeWriteErr := <-pipeErrCh

		// Close the pipe to signal EOF to the reader
		if chunkErr != nil {
			pipeWriter.CloseWithError(chunkErr)
		} else if pipeWriteErr != nil {
			pipeWriter.CloseWithError(pipeWriteErr)
		} else {
			pipeWriter.Close()
		}

		// Wait for cache write to complete and return its error
		cacheErr := <-cacheErrCh
		if chunkErr != nil {
			errCh <- chunkErr
		} else if pipeWriteErr != nil {
			errCh <- pipeWriteErr
		} else {
			errCh <- cacheErr
		}
	}()

	return pipeWriter, errCh
}

// acquireWarmPopulateSlot gives each warm-on-write reservation its own bounded wait
// context. A block-scratch retry can happen after the upstream fetch, so it must not
// reuse the initial acquisition context whose timeout began before that fetch.
func (s *Service) acquireWarmPopulateSlot(ctx context.Context, weight int64) bool {
	acqCtx, cancel := context.WithTimeout(ctx, warmPopulateAcquireTimeout)
	defer cancel()
	return s.acquireCacheSlot(acqCtx, weight, priorityWarmWrite)
}

// fetchFullObjectToCache fetches the full object and caches it.
// This makes a full-object request (no Range header) and streams directly to cache.
// When anonymous is true the fetch is unsigned and, if it succeeds, the object is
// cached as public-read. These are inseparable: public-read may be inferred ONLY
// from a successful UNSIGNED read (which proves anonymous readability). A signed
// fetch — which uses TAG's credentials and can read private objects — must never
// mark an entry public-read, or a later anonymous request could be served a private
// object from cache.
func (s *Service) fetchFullObjectToCache(
	ctx context.Context,
	bucket, key, accessKey, secretKey string,
	anonymous bool,
	prio populatePriority,
) error {
	// This is a background fetch whose only purpose is to populate the cache, so
	// reserve a cache-populate slot up front. If the concurrent-write limit is
	// saturated, skip the whole operation — including the upstream request —
	// rather than fetch and then discard the result under the very pressure the
	// limit is meant to relieve. errCachePopulateDeclined distinguishes this
	// deliberate skip from a real fetch success/failure for the caller's metrics.
	// Background fetches warm the full object and the size isn't known until the
	// response arrives, so reserve the direct writer's fixed buffers up front. If the
	// response selects block representation, add its configured scratch buffer before
	// starting the cache write; a small whole-object response must not pay for scratch
	// it will never use. Unlike the foreground path, the direct writer has no listener
	// channel or relay queue to reserve.
	weight := s.backgroundPopulateWeight()
	// Warm-on-write populates wait (bounded) for their reserved budget and take
	// priority; read-miss populates acquire non-blocking. The warm helper creates a
	// fresh wait context for every reservation, including a post-fetch scratch retry.
	acquired := false
	if prio == priorityWarmWrite {
		acquired = s.acquireWarmPopulateSlot(ctx, weight)
	} else {
		acquired = s.acquireCacheSlot(ctx, weight, prio)
	}
	if !acquired {
		metrics.RecordCachePopulateSkipped(prio.metricSource())
		return errCachePopulateDeclined
	}
	slotOwned := true
	defer func() {
		if slotOwned {
			s.releaseCacheSlot(weight)
		}
	}()

	// Stamp the cache-write start BEFORE the upstream request, for the same reason
	// as the inline path: a timestamp taken after the response leaves the whole
	// round-trip unguarded, letting an invalidation that landed mid-fetch look older
	// than our write and pass the tombstone check.
	writeStartTime := time.Now().UnixNano()

	// Execute full object request (no Range header). An anonymous warm uses an
	// unsigned request so upstream applies anonymous authorization — 200 only if the
	// object is publicly readable — which is what lets us cache public-read safely.
	var (
		resp *http.Response
		err  error
	)
	if anonymous {
		resp, err = s.forwarder.DoAnonymousFullObjectRequest(ctx, bucket, key)
	} else {
		resp, err = s.forwarder.DoFullObjectRequest(ctx, bucket, key, accessKey, secretKey)
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// For an anonymous probe, a 403 is a definitive answer — the object is not
		// publicly readable, so there is nothing to warm as public — not a failure.
		if anonymous && resp.StatusCode == http.StatusForbidden {
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Anonymous warm skipped - object not publicly readable")
			return errCachePopulateDeclined
		}
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

	// The unsigned fetch succeeded, so the object is anonymously readable. Tigris
	// omits X-Amz-Acl for objects inheriting bucket-level access, so inject public-read
	// to ensure the cached metadata allows anonymous reads. Gated on `anonymous`: a
	// signed fetch can read private objects, so it must never mark an entry public.
	if anonymous && resp.Header.Get("X-Amz-Acl") == "" {
		resp.Header.Set("X-Amz-Acl", "public-read")
	}

	// Build metadata only after the response-level gates and anonymous ACL inference.
	// The cache writer owns the body read directly, so no broadcaster, listener
	// channel, intermediate queue, pipe, or relay goroutine is needed on this path.
	meta := cache.MetaFromHTTPHeaders(bucket, key, resp.StatusCode, resp.Header)
	if !meta.IsCacheable(s.config.Cache.SizeThreshold) {
		// The former background relay continued consuming the response after its
		// listener rejected the metadata. Drain it here too so a reusable upstream
		// connection is not lost merely because this response cannot be cached.
		_, _ = io.Copy(io.Discard, resp.Body)
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Skipping background cache - response is not cacheable")
		return nil
	}

	// The representation decision is available only after the response metadata is
	// known. Reserve the block writer's scratch exactly before it starts, so the byte
	// budget covers its actual lifetime without rejecting a smaller whole-object warm.
	// The initial writer bytes and count slot are already held. A read-miss sheds
	// immediately when scratch is unavailable; a warm retries the combined reservation
	// after releasing both, so its priority is preserved without stalling a count slot.
	blockMode := s.isBlockEligibleSize(meta.ContentLength)
	if blockMode {
		blockScratchWeight := int64(s.config.Cache.BlockSize)
		if !s.tryAcquireCacheBytes(blockScratchWeight, prio) {
			if prio == priorityWarmWrite {
				// Do not wait for scratch while holding the initial slot. Re-admit the
				// complete block reservation as a warm so pending read misses yield to it.
				s.releaseCacheSlot(weight)
				slotOwned = false
				combinedWeight := weight + blockScratchWeight
				if !s.acquireWarmPopulateSlot(ctx, combinedWeight) {
					_, _ = io.Copy(io.Discard, resp.Body)
					metrics.RecordCachePopulateSkipped(prio.metricSource())
					return errCachePopulateDeclined
				}
				weight = combinedWeight
				slotOwned = true
			} else {
				_, _ = io.Copy(io.Discard, resp.Body)
				metrics.RecordCachePopulateSkipped(prio.metricSource())
				return errCachePopulateDeclined
			}
		} else {
			weight += blockScratchWeight
		}
	}

	cacheCtx, cacheCancel := context.WithTimeout(context.Background(), cacheWriteTimeoutForSize(meta.ContentLength))
	defer cacheCancel()
	// Embedded local storage reaches storage.Put through a non-seekable reader and
	// does not observe the context itself. Close the upstream body when the detached
	// write deadline fires, and have the reader turn a close-induced EOF into the
	// context error so a truncated body cannot be committed as a successful cache put.
	stopBodyClose := context.AfterFunc(cacheCtx, func() { _ = resp.Body.Close() })
	defer stopBodyClose()
	body := &contextBoundReader{ctx: cacheCtx, reader: resp.Body}
	ttl := int(s.config.Cache.TTL.Seconds())

	// Keep the direct write asynchronous only for deadline handling. On timeout the
	// fetch returns like the former listener path, but the reservation stays owned by
	// the writer until it actually exits; releasing it while an uncooperative cache
	// client still holds buffers would break the aggregate memory bound.
	cacheErrCh := make(chan error, 1)
	go func() {
		var cacheErr error
		// Block-eligible full fetches retain the size-based representation used by the
		// foreground miss path: blocks are written first and tombstone-aware metadata is
		// published last. Smaller objects use the whole-body stream writer.
		if blockMode {
			meta.BlockSize = s.config.Cache.BlockSize
			cacheErr = s.putBlocksFromStream(cacheCtx, bucket, key, meta, body, ttl, writeStartTime)
		} else {
			_, cacheErr = s.cache.PutWithMetaStreamTombstoneAware(
				cacheCtx, bucket, key, meta, body, ttl, writeStartTime,
			)
		}
		cacheErrCh <- cacheErr
	}()

	cacheErr, timedOut := waitForCacheWrite(cacheCtx, cacheErrCh)
	if timedOut {
		// Close synchronously as well as through AfterFunc: the callback may still be
		// queued when the context case wins, and net/http bodies use Close to unblock Read.
		_ = resp.Body.Close()
		log.Warn().Str("bucket", bucket).Str("key", key).Msg("Background cache write timeout")
		slotOwned = false
		go func() {
			<-cacheErrCh
			s.releaseCacheSlot(weight)
		}()
		return errors.New("cache write timeout")
	}
	if cacheErr != nil {
		// A cache client may stop reading after its own failure. The former relay
		// had already consumed the upstream body before observing that failure, so
		// drain the remainder here to preserve a reusable upstream connection.
		_, _ = io.Copy(io.Discard, resp.Body)
		log.Warn().Err(cacheErr).Str("bucket", bucket).Str("key", key).Msg("Background cache write failed")
	}
	return cacheErr
}

// triggerBackgroundCacheFetch starts a background fetch of the full object.
// Uses sync.Map for deduplication: only the first trigger for a given object
// starts a fetch; subsequent triggers while the fetch is in progress are no-ops.
// This avoids broadcast.Manager's "no late joiners" policy which incorrectly
// allows multiple fetches when the first has already started streaming.
// When anonymous is true the fetch is issued without credentials and, on success,
// cached as public-read (see fetchFullObjectToCache); accessKey/secretKey are then
// ignored. Pass anonymous=true exactly when the triggering request was anonymous, so
// public-read is only ever inferred from a confirmed anonymous read.
func (s *Service) triggerBackgroundCacheFetch(bucket, key, accessKey, secretKey string, anonymous bool, prio populatePriority) {
	bcastKey := "bg:" + bucket + "/" + key

	// Atomic check-and-set: if key exists, a fetch is already in progress
	if _, loaded := s.activeBackgroundFetches.LoadOrStore(bcastKey, struct{}{}); loaded {
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Background fetch already in progress, coalescing")
		return
	}

	metrics.RecordBackgroundFetchTriggered()
	metrics.ActiveBackgroundFetches.Inc()

	go func() {
		defer metrics.ActiveBackgroundFetches.Dec()
		defer s.activeBackgroundFetches.Delete(bcastKey)

		ctx, cancel := context.WithTimeout(context.Background(), backgroundFetchTimeout)
		defer cancel()

		err := s.fetchFullObjectToCache(ctx, bucket, key, accessKey, secretKey, anonymous, prio)

		switch {
		case errors.Is(err, errCachePopulateDeclined):
			// Deliberately skipped because the cache-write limit was saturated —
			// not a fetch success or failure (already counted as a populate skip).
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Background cache fetch skipped - concurrent write limit reached")
		case err != nil:
			log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Background cache fetch failed")
			metrics.RecordBackgroundFetchFailed()
		default:
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Background cache fetch completed")
			metrics.RecordBackgroundFetchSucceeded()
		}
	}()
}

// hasNoCacheDirectives reports whether any Cache-Control field has a directive that
// forbids shared caching. It matches directive names rather than substrings, so extension
// directives and parameter values such as no-storex or foo="no-store" retain normal caching.
func hasNoCacheDirectives(cacheControls []string) bool {
	for _, cacheControl := range cacheControls {
		for i := 0; i < len(cacheControl); {
			// A field can contain a comma-separated list of directives. Skip optional
			// whitespace and any empty directives before reading the next name.
			for i < len(cacheControl) && (cacheControl[i] == ',' || cacheControl[i] == ' ' || cacheControl[i] == '\t') {
				i++
			}
			start := i
			for i < len(cacheControl) && cacheControl[i] != '=' && cacheControl[i] != ',' && cacheControl[i] != ' ' && cacheControl[i] != '\t' {
				i++
			}
			if strings.EqualFold(cacheControl[start:i], "no-store") || strings.EqualFold(cacheControl[start:i], "private") {
				return true
			}

			// Consume the rest of this directive. Commas in quoted extension values do
			// not start another directive, and an escaped quote stays in that value.
			quoted, escaped := false, false
		scanDirective:
			for i < len(cacheControl) {
				switch cacheControl[i] {
				case '\\':
					if quoted {
						escaped = !escaped
					}
				case '"':
					if !escaped {
						quoted = !quoted
					}
					escaped = false
				case ',':
					if !quoted {
						i++
						break scanDirective
					}
				default:
					escaped = false
				}
				i++
			}
		}
	}
	return false
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
