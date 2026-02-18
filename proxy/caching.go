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
) (*io.PipeWriter, chan error) {
	// Record when this write started - used to check against tombstones
	writeStartTime := time.Now().UnixNano()

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
			listener.DrainAndRelease() // Return any buffered pooled chunks
			errCh <- err
			return
		}

		// Build metadata from response headers
		meta := cache.MetaFromHTTPHeaders(bucket, key, statusCode, headers)

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
			cacheErr := s.cache.PutWithMetaStreamTombstoneAware(cacheCtx, bucket, key, meta, sigReader, ttl, writeStartTime)
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

	var streamErr error
streamLoop:
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			broadcaster.Broadcast(buf[:n])
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			streamErr = readErr
			break
		}

		select {
		case <-ctx.Done():
			streamErr = ctx.Err()
			break streamLoop
		default:
		}
	}

	// Signal broadcast completion BEFORE waiting for cache write.
	// This is critical: the cache listener's chunk loop blocks until the channel closes,
	// which only happens when Complete() is called. Without this, we'd have a deadlock:
	// - fetchFullObjectToCache waits for cacheErrCh
	// - setupCacheListener waits for listener.Chunks() to close
	// - listener.Chunks() only closes when Complete() is called
	broadcaster.Complete(streamErr)

	// Wait for cache write to complete
	if cacheErrCh != nil {
		select {
		case err := <-cacheErrCh:
			if err != nil {
				log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Background cache write failed")
			}
			// Return stream error if that's what caused the failure
			if streamErr != nil {
				return streamErr
			}
			return err
		case <-time.After(cacheWriteTimeoutForSize(resp.ContentLength)):
			log.Warn().Str("bucket", bucket).Str("key", key).Msg("Background cache write timeout")
			return errors.New("cache write timeout")
		}
	}

	_ = cachePipeWriter // managed by cache listener goroutine
	return streamErr
}

// triggerBackgroundCacheFetch starts a background fetch of the full object.
// Uses sync.Map for deduplication: only the first trigger for a given object
// starts a fetch; subsequent triggers while the fetch is in progress are no-ops.
// This avoids broadcast.Manager's "no late joiners" policy which incorrectly
// allows multiple fetches when the first has already started streaming.
func (s *Service) triggerBackgroundCacheFetch(bucket, key, accessKey, secretKey string) {
	bcastKey := "bg:" + bucket + "/" + key

	// Atomic check-and-set: if key exists, a fetch is already in progress
	if _, loaded := s.activeBackgroundFetches.LoadOrStore(bcastKey, struct{}{}); loaded {
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Background fetch already in progress, coalescing")
		return
	}

	metrics.RecordBackgroundFetchTriggered()
	metrics.ActiveBackgroundFetches.Inc()

	go func() {
		defer s.activeBackgroundFetches.Delete(bcastKey)
		defer metrics.ActiveBackgroundFetches.Dec()

		ctx, cancel := context.WithTimeout(context.Background(), backgroundFetchTimeout)
		defer cancel()

		// Create broadcaster directly — only used for streaming to cache listener
		channelBuf := s.config.Broadcast.ChannelBuffer
		if channelBuf <= 0 {
			channelBuf = broadcast.DefaultChannelBuffer
		}
		broadcaster := broadcast.NewBroadcaster(channelBuf)

		err := s.fetchFullObjectToCache(ctx, bucket, key, accessKey, secretKey, broadcaster)

		if err != nil {
			log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Background cache fetch failed")
			metrics.RecordBackgroundFetchFailed()
		} else {
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Background cache fetch completed")
			metrics.RecordBackgroundFetchSucceeded()
		}
	}()
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

// shouldSkipCache checks if cache should be skipped for this request.
func shouldSkipCache(r *http.Request) bool {
	cc := r.Header.Get("Cache-Control")
	return strings.Contains(cc, "no-cache") || strings.Contains(cc, "max-age=0")
}
