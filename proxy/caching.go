package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

const (
	// cacheWriteTimeout is the timeout for cache writes.
	cacheWriteTimeout = 60 * time.Second

	// backgroundFetchTimeout is the timeout for background fetches.
	backgroundFetchTimeout = 5 * time.Minute
)

// setupCacheListener creates a listener that streams chunks directly to cache via io.Pipe.
// This avoids buffering the entire response in memory.
// Stores both metadata (from headers) and body in separate cache entries.
// Uses tombstone-aware writes to prevent stale cache after invalidation.
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
		cacheCtx, cacheCancel := context.WithTimeout(context.Background(), cacheWriteTimeout)
		defer cacheCancel()

		// Write to cache with metadata (streaming, tombstone-aware)
		// Tombstone-aware write will skip metadata if key was invalidated after writeStartTime
		ttl := int(s.config.Cache.TTL.Seconds())
		cacheErr := s.cache.PutWithMetaStreamTombstoneAware(cacheCtx, bucket, key, meta, pipeReader, ttl, writeStartTime)
		if cacheErr != nil {
			log.Debug().Err(cacheErr).Str("bucket", bucket).Str("key", key).Msg("Cache write with metadata failed")
		}
		errCh <- cacheErr
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
		case <-time.After(cacheWriteTimeout):
			log.Warn().Str("bucket", bucket).Str("key", key).Msg("Background cache write timeout")
			return errors.New("cache write timeout")
		}
	}

	_ = cachePipeWriter // managed by cache listener goroutine
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

		ctx, cancel := context.WithTimeout(context.Background(), backgroundFetchTimeout)
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
