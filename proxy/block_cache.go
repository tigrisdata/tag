package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/s3err"
	"golang.org/x/sync/errgroup"
)

// maxConcurrentBlockFetches bounds how many missing blocks of one request are fetched from
// upstream concurrently.
const maxConcurrentBlockFetches = 4

// errBlockETagMismatch means a block fetched from upstream belongs to a different object
// version than the meta we hold (a concurrent overwrite), so it must not be cached.
var errBlockETagMismatch = errors.New("block etag mismatch")

// isBlockEligibleSize reports whether an object of the given content length is handled by
// block-mode caching (RFC 0001) rather than whole-object caching: it is at or above the
// block-mode boundary and block caching is enabled. A negative/unknown length is not eligible.
// Whole-object populate paths (warm-on-write, full-object background fetch, full-GET stream
// caching) skip such objects — they are cached at block granularity on read instead.
func (s *Service) isBlockEligibleSize(contentLength int64) bool {
	return s.config.Cache.BlockCachingEnabled && contentLength >= s.config.Cache.BlockCacheMinSize
}

// blockBounds returns the inclusive object-byte bounds [start,end] of block i for an object
// of contentLength bytes cached at blockSize granularity. The last block is clamped to the
// object end.
func blockBounds(i, blockSize, contentLength int64) (start, end int64) {
	start = i * blockSize
	end = start + blockSize - 1
	if end > contentLength-1 {
		end = contentLength - 1
	}
	return start, end
}

// coveringBlocks returns the inclusive range of block indices [b0,bK] covering the inclusive
// object byte range [s,e].
func coveringBlocks(s, e, blockSize int64) (b0, bK int64) {
	return s / blockSize, e / blockSize
}

// touchedBlocks returns the block indices a single-range request covers, or nil for a
// malformed/empty/multi-range request (which is not block-populated).
func touchedBlocks(rangeHeader string, totalSize, blockSize int64) []int64 {
	ranges, err := parseRangeHeader(rangeHeader, totalSize)
	if err != nil || len(ranges) != 1 {
		return nil
	}
	b0, bK := coveringBlocks(ranges[0].start, ranges[0].end, blockSize)
	idxs := make([]int64, 0, bK-b0+1)
	for i := b0; i <= bK; i++ {
		idxs = append(idxs, i)
	}
	return idxs
}

// serveRangeFromBlockCache serves a single-range request from a block-mode cache entry
// (meta.BlockSize > 0). It probes the covering blocks, fetches any that are missing (from
// upstream, coalesced), then streams the requested bytes assembled from those blocks.
//
// It returns served=true when it has produced a complete client response (a 206 body, or a
// definitive 416). It returns served=false, without having written anything to w, when the
// covering blocks cannot be populated (budget shed, upstream/ETag failure) — the caller then
// forwards the range to upstream instead.
func (s *Service) serveRangeFromBlockCache(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key, accessKey, secretKey string,
	meta *cache.CachedObjectMeta,
	rangeHeader string,
	startTime time.Time,
) (served bool, err error) {
	// Preserve serveRangeFromCache's error semantics BEFORE computing any blocks: a
	// malformed/unsatisfiable range, an empty range list, or a multi-range request all
	// return 416 (multi-range from cache is unsupported).
	ranges, parseErr := parseRangeHeader(rangeHeader, meta.ContentLength)
	if parseErr != nil || len(ranges) != 1 {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.ContentLength))
		writeCacheStatus(w, XCacheHit)
		s3err.WriteError(w, r, s3err.ErrInvalidRange)
		metrics.RecordRequest("GetObject", "range_not_satisfiable", time.Since(startTime).Seconds())
		return true, nil
	}
	rng := ranges[0]
	blockSize := meta.BlockSize
	b0, bK := coveringBlocks(rng.start, rng.end, blockSize)

	// Make the covering blocks present (probe + fetch any missing, coalesced/concurrent).
	if ferr := s.ensureBlocksCached(ctx, bucket, key, accessKey, secretKey, meta, b0, bK); ferr != nil {
		// Couldn't populate the missing blocks (budget shed, upstream error, or a concurrent
		// overwrite). Nothing written yet — let the caller forward upstream.
		log.Debug().Err(ferr).Str("bucket", bucket).Str("key", key).Msg("Block populate failed - falling through to upstream")
		return false, ferr
	}

	// All covering blocks are present: commit the 206 and stream the assembled range.
	meta.WriteHeaders(w, cache.WithRangeHeaders(rng.start, rng.end, meta.ContentLength))
	writeCacheStatus(w, XCacheHit)
	w.WriteHeader(http.StatusPartialContent)

	cw := &countingWriter{w: w}
	for i := b0; i <= bK; i++ {
		bStart, bEnd := blockBounds(i, blockSize, meta.ContentLength)
		localStart := max(rng.start, bStart) - bStart
		localEnd := min(rng.end, bEnd) - bStart
		if berr := s.cache.GetBlockRangeStream(ctx, bucket, key, meta.ETag, i, localStart, localEnd, cw); berr != nil {
			// A block was evicted between populate and serve. Headers are already sent, so we
			// cannot fall through; report the error (the client sees a truncated body).
			if cw.written > 0 {
				metrics.BytesTransferred.WithLabelValues("out").Add(float64(cw.written))
			}
			log.Warn().Err(berr).Str("bucket", bucket).Str("key", key).Int64("block", i).Msg("Block vanished mid-serve")
			return true, berr
		}
	}
	if cw.written > 0 {
		metrics.BytesTransferred.WithLabelValues("out").Add(float64(cw.written))
	}
	metrics.RecordRangeFromCacheHit()
	metrics.RecordRequest("GetObject", "success", time.Since(startTime).Seconds())
	return true, nil
}

// serveFullObjectFromBlockCache serves a full-object GET from a block-mode entry by assembling
// all of its blocks (fetching any missing), streaming them in order. It returns served=false,
// without writing anything, when the blocks cannot be populated — the caller then falls through
// to the miss path.
func (s *Service) serveFullObjectFromBlockCache(
	ctx context.Context,
	w http.ResponseWriter,
	bucket, key, accessKey, secretKey string,
	meta *cache.CachedObjectMeta,
	startTime time.Time,
) (served bool, err error) {
	// Zero-byte object: headers only.
	if meta.ContentLength == 0 {
		meta.WriteHeaders(w)
		writeCacheStatus(w, XCacheHit)
		w.WriteHeader(meta.StatusCode)
		metrics.RecordRequest("GetObject", "success", time.Since(startTime).Seconds())
		return true, nil
	}

	blockSize := meta.BlockSize
	lastBlock := (meta.ContentLength - 1) / blockSize

	if ferr := s.ensureBlocksCached(ctx, bucket, key, accessKey, secretKey, meta, 0, lastBlock); ferr != nil {
		log.Debug().Err(ferr).Str("bucket", bucket).Str("key", key).Msg("Full-object block assembly failed - falling through to upstream")
		return false, ferr
	}

	meta.WriteHeaders(w)
	writeCacheStatus(w, XCacheHit)
	w.WriteHeader(meta.StatusCode)

	cw := &countingWriter{w: w}
	for i := int64(0); i <= lastBlock; i++ {
		bStart, bEnd := blockBounds(i, blockSize, meta.ContentLength)
		if berr := s.cache.GetBlockRangeStream(ctx, bucket, key, meta.ETag, i, 0, bEnd-bStart, cw); berr != nil {
			if cw.written > 0 {
				metrics.BytesTransferred.WithLabelValues("out").Add(float64(cw.written))
			}
			log.Warn().Err(berr).Str("bucket", bucket).Str("key", key).Int64("block", i).Msg("Block vanished mid-assembly")
			return true, berr
		}
	}
	if cw.written > 0 {
		metrics.BytesTransferred.WithLabelValues("out").Add(float64(cw.written))
	}
	metrics.RecordRequest("GetObject", "success", time.Since(startTime).Seconds())
	return true, nil
}

// fetchBlocksToCache fetches the given missing blocks from upstream and writes them to cache,
// concurrently (bounded) and coalesced per block across requests. It returns the first error;
// on any error the caller must not serve from cache (some blocks may be absent).
func (s *Service) fetchBlocksToCache(ctx context.Context, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, blockIdxs []int64) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentBlockFetches)
	for _, idx := range blockIdxs {
		g.Go(func() error {
			return s.fetchOneBlock(gctx, bucket, key, accessKey, secretKey, meta, idx)
		})
	}
	return g.Wait()
}

// fetchOneBlock fetches a single block from upstream (an aligned range GET) and writes it to
// cache. Concurrent fetches of the same block (across requests) are coalesced via singleflight.
// The populate budget is reserved by the block's actual size (read-miss priority, non-blocking);
// on decline the block is not cached and errCachePopulateDeclined is returned. The fetched
// block's ETag must match meta.ETag or it is rejected (a concurrent overwrite).
func (s *Service) fetchOneBlock(ctx context.Context, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, blockIdx int64) error {
	blockKey := cache.MakeBlockKey(bucket, key, meta.ETag, blockIdx)
	_, err, _ := s.blockFetch.Do(blockKey, func() (interface{}, error) {
		// Re-check presence inside the singleflight leader: a prior fetch may have landed.
		if s.cache.BlockExists(ctx, bucket, key, meta.ETag, blockIdx) {
			return nil, nil
		}
		bStart, bEnd := blockBounds(blockIdx, meta.BlockSize, meta.ContentLength)
		weight := bEnd - bStart + 1

		// Non-blocking read-miss reservation by the block's actual size. On decline the block
		// isn't cached; the caller falls through to a direct upstream range.
		if !s.acquireCacheSlot(ctx, weight, priorityReadMiss) {
			metrics.RecordCachePopulateSkipped(priorityReadMiss.metricSource())
			return nil, errCachePopulateDeclined
		}
		defer s.releaseCacheSlot(weight)

		resp, rerr := s.forwarder.DoConditionalGetRequest(ctx, bucket, key, accessKey, secretKey, "", 0, fmt.Sprintf("bytes=%d-%d", bStart, bEnd))
		if rerr != nil {
			return nil, rerr
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unexpected status %d fetching block %d", resp.StatusCode, blockIdx)
		}
		// ETag guard: the block must belong to the exact version meta describes.
		if respETag := resp.Header.Get("ETag"); respETag != "" && respETag != meta.ETag {
			return nil, errBlockETagMismatch
		}
		ttl := int(s.config.Cache.TTL.Seconds())
		if perr := s.cache.PutBlockStream(ctx, bucket, key, meta.ETag, blockIdx, resp.Body, ttl); perr != nil {
			return nil, perr
		}
		metrics.CacheBlockPopulated.Inc()
		metrics.CacheBlockBytesPopulated.Add(float64(weight))
		return nil, nil
	})
	return err
}

// ensureBlocksCached makes covering blocks [b0,bK] present in cache: it probes each (recording
// per-block hits/misses), fetches any that are missing, and records whether the request was a
// full hit or a partial hit. It returns an error only when a missing block could not be
// populated (budget shed, upstream/ETag failure) — the caller then falls through to upstream.
func (s *Service) ensureBlocksCached(ctx context.Context, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, b0, bK int64) error {
	var missing []int64
	for i := b0; i <= bK; i++ {
		if s.cache.BlockExists(ctx, bucket, key, meta.ETag, i) {
			metrics.CacheBlockHits.Inc()
		} else {
			metrics.CacheBlockMisses.Inc()
			missing = append(missing, i)
		}
	}
	if len(missing) == 0 {
		metrics.CacheBlockRangeServed.WithLabelValues("full_hit").Inc()
		return nil
	}
	if err := s.fetchBlocksToCache(ctx, bucket, key, accessKey, secretKey, meta, missing); err != nil {
		return err
	}
	metrics.CacheBlockRangeServed.WithLabelValues("partial_hit").Inc()
	return nil
}

// buildBlockMeta builds a block-mode CachedObjectMeta from a range (206) response's headers.
// The 206 Content-Length is the range length, so ContentLength is set to the object's total
// size (from Content-Range) and StatusCode to 200 (the object status, not the partial one).
func (s *Service) buildBlockMeta(bucket, key string, respHeader http.Header, totalSize int64) *cache.CachedObjectMeta {
	meta := cache.MetaFromHTTPHeaders(bucket, key, http.StatusOK, respHeader)
	meta.ContentLength = totalSize
	meta.BlockSize = s.config.Cache.BlockSize
	return meta
}

// triggerBlockModePopulate populates a block-mode entry in the background after a cold range
// miss: it fetches the blocks the request touched and, only if they all land, writes the
// block-mode meta (tombstone-aware, the visibility gate). It never touches the original
// request or its response body — blocks are fetched with fresh aligned range GETs.
func (s *Service) triggerBlockModePopulate(bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, touched []int64) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), backgroundFetchTimeout)
		defer cancel()

		// Stamp the write start BEFORE fetching, so an invalidation that lands mid-populate is
		// newer than our timestamp and blocks the meta write (mirrors the whole-object paths).
		writeStartTime := time.Now().UnixNano()

		if err := s.fetchBlocksToCache(ctx, bucket, key, accessKey, secretKey, meta, touched); err != nil {
			log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Block-mode populate skipped - block fetch failed")
			return
		}
		ttl := int(s.config.Cache.TTL.Seconds())
		wrote, err := s.cache.PutMetaTombstoneAware(ctx, bucket, key, meta, ttl, writeStartTime)
		if err != nil || !wrote {
			log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Bool("wrote", wrote).Msg("Block-mode meta not written (tombstone or error)")
			return
		}
		log.Debug().Str("bucket", bucket).Str("key", key).Int("blocks", len(touched)).Int64("block_size", meta.BlockSize).Msg("Block-mode entry populated")
	}()
}
