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

// errBlockUpstreamGone means a block fetch got a definitive "not there for us" status (404
// object deleted, or 403 access revoked). Like an ETag mismatch, it signals the cached
// block-mode entry is stale and should be invalidated rather than repeatedly retried.
var errBlockUpstreamGone = errors.New("block upstream gone")

// isBlockEligibleSize reports whether an object of the given content length is handled by
// block-mode caching (RFC 0001) rather than whole-object caching: block caching is enabled,
// the block size is valid, and the object is at or above the block-mode boundary. A
// negative/unknown length is not eligible. Requiring BlockSize > 0 here means a config that
// enables block caching but leaves BlockSize at 0 (e.g. a Config built directly, bypassing
// Load's normalization) behaves as block-caching-off rather than dividing by zero in block
// arithmetic. Whole-object populate paths (warm-on-write, full-object background fetch,
// full-GET stream caching) skip block-eligible objects — they are cached per-block on read.
func (s *Service) isBlockEligibleSize(contentLength int64) bool {
	return s.config.Cache.BlockCachingEnabled &&
		s.config.Cache.BlockSize > 0 &&
		contentLength >= s.config.Cache.BlockCacheMinSize
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
		if berr := s.cache.GetBlockRangeStream(ctx, bucket, key, meta.ETag, blockSize, i, localStart, localEnd, cw); berr != nil {
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
		if berr := s.cache.GetBlockRangeStream(ctx, bucket, key, meta.ETag, blockSize, i, 0, bEnd-bStart, cw); berr != nil {
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
// The shared fetch (the singleflight leader) runs under a DETACHED, timeout-bounded context,
// not the caller's: binding it to one caller's context would let that caller's cancellation
// fail the block for every waiter. But the caller does not block on it indefinitely — via
// DoChan we select on the caller's ctx, so a caller that goes away (e.g. a disconnected
// client on the synchronous serve path) stops waiting immediately and returns, while the
// detached fetch keeps running to populate the cache for any other waiters.
func (s *Service) fetchOneBlock(ctx context.Context, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, blockIdx int64) error {
	blockKey := cache.MakeBlockKey(bucket, key, meta.ETag, meta.BlockSize, blockIdx)
	ch := s.blockFetch.DoChan(blockKey, func() (interface{}, error) {
		fetchCtx, cancel := context.WithTimeout(context.Background(), backgroundFetchTimeout)
		defer cancel()

		// Re-check presence inside the singleflight leader: a prior fetch may have landed.
		if s.cache.BlockExists(fetchCtx, bucket, key, meta.ETag, meta.BlockSize, blockIdx) {
			return nil, nil
		}
		bStart, bEnd := blockBounds(blockIdx, meta.BlockSize, meta.ContentLength)
		blockLen := bEnd - bStart + 1

		// Reserve populate budget by the block size, clamped via populateWeight so a block
		// larger than the whole budget still acquires (one-at-a-time) instead of being shed
		// forever. Non-blocking read-miss: on decline the block isn't cached and the caller
		// falls through to a direct upstream range.
		reserveWeight := s.populateWeight(blockLen)
		if !s.acquireCacheSlot(fetchCtx, reserveWeight, priorityReadMiss) {
			metrics.RecordCachePopulateSkipped(priorityReadMiss.metricSource())
			return nil, errCachePopulateDeclined
		}
		defer s.releaseCacheSlot(reserveWeight)

		resp, rerr := s.forwarder.DoConditionalGetRequest(fetchCtx, bucket, key, accessKey, secretKey, "", 0, fmt.Sprintf("bytes=%d-%d", bStart, bEnd))
		if rerr != nil {
			return nil, rerr
		}
		defer resp.Body.Close()
		// A definitive "gone for us" status (404 deleted, 403 access revoked) means the cached
		// entry is stale — signal it so the caller invalidates rather than retrying every read.
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
			return nil, errBlockUpstreamGone
		}
		// A block fetch must be a 206 whose body is exactly the requested block bytes. A 200
		// means upstream ignored the Range and returned the whole object (whose byte 0 is the
		// object start, not this block's offset), and a length that differs from the requested
		// block means the 206 bounds don't match — storing either would corrupt block offsets.
		// Reject both; the caller falls through to a direct upstream range.
		if resp.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf("block %d fetch: unexpected status %d (want 206)", blockIdx, resp.StatusCode)
		}
		// ETag guard FIRST: the block must belong to the exact version meta describes. Checking
		// this before the length/bounds guards ensures an out-of-band overwrite (which changes
		// the ETag, and may also change the touched block's length) is reported as
		// errBlockETagMismatch — so ensureBlocksCached invalidates the stale entry — rather than
		// masked by a plain length/bounds error that leaves the stale meta to retry until TTL.
		if respETag := resp.Header.Get("ETag"); respETag != "" && respETag != meta.ETag {
			return nil, errBlockETagMismatch
		}
		// Same version (ETag matches): the response length must be exactly the requested block.
		// This rejects a 200 (whole object) and any 206 whose length differs — including an
		// unknown/chunked length (ContentLength < 0), which we can't validate and so must not
		// store (a short/long body would be served at fixed block-local offsets, corrupting
		// reads). S3/Tigris always set Content-Length on a 206, so a well-formed fetch passes.
		if resp.ContentLength != blockLen {
			return nil, fmt.Errorf("block %d fetch: content-length %d, want %d", blockIdx, resp.ContentLength, blockLen)
		}
		// Validate the 206's Content-Range bounds match the block we asked for. A same-length
		// response at a different offset (a non-conformant upstream) would otherwise be stored
		// under this block index and served at the wrong object offset.
		if rs, re, ok := parseContentRangeBounds(resp.Header.Get("Content-Range")); !ok || rs != bStart || re != bEnd {
			return nil, fmt.Errorf("block %d fetch: content-range bounds %d-%d (ok=%t), want %d-%d", blockIdx, rs, re, ok, bStart, bEnd)
		}
		ttl := int(s.config.Cache.TTL.Seconds())
		if perr := s.cache.PutBlockStream(fetchCtx, bucket, key, meta.ETag, meta.BlockSize, blockIdx, resp.Body, ttl); perr != nil {
			return nil, perr
		}
		metrics.CacheBlockPopulated.Inc()
		metrics.CacheBlockBytesPopulated.Add(float64(blockLen))
		return nil, nil
	})
	select {
	case res := <-ch:
		return res.Err
	case <-ctx.Done():
		// The caller gave up (e.g. client disconnected). Stop waiting and return promptly; the
		// detached fetch above keeps running to populate the cache for any remaining waiters.
		return ctx.Err()
	}
}

// ensureBlocksCached makes covering blocks [b0,bK] present in cache: it probes each (recording
// per-block hits/misses), fetches any that are missing, and records whether the request was a
// full hit or a partial hit. It returns an error only when a missing block could not be
// populated (budget shed, upstream/ETag failure) — the caller then falls through to upstream.
func (s *Service) ensureBlocksCached(ctx context.Context, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, b0, bK int64) error {
	var missing []int64
	for i := b0; i <= bK; i++ {
		if s.cache.BlockExists(ctx, bucket, key, meta.ETag, meta.BlockSize, i) {
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
		// A definitive "stale entry" signal — the cached version was overwritten (ETag
		// mismatch) or the object is gone/forbidden (404/403) out of band — means the cached
		// block-mode meta is stale. Invalidate it so the caller's fall-through re-establishes
		// the current state, instead of repeating this failure on every read until the meta
		// TTL expires. Transient failures (budget shed, 5xx, upstream blip) leave the
		// still-valid meta in place to retry later.
		if errors.Is(err, errBlockETagMismatch) || errors.Is(err, errBlockUpstreamGone) {
			log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Invalidating stale block-mode meta")
			s.cache.Delete(context.Background(), bucket, key)
		}
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
