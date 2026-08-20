package proxy

import (
	"context"
	"encoding/binary"
	"slices"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
)

// Parquet-aware footer prefetch (opt-in via cache.parquet_optimization).
//
// A parquet reader cannot read any data before it reads the file's metadata,
// and that metadata lives at the END of the object: the last 8 bytes are a
// 4-byte little-endian metadata length followed by the "PAR1" magic, and the
// metadata occupies the length bytes immediately before them. So a read that
// touches an object's tail block is a reliable signal that the reader is
// opening the file and is about to read the whole metadata region.
//
// The triggering read caches only the TAIL block, and that is a partial block:
// ContentLength mod block_size, averaging half a block. So the prefetch is needed
// whenever footer+8 exceeds that remainder, not merely when it exceeds block_size.
//
// Measured on the ORD parseable bucket (26 objects, two days): footers run ~1.25%
// of object size -- 3.0 MB at 244 MB, 4.7 MB at 394 MB -- so against 1 MiB blocks
// the metadata spans 3-5 blocks and this fires on ~69% of objects. Only the small
// (<2 MB) files, whose footers are 20-50 KB, fit in the remainder.
//
// It fetches only blocks the metadata provably spans, computed from the length the
// file itself declares, so speculation is bounded by the object, not by a guess.
const (
	parquetMagic = "PAR1"

	// parquetTrailerSize is the 4-byte metadata length plus the 4-byte magic.
	parquetTrailerSize = 8

	// maxParquetFooterPrefetchBlocks bounds one prefetch. A metadata region
	// larger than this is pathological (or a corrupt length that passed the
	// sanity checks), and prefetching it would evict more than it can repay.
	maxParquetFooterPrefetchBlocks = 32
)

// isParquetKey reports whether the key names a parquet object. Matching on the
// suffix keeps the format coupling to exactly one place.
func isParquetKey(key string) bool {
	return strings.HasSuffix(strings.ToLower(key), ".parquet")
}

// maybePrefetchParquetFooter starts a background prefetch of the metadata
// blocks preceding the object's tail block, when the just-served range touched
// that tail block. It returns immediately; the fetch runs detached.
//
// Callers pass the range that was SERVED, not the range requested: a serve that
// failed before reaching the tail proves nothing about what the reader does next.
//
// served, when non-nil, is exactly those bytes. It matters on a cold open: the
// assembled path serves a freshly fetched tail from an in-memory lease while its
// cache write is still detached, so reading the trailer back from cache would
// miss and silently skip the prefetch on a reader's FIRST open -- the case this
// exists for. Callers that no longer hold the bytes pass nil.
func (s *Service) maybePrefetchParquetFooter(bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, servedStart, servedEnd int64, served []byte) {
	if !s.parquetFooterPrefetchWanted(key, meta, servedStart, servedEnd) {
		return
	}

	// Copy the trailer out before returning: served is a pooled buffer the caller
	// releases as soon as we return, and the fetch below runs detached.
	var trailer []byte
	if int64(len(served)) == servedEnd-servedStart+1 {
		off := meta.ContentLength - parquetTrailerSize - servedStart
		trailer = append([]byte(nil), served[off:off+parquetTrailerSize]...)
	}

	// Every tail read of a hot object hits this path, so coalesce: one prefetch
	// per object version at a time. Without this a repeatedly-read object would
	// spawn a goroutine and a cache read per request to reach the same
	// already-satisfied conclusion.
	dedupKey := "pq:" + bucket + "/" + key + "/" + meta.ETag
	if _, loaded := s.activeBackgroundFetches.LoadOrStore(dedupKey, struct{}{}); loaded {
		return
	}
	go func() {
		defer s.activeBackgroundFetches.Delete(dedupKey)
		s.prefetchParquetFooterBlocks(bucket, key, accessKey, secretKey, meta, trailer)
	}()
}

// parquetFooterPrefetchWanted reports whether a completed serve is the signal
// this prefetch keys on: the optimization enabled, a parquet key, and a read
// that covered the object's trailer.
func (s *Service) parquetFooterPrefetchWanted(key string, meta *cache.CachedObjectMeta, servedStart, servedEnd int64) bool {
	if s.config == nil || !s.config.Cache.ParquetOptimization {
		return false
	}
	if meta == nil || meta.BlockSize <= 0 || meta.ContentLength <= parquetTrailerSize {
		return false
	}
	if !isParquetKey(key) {
		return false
	}
	// The read must have covered the trailer the metadata length lives in.
	return servedStart <= meta.ContentLength-parquetTrailerSize && servedEnd >= meta.ContentLength-1
}

// prefetchParquetFooterBlocks reads the metadata length the object declares,
// then fetches the metadata blocks that are not already cached.
func (s *Service) prefetchParquetFooterBlocks(bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, trailer []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), backgroundFetchTimeout)
	defer cancel()

	footerLen, ok := s.parquetFooterLength(ctx, bucket, key, meta, trailer)
	if !ok {
		return
	}
	metrics.CacheParquetFooterBytes.Observe(float64(footerLen))

	// The metadata region is the declared length plus the trailer that
	// describes it; the reader fetches both.
	metaStart := meta.ContentLength - parquetTrailerSize - footerLen
	tailBlock := (meta.ContentLength - 1) / meta.BlockSize
	firstBlock := metaStart / meta.BlockSize
	if firstBlock >= tailBlock {
		// Metadata fits the remainder block the triggering read already cached.
		// Measured: only the small (<2 MB) objects land here.
		return
	}
	if tailBlock-firstBlock > maxParquetFooterPrefetchBlocks {
		firstBlock = tailBlock - maxParquetFooterPrefetchBlocks
		log.Debug().Str("bucket", bucket).Str("key", key).Int64("footer_bytes", footerLen).
			Msg("Parquet metadata larger than the prefetch bound - prefetching the tail of it")
	}

	for i := firstBlock; i < tailBlock; i++ {
		if ctx.Err() != nil {
			return
		}
		if s.cache.BlockExists(ctx, bucket, key, meta.ETag, meta.BlockSize, i) {
			continue
		}
		// fetchOneBlock coalesces against any in-flight fetch of the same block
		// and acquires the populate budget non-blocking, so a prefetch is shed
		// rather than queued when the budget is contended by real reads.
		if err := s.fetchOneBlock(ctx, bucket, key, accessKey, secretKey, meta, i); err != nil {
			log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Int64("block", i).
				Msg("Parquet footer prefetch failed")
			return
		}
		metrics.CacheBlockPrefetched.WithLabelValues("parquet_footer").Inc()
		s.notePrefetchedBlock(bucket, key, meta.ETag, meta.BlockSize, i)
	}
}

// parquetFooterLength returns the declared metadata length, preferring a trailer
// the caller already holds over a cache read that can race a detached write.
func (s *Service) parquetFooterLength(ctx context.Context, bucket, key string, meta *cache.CachedObjectMeta, trailer []byte) (int64, bool) {
	if len(trailer) == parquetTrailerSize {
		return parseParquetTrailer(trailer, meta.ContentLength)
	}
	return s.readParquetFooterLength(ctx, bucket, key, meta)
}

// parseParquetTrailer validates the 8-byte trailer and returns the metadata
// length it declares, rejecting anything that cannot describe this object.
func parseParquetTrailer(buf []byte, contentLength int64) (int64, bool) {
	if string(buf[4:]) != parquetMagic {
		return 0, false
	}
	footerLen := int64(binary.LittleEndian.Uint32(buf[:4]))
	if footerLen <= 0 || footerLen > contentLength-parquetTrailerSize {
		return 0, false
	}
	return footerLen, true
}

// readParquetFooterLength reads the object's 8-byte trailer from cache. It
// reports false when the trailer is unreadable or does not describe a parquet
// file, which also covers a key that merely ends in ".parquet".
func (s *Service) readParquetFooterLength(ctx context.Context, bucket, key string, meta *cache.CachedObjectMeta) (int64, bool) {
	tailBlock := (meta.ContentLength - 1) / meta.BlockSize
	blockStart := tailBlock * meta.BlockSize
	trailerStart := meta.ContentLength - parquetTrailerSize
	if trailerStart < blockStart {
		// A trailer straddling two blocks would need a second read; the object
		// is degenerate enough that skipping it costs nothing.
		return 0, false
	}

	buf := make([]byte, parquetTrailerSize)
	localStart := trailerStart - blockStart
	if err := s.readCachedBlockSlice(ctx, bucket, key, meta, tailBlock, localStart, localStart+parquetTrailerSize-1, buf); err != nil {
		// The triggering read cached this block, so a failure here means it was
		// evicted or the node lost it: no reason to speculate further.
		return 0, false
	}
	return parseParquetTrailer(buf, meta.ContentLength)
}

// notePrefetchedBlock records a speculatively fetched block so that a later
// serve of it can be attributed. The set is bounded and TTL-expiring: an entry
// that ages out simply stops being attributable, which understates precision
// rather than overstating it.
func (s *Service) notePrefetchedBlock(bucket, key, etag string, blockSize, idx int64) {
	if s.prefetchedBlocks == nil {
		return
	}
	s.prefetchedBlocks.Add(cache.MakeBlockKey(bucket, key, etag, blockSize, idx), struct{}{})
}

// notePrefetchHit attributes a cache hit to an earlier prefetch, once. Removing
// the entry keeps the ratio a count of blocks rather than of reads, so a block
// read many times cannot inflate precision.
func (s *Service) notePrefetchHit(bucket, key, etag string, blockSize, idx int64) {
	if s.prefetchedBlocks == nil {
		return
	}
	// Remove reports whether the key was present and clears it under one lock, so
	// two serves racing on the same block cannot both attribute it. A Get-then-
	// Remove pair would let precision exceed 1.
	if s.prefetchedBlocks.Remove(cache.MakeBlockKey(bucket, key, etag, blockSize, idx)) {
		metrics.CacheBlockPrefetchUsed.Inc()
	}
}

// noteAssembledPrefetchHits attributes the covering blocks that an assembled serve
// read straight from cache. missing lists the blocks it had to fetch, which by
// definition were not prefetch hits. The slice holds at most two entries, so the
// linear scan is cheaper than building a set.
func (s *Service) noteAssembledPrefetchHits(bucket, key string, meta *cache.CachedObjectMeta, b0, bK int64, missing []int64) {
	if s.prefetchedBlocks == nil {
		return
	}
	for i := b0; i <= bK; i++ {
		if slices.Contains(missing, i) {
			continue
		}
		s.notePrefetchHit(bucket, key, meta.ETag, meta.BlockSize, i)
	}
}
