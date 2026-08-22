package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

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
// Footer size scales with row groups and columns, since it carries per-column
// statistics. Measured on a production deployment with a wide schema, footers ran
// ~1.25% of object size -- several MB on a few-hundred-MB object -- so at a 1 MiB
// block_size the metadata spans several blocks and this fires on most objects.
// Narrow schemas produce much smaller footers; tag_cache_parquet_footer_bytes is
// how a deployment tells which case it is in.
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

	// Trigger labels for the prefetch counters. Precision is compared BETWEEN these,
	// so both the prefetched and the used counter must carry them.
	triggerReadPrefetch = "parquet_footer"
	triggerWriteWarm    = "write_warm"
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

	// Two separate guards, because in-flight coalescing alone does not stop the
	// repeat work. The dedup key is released as soon as the goroutine returns, and
	// for an already-warm object it returns almost immediately -- so every tail read
	// would still spawn a goroutine that re-probes the metadata blocks, which are
	// mostly remote in a cluster. The cooldown suppresses the scan itself for a
	// while after one completes.
	versionKey := bucket + "/" + key + "/" + meta.ETag
	if s.recentFooterWork != nil {
		if _, recent := s.recentFooterWork.Get(versionKey); recent {
			return
		}
	}
	dedupKey := "pq:" + versionKey
	if _, loaded := s.activeBackgroundFetches.LoadOrStore(dedupKey, struct{}{}); loaded {
		return
	}
	go func() {
		defer s.activeBackgroundFetches.Delete(dedupKey)
		// Cooldown ONLY on a completed scan. Recording it unconditionally would apply
		// the full window to a budget shed or a transient fetch failure, turning one
		// shed under load into minutes of silence and more serial footer misses --
		// precisely when the prefetch is most worth retrying.
		if s.prefetchParquetFooterBlocks(bucket, key, accessKey, secretKey, meta, trailer) && s.recentFooterWork != nil {
			s.recentFooterWork.Add(versionKey, struct{}{})
		}
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
// It reports whether the scan ran to completion; a caller may use that to decide
// whether to suppress repeat work, which must not happen after a failure.
func (s *Service) prefetchParquetFooterBlocks(bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, trailer []byte) bool {
	ctx, cancel := context.WithTimeout(context.Background(), backgroundFetchTimeout)
	defer cancel()

	footerLen, ok := s.parquetFooterLength(ctx, bucket, key, meta, trailer)
	if !ok {
		// Usually a key that merely ends in ".parquet" — a stable property, so treat
		// it as complete and stop re-examining the object on every tail read.
		return true
	}
	return s.ensureParquetFooterBlocks(ctx, bucket, key, accessKey, secretKey, meta, footerLen, true /*tailServedByCaller*/, triggerReadPrefetch)
}

// ensureParquetFooterBlocks makes the object's metadata blocks present, and is the
// single place that decides which blocks those are, fetches them, and counts them.
// Both triggers funnel through here: they differ only in how they learn the footer
// length (a served/cached trailer on read, a suffix-range GET on write), never in
// what they then do about it. Keeping that one implementation is deliberate — the
// two paths previously diverged in exactly the bookkeeping that decides whether the
// feature is judged to work.
//
// It reports whether the work completed. A caller may use that to suppress repeat
// scans, which must not happen after a retryable failure.
func (s *Service) ensureParquetFooterBlocks(ctx context.Context, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, footerLen int64, tailServedByCaller bool, trigger string) bool {
	metrics.CacheParquetFooterBytes.Observe(float64(footerLen))

	blocks := parquetFooterBlocks(meta, footerLen, tailServedByCaller)
	if len(blocks) == 0 {
		return true
	}

	// Presence decides the rest of the work, so an object whose metadata blocks are
	// already cached costs nothing beyond the probes.
	absent := make([]int64, 0, len(blocks))
	for _, idx := range blocks {
		if !s.cache.BlockExists(ctx, bucket, key, meta.ETag, meta.BlockSize, idx) {
			absent = append(absent, idx)
		}
	}
	if len(absent) == 0 {
		return true
	}

	// fetchBlocksToCache fetches concurrently, acquires the populate budget
	// non-blocking (so a prefetch sheds rather than queues against real reads), and
	// centralises the stale-meta invalidation that a per-block loop here used to
	// miss: an ETag mismatch means the entry describes a version upstream no longer
	// serves, and leaving it would fail every later read until TTL.
	// Volume, not precision: how much speculative fetching this trigger is doing.
	// Counted before the fetch because that is what was asked for; a block another
	// caller happened to transfer first is close enough for a cost signal, and
	// pinning it exactly cost more than it was worth (see the metric's doc comment).
	metrics.CacheBlockPrefetched.WithLabelValues(trigger).Add(float64(len(absent)))

	if err := s.fetchBlocksToCache(ctx, bucket, key, accessKey, secretKey, meta, absent); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Str("trigger", trigger).
			Msg("Parquet footer blocks not fully cached")
		// Retryable: a shed happens under load, which is when a later read most wants
		// another attempt. Never start a cooldown on this.
		return false
	}
	return true
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

// Write-time footer warming (RFC 0002).
//
// The read-triggered prefetch above can only help a file's SECOND read: the first
// one is what fires it. Under a sliding-window reader -- a dashboard querying
// [now-1h, now] on a refresh timer -- that is the only read that ever misses,
// because every later refresh finds the window already warm. So the remaining
// cold reads are exactly the files written since the last refresh.
//
// TAG proxies those writes, so it can warm them before the first query rather than
// during it. A just-written file is inside the window by definition; this schedules
// a fetch rather than guessing at one.

// warmParquetFooterOnWrite caches a freshly written parquet object's metadata blocks.
// It returns immediately; the work runs detached, after the client's write response
// has already been committed.
func (s *Service) warmParquetFooterOnWrite(r *http.Request, bucket, key string) {
	if s.config == nil || !s.config.Cache.ParquetOptimization || !s.cache.IsEnabled() {
		return
	}
	if !s.config.Cache.IsBlockCachingEnabled() || s.config.Cache.BlockSize <= 0 {
		return
	}
	if !isParquetKey(key) {
		return
	}
	// Warming reads the object back, so it needs credentials that can read it. An
	// anonymous write tells us nothing about read access, so skip rather than guess.
	_, accessKey, secretKey, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil || accessKey == "" || secretKey == "" {
		return
	}

	// One warm per object in flight, reusing the read-path coalescer, so a retried
	// CompleteMultipartUpload does not warm twice. Deliberately NOT keyed by version:
	// the ETag is only learned from the trailer read below, so it cannot be in the
	// key. A second write landing mid-warm is therefore skipped, and that version is
	// warmed by the read-triggered path on its first read instead -- one cold read,
	// not a permanent gap.
	dedupKey := "pqw:" + bucket + "/" + key
	if _, loaded := s.activeBackgroundFetches.LoadOrStore(dedupKey, struct{}{}); loaded {
		return
	}
	go func() {
		defer s.activeBackgroundFetches.Delete(dedupKey)
		s.warmParquetFooterBlocks(bucket, key, accessKey, secretKey)
	}()
}

// warmParquetFooterBlocks resolves the object's size, ETag and metadata length with a
// single suffix-range read, then populates the blocks the metadata spans.
func (s *Service) warmParquetFooterBlocks(bucket, key, accessKey, secretKey string) {
	ctx, cancel := context.WithTimeout(context.Background(), backgroundFetchTimeout)
	defer cancel()

	// Stamp BEFORE the upstream trailer read, not after: an invalidation landing
	// during that round trip must be newer than this timestamp or it will not block
	// the meta write below. That is not theoretical -- blocks are ETag-keyed and
	// survive the meta delete, and runBlockFetch short-circuits on BlockExists
	// without contacting upstream, so a warm whose footer blocks are all still
	// cached performs NO upstream validation and would happily re-publish meta for
	// a deleted object. meta_on_write.go and write_through.go both stamp before
	// their HEAD for the same reason.
	writeStartTime := time.Now().UnixNano()

	meta, footerLen, ok := s.readParquetTrailerFromUpstream(ctx, bucket, key, accessKey, secretKey)
	if !ok {
		return
	}
	if !s.ensureParquetFooterBlocks(ctx, bucket, key, accessKey, secretKey, meta, footerLen, false /*tailServedByCaller*/, triggerWriteWarm) {
		// Blocks did not land, so publishing the entry would advertise a block-mode
		// object with nothing behind it that this write put there.
		return
	}

	// Meta last, tombstone-aware -- the RFC 0001 visibility gate. Blocks stay useful
	// even if this backs off, since they are keyed by ETag.
	s.finalizeBlockModeMeta(ctx, bucket, key, meta, 0, writeStartTime)
}

// readParquetTrailerFromUpstream fetches the object's last 8 bytes. A suffix range is
// used deliberately: it needs no prior knowledge of the object's size, and the 206's
// Content-Range reports that size back -- which is what lets this run for an object
// TAG has never seen, without a HEAD and without consulting a manifest.
func (s *Service) readParquetTrailerFromUpstream(ctx context.Context, bucket, key, accessKey, secretKey string) (*cache.CachedObjectMeta, int64, bool) {
	resp, err := s.forwarder.DoConditionalGetRequest(ctx, bucket, key, accessKey, secretKey, "", 0,
		fmt.Sprintf("bytes=-%d", parquetTrailerSize))
	if err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Parquet footer warm - trailer read failed")
		return nil, 0, false
	}
	if resp.StatusCode != http.StatusPartialContent {
		// The range was ignored and the whole object is coming. Close WITHOUT
		// draining: draining for connection reuse would pull hundreds of MB through
		// the warm path, which is the exact cost the suffix range exists to avoid.
		_ = resp.Body.Close()
		return nil, 0, false
	}
	defer func() {
		// A valid 206 here is 8 bytes and is fully consumed below, so this drains
		// nothing in the normal case. Bounded anyway: an over-long body must not be
		// read to EOF just to make the connection reusable.
		_, _ = io.CopyN(io.Discard, resp.Body, parquetTrailerSize)
		_ = resp.Body.Close()
	}()

	// Trust the interval, not just the total: derive block indices only from a range
	// that really is the object's last parquetTrailerSize bytes. A server that
	// answered a different interval would otherwise have its bytes parsed as a
	// trailer.
	first, last, contentLength, hasBounds := parseContentRange(resp.Header.Get("Content-Range"))
	if !hasBounds || contentLength <= parquetTrailerSize {
		return nil, 0, false
	}
	if first != contentLength-parquetTrailerSize || last != contentLength-1 {
		log.Debug().Str("bucket", bucket).Str("key", key).
			Str("content_range", resp.Header.Get("Content-Range")).
			Msg("Parquet footer warm - upstream answered a different interval than the suffix range")
		return nil, 0, false
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		// The ETag keys every block and the meta entry; without it nothing can be stored.
		return nil, 0, false
	}
	if !s.isBlockEligibleSize(contentLength) {
		// Sub-block objects are whole-cached, and their footer is the whole object.
		return nil, 0, false
	}
	// Same gate every other populate path applies: an object the client marked
	// no-store/private, or one over the size threshold, must not be cached here
	// either. Built from the response headers so Cache-Control is actually seen.
	if probe := s.buildBlockMeta(bucket, key, resp.Header, contentLength); !probe.IsCacheable(s.config.Cache.SizeThreshold) {
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Parquet footer warm skipped - object is not cacheable")
		return nil, 0, false
	}

	buf := make([]byte, parquetTrailerSize)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		return nil, 0, false
	}
	footerLen, valid := parseParquetTrailer(buf, contentLength)
	if !valid {
		// A ".parquet" suffix is a hint, not a guarantee.
		return nil, 0, false
	}

	// Build the entry the same way the range path does, from the 206's headers.
	// A hand-rolled struct would omit StatusCode -- and this meta is the visibility
	// gate, so a later HEAD hit would call WriteHeader(0) and panic net/http -- as
	// well as Content-Type, Last-Modified and user metadata that clients expect.
	return s.buildBlockMeta(bucket, key, resp.Header, contentLength), footerLen, true
}

// parquetFooterBlocks returns the block indices this caller should fetch for the
// metadata region. Empty when there is nothing to do.
//
// tailServedByCaller drops the tail block: the read that fired the prefetch already
// produced it and may still be writing it, so re-fetching would duplicate that
// transfer on every cold open. The cap is applied AFTER that, so it bounds the
// blocks actually fetched rather than a span one of them is about to be removed
// from — otherwise the read trigger would silently prefetch one block fewer than
// the write trigger from the same limit.
func parquetFooterBlocks(meta *cache.CachedObjectMeta, footerLen int64, tailServedByCaller bool) []int64 {
	tailBlock := (meta.ContentLength - 1) / meta.BlockSize
	lastWanted := tailBlock
	if tailServedByCaller {
		lastWanted--
	}
	firstBlock := (meta.ContentLength - parquetTrailerSize - footerLen) / meta.BlockSize
	if firstBlock < 0 {
		firstBlock = 0
	}
	if lastWanted < firstBlock {
		return nil
	}
	if lastWanted-firstBlock+1 > maxParquetFooterPrefetchBlocks {
		firstBlock = lastWanted - maxParquetFooterPrefetchBlocks + 1
	}
	blocks := make([]int64, 0, lastWanted-firstBlock+1)
	for i := firstBlock; i <= lastWanted; i++ {
		blocks = append(blocks, i)
	}
	return blocks
}
