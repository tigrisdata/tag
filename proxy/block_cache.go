package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
	"golang.org/x/sync/errgroup"
)

// maxConcurrentBlockFetches bounds how many missing blocks of one request are fetched from
// upstream concurrently.
const maxConcurrentBlockFetches = 4

// maxRangeBlockFanout caps how many absent blocks a single range read (serve or background
// populate) will fetch as individual aligned GETs. A footer or row-group read touches only a few
// blocks and assembles inline; a pathologically large client range (e.g. most of a multi-hundred-MB
// object in one Range header) exceeds this and is instead served from a single upstream range GET
// with no block population, bounding the per-request upstream fan-out. At the 4 MiB default block
// size this is ~128 MiB of range before the cap engages.
const maxRangeBlockFanout = 32

// errBlockETagMismatch means a block fetched from upstream belongs to a different object
// version than the meta we hold (a concurrent overwrite), so it must not be cached.
var errBlockETagMismatch = errors.New("block etag mismatch")

// errBlockUpstreamGone means a block fetch got a 404 — the object was deleted out of band. Like
// an ETag mismatch, it is an object-level signal that the cached block-mode entry is stale and
// should be invalidated rather than repeatedly retried. A 403 is deliberately NOT included: it is
// principal-level (access denied for these credentials), not proof the object is gone, so it must
// not invalidate an entry shared across principals.
var errBlockUpstreamGone = errors.New("block upstream gone")

// errBlockAssemblyWouldAmplify means ensureBlocksCached bailed (only when the caller opts in via
// bailIfMostlyMissing) because most covering blocks are absent, so per-block assembly would be a
// large upstream amplification versus one streaming fetch. The full-object serve path treats it
// as "fall through to the miss path", not a real error.
var errBlockAssemblyWouldAmplify = errors.New("block assembly would amplify")

// maxInlineFetchesPerServe bounds how many absent blocks one COMMITTED block serve recovers via
// individual aligned upstream fetches. BlocksComplete is a hint: blocks and meta evict/expire
// independently, so a surviving meta over mass-evicted blocks would otherwise turn one full GET
// into N serial upstream range GETs — the exact amplification bailIfMostlyMissing prevents on
// the pre-commit probe path. Past this cap the serve switches to a single uncached upstream
// remainder stream and the degenerate entry is invalidated.
const maxInlineFetchesPerServe = 2

// errBlockStreamDegraded marks a committed block serve that hit a block the cache can no longer
// produce and an inline fetch could not (or should not) recover — but whose remaining bytes
// upstream can still supply. streamBlockRange salvages the response with one uncached upstream
// range GET for the remainder instead of truncating the committed body. Stale signals
// (errBlockETagMismatch / errBlockUpstreamGone) are deliberately NEVER wrapped in it: a
// different object version must not be mixed into an already-committed response.
var errBlockStreamDegraded = errors.New("block stream degraded")

// errBlocksMostlyAbsent wraps errBlockStreamDegraded when the inline-fetch cap was exhausted:
// the entry has lost too many blocks to be worth keeping, so after the remainder salvage the
// serve invalidates it and the next GET re-establishes it via a single streaming re-split.
var errBlocksMostlyAbsent = fmt.Errorf("too many absent blocks: %w", errBlockStreamDegraded)

// isBlockEligibleSize reports whether an object of the given content length is block-cached on
// the read-miss path (RFC 0001): block caching is enabled and the object is at least one block
// (BlockSize is the whole-vs-block boundary — a sub-block object is whole-cached, since blocking
// it would just store a single blob). A negative/unknown length is not eligible. Requiring
// BlockSize > 0 keeps a config that enables block caching but leaves BlockSize at 0 (e.g. a
// Config built directly, bypassing Load's normalization) from dividing by zero in block
// arithmetic. A read miss for a block-eligible object populates blocks, not a whole blob.
func (s *Service) isBlockEligibleSize(contentLength int64) bool {
	return s.config.Cache.BlockCachingEnabled &&
		s.config.Cache.BlockSize > 0 &&
		contentLength >= s.config.Cache.BlockSize
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

// blockLocalRange returns the block-local byte bounds [localStart,localEnd] of the portion of
// the object byte range [start,end] that falls inside block i (clamped to the object end).
func blockLocalRange(i, start, end, blockSize, contentLength int64) (localStart, localEnd int64) {
	bStart, bEnd := blockBounds(i, blockSize, contentLength)
	return max(start, bStart) - bStart, min(end, bEnd) - bStart
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
// (meta.BlockSize > 0). A range no longer than one block — the hot footer/row-group pattern —
// is assembled probe-free into a pooled buffer (serveAssembledRange): each present block is
// read exactly once, with the read itself acting as the existence check. Larger ranges (and
// small ones the assembly-buffer budget declines) take the probe-first path: probe the
// covering blocks, fetch any missing, then stream.
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
		writeRangeNotSatisfiable(w, r, meta, startTime)
		return true, nil
	}
	rng := ranges[0]

	// Probe-free hot path: a range no longer than one block covers at most two blocks, so the
	// requested bytes are assembled into a pooled buffer BEFORE anything is committed. Each
	// present block is read exactly ONCE (the read doubles as the existence probe, halving
	// warm-serve cache ops versus the probe-then-stream path below); a missing block is
	// discovered by that same read and fetched pre-commit, so every failure still leaves the
	// response unwritten for a clean upstream fall-through — identical semantics, fewer ops.
	// The buffer (<= one block) is reserved against the serve-staging byte budget (NOT the
	// populate budget — long-held serve buffers must not crowd out cold-miss populates — and
	// NOT a cacheSemaphore count slot, which bounds concurrent cache WRITES and must not be
	// held across a (possibly slow) client write). On budget decline, serve via the probe path.
	// A fetch failure inside assembly (including a populate-budget decline) returns with the
	// response untouched and the caller forwards upstream — the probe path's own fetch draws
	// on the same populate budget, so retrying there could only repeat the outcome.
	if rangeLen := rng.end - rng.start + 1; rangeLen <= meta.BlockSize {
		weight := s.stagingWeight(rangeLen)
		if s.populateBudget == nil || s.populateBudget.tryAcquireStaging(weight) {
			assembled, aerr := s.serveAssembledRange(ctx, w, bucket, key, accessKey, secretKey, meta, rng, startTime)
			if s.populateBudget != nil {
				s.populateBudget.releaseStaging(weight)
			}
			return assembled, aerr
		}
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Assembly buffer budget declined - serving range via probe path")
	}

	b0, bK := coveringBlocks(rng.start, rng.end, meta.BlockSize)

	// Make the covering blocks present (probe + fetch any missing, coalesced/concurrent). Cap the
	// per-request fan-out (maxRangeBlockFanout): a normal footer/row-group read assembles its few
	// blocks inline, but a pathologically large client range whose covering blocks are mostly
	// absent bails to a single upstream range GET instead of a fetch storm.
	if ferr := s.ensureBlocksCached(ctx, bucket, key, accessKey, secretKey, meta, b0, bK, false, maxRangeBlockFanout); ferr != nil {
		if errors.Is(ferr, errBlockAssemblyWouldAmplify) {
			// Too many absent blocks to assemble; serve this range from a single upstream GET. The
			// entry is still valid (other blocks may be cached) — do NOT invalidate it here.
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Range block assembly would amplify - falling through to single upstream range GET")
			return false, nil
		}
		// Couldn't populate the missing blocks (budget shed, upstream error, or a concurrent
		// overwrite). Nothing written yet — let the caller forward upstream.
		log.Debug().Err(ferr).Str("bucket", bucket).Str("key", key).Msg("Block populate failed - falling through to upstream")
		return false, ferr
	}

	// All covering blocks are present: commit the 206 and stream the assembled range.
	meta.WriteHeaders(w, cache.WithRangeHeaders(rng.start, rng.end, meta.ContentLength))
	writeCacheStatus(w, XCacheHit)
	w.WriteHeader(http.StatusPartialContent)

	if _, berr := s.streamBlockRange(ctx, w, bucket, key, accessKey, secretKey, meta, rng.start, rng.end); berr != nil {
		return true, berr
	}
	metrics.RecordRangeFromCacheHit()
	metrics.RecordRequest("GetObject", "success", time.Since(startTime).Seconds())
	return true, nil
}

// sliceWriter writes into a fixed destination slice, failing on overflow. Used to land a
// block's bytes at their exact offset in a pre-commit assembly buffer.
type sliceWriter struct {
	dst []byte
	off int64
}

func (sw *sliceWriter) Write(p []byte) (int, error) {
	n := copy(sw.dst[sw.off:], p)
	sw.off += int64(n)
	if n < len(p) {
		return n, io.ErrShortBuffer // more bytes than the requested block-local range
	}
	return n, nil
}

// maxPooledBlockBufBytes caps the capacity of block buffers the pool retains, so an entry
// written under an unusually large historical block_size can't pin oversized buffers in the
// pool forever. It starts at the 4 MiB default block size and is raised (monotonically —
// services in one process share the pool) to the configured block_size at service
// construction, so pooling isn't silently disabled for deployments with larger blocks.
var maxPooledBlockBufBytes atomic.Int64

func init() { maxPooledBlockBufBytes.Store(4 << 20) }

// blockBufPool recycles block-sized scratch buffers: pre-commit range/first-block assembly on
// the serve paths, and the block staging buffers on the populate paths (fetchOneBlock,
// putBlocksFromStream). Buffers are handed out by capacity; a request larger than a pooled
// buffer's capacity allocates fresh.
var blockBufPool sync.Pool

func getBlockBuf(n int64) *[]byte {
	if v := blockBufPool.Get(); v != nil {
		bp := v.(*[]byte)
		if int64(cap(*bp)) >= n {
			return bp
		}
		blockBufPool.Put(bp) // too small for this request; keep it for a smaller one
	}
	b := make([]byte, n)
	return &b
}

func putBlockBuf(bp *[]byte) {
	if int64(cap(*bp)) <= maxPooledBlockBufBytes.Load() {
		blockBufPool.Put(bp)
	}
}

// serveAssembledRange serves a single-range request (spanning at most two blocks) by
// assembling the requested bytes from the entry's blocks into a pooled buffer before
// committing anything. Present blocks are read exactly once — the read itself is the
// existence check, there is no separate probe pass — and blocks the read reports absent are
// fetched (coalesced, bounded) and re-read, all BEFORE headers are written. It returns
// served=true only once the 206 is committed; any assembly failure returns served=false with
// the response untouched, so the caller falls through to upstream exactly as the probe-first
// path would. Block hit/miss and range-served metrics are recorded only on a committed
// serve, and a definitive stale signal invalidates the entry — both matching
// ensureBlocksCached.
func (s *Service) serveAssembledRange(
	ctx context.Context,
	w http.ResponseWriter,
	bucket, key, accessKey, secretKey string,
	meta *cache.CachedObjectMeta,
	rng byteRange,
	startTime time.Time,
) (served bool, err error) {
	b0, bK := coveringBlocks(rng.start, rng.end, meta.BlockSize)
	rangeLen := rng.end - rng.start + 1
	bufp := getBlockBuf(rangeLen)
	defer putBlockBuf(bufp)
	buf := (*bufp)[:rangeLen]

	// Pass 1: read every covering block's slice of the range into place. ErrNotFound marks
	// the block missing (exactly what the old probe detected). Any other error is transient —
	// abort with nothing committed. A nil-error short read means the cache delivered fewer
	// bytes than the in-bounds request, which a present block never legitimately does — also
	// treated as transient rather than as a missing block.
	var missing []int64
	off := int64(0)
	for i := b0; i <= bK; i++ {
		localStart, localEnd := blockLocalRange(i, rng.start, rng.end, meta.BlockSize, meta.ContentLength)
		n := localEnd - localStart + 1
		sw := &sliceWriter{dst: buf[off : off+n]}
		rerr := s.cache.GetBlockRangeStream(ctx, bucket, key, meta.ETag, meta.BlockSize, i, localStart, localEnd, sw)
		switch {
		case errors.Is(rerr, cache.ErrNotFound):
			missing = append(missing, i)
		case rerr != nil:
			return false, rerr
		case sw.off != n:
			return false, fmt.Errorf("block %d assembly: short read %d of %d bytes", i, sw.off, n)
		}
		off += n
	}

	// Fetch whatever pass 1 found missing (coalesced across requests, bounded fan-out, stale
	// signals prioritized over transient ones — a stale signal also invalidates the entry
	// inside fetchBlocksToCache), then fill the gaps. Still pre-commit: a fetch or re-read
	// failure falls through with the response untouched.
	if len(missing) > 0 {
		if ferr := s.fetchBlocksToCache(ctx, bucket, key, accessKey, secretKey, meta, missing); ferr != nil {
			return false, ferr
		}
		for _, i := range missing {
			localStart, localEnd := blockLocalRange(i, rng.start, rng.end, meta.BlockSize, meta.ContentLength)
			bStart, _ := blockBounds(i, meta.BlockSize, meta.ContentLength)
			goff := bStart + localStart - rng.start
			n := localEnd - localStart + 1
			sw := &sliceWriter{dst: buf[goff : goff+n]}
			rerr := s.cache.GetBlockRangeStream(ctx, bucket, key, meta.ETag, meta.BlockSize, i, localStart, localEnd, sw)
			if rerr != nil || sw.off != n {
				return false, fmt.Errorf("block %d assembly: fetched block unreadable (err=%v, %d of %d bytes)", i, rerr, sw.off, n)
			}
		}
	}

	// Committed serve: record hit/miss + serve metrics (only here, never on a fall-through),
	// then write the response.
	recordBlockServeMetrics(bK-b0+1, int64(len(missing)))
	meta.WriteHeaders(w, cache.WithRangeHeaders(rng.start, rng.end, meta.ContentLength))
	writeCacheStatus(w, XCacheHit)
	w.WriteHeader(http.StatusPartialContent)
	n, werr := w.Write(buf)
	if n > 0 {
		metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))
	}
	if werr != nil {
		return true, werr
	}
	metrics.RecordRangeFromCacheHit()
	metrics.RecordRequest("GetObject", "success", time.Since(startTime).Seconds())
	return true, nil
}

// streamOutcome describes what a committed block-range stream actually did, so callers can
// record hit/miss metrics from facts instead of assumptions.
type streamOutcome struct {
	fromCache int64 // block slices fully read from cache (no fetch needed)
	fetched   int64 // blocks recovered via an inline upstream fetch-and-cache
	remainder bool  // tail streamed from upstream in one uncached range GET (degraded serve)
}

// streamBlockRange streams object bytes [start,end] (inclusive) from a block-mode entry's
// cached blocks, in order, into w. The caller must have committed the status line + headers.
// A block the cache reports absent (evicted since the caller last saw it) is fetched from
// upstream (coalesced, budget-gated) and re-read — but at most maxInlineFetchesPerServe times
// per serve, since a meta surviving mass block eviction would otherwise turn one committed
// serve into N serial upstream fetches. Past the cap, on a fetch decline, or on any transient
// read failure, the serve is SALVAGED rather than truncated: the remaining bytes stream from
// upstream in a single uncached range GET (streamRemainderFromUpstream), and a cap-tripped
// (degenerate) entry is invalidated so the next GET re-establishes it with one streaming
// re-split. Only stale signals (a different object version — never mixable into a committed
// body), client write failures, and a failed remainder still truncate.
//
// Multi-block serves are PIPELINED: a reader goroutine prefetches block i+1 into a pooled
// buffer while block i's bytes are being written to the client, so the cache read latency and
// the client write latency overlap instead of adding up — the structural stall that made warm
// multi-block GETs trail whole-object serves (whose single contiguous body never waits
// per-block). The pipeline holds at most two block buffers (one draining to the client, one
// filling), reserved against the serve-staging byte budget (never the populate budget — see
// NewService); on decline — and for single-block serves, where there is nothing to overlap —
// it degrades to the direct sequential path.
func (s *Service) streamBlockRange(ctx context.Context, w http.ResponseWriter, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, start, end int64) (out streamOutcome, err error) {
	cw := &countingWriter{w: w}
	defer func() {
		if cw.written > 0 {
			metrics.BytesTransferred.WithLabelValues("out").Add(float64(cw.written))
		}
	}()
	b0, bK := coveringBlocks(start, end, meta.BlockSize)
	if b0 == bK {
		out, err = s.streamBlockRangeSequential(ctx, cw, bucket, key, accessKey, secretKey, meta, start, end)
	} else if weight := s.stagingWeight(2 * meta.BlockSize); s.populateBudget == nil || s.populateBudget.tryAcquireStaging(weight) {
		if s.populateBudget != nil {
			defer s.populateBudget.releaseStaging(weight)
		}
		out, err = s.streamBlockRangePipelined(ctx, cw, bucket, key, accessKey, secretKey, meta, start, end)
	} else {
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Pipeline buffer budget declined - streaming blocks sequentially")
		out, err = s.streamBlockRangeSequential(ctx, cw, bucket, key, accessKey, secretKey, meta, start, end)
	}
	if err == nil {
		return out, nil
	}
	if errors.Is(err, errBlockStreamDegraded) && ctx.Err() == nil && start+cw.written <= end {
		// The committed response can still be completed byte-exact from upstream: cw.written
		// counts exactly the bytes delivered so far (buffered blocks are written whole; a
		// direct-streamed partial block advances it precisely), so the remainder picks up at
		// start+cw.written. Cap-tripped entries are additionally invalidated — they have lost
		// too many blocks to keep serving one recovery at a time.
		absStart := start + cw.written
		log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Int64("from", absStart).Msg("Block serve degraded - streaming remainder from upstream uncached")
		if rerr := s.streamRemainderFromUpstream(ctx, cw, bucket, key, accessKey, secretKey, meta, absStart, end); rerr != nil {
			log.Warn().Err(rerr).Str("bucket", bucket).Str("key", key).Msg("Remainder stream failed - response truncated")
			return out, rerr
		}
		if errors.Is(err, errBlocksMostlyAbsent) {
			s.invalidateStaleBlockMeta(bucket, key, meta.ETag)
		}
		out.remainder = true
		return out, nil
	}
	// Not salvageable: stale signal (version must not be mixed), client write failure, or the
	// client is already gone. Route the noise accordingly — a disconnect is routine.
	if ctx.Err() != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Block stream aborted - client gone")
	} else {
		log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Block stream failed mid-serve")
	}
	return out, err
}

// streamBlockRangePipelined is streamBlockRange's overlapped core: an unbuffered channel
// hands each prefetched block from the reader goroutine to the write loop, so exactly one
// block is filling while one is draining (double buffering — two pooled buffers alive at
// peak, which is what streamBlockRange reserved). Absent blocks are recovered by the reader
// via readBlockSlice (bounded by the shared per-serve fetch cap) before the handoff; the
// first reader error ends the stream in order, exactly where the sequential path would have
// failed. The stop channel unwinds the reader (and returns its in-flight buffer) when the
// write loop exits early on a client write error.
func (s *Service) streamBlockRangePipelined(ctx context.Context, cw *countingWriter, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, start, end int64) (out streamOutcome, err error) {
	b0, bK := coveringBlocks(start, end, meta.BlockSize)
	type blockRead struct {
		bufp      *[]byte
		n         int64
		fetchedIt bool
		err       error
	}
	results := make(chan blockRead)
	stop := make(chan struct{})
	go func() {
		defer close(results)
		fetchesLeft := int64(maxInlineFetchesPerServe)
		for i := b0; i <= bK; i++ {
			localStart, localEnd := blockLocalRange(i, start, end, meta.BlockSize, meta.ContentLength)
			br := blockRead{n: localEnd - localStart + 1}
			br.bufp = getBlockBuf(br.n)
			br.fetchedIt, br.err = s.readBlockSlice(ctx, bucket, key, accessKey, secretKey, meta, i, localStart, localEnd, (*br.bufp)[:br.n], &fetchesLeft)
			select {
			case results <- br:
			case <-stop:
				putBlockBuf(br.bufp)
				return
			}
			if br.err != nil {
				return
			}
		}
	}()
	defer close(stop)
	for br := range results {
		if br.fetchedIt {
			out.fetched++ // counted even when the post-fetch re-read failed: the miss happened
		}
		if br.err != nil {
			putBlockBuf(br.bufp)
			return out, br.err
		}
		if !br.fetchedIt {
			out.fromCache++
		}
		_, werr := cw.Write((*br.bufp)[:br.n])
		putBlockBuf(br.bufp)
		if werr != nil {
			return out, werr
		}
	}
	return out, nil
}

// readBlockSlice reads block idx's local byte range [localStart,localEnd] into dst
// (len(dst) == the range length). A block the cache reports absent is fetched from upstream
// (coalesced, budget-gated) and re-read — dst is scratch, never client-visible, so the retry
// can't duplicate bytes in the response the way a direct-to-client re-read could. A non-nil
// fetchesLeft caps how many absent blocks the serve recovers this way (the shared per-serve
// budget); nil means uncapped (the pre-commit first-block check, where a failure falls
// through instead of truncating). A nil-error short read is reported as an error: a present
// block never legitimately under-delivers an in-bounds range.
//
// Failures that upstream could still satisfy — cap exhausted, fetch declined or transiently
// failed, unreadable after fetch, transient read error — are wrapped in errBlockStreamDegraded
// so a committed caller can salvage the response with one uncached remainder stream. Stale
// signals pass through unwrapped: a different version must never be mixed into a committed
// body (fetchBlocksToCache has already invalidated the entry).
func (s *Service) readBlockSlice(ctx context.Context, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, idx, localStart, localEnd int64, dst []byte, fetchesLeft *int64) (fetched bool, err error) {
	read := func() error {
		sw := &sliceWriter{dst: dst}
		rerr := s.cache.GetBlockRangeStream(ctx, bucket, key, meta.ETag, meta.BlockSize, idx, localStart, localEnd, sw)
		if rerr == nil && sw.off != int64(len(dst)) {
			return fmt.Errorf("block %d stream: short read %d of %d bytes", idx, sw.off, len(dst))
		}
		return rerr
	}
	rerr := read()
	switch {
	case rerr == nil:
		return false, nil
	case errors.Is(rerr, cache.ErrNotFound):
		if fetchesLeft != nil {
			if *fetchesLeft <= 0 {
				return false, errBlocksMostlyAbsent
			}
			*fetchesLeft--
		}
		if ferr := s.fetchBlocksToCache(ctx, bucket, key, accessKey, secretKey, meta, []int64{idx}); ferr != nil {
			if errors.Is(ferr, errBlockETagMismatch) || errors.Is(ferr, errBlockUpstreamGone) {
				return false, ferr
			}
			return false, fmt.Errorf("block %d inline fetch failed (%w): %w", idx, ferr, errBlockStreamDegraded)
		}
		if rerr = read(); rerr != nil {
			return true, fmt.Errorf("block %d unreadable after fetch (%w): %w", idx, rerr, errBlockStreamDegraded)
		}
		return true, nil
	default:
		return false, fmt.Errorf("block %d read failed (%w): %w", idx, rerr, errBlockStreamDegraded)
	}
}

// clientWriteTracker distinguishes write-side failures from cache-read failures on the
// sequential path, where cache bytes stream STRAIGHT into the client connection: an error
// surfaced by GetBlockRangeStream could have originated on either side, and only cache-side
// failures are salvageable by an upstream remainder stream — retrying a broken client write
// from upstream would waste a round trip on a dead connection (and a bytes-plus-error final
// write could even mask a fully delivered body).
type clientWriteTracker struct {
	w        io.Writer
	writeErr error
}

func (t *clientWriteTracker) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if err != nil && t.writeErr == nil {
		t.writeErr = err
	}
	return n, err
}

// streamBlockRangeSequential is streamBlockRange's unbuffered fallback (single-block serves,
// or the pipeline's buffer budget declined): each block streams from cache directly into the
// client writer with no staging buffer. Because the read target IS the response body here, an
// absent block is refetched and re-read only when its failed read delivered nothing — a
// partial write followed by a re-read would duplicate bytes in the response (a partial write
// followed by the remainder salvage is fine: cw.written stays byte-exact).
func (s *Service) streamBlockRangeSequential(ctx context.Context, cw *countingWriter, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, start, end int64) (out streamOutcome, err error) {
	b0, bK := coveringBlocks(start, end, meta.BlockSize)
	fetchesLeft := int64(maxInlineFetchesPerServe)
	tw := &clientWriteTracker{w: cw}
	for i := b0; i <= bK; i++ {
		localStart, localEnd := blockLocalRange(i, start, end, meta.BlockSize, meta.ContentLength)
		before := cw.written
		wasFetched := false
		berr := s.cache.GetBlockRangeStream(ctx, bucket, key, meta.ETag, meta.BlockSize, i, localStart, localEnd, tw)
		if errors.Is(berr, cache.ErrNotFound) && cw.written == before {
			if fetchesLeft <= 0 {
				return out, errBlocksMostlyAbsent
			}
			fetchesLeft--
			if ferr := s.fetchBlocksToCache(ctx, bucket, key, accessKey, secretKey, meta, []int64{i}); ferr != nil {
				if errors.Is(ferr, errBlockETagMismatch) || errors.Is(ferr, errBlockUpstreamGone) {
					return out, ferr
				}
				return out, fmt.Errorf("block %d inline fetch failed (%w): %w", i, ferr, errBlockStreamDegraded)
			}
			out.fetched++
			wasFetched = true
			berr = s.cache.GetBlockRangeStream(ctx, bucket, key, meta.ETag, meta.BlockSize, i, localStart, localEnd, tw)
		}
		if berr != nil {
			// A write-side failure is the client's, not the cache's: return it raw so the
			// caller truncates instead of attempting an upstream remainder salvage.
			if tw.writeErr != nil {
				return out, berr
			}
			return out, fmt.Errorf("block %d stream failed (%w): %w", i, berr, errBlockStreamDegraded)
		}
		if !wasFetched {
			out.fromCache++
		}
	}
	return out, nil
}

// streamRemainderFromUpstream salvages a committed block serve whose entry can no longer
// produce object bytes [absStart,absEnd]: one upstream range GET streams the remaining bytes
// straight to the client, uncached (a pass-through rescue, not a populate — no budget, no
// block writes). The response must be a 206 for exactly the requested range carrying the
// entry's ETag: a different version cannot be mixed into an already-committed body, so a
// changed ETag invalidates the entry and truncates, and a missing one (unverifiable) just
// truncates.
func (s *Service) streamRemainderFromUpstream(ctx context.Context, cw *countingWriter, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, absStart, absEnd int64) error {
	resp, err := s.forwarder.DoConditionalGetRequest(ctx, bucket, key, accessKey, secretKey, "", 0, fmt.Sprintf("bytes=%d-%d", absStart, absEnd))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		s.invalidateStaleBlockMeta(bucket, key, meta.ETag)
		return errBlockUpstreamGone
	}
	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("remainder fetch: unexpected status %d (want 206)", resp.StatusCode)
	}
	respETag := resp.Header.Get("ETag")
	if respETag == "" {
		return fmt.Errorf("remainder fetch: response missing ETag, cannot verify version")
	}
	if respETag != meta.ETag {
		s.invalidateStaleBlockMeta(bucket, key, meta.ETag)
		return errBlockETagMismatch
	}
	if rs, re, _, ok := parseContentRange(resp.Header.Get("Content-Range")); !ok || rs != absStart || re != absEnd {
		return fmt.Errorf("remainder fetch: content-range bounds %d-%d (ok=%t), want %d-%d", rs, re, ok, absStart, absEnd)
	}
	want := absEnd - absStart + 1
	n, cerr := io.Copy(cw, resp.Body)
	if cerr != nil {
		return cerr
	}
	if n != want {
		return fmt.Errorf("remainder fetch: body %d bytes, want %d", n, want)
	}
	return nil
}

// serveFullObjectFromBlockCache serves a full-object GET from a block-mode entry. A complete
// entry (meta.BlocksComplete — every block present when the meta was written) is served
// probe-free: the first block is read into a pooled buffer pre-commit (so a gone-first-block
// still falls through cleanly), then the rest stream with an inline fetch recovering any block
// evicted since populate — one cache op per block instead of the probe pass's two. Entries not
// known complete take the probe-first path (the mostly-missing amplification bail needs the
// up-front missing count) and are promoted to complete after a successful full assembly, so
// they pay the probe pass at most once. It returns served=false, without writing anything, when
// the blocks cannot be produced — the caller then falls through to the miss path. A block-mode
// meta always has ContentLength >= BlockSize >= 1, so there is no zero-byte case to handle here.
func (s *Service) serveFullObjectFromBlockCache(
	ctx context.Context,
	w http.ResponseWriter,
	bucket, key, accessKey, secretKey string,
	meta *cache.CachedObjectMeta,
	startTime time.Time,
) (served bool, err error) {
	lastBlock := (meta.ContentLength - 1) / meta.BlockSize

	// Probe-free fast path for complete entries. The first-block buffer (<= one block) is
	// reserved against the serve-staging byte budget like the assembled-range path; on
	// decline the probe-first path below still serves.
	if meta.BlocksComplete {
		firstLen := min(meta.BlockSize, meta.ContentLength)
		weight := s.stagingWeight(firstLen)
		if s.populateBudget == nil || s.populateBudget.tryAcquireStaging(weight) {
			served, err = s.serveCompleteFromBlocks(ctx, w, bucket, key, accessKey, secretKey, meta, startTime)
			if s.populateBudget != nil {
				s.populateBudget.releaseStaging(weight)
			}
			// Any pre-commit failure (including a budget-declined first-block fetch) returns
			// served=false with the response untouched and the caller forwards upstream — the
			// probe path's fetches draw on the same populate budget, so retrying there could
			// only repeat the outcome.
			return served, err
		}
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Complete-serve buffer budget declined - serving via probe path")
	}

	// Stamp the promotion's write-start BEFORE the assembly work below: any invalidation that
	// lands during it is then provably newer and blocks the promoted meta write.
	writeStartTime := startTime.UnixNano()

	// A full GET must produce every block. bailIfMostlyMissing=true asks ensureBlocksCached to
	// fall through (rather than assemble) when most blocks are absent — e.g. only a footer range
	// was ever cached — since per-block assembly would be a large amplification versus one
	// streaming upstream GET. The miss path then streams the object in a single fetch and, being
	// block-mode, re-splits it. When the object is mostly cached it assembles the few missing.
	if ferr := s.ensureBlocksCached(ctx, bucket, key, accessKey, secretKey, meta, 0, lastBlock, true, 0); ferr != nil {
		if errors.Is(ferr, errBlockAssemblyWouldAmplify) {
			log.Debug().Str("bucket", bucket).Str("key", key).Int64("blocks", lastBlock+1).Msg("Full-object block assembly would amplify - falling through to single upstream GET")
			// Don't leave the mostly-missing entry in place: the bail skips the per-block staleness
			// check, so if the object was deleted/overwritten out of band its already-cached blocks
			// would keep serving stale bytes on later range reads until TTL. Invalidate it (only if
			// still this version, so a concurrently re-established entry isn't wiped) and let the
			// miss path re-establish the current version via a single streaming re-split.
			s.invalidateStaleBlockMeta(bucket, key, meta.ETag)
			return false, nil
		}
		log.Debug().Err(ferr).Str("bucket", bucket).Str("key", key).Msg("Full-object block assembly failed - falling through to upstream")
		return false, ferr
	}

	// Every block is now present: promote the entry to complete (async, tombstone-aware with
	// the pre-assembly timestamp, so a racing invalidation blocks the write) — later full GETs
	// then take the probe-free path instead of re-probing every block. The rewrite carries the
	// entry's REMAINING TTL, never a fresh one: promotion does not consult upstream, so
	// extending the lifetime would reset the staleness clock (up to doubling the configured
	// bound after an out-of-band overwrite) and guarantee a window where the meta outlives
	// every block. Best-effort: a skipped or failed promotion only means the next full GET
	// probes again.
	if !meta.BlocksComplete {
		if remaining := s.remainingMetaTTL(meta); remaining > 0 {
			promoted := *meta
			promoted.BlocksComplete = true
			go func() {
				pctx, cancel := context.WithTimeout(context.Background(), cacheWriteTimeout)
				defer cancel()
				if _, perr := s.cache.PutMetaTombstoneAware(pctx, bucket, key, &promoted, remaining, writeStartTime); perr != nil {
					log.Debug().Err(perr).Str("bucket", bucket).Str("key", key).Msg("Blocks-complete promotion failed")
				}
			}()
		}
	}

	meta.WriteHeaders(w)
	writeCacheStatus(w, XCacheHit)
	w.WriteHeader(meta.StatusCode)

	if _, berr := s.streamBlockRange(ctx, w, bucket, key, accessKey, secretKey, meta, 0, meta.ContentLength-1); berr != nil {
		return true, berr
	}
	metrics.RecordRequest("GetObject", "success", time.Since(startTime).Seconds())
	return true, nil
}

// remainingMetaTTL returns the seconds left of meta's original TTL, from its CachedAt stamp.
// Returns 0 — meaning "do not rewrite" — when the entry is already past its TTL or its age is
// unknown (CachedAt == 0: entries written before the field existed).
func (s *Service) remainingMetaTTL(meta *cache.CachedObjectMeta) int {
	if meta.CachedAt <= 0 {
		return 0
	}
	rem := int64(s.config.Cache.TTL.Seconds()) - (time.Now().Unix() - meta.CachedAt)
	if rem <= 0 {
		return 0
	}
	return int(rem)
}

// serveCompleteFromBlocks serves a full-object GET from a BlocksComplete entry with one cache
// op per block and no probe pass. The first block is read into a pooled buffer BEFORE headers
// commit: if it is absent (evicted since populate) it is fetched pre-commit, and if that fails
// the response is untouched — (false, nil) lets the caller fall through to the miss path, and a
// definitive stale signal invalidates the entry first. After commit, the remaining blocks
// stream with an inline fetch recovering any absent one; only a fetch failure there truncates
// (headers are already sent), which is the same terminal behavior the probe path has for a
// block evicted between probe and stream. Hit/miss metrics count inline-fetched blocks as
// misses, recorded once the serve outcome is known.
func (s *Service) serveCompleteFromBlocks(
	ctx context.Context,
	w http.ResponseWriter,
	bucket, key, accessKey, secretKey string,
	meta *cache.CachedObjectMeta,
	startTime time.Time,
) (served bool, err error) {
	lastBlock := (meta.ContentLength - 1) / meta.BlockSize
	_, firstEnd := blockBounds(0, meta.BlockSize, meta.ContentLength)
	firstLen := firstEnd + 1

	bufp := getBlockBuf(firstLen)
	defer putBlockBuf(bufp)
	buf := (*bufp)[:firstLen]

	// Pre-commit first block via the shared read/recover protocol (uncapped fetch — this
	// doubles as the existence check, and a failure here falls through with the response
	// untouched; a stale signal has already invalidated the entry inside fetchBlocksToCache).
	firstFetched, rerr := s.readBlockSlice(ctx, bucket, key, accessKey, secretKey, meta, 0, 0, firstEnd, buf, nil)
	if rerr != nil {
		return false, rerr
	}

	meta.WriteHeaders(w)
	writeCacheStatus(w, XCacheHit)
	w.WriteHeader(meta.StatusCode)
	out := streamOutcome{}
	if firstFetched {
		out.fetched++
	} else {
		out.fromCache++
	}
	n, werr := w.Write(buf)
	if n > 0 {
		metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))
	}
	var berr error
	if werr != nil {
		berr = werr
	} else if lastBlock > 0 {
		var rest streamOutcome
		rest, berr = s.streamBlockRange(ctx, w, bucket, key, accessKey, secretKey, meta, firstEnd+1, meta.ContentLength-1)
		out.fromCache += rest.fromCache
		out.fetched += rest.fetched
	}
	// Hit/miss reflects what actually happened: blocks read from cache are hits; inline
	// fetches, remainder-streamed bytes, and blocks never reached on an aborted serve all
	// count against the entry — never as hits for blocks that were never read.
	total := lastBlock + 1
	metrics.CacheBlockHits.Add(float64(out.fromCache))
	if miss := total - out.fromCache; miss > 0 {
		metrics.CacheBlockMisses.Add(float64(miss))
	}
	if berr == nil {
		if out.fromCache == total {
			metrics.CacheBlockRangeServed.WithLabelValues("full_hit").Inc()
		} else {
			metrics.CacheBlockRangeServed.WithLabelValues("partial_hit").Inc()
		}
	}
	if berr != nil {
		return true, berr
	}
	metrics.RecordRequest("GetObject", "success", time.Since(startTime).Seconds())
	return true, nil
}

// fetchBlocksToCache fetches the given missing blocks from upstream and writes them to cache,
// concurrently (bounded) and coalesced per block across requests. On any error the caller must
// not serve from cache (some blocks may be absent). It reports a definitive stale-entry signal
// (ETag mismatch / upstream gone) in preference to a transient one: those two are the only
// errors ensureBlocksCached acts on to invalidate, so a fast transient failure of one block
// must not mask a slower stale signal from another (which would leave the stale meta to retry
// until TTL). Blocks are therefore not canceled on a sibling's error — each runs to completion
// so its signal is observed — but the caller's ctx still aborts them (e.g. client disconnect).
func (s *Service) fetchBlocksToCache(ctx context.Context, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, blockIdxs []int64) error {
	var g errgroup.Group
	g.SetLimit(maxConcurrentBlockFetches)
	var mu sync.Mutex
	var stale, transient error
	for _, idx := range blockIdxs {
		g.Go(func() error {
			err := s.fetchOneBlock(ctx, bucket, key, accessKey, secretKey, meta, idx)
			if err == nil {
				return nil
			}
			mu.Lock()
			if errors.Is(err, errBlockETagMismatch) || errors.Is(err, errBlockUpstreamGone) {
				if stale == nil {
					stale = err
				}
			} else if transient == nil {
				transient = err
			}
			mu.Unlock()
			// Return nil so errgroup neither cancels siblings nor collapses to a single
			// error; the prioritized result is chosen from the errors collected above.
			return nil
		})
	}
	_ = g.Wait()
	if stale != nil {
		// A definitive stale signal means the cached meta describes a version upstream no
		// longer serves. Invalidate HERE — every block fetch flows through this point — so no
		// caller can forget it and leave the stale meta to fail again until TTL.
		// invalidateStaleBlockMeta is ETag-guarded and idempotent, so central invocation is
		// safe for every caller.
		log.Debug().Err(stale).Str("bucket", bucket).Str("key", key).Msg("Invalidating stale block-mode meta")
		s.invalidateStaleBlockMeta(bucket, key, meta.ETag)
		return stale
	}
	return transient
}

// recordBlockServeMetrics records per-block hit/miss counts and the full/partial serve
// counter for a committed block serve of total covering blocks, missed of which were fetched.
func recordBlockServeMetrics(total, missed int64) {
	metrics.CacheBlockHits.Add(float64(total - missed))
	if missed == 0 {
		metrics.CacheBlockRangeServed.WithLabelValues("full_hit").Inc()
	} else {
		metrics.CacheBlockMisses.Add(float64(missed))
		metrics.CacheBlockRangeServed.WithLabelValues("partial_hit").Inc()
	}
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
		// A 404 is an object-level "gone" — the cached entry is stale, so signal it for
		// invalidation. A 403 is NOT: it is principal/permission-level (these credentials can't
		// read the object right now), not proof the object is gone, so it must not invalidate the
		// block-mode entry shared across all principals — otherwise one caller's denial would wipe
		// a valid entry for everyone. A 403 falls through to the generic non-206 failure below
		// (fail this fetch, no invalidation); the caller then forwards the request upstream.
		if resp.StatusCode == http.StatusNotFound {
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
		// A block-mode entry is only established from a range response that carried an ETag, so
		// meta.ETag is always set. If a per-block fetch OMITS its ETag we cannot confirm the
		// version: a same-size overwrite would otherwise be stored under the old meta.ETag,
		// mixing versions across blocks. Treat missing-ETag as a transient failure (don't cache,
		// don't invalidate — it's unconfirmed, not a definitive mismatch); the caller falls
		// through to a direct upstream range that serves the current bytes.
		respETag := resp.Header.Get("ETag")
		if respETag == "" {
			return nil, fmt.Errorf("block %d fetch: response missing ETag, cannot verify version", blockIdx)
		}
		if respETag != meta.ETag {
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
		if rs, re, _, ok := parseContentRange(resp.Header.Get("Content-Range")); !ok || rs != bStart || re != bEnd {
			return nil, fmt.Errorf("block %d fetch: content-range bounds %d-%d (ok=%t), want %d-%d", blockIdx, rs, re, ok, bStart, bEnd)
		}
		// Read the full block into memory before storing: the guards above validate the 206's
		// headers, not that the body actually delivered blockLen bytes. A body that ends short
		// (truncated response) streamed straight into PutBlockStream could persist a short block,
		// which BlockExists can't distinguish from a complete one — so it would later serve
		// truncated bytes. io.ReadFull errors on a short body, so a partial block is never stored.
		// blockLen <= block_size, bounded by maxConcurrentBlockFetches concurrent fetches. The
		// buffer is pooled and safely returned on exit: PutBlockStream consumes the reader
		// fully before returning, so nothing references it afterwards.
		bufp := getBlockBuf(blockLen)
		defer putBlockBuf(bufp)
		blockBuf := (*bufp)[:blockLen]
		if _, rerr := io.ReadFull(resp.Body, blockBuf); rerr != nil {
			return nil, fmt.Errorf("block %d fetch: short body (%w), want %d bytes", blockIdx, rerr, blockLen)
		}
		ttl := int(s.config.Cache.TTL.Seconds())
		if perr := s.cache.PutBlockStream(fetchCtx, bucket, key, meta.ETag, meta.BlockSize, blockIdx, bytes.NewReader(blockBuf), ttl); perr != nil {
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

// invalidateStaleBlockMeta deletes the object's cache entry only if the stored meta still carries
// the given (stale) ETag. A request that detected staleness for version X must not wipe a newer
// entry that another request re-established after an out-of-band overwrite (X' != X): deleting that
// fresh entry would force needless re-population and churn under concurrency. There is a small
// GetMeta→Delete window, but this narrows it from "always deletes whatever is stored" to "deletes
// only the version we just observed as stale".
func (s *Service) invalidateStaleBlockMeta(bucket, key, staleETag string) {
	ctx := context.Background()
	if m, found, err := s.cache.GetMeta(ctx, bucket, key); err != nil || !found || m == nil || m.ETag != staleETag {
		return // already gone, or replaced by a newer version — leave it
	}
	s.cache.Delete(ctx, bucket, key)
}

// ensureBlocksCached makes covering blocks [b0,bK] present in cache: it probes the range once,
// fetches any that are missing, and records per-block hits/misses plus whether the serve was a
// full or partial hit. It returns an error only when a missing block could not be populated
// (budget shed, upstream/ETag failure) — the caller then falls through to upstream.
//
// It returns errBlockAssemblyWouldAmplify — without fetching or recording serve metrics — when
// fanning out into per-block fetches would be a large amplification versus one streaming fetch, so
// the caller falls through instead. Two amplification gates: bailIfMostlyMissing (the full-object
// serve path) bails when a MAJORITY of covering blocks are absent (assemble only a mostly-cached
// object, else stream it once); maxFetchFanout>0 (the range serve path) bails on an ABSOLUTE count
// of absent blocks, so a footer/row-group read still assembles its few blocks but a pathologically
// large client range doesn't fan out into hundreds of aligned GETs.
//
// Probing uses BlockExistsErr so a transient probe failure (canceled ctx, cluster gRPC blip) is
// NOT counted as a missing block: it returns that error immediately instead, so a network hiccup
// can't inflate the missing count into a false amplify-bail (which the full-object caller would
// then act on by DELETING a still-valid entry).
func (s *Service) ensureBlocksCached(ctx context.Context, bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, b0, bK int64, bailIfMostlyMissing bool, maxFetchFanout int64) error {
	var missing []int64
	for i := b0; i <= bK; i++ {
		present, perr := s.cache.BlockExistsErr(ctx, bucket, key, meta.ETag, meta.BlockSize, i)
		if perr != nil {
			return perr // transient probe failure — abort without bailing or invalidating
		}
		if !present {
			missing = append(missing, i)
		}
	}
	total := bK - b0 + 1
	// Decided from the single probe above — no second scan. Hit/miss and serve metrics are all
	// recorded ONLY on a committed block-cache serve below, so a bail or a failed fetch (both fall
	// through to upstream, not a block serve) records none — matching CacheBlockRangeServed and
	// avoiding a hit-ratio skew (failed fetches correlate with more-missing requests).
	if (bailIfMostlyMissing && int64(len(missing))*2 > total) ||
		(maxFetchFanout > 0 && int64(len(missing)) > maxFetchFanout) {
		return errBlockAssemblyWouldAmplify
	}
	if len(missing) == 0 {
		recordBlockServeMetrics(total, 0)
		return nil
	}
	if err := s.fetchBlocksToCache(ctx, bucket, key, accessKey, secretKey, meta, missing); err != nil {
		// A definitive stale signal has already invalidated the entry inside
		// fetchBlocksToCache, so the caller's fall-through re-establishes the current state.
		// Transient failures (budget shed, 5xx, upstream blip) leave the still-valid meta in
		// place to retry later.
		return err
	}
	// Committed partial-hit serve: the covering blocks are now all present.
	recordBlockServeMetrics(total, int64(len(missing)))
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

// putBlocksFromStream consumes a full-object body and writes it as fixed-size blocks
// (meta.BlockSize), then stamps the block-mode meta (tombstone-aware) as the visibility gate —
// mirroring the range path's "blocks first, meta last" ordering. It buffers at most one block
// (<= block_size) at a time, so peak memory is bounded regardless of object size. This is the
// shared full-object populate path: block mode is established from ANY full-object fetch (a
// full-GET read miss or a warm-on-write read-back), not only the range path — the whole-vs-block
// boundary is size, not access pattern (RFC 0001).
//
// Each block is read to its EXACT expected length (from blockBounds) and only written if the full
// length arrived: a short read means the upstream body ended before Content-Length (truncated),
// and the meta — plus that (partial) block — is left unwritten. This matters because BlockExists
// only tests presence, not length, so a stored short block would look complete and could serve
// truncated bytes under a committed length (and poison a later range-path populate that trusts
// existing blocks). fetchOneBlock validates block length the same way; this keeps the two block
// writers consistent. A body longer than Content-Length is likewise rejected before the meta.
func (s *Service) putBlocksFromStream(ctx context.Context, bucket, key string, meta *cache.CachedObjectMeta, r io.Reader, ttl int, writeStartTime int64) (err error) {
	// On any early return, drain the rest of r. setupCacheListener feeds this from an io.Pipe; if
	// we stop reading with bytes still queued (a mid-object PutBlockStream error, or an oversize
	// body), the pipe writer goroutine blocks forever on Write, leaking it and never releasing the
	// reserved populate budget. On the short-read/EOF paths r is already drained, so this is a
	// no-op there.
	defer func() {
		if err != nil {
			_, _ = io.Copy(io.Discard, r)
		}
	}()
	// Pooled and safely returned on exit: each PutBlockStream consumes its reader fully
	// before returning, so no block write outlives the loop iteration that staged it.
	bufp := getBlockBuf(meta.BlockSize)
	defer putBlockBuf(bufp)
	buf := (*bufp)[:meta.BlockSize]
	lastBlock := (meta.ContentLength - 1) / meta.BlockSize
	for idx := int64(0); idx <= lastBlock; idx++ {
		bStart, bEnd := blockBounds(idx, meta.BlockSize, meta.ContentLength)
		want := bEnd - bStart + 1
		if _, err := io.ReadFull(r, buf[:want]); err != nil {
			return fmt.Errorf("block split: block %d short read (%w) - upstream body shorter than Content-Length %d", idx, err, meta.ContentLength)
		}
		if perr := s.cache.PutBlockStream(ctx, bucket, key, meta.ETag, meta.BlockSize, idx, bytes.NewReader(buf[:want]), ttl); perr != nil {
			return perr
		}
		metrics.CacheBlockPopulated.Inc()
		metrics.CacheBlockBytesPopulated.Add(float64(want))
	}
	// The body must be exactly Content-Length: a longer stream (malformed upstream) is rejected
	// before the meta so the entry can't claim a length its blocks overrun.
	var probe [1]byte
	if _, err := io.ReadFull(r, probe[:]); err != io.EOF {
		return fmt.Errorf("block split: upstream body longer than Content-Length %d", meta.ContentLength)
	}
	// Every block was just written, so stamp the meta complete: full-object serves can skip
	// the per-block probe pass and stream optimistically.
	meta.BlocksComplete = true
	if _, err := s.cache.PutMetaTombstoneAware(ctx, bucket, key, meta, ttl, writeStartTime); err != nil {
		return err
	}
	return nil
}

// triggerBlockModePopulate populates a block-mode entry in the background after a cold range
// miss: it fetches the blocks the request touched and, only if they all land, writes the
// block-mode meta (tombstone-aware, the visibility gate). It never touches the original
// request or its response body — blocks are fetched with fresh aligned range GETs.
func (s *Service) triggerBlockModePopulate(bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, touched []int64) {
	// Cap the background fan-out (same bound as the range serve): a request that touched a huge
	// number of blocks (a very large client range) would fetch them all as individual aligned GETs,
	// a per-block upstream storm. Skip populating this range — it was already served from upstream,
	// and the object still gets block-cached by later footer/row-group reads that touch few blocks.
	if int64(len(touched)) > maxRangeBlockFanout {
		log.Debug().Str("bucket", bucket).Str("key", key).Int("touched", len(touched)).Msg("Block-mode populate skipped - range touches too many blocks (would amplify)")
		return
	}
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
		// Re-check that no entry was established concurrently before stamping block-mode meta.
		// The schedule-time !found gate can go stale during the block fetch above: a racing
		// full-GET miss may have whole-cached the object. Overwriting that with block-mode meta
		// would demote a complete whole-mode entry to a partial one, so back off and let it win
		// (the touched blocks we fetched are harmless orphans that age out).
		if _, found, _ := s.cache.GetMeta(ctx, bucket, key); found {
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Block-mode meta write skipped - entry established concurrently")
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
