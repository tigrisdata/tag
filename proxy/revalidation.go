package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
)

// revalidateAndServe sends a conditional GET to upstream and serves the result.
// Supports both full-object and range requests. For range requests, the Range header
// is included in the conditional GET so upstream returns 304 or 206.
// On 304 Not Modified: serves from cached body (or range).
// On 200/206 (changed): streams new body to client and updates cache.
// On error: serves stale data from cache as fallback.
func (s *Service) revalidateAndServe(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key, accessKey, secretKey string,
	meta *cache.CachedObjectMeta,
	start time.Time,
) error {
	metrics.RecordRevalidationTriggered()

	rangeHeader := r.Header.Get("Range")

	log.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Str("etag", meta.ETag).
		Str("range", rangeHeader).
		Msg("Revalidating cached object with upstream")

	// Send conditional GET to upstream (includes Range header if present).
	// Uses the parent context (not a separate timeout) to avoid truncating body
	// streaming for large objects. The httpClient has its own 5-minute timeout.
	resp, err := s.forwarder.DoConditionalGetRequest(ctx, bucket, key, accessKey, secretKey, meta.ETag, meta.LastModified, rangeHeader)
	if err != nil {
		// Upstream error — serve stale from cache
		log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Revalidation failed, serving stale")
		metrics.RecordRevalidationFailed()
		served, staleErr := s.serveStaleFromCache(ctx, w, r, bucket, key, meta, rangeHeader, start)
		if served {
			metrics.RecordRevalidationStaleServed()
			return staleErr
		}
		// No stale bytes available (versioned body gone) and nothing written yet —
		// last-resort direct fetch so the client gets a real response.
		return s.forwardAfterCacheMiss(ctx, w, r, bucket, key, meta.ETag, staleErr, start)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotModified:
		served, revalErr := s.handleRevalidation304(ctx, w, r, bucket, key, meta, rangeHeader, start)
		if served {
			// A response (full body, range, or a committed-then-failed stream) was
			// produced; propagate its result. Headers are already sent on error, so
			// we cannot safely forward.
			return revalErr
		}
		// Cache body unavailable despite 304 and no response bytes written yet —
		// fetch from upstream. The Range header (if any) is still on r, so upstream
		// returns the correct 206; this unifies the full-object and range paths.
		log.Warn().Err(revalErr).Str("bucket", bucket).Str("key", key).
			Msg("Revalidation 304 cache body unavailable, fetching from upstream")
		return s.forwardAfterCacheMiss(ctx, w, r, bucket, key, meta.ETag, revalErr, start)
	case http.StatusOK:
		// Full-object response (no range or upstream ignored range)
		return s.handleRevalidation200(ctx, w, bucket, key, meta.ETag, resp, start)
	case http.StatusPartialContent:
		// Range response — object changed, upstream returned only the requested range
		return s.handleRevalidation206Range(ctx, w, r, bucket, key, accessKey, secretKey, meta.ETag, resp, start)
	default:
		// Unexpected status (4xx, 5xx) — drain body for connection reuse, serve stale
		io.Copy(io.Discard, resp.Body)
		log.Warn().
			Int("status", resp.StatusCode).
			Str("bucket", bucket).
			Str("key", key).
			Msg("Revalidation got unexpected status, serving stale")
		metrics.RecordRevalidationFailed()
		served, staleErr := s.serveStaleFromCache(ctx, w, r, bucket, key, meta, rangeHeader, start)
		if served {
			metrics.RecordRevalidationStaleServed()
			return staleErr
		}
		return s.forwardAfterCacheMiss(ctx, w, r, bucket, key, meta.ETag, staleErr, start)
	}
}

// forwardAfterCacheMiss fetches the object fresh from upstream when a
// revalidation or stale-serve path could not produce any response bytes from
// cache (typically the ETag-versioned body was evicted while its metadata
// survived). No response headers have been written yet, so this is safe for both
// full-object and range requests — the Range header on r makes upstream return
// the correct 206.
// bodyErr is why the cache could not serve; it decides whether the metadata is
// invalidated (see bodyGone).
func (s *Service) forwardAfterCacheMiss(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key, staleETag string, bodyErr error, start time.Time) error {
	// Invalidate the metadata only when its body is genuinely gone. Otherwise the
	// meta entry survives and every subsequent request is a meta-hit whose body
	// probe fails and re-forwards to upstream — a persistent cold-miss loop,
	// because the normal re-warm paths are gated on the metadata being absent.
	// Deleting it makes the next request a clean miss that repopulates the cache.
	// A transient failure (e.g. a canceled context from a disconnected client) must
	// not evict a still-valid entry, so bodyGone gates the delete.
	if s.cache.IsEnabled() && bodyGone(bodyErr) {
		// ETag-guarded: only the entry whose body was observed gone is removed,
		// never one a concurrent request re-established under a newer version.
		s.invalidateStaleMeta(bucket, key, staleETag)
	}
	writeCacheStatus(w, XCacheMiss)
	forwardErr := s.forwarder.Forward(ctx, w, r)
	status := "success"
	if forwardErr != nil {
		status = "error"
	}
	metrics.RecordRequest("GetObject", status, metrics.SourceUpstream, time.Since(start).Seconds())
	return forwardErr
}

// handleRevalidation304 handles a 304 Not Modified revalidation response.
// Serves from cached body (or range if requested). Does not refresh cache TTL
// because ocache has no TTL-refresh operation and re-writing data is inefficient.
// It returns served=true when a client response was produced (a full body, a
// range, or a committed stream that then failed). It returns served=false,
// without writing any response bytes, when the cached body cannot be resolved —
// the caller then fetches from upstream.
func (s *Service) handleRevalidation304(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
	meta *cache.CachedObjectMeta,
	rangeHeader string,
	start time.Time,
) (served bool, err error) {
	metrics.RecordRevalidationNotModified()
	log.Debug().Str("bucket", bucket).Str("key", key).Msg("Revalidation 304 - object unchanged")

	// Serve range or full body from cache. Both helpers commit only after a
	// nonempty body write and report an unresolvable body before committing, so
	// the caller can safely forward to upstream.
	if rangeHeader != "" {
		return s.serveRangeFromCache(ctx, w, r, bucket, key, meta, rangeHeader, start)
	}
	if bodyErr := s.serveFromCache(ctx, w, bucket, key, meta, start); bodyErr != nil {
		return false, bodyErr
	}
	return true, nil
}

// revalidationExpectedVersion picks the meta-write precondition for a
// revalidation repopulate. The guarded delete that precedes the repopulate is
// best-effort, so "expect absent" alone is wrong: a transiently failed delete
// leaves the KNOWN-STALE row in place, and put-if-absent would then refuse the
// replacement and keep serving stale data. Instead:
//   - entry absent → 0 (put-if-absent);
//   - the observed stale row still present → its version (the replacement
//     overwrites exactly that row; if it moves first, the newer write wins);
//   - anything else present → a racer already re-established a fresh entry →
//     0, which is guaranteed to mismatch, skipping the write in its favor.
//
// A read failure returns 0 as well: refusing to overwrite is the safe
// direction when the store cannot be consulted, and the entry converges via
// the next revalidation or TTL.
func (s *Service) revalidationExpectedVersion(bucket, key, staleETag string) (uint64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cur, version, found, err := s.cache.GetMetaWithVersion(ctx, bucket, key)
	if err != nil {
		// No token, no ordered commit — expected=0 is the legacy unordered
		// put-if-absent and could publish over a fence. Skip the repopulate;
		// the client is still served and the entry heals on a later read.
		return 0, false
	}
	if !found || cur == nil {
		// Absent: use the absence TOKEN (ocache v1.13.0), not 0 — the repopulate
		// is then ordered against a fenced delete landing after this look.
		return version, true
	}
	if cur.ETag == staleETag {
		return version, true
	}
	// A fresh racer holds the key: skip the write in its favor. There is no
	// precondition that fails in every future — 0 mismatches the live row, but
	// if the racer is fenced-deleted before our (asynchronous) commit, 0
	// becomes legacy put-if-absent over absence and would publish our
	// pre-fetch bytes over that fence.
	return 0, false
}

// handleRevalidation200 handles a 200 OK revalidation response (object changed).
// Streams the new body to the client while simultaneously updating the cache.
func (s *Service) handleRevalidation200(
	ctx context.Context,
	w http.ResponseWriter,
	bucket, key, staleETag string,
	resp *http.Response,
	start time.Time,
) error {
	metrics.RecordRevalidationUpdated()
	log.Debug().Str("bucket", bucket).Str("key", key).Msg("Revalidation 200 - object changed, streaming new data")

	// Build new metadata from upstream response
	newMeta := cache.MetaFromHTTPHeaders(bucket, key, resp.StatusCode, resp.Header)

	// Check if the new object should be cached
	shouldCache := newMeta.IsCacheable(s.config.Cache.SizeThreshold) &&
		s.cache.IsEnabled() &&
		!s.hasNoCacheHeaders(resp.Header)

	// Always delete the stale cache entry when upstream confirms the object
	// changed. Even if the new version is uncacheable (too large, no-store), the
	// old cached version is known-stale and must not be served to future
	// requests. ETag-guarded: if a concurrent request already replaced the entry
	// (with the fresh version), that newer entry is left in place.
	s.invalidateStaleMeta(bucket, key, staleETag)

	// The repopulate's precondition is read AFTER the guarded delete, so our
	// own invalidation doesn't block the write, while a concurrent DELETE
	// arriving later bumps the fence past it and correctly does. It must be
	// read BEFORE the not-cacheable early-return below: a failed token read
	// downgrades this response to stream-only — never a commit with the
	// legacy unordered expected=0.
	expected, tokenOK := s.revalidationExpectedVersion(bucket, key, staleETag)
	shouldCache = shouldCache && tokenOK

	// Write response headers to client
	copyHeaders(w.Header(), resp.Header)
	writeCacheStatus(w, XCacheRevalidated)
	w.WriteHeader(resp.StatusCode)

	if !shouldCache {
		// Not cacheable — just stream to client
		n, copyErr := io.Copy(w, resp.Body)
		metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))
		status := "success"
		if copyErr != nil {
			status = "error"
		}
		metrics.RecordRequest("GetObject", status, metrics.SourceUpstream, time.Since(start).Seconds())
		return copyErr
	}

	// Stream to client AND cache simultaneously via TeeReader + pipe
	pr, pw := io.Pipe()
	ttl := int(s.config.Cache.TTL.Seconds())

	// Background goroutine: write to cache from pipe reader. Block-eligible objects are stored as
	// blocks (size-only mode, RFC 0001) exactly as the read-miss/warm paths do — a revalidated
	// whole-mode entry that grew into a block-eligible object must not be re-stored as one whole
	// blob. Sub-block objects keep the whole-body write.
	cacheErrCh := make(chan error, 1)
	if !s.startBackgroundTask(nil, func(parent context.Context) {
		cacheCtx, cacheCancel := context.WithTimeout(parent, cacheWriteTimeoutForSize(newMeta.ContentLength))
		defer cacheCancel()

		var cacheErr error
		if s.isBlockEligibleSize(newMeta.ContentLength) {
			newMeta.BlockSize = s.config.Cache.BlockSize
			_, cacheErr = s.putBlocksFromStream(cacheCtx, bucket, key, newMeta, pr, ttl, expected)
		} else {
			_, cacheErr = s.cache.PutWithMetaStreamIfVersion(
				cacheCtx, bucket, key, newMeta, pr, ttl, expected,
			)
		}
		if cacheErr != nil {
			if errors.Is(cacheErr, cache.ErrPopulateShed) {
				log.Debug().Str("bucket", bucket).Str("key", key).Msg("Cache write shed during revalidation update: owner over disk budget")
			} else {
				log.Warn().Err(cacheErr).Str("bucket", bucket).Str("key", key).Msg("Cache write failed during revalidation update")
			}
			// Drain remaining pipe data so the foreground TeeReader doesn't block. Without this,
			// io.Pipe's zero-buffer causes pw.Write to hang. (putBlocksFromStream also drains on
			// error; this covers the whole-body path and is harmless when already drained.)
			io.Copy(io.Discard, pr)
		}
		cacheErrCh <- cacheErr
	}) {
		_ = pr.CloseWithError(context.Canceled)
		_ = pw.CloseWithError(context.Canceled)
		// Service shutdown has already closed worker admission. Keep the already
		// committed upstream response independent of cache availability.
		n, copyErr := io.Copy(w, resp.Body)
		metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))
		metrics.RecordRequestResult("GetObject", metrics.SourceUpstream, copyErr, time.Since(start).Seconds())
		return copyErr
	}

	// Foreground: stream to client via TeeReader (also writes to pipe for cache)
	teeReader := io.TeeReader(resp.Body, pw)
	n, copyErr := io.Copy(w, teeReader)
	metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))

	// Close pipe writer to signal EOF to cache reader
	if copyErr != nil {
		pw.CloseWithError(copyErr)
	} else {
		pw.Close()
	}

	// Wait for cache write to complete (with timeout)
	select {
	case cacheErr := <-cacheErrCh:
		if cacheErr != nil {
			log.Debug().Err(cacheErr).Str("bucket", bucket).Str("key", key).Msg("Cache write error after revalidation")
		}
	case <-time.After(cacheWriteTimeoutForSize(newMeta.ContentLength)):
		log.Warn().Str("bucket", bucket).Str("key", key).Msg("Cache write timeout after revalidation")
	}

	status := "success"
	if copyErr != nil {
		status = "error"
	}
	metrics.RecordRequest("GetObject", status, metrics.SourceUpstream, time.Since(start).Seconds())
	return copyErr
}

// errCacheBodyLengthMismatch marks a cached whole body whose byte count differs
// from the Content-Length its metadata advertises (a truncated populate, or a
// body overwritten in place). bodyGone treats it as an unusable body, so the
// pre-commit callers invalidate and refetch exactly as for a missing body.
var errCacheBodyLengthMismatch = errors.New("cache body length mismatch")

// checkCachedBodyLength compares the bytes a cache body produced with the
// metadata's Content-Length. A negative ContentLength (unknown upstream
// length) has nothing to compare against. For a body cut off mid-stream by
// lengthBoundedWriter, got is the count the body would have reached, so it is
// a lower bound rather than the stored size.
func checkCachedBodyLength(bucket, key string, meta *cache.CachedObjectMeta, got int64) error {
	if meta.ContentLength < 0 || got == meta.ContentLength {
		return nil
	}
	metrics.RecordCacheOperation("get", "length_mismatch")
	return fmt.Errorf("%w for %s/%s: body is %d bytes, metadata says %d",
		errCacheBodyLengthMismatch, bucket, key, got, meta.ContentLength)
}

// lengthBoundedWriter sits in front of the streamed path's committing writer
// and keeps the response from ever satisfying meta.ContentLength unless the
// cache stream ends cleanly with exactly that many bytes.
//
// Two mechanisms combine for that:
//
//   - A chunk that would carry the body past ContentLength is refused in full,
//     classified as errCacheBodyLengthMismatch. The excess never reaches
//     net/http, whose own Content-Length enforcement would reject it with
//     http.ErrContentLength, which serveFromCache could not tell from a
//     transient stream failure, so the over-long entry would stay cached and
//     fail every later request the same way until its TTL.
//   - The last promised byte (the one that would land at offset
//     ContentLength-1) is withheld, not forwarded. Refusing the excess chunk
//     alone is not enough: when chunks align with ContentLength the client has
//     already received every promised byte by the time the excess arrives, and
//     sees a clean, complete response from an entry detected as corrupt. Only
//     release, called after the stream has ended cleanly with exactly
//     ContentLength bytes, forwards the held byte and completes the response.
//     An over-run, short, or failed stream never releases it, so the declared
//     Content-Length is never met and the client sees an aborted body.
//
// If the very first chunk is already too long, or the whole body fits in the
// single held byte, nothing has committed and the caller falls through to
// upstream. A negative ContentLength (unknown upstream length) is passed
// through unbounded.
type lengthBoundedWriter struct {
	w      io.Writer
	bucket string
	key    string
	meta   *cache.CachedObjectMeta
	// written counts the bytes forwarded to w; the held byte is excluded
	// until release.
	written int64
	held    [1]byte
	holding bool
}

// seen is the number of body bytes the cache stream has delivered so far,
// forwarded or held.
func (bw *lengthBoundedWriter) seen() int64 {
	if bw.holding {
		return bw.written + 1
	}
	return bw.written
}

func (bw *lengthBoundedWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	limit := bw.meta.ContentLength
	if limit < 0 {
		n, err := bw.w.Write(p)
		bw.written += int64(n)
		return n, err
	}
	next := bw.seen() + int64(len(p))
	if next > limit {
		return 0, checkCachedBodyLength(bw.bucket, bw.key, bw.meta, next)
	}
	forward := p
	if next == limit {
		// This chunk carries the last promised byte: keep it back. p may be
		// reused by the cache client once Write returns, so copy the byte.
		forward = p[:len(p)-1]
		bw.held[0] = p[len(p)-1]
		bw.holding = true
	}
	if len(forward) == 0 {
		return len(p), nil
	}
	n, err := bw.w.Write(forward)
	bw.written += int64(n)
	if err != nil {
		return n, err
	}
	return len(p), nil
}

// release forwards the held last byte, completing the response. It is only
// correct to call once the cache stream has ended cleanly with exactly
// meta.ContentLength bytes; serveFromCache checks that first.
func (bw *lengthBoundedWriter) release() error {
	if !bw.holding {
		return nil
	}
	bw.holding = false
	n, err := bw.w.Write(bw.held[:])
	bw.written += int64(n)
	return err
}

// serveFromCache serves an object from the cache body.
// Used as fallback during revalidation (304 or error paths).
// The body is checked against meta.ContentLength on the way out: a short or
// long body is never served as a clean hit. Before headers commit the mismatch
// is returned like any other unusable-body error (callers invalidate via
// bodyGone and fall through to upstream); after commit the response is left
// incomplete, as for any committed-then-failed stream, and the entry is
// invalidated here.
func (s *Service) serveFromCache(
	ctx context.Context,
	w http.ResponseWriter,
	bucket, key string,
	meta *cache.CachedObjectMeta,
	start time.Time,
) error {
	// Zero-byte objects: no body to serve
	if meta.ContentLength == 0 {
		serveMetaHit(w, meta, "GetObject", start)
		return nil
	}

	// Small objects: buffer and serve. The read lands in a buffer, so it is bounded
	// by the size-scaled deadline: a stalled peer fails the serve pre-commit and the
	// caller falls through upstream.
	if meta.ContentLength <= smallObjectThreshold {
		ctx, cancel := context.WithTimeout(ctx, cacheReadTimeoutForSize(meta.ContentLength))
		defer cancel()
		bodyBuf := bufferPool.Get().(*bytes.Buffer)
		bodyBuf.Reset()

		bodyErr := s.cache.GetBodyStream(ctx, bucket, key, meta.BodyDiscriminator(), bodyBuf)
		if bodyErr == nil && bodyBuf.Len() > 0 {
			// Whole body is buffered, so the check runs before any header is written.
			if lenErr := checkCachedBodyLength(bucket, key, meta, int64(bodyBuf.Len())); lenErr != nil {
				putBuffer(bodyBuf)
				return lenErr
			}
			meta.WriteHeaders(w)
			writeCacheStatus(w, XCacheHit)
			w.WriteHeader(meta.StatusCode)
			n, _ := w.Write(bodyBuf.Bytes())
			metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))
			metrics.RecordRequest("GetObject", "success", metrics.SourceLocal, time.Since(start).Seconds())
			putBuffer(bodyBuf)
			return nil
		}
		putBuffer(bodyBuf)
		// Body unavailable — return error (caller may fall through to upstream)
		if bodyErr != nil {
			return fmt.Errorf("cache body read failed: %w", bodyErr)
		}
		return fmt.Errorf("cache body empty for %s/%s", bucket, key)
	}

	// Large objects: stream directly to a writer that commits only when the
	// cache produces its first nonempty chunk. This preserves the pre-commit
	// fallback for an absent or empty body without staging the stream through a
	// pipe and a second copy.
	// An over-long body is caught chunk by chunk, before the excess reaches
	// the response writer; a short one only once the stream ends cleanly. In
	// both cases the last promised byte, held back by lengthBoundedWriter, is
	// never released, so the client cannot mistake the body for complete.
	// The client paces this read (the response writer is its sink), so a size-scaled
	// bound would punish slow clients; the flat cap matches the server's WriteTimeout.
	ctx, cancel := context.WithTimeout(ctx, cacheStreamReadTimeout)
	defer cancel()
	cw := &lazyCommitWriter{w: w, meta: meta}
	bw := &lengthBoundedWriter{w: cw, bucket: bucket, key: key, meta: meta}
	bodyErr := s.cache.GetBodyStream(ctx, bucket, key, meta.BodyDiscriminator(), bw)
	if bodyErr == nil {
		// The stream ended cleanly: only now is the delivered count meaningful.
		// A failed stream is already incomplete and may be transient, so it is
		// not treated as a corrupt entry.
		bodyErr = checkCachedBodyLength(bucket, key, meta, bw.seen())
	}
	if bodyErr == nil {
		// Exactly the promised bytes arrived: let the held last byte through,
		// which completes the response (and commits it, if the whole body was
		// that one byte).
		bodyErr = bw.release()
	}
	if !cw.committed {
		if bodyErr != nil {
			return fmt.Errorf("cache body unavailable: %w", bodyErr)
		}
		return fmt.Errorf("cache body empty for %s/%s", bucket, key)
	}
	if errors.Is(bodyErr, http.ErrContentLength) {
		// Belt and braces: lengthBoundedWriter should have refused the excess
		// first, but a writer that enforces Content-Length ahead of it reports
		// the same over-long body this way.
		metrics.RecordCacheOperation("get", "length_mismatch")
		bodyErr = fmt.Errorf("%w for %s/%s: %w", errCacheBodyLengthMismatch, bucket, key, bodyErr)
	}
	if bodyErr != nil {
		// The response is already committed. Do not return the error to the
		// caller: HandleGetObject would otherwise try to append an upstream
		// response to the partial cached body. The response is incomplete,
		// however, so record the request as an error. The declared
		// Content-Length is never satisfied, so net/http does not end the
		// body cleanly for the client.
		log.Warn().Err(bodyErr).Str("bucket", bucket).Str("key", key).
			Msg("Failed to stream cache body after headers committed")
		if errors.Is(bodyErr, errCacheBodyLengthMismatch) {
			// The entry itself is bad, not transiently unreadable, and no caller
			// sees a committed failure, so drop it here. ETag/version-guarded:
			// an entry a concurrent request re-established under a newer
			// version is left alone.
			s.invalidateStaleMeta(bucket, key, meta.ETag)
		}
	}

	metrics.BytesTransferred.WithLabelValues("out").Add(float64(cw.written))
	metrics.RecordRequestResult("GetObject", metrics.SourceLocal, bodyErr, time.Since(start).Seconds())
	return nil
}

// handleRevalidation206Range handles a 206 Partial Content revalidation response.
// The object changed and upstream returned only the requested range.
// Streams the range to the client, deletes stale cache, and triggers a background
// full-object fetch to repopulate the cache (same pattern as handleRangeWithBackgroundCache).
func (s *Service) handleRevalidation206Range(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key, accessKey, secretKey, staleETag string,
	resp *http.Response,
	start time.Time,
) error {
	metrics.RecordRevalidationUpdated()
	log.Debug().Str("bucket", bucket).Str("key", key).Msg("Revalidation 206 - object changed, streaming range")

	// Delete the stale cache entry before repopulating (ETag-guarded, as in
	// handleRevalidation200).
	s.invalidateStaleMeta(bucket, key, staleETag)

	// Determine total object size from Content-Range header
	_, _, totalSize, _ := parseContentRange(resp.Header.Get("Content-Range"))

	// Stream range response to client
	copyHeaders(w.Header(), resp.Header)
	writeCacheStatus(w, XCacheRevalidated)
	w.WriteHeader(resp.StatusCode)

	n, copyErr := io.Copy(w, resp.Body)
	metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))

	status := "success"
	if copyErr != nil {
		status = "error"
	}
	metrics.RecordRequest("GetObject", status, metrics.SourceUpstream, time.Since(start).Seconds())

	// Trigger background full-object fetch to repopulate cache
	if totalSize > 0 &&
		totalSize <= s.config.Cache.SizeThreshold &&
		s.cache.IsEnabled() &&
		accessKey != "" && secretKey != "" {
		// Revalidation re-warm is a read-triggered populate. Its precondition
		// comes from the same picker as the 200 path: the guarded delete above
		// is best-effort, and if it failed the fetch must overwrite exactly
		// the surviving known-stale row rather than being refused by
		// put-if-absent.
		if rewarmTok, tokenOK := s.revalidationExpectedVersion(bucket, key, staleETag); tokenOK {
			s.triggerBackgroundCacheFetch(bucket, key, accessKey, secretKey, hasNoAuthCredentials(r), priorityReadMiss, rewarmTok)
		}
	}

	return copyErr
}

// serveStaleFromCache serves stale content from cache, handling both full and
// range requests. It returns served=false, without writing any response bytes,
// when the cached body cannot be resolved so the caller can fall back to a direct
// upstream fetch instead of emitting a truncated or empty response.
func (s *Service) serveStaleFromCache(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
	meta *cache.CachedObjectMeta,
	rangeHeader string,
	start time.Time,
) (served bool, err error) {
	if rangeHeader != "" {
		return s.serveRangeFromCache(ctx, w, r, bucket, key, meta, rangeHeader, start)
	}
	if bodyErr := s.serveFromCache(ctx, w, bucket, key, meta, start); bodyErr != nil {
		return false, bodyErr
	}
	return true, nil
}

// revalidateAndServeHead sends a conditional HEAD to upstream for a HEAD request.
// On 304: serves cached headers (no body).
// On 200: serves new headers from upstream and invalidates stale cache.
// On error: serves stale headers from cache.
func (s *Service) revalidateAndServeHead(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key, accessKey, secretKey string,
	meta *cache.CachedObjectMeta,
	start time.Time,
) error {
	metrics.RecordRevalidationTriggered()

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("Revalidating cached HEAD with upstream (conditional HEAD)")

	resp, err := s.forwarder.DoConditionalHeadRequest(ctx, bucket, key, accessKey, secretKey, meta.ETag, meta.LastModified)

	// Object changed — serve new headers from upstream, invalidate cache
	if err == nil && resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		metrics.RecordRevalidationUpdated()
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("HEAD revalidation 200 - object changed")

		s.invalidateStaleMeta(bucket, key, meta.ETag)
		io.Copy(io.Discard, resp.Body)

		copyHeaders(w.Header(), resp.Header)
		writeCacheStatus(w, XCacheRevalidated)
		w.WriteHeader(resp.StatusCode)
		// The response headers came from upstream's 200 — this is an upstream
		// answer, unlike the 304 path below that serves the cached metadata.
		metrics.RecordRequest("HeadObject", "success", metrics.SourceUpstream, time.Since(start).Seconds())
		return nil
	}

	// 304, error, or unexpected status — record specific metrics, then serve cached headers
	if err != nil {
		log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("HEAD revalidation failed, serving stale")
		metrics.RecordRevalidationFailed()
		metrics.RecordRevalidationStaleServed()
	} else {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotModified {
			metrics.RecordRevalidationNotModified()
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("HEAD revalidation 304 - unchanged")
		} else {
			io.Copy(io.Discard, resp.Body)
			log.Warn().Int("status", resp.StatusCode).Str("bucket", bucket).Str("key", key).Msg("HEAD revalidation unexpected status, serving stale")
			metrics.RecordRevalidationFailed()
			metrics.RecordRevalidationStaleServed()
		}
	}

	// The client's own conditionals still apply to the served headers — a
	// matching If-None-Match is a 304, not a 200 (the same 304-only rule as
	// the non-revalidate HEAD hit path; the upstream revalidation validated
	// TAG's cached ETag, not the client's request).
	if s.writeNotModifiedFromCache(w, r, meta, "HeadObject", start) {
		return nil
	}
	serveMetaHit(w, meta, "HeadObject", start)
	return nil
}

// shouldForceRevalidate checks if the client is requesting cache revalidation.
// Per RFC 7234, Cache-Control: no-cache means "must revalidate with origin before serving".
func shouldForceRevalidate(r *http.Request) bool {
	cc := r.Header.Get("Cache-Control")
	return strings.Contains(cc, "no-cache") || strings.Contains(cc, "max-age=0")
}

// shouldBypassCache checks if the client is requesting full cache bypass.
// Cache-Control: no-store means "do not use or store cached data".
func shouldBypassCache(r *http.Request) bool {
	cc := r.Header.Get("Cache-Control")
	return strings.Contains(cc, "no-store")
}

// readRepresentationSelectors are the GetObject/HeadObject query parameters
// that select a representation other than the current object as stored:
// another version, a single part, or the current body under overridden
// response headers. The cache holds only the current object under its own
// headers, so a request carrying any of them can neither be answered from the
// cache nor populate it. The transparent forwarder's read-authorization check
// (isReadAuthorizationQuery) shares this list: these are exactly the
// parameters that still mean "GetObject/HeadObject" rather than another S3
// operation.
var readRepresentationSelectors = []string{
	"versionId", "partNumber",
	"response-cache-control", "response-content-disposition", "response-content-encoding",
	"response-content-language", "response-content-type", "response-expires",
}

// isReadRepresentationSelector reports whether a query parameter name is one
// of readRepresentationSelectors. S3 query parameter names are case-sensitive.
func isReadRepresentationSelector(name string) bool {
	for _, p := range readRepresentationSelectors {
		if name == p {
			return true
		}
	}
	return false
}

// hasRepresentationSelectors reports whether an object GET/HEAD carries any
// readRepresentationSelectors. Such a request is forwarded upstream as-is:
// never served from the cache (it would return the current object with the
// cached headers, ignoring the selector) and never used to populate it.
// representationSelectorNames returns the representation selector names
// present in the request query, sorted, for logging. Only the names are
// returned: the raw query of a presigned request carries its signature and
// the other fields needed to reuse the URL, which must not reach the logs.
func representationSelectorNames(r *http.Request) []string {
	var names []string
	for name := range r.URL.Query() {
		if isReadRepresentationSelector(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func hasRepresentationSelectors(r *http.Request) bool {
	if r.URL.RawQuery == "" {
		return false
	}
	for name := range r.URL.Query() {
		if isReadRepresentationSelector(name) {
			return true
		}
	}
	return false
}

// forwardRepresentationSelector is the GET/HEAD path for a request with
// representation selectors: a plain upstream forward with no cache read, no
// broadcast coalescing (the broadcast key is bucket/key only, so a
// ?versionId=old fetch must not be shared with a current-object fetch), and
// no populate of any kind. X-Cache reports BYPASS, like Cache-Control:
// no-store.
func (s *Service) forwardRepresentationSelector(ctx context.Context, w http.ResponseWriter, r *http.Request, operation string, start time.Time) error {
	writeCacheStatus(w, s.cacheMissStatus(true))
	err := s.forwarder.Forward(ctx, w, r)
	metrics.RecordRequestResult(operation, metrics.SourceUpstream, err, time.Since(start).Seconds())
	return err
}
