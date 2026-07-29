package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
)

// bodyTeeingForwarder is implemented by forwarders that can tee the decoded request body
// while forwarding a single PutObject. Only the signing forwarder implements it: it decodes
// AWS chunked encoding and thus sees the assembled object bytes. The transparent forwarder
// preserves the client's opaque body + signature, so it can't tee cleanly.
type bodyTeeingForwarder interface {
	ForwardTeeingBody(ctx context.Context, w http.ResponseWriter, r *http.Request, tee io.Writer) (int, http.Header, error)
}

// cappedBuffer is an io.Writer that accumulates up to cap bytes, then silently discards the
// rest and records that it overflowed. It NEVER returns an error — that is essential, because
// it is used as an io.TeeReader sink where a write error would truncate the upstream stream.
// Overflow (a client sending more than its declared Content-Length) is surfaced out of band
// so the caller can decline to cache a partial body.
type cappedBuffer struct {
	buf        []byte
	cap        int
	overflowed bool
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if !c.overflowed {
		room := c.cap - len(c.buf)
		if len(p) <= room {
			c.buf = append(c.buf, p...)
		} else {
			if room > 0 {
				c.buf = append(c.buf, p[:room]...)
			}
			c.overflowed = true
		}
	}
	return len(p), nil
}

// teeState carries a write-through tee attempt from the forward call to the post-forward
// cache write. When non-nil, the caller has reserved `weight` bytes of populate budget that
// must be released exactly once — writeThroughCache takes ownership of that release.
type teeState struct {
	buf         *cappedBuffer
	respHeaders http.Header
	statusCode  int
	weight      int64
}

// teeObjectSize returns the decoded object size for a PutObject, preferring the AWS
// decoded-content-length header (chunked uploads) over the raw Content-Length.
func teeObjectSize(r *http.Request) int64 {
	if dcl := r.Header.Get("X-Amz-Decoded-Content-Length"); dcl != "" {
		if n, err := strconv.ParseInt(dcl, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return r.ContentLength
}

// forwardPutMaybeTee forwards a PutObject. When the object is eligible for a write-through
// tee — signing-mode forwarder, cache enabled, a single object within the cache size
// threshold, and the populate budget admits it without blocking — it tees the decoded body
// so the caller can populate the cache without a read-back warm GET. It returns a non-nil
// *teeState only when a tee was attempted; the caller must then route it through
// writeThroughCache (which releases the reserved budget). Otherwise it performs a plain
// forward and returns nil, and the caller falls back to warm-on-write.
func (s *Service) forwardPutMaybeTee(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) (*teeState, error) {
	tf, ok := s.forwarder.(bodyTeeingForwarder)
	size := teeObjectSize(r)
	eligible := ok &&
		s.cache.IsEnabled() &&
		s.populateBudget != nil &&
		// Anonymous writes fall back to warm-on-write, whose unsigned probe learns whether
		// the object is public-read before caching it; the tee can't make that inference.
		!hasNoAuthCredentials(r) &&
		size > 0 &&
		size <= s.config.Cache.SizeThreshold

	if !eligible {
		return nil, s.forwarder.Forward(ctx, w, r)
	}

	// Non-blocking admission: the tee runs on the client PUT path, so it must never block
	// the write waiting for budget. priorityReadMiss acquires the count slot and byte budget
	// without blocking; on denial we fall back to a plain forward + (async, blocking)
	// warm-on-write, so the object still gets cached under budget pressure.
	if !s.acquireCacheSlot(ctx, size, priorityReadMiss) {
		return nil, s.forwarder.Forward(ctx, w, r)
	}

	ts := &teeState{buf: &cappedBuffer{cap: int(size)}, weight: size}
	status, headers, err := tf.ForwardTeeingBody(ctx, w, r, ts.buf)
	ts.statusCode = status
	ts.respHeaders = headers
	return ts, err
}

// writeThroughCache populates the cache from a teed PutObject body, avoiding a read-back
// GET. It returns true when the object was handed off to an async cache write, false when
// it declined (over-cap body, missing ETag, or not cacheable) — in which case the caller
// should fall back to warm-on-write. It always releases the reserved populate budget:
// immediately on decline, or when the async write completes on success.
//
// writeStartTime is stamped here (after the caller's post-forward invalidation) so this
// write's own tombstone ordering matches warm-on-write: the object's own invalidations are
// older and don't block it, while a later competing DELETE/overwrite does.
func (s *Service) writeThroughCache(bucket, key string, r *http.Request, ts *teeState) bool {
	if ts.buf.overflowed {
		// Client sent more than its declared length; caching a truncated body would be wrong.
		s.releaseCacheSlot(ts.weight)
		return false
	}
	etag := ts.respHeaders.Get("ETag")
	if etag == "" {
		// No version identity from upstream — bodies are ETag-addressed, so this isn't
		// cacheable. Fall back to warm-on-write (which learns the ETag from a GET).
		s.releaseCacheSlot(ts.weight)
		return false
	}

	// Build the headers a GET of this object would return: object metadata (Content-Type,
	// Content-Encoding, x-amz-meta-*, ...) from the PUT request; ETag/version from the PUT
	// response; Content-Length from the bytes actually teed.
	h := r.Header.Clone()
	h.Set("ETag", etag)
	if v := ts.respHeaders.Get("x-amz-version-id"); v != "" {
		h.Set("x-amz-version-id", v)
	} else {
		h.Del("x-amz-version-id")
	}
	h.Set("Content-Length", strconv.FormatInt(int64(len(ts.buf.buf)), 10))

	meta := cache.MetaFromHTTPHeaders(bucket, key, http.StatusOK, h)
	if !meta.IsCacheable(s.config.Cache.SizeThreshold) {
		s.releaseCacheSlot(ts.weight)
		return false
	}

	writeStartTime := time.Now().UnixNano()
	ttl := int(s.config.Cache.TTL.Seconds())
	body := ts.buf.buf

	go func() {
		defer s.releaseCacheSlot(ts.weight)
		cacheCtx, cancel := context.WithTimeout(context.Background(), cacheWriteTimeoutForSize(meta.ContentLength))
		defer cancel()
		if err := s.cache.PutWithMetaStreamTombstoneAware(cacheCtx, bucket, key, meta, bytes.NewReader(body), ttl, writeStartTime); err != nil {
			log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Write-through cache tee failed")
		}
	}()
	metrics.CacheWriteThrough.Inc()
	return true
}
