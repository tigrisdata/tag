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
	// ForwardTeeingBody forwards the PUT while teeing the decoded body into tee, and returns
	// the upstream status + response headers plus the client credentials it already validated
	// and derived (so the caller can HEAD/warm without re-validating the signature).
	ForwardTeeingBody(ctx context.Context, w http.ResponseWriter, r *http.Request, tee io.Writer) (statusCode int, respHeaders http.Header, accessKey, secretKey string, err error)
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
	accessKey   string // client credentials validated by ForwardTeeingBody, reused for the HEAD/warm
	secretKey   string
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
		// The tee is an optimization of warm-on-write (cache-on-write), so it must respect
		// the same flag: when warm-on-write is disabled (the default), a successful PUT must
		// not populate the cache at all.
		s.config.Cache.WarmOnWrite &&
		s.cache.IsEnabled() &&
		s.populateBudget != nil &&
		// Anonymous writes fall back to warm-on-write, whose unsigned probe learns whether
		// the object is public-read before caching it; the tee can't make that inference.
		!hasNoAuthCredentials(r) &&
		size > 0 &&
		// The tee buffers the whole object in memory, so it uses WriteThroughMaxSize (a small
		// cap), not SizeThreshold. Larger-but-cacheable objects fall back to streaming
		// warm-on-write. IsCacheable(SizeThreshold) still applies to the meta after the HEAD.
		size <= s.config.Cache.WriteThroughMaxSize

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

	// Preallocate to the exact known size to avoid repeated append reallocations on the
	// write hot path.
	ts := &teeState{buf: &cappedBuffer{buf: make([]byte, 0, int(size)), cap: int(size)}, weight: size}
	status, headers, accessKey, secretKey, err := tf.ForwardTeeingBody(ctx, w, r, ts.buf)
	ts.statusCode = status
	ts.respHeaders = headers
	ts.accessKey = accessKey
	ts.secretKey = secretKey
	return ts, err
}

// writeThroughCache populates the cache from a teed PutObject body without a full-object
// read-back. It fetches AUTHORITATIVE object metadata with a HEAD (which returns exactly the
// headers a GET would, minus the body), so the cached entry can never diverge from an origin
// GET, then writes that meta with the bytes already teed.
//
// Returns true when the write was handed off to the async path (which caches the object or,
// if the HEAD can't confirm it, falls back to a read-back warm), false when it declined
// synchronously (over-cap body, missing ETag, or no usable credentials) so the caller warms.
// The reserved populate budget is always released: synchronously on decline, or when the
// async work completes.
func (s *Service) writeThroughCache(bucket, key string, ts *teeState) bool {
	putETag := ts.respHeaders.Get("ETag")
	// Credentials were validated and derived by ForwardTeeingBody (no re-validation here). The
	// async goroutine uses only these captured values — never r, which the server may recycle
	// after the handler returns.
	if ts.buf.overflowed || putETag == "" || ts.accessKey == "" || ts.secretKey == "" {
		// Over-declared body, no version identity from upstream (bodies are ETag-addressed),
		// or no usable credentials.
		s.releaseCacheSlot(ts.weight)
		return false
	}

	body := ts.buf.buf
	accessKey, secretKey := ts.accessKey, ts.secretKey
	weight := ts.weight
	go func() {
		// Hold the tee's populate reservation for the whole goroutine: it accounts for the
		// object-sized teed body, which stays live until this returns. Releasing it early
		// (before a fallback allocates) would let the body coexist unaccounted with the
		// fallback's buffers and push live populate memory past the budget.
		defer s.releaseCacheSlot(weight)

		outcome := s.cacheTeedBodyFromHead(bucket, key, putETag, body, accessKey, secretKey)
		if outcome != teeFallbackWarm {
			// teeCached: already cached. teeNotCacheable: the authoritative HEAD says the object
			// isn't cacheable (Cache-Control no-store/private, or over threshold), so a warm would
			// only re-download the full body to reject it again — skip it.
			return
		}
		// The HEAD couldn't confirm the version we teed (HEAD failed, object changed/removed, size
		// disagreement, or a competing write superseded it). Fall back to a read-back warm to cache
		// the current object, with the same protected (blocking) admission as the normal
		// warm-on-write path — so budget pressure doesn't shed it and leave a cold-read window.
		// Holding the tee reservation across this doesn't stall the warm: triggerBackgroundCacheFetch
		// only SPAWNS the fetch and returns, so this goroutine returns and its deferred release frees
		// the slot, which the warm's (separate) blocking acquire then observes.
		metrics.WarmOnWriteTriggered.Inc()
		s.triggerBackgroundCacheFetch(bucket, key, accessKey, secretKey, false /*anonymous*/, priorityWarmWrite)
	}()
	return true
}

// teeOutcome is the result of a HEAD-verified write-through cache attempt.
type teeOutcome int

const (
	teeCached       teeOutcome = iota // meta+body written; no fallback needed
	teeNotCacheable                   // HEAD authoritatively says not cacheable; a warm would only re-download and re-reject
	teeFallbackWarm                   // couldn't confirm/write the version; a read-back warm may still cache the current object
)

// cacheTeedBodyFromHead HEADs the object for authoritative metadata and, only if the object
// is still the exact version we teed (ETag matches) and is cacheable, writes that meta with
// the teed body. It returns teeCached on a successful write, teeNotCacheable when the HEAD
// authoritatively shows the object should not be cached (so warming is pointless), and
// teeFallbackWarm when it couldn't confirm/write the version (so a read-back warm may help).
func (s *Service) cacheTeedBodyFromHead(bucket, key, putETag string, body []byte, accessKey, secretKey string) teeOutcome {
	// Stamp the tombstone reference BEFORE the HEAD (mirroring warm-on-write, which stamps
	// before its fetch), so the whole HEAD-to-write window is guarded: a competing overwrite
	// whose invalidation lands after this point is newer than writeStartTime and skips our
	// write, while one that landed earlier is caught by the ETag-consistency check below. It
	// is still newer than this PUT's own post-forward invalidation (stamped before this
	// goroutine ran), so our own invalidation doesn't block us.
	writeStartTime := time.Now().UnixNano()

	headCtx, cancel := context.WithTimeout(context.Background(), backgroundFetchTimeout)
	defer cancel()

	// Empty etag/lastModified => a plain (non-conditional) HEAD.
	resp, err := s.forwarder.DoConditionalHeadRequest(headCtx, bucket, key, accessKey, secretKey, "", 0)
	if err != nil || resp == nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Write-through tee HEAD failed")
		return teeFallbackWarm
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	if resp.StatusCode != http.StatusOK {
		return teeFallbackWarm // object removed/unreadable now; a warm may still fetch it
	}
	// Only cache if the object is still the exact version we teed. A concurrent overwrite
	// returns a different ETag whose body is NOT what we hold; caching it would pair
	// mismatched meta and body (the torn-pair hazard ETag-versioned bodies prevent).
	if resp.Header.Get("ETag") != putETag {
		return teeFallbackWarm // superseded by a concurrent overwrite; warm caches the current version
	}

	meta := cache.MetaFromHTTPHeaders(bucket, key, http.StatusOK, resp.Header)
	if !meta.IsCacheable(s.config.Cache.SizeThreshold) {
		// The authoritative HEAD says this object must not be cached (Cache-Control no-store/
		// private, or over threshold). A warm would only re-download the full body to reject it
		// again, so don't fall back.
		return teeNotCacheable
	}
	if meta.ContentLength != int64(len(body)) {
		// HEAD's advertised length disagrees with the bytes we teed; let a warm fetch the
		// authoritative body instead of caching a mismatch.
		return teeFallbackWarm
	}

	ttl := int(s.config.Cache.TTL.Seconds())
	cacheCtx, cacheCancel := context.WithTimeout(context.Background(), cacheWriteTimeoutForSize(meta.ContentLength))
	defer cacheCancel()
	wrote, err := s.cache.PutWithMetaStreamTombstoneAware(cacheCtx, bucket, key, meta, bytes.NewReader(body), ttl, writeStartTime)
	if err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Write-through cache tee write failed")
		return teeFallbackWarm
	}
	if !wrote {
		// A newer tombstone (competing DELETE/overwrite) superseded our teed version, so
		// nothing was cached. Warm instead: its own writeStartTime is newer than that tombstone,
		// so it caches the CURRENT version (or no-ops on a delete) rather than our stale body.
		return teeFallbackWarm
	}
	metrics.CacheWriteThrough.Inc()
	return teeCached
}
