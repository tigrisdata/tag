package proxy

import (
	"context"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/s3err"
)

// Origin-less mode: TAG with no upstream, serving only what its cache holds.
//
// The mode is expressed at the ROUTER, not inside the proxy handlers: the server
// registers this handler set instead of the proxying one, so every path that
// assumes an upstream — revalidation, broadcast coalescing, background fetch, the
// mutation handlers with their invalidate-before-forward ordering — is unreachable
// by construction rather than defused guard-by-guard. What this file does not
// call, this mode cannot do.
//
// Phase-1 trust model: only anonymous requests for public-read objects are
// served. Signed requests cannot be validated (signature validation learns keys
// from an upstream), and non-public objects are withheld until the explicit
// auth-less trust decision lands in a later phase. Both answer NoSuchKey rather
// than a 403, so an unauthorized probe cannot distinguish "absent" from "held".

// Originless reports whether this Service runs without an upstream. The server
// uses it to select which handler set to register.
func (s *Service) Originless() bool {
	return s.config != nil && !s.config.Upstream.HasOrigin()
}

// HandleOriginlessObject serves GET and HEAD for a single object from cache
// alone. A miss is the final answer: NoSuchKey, which the caller (e.g. the Tigris
// gateway) treats as its cue to fall back to the block owner.
func (s *Service) HandleOriginlessObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	ctx := r.Context()
	bucket, key := ParseBucketKey(r)

	// Plain reads only. A query parameter selects a representation or an
	// operation this mode does not implement — ?versionId asks for a specific
	// version, ?partNumber for one part, ?tagging for a subresource — and serving
	// the current full object for those is silently wrong data, not a convenience.
	// One rule instead of an enumerated parameter list that goes stale.
	//
	// The single exception is x-id, the operation tag aws-sdk-go-v2 appends to
	// every plain request (?x-id=GetObject). It restates what method+path already
	// say and selects nothing, and rejecting it would 501 every standard SDK read
	// — including the tigris-os gateway this mode exists to serve.
	if r.URL.RawQuery != "" {
		q := r.URL.Query()
		q.Del("x-id")
		if len(q) > 0 {
			return s.HandleOriginlessUnsupported(w, r)
		}
	}

	operation := "GetObject"
	if r.Method == http.MethodHead {
		operation = "HeadObject"
	}

	log.Debug().Str("bucket", bucket).Str("key", key).Str("op", operation).Msg("HandleOriginlessObject")

	if !s.cache.IsEnabled() {
		writeCacheStatus(w, XCacheDisabled)
		s3err.WriteError(w, r, s3err.ErrNoSuchKey)
		metrics.RecordRequest(operation, "success", time.Since(start).Seconds())
		return nil
	}

	meta, found, cacheErr := s.cache.GetMeta(ctx, bucket, key)
	if cacheErr != nil || !found || meta == nil {
		return s.originlessMiss(w, r, operation, start)
	}

	// Phase-1 trust: anonymous + public-read only. NoSuchKey either way — absence
	// and denial must be indistinguishable to a probe.
	if !hasNoAuthCredentials(r) || !meta.IsPublicRead() {
		return s.originlessMiss(w, r, operation, start)
	}

	// ONE existence gate for the whole handler: an entry is visible only when its
	// data is fully present — every block of a block-mode entry, the body of a
	// whole-object one. Every answer below (304, HEAD 200, any serve) implies
	// existence, and each of HEAD, conditionals, and the serve paths independently
	// answering that question is exactly how three review rounds of
	// existence-vs-serveability disagreements happened. An incomplete entry is
	// simply invisible: metadata alone cannot be served, and claiming existence
	// from it makes HEAD-as-existence callers (the tigris-os distribute worker
	// skips re-population when IsObjectExists is true) skip the healing that would
	// make the entry servable again. A probe-to-serve race can still truncate a
	// concurrent eviction; the gate narrows the window, nothing can close it.
	if !s.entryServable(ctx, bucket, key, meta) {
		return s.originlessMiss(w, r, operation, start)
	}

	// Conditional requests are answered from the cached metadata; there is no
	// upstream for a client's Cache-Control to revalidate against, so no-cache and
	// no-store are simply not consulted — the cached copy is the only copy.
	if s.writeNotModifiedFromCache(w, r, meta, operation, start) {
		return nil
	}

	if r.Method == http.MethodHead {
		meta.WriteHeaders(w)
		writeCacheStatus(w, XCacheHit)
		w.WriteHeader(meta.StatusCode)
		metrics.RecordRequest(operation, "success", time.Since(start).Seconds())
		return nil
	}

	// The serve helpers below are shared with the proxying mode, and the
	// block-mode ones stream OPTIMISTICALLY: they commit headers first and recover
	// a mid-stream missing block from upstream (streamRemainderFromUpstream). With
	// no origin that recovery fails after the 200/206 is already sent, leaving the
	// client a truncated body instead of a miss. So block-mode serves are gated on
	// a presence probe of every covering block — absent anything, this is a clean
	// NoSuchKey before a single header is written. Whole-object (non-block) serves
	// need no probe: they fail before committing.
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		if meta.BlockSize > 0 {
			// The existence gate above proved every block present, so a non-serve
			// here is a budget shed or a probe-to-serve race, not routine absence.
			// serveRangeFromBlockCache keeps ownership of the 416 for malformed,
			// unsatisfiable, and multi-range requests.
			served, rangeErr := s.serveRangeFromBlockCache(ctx, w, r, bucket, key, "", "", meta, rangeHeader, start)
			if served {
				return rangeErr
			}
			return s.originlessMiss(w, r, operation, start)
		}
		served, rangeErr := s.serveRangeFromCache(ctx, w, r, bucket, key, meta, rangeHeader, start)
		if served {
			return rangeErr
		}
		if bodyGone(rangeErr) {
			s.cache.Delete(ctx, bucket, key)
		}
		return s.originlessMiss(w, r, operation, start)
	}

	if meta.BlockSize > 0 {
		served, assembleErr := s.serveFullObjectFromBlockCache(ctx, w, bucket, key, "", "", meta, start)
		if served {
			return assembleErr
		}
		return s.originlessMiss(w, r, operation, start)
	}
	if bodyErr := s.serveFromCache(ctx, w, bucket, key, meta, start); bodyErr != nil {
		// serveFromCache fails before committing headers, so a miss response is
		// still writable. Only a genuinely absent body orphans the metadata.
		if bodyGone(bodyErr) {
			s.cache.Delete(ctx, bucket, key)
		}
		return s.originlessMiss(w, r, operation, start)
	}
	return nil
}

// allBlocksPresent probes every block in [b0,bK] before a block-mode serve
// commits its headers. A probe is a point lookup, so even a large object costs
// one read per covering block — the price of guaranteeing that this mode never
// sends a 200/206 it cannot finish. A probe-to-serve race (eviction between the
// probe and the read) is still possible and still truncates; the probe narrows
// the window from "any evicted block" to "evicted in the microseconds between
// probe and read", which is the strongest guarantee available without holding
// every block in memory first.
func (s *Service) allBlocksPresent(ctx context.Context, bucket, key string, meta *cache.CachedObjectMeta, b0, bK int64) bool {
	for i := b0; i <= bK; i++ {
		present, err := s.cache.BlockExistsErr(ctx, bucket, key, meta.ETag, meta.BlockSize, i)
		if err != nil || !present {
			return false
		}
	}
	return true
}

// entryServable is the handler's single existence answer: all blocks present for
// a block-mode entry, the body present for a whole-object one. Everything the
// handler says — 304, HEAD 200, a served body — flows from this one predicate, so
// existence and serveability cannot disagree.
func (s *Service) entryServable(ctx context.Context, bucket, key string, meta *cache.CachedObjectMeta) bool {
	if meta.BlockSize > 0 {
		if meta.ContentLength <= 0 {
			return false
		}
		b0, bK := coveringBlocks(0, meta.ContentLength-1, meta.BlockSize)
		return s.allBlocksPresent(ctx, bucket, key, meta, b0, bK)
	}
	// A zero-length object is vacuously servable: no byte can be missing, and the
	// first-byte probe below cannot see one anyway — the embedded backend returns
	// nil + zero bytes for a present-but-empty body and an absent one alike (the
	// quirk countingWriter exists for). Serving from metadata alone is exact for an
	// empty body, evicted or not.
	if meta.ContentLength == 0 {
		return true
	}
	present, err := s.cache.BodyExistsErr(ctx, bucket, key, meta.ETag)
	return err == nil && present
}

// originlessMiss answers the one thing a miss can be in this mode.
func (s *Service) originlessMiss(w http.ResponseWriter, r *http.Request, operation string, start time.Time) error {
	writeCacheStatus(w, XCacheMiss)
	s3err.WriteError(w, r, s3err.ErrNoSuchKey)
	metrics.RecordRequest(operation, "success", time.Since(start).Seconds())
	return nil
}

// HandleOriginlessUnsupported rejects every operation this mode does not
// implement — listings, mutations, multipart, tagging, ACLs. Recorded under a
// distinct status so a client persistently writing to an origin-less tier (a
// misconfigured gateway, for instance) is visible on the dashboard instead of
// blending into the success rate.
func (s *Service) HandleOriginlessUnsupported(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	s3err.WriteError(w, r, s3err.ErrNotImplemented)
	metrics.RecordRequest("Unsupported", "unsupported", time.Since(start).Seconds())
	return nil
}
