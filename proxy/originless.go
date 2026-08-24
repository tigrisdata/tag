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
			// Probe only a well-formed single range. A malformed, unsatisfiable, or
			// multi-range request is NOT a miss — the object is present — and
			// serveRangeFromBlockCache owns the 416 for those (it answers and
			// reports served), so it must be reached rather than short-circuited.
			if ranges, rerr := parseRangeHeader(rangeHeader, meta.ContentLength); rerr == nil && len(ranges) == 1 {
				b0, bK := coveringBlocks(ranges[0].start, ranges[0].end, meta.BlockSize)
				if !s.allBlocksPresent(ctx, bucket, key, meta, b0, bK) {
					return s.originlessMiss(w, r, operation, start)
				}
			}
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
		if meta.ContentLength <= 0 {
			return s.originlessMiss(w, r, operation, start)
		}
		b0, bK := coveringBlocks(0, meta.ContentLength-1, meta.BlockSize)
		if !s.allBlocksPresent(ctx, bucket, key, meta, b0, bK) {
			return s.originlessMiss(w, r, operation, start)
		}
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
