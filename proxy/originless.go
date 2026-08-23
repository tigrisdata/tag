package proxy

import (
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
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

	// The serve helpers below are shared with the proxying mode. Their
	// fetch-missing-blocks paths go through the forwarder, which in this mode
	// errors instead of fetching — so an entry with evicted blocks degrades to a
	// clean miss here rather than a partial serve.
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		if meta.BlockSize > 0 {
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
