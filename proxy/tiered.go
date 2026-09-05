package proxy

// Tiered store mode (issue #201): TAG in front of an upstream that is a cheap,
// capacity-priced store — a cache bucket — rather than the system of record.
//
// The metadata for EVERY object TAG holds lives locally, stamped with the tier
// its body lives in (BodyUpstream). That makes the local metadata authoritative
// for existence: a metadata miss answers NoSuchKey without touching upstream,
// in both tiers — TAG is a cache, and "not cached" is a complete answer. The
// caller treats it as its cue to fall back to the system of record and
// re-populate by writing back through TAG.
//
// Small objects (declared size ≤ cache.size_threshold) are the LOCAL tier:
// stored whole by the local-store engine (localstore.go), served and deleted
// without any upstream request — reads of cached objects cost zero upstream
// rate limit and zero upstream writes. Large objects are the UPSTREAM tier:
// the PUT passes through, a metadata marker is stored locally, HEADs answer
// from the marker, and a GET body forward is the mode's only body traffic.
//
// Cross-tier overwrites clean up the displaced version: a small write over an
// upstream-tier object deletes the upstream copy asynchronously; a large write
// over a local-tier object frees the local copy via the ordinary
// invalidate-before-forward. No prior metadata means nothing is cached — there
// is nothing to clean.
//
// Authentication is the transparent-proxy flow, unchanged: tiered semantics
// apply only to requests whose signature TAG validated locally. A request it
// cannot validate (unknown key, anonymous) forwards to upstream — the auth
// authority — exactly as in transparent mode; keys are learned from those
// responses, and the window before learning behaves like a cache miss.
//
// Deliberately NOT reimplemented here (v1): listings, multipart, copies,
// tagging, ACLs all pass through to upstream. Objects created upstream without
// TAG's involvement (a multipart completion, a server-side copy) get no local
// metadata and therefore read as misses through TAG until written again.

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
)

// tieredCleanupTimeout bounds the background cross-tier DELETE.
const tieredCleanupTimeout = 30 * time.Second

// handleTieredObject serves GET and HEAD in tiered mode.
func (s *Service) handleTieredObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	ctx := r.Context()
	bucket, key := ParseBucketKey(r)

	operation := "GetObject"
	if r.Method == http.MethodHead {
		operation = "HeadObject"
	}

	result, _, _, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil {
		metrics.RecordRequest(operation, "auth_error", time.Since(start).Seconds())
		return err
	}
	if result != AuthValidated {
		return s.forwarder.Forward(ctx, w, r)
	}

	if meta, found, cacheErr := s.cache.GetMeta(ctx, bucket, key); cacheErr == nil && found && meta != nil && meta.BodyUpstream {
		// Upstream tier: the local metadata answers everything except a GET body.
		if r.Method == http.MethodHead && originlessPlainObject(r) {
			if writePreconditionFailed(w, r, meta) {
				metrics.RecordRequest(operation, "success", time.Since(start).Seconds())
				return nil
			}
			if s.writeNotModifiedFromCache(w, r, meta, operation, start) {
				return nil
			}
			meta.WriteHeaders(w)
			writeCacheStatus(w, XCacheHit)
			w.WriteHeader(meta.StatusCode)
			metrics.RecordRequest(operation, "success", time.Since(start).Seconds())
			return nil
		}
		// The mode's one body forward. Plain Forward: no read-populate — the
		// large tier is populated by writes alone.
		return s.forwarder.Forward(ctx, w, r)
	}

	// Local tier, or no metadata at all: the engine serves it, and its miss is
	// the authoritative NoSuchKey.
	return s.HandleOriginlessObject(w, r)
}

// handleTieredPut routes a PUT to its tier by declared size.
func (s *Service) handleTieredPut(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	ctx := r.Context()
	bucket, key := ParseBucketKey(r)

	result, accessKey, secretKey, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil {
		metrics.RecordRequest("PutObject", "auth_error", time.Since(start).Seconds())
		return err
	}

	// The prior version's tier decides what an overwrite must clean up.
	var prior *cache.CachedObjectMeta
	if meta, found, cacheErr := s.cache.GetMeta(ctx, bucket, key); cacheErr == nil && found {
		prior = meta
	}

	declaredSize, sized := originlessPutSize(r)
	conditional := r.Header.Get("If-Match") != "" || r.Header.Get("If-None-Match") != ""

	// Local tier requires: a validated caller (the engine serves from cache with
	// no upstream auth check), a plain object PUT, a declared size within the
	// threshold — and no conditional write against an upstream-tier prior (the
	// engine can only evaluate preconditions against a local body; upstream owns
	// that object's version, so upstream evaluates them).
	local := result == AuthValidated &&
		originlessPlainObject(r) &&
		sized && declaredSize <= s.config.Cache.SizeThreshold &&
		!(conditional && prior != nil && prior.BodyUpstream)

	if local {
		rec := &statusRecorder{ResponseWriter: w}
		err := s.HandleOriginlessPut(rec, r)
		if err == nil && rec.wroteSuccess() && prior != nil && prior.BodyUpstream {
			// Small write displaced an upstream-tier version: remove the
			// upstream copy so it doesn't linger as an orphan.
			s.deleteUpstreamObjectAsync(bucket, key, accessKey, secretKey)
		}
		return err
	}

	// Upstream tier: pass the PUT through, then stamp the local metadata marker
	// that makes this object exist in TAG's authoritative view. The
	// invalidate-before/after ordering mirrors HandlePutObject; the pre-forward
	// invalidation also frees a displaced local-tier body.
	s.invalidateObject(context.Background(), bucket, key)

	rec := &statusRecorder{ResponseWriter: w}
	err = s.forwarder.Forward(ctx, rec, r)

	if err == nil && rec.wroteSuccess() && s.cache.IsEnabled() {
		s.invalidateObject(context.Background(), bucket, key)
		s.putUpstreamMarker(r, w.Header().Get("ETag"), bucket, key)
	}

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("PutObject", status, time.Since(start).Seconds())
	return err
}

// putUpstreamMarker stores the metadata-only entry for an object whose body
// was just written upstream. Headers come from the PUT request (the same
// mapping as every populate path); the ETag comes from upstream's response —
// without one the marker is skipped and the object reads as a miss until
// written again.
func (s *Service) putUpstreamMarker(r *http.Request, etag, bucket, key string) {
	if etag == "" {
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Upstream PUT response had no ETag - skipping tier marker")
		return
	}
	meta := cache.MetaFromHTTPHeaders(bucket, key, http.StatusOK, r.Header)
	meta.ETag = etag
	meta.BodyUpstream = true
	meta.LastModified = time.Now().Unix()
	if declaredSize, ok := originlessPutSize(r); ok {
		meta.ContentLength = declaredSize
	}

	ttl := int(s.config.Cache.TTL.Seconds())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The write stamp is taken AFTER the post-forward invalidation above wrote
	// its tombstone — stamped with the handler's start, the marker would always
	// lose to that tombstone and never be written. With a fresh stamp, only a
	// DELETE arriving after this point suppresses the marker, which is the
	// serialization that DELETE deserves to win.
	if _, err := s.cache.PutMetaTombstoneAware(ctx, bucket, key, meta, ttl, time.Now().UnixNano()); err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Failed to write upstream tier marker")
	}
}

// handleTieredDeleteLocal answers a DELETE entirely locally when the object's
// body is in the local tier. Returns handled=false when the caller should run
// the ordinary forwarding DELETE instead — upstream-tier objects, objects with
// no metadata (idempotent 204 from upstream, and it covers an expired marker's
// orphan), and requests TAG could not validate.
func (s *Service) handleTieredDeleteLocal(w http.ResponseWriter, r *http.Request) (handled bool, err error) {
	start := time.Now()
	result, _, _, authErr := s.forwarder.ValidateAndGetCredentials(r)
	if authErr != nil {
		metrics.RecordRequest("DeleteObject", "auth_error", time.Since(start).Seconds())
		return true, authErr
	}
	if result != AuthValidated || !originlessPlainObject(r) {
		return false, nil
	}

	ctx := r.Context()
	bucket, key := ParseBucketKey(r)
	meta, found, cacheErr := s.cache.GetMeta(ctx, bucket, key)
	if cacheErr != nil || !found || meta == nil || meta.BodyUpstream {
		return false, nil
	}
	return true, s.HandleOriginlessDelete(w, r)
}

// deleteUpstreamObjectAsync issues the cross-tier cleanup DELETE in the
// background, signed with the (validated) caller's credentials. Best-effort:
// a failure leaves an orphan that the cache bucket's own expiry collects.
func (s *Service) deleteUpstreamObjectAsync(bucket, key, accessKey, secretKey string) {
	if accessKey == "" || secretKey == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), tieredCleanupTimeout)
		defer cancel()
		resp, err := s.forwarder.DoObjectDeleteRequest(ctx, bucket, key, accessKey, secretKey)
		if err != nil {
			log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cross-tier cleanup delete failed")
			return
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
			log.Debug().Int("status", resp.StatusCode).Str("bucket", bucket).Str("key", key).Msg("Cross-tier cleanup delete rejected")
		}
	}()
}
