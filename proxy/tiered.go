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
// The tier boundary is enforced on reads as well as writes: a validated GET
// that hits an upstream-tier marker whose size fits the local tier triggers a
// one-shot background re-tier (maybeRetierOnRead), healing objects that were
// mis-placed — e.g. a small PUT forwarded before its key was learned. Damage
// from mis-placement is thereby capped at one extra upstream fetch per object
// instead of one body forward per read until TTL.
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
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
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

	result, accessKey, secretKey, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil {
		metrics.RecordRequest(operation, "auth_error", time.Since(start).Seconds())
		return err
	}
	if result != AuthValidated {
		return s.forwarder.Forward(ctx, w, r)
	}

	meta, found, cacheErr := s.cache.GetMeta(ctx, bucket, key)
	if cacheErr != nil {
		// A transient metadata failure is not absence. The miss below is
		// authoritative — served for an existing object it would make the caller
		// drop its cached copy — so this must surface as a retryable error.
		metrics.RecordRequest(operation, "error", time.Since(start).Seconds())
		return cacheErr
	}
	if found && meta != nil && meta.BodyUpstream {
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
		// The mode's one body forward. The forward itself never populates;
		// a mis-tiered small object is healed by the background re-tier, which
		// carries its own guards (see maybeRetierOnRead).
		if r.Method == http.MethodGet {
			s.maybeRetierOnRead(bucket, key, accessKey, secretKey, meta)
		}
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

	declaredSize, sized := originlessPutSize(r)

	// Local-tier eligibility, before any metadata lookup: a validated caller
	// (the engine serves from cache with no upstream auth check), a plain
	// object PUT, and a declared size within the threshold. Everything else
	// forwards and never needs the prior version — a metadata failure must not
	// block a PUT that goes upstream anyway (including the unknown-key writes
	// that bootstrap credential learning).
	if result == AuthValidated && originlessPlainObject(r) && sized && declaredSize <= s.config.Cache.SizeThreshold {
		// The prior version's tier decides what an overwrite must clean up and
		// where a conditional write is evaluated, so a failed lookup cannot be
		// read as "no prior". Fail retryably instead.
		prior, found, cacheErr := s.cache.GetMeta(ctx, bucket, key)
		if cacheErr != nil {
			metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
			return cacheErr
		}
		if !found {
			prior = nil
		}
		conditional := r.Header.Get("If-Match") != "" || r.Header.Get("If-None-Match") != ""

		// A conditional write against an upstream-tier prior forwards: the
		// engine can only evaluate preconditions against a local body, and
		// upstream owns that object's version.
		if !(conditional && prior != nil && prior.BodyUpstream) {
			rec := &statusRecorder{ResponseWriter: w}
			err := s.HandleOriginlessPut(rec, r)
			if err == nil && rec.wroteSuccess() && prior != nil && prior.BodyUpstream {
				// Small write displaced an upstream-tier version: remove the
				// upstream copy so it doesn't linger as an orphan. Bound to the
				// displaced ETag so it can never remove a newer object that a
				// concurrent large write put in its place.
				s.deleteUpstreamObjectAsync(bucket, key, prior.ETag, accessKey, secretKey)
			}
			return err
		}
	}

	// Upstream tier: pass the PUT through, then stamp the local metadata marker
	// that makes this object exist in TAG's authoritative view.
	//
	// No pre-forward invalidation, unlike HandlePutObject: for a local-tier
	// prior the cache holds the ONLY copy, and a failed forward must leave it
	// intact — S3 semantics say a rejected PUT changes nothing. Reads racing
	// the in-flight PUT serve the prior version, which is the atomic-replace
	// behavior clients expect. The one read-triggered populate in this mode —
	// the re-tier — cannot be blocked by tombstone ordering here (this path
	// writes none); it defends itself with an identity-guarded commit instead
	// (see maybeRetierOnRead).
	// No post-success invalidation either: the marker overwrites the prior
	// metadata directly (a displaced local body ages out by TTL, the engine's
	// own overwrite semantics), which lets the marker be stamped with the
	// handler's START — see putUpstreamMarker for why that closes the
	// concurrent-DELETE resurrection race.
	// Capture the displaced prior before forwarding — tolerated, never blocking:
	// it only arms the identity guard of the failure sweep in putUpstreamMarker.
	// A failed lookup leaves the prior unknown, and the sweep then refuses to
	// delete anything rather than guess.
	var prior *cache.CachedObjectMeta
	priorKnown := false
	if s.cache.IsEnabled() {
		if m, found, cacheErr := s.cache.GetMeta(ctx, bucket, key); cacheErr == nil {
			priorKnown = true
			if found {
				prior = m
			}
		}
	}

	rec := &statusRecorder{ResponseWriter: w}
	err = s.forwarder.Forward(ctx, rec, r)

	if err == nil && rec.wroteSuccess() && s.cache.IsEnabled() {
		s.putUpstreamMarker(r, w.Header().Get("ETag"), bucket, key, start, prior, priorKnown)
	}

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("PutObject", status, time.Since(start).Seconds())
	return err
}

// putUpstreamMarker stores the metadata-only entry for an object whose body
// was just written upstream, replacing whatever metadata the displaced version
// left. Headers come from the PUT request (the same mapping as every populate
// path); the ETag comes from upstream's response.
//
// The write stamp is the handler's START, from before the forward began. Any
// DELETE that runs concurrently with — or after — this PUT writes its
// tombstone after that instant, so the tombstone-aware write refuses the
// marker and a deleted object can never be resurrected as metadata. The
// forward path writes no tombstones of its own between start and here, so
// only a genuine DELETE can suppress the marker.
//
// When the marker cannot be established (no response ETag, store failure, or
// tombstone suppression), the entry converges on an authoritative miss via
// invalidateDisplacedTieredMeta: the displaced prior is invalidated so a
// stale version cannot keep serving, while a newer write that raced in keeps
// the key. The client's 200 stands (the object IS stored upstream), and the
// caller's ordinary miss handling re-populates on the next read. Failures log
// at Warn (flood-safe: only successful 2xx PUTs reach here).
func (s *Service) putUpstreamMarker(r *http.Request, etag, bucket, key string, start time.Time, prior *cache.CachedObjectMeta, priorKnown bool) {
	if etag == "" {
		s.invalidateDisplacedTieredMeta(bucket, key, prior, priorKnown)
		log.Warn().Str("bucket", bucket).Str("key", key).Msg("Upstream PUT response had no ETag - no tier marker; object reads as a miss until re-put")
		return
	}
	meta := cache.MetaFromHTTPHeaders(bucket, key, http.StatusOK, r.Header)
	meta.ETag = etag
	meta.BodyUpstream = true
	meta.LastModified = time.Now().Unix()
	if declaredSize, ok := originlessPutSize(r); ok {
		meta.ContentLength = declaredSize
	}
	// Mirror the engine's PUT: upstream stores the decoded object, so a
	// streaming-signed upload's aws-chunked token is wire framing, not the
	// stored object's encoding — a HEAD advertising it would mislabel the
	// bytes a forwarded GET returns.
	meta.ContentEncoding = strings.Join(r.Header.Values("Content-Encoding"), ",")
	if IsStreamingPayload(r.Header.Get("X-Amz-Content-Sha256")) {
		meta.ContentEncoding = stripAWSChunkedToken(meta.ContentEncoding)
	}

	ttl := int(s.config.Cache.TTL.Seconds())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wrote, err := s.cache.PutMetaTombstoneAware(ctx, bucket, key, meta, ttl, start.UnixNano())
	if err != nil {
		s.invalidateDisplacedTieredMeta(bucket, key, prior, priorKnown)
		log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Failed to write upstream tier marker; object reads as a miss until re-put")
		return
	}
	if !wrote {
		// Suppressed by a newer tombstone: a DELETE won the key. Its own
		// invalidation normally removes the prior metadata; the guarded sweep
		// covers the case where that removal failed after the tombstone landed,
		// so the stale prior cannot outlive the delete.
		s.invalidateDisplacedTieredMeta(bucket, key, prior, priorKnown)
	}
}

// invalidateDisplacedTieredMeta converges a key on an authoritative miss
// after a marker could not be established, without either destroying a newer
// racing write or deleting blind: the entry is dropped only when it still IS
// the displaced prior captured before the forward (same ETag, same tier).
// Anything else present is a newer write and keeps the key — this PUT's
// upstream copy is left as an orphan for the upstream bucket's expiry.
//
// No identity, no delete: when the prior is unknown (its pre-forward lookup
// failed) or the current entry cannot be read, nothing is removed and the
// possibly-stale prior serves until TTL. Both cases require the metadata
// store to be failing already — an unguarded delete there would trade a
// bounded staleness window for the unbounded loss of a racing local write
// that has no upstream copy. The compare-then-delete pair is not atomic; the
// residual race is a write landing between the two, which the store cannot
// close without a conditional delete primitive.
func (s *Service) invalidateDisplacedTieredMeta(bucket, key string, prior *cache.CachedObjectMeta, priorKnown bool) {
	if !priorKnown {
		log.Warn().Str("bucket", bucket).Str("key", key).Msg("Tier marker failed with unknown prior; possibly-stale metadata serves until TTL")
		return
	}
	if prior == nil {
		// Nothing predated this PUT: anything present now is a newer write.
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cur, found, err := s.cache.GetMeta(ctx, bucket, key)
	if err != nil {
		log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Tier marker failed and prior could not be re-read; possibly-stale metadata serves until TTL")
		return
	}
	if !found || cur == nil || cur.ETag != prior.ETag || cur.BodyUpstream != prior.BodyUpstream {
		// Already gone, or replaced by a newer write.
		return
	}
	s.invalidateObject(ctx, bucket, key)
}

// retierFetchTimeout bounds the background re-tier fetch and store.
const retierFetchTimeout = 60 * time.Second

// maybeRetierOnRead heals a mis-tiered object: a validated GET hit an
// upstream-tier marker whose size fits the local tier — typically a small PUT
// that was forwarded before its key was learned. A background fetch moves the
// body into the local tier so subsequent reads stop paying a body forward.
//
// Guards, in order:
//   - dedup: one re-tier in flight per key, others ride the existing marker;
//   - budget: the buffer mirrors the engine's PUT (the allocation IS the
//     reservation, size+1 to detect an overlong body), shed non-blocking;
//   - version: the fetched ETag must match the marker's — anything else means
//     a concurrent write replaced the object, whose state must be left alone;
//   - identity commit: the entry is re-written only if it still IS the marker
//     (same ETag, still upstream-tier) — the same compare-then-write guard as
//     invalidateDisplacedTieredMeta, with the same documented residual race;
//   - tombstones: the store is stamped from BEFORE the fetch, so a DELETE
//     racing the re-tier writes a provably newer tombstone and blocks it.
//
// The upstream copy is left as an orphan for the upstream bucket's expiry —
// deleting it here could race a concurrent write of the same key.
func (s *Service) maybeRetierOnRead(bucket, key, accessKey, secretKey string, marker *cache.CachedObjectMeta) {
	if marker.ContentLength < 0 || marker.ContentLength > s.config.Cache.SizeThreshold {
		return
	}
	if accessKey == "" || secretKey == "" || !s.cache.IsEnabled() {
		return
	}
	inflightKey := bucket + "|" + key
	if _, loaded := s.retierInflight.LoadOrStore(inflightKey, struct{}{}); loaded {
		return
	}

	bufSize := marker.ContentLength + 1
	weight := bufSize
	if s.populateBudget != nil && weight > s.populateBudget.total {
		s.retierInflight.Delete(inflightKey)
		metrics.RecordTieredRetier("shed")
		return
	}
	if !s.acquireCacheSlot(context.Background(), weight, priorityReadMiss) {
		s.retierInflight.Delete(inflightKey)
		metrics.RecordTieredRetier("shed")
		return
	}

	etag := marker.ETag
	go func() {
		defer s.retierInflight.Delete(inflightKey)
		defer s.releaseCacheSlot(weight)
		ctx, cancel := context.WithTimeout(context.Background(), retierFetchTimeout)
		defer cancel()

		// Stamp BEFORE the fetch: a DELETE racing this populate writes its
		// tombstone after this instant and provably blocks the commit below.
		stamp := time.Now().UnixNano()

		resp, err := s.forwarder.DoFullObjectRequest(ctx, bucket, key, accessKey, secretKey)
		if err != nil {
			metrics.RecordTieredRetier("error")
			log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Re-tier fetch failed")
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK || resp.Header.Get("ETag") != etag {
			// Gone or replaced by a concurrent write; its state wins the key.
			_, _ = io.Copy(io.Discard, resp.Body)
			metrics.RecordTieredRetier("changed")
			return
		}

		buf := make([]byte, bufSize)
		n, err := io.ReadFull(resp.Body, buf)
		if err == nil || (!errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF)) || int64(n) != marker.ContentLength {
			// Longer than the marker declared, shorter (truncated read), or a
			// transport failure: nothing safe to store.
			metrics.RecordTieredRetier("error")
			log.Debug().Err(err).Int("read", n).Str("bucket", bucket).Str("key", key).Msg("Re-tier body mismatch")
			return
		}
		body := buf[:marker.ContentLength]

		// Identity-guarded commit: only re-write the entry if it still IS the
		// marker this re-tier was triggered by.
		cur, found, gerr := s.cache.GetMeta(ctx, bucket, key)
		if gerr != nil || !found || cur == nil || cur.ETag != etag || !cur.BodyUpstream {
			metrics.RecordTieredRetier("changed")
			return
		}

		meta := cache.MetaFromHTTPHeaders(bucket, key, http.StatusOK, resp.Header)
		// The stored body is authoritative for length — a chunked response
		// carries no Content-Length header for MetaFromHTTPHeaders to copy.
		meta.ContentLength = int64(len(body))
		ttl := int(s.config.Cache.TTL.Seconds())
		if _, err := s.cache.PutWithMetaStreamTombstoneAware(ctx, bucket, key, meta, bytes.NewReader(body), ttl, stamp); err != nil {
			metrics.RecordTieredRetier("error")
			log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Re-tier store failed")
			return
		}
		metrics.RecordTieredRetier("retiered")
	}()
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
	if cacheErr != nil {
		// Falling through would forward the DELETE, ack 204 upstream, and leave
		// a possibly local-tier copy being served. Fail retryably instead.
		metrics.RecordRequest("DeleteObject", "error", time.Since(start).Seconds())
		return true, cacheErr
	}
	if !found || meta == nil || meta.BodyUpstream {
		return false, nil
	}
	return true, s.HandleOriginlessDelete(w, r)
}

// deleteUpstreamObjectAsync issues the cross-tier cleanup DELETE in the
// background, signed with the (validated) caller's credentials. Best-effort:
// a failure leaves an orphan that the cache bucket's own expiry collects.
func (s *Service) deleteUpstreamObjectAsync(bucket, key, etag, accessKey, secretKey string) {
	if accessKey == "" || secretKey == "" {
		return
	}
	if etag == "" {
		// No ETag to bind the delete to — an unconditional delete could remove
		// a newer object, so the orphan is left for upstream expiry instead.
		metrics.RecordTieredCleanupSkipped("no_etag")
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), tieredCleanupTimeout)
		defer cancel()
		resp, err := s.forwarder.DoObjectDeleteRequest(ctx, bucket, key, etag, accessKey, secretKey)
		if err != nil {
			// Debug, not Info: per-request error logs flood under an upstream
			// outage; tag_tiered_cleanup_total{outcome} is the visibility signal,
			// with outcome classification owned by the metrics layer.
			metrics.RecordTieredCleanup(0, err)
			log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cross-tier cleanup delete failed")
			return
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		metrics.RecordTieredCleanup(resp.StatusCode, nil)
		// 404 (already gone) and 412 (If-Match lost to a newer version, which
		// must be left alone) are completed outcomes, not failures.
		if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusPreconditionFailed {
			log.Debug().Int("status", resp.StatusCode).Str("bucket", bucket).Str("key", key).Msg("Cross-tier cleanup delete rejected")
		}
	}()
}
