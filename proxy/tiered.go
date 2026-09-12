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
// Deliberately NOT reimplemented here (v1): listings, multipart transfers,
// copies, tagging, ACLs all pass through to upstream. A multipart COMPLETION
// does stamp a BodyUpstream marker (the assembled object is upstream-tier by
// construction — see HandleCompleteMultipartUpload), so multipart-written
// objects exist in the authoritative view; objects created upstream without
// any marker-stamping op (a server-side copy) still read as misses through
// TAG until written again.

import (
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

// forwardStatus maps a Forward result to the request-metric status label.
func forwardStatus(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

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
		metrics.RecordRequest(operation, "auth_error", metrics.SourceLocal, time.Since(start).Seconds())
		return err
	}
	if result != AuthValidated {
		// Unvalidated requests forward to upstream (the auth authority) — an
		// upstream-sourced response, recorded like every sibling path so the
		// mode's traffic is visible in tag_requests_total.
		ferr := s.forwarder.Forward(ctx, w, r)
		metrics.RecordRequest(operation, forwardStatus(ferr), metrics.SourceUpstream, time.Since(start).Seconds())
		return ferr
	}

	meta, metaVersion, found, cacheErr := s.cache.GetMetaWithVersion(ctx, bucket, key)
	if cacheErr != nil {
		// A transient metadata failure is not absence. The miss below is
		// authoritative — served for an existing object it would make the caller
		// drop its cached copy — so this must surface as a retryable error.
		metrics.RecordRequest(operation, "error", metrics.SourceLocal, time.Since(start).Seconds())
		return cacheErr
	}
	if found && meta != nil && meta.BodyUpstream {
		// Upstream tier: the local metadata answers everything except a GET body.
		if r.Method == http.MethodHead && originlessPlainObject(r) {
			// Same conditional-then-serve shape as the engine's HEAD path,
			// minus its servability probe: a marker has no local body to
			// probe, and the metadata alone is the authoritative answer.
			if s.answerConditionalsFromMeta(w, r, meta, operation, start) {
				return nil
			}
			serveMetaHit(w, meta, operation, start)
			return nil
		}
		// The mode's one body forward. The forward itself never populates;
		// a mis-tiered small object is healed by the background re-tier, which
		// carries its own guards (see maybeRetierOnRead).
		// Plain-object GETs only: a sub-resource GET (?tagging, ?acl, …)
		// forwards through this branch too, and healing on it would launch a
		// full-body download the triggering request never serves.
		if r.Method == http.MethodGet && originlessPlainObject(r) {
			s.maybeRetierOnRead(bucket, key, accessKey, secretKey, meta)
		}
		// The mode's principal body traffic — record it (upstream-sourced),
		// or a forward-heavy tiered node reads as ~100% local.
		ferr := s.forwarder.Forward(ctx, w, r)
		metrics.RecordRequest(operation, forwardStatus(ferr), metrics.SourceUpstream, time.Since(start).Seconds())
		return ferr
	}

	// Local tier, or no metadata at all: the engine serves it, and its miss is
	// the authoritative NoSuchKey. The metadata read ABOVE — the one the tier
	// decision was made from — is threaded through: a second read here could
	// see a large PUT's marker committed in between and mint a false
	// authoritative miss from a mid-overwrite snapshot.
	if !originlessPlainObject(r) {
		return s.HandleOriginlessUnsupported(w, r)
	}
	if !found {
		meta = nil
	}
	return s.serveOriginlessObject(w, r, operation, start, meta, metaVersion)
}

// handleTieredPut routes a PUT to its tier by declared size.
func (s *Service) handleTieredPut(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	ctx := r.Context()
	bucket, key := ParseBucketKey(r)

	// A write claims the key for its whole duration: any in-flight re-tier is
	// canceled and no new one can start until the claim is released (see
	// maybeRetierOnRead's guards).
	s.claimRetierWrite(bucket, key)
	defer s.releaseRetierWrite(bucket, key)

	result, accessKey, secretKey, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil {
		metrics.RecordRequest("PutObject", "auth_error", metrics.SourceLocal, time.Since(start).Seconds())
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
		priorMeta, priorVersion, found, cacheErr := s.cache.GetMetaWithVersion(ctx, bucket, key)
		if cacheErr != nil {
			metrics.RecordRequest("PutObject", "error", metrics.SourceLocal, time.Since(start).Seconds())
			return cacheErr
		}
		var prior *cache.CachedObjectMeta
		if found {
			prior = priorMeta
		}
		conditional := r.Header.Get("If-Match") != "" || r.Header.Get("If-None-Match") != ""

		// A conditional write against an upstream-tier prior forwards: the
		// engine can only evaluate preconditions against a local body, and
		// upstream owns that object's version.
		if !(conditional && prior != nil && prior.BodyUpstream) {
			rec := &statusRecorder{ResponseWriter: w}
			// Thread the SAME snapshot into the engine: the tier decision
			// above and the engine's precondition/token now share one read,
			// so a marker committing in between cannot make them disagree.
			err := s.handleOriginlessPut(rec, r, &putPrior{meta: priorMeta, version: priorVersion, found: found})
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
	// the re-tier — cannot be ordered against this path's writes here (it
	// performs none pre-forward); it defends itself with a claim plus its own
	// pre-fetch decision token instead (see maybeRetierOnRead).
	// No post-success invalidation either: the marker overwrites the prior
	// metadata directly (a displaced local body ages out by TTL, the engine's
	// own overwrite semantics), which lets the marker commit under the
	// PRE-FORWARD decision token — see putUpstreamMarker for why that closes
	// the concurrent-DELETE resurrection race.
	// Capture the displaced prior before forwarding — tolerated, never blocking:
	// it only arms the identity guard of the failure sweep in putUpstreamMarker.
	// A failed lookup leaves the prior unknown, and the sweep then refuses to
	// delete anything rather than guess.
	// Only a PLAIN object PUT writes the object and therefore owns the marker.
	// A sub-resource PUT with no dedicated route (?retention, ?legal-hold, …)
	// reaches this path too, but it does not create a new object version: it
	// must forward untouched — stamping a marker from its request headers
	// would replace the real metadata with the sub-resource call's shape, and
	// a 2xx response without an ETag would sweep the object's live metadata
	// into an authoritative miss.
	markerOwning := originlessPlainObject(r)

	var prior *cache.CachedObjectMeta
	var priorVersion uint64
	priorKnown := false
	if markerOwning {
		prior, priorVersion, priorKnown = s.captureMarkerPrior(ctx, bucket, key)
	}

	rec := &statusRecorder{ResponseWriter: w}
	err = s.forwarder.Forward(ctx, rec, r)

	if err == nil && rec.wroteSuccess() && markerOwning && s.cache.IsEnabled() {
		s.putUpstreamMarker(r, w.Header().Get("ETag"), bucket, key, prior, priorVersion, priorKnown)
	}

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("PutObject", status, metrics.SourceUpstream, time.Since(start).Seconds())
	return err
}

// putUpstreamMarker stores the metadata-only entry for an object whose body
// was just written upstream, replacing whatever metadata the displaced version
// left. Headers come from the PUT request (the same mapping as every populate
// path); the ETag comes from upstream's response.
//
// The store's expectation is the PRE-FORWARD decision token (read before the
// forward began). Any DELETE that runs concurrently with — or after — this
// PUT enters the store after that token, so the versioned write refuses the
// marker and a deleted object can never be resurrected as metadata. The
// forward path performs no meta writes of its own between the token read and
// here, so only a genuine racer can suppress the marker.
//
// When the marker cannot be established (no response ETag, store failure, or
// tombstone suppression), the entry converges on an authoritative miss via
// invalidateDisplacedTieredMeta: the displaced prior is invalidated so a
// stale version cannot keep serving, while a newer write that raced in keeps
// the key. The client's 200 stands (the object IS stored upstream), and the
// caller's ordinary miss handling re-populates on the next read. Failures log
// at Warn (flood-safe: only successful 2xx PUTs reach here).
// captureMarkerPrior reads the displaced prior and its decision-time token
// BEFORE a marker-stamping forward (large PUT, multipart completion). A
// failed lookup leaves the prior unknown — the marker is then not written and
// the sweep refuses to delete anything rather than guess (see
// commitUpstreamMarker). Tolerated, never blocking.
func (s *Service) captureMarkerPrior(ctx context.Context, bucket, key string) (prior *cache.CachedObjectMeta, priorVersion uint64, priorKnown bool) {
	if !s.cache.IsEnabled() {
		return nil, 0, false
	}
	if m, version, found, cacheErr := s.cache.GetMetaWithVersion(ctx, bucket, key); cacheErr == nil {
		priorKnown = true
		priorVersion = version
		if found {
			prior = m
		}
	}
	return prior, priorVersion, priorKnown
}

func (s *Service) putUpstreamMarker(r *http.Request, etag, bucket, key string, prior *cache.CachedObjectMeta, priorVersion uint64, priorKnown bool) {
	if etag == "" {
		s.invalidateDisplacedTieredMeta(bucket, key, prior, priorVersion, priorKnown)
		log.Warn().Str("bucket", bucket).Str("key", key).Msg("Upstream PUT response had no ETag - no tier marker; object reads as a miss until re-put")
		return
	}
	meta := cache.MetaFromHTTPHeaders(bucket, key, http.StatusOK, r.Header)
	meta.ETag = etag
	meta.BodyUpstream = true
	meta.LastModified = time.Now().Unix()
	if declaredSize, ok := originlessPutSize(r); ok {
		meta.ContentLength = declaredSize
	} else {
		// No decoded size declared (a streaming-signed PUT without
		// X-Amz-Decoded-Content-Length): MetaFromHTTPHeaders copied the
		// request's WIRE Content-Length, which counts aws-chunked framing.
		// Advertising it would misreport HEADs, and an inflated-but-under-
		// threshold value would send every validated GET into a re-tier whose
		// fetch can never match the length. Unknown is the honest value; the
		// re-tier skips unknown-length markers.
		meta.ContentLength = -1
	}
	// Mirror the engine's PUT: upstream stores the decoded object, so a
	// streaming-signed upload's aws-chunked token is wire framing, not the
	// stored object's encoding — a HEAD advertising it would mislabel the
	// bytes a forwarded GET returns.
	meta.ContentEncoding = strings.Join(r.Header.Values("Content-Encoding"), ",")
	if IsStreamingPayload(r.Header.Get("X-Amz-Content-Sha256")) {
		meta.ContentEncoding = stripAWSChunkedToken(meta.ContentEncoding)
	}

	s.commitUpstreamMarker(bucket, key, meta, prior, priorVersion, priorKnown)
}

// commitUpstreamMarker commits a BodyUpstream marker under the caller's
// PRE-FORWARD decision token — shared by the large-PUT path and the multipart
// completion. The marker replaces exactly the displaced prior: the token (the
// prior's live version, or the absence token when nothing predated the write)
// is the store's expectation, so a DELETE or write that raced in during the
// forward wins the key and the marker is refused into the convergence sweep.
// With the prior unknown (its lookup failed) there is no token to order by,
// and an unordered marker could resurrect over a DELETE that ran during the
// forward — so no marker is written: the object reads as a miss until re-put,
// on a path that already requires the metadata store to be failing.
func (s *Service) commitUpstreamMarker(bucket, key string, meta *cache.CachedObjectMeta, prior *cache.CachedObjectMeta, priorVersion uint64, priorKnown bool) {
	ttl := int(s.config.Cache.TTL.Seconds())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if !priorKnown {
		s.invalidateDisplacedTieredMeta(bucket, key, prior, priorVersion, priorKnown)
		log.Warn().Str("bucket", bucket).Str("key", key).Msg("Prior lookup failed - no tier marker; object reads as a miss until re-put")
		return
	}
	wrote, err := s.cache.PutMetaIfVersion(ctx, bucket, key, meta, ttl, priorVersion)
	if err != nil {
		s.invalidateDisplacedTieredMeta(bucket, key, prior, priorVersion, priorKnown)
		log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Failed to write upstream tier marker; object reads as a miss until re-put")
		return
	}
	if !wrote {
		// A racer entered the store after the pre-forward token was read — a
		// DELETE (whose own invalidation normally removes the prior metadata)
		// or a newer write. The guarded sweep covers the case where a DELETE's
		// removal failed after it won, so the stale prior cannot outlive it;
		// a newer write keeps the key through the sweep's identity guard.
		s.invalidateDisplacedTieredMeta(bucket, key, prior, priorVersion, priorKnown)
	}
}

// stampUpstreamMarkerAfterCompletion makes a multipart-completed object exist
// in TAG's authoritative view: completion assembles the body UPSTREAM, so the
// object is upstream-tier by construction, and without a marker it would read
// as an authoritative miss until re-put (the old v1 punt). Metadata comes
// from a HEAD to upstream when the caller's request validated (TAG's
// credentials work on the cache bucket, and the HEAD's Content-Length keeps
// range-reading callers honest); an ETag-only marker with unknown length is
// the fallback — GETs forward regardless, HEAD just omits the length, and
// the re-tier skips unknown-length markers. Commit and sweep are the
// large-PUT path's, under the same pre-forward token.
func (s *Service) stampUpstreamMarkerAfterCompletion(bucket, key, etag, accessKey, secretKey string, prior *cache.CachedObjectMeta, priorVersion uint64, priorKnown bool) {
	if etag == "" {
		s.invalidateDisplacedTieredMeta(bucket, key, prior, priorVersion, priorKnown)
		log.Warn().Str("bucket", bucket).Str("key", key).Msg("Multipart completion carried no ETag - no tier marker; object reads as a miss until re-put")
		return
	}
	// TWO PHASES, because the client already holds its 200: the ETag-only
	// marker commits IMMEDIATELY (one local cache op), so a read-after-write
	// never sees an authoritative NoSuchKey while an upstream HEAD is in
	// flight. The HEAD then runs in the background and upgrades the marker
	// with real metadata (Content-Length above all, for range-reading
	// callers) under an identity guard — GETs forward regardless, and the
	// re-tier skips unknown-length markers, so the window is HEAD-omits-
	// length, never wrong data.
	meta := &cache.CachedObjectMeta{
		Bucket: bucket, Key: key, ETag: etag, BodyUpstream: true,
		StatusCode: http.StatusOK, CachedAt: time.Now().Unix(),
		LastModified: time.Now().Unix(), ContentLength: -1,
	}
	s.commitUpstreamMarker(bucket, key, meta, prior, priorVersion, priorKnown)
	if accessKey == "" || secretKey == "" {
		return
	}
	go func() {
		hctx, hcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer hcancel()
		full, ok := s.headObjectMeta(hctx, bucket, key, accessKey, secretKey, etag)
		if !ok {
			// Gone or replaced already; whatever state won the key stands.
			return
		}
		// Identity-guarded upgrade: only the just-stamped unknown-length
		// marker for THIS ETag is upgraded, under its observed version — a
		// racing write refuses the commit and keeps the key.
		cur, curVersion, found, gerr := s.cache.GetMetaWithVersion(hctx, bucket, key)
		if gerr != nil || !found || cur == nil || !cur.BodyUpstream || cur.ETag != etag || cur.ContentLength >= 0 {
			return
		}
		full.BodyUpstream = true
		if full.LastModified == 0 {
			// A HEAD without Last-Modified must not zero the phase-1 write
			// stamp: If-Unmodified-Since fails closed on 0 and HEAD would
			// omit the header.
			full.LastModified = cur.LastModified
		}
		ttl := int(s.config.Cache.TTL.Seconds())
		if _, perr := s.cache.PutMetaIfVersion(hctx, bucket, key, full, ttl, curVersion); perr != nil {
			log.Debug().Err(perr).Str("bucket", bucket).Str("key", key).Msg("Completion marker upgrade failed; unknown-length marker serves until TTL")
		}
	}()
}

// headObjectMeta HEADs the object with TAG-signed credentials and builds its
// metadata, returning ok only when upstream answered 200 for EXACTLY the
// expected ETag — a mismatch means a concurrent overwrite superseded the
// caller's version, and describing the newer object under the older identity
// is the torn-pair hazard the write paths guard against. Shared by the
// completion-marker upgrade and meta-on-write (establishBlockMetaFromHead);
// each caller applies its mode-specific fields and commit discipline.
func (s *Service) headObjectMeta(ctx context.Context, bucket, key, accessKey, secretKey, expectETag string) (*cache.CachedObjectMeta, bool) {
	resp, err := s.forwarder.DoConditionalHeadRequest(ctx, bucket, key, accessKey, secretKey, "", 0)
	if err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Marker HEAD failed")
		return nil, false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("ETag") != expectETag {
		return nil, false
	}
	meta := cache.MetaFromHTTPHeaders(bucket, key, http.StatusOK, resp.Header)
	meta.ETag = expectETag
	return meta, true
}

// invalidateDisplacedTieredMeta converges a key on an authoritative miss
// after a marker could not be established, without either destroying a newer
// racing write or deleting blind: cache.DeleteIfETag removes the entry only
// while it still IS the displaced prior — the compare and the delete are one
// CAS, so the compare-then-delete window the pre-CAS helper documented is
// gone. Anything else present is a newer write and keeps the key; this PUT's
// upstream copy is left as an orphan for the upstream bucket's expiry.
//
// No identity, no delete: when the prior is unknown (its pre-forward lookup
// failed) nothing is removed and the possibly-stale prior serves until TTL —
// that path requires the metadata store to be failing already, and an
// unguarded delete there would trade a bounded staleness window for the
// unbounded loss of a racing local write that has no upstream copy.
func (s *Service) invalidateDisplacedTieredMeta(bucket, key string, prior *cache.CachedObjectMeta, priorVersion uint64, priorKnown bool) {
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
	// VERSION-guarded, not ETag-guarded: MD5 ETags collide on identical
	// bytes, so an ETag guard could delete a same-content successor write
	// that won the race (an idempotent client re-PUT) — an acked local-tier
	// only-copy. The observed version is exactly the snapshot this sweep is
	// entitled to remove; anything newer keeps the key.
	if _, err := s.cache.DeleteMetaIfVersion(ctx, bucket, key, priorVersion); err != nil {
		log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Tier marker failed and displaced prior could not be invalidated; possibly-stale metadata serves until TTL")
	}
}

// retierFetchTimeout bounds the background re-tier fetch and store.
const retierFetchTimeout = 60 * time.Second

// A marker version whose re-tier fetch found upstream changed/gone is not
// retried for this long — the next attempt costs a full discarded download.
const (
	maxRetierMismatchTracking = 4096
	retierMismatchCooldown    = 5 * time.Minute
)

// maybeRetierOnRead heals a mis-tiered object: a validated GET hit an
// upstream-tier marker whose size fits the local tier — typically a small PUT
// that was forwarded before its key was learned. A background fetch moves the
// body into the local tier so subsequent reads stop paying a body forward.
//
// Guards, in order:
//   - dedup: one re-tier in flight per key, others ride the existing marker;
//   - budget: STREAMED like the engine's PUT — the body goes straight into
//     the cache under a BodyRef, so the reservation is the fixed stream
//     weight, never the object; shed non-blocking;
//   - version: the fetched ETag must match the marker's — anything else means
//     a concurrent write replaced the object, whose state must be left alone;
//   - identity commit: the entry is re-written only if it still IS the marker
//     (same ETag, still upstream-tier);
//   - write claim: PUTs write no tombstones, so a concurrent PUT could land
//     between the identity check and the commit and be overwritten by the
//     older version — a local-tier PUT's ONLY copy, in the worst case. Every
//     tiered PUT therefore holds a per-key claim for its whole duration
//     (claimRetierWrite): taking it cancels the running re-tier, and while
//     held no new re-tier can register — a GET mid-PUT sees the old marker
//     but cannot start a heal against it. Claim and registration share one
//     mutex, so every interleaving lands on cancel-or-refuse. What remains is
//     two truly concurrent commits racing in the store — the same unordered
//     outcome S3 itself gives two concurrent writers;
//   - decision tokens: the commit's token is read BEFORE the fetch, so a
//     DELETE racing the re-tier enters the store provably after it and the
//     commit is refused (DELETEs need no cancellation — the coordinator's
//     ordering already covers them).
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
	// Backoff, not an outcome: a recent attempt for this exact marker version
	// already found upstream changed or gone (recorded then as "changed"), so
	// repeating the full-body fetch within the cooldown only burns bandwidth.
	if s.retierRecentMismatch != nil {
		if _, recent := s.retierRecentMismatch.Get(bucket + "|" + key + "|" + marker.ETag); recent {
			return
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), retierFetchTimeout)
	if !s.tryRegisterRetier(bucket, key, cancel) {
		cancel()
		return
	}

	// STREAMING heal: the body goes straight from the upstream response into
	// the cache under a per-write BodyRef (the engine PUT's pattern), so the
	// weight is the fixed stream buffering, never the object — a threshold
	// sized above the populate budget no longer makes markers unhealable.
	weight := int64(originlessPutStreamWeight)
	if !s.acquireCacheSlot(context.Background(), weight, priorityReadMiss) {
		s.unregisterRetier(bucket, key)
		cancel()
		metrics.RecordTieredRetier("shed")
		return
	}

	etag := marker.ETag
	go func() {
		defer s.unregisterRetier(bucket, key)
		defer s.releaseCacheSlot(weight)
		defer cancel()

		// Decision token BEFORE the fetch: a DELETE (or any write) racing this
		// populate enters the store after this token and provably refuses the
		// commit below — the token-model form of the old stamp-before-fetch.
		_, decToken, tfound, terr := s.cache.GetMetaWithVersion(ctx, bucket, key)
		if terr != nil || !tfound {
			metrics.RecordTieredRetier("changed")
			return
		}

		resp, err := s.forwarder.DoFullObjectRequest(ctx, bucket, key, accessKey, secretKey)
		if err != nil {
			// A write's cancellation is normal coordination, not a failure —
			// counting it as error would let routine PUT traffic drown the
			// metric's signal.
			if errors.Is(ctx.Err(), context.Canceled) {
				metrics.RecordTieredRetier("canceled")
				return
			}
			metrics.RecordTieredRetier("error")
			log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Re-tier fetch failed")
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK || resp.Header.Get("ETag") != etag {
			// Drain and COUNT: for a chunked 200 the discarded bytes are the
			// replacement body itself — the one true length a headerless
			// response can yield — consumed below by the converge rewrite.
			drained, drainErr := io.Copy(io.Discard, resp.Body)
			// Only a DEFINITIVE answer about the object counts as changed and
			// enters the backoff: a 200 with a different ETag (replaced) or a
			// 404 (gone). A transient status (5xx, 429, an auth blip) says
			// nothing about the object — recording it would suppress healing
			// for the whole cooldown and mislabel the outcome.
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
				if s.retierRecentMismatch != nil {
					s.retierRecentMismatch.Add(bucket+"|"+key+"|"+etag, struct{}{})
				}
				// The marker is now PROVEN wrong, and the pre-fetch token is
				// still in hand — converge it instead of leaving the
				// divergence authoritative for the marker's TTL. Gone (404 —
				// e.g. the cache bucket's own expiry collected the body):
				// remove the marker under the token, so HEAD stops answering
				// 200 for an object whose GET forwards to 404. Replaced (a
				// different live ETag): rewrite the marker from the response,
				// so HEAD advertises the object that actually serves. Either
				// commit refuses if anything else won the key meanwhile; a
				// refused or failed converge just leaves the backoff doing
				// its job until the next attempt.
				if resp.StatusCode == http.StatusNotFound {
					if _, derr := s.cache.DeleteMetaIfVersion(ctx, bucket, key, decToken); derr != nil {
						log.Debug().Err(derr).Str("bucket", bucket).Str("key", key).Msg("Re-tier converge: marker removal failed")
					}
				} else {
					fresh := cache.MetaFromHTTPHeaders(bucket, key, http.StatusOK, resp.Header)
					fresh.BodyUpstream = true
					// A chunked response carries no Content-Length header
					// (and Go's resp.ContentLength is -1 for exactly those),
					// but the drain above read the whole replacement body —
					// its count is the true length. A marker left
					// unknown-length is honest but re-tier-ineligible, so
					// use it when the drain completed cleanly.
					if fresh.ContentLength < 0 && drainErr == nil {
						fresh.ContentLength = drained
					}
					if _, perr := s.cache.PutMetaIfVersion(ctx, bucket, key, fresh, int(s.config.Cache.TTL.Seconds()), decToken); perr != nil {
						log.Debug().Err(perr).Str("bucket", bucket).Str("key", key).Msg("Re-tier converge: marker rewrite failed")
					}
				}
				metrics.RecordTieredRetier("changed")
				return
			}
			metrics.RecordTieredRetier("error")
			log.Debug().Int("status", resp.StatusCode).Str("bucket", bucket).Str("key", key).Msg("Re-tier fetch returned transient status")
			return
		}

		// Stream the body into the cache under a fresh BodyRef, limited to
		// CL+1 (the extra byte detects a longer-than-declared body) and
		// counted. The upstream ETag was verified above, so unlike the
		// engine PUT no digest needs computing — the ref exists purely so
		// the body can stream before the commit. A body that never commits
		// is a TTL-reclaimed orphan; definitive mismatches also delete it
		// eagerly below.
		ref := cache.NewBodyRef()
		src := &captureReader{r: io.LimitReader(resp.Body, marker.ContentLength+1)}
		putBodyErr := s.cache.PutBodyStream(ctx, bucket, key, ref, src, int64(s.config.Cache.TTL.Seconds()))
		discardRef := func() {
			// Detached context: the fetch ctx is CANCELED by a racing PUT —
			// precisely the interleaving whose staged body most needs
			// reclaiming — so the delete must not ride it.
			dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer dcancel()
			if derr := s.cache.DeleteBody(dctx, bucket, key, ref); derr != nil {
				log.Debug().Err(derr).Str("bucket", bucket).Str("key", key).Msg("Re-tier staged body delete failed; orphan ages out by TTL")
			}
		}
		if putBodyErr != nil {
			// Transport failure mid-body or a cache write fault: transient,
			// nothing safe to commit — and no backoff, a blip must not
			// suppress healing.
			discardRef()
			metrics.RecordTieredRetier("error")
			log.Debug().Err(putBodyErr).Int64("read", src.n).Str("bucket", bucket).Str("key", key).Msg("Re-tier body stream failed")
			return
		}
		if src.n != marker.ContentLength {
			// The body streamed COMPLETELY and its length contradicts the
			// marker (longer: the CL+1 limit filled; shorter: clean EOF
			// before CL). That is as definitive as a changed ETag — this
			// marker version can never re-tier — so it enters the same
			// backoff; without it the doomed full download repeats on every
			// validated GET for the marker's TTL.
			discardRef()
			if s.retierRecentMismatch != nil {
				s.retierRecentMismatch.Add(bucket+"|"+key+"|"+etag, struct{}{})
			}
			metrics.RecordTieredRetier("changed")
			log.Debug().Int64("read", src.n).Int64("declared", marker.ContentLength).Str("bucket", bucket).Str("key", key).Msg("Re-tier body length contradicts marker")
			return
		}

		// Identity-guarded commit: only re-write the entry if it still IS the
		// marker this re-tier was triggered by. The commit's expectation is
		// the PRE-FETCH decision token, so anything landing after that
		// instant — including during the fetch — refuses the commit
		// atomically; this re-read is purely the identity guard. The
		// cancellation check comes after it — a racing PUT cancels before it
		// stores, so an alive context here means no PUT has entered the store
		// ahead of us; the token precondition then holds the line for
		// whatever the claim window cannot see.
		cur, _, found, gerr := s.cache.GetMetaWithVersion(ctx, bucket, key)
		if gerr != nil || !found || cur == nil || cur.ETag != etag || !cur.BodyUpstream {
			discardRef()
			metrics.RecordTieredRetier("changed")
			return
		}
		if cerr := ctx.Err(); cerr != nil {
			discardRef()
			// Distinguish a write's cancellation (coordination) from the
			// 60-second deadline expiring (a genuine failure).
			if errors.Is(cerr, context.Canceled) {
				metrics.RecordTieredRetier("canceled")
			} else {
				metrics.RecordTieredRetier("error")
			}
			return
		}

		meta := cache.MetaFromHTTPHeaders(bucket, key, http.StatusOK, resp.Header)
		meta.BodyRef = ref
		// The streamed byte count is authoritative for length — a chunked
		// response carries no Content-Length header for MetaFromHTTPHeaders
		// to copy.
		meta.ContentLength = src.n
		ttl := int(s.config.Cache.TTL.Seconds())
		wrote, werr := s.cache.PutMetaIfVersion(ctx, bucket, key, meta, ttl, decToken)
		if werr != nil {
			// AMBIGUOUS commit (see the engine PUT's rule): the owner may
			// have applied it, so the staged body must not be deleted —
			// TTL reclaims a genuine orphan.
			metrics.RecordTieredRetier("error")
			log.Debug().Err(werr).Str("bucket", bucket).Str("key", key).Msg("Re-tier store failed")
			return
		}
		if !wrote {
			// A racer entered the store after the pre-fetch token: the object
			// stays on its current state (marker, newer write, or deleted) —
			// not a re-tier. Definitive refusal: reclaim the staged body.
			discardRef()
			metrics.RecordTieredRetier("changed")
			return
		}
		metrics.RecordTieredRetier("retiered")
	}()
}

// claimRetierWrite marks a PUT in flight for the key: it cancels any running
// re-tier and excludes new ones from starting until the matching release.
// PUTs write no tombstones, so this claim is what stops a re-tier from
// committing an older version over the PUT — for a local-tier PUT, the only
// copy. Held for the PUT's whole duration, it also covers re-tiers a GET
// might otherwise start mid-PUT against the still-visible old marker.
func (s *Service) claimRetierWrite(bucket, key string) {
	k := bucket + "|" + key
	s.retierMu.Lock()
	s.retierClaims[k]++
	if cancel := s.retierInflight[k]; cancel != nil {
		cancel()
	}
	s.retierMu.Unlock()
}

func (s *Service) releaseRetierWrite(bucket, key string) {
	k := bucket + "|" + key
	s.retierMu.Lock()
	if s.retierClaims[k]--; s.retierClaims[k] <= 0 {
		delete(s.retierClaims, k)
	}
	s.retierMu.Unlock()
}

// tryRegisterRetier registers a re-tier for the key unless one is already
// running or a write holds the claim. Registration and claim share one mutex,
// so every interleaving either cancels the re-tier or refuses to start it.
func (s *Service) tryRegisterRetier(bucket, key string, cancel context.CancelFunc) bool {
	k := bucket + "|" + key
	s.retierMu.Lock()
	defer s.retierMu.Unlock()
	if s.retierClaims[k] > 0 || s.retierInflight[k] != nil {
		return false
	}
	s.retierInflight[k] = cancel
	return true
}

func (s *Service) unregisterRetier(bucket, key string) {
	s.retierMu.Lock()
	delete(s.retierInflight, bucket+"|"+key)
	s.retierMu.Unlock()
}

// retierRunning reports whether a re-tier is in flight for the key (tests).
func (s *Service) retierRunning(bucket, key string) bool {
	s.retierMu.Lock()
	defer s.retierMu.Unlock()
	return s.retierInflight[bucket+"|"+key] != nil
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
		metrics.RecordRequest("DeleteObject", "auth_error", metrics.SourceLocal, time.Since(start).Seconds())
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
		metrics.RecordRequest("DeleteObject", "error", metrics.SourceLocal, time.Since(start).Seconds())
		return true, cacheErr
	}
	if !found || meta == nil || meta.BodyUpstream {
		return false, nil
	}
	return true, s.HandleOriginlessDelete(w, r)
}

// deleteUpstreamObjectAsync issues the cross-tier cleanup DELETE in the
// background, signed with the credentials the validated request resolved —
// which in the transparent-auth flow are TAG's OWN credentials, not the
// client's. Tiered deployments must therefore grant TAG's credentials delete
// permission on the upstream cache bucket (unlike proxy mode's read-only
// guidance, which targets customer buckets); with read-only credentials every
// cleanup is rejected 403 and orphans accumulate until bucket expiry.
// Best-effort: a failure leaves an orphan that the cache bucket's own expiry
// collects.
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
		// The If-Match guard below cannot protect a LIVE upstream object that
		// carries the SAME ETag: plain-PUT ETags are content MD5, so a
		// delete + re-put of identical bytes recreates the object under the
		// displaced prior's ETag, and this delayed delete would then remove
		// the body the current marker points at. Re-check the key's current
		// metadata first and abort when it is an upstream-tier marker for
		// exactly this ETag — that body is authoritative again, not an
		// orphan. (A racer between this check and the DELETE narrows to the
		// same-ETag re-establishment landing inside one round trip; the
		// If-Match still guards every different-ETag interleaving — Tigris
		// enforces conditional DELETEs against the object's current version,
		// so a different-ETag racer's body is provably left intact.)
		if cur, found, gerr := s.cache.GetMeta(ctx, bucket, key); gerr == nil && found && cur != nil && cur.BodyUpstream && cur.ETag == etag {
			metrics.RecordTieredCleanupSkipped("live_marker")
			return
		}
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
		// 404 (already gone) and 412 (If-Match lost to a newer version, which
		// must be left alone) are completed outcomes, not failures.
		if resp.StatusCode >= 300 {
			metrics.RecordTieredCleanup(resp.StatusCode, nil)
			if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusPreconditionFailed {
				log.Debug().Int("status", resp.StatusCode).Str("bucket", bucket).Str("key", key).Msg("Cross-tier cleanup delete rejected")
			}
			return
		}
		// The DELETE landed. REPAIR the residual same-ETag race the pre-check
		// cannot close: a concurrent large PUT of identical bytes can
		// recreate the upstream object under this ETag and publish its marker
		// between the check above and the DELETE landing, and MD5 ETags give
		// If-Match no way to tell the versions apart. If the key's CURRENT
		// metadata is a live marker for this ETag, the body it points at may
		// be the one just deleted — converge on the authoritative miss
		// instead of advertising it: the caller re-populates from its system
		// of record, and tiered semantics make "not cached" a complete, safe
		// answer. The removal commits under the OBSERVED VERSION, not the
		// ETag — identical content reuses MD5 ETags, so an ETag guard could
		// take out a same-content successor (a local-tier PUT or a re-tier
		// commit) that landed after this read; the version guard refuses
		// exactly those. One outcome is recorded per cleanup: deleted,
		// marker_repaired, or repair_failed.
		cur, curVersion, found, gerr := s.cache.GetMetaWithVersion(ctx, bucket, key)
		if gerr != nil {
			// The repair check could not run: a raced-in same-ETag marker may
			// remain authoritative over the just-deleted body until TTL. Its
			// own outcome, never folded into "deleted".
			metrics.RecordTieredCleanupSkipped("repair_failed")
			log.Debug().Err(gerr).Str("bucket", bucket).Str("key", key).Msg("Cleanup repair: post-delete read failed; a raced-in marker may serve until TTL")
			return
		}
		if !found || cur == nil || !cur.BodyUpstream || cur.ETag != etag {
			metrics.RecordTieredCleanup(resp.StatusCode, nil)
			return
		}
		repaired, derr := s.cache.DeleteMetaIfVersion(ctx, bucket, key, curVersion)
		switch {
		case derr != nil:
			metrics.RecordTieredCleanupSkipped("repair_failed")
			log.Debug().Err(derr).Str("bucket", bucket).Str("key", key).Msg("Cleanup repair: marker removal failed; stale marker serves until TTL")
		case repaired:
			metrics.RecordTieredCleanupSkipped("marker_repaired")
		default:
			// The entry moved since the read — a newer write owns the key and
			// keeps its metadata; the cleanup itself still completed.
			metrics.RecordTieredCleanup(resp.StatusCode, nil)
		}
	}()
}
