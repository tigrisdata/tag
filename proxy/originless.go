package proxy

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/s3err"
)

// Origin-less mode: TAG with no upstream, serving and storing on its own.
//
// The mode is expressed at the ROUTER: the server registers this handler set
// instead of the proxying one, so every path that assumes an upstream —
// revalidation, broadcast coalescing, background fetch, the proxy mutation
// handlers with their invalidate-before-forward ordering — is unreachable by
// construction. What this file does not implement, the mode cannot do.
//
// Trust model: THE NETWORK IS THE BOUNDARY. Origin-less TAG cannot validate
// signatures (signature validation learns keys from an upstream), so requests
// are served and accepted regardless of authentication or cached ACL — an
// Authorization header is ignored, not evaluated. Deploy this mode only on a
// network segment reachable solely by its intended callers; the explicit,
// contradiction-checked upstream.disabled switch is the consent for that trade.
//
// Reads:  GET/HEAD of one object from cache; a miss is NoSuchKey, the caller's
//         cue to fall back to its authoritative store.
// Writes: PUT stores the object in the local cache under cache.ttl; DELETE
//         invalidates. This is how the tier is populated — by its callers,
//         directly (e.g. the tigris gateway).
// Everything else — listings, multipart, copies, tagging, ACLs — answers 501.

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

	// Plain reads only: a query parameter (beyond the SDK's no-op x-id tag)
	// selects a representation or operation this mode does not implement —
	// ?versionId, ?partNumber, ?tagging — and serving the current full object for
	// those would be silently wrong data.
	if !originlessPlainObject(r) {
		return s.HandleOriginlessUnsupported(w, r)
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

// originlessPlainObject reports whether the request is a plain single-object
// operation: no query parameters beyond the SDK's no-op x-id tag, and no
// copy-source header (server-side copy needs a source read this mode does not
// implement as an operation).
func originlessPlainObject(r *http.Request) bool {
	if r.Header.Get("X-Amz-Copy-Source") != "" {
		return false
	}
	if r.URL.RawQuery == "" {
		return true
	}
	q := r.URL.Query()
	q.Del("x-id")
	return len(q) == 0
}

// HandleOriginlessPut stores an object directly into the local cache — the
// population path for this tier. The body is buffered to compute the ETag
// (bodies are keyed by ETag so each version is immutable), bounded by the cache
// size threshold, and stored under cache.ttl. Overwrites follow the proxying
// mode's semantics: new meta points at the new ETag-keyed body; the old body
// ages out by TTL.
func (s *Service) HandleOriginlessPut(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	ctx := r.Context()
	bucket, key := ParseBucketKey(r)

	if !originlessPlainObject(r) {
		return s.HandleOriginlessUnsupported(w, r)
	}
	if !s.cache.IsEnabled() {
		s3err.WriteError(w, r, s3err.ErrNotImplemented)
		metrics.RecordRequest("PutObject", "unsupported", time.Since(start).Seconds())
		return nil
	}

	// Decoded size, not wire size: a streaming-signed SDK upload frames the body
	// (aws-chunked) and declares the real length in X-Amz-Decoded-Content-Length.
	// Judging the threshold by wire length would reject payloads that fit.
	declaredSize := teeObjectSize(r)
	maxSize := s.config.Cache.SizeThreshold
	if declaredSize > maxSize {
		s3err.WriteError(w, r, s3err.ErrEntityTooLarge)
		metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
		return nil
	}

	// The buffer below is real memory held for the duration of the store, so it
	// draws on the same populate budget and count ceiling as every other cache
	// write — without this, concurrent large PUTs would allocate past the
	// configured budget unbounded. Shed with SlowDown when the budget declines:
	// under pressure a retryable 503 beats an OOM kill.
	weight := declaredSize
	if weight <= 0 {
		weight = smallObjectThreshold
	}
	if weight > s.perPopulateCap {
		weight = s.perPopulateCap
	}
	if !s.acquireCacheSlot(ctx, weight, priorityWarmWrite) {
		s3err.WriteError(w, r, s3err.ErrSlowDown)
		metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
		return nil
	}
	defer s.releaseCacheSlot(weight)

	// Unwrap AWS chunked framing so the stored bytes — and the ETag computed over
	// them — are the object, not the wire encoding. Storing the framing serves
	// corrupt bytes with a 200, which the caller cannot detect as a miss.
	reader := r.Body
	if IsStreamingPayload(r.Header.Get("X-Amz-Content-Sha256")) {
		reader = io.NopCloser(newAWSChunkedReader(r.Body))
	}

	// LimitReader at threshold+1: a body that exceeds the declared length or the
	// threshold is detected by the extra byte rather than silently truncated.
	body, err := io.ReadAll(io.LimitReader(reader, maxSize+1))
	if err != nil {
		metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
		return err
	}
	if int64(len(body)) > maxSize {
		s3err.WriteError(w, r, s3err.ErrEntityTooLarge)
		metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
		return nil
	}

	sum := md5.Sum(body)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`

	meta := &cache.CachedObjectMeta{
		Bucket: bucket, Key: key, StatusCode: http.StatusOK,
		ETag: etag, ContentLength: int64(len(body)),
		ContentType:  r.Header.Get("Content-Type"),
		CacheControl: r.Header.Get("Cache-Control"),
		CachedAt:     time.Now().Unix(), LastModified: time.Now().Unix(),
		UserMetadata: make(map[string]string),
	}
	for name, vals := range r.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-meta-") && len(vals) > 0 {
			meta.UserMetadata[strings.ToLower(name)] = vals[0]
		}
	}

	if err := s.cache.PutWithMeta(ctx, bucket, key, meta, body, int(s.config.Cache.TTL.Seconds())); err != nil {
		metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
		return err
	}

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
	metrics.RecordRequest("PutObject", "success", time.Since(start).Seconds())
	return nil
}

// HandleOriginlessDelete invalidates the object. Deletion is not required for
// correctness — entries lapse by cache.ttl — but a caller that expires objects
// explicitly (the tigris gateway may, on object expiry) gets prompt removal.
func (s *Service) HandleOriginlessDelete(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	bucket, key := ParseBucketKey(r)

	if !originlessPlainObject(r) {
		return s.HandleOriginlessUnsupported(w, r)
	}
	s.invalidateObject(r.Context(), bucket, key)
	w.WriteHeader(http.StatusNoContent)
	metrics.RecordRequest("DeleteObject", "success", time.Since(start).Seconds())
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
