package proxy

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"strconv"
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
//         directly.
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
	// from it makes HEAD-as-existence callers — anything that skips re-population
	// when a HEAD says the object exists — skip the healing that would make the
	// entry servable again. A probe-to-serve race can still truncate a
	// concurrent eviction; the gate narrows the window, nothing can close it.
	if !s.entryServable(ctx, bucket, key, meta) {
		return s.originlessMiss(w, r, operation, start)
	}

	// Conditional requests are answered from the cached metadata; there is no
	// upstream for a client's Cache-Control to revalidate against, so no-cache and
	// no-store are simply not consulted — the cached copy is the only copy.
	if writePreconditionFailed(w, r, meta) {
		metrics.RecordRequest(operation, "success", time.Since(start).Seconds())
		return nil
	}
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

// HandleOriginlessBucket answers the bucket-lifecycle ceremony standard S3
// tooling insists on. Origin-less TAG has no buckets — the keyspace is implicit
// — so creation and deletion are honest no-ops: PUT (CreateBucket) answers 200,
// HEAD 200, DELETE 204. GET is a listing, served by HandleOriginlessList.
// Accepting the ceremony lets stock fixtures (SDKs, warp, ceph s3-tests) run.
func (s *Service) HandleOriginlessBucket(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	// Same query rule as objects: the SDK appends ?x-id=CreateBucket etc. to the
	// ceremony calls too, and rejecting the tag would 501 every stock client.
	if !originlessPlainObject(r) {
		return s.HandleOriginlessUnsupported(w, r)
	}
	switch r.Method {
	case http.MethodPut, http.MethodHead:
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	default:
		return s.HandleOriginlessUnsupported(w, r)
	}
	metrics.RecordRequest("Bucket", "success", time.Since(start).Seconds())
	return nil
}

// writePreconditionFailed answers the 412 preconditions (RFC 7232 order:
// If-Match before If-Unmodified-Since) from cached metadata. Returns true when
// it wrote the response. The 304 conditionals (If-None-Match/If-Modified-Since)
// are evaluated separately, after these.
func writePreconditionFailed(w http.ResponseWriter, r *http.Request, meta *cache.CachedObjectMeta) bool {
	if im := r.Header.Get("If-Match"); im != "" {
		if im != "*" && !meta.MatchesETag(im) {
			s3err.WriteError(w, r, s3err.ErrPreconditionFailed)
			return true
		}
		// RFC 7232 §3.4: when If-Match is present, If-Unmodified-Since is ignored
		// — a matching ETag with a stale date must serve, not 412.
		return false
	}
	if ius := r.Header.Get("If-Unmodified-Since"); ius != "" {
		if t, err := http.ParseTime(ius); err == nil && meta.IsModifiedSince(t) {
			s3err.WriteError(w, r, s3err.ErrPreconditionFailed)
			return true
		}
	}
	return false
}

// originlessPutSize returns the number of body bytes a PUT declares. For a
// streaming (aws-chunked) payload that is X-Amz-Decoded-Content-Length —
// REQUIRED, and zero is valid (the SDK's empty-body default); sizing a framed
// body by its wire Content-Length would reject valid uploads as IncompleteBody.
// For a plain payload it is Content-Length, which Go reports as -1 when absent.
func originlessPutSize(r *http.Request) (int64, bool) {
	if IsStreamingPayload(r.Header.Get("X-Amz-Content-Sha256")) {
		dcl := r.Header.Get("X-Amz-Decoded-Content-Length")
		if dcl == "" {
			return 0, false
		}
		n, err := strconv.ParseInt(dcl, 10, 64)
		return n, err == nil && n >= 0
	}
	if r.ContentLength < 0 {
		return 0, false
	}
	return r.ContentLength, true
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

	// Conditional writes. Check-then-store, not atomic — acceptable for a cache
	// tier, and it gives callers the idiom that matters: If-None-Match: * turns
	// put-if-absent into one request instead of an exists-probe plus a racy PUT.
	// Semantics follow the ceph suite: If-Match against a MISSING object answers
	// NoSuchKey (there is nothing to match), a present-but-different ETag is the
	// 412; If-None-Match refuses when the object exists.
	ifMatch := r.Header.Get("If-Match")
	ifNoneMatch := r.Header.Get("If-None-Match")
	if ifMatch != "" || ifNoneMatch != "" {
		existing, found, merr := s.cache.GetMeta(ctx, bucket, key)
		if merr != nil {
			metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
			return merr
		}
		// Existence means SERVABLE existence — the same gate every read uses. An
		// incomplete entry (orphaned meta, missing blocks) is invisible on every
		// request shape, so If-None-Match:* must store over it (that IS the
		// healing put-if-absent) and If-Match must answer NoSuchKey, not 412.
		exists := found && existing != nil && s.entryServable(ctx, bucket, key, existing)
		switch {
		case ifMatch != "" && !exists:
			s3err.WriteError(w, r, s3err.ErrNoSuchKey)
			metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
			return nil
		case ifMatch != "" && ifMatch != "*" && !existing.MatchesETag(ifMatch):
			s3err.WriteError(w, r, s3err.ErrPreconditionFailed)
			metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
			return nil
		case ifNoneMatch != "" && exists && (ifNoneMatch == "*" || existing.MatchesETag(ifNoneMatch)):
			s3err.WriteError(w, r, s3err.ErrPreconditionFailed)
			metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
			return nil
		}
	}

	// Decoded size, not wire size: a streaming-signed SDK upload frames the body
	// (aws-chunked) and declares the real length in X-Amz-Decoded-Content-Length.
	// Judging the threshold by wire length would reject payloads that fit.
	//
	// The declared size is REQUIRED, as on real S3 (411 without one): the budget
	// reservation below must equal the bytes this handler can actually hold, and
	// with no declared size the only safe reservation would be the full threshold
	// for every request. Zero is a valid declaration — an empty object is the AWS
	// SDK's default for empty bodies and must not be sized by its wire framing.
	declaredSize, ok := originlessPutSize(r)
	if !ok {
		s3err.WriteError(w, r, s3err.ErrMissingContentLength)
		metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
		return nil
	}
	if declaredSize > s.config.Cache.SizeThreshold {
		s3err.WriteError(w, r, s3err.ErrEntityTooLarge)
		metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
		return nil
	}

	// The buffer below is real memory held for the duration of the store, so it
	// draws on the same populate budget and count ceiling as every other cache
	// write. The reservation is EXACTLY the limiter's bound — declared size plus
	// the one detection byte — never a clamped or nominal figure: a reservation
	// smaller than the possible buffer re-creates the OOM the budget prevents.
	//
	// Admission is NON-BLOCKING (the read-miss path): a client-facing PUT must
	// shed with a retryable SlowDown immediately, not park on the budget's
	// condition variable — a reservation larger than the whole budget would
	// otherwise wait out the server timeout before failing. An object too large
	// to EVER fit the configured budget is a configuration mismatch and answers
	// EntityTooLarge up front rather than SlowDown forever.
	weight := declaredSize + 1
	if s.populateBudget != nil && weight > s.populateBudget.total {
		s3err.WriteError(w, r, s3err.ErrEntityTooLarge)
		metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
		return nil
	}
	if !s.acquireCacheSlot(ctx, weight, priorityReadMiss) {
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

	// The allocation IS the reservation: one buffer of exactly declared+1 bytes,
	// filled with ReadFull. io.ReadAll would grow its backing array geometrically
	// and could retain up to ~2x the reservation — the undercount the budget
	// exists to prevent. The extra byte detects a body longer than declared; a
	// shorter one is an IncompleteBody. Both are the client misdescribing the
	// request, not data to store under a wrong ETag.
	buf := make([]byte, weight)
	n, err := io.ReadFull(reader, buf)
	switch {
	case err == nil:
		// Filled declared+1 bytes: the body is longer than declared.
		s3err.WriteError(w, r, s3err.ErrIncompleteBody)
		metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
		return nil
	case err == io.EOF || err == io.ErrUnexpectedEOF:
		if int64(n) != declaredSize {
			s3err.WriteError(w, r, s3err.ErrIncompleteBody)
			metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
			return nil
		}
	default:
		metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
		return err
	}
	body := buf[:declaredSize]

	sum := md5.Sum(body)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`

	// Same header→meta mapping as the proxying populate path, then override
	// what a terminal store owns: the ETag is computed (never client-supplied),
	// ContentLength is the decoded body (the wire length counts chunk framing),
	// Content-Encoding drops the aws-chunked token (that layer was decoded above;
	// dropping the header entirely would serve gzip bytes read as the raw
	// object), and LastModified is the write time, not a client header.
	meta := cache.MetaFromHTTPHeaders(bucket, key, http.StatusOK, r.Header)
	meta.ETag = etag
	meta.ContentLength = int64(len(body))
	meta.ContentEncoding = stripAWSChunkedToken(r.Header.Get("Content-Encoding"))
	meta.LastModified = time.Now().Unix()

	// Same whole-vs-block boundary as proxying mode (size, not access pattern),
	// through the same writer: putBlocksFromStream is the shared full-object
	// populate path, so block entries written here are indistinguishable from
	// proxy-populated ones — one read path, one visibility gate.
	ttl := int(s.config.Cache.TTL.Seconds())
	if s.config.Cache.IsBlockCachingEnabled() && meta.ContentLength >= s.config.Cache.BlockSize {
		meta.BlockSize = s.config.Cache.BlockSize
		// writeStartTime is UnixNano — the tombstone gate compares against
		// nanosecond stamps; seconds would read every live tombstone as newer
		// and silently skip the meta write under a 200.
		if err := s.putBlocksFromStream(ctx, bucket, key, meta, bytes.NewReader(body), ttl, start.UnixNano()); err != nil {
			metrics.RecordRequest("PutObject", "error", time.Since(start).Seconds())
			return err
		}
	} else if err := s.cache.PutWithMeta(ctx, bucket, key, meta, body, ttl); err != nil {
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
// explicitly (a caller expiring objects on its own schedule) gets prompt removal.
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

// multiDeleteResult is the S3 DeleteResult response.
type multiDeleteResult struct {
	XMLName xml.Name           `xml:"DeleteResult"`
	Xmlns   string             `xml:"xmlns,attr"`
	Deleted []multiDeletedItem `xml:"Deleted"`
}

type multiDeletedItem struct {
	Key string `xml:"Key"`
}

// HandleOriginlessMultiDelete implements POST ?delete: invalidate every named
// key. Deleting an absent key is a success, as on S3 — invalidation is
// idempotent — so there is no Errors list to produce. Quiet mode omits the
// per-key confirmations.
func (s *Service) HandleOriginlessMultiDelete(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	bucket, _ := ParseBucketKey(r)

	q := r.URL.Query()
	q.Del("delete")
	q.Del("x-id")
	if len(q) > 0 {
		return s.HandleOriginlessUnsupported(w, r)
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		metrics.RecordRequest("DeleteObjects", "error", time.Since(start).Seconds())
		return err
	}
	var req deleteObjectsRequest
	if err := xml.Unmarshal(body, &req); err != nil || len(req.Objects) == 0 || len(req.Objects) > 1000 {
		s3err.WriteError(w, r, s3err.ErrInvalidRequest)
		metrics.RecordRequest("DeleteObjects", "error", time.Since(start).Seconds())
		return nil
	}

	res := multiDeleteResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	for _, obj := range req.Objects {
		s.invalidateObject(r.Context(), bucket, obj.Key)
		if !req.Quiet {
			res.Deleted = append(res.Deleted, multiDeletedItem{Key: obj.Key})
		}
	}

	out, err := xml.Marshal(res)
	if err != nil {
		metrics.RecordRequest("DeleteObjects", "error", time.Since(start).Seconds())
		return err
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xml.Header))
	w.Write(out)
	metrics.RecordRequest("DeleteObjects", "success", time.Since(start).Seconds())
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
