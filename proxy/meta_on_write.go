package proxy

import (
	"context"
	"encoding/xml"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/tigrisdata/tag/cache"
)

// Metadata caching on write (prototype).
//
// TAG invalidates on write and caches nothing, so the FIRST read of a freshly
// written object must miss on metadata, round-trip upstream to establish it, and
// only then engage block mode. That miss is paid once per object, on the read that
// is most likely to be latency-sensitive.
//
// TAG already knows the object exists — it just proxied the write — and
// write_through.go already caches meta+body for a teed PutObject. What is missing
// is the multipart case, where TAG never sees the assembled body and therefore
// caches nothing at all. This closes that gap for METADATA only: the body stays
// uncached and blocks are fetched on demand, exactly as block mode already does
// for evicted blocks.
//
// It uses write_through's guards rather than inventing new ones: an authoritative
// HEAD supplies the meta, the ETag must match what the write returned (a concurrent
// overwrite means the HEAD describes a different object), and IsCacheable decides
// whether the object may be cached at all.

// cacheBlockMetaOnWrite establishes a block-mode entry for a just-written object
// without caching its body. Returns immediately; the HEAD runs detached, after the
// client's write response has been committed.
//
// writtenETag is the ETag the write returned, or "" when the caller does not have
// one — in which case the entry is not established, since without it a concurrent
// overwrite cannot be detected.
func (s *Service) cacheBlockMetaOnWrite(r *http.Request, bucket, key, writtenETag string) {
	if s.config == nil || !s.config.Cache.MetaOnWrite || !s.cache.IsEnabled() {
		return
	}
	if !s.config.Cache.IsBlockCachingEnabled() || s.config.Cache.BlockSize <= 0 {
		return
	}
	if writtenETag == "" {
		return
	}
	_, accessKey, secretKey, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil || accessKey == "" || secretKey == "" {
		return
	}

	dedupKey := "meta:" + bucket + "/" + key
	if _, loaded := s.activeBackgroundFetches.LoadOrStore(dedupKey, struct{}{}); loaded {
		return
	}
	go func() {
		defer s.activeBackgroundFetches.Delete(dedupKey)
		s.establishBlockMetaFromHead(bucket, key, accessKey, secretKey, writtenETag)
	}()
}

// establishBlockMetaFromHead HEADs the object and stamps a block-mode entry whose
// blocks are all absent. That is a legal state: block mode already serves entries
// with missing blocks by fetching them on demand (RFC 0001), which is the same path
// an evicted block takes. What it buys is that the first read resolves metadata from
// cache instead of paying an upstream round trip to discover it.
func (s *Service) establishBlockMetaFromHead(bucket, key, accessKey, secretKey, writtenETag string) {
	ctx, cancel := context.WithTimeout(context.Background(), backgroundFetchTimeout)
	defer cancel()

	// Stamp before the HEAD, so an invalidation landing mid-flight is newer than this
	// timestamp and blocks the meta write.
	writeStartTime := time.Now().UnixNano()

	resp, err := s.forwarder.DoConditionalHeadRequest(ctx, bucket, key, accessKey, secretKey, "", 0)
	if err != nil {
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Meta-on-write - HEAD failed")
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}

	// Only establish the version we actually wrote. A concurrent overwrite returns a
	// different ETag, and caching that meta would describe an object whose blocks a
	// later read would fetch under a different version — the same torn-pair hazard
	// write_through guards against.
	if resp.Header.Get("ETag") != writtenETag {
		log.Debug().Str("bucket", bucket).Str("key", key).Msg("Meta-on-write skipped - superseded by a concurrent overwrite")
		return
	}

	meta := cache.MetaFromHTTPHeaders(bucket, key, http.StatusOK, resp.Header)
	if !meta.IsCacheable(s.config.Cache.SizeThreshold) {
		return
	}
	if !s.isBlockEligibleSize(meta.ContentLength) {
		// Sub-block objects are whole-cached; establishing a block-mode entry for one
		// would misdescribe how it will be stored.
		return
	}
	meta.BlockSize = s.config.Cache.BlockSize

	// Deliberately NOT BlocksComplete: no block has been written, so a full-object
	// serve must take the probe pass rather than stream optimistically.
	s.finalizeBlockModeMeta(ctx, bucket, key, meta, 0, writeStartTime)
}

// completedMultipartETag extracts the ETag from a CompleteMultipartUpload response.
// S3 returns it in the XML body, not (necessarily) as a header, so reading the header
// alone would leave writtenETag empty and silently disable this on the very path it
// exists for -- multipart is where TAG caches nothing today. The header is still
// preferred when present, since it costs nothing to check.
func completedMultipartETag(capture *ResponseCapture) string {
	if capture == nil {
		return ""
	}
	if etag := capture.Headers.Get("ETag"); etag != "" {
		return etag
	}
	var result struct {
		XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
		ETag    string   `xml:"ETag"`
	}
	if err := xml.Unmarshal(capture.Body, &result); err != nil {
		return ""
	}
	return result.ETag
}
