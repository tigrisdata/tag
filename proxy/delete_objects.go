package proxy

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/metrics"
)

// ============================================================================
// DeleteObjects (Multi-Object Delete) Support
// ============================================================================

// deleteObjectsRequest represents the S3 DeleteObjects request body.
type deleteObjectsRequest struct {
	XMLName xml.Name            `xml:"Delete"`
	Objects []deleteObjectEntry `xml:"Object"`
	Quiet   bool                `xml:"Quiet"`
}

type deleteObjectEntry struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"`
}

// deleteObjectsResult represents the S3 DeleteObjects response. Only the per-object
// <Error> entries are captured: successful deletes may be omitted (Quiet mode), but
// errors are always listed in both modes, so a key had at least one successful
// delete iff more entries were requested for it than upstream reported as errored.
type deleteObjectsResult struct {
	XMLName xml.Name `xml:"DeleteResult"`
	Errors  []struct {
		Key string `xml:"Key"`
	} `xml:"Error"`
}

// erroredDeleteKeyCounts returns the number of failed <Error> entries per key, and
// whether the response parsed. Counting by key (not by version) is deliberate: it
// is robust to VersionId representation differences between request and response
// (omitted "" vs "null" vs a concrete id), which exact-tuple matching gets wrong.
// On a parse failure the caller re-invalidates all requested keys (safe
// over-invalidation).
func erroredDeleteKeyCounts(body []byte) (map[string]int, bool) {
	var result deleteObjectsResult
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, false
	}
	counts := make(map[string]int)
	for _, e := range result.Errors {
		counts[e.Key]++
	}
	return counts, true
}

// isS3ErrorBody reports whether an XML body's root element is <Error>, the shape S3
// uses for an operation-level failure (including 200-status CopyObject failures).
func isS3ErrorBody(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	dec := xml.NewDecoder(bytes.NewReader(body))
	for {
		tok, err := dec.Token()
		if err != nil {
			return false
		}
		if se, ok := tok.(xml.StartElement); ok {
			return se.Name.Local == "Error"
		}
	}
}

// HandleDeleteObjects handles POST /{bucket}?delete for bulk object deletion.
// Invalidates cache for requested objects BEFORE forwarding to ensure consistency.
func (s *Service) HandleDeleteObjects(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	bucket, _ := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Msg("HandleDeleteObjects")

	// Read request body to buffer it for parsing and forwarding
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("failed to read request body: %w", err)
	}
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	// Parse request to get keys being deleted and invalidate cache BEFORE forwarding
	// This ensures cache consistency even if forwarding fails after cache invalidation.
	// requestedCounts tracks how many entries each key was requested under (the same
	// key may appear multiple times with different version IDs).
	var requestedCounts map[string]int
	if s.cache.IsEnabled() {
		var deleteReq deleteObjectsRequest
		if xmlErr := xml.Unmarshal(bodyBytes, &deleteReq); xmlErr == nil {
			requestedCounts = make(map[string]int)
			for _, obj := range deleteReq.Objects {
				s.invalidateObject(context.Background(), bucket, obj.Key)
				requestedCounts[obj.Key]++
				log.Debug().
					Str("bucket", bucket).
					Str("key", obj.Key).
					Msg("Cache invalidated for bulk-delete object")
			}
		}
	}

	// Forward request to upstream, capturing the response so we can tell which
	// per-object deletes actually succeeded — S3 returns 200 OK with per-key
	// <Error> elements for partial failures.
	capture, err := s.forwarder.ForwardWithCapture(r.Context(), w, r)

	// Re-invalidate AFTER upstream confirms the deletes, for the same
	// read-after-write reason as HandleDeleteObject: a GET racing the in-flight
	// bulk delete may have re-cached a not-yet-deleted object; this second tombstone
	// blocks that stale repopulation. A key is re-invalidated when at least one of
	// its requested entries was deleted — i.e. more entries were requested for the
	// key than upstream reported as errored. Counting by key (never matching version
	// IDs) is robust to VersionId representation differences and to Quiet mode, and
	// can never leave a truly-deleted object cached (a success is never an <Error>).
	// A key whose entries ALL errored keeps its refill (it's still upstream). The
	// metric is recorded once on the pre-forward invalidation above, not again here.
	if err == nil && capture != nil && capture.StatusCode >= 200 && capture.StatusCode < 300 && s.cache.IsEnabled() {
		erroredCounts, parsed := erroredDeleteKeyCounts(capture.Body)
		for key, reqN := range requestedCounts {
			// On a parse failure, over-invalidate every requested key (safe).
			if parsed && reqN <= erroredCounts[key] {
				continue // every requested entry for this key errored — object still present
			}
			s.cache.Delete(context.Background(), bucket, key)
		}
	}

	// Record metrics
	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("DeleteObjects", status, time.Since(start).Seconds())

	return err
}
