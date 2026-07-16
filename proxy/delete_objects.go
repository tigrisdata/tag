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
// <Error> entries are captured (with their VersionId): successful deletes may be
// omitted (Quiet mode), but errors are always listed in both modes, so a requested
// entry succeeded iff it is NOT in the errored set.
type deleteObjectsResult struct {
	XMLName xml.Name `xml:"DeleteResult"`
	Errors  []struct {
		Key       string `xml:"Key"`
		VersionId string `xml:"VersionId"`
	} `xml:"Error"`
}

// deleteEntryID identifies a delete request/response entry by (key, versionId).
// A DeleteObjects request may list the same key multiple times with different
// version IDs, so entries must be matched by both, not by key alone.
func deleteEntryID(key, versionID string) string {
	return key + "\x00" + versionID
}

// erroredDeleteEntries returns the set of (key, versionId) entries upstream
// reported as NOT deleted, and whether the response parsed. On a parse failure the
// caller re-invalidates all requested keys (safe over-invalidation).
func erroredDeleteEntries(body []byte) (map[string]struct{}, bool) {
	var result deleteObjectsResult
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, false
	}
	errored := make(map[string]struct{}, len(result.Errors))
	for _, e := range result.Errors {
		errored[deleteEntryID(e.Key, e.VersionId)] = struct{}{}
	}
	return errored, true
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
	// This ensures cache consistency even if forwarding fails after cache invalidation
	var requested []deleteObjectEntry
	if s.cache.IsEnabled() {
		var deleteReq deleteObjectsRequest
		if xmlErr := xml.Unmarshal(bodyBytes, &deleteReq); xmlErr == nil {
			for _, obj := range deleteReq.Objects {
				s.cache.Delete(context.Background(), bucket, obj.Key)
				metrics.RecordCacheOperation("delete", "success")
				requested = append(requested, obj)
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
	// bulk delete may have re-cached a not-yet-deleted object; this second
	// tombstone blocks that stale repopulation. A key is re-invalidated if AT LEAST
	// ONE of its requested entries succeeded — the request may list the same key
	// under multiple version IDs, so matching failures by key alone would wrongly
	// skip a key whose current version was deleted while an old version's delete
	// failed, leaving a racing refill cached as stale. The metric is recorded once
	// on the pre-forward invalidation above, so it is not counted again here.
	if err == nil && capture != nil && capture.StatusCode >= 200 && capture.StatusCode < 300 && s.cache.IsEnabled() {
		errored, parsed := erroredDeleteEntries(capture.Body)
		reinvalidate := make(map[string]struct{}, len(requested))
		for _, obj := range requested {
			// On a parse failure, over-invalidate every requested key (safe).
			if parsed {
				if _, failed := errored[deleteEntryID(obj.Key, obj.VersionId)]; failed {
					continue // this entry failed; another entry for the same key may still succeed
				}
			}
			reinvalidate[obj.Key] = struct{}{}
		}
		for key := range reinvalidate {
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
