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
// <Error> entries are captured: successful deletes may be omitted (Quiet mode), so
// success is derived as "requested minus errored".
type deleteObjectsResult struct {
	XMLName xml.Name `xml:"DeleteResult"`
	Errors  []struct {
		Key string `xml:"Key"`
	} `xml:"Error"`
}

// failedDeleteKeys returns the set of keys upstream reported as NOT deleted. On a
// parse failure it returns an empty set, so callers re-invalidate all requested
// keys — the safe over-invalidation.
func failedDeleteKeys(body []byte) map[string]struct{} {
	failed := make(map[string]struct{})
	var result deleteObjectsResult
	if err := xml.Unmarshal(body, &result); err != nil {
		return failed
	}
	for _, e := range result.Errors {
		failed[e.Key] = struct{}{}
	}
	return failed
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
	var deletedKeys []string
	if s.cache.IsEnabled() {
		var deleteReq deleteObjectsRequest
		if xmlErr := xml.Unmarshal(bodyBytes, &deleteReq); xmlErr == nil {
			for _, obj := range deleteReq.Objects {
				s.cache.Delete(context.Background(), bucket, obj.Key)
				metrics.RecordCacheOperation("delete", "success")
				deletedKeys = append(deletedKeys, obj.Key)
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
	// tombstone blocks that stale repopulation. Only keys upstream actually deleted
	// are re-invalidated — a key that failed is still present, so tombstoning it
	// would discard a valid racing refill. The metric is recorded once on the
	// pre-forward invalidation above, so it is not counted again here.
	if err == nil && capture != nil && capture.StatusCode >= 200 && capture.StatusCode < 300 && s.cache.IsEnabled() {
		failed := failedDeleteKeys(capture.Body)
		for _, key := range deletedKeys {
			if _, bad := failed[key]; bad {
				continue
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
