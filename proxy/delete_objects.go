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

	// Forward request to upstream
	err = s.forwarder.Forward(r.Context(), w, r)

	// Re-invalidate AFTER upstream confirms the deletes, for the same
	// read-after-write reason as HandleDeleteObject: a GET racing the in-flight
	// bulk delete may have re-cached a not-yet-deleted object; this second
	// tombstone blocks that stale repopulation. The metric is recorded once on
	// the pre-forward invalidation above, so it is not counted again here.
	if err == nil && s.cache.IsEnabled() {
		for _, key := range deletedKeys {
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
