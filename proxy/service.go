package proxy

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

const (
	// maxConcurrentCacheWrites limits the number of concurrent background cache operations.
	maxConcurrentCacheWrites = 100

	// X-Cache header constants for indicating cache status.
	XCacheHeader   = "X-Cache"
	XCacheHit      = "HIT"
	XCacheMiss     = "MISS"
	XCacheBypass   = "BYPASS"   // Range requests bypass cache
	XCacheDisabled = "DISABLED" // Cache is disabled
)

// Service provides the core caching proxy logic.
type Service struct {
	forwarder              RequestForwarder
	cache                  *cache.Cache
	config                 *config.Config
	cacheSemaphore         chan struct{}      // Limits concurrent cache writes
	broadcastManager       *broadcast.Manager // For streaming request coalescing
	backgroundFetchManager *broadcast.Manager // For background full-object fetches (range caching)
}

// NewService creates a new proxy service.
func NewService(forwarder RequestForwarder, cache *cache.Cache, cfg *config.Config) *Service {
	channelBuf := cfg.Broadcast.ChannelBuffer
	if channelBuf <= 0 {
		channelBuf = broadcast.DefaultChannelBuffer
	}

	return &Service{
		forwarder:              forwarder,
		cache:                  cache,
		config:                 cfg,
		cacheSemaphore:         make(chan struct{}, maxConcurrentCacheWrites),
		broadcastManager:       broadcast.NewManager(channelBuf),
		backgroundFetchManager: broadcast.NewManager(channelBuf),
	}
}

// HandlePutObject handles PUT requests for objects.
// Invalidates cache BEFORE forwarding to ensure consistency.
func (s *Service) HandlePutObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	bucket, key := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandlePutObject")

	// Invalidate cache BEFORE forwarding to ensure consistency
	// This prevents stale data from being served if forwarding succeeds but cache invalidation fails
	if s.cache.IsEnabled() {
		s.cache.Delete(context.Background(), bucket, key)
		metrics.RecordCacheOperation("delete", "success")
	}

	// Forward to Tigris
	err := s.forwarder.Forward(r.Context(), w, r)

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("PutObject", status, time.Since(start).Seconds())

	return err
}

// HandleDeleteObject handles DELETE requests for objects.
// Invalidates cache BEFORE forwarding to ensure consistency.
func (s *Service) HandleDeleteObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	bucket, key := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandleDeleteObject")

	// Invalidate cache BEFORE forwarding to ensure consistency
	// This prevents stale data from being served if forwarding succeeds but cache invalidation fails
	if s.cache.IsEnabled() {
		s.cache.Delete(context.Background(), bucket, key)
		metrics.RecordCacheOperation("delete", "success")
	}

	// Forward to upstream
	err := s.forwarder.Forward(r.Context(), w, r)

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("DeleteObject", status, time.Since(start).Seconds())

	return err
}

// HandleHeadObject handles HEAD requests for objects.
// Serves from cached metadata when available (no body fetch needed).
func (s *Service) HandleHeadObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	ctx := r.Context()
	bucket, key := ParseBucketKey(r)
	noCacheRead := shouldSkipCache(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandleHeadObject")

	// Validate credentials before serving from cache
	result, _, _, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil {
		metrics.RecordRequest("HeadObject", "auth_error", time.Since(start).Seconds())
		return err
	}

	// Try to serve from cached metadata (no body needed)
	if result == AuthValidated && !noCacheRead && s.cache.IsEnabled() {
		meta, found, cacheErr := s.cache.GetMeta(ctx, bucket, key)
		if cacheErr == nil && found && meta != nil {
			metrics.RecordCacheHit()
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("HEAD served from cache")
			meta.WriteHeaders(w)
			w.Header().Set(XCacheHeader, XCacheHit)
			w.WriteHeader(meta.StatusCode)
			metrics.RecordRequest("HeadObject", "success", time.Since(start).Seconds())
			return nil
		}
		metrics.RecordCacheMiss()
	}

	// Cache miss - forward to upstream
	// Set X-Cache header before forwarding (will be included in response)
	if s.cache.IsEnabled() {
		w.Header().Set(XCacheHeader, XCacheMiss)
	} else {
		w.Header().Set(XCacheHeader, XCacheDisabled)
	}
	err = s.forwarder.Forward(ctx, w, r)

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("HeadObject", status, time.Since(start).Seconds())
	return err
}

// HandleCopyObject handles copy object requests.
// Invalidates cache BEFORE forwarding to ensure consistency.
func (s *Service) HandleCopyObject(w http.ResponseWriter, r *http.Request) error {
	bucket, key := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandleCopyObject")

	// Invalidate cache for destination object BEFORE forwarding to ensure consistency
	// This prevents stale data from being served if forwarding succeeds but cache invalidation fails
	if s.cache.IsEnabled() {
		s.cache.Delete(context.Background(), bucket, key)
	}

	// Forward to upstream
	return s.forwarder.Forward(r.Context(), w, r)
}

// HandlePassthrough handles requests that are passed through without caching.
func (s *Service) HandlePassthrough(w http.ResponseWriter, r *http.Request) error {
	log.Debug().Str("method", r.Method).Str("path", r.URL.Path).Msg("HandlePassthrough")
	return s.forwarder.Forward(r.Context(), w, r)
}

// HandleCompleteMultipartUpload handles CompleteMultipartUpload with idempotency caching.
// This caches successful completion responses in ocache to support idempotent calls,
// matching tigris-os behavior where a second CompleteMultipartUpload call returns success.
func (s *Service) HandleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request) error {
	bucket, key := ParseBucketKey(r)
	uploadId := r.URL.Query().Get("uploadId")
	ctx := r.Context()

	log.Debug().Str("bucket", bucket).Str("key", key).Str("uploadId", uploadId).Msg("HandleCompleteMultipartUpload")

	// Check ocache first for idempotent completion (works across TAG pods)
	if s.cache.IsEnabled() {
		entry, found, err := s.cache.GetCompletion(ctx, bucket, key, uploadId)
		if err == nil && found {
			log.Debug().Str("uploadId", uploadId).Msg("CompleteMultipartUpload cache hit - returning cached response")
			for k, v := range entry.Headers {
				w.Header().Set(k, v)
			}
			w.WriteHeader(entry.StatusCode)
			_, _ = w.Write(entry.Body)
			return nil
		}
	}

	// Forward to upstream with response capture
	capture, err := s.forwarder.ForwardWithCapture(ctx, w, r)
	if err != nil {
		return err
	}

	// Cache successful completions (2xx status codes) in ocache
	// Only cache if the body was fully captured to avoid storing corrupted responses
	if capture.StatusCode >= 200 && capture.StatusCode < 300 && capture.Complete {
		if cacheErr := s.cache.PutCompletion(ctx, bucket, key, uploadId, capture.StatusCode, capture.Headers, capture.Body); cacheErr != nil {
			log.Debug().Err(cacheErr).Msg("Failed to cache completion response")
			// Don't fail the request if caching fails
		}
	}

	return nil
}

// ParseBucketKey extracts bucket and key from request path.
func ParseBucketKey(r *http.Request) (bucket, key string) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) >= 1 {
		bucket = parts[0]
	}
	if len(parts) >= 2 {
		key = parts[1]
	}
	return
}

// extractTotalSizeFromContentRange extracts total size from Content-Range header.
// Header format: "bytes start-end/total" (e.g., "bytes 0-499/1234")
// Returns 0 if header is missing or malformed.
func extractTotalSizeFromContentRange(contentRange string) int64 {
	if contentRange == "" {
		return 0
	}

	// Find the slash separator
	slashIdx := strings.LastIndex(contentRange, "/")
	if slashIdx == -1 {
		return 0
	}

	totalStr := contentRange[slashIdx+1:]
	if totalStr == "*" {
		// Unknown total size
		return 0
	}

	total, err := strconv.ParseInt(totalStr, 10, 64)
	if err != nil {
		return 0
	}
	return total
}

// GetRegion returns the configured upstream region.
func (s *Service) GetRegion() string {
	return s.config.Upstream.Region
}
