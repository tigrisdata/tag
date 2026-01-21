package cache

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/rs/zerolog/log"
)

const (
	// CompletionCacheTTL is the TTL for completion cache entries in seconds.
	// tigris-os uses 1 second; we use 5 seconds for safety margin.
	CompletionCacheTTL = 5

	// completionKeyPrefix is the prefix for completion cache keys.
	completionKeyPrefix = "complete:"
)

// CompletionEntry stores a cached CompleteMultipartUpload response.
type CompletionEntry struct {
	StatusCode int               `json:"status_code"`
	Headers    map[string]string `json:"headers"`
	Body       []byte            `json:"body"`
}

// MakeCompletionKey creates a cache key for a completion response.
func MakeCompletionKey(bucket, key, uploadId string) string {
	return completionKeyPrefix + bucket + "|" + key + "|" + uploadId
}

// GetCompletion retrieves a cached completion response.
func (c *Cache) GetCompletion(ctx context.Context, bucket, key, uploadId string) (*CompletionEntry, bool, error) {
	if !c.IsEnabled() {
		return nil, false, nil
	}

	cacheKey := MakeCompletionKey(bucket, key, uploadId)
	data, err := c.client.Get(ctx, cacheKey)
	if err != nil {
		if isNotFoundError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if data == nil {
		return nil, false, nil
	}

	var entry CompletionEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		log.Debug().Err(err).Str("key", cacheKey).Msg("Completion cache decode error")
		return nil, false, nil // Treat decode errors as cache miss
	}

	log.Debug().Str("bucket", bucket).Str("uploadId", uploadId).Msg("Completion cache hit")
	return &entry, true, nil
}

// PutCompletion stores a completion response in cache.
func (c *Cache) PutCompletion(ctx context.Context, bucket, key, uploadId string, statusCode int, headers http.Header, body []byte) error {
	if !c.IsEnabled() {
		return nil
	}

	// Convert headers to simple map (single value per key)
	headerMap := make(map[string]string)
	for k, v := range headers {
		if len(v) > 0 {
			headerMap[k] = v[0]
		}
	}

	entry := CompletionEntry{
		StatusCode: statusCode,
		Headers:    headerMap,
		Body:       body,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	cacheKey := MakeCompletionKey(bucket, key, uploadId)
	if err := c.client.Put(ctx, cacheKey, data, CompletionCacheTTL); err != nil {
		log.Debug().Err(err).Str("key", cacheKey).Msg("Completion cache put error")
		return err
	}

	log.Debug().Str("bucket", bucket).Str("uploadId", uploadId).Msg("Completion cached")
	return nil
}
