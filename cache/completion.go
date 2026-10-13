package cache

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	json "github.com/goccy/go-json"
	"github.com/rs/zerolog/log"
)

const (
	// CompletionCacheTTL is the TTL for completion cache entries in seconds.
	// Chosen with margin over the upstream implementation-observed window.
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

// completionCacheRecord keeps caller identity in the cache's internal record;
// it is never returned to callers.
type completionCacheRecord struct {
	AccessKey  string            `json:"access_key,omitempty"`
	StatusCode int               `json:"status_code"`
	Headers    map[string]string `json:"headers"`
	Body       []byte            `json:"body"`
}

// MakeCompletionKey creates a cache key for a completion response.
func MakeCompletionKey(bucket, key, uploadId string) string {
	if strings.ContainsAny(bucket, "|") || strings.ContainsAny(key, "|") || strings.ContainsAny(uploadId, "|") {
		// Keep the legacy form for ordinary tuples. For components containing its
		// delimiter, escape every component and use a separator that QueryEscape
		// also escapes. The encoded form contains no '|', so it cannot alias a
		// legacy key, which always contains two delimiters.
		return completionKeyPrefix + url.QueryEscape(bucket) + ":" + url.QueryEscape(key) + ":" + url.QueryEscape(uploadId)
	}
	return completionKeyPrefix + bucket + "|" + key + "|" + uploadId
}

// GetCompletion retrieves a cached completion response only for a caller that
// matches the identity stored with the successful completion. The callback is
// evaluated only when a bound cache entry exists.
func (c *Cache) GetCompletion(ctx context.Context, bucket, key, uploadId string, callerAccessKey func() (string, bool)) (*CompletionEntry, bool, error) {
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

	var entry completionCacheRecord
	if err := json.Unmarshal(data, &entry); err != nil {
		log.Debug().Err(err).Str("key", cacheKey).Msg("Completion cache decode error")
		return nil, false, nil // Treat decode errors as cache miss
	}
	if entry.AccessKey == "" || callerAccessKey == nil {
		return nil, false, nil
	}
	accessKey, authenticated := callerAccessKey()
	if !authenticated || accessKey == "" || entry.AccessKey != accessKey {
		return nil, false, nil
	}

	log.Debug().Str("bucket", bucket).Str("uploadId", uploadId).Msg("Completion cache hit")
	return &CompletionEntry{StatusCode: entry.StatusCode, Headers: entry.Headers, Body: entry.Body}, true, nil
}

// PutCompletion stores a completion response in cache, bound to the access key
// that successfully completed the upload. Responses without an identified
// principal are not replayable.
func (c *Cache) PutCompletion(ctx context.Context, bucket, key, uploadId, accessKey string, statusCode int, headers http.Header, body []byte) error {
	if !c.IsEnabled() || accessKey == "" {
		return nil
	}

	// Convert headers to simple map (single value per key)
	headerMap := make(map[string]string)
	for k, v := range headers {
		if len(v) > 0 {
			headerMap[k] = v[0]
		}
	}

	entry := completionCacheRecord{
		AccessKey:  accessKey,
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
