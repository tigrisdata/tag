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

// CompletionEntry stores a cached CompleteMultipartUpload response, bound to the
// access key that completed the upload. An entry without an access key (written
// before identity binding existed) is never replayed.
type CompletionEntry struct {
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

// GetCompletion retrieves the cached completion response bound to accessKey.
// Entries bound to another principal, or to none, are reported as misses.
func (c *Cache) GetCompletion(ctx context.Context, bucket, key, uploadId, accessKey string) (*CompletionEntry, bool, error) {
	if !c.IsEnabled() || accessKey == "" {
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
	if entry.AccessKey == "" || entry.AccessKey != accessKey {
		log.Debug().Str("bucket", bucket).Str("uploadId", uploadId).Msg("Completion cache entry bound to another principal")
		return nil, false, nil
	}

	log.Debug().Str("bucket", bucket).Str("uploadId", uploadId).Msg("Completion cache hit")
	return &entry, true, nil
}

// PutCompletion stores a completion response in cache, bound to the access key
// that successfully completed the upload. Responses without an identified
// principal are not stored, so they can never be replayed.
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

	entry := CompletionEntry{
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
