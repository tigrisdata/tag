package auth

import "time"

// DefaultAuthzCacheTTL is the default TTL for authorization cache entries.
const DefaultAuthzCacheTTL = 10 * time.Minute

// maxAuthzCacheSize bounds the total authorization entries across all shards.
const maxAuthzCacheSize = 1_000_000

// AuthzCache caches authorization decisions at the (access_key, bucket) level.
// Entries are granted after a client GetObject or HeadObject succeeds upstream,
// revoked when Tigris returns 403, and expire after a configurable TTL.
//
// Uses fixed expirable-LRU shards; Peek keeps cache hits from updating recency.
type AuthzCache struct {
	cache *readMostlyTTLTable[struct{}]
}

// NewAuthzCache creates a new authorization cache with the given TTL.
func NewAuthzCache(ttl time.Duration) *AuthzCache {
	if ttl <= 0 {
		ttl = DefaultAuthzCacheTTL
	}
	return &AuthzCache{
		cache: newReadMostlyTTLTable[struct{}](maxAuthzCacheSize, ttl),
	}
}

// IsAuthorized checks if the given access key is authorized for the given bucket.
// Returns false if the entry is expired or not found.
func (c *AuthzCache) IsAuthorized(accessKey, bucket string) bool {
	_, ok := c.cache.Get(authzKey(accessKey, bucket))
	return ok
}

// Grant records that the given access key is authorized for the given bucket.
// The entry will expire after the configured TTL.
func (c *AuthzCache) Grant(accessKey, bucket string) {
	c.cache.Store(authzKey(accessKey, bucket), struct{}{})
}

// Revoke immediately removes authorization for the given access key and bucket.
func (c *AuthzCache) Revoke(accessKey, bucket string) {
	c.cache.Remove(authzKey(accessKey, bucket))
}

// Count returns the number of entries currently in the cache.
func (c *AuthzCache) Count() int {
	return c.cache.Len()
}

// authzKey builds the composite map key for the authorization cache.
func authzKey(accessKey, bucket string) string {
	return accessKey + "\x00" + bucket
}
