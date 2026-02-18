package auth

import (
	"time"

	expirable "github.com/hashicorp/golang-lru/v2/expirable"
)

// DefaultAuthzCacheTTL is the default TTL for authorization cache entries.
const DefaultAuthzCacheTTL = 10 * time.Minute

// maxAuthzCacheSize is the maximum number of entries in the authorization cache.
// Each entry is an (access_key, bucket) string key (~60 bytes) mapped to struct{} (0 bytes),
// plus LRU node overhead (~200 bytes). At ~250 bytes per entry, 1M entries ≈ 250MB.
const maxAuthzCacheSize = 1_000_000

// AuthzCache caches authorization decisions at the (access_key, bucket) level.
// Entries are granted when Tigris returns a successful response with signing keys,
// revoked when Tigris returns 403, and expire after a configurable TTL.
//
// Uses hashicorp/golang-lru/v2/expirable for TTL-based expiration and bounded size.
type AuthzCache struct {
	cache *expirable.LRU[string, struct{}]
}

// NewAuthzCache creates a new authorization cache with the given TTL.
func NewAuthzCache(ttl time.Duration) *AuthzCache {
	if ttl <= 0 {
		ttl = DefaultAuthzCacheTTL
	}
	return &AuthzCache{
		cache: expirable.NewLRU[string, struct{}](maxAuthzCacheSize, nil, ttl),
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
	c.cache.Add(authzKey(accessKey, bucket), struct{}{})
}

// Revoke immediately removes authorization for the given access key and bucket.
func (c *AuthzCache) Revoke(accessKey, bucket string) {
	c.cache.Remove(authzKey(accessKey, bucket))
}

// Count returns the number of entries currently in the cache.
func (c *AuthzCache) Count() int {
	return c.cache.Len()
}

// publicAccessKey is the sentinel access key used for anonymous/public bucket access.
// Kept internal — callers use GrantPublic/IsPublicAuthorized/RevokePublic.
const publicAccessKey = "__public__"

// GrantPublic records that the given bucket is publicly accessible.
// The entry will expire after the configured TTL.
func (c *AuthzCache) GrantPublic(bucket string) {
	c.cache.Add(authzKey(publicAccessKey, bucket), struct{}{})
}

// IsPublicAuthorized checks if the given bucket has been confirmed as publicly accessible.
// Returns false if the entry is expired or not found.
func (c *AuthzCache) IsPublicAuthorized(bucket string) bool {
	_, ok := c.cache.Get(authzKey(publicAccessKey, bucket))
	return ok
}

// RevokePublic removes public access authorization for the given bucket.
func (c *AuthzCache) RevokePublic(bucket string) {
	c.cache.Remove(authzKey(publicAccessKey, bucket))
}

// authzKey builds the composite map key for the authorization cache.
func authzKey(accessKey, bucket string) string {
	return accessKey + "\x00" + bucket
}
