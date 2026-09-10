package auth

import (
	"sync"
	"sync/atomic"
	"time"

	expirable "github.com/hashicorp/golang-lru/v2/expirable"
)

// DefaultAuthzCacheTTL is the default TTL for authorization cache entries.
const DefaultAuthzCacheTTL = 10 * time.Minute

// maxAuthzCacheSize is the maximum number of entries in the authorization cache.
// Each entry is an (access_key, bucket) string key (~60 bytes) mapped to a small
// grant record, plus LRU node overhead (~200 bytes). At roughly 300 bytes per entry,
// 1M entries is about 300MB.
const maxAuthzCacheSize = 1_000_000

type authorizationGrant struct {
	expiresAt time.Time
	revoked   atomic.Bool
}

// AuthzToken records the grant observed before local signature validation. The
// token can be checked without another LRU lookup, so a revoke or expiry during
// validation cannot turn an otherwise stale authorization decision into a cache
// hit.
type AuthzToken struct {
	cache *AuthzCache
	grant *authorizationGrant
}

// AuthzCache caches authorization decisions at the (access_key, bucket) level.
// Entries are granted when Tigris returns a successful response with signing keys,
// revoked when Tigris returns 403, and expire after a configurable TTL.
//
// Uses hashicorp/golang-lru/v2/expirable for TTL-based expiration and bounded size.
type AuthzCache struct {
	cache *expirable.LRU[string, *authorizationGrant]
	ttl   time.Duration
	mu    sync.Mutex // serializes grant replacement/revocation with token invalidation
}

// NewAuthzCache creates a new authorization cache with the given TTL.
func NewAuthzCache(ttl time.Duration) *AuthzCache {
	if ttl <= 0 {
		ttl = DefaultAuthzCacheTTL
	}

	c := &AuthzCache{ttl: ttl}
	c.cache = expirable.NewLRU(maxAuthzCacheSize, func(_ string, grant *authorizationGrant) {
		grant.revoked.Store(true)
	}, ttl)
	return c
}

// AuthorizationToken checks the grant and returns a cheap token that can be
// checked again after work that depends on the grant. The second check observes
// revocation and TTL expiry without taking the LRU lock again.
func (c *AuthzCache) AuthorizationToken(accessKey, bucket string) (AuthzToken, bool) {
	grant, ok := c.cache.Get(authzKey(accessKey, bucket))
	if !ok || !grant.active() {
		return AuthzToken{}, false
	}
	return AuthzToken{cache: c, grant: grant}, true
}

// IsTokenCurrent reports whether the grant represented by token is still valid.
// It is intentionally independent of the LRU: the token's grant is invalidated
// atomically by Revoke, replacement, eviction, or expiry.
func (c *AuthzCache) IsTokenCurrent(token AuthzToken) bool {
	return token.cache == c && token.grant != nil && token.grant.active()
}

// IsAuthorized checks if the given access key is authorized for the given bucket.
// Returns false if the entry is expired or not found.
func (c *AuthzCache) IsAuthorized(accessKey, bucket string) bool {
	_, ok := c.AuthorizationToken(accessKey, bucket)
	return ok
}

// Grant records that the given access key is authorized for the given bucket.
// The entry will expire after the configured TTL.
func (c *AuthzCache) Grant(accessKey, bucket string) {
	key := authzKey(accessKey, bucket)
	c.mu.Lock()
	defer c.mu.Unlock()

	// Replacing an existing grant must invalidate tokens that observed the old
	// entry before the new pointer is installed.
	if previous, ok := c.cache.Peek(key); ok {
		previous.revoked.Store(true)
	}
	c.cache.Add(key, &authorizationGrant{expiresAt: time.Now().Add(c.ttl)})
}

// Revoke immediately removes authorization for the given access key and bucket.
func (c *AuthzCache) Revoke(accessKey, bucket string) {
	key := authzKey(accessKey, bucket)
	c.mu.Lock()
	defer c.mu.Unlock()

	if grant, ok := c.cache.Peek(key); ok {
		grant.revoked.Store(true)
	}
	c.cache.Remove(key)
}

// Count returns the number of entries currently in the cache.
func (c *AuthzCache) Count() int {
	return c.cache.Len()
}

func (g *authorizationGrant) active() bool {
	return !g.revoked.Load() && time.Now().Before(g.expiresAt)
}

// authzKey builds the composite map key for the authorization cache.
func authzKey(accessKey, bucket string) string {
	return accessKey + "\x00" + bucket
}
