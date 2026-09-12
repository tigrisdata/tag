package auth

import (
	"sync"

	"github.com/hashicorp/golang-lru/v2"
)

// maxSigningKeyCacheSize leaves room for one block-population batch while
// bounding the number of credentials and derived keys retained by a signer.
const maxSigningKeyCacheSize = 64

// signingKeyCacheKey includes every input that can affect a derived SigV4 key.
// Keep the access key in the key even though derivation uses only the secret so
// credentials are never implicitly shared between access-key identities.
type signingKeyCacheKey struct {
	accessKey string
	secretKey string
	date      string
	region    string
	service   string
}

// signingKey is owned by the cache and is only passed as a read-only HMAC key.
// Keeping the derived bytes shared avoids copying them on every cache hit.
type signingKey []byte

type signingKeyCache struct {
	cache  *lru.Cache[signingKeyCacheKey, signingKey]
	missMu sync.Mutex
}

func newSigningKeyCache() *signingKeyCache {
	cache, err := lru.New[signingKeyCacheKey, signingKey](maxSigningKeyCacheSize)
	if err != nil {
		panic(err)
	}
	return &signingKeyCache{cache: cache}
}

func (c *signingKeyCache) getOrDerive(key signingKeyCacheKey, secretKey, date, region string) signingKey {
	if cached, ok := c.cache.Get(key); ok {
		return cached
	}

	// Coalesce cold calls for the same signer while keeping the common cache-hit
	// path out of this lock. The second lookup also avoids duplicate derivation
	// when several requests arrive for the same credential at once.
	c.missMu.Lock()
	defer c.missMu.Unlock()
	if cached, ok := c.cache.Get(key); ok {
		return cached
	}

	derived := deriveSigningKey(secretKey, date, region)
	c.cache.Add(key, derived)
	return derived
}
