package auth

import (
	"time"

	expirable "github.com/hashicorp/golang-lru/v2/expirable"
)

// DefaultDerivedKeyTTL is the default TTL for derived signing keys.
// 48 hours covers today + yesterday for SigV4 clock skew tolerance.
const DefaultDerivedKeyTTL = 48 * time.Hour

// maxDerivedKeyStoreSize is the maximum number of signing key entries.
// Each entry is a composite key (~80 bytes) mapped to a 32-byte signing key,
// plus LRU node overhead (~200 bytes). At ~300 bytes per entry, 100K entries ≈ 30MB.
const maxDerivedKeyStoreSize = 100_000

// maxAccessKeyCacheSize is the maximum number of tracked access keys.
const maxAccessKeyCacheSize = 50_000

// DerivedKeyStore stores pre-derived SigV4 signing keys for transparent proxy mode.
// Keys are indexed by (accessKey, date, region) and expire after a configurable TTL.
// It implements the KeyProvider interface.
type DerivedKeyStore struct {
	keys       *expirable.LRU[string, []byte]
	accessKeys *expirable.LRU[string, struct{}]
}

// NewDerivedKeyStore creates a new derived key store with the given TTL.
func NewDerivedKeyStore(ttl time.Duration) *DerivedKeyStore {
	if ttl <= 0 {
		ttl = DefaultDerivedKeyTTL
	}
	return &DerivedKeyStore{
		keys:       expirable.NewLRU[string, []byte](maxDerivedKeyStoreSize, nil, ttl),
		accessKeys: expirable.NewLRU[string, struct{}](maxAccessKeyCacheSize, nil, ttl),
	}
}

// Store adds or updates a derived signing key for the given access key, date, and region.
func (d *DerivedKeyStore) Store(accessKey, date, region string, signingKey []byte) {
	d.keys.Add(makeKey(accessKey, date, region), signingKey)
	d.accessKeys.Add(accessKey, struct{}{})
}

// GetSigningKey returns the signing key for the given access key, date, and region.
// Returns an error wrapping ErrUnknownAccessKey if the key is not found.
// Implements the KeyProvider interface.
func (d *DerivedKeyStore) GetSigningKey(accessKey, date, region string) ([]byte, error) {
	key, ok := d.keys.Get(makeKey(accessKey, date, region))
	if !ok {
		return nil, ErrUnknownAccessKey
	}
	return key, nil
}

// HasKey returns whether any signing key exists for the given access key.
// Implements the KeyProvider interface.
func (d *DerivedKeyStore) HasKey(accessKey string) bool {
	_, ok := d.accessKeys.Get(accessKey)
	return ok
}

// Count returns the number of stored signing keys.
func (d *DerivedKeyStore) Count() int {
	return d.keys.Len()
}

// makeKey builds the composite map key from access key, date, and region.
func makeKey(accessKey, date, region string) string {
	return accessKey + "\x00" + date + "\x00" + region
}

// Compile-time check that DerivedKeyStore implements KeyProvider.
var _ KeyProvider = (*DerivedKeyStore)(nil)
