package auth

import (
	"strings"
	"sync"
	"time"
)

// DerivedKeyStore stores pre-derived SigV4 signing keys for transparent proxy mode.
// Keys are indexed by (accessKey, date, region) and are day-scoped.
// It implements the KeyProvider interface.
type DerivedKeyStore struct {
	keys       map[string][]byte // "accessKey\x00date\x00region" → kSigning
	accessKeys map[string]bool   // quick existence check by access key
	mu         sync.RWMutex
}

// NewDerivedKeyStore creates a new empty derived key store.
func NewDerivedKeyStore() *DerivedKeyStore {
	return &DerivedKeyStore{
		keys:       make(map[string][]byte),
		accessKeys: make(map[string]bool),
	}
}

// Store adds or updates a derived signing key for the given access key, date, and region.
// Old keys (dates before yesterday) are lazily cleaned up on each Store call.
func (d *DerivedKeyStore) Store(accessKey, date, region string, signingKey []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.keys[makeKey(accessKey, date, region)] = signingKey
	d.accessKeys[accessKey] = true

	// Lazy cleanup: remove keys older than yesterday
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Format("20060102")
	d.cleanupBeforeLocked(yesterday)
}

// GetSigningKey returns the signing key for the given access key, date, and region.
// Returns an error wrapping ErrUnknownAccessKey if the key is not found.
// Implements the KeyProvider interface.
func (d *DerivedKeyStore) GetSigningKey(accessKey, date, region string) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	key, ok := d.keys[makeKey(accessKey, date, region)]
	if !ok {
		return nil, ErrUnknownAccessKey
	}
	return key, nil
}

// HasKey returns whether any signing key exists for the given access key.
// Implements the KeyProvider interface.
func (d *DerivedKeyStore) HasKey(accessKey string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.accessKeys[accessKey]
}

// Count returns the number of stored signing keys.
func (d *DerivedKeyStore) Count() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.keys)
}

// cleanupBeforeLocked removes keys with dates strictly before the given date string.
// Must be called with d.mu held for writing.
func (d *DerivedKeyStore) cleanupBeforeLocked(minDate string) {
	for k := range d.keys {
		date := extractDate(k)
		if date < minDate {
			accessKey := extractAccessKey(k)
			delete(d.keys, k)

			// Check if this access key still has any keys
			if !d.hasKeysForAccessKeyLocked(accessKey) {
				delete(d.accessKeys, accessKey)
			}
		}
	}
}

// hasKeysForAccessKeyLocked checks if any keys exist for the given access key.
// Must be called with d.mu held.
func (d *DerivedKeyStore) hasKeysForAccessKeyLocked(accessKey string) bool {
	prefix := accessKey + "\x00"
	for k := range d.keys {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

// makeKey builds the composite map key from access key, date, and region.
func makeKey(accessKey, date, region string) string {
	return accessKey + "\x00" + date + "\x00" + region
}

// extractDate extracts the date component from a composite key.
func extractDate(compositeKey string) string {
	parts := strings.SplitN(compositeKey, "\x00", 3)
	if len(parts) >= 2 {
		return parts[1]
	}
	return ""
}

// extractAccessKey extracts the access key component from a composite key.
func extractAccessKey(compositeKey string) string {
	parts := strings.SplitN(compositeKey, "\x00", 2)
	if len(parts) >= 1 {
		return parts[0]
	}
	return ""
}

// Compile-time check that DerivedKeyStore implements KeyProvider.
var _ KeyProvider = (*DerivedKeyStore)(nil)
