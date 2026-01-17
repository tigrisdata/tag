// Package auth provides authentication and authorization for TAG.
package auth

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// ErrUnknownAccessKey is returned when the access key is not found in the store.
var ErrUnknownAccessKey = errors.New("unknown access key")

// CredentialStore holds access_key → secret_key mappings.
// It is safe for concurrent use.
type CredentialStore struct {
	credentials map[string]string
	mu          sync.RWMutex
}

// NewCredentialStore creates a new empty credential store.
func NewCredentialStore() *CredentialStore {
	return &CredentialStore{
		credentials: make(map[string]string),
	}
}

// LoadFromEnv loads a single credential from environment variables.
// This is useful for development or simple deployments.
func (c *CredentialStore) LoadFromEnv() error {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if accessKey == "" || secretKey == "" {
		return nil // No env credentials, not an error
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.credentials[accessKey] = secretKey
	return nil
}

// GetSecretKey looks up the secret key for a given access key.
func (c *CredentialStore) GetSecretKey(accessKey string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if secret, ok := c.credentials[accessKey]; ok {
		return secret, nil
	}
	return "", fmt.Errorf("%w: %s", ErrUnknownAccessKey, accessKey)
}

// AddCredential adds or updates a credential mapping.
func (c *CredentialStore) AddCredential(accessKey, secretKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.credentials[accessKey] = secretKey
}

// RemoveCredential removes a credential mapping.
func (c *CredentialStore) RemoveCredential(accessKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.credentials, accessKey)
}

// Count returns the number of credentials stored.
func (c *CredentialStore) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.credentials)
}

// HasCredential checks if a credential exists for the given access key.
func (c *CredentialStore) HasCredential(accessKey string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.credentials[accessKey]
	return ok
}
