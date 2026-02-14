package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

const (
	// gcmNonceSize is the AES-GCM nonce size in bytes.
	gcmNonceSize = 12
)

// SigningKeyEntry represents a single derived signing key from Tigris.
type SigningKeyEntry struct {
	Date       string `json:"date"`   // YYYYMMDD format
	Region     string `json:"region"` // e.g., "auto"
	SigningKey string `json:"key"`    // hex-encoded 32-byte kSigning
}

// KeyUnwrapper decrypts derived signing keys from the X-Tigris-Proxy-Signing-Keys
// response header. Keys are encrypted with AES-256-GCM using SHA256(proxy_secret_key)
// as the encryption key and the client's access key as AAD.
type KeyUnwrapper struct {
	gcm cipher.AEAD
}

// NewKeyUnwrapper creates a new key unwrapper using the proxy secret key.
func NewKeyUnwrapper(proxySecretKey string) (*KeyUnwrapper, error) {
	// Derive 256-bit encryption key from proxy secret
	keyHash := sha256.Sum256([]byte(proxySecretKey))

	block, err := aes.NewCipher(keyHash[:])
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	return &KeyUnwrapper{gcm: gcm}, nil
}

// Unwrap decrypts and parses the X-Tigris-Proxy-Signing-Keys header value.
// The accessKey is used as additional authenticated data (AAD) to prevent
// ciphertext transplant attacks between different clients.
func (u *KeyUnwrapper) Unwrap(headerValue string, accessKey string) ([]SigningKeyEntry, error) {
	// Base64 decode
	raw, err := base64.StdEncoding.DecodeString(headerValue)
	if err != nil {
		return nil, fmt.Errorf("failed to base64 decode signing keys header: %w", err)
	}

	// Validate minimum length: nonce (12) + at least 1 byte ciphertext + GCM tag (16)
	if len(raw) < gcmNonceSize+1+u.gcm.Overhead() {
		return nil, fmt.Errorf("signing keys header too short: %d bytes", len(raw))
	}

	// Split nonce and ciphertext+tag
	nonce := raw[:gcmNonceSize]
	ciphertext := raw[gcmNonceSize:]

	// Decrypt with client access key as AAD
	plaintext, err := u.gcm.Open(nil, nonce, ciphertext, []byte(accessKey))
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt signing keys: %w", err)
	}

	// Parse JSON
	var entries []SigningKeyEntry
	if err := json.Unmarshal(plaintext, &entries); err != nil {
		return nil, fmt.Errorf("failed to parse signing keys JSON: %w", err)
	}

	return entries, nil
}
