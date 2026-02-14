package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
)

// encryptForTest encrypts signing key entries the same way Tigris does.
func encryptForTest(t *testing.T, proxySecret, accessKey string, entries []SigningKeyEntry) string {
	t.Helper()

	payload, err := json.Marshal(entries)
	if err != nil {
		t.Fatalf("failed to marshal entries: %v", err)
	}

	keyHash := sha256.Sum256([]byte(proxySecret))
	block, err := aes.NewCipher(keyHash[:])
	if err != nil {
		t.Fatalf("failed to create cipher: %v", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("failed to create GCM: %v", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("failed to generate nonce: %v", err)
	}

	ciphertext := gcm.Seal(nil, nonce, payload, []byte(accessKey))

	raw := append(nonce, ciphertext...)
	return base64.StdEncoding.EncodeToString(raw)
}

func TestKeyUnwrapper_Unwrap(t *testing.T) {
	proxySecret := "my-proxy-secret-key"
	accessKey := "AKIAIOSFODNN7EXAMPLE"

	unwrapper, err := NewKeyUnwrapper(proxySecret)
	if err != nil {
		t.Fatalf("NewKeyUnwrapper() error = %v", err)
	}

	entries := []SigningKeyEntry{
		{Date: "20250211", Region: "auto", SigningKey: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},
	}

	headerValue := encryptForTest(t, proxySecret, accessKey, entries)

	got, err := unwrapper.Unwrap(headerValue, accessKey)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("Unwrap() returned %d entries, want 1", len(got))
	}

	if got[0].Date != "20250211" {
		t.Errorf("entry.Date = %q, want %q", got[0].Date, "20250211")
	}
	if got[0].Region != "auto" {
		t.Errorf("entry.Region = %q, want %q", got[0].Region, "auto")
	}
	if got[0].SigningKey != entries[0].SigningKey {
		t.Errorf("entry.SigningKey mismatch")
	}
}

func TestKeyUnwrapper_WrongAccessKey(t *testing.T) {
	proxySecret := "my-proxy-secret-key"

	unwrapper, err := NewKeyUnwrapper(proxySecret)
	if err != nil {
		t.Fatalf("NewKeyUnwrapper() error = %v", err)
	}

	entries := []SigningKeyEntry{
		{Date: "20250211", Region: "auto", SigningKey: "abcdef"},
	}

	headerValue := encryptForTest(t, proxySecret, "CORRECT_KEY", entries)

	// Try to decrypt with wrong access key (AAD mismatch)
	_, err = unwrapper.Unwrap(headerValue, "WRONG_KEY")
	if err == nil {
		t.Error("Unwrap() should fail with wrong access key (AAD mismatch)")
	}
}

func TestKeyUnwrapper_WrongSecret(t *testing.T) {
	unwrapper, err := NewKeyUnwrapper("wrong-secret")
	if err != nil {
		t.Fatalf("NewKeyUnwrapper() error = %v", err)
	}

	entries := []SigningKeyEntry{
		{Date: "20250211", Region: "auto", SigningKey: "abcdef"},
	}

	headerValue := encryptForTest(t, "correct-secret", "AKID", entries)

	_, err = unwrapper.Unwrap(headerValue, "AKID")
	if err == nil {
		t.Error("Unwrap() should fail with wrong proxy secret")
	}
}

func TestKeyUnwrapper_InvalidBase64(t *testing.T) {
	unwrapper, err := NewKeyUnwrapper("secret")
	if err != nil {
		t.Fatalf("NewKeyUnwrapper() error = %v", err)
	}

	_, err = unwrapper.Unwrap("not-valid-base64!!!", "AKID")
	if err == nil {
		t.Error("Unwrap() should fail with invalid base64")
	}
}

func TestKeyUnwrapper_TooShort(t *testing.T) {
	unwrapper, err := NewKeyUnwrapper("secret")
	if err != nil {
		t.Fatalf("NewKeyUnwrapper() error = %v", err)
	}

	short := base64.StdEncoding.EncodeToString([]byte("too-short"))
	_, err = unwrapper.Unwrap(short, "AKID")
	if err == nil {
		t.Error("Unwrap() should fail with too-short input")
	}
}

func TestKeyUnwrapper_MultipleEntries(t *testing.T) {
	proxySecret := "my-proxy-secret-key"
	accessKey := "AKID"

	unwrapper, err := NewKeyUnwrapper(proxySecret)
	if err != nil {
		t.Fatalf("NewKeyUnwrapper() error = %v", err)
	}

	entries := []SigningKeyEntry{
		{Date: "20250211", Region: "auto", SigningKey: "aaaaaa"},
		{Date: "20250212", Region: "auto", SigningKey: "bbbbbb"},
	}

	headerValue := encryptForTest(t, proxySecret, accessKey, entries)

	got, err := unwrapper.Unwrap(headerValue, accessKey)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("Unwrap() returned %d entries, want 2", len(got))
	}
}
