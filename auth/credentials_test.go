package auth

import (
	"os"
	"testing"
)

func TestCredentialStore_LoadFromEnv(t *testing.T) {
	// Set environment variables
	t.Setenv("AWS_ACCESS_KEY_ID", "env_access_key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env_secret_key")

	store := NewCredentialStore()
	if err := store.LoadFromEnv(); err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	if store.Count() != 1 {
		t.Errorf("Count() = %d, want 1", store.Count())
	}

	secret, err := store.GetSecretKey("env_access_key")
	if err != nil {
		t.Fatalf("GetSecretKey() error = %v", err)
	}
	if secret != "env_secret_key" {
		t.Errorf("GetSecretKey() = %q, want %q", secret, "env_secret_key")
	}

	// Verify unknown key returns error
	_, err = store.GetSecretKey("unknown_key")
	if err == nil {
		t.Error("GetSecretKey() should return error for unknown key")
	}
}

func TestCredentialStore_LoadFromEnv_NoEnvVars(t *testing.T) {
	// Ensure env vars are not set
	os.Unsetenv("AWS_ACCESS_KEY_ID")
	os.Unsetenv("AWS_SECRET_ACCESS_KEY")

	store := NewCredentialStore()
	if err := store.LoadFromEnv(); err != nil {
		t.Fatalf("LoadFromEnv() error = %v, should not error when env vars not set", err)
	}

	if store.Count() != 0 {
		t.Errorf("Count() = %d, want 0", store.Count())
	}
}
