package auth

import (
	"testing"
	"time"
)

func TestAuthzCache_GrantAndCheck(t *testing.T) {
	cache := NewAuthzCache(10 * time.Minute)

	if cache.IsAuthorized("AKID1", "my-bucket") {
		t.Error("IsAuthorized() should return false for empty cache")
	}

	cache.Grant("AKID1", "my-bucket")

	if !cache.IsAuthorized("AKID1", "my-bucket") {
		t.Error("IsAuthorized() should return true after Grant")
	}

	// Different bucket should not be authorized
	if cache.IsAuthorized("AKID1", "other-bucket") {
		t.Error("IsAuthorized() should return false for different bucket")
	}

	// Different access key should not be authorized
	if cache.IsAuthorized("AKID2", "my-bucket") {
		t.Error("IsAuthorized() should return false for different access key")
	}
}

func TestAuthzCache_Revoke(t *testing.T) {
	cache := NewAuthzCache(10 * time.Minute)

	cache.Grant("AKID1", "my-bucket")
	if !cache.IsAuthorized("AKID1", "my-bucket") {
		t.Fatal("IsAuthorized() should return true after Grant")
	}

	cache.Revoke("AKID1", "my-bucket")
	if cache.IsAuthorized("AKID1", "my-bucket") {
		t.Error("IsAuthorized() should return false after Revoke")
	}
}

func TestAuthzCache_Expiry(t *testing.T) {
	// Use a very short TTL for testing
	cache := NewAuthzCache(10 * time.Millisecond)

	cache.Grant("AKID1", "my-bucket")
	if !cache.IsAuthorized("AKID1", "my-bucket") {
		t.Fatal("IsAuthorized() should return true immediately after Grant")
	}

	// Wait for expiry
	time.Sleep(20 * time.Millisecond)

	if cache.IsAuthorized("AKID1", "my-bucket") {
		t.Error("IsAuthorized() should return false after TTL expires")
	}
}

func TestAuthzCache_Count(t *testing.T) {
	cache := NewAuthzCache(10 * time.Minute)

	if cache.Count() != 0 {
		t.Errorf("Count() = %d, want 0", cache.Count())
	}

	cache.Grant("AKID1", "bucket1")
	cache.Grant("AKID1", "bucket2")
	cache.Grant("AKID2", "bucket1")

	if cache.Count() != 3 {
		t.Errorf("Count() = %d, want 3", cache.Count())
	}
}

func TestAuthzCache_DefaultTTL(t *testing.T) {
	// Zero TTL should use default
	cache := NewAuthzCache(0)

	cache.Grant("AKID1", "bucket1")
	if !cache.IsAuthorized("AKID1", "bucket1") {
		t.Error("IsAuthorized() should return true with default TTL")
	}
}

func TestAuthzCache_RevokeNonexistent(t *testing.T) {
	cache := NewAuthzCache(10 * time.Minute)

	// Should not panic
	cache.Revoke("AKID1", "nonexistent-bucket")
}
