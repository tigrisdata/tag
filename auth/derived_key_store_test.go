package auth

import (
	"testing"
	"time"
)

func TestDerivedKeyStore_StoreAndGet(t *testing.T) {
	store := NewDerivedKeyStore()
	today := time.Now().UTC().Format("20060102")

	key := []byte("test-signing-key-32-bytes-long!!")
	store.Store("AKID1", today, "auto", key)

	got, err := store.GetSigningKey("AKID1", today, "auto")
	if err != nil {
		t.Fatalf("GetSigningKey() error = %v", err)
	}
	if string(got) != string(key) {
		t.Errorf("GetSigningKey() = %x, want %x", got, key)
	}
}

func TestDerivedKeyStore_UnknownKey(t *testing.T) {
	store := NewDerivedKeyStore()
	today := time.Now().UTC().Format("20060102")

	_, err := store.GetSigningKey("UNKNOWN", today, "auto")
	if err == nil {
		t.Fatal("GetSigningKey() should return error for unknown key")
	}
}

func TestDerivedKeyStore_HasKey(t *testing.T) {
	store := NewDerivedKeyStore()
	today := time.Now().UTC().Format("20060102")

	if store.HasKey("AKID1") {
		t.Error("HasKey() should return false for empty store")
	}

	store.Store("AKID1", today, "auto", []byte("key"))

	if !store.HasKey("AKID1") {
		t.Error("HasKey() should return true after Store")
	}

	if store.HasKey("AKID2") {
		t.Error("HasKey() should return false for unknown key")
	}
}

func TestDerivedKeyStore_DifferentDates(t *testing.T) {
	store := NewDerivedKeyStore()

	key1 := []byte("key-for-day-1")
	key2 := []byte("key-for-day-2")

	today := time.Now().UTC().Format("20060102")
	tomorrow := time.Now().UTC().AddDate(0, 0, 1).Format("20060102")

	store.Store("AKID1", today, "auto", key1)
	store.Store("AKID1", tomorrow, "auto", key2)

	got1, err := store.GetSigningKey("AKID1", today, "auto")
	if err != nil {
		t.Fatalf("GetSigningKey(today) error = %v", err)
	}
	if string(got1) != string(key1) {
		t.Errorf("GetSigningKey(today) = %x, want %x", got1, key1)
	}

	got2, err := store.GetSigningKey("AKID1", tomorrow, "auto")
	if err != nil {
		t.Fatalf("GetSigningKey(tomorrow) error = %v", err)
	}
	if string(got2) != string(key2) {
		t.Errorf("GetSigningKey(tomorrow) = %x, want %x", got2, key2)
	}
}

func TestDerivedKeyStore_LazyCleanup(t *testing.T) {
	store := NewDerivedKeyStore()
	today := time.Now().UTC().Format("20060102")
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Format("20060102")

	// Store keys for today and yesterday
	store.Store("AKID1", yesterday, "auto", []byte("yesterday-key"))
	store.Store("AKID1", today, "auto", []byte("today-key"))

	// Yesterday's key should survive (cleanup removes keys older than yesterday)
	if store.Count() != 2 {
		t.Fatalf("Count() = %d, want 2", store.Count())
	}

	// Store a key with a very old date — it gets cleaned up immediately
	store.Store("AKID2", "20200101", "auto", []byte("old-key"))

	// Old key should NOT survive (cleaned up during Store)
	_, err := store.GetSigningKey("AKID2", "20200101", "auto")
	if err == nil {
		t.Error("Very old key should have been cleaned up during Store")
	}

	// AKID2 should not be tracked since its only key was cleaned up
	if store.HasKey("AKID2") {
		t.Error("HasKey(AKID2) should return false after cleanup")
	}

	// Today's key should still exist
	_, err = store.GetSigningKey("AKID1", today, "auto")
	if err != nil {
		t.Errorf("Today's key should still exist: %v", err)
	}
}

func TestDerivedKeyStore_Count(t *testing.T) {
	store := NewDerivedKeyStore()

	if store.Count() != 0 {
		t.Errorf("Count() = %d, want 0", store.Count())
	}

	today := time.Now().UTC().Format("20060102")
	store.Store("AKID1", today, "auto", []byte("key1"))
	store.Store("AKID2", today, "auto", []byte("key2"))

	if store.Count() != 2 {
		t.Errorf("Count() = %d, want 2", store.Count())
	}
}

func TestDerivedKeyStore_Overwrite(t *testing.T) {
	store := NewDerivedKeyStore()

	today := time.Now().UTC().Format("20060102")
	store.Store("AKID1", today, "auto", []byte("old-value"))
	store.Store("AKID1", today, "auto", []byte("new-value"))

	got, _ := store.GetSigningKey("AKID1", today, "auto")
	if string(got) != "new-value" {
		t.Errorf("GetSigningKey() = %s, want new-value", got)
	}

	if store.Count() != 1 {
		t.Errorf("Count() = %d, want 1 (should overwrite, not duplicate)", store.Count())
	}
}
