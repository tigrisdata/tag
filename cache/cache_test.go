package cache

import (
	"testing"
)

func TestIsNotFoundError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "key not found",
			err:      &testError{msg: "key not found"},
			expected: true,
		},
		{
			name:     "not found",
			err:      &testError{msg: "not found"},
			expected: true,
		},
		{
			name:     "contains NotFound",
			err:      &testError{msg: "rpc error: code = NotFound desc = key does not exist"},
			expected: true,
		},
		{
			name:     "contains not found lowercase",
			err:      &testError{msg: "cache: key not found in store"},
			expected: true,
		},
		{
			name:     "other error",
			err:      &testError{msg: "connection timeout"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNotFoundError(tt.err)
			if result != tt.expected {
				t.Errorf("isNotFoundError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

// testError is a simple error implementation for testing
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestCache_IsEnabled_Disabled(t *testing.T) {
	cache := &Cache{enabled: false}

	if cache.IsEnabled() {
		t.Error("IsEnabled() = true, want false for disabled cache")
	}
}

func TestCache_IsEnabled_Enabled(t *testing.T) {
	cache := &Cache{enabled: true}

	if !cache.IsEnabled() {
		t.Error("IsEnabled() = false, want true for enabled cache")
	}
}

func TestCache_GetMode_Disabled(t *testing.T) {
	cache := &Cache{enabled: false}

	mode := cache.GetMode()
	if mode != "disabled" {
		t.Errorf("GetMode() = %q, want %q", mode, "disabled")
	}
}

func TestCache_GetConnectedNodes_Disabled(t *testing.T) {
	cache := &Cache{enabled: false}

	nodes := cache.GetConnectedNodes()
	if nodes != nil {
		t.Errorf("GetConnectedNodes() = %v, want nil", nodes)
	}
}

func TestCache_Close_Disabled(t *testing.T) {
	cache := &Cache{enabled: false}

	err := cache.Close()
	if err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

func TestCache_IsClosed(t *testing.T) {
	cache := &Cache{enabled: true, closed: false}

	if cache.IsClosed() {
		t.Error("IsClosed() = true before close")
	}

	cache.closed = true

	if !cache.IsClosed() {
		t.Error("IsClosed() = false after close")
	}
}

func TestCache_DisabledOperationsReturnNil(t *testing.T) {
	cache := &Cache{enabled: false}

	// GetWithMeta should return not found for disabled cache
	meta, reader, found, err := cache.GetWithMeta(t.Context(), "bucket", "key")
	if err != nil {
		t.Errorf("GetWithMeta() error = %v, want nil", err)
	}
	if found {
		t.Error("GetWithMeta() found = true, want false")
	}
	if reader != nil {
		t.Error("GetWithMeta() reader != nil, want nil")
	}
	if meta != nil {
		t.Error("GetWithMeta() meta != nil, want nil")
	}

	// PutWithMeta should succeed silently
	testMeta := &CachedObjectMeta{Bucket: "bucket", Key: "key"}
	if err := cache.PutWithMeta(t.Context(), "bucket", "key", testMeta, []byte("data"), 60); err != nil {
		t.Errorf("PutWithMeta() error = %v, want nil", err)
	}

	// Delete should succeed silently
	if err := cache.Delete(t.Context(), "bucket", "key"); err != nil {
		t.Errorf("Delete() error = %v, want nil", err)
	}

	// Has should return false
	if cache.Has(t.Context(), "bucket", "key") {
		t.Error("Has() = true, want false")
	}
}
