package cache

import (
	"context"
	"net/http"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
)

func TestCompletionCache(t *testing.T) {
	// Create an in-memory cache for testing
	memCache := cacheclient.NewMemoryCache()
	cache := NewCacheWithClient(memCache, nil)

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-key"
	uploadId := "upload-123"

	// Test 1: GetCompletion returns not found for non-existent entry without asking for caller identity.
	t.Run("GetCompletion_NotFound", func(t *testing.T) {
		identityChecked := false
		entry, found, err := cache.GetCompletion(ctx, bucket, key, uploadId, func() (string, bool) {
			identityChecked = true
			return "client-a", true
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if found {
			t.Error("expected not found, got found")
		}
		if entry != nil {
			t.Error("expected nil entry, got non-nil")
		}
		if identityChecked {
			t.Error("caller identity was checked for a cache miss")
		}
	})

	// Test 2: PutCompletion stores and GetCompletion retrieves
	t.Run("PutCompletion_and_GetCompletion", func(t *testing.T) {
		headers := http.Header{
			"Content-Type": []string{"application/xml"},
			"X-Amz-Id-2":   []string{"test-id"},
			"ETag":         []string{"\"abc123\""},
		}
		body := []byte(`<CompleteMultipartUploadResult><ETag>"abc123"</ETag></CompleteMultipartUploadResult>`)
		statusCode := 200

		err := cache.PutCompletion(ctx, bucket, key, uploadId, "client-a", statusCode, headers, body)
		if err != nil {
			t.Fatalf("PutCompletion failed: %v", err)
		}

		// Retrieve and verify for the caller bound to this completion.
		entry, found, err := cache.GetCompletion(ctx, bucket, key, uploadId, func() (string, bool) {
			return "client-a", true
		})
		if err != nil {
			t.Fatalf("GetCompletion failed: %v", err)
		}
		if !found {
			t.Fatal("expected entry to be found")
		}
		if entry == nil {
			t.Fatal("expected non-nil entry")
		}
		if entry.StatusCode != statusCode {
			t.Errorf("expected status code %d, got %d", statusCode, entry.StatusCode)
		}
		if entry.Headers["Content-Type"] != "application/xml" {
			t.Errorf("expected Content-Type 'application/xml', got %q", entry.Headers["Content-Type"])
		}
		if entry.Headers["ETag"] != "\"abc123\"" {
			t.Errorf("expected ETag '\"abc123\"', got %q", entry.Headers["ETag"])
		}
		if string(entry.Body) != string(body) {
			t.Errorf("expected body %q, got %q", body, entry.Body)
		}

		wrongEntry, wrongFound, err := cache.GetCompletion(ctx, bucket, key, uploadId, func() (string, bool) {
			return "client-b", true
		})
		if err != nil {
			t.Fatalf("GetCompletion for a different principal failed: %v", err)
		}
		if wrongFound || wrongEntry != nil {
			t.Fatalf("completion for a different principal was returned: entry=%+v found=%t", wrongEntry, wrongFound)
		}
	})

	// Test 3: Different uploadId returns not found
	t.Run("Different_UploadId_NotFound", func(t *testing.T) {
		entry, found, err := cache.GetCompletion(ctx, bucket, key, "different-upload-id", func() (string, bool) {
			return "client-a", true
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if found {
			t.Error("expected not found for different uploadId")
		}
		if entry != nil {
			t.Error("expected nil entry for different uploadId")
		}
	})

	// Test 4: Responses without an authenticated principal cannot be replayed.
	t.Run("PutCompletion_WithoutPrincipal", func(t *testing.T) {
		if err := cache.PutCompletion(ctx, bucket, key, "anonymous-upload", "", http.StatusOK, nil, nil); err != nil {
			t.Fatalf("PutCompletion failed: %v", err)
		}
		entry, found, err := cache.GetCompletion(ctx, bucket, key, "anonymous-upload", func() (string, bool) {
			return "client-a", true
		})
		if err != nil {
			t.Fatalf("GetCompletion failed: %v", err)
		}
		if found || entry != nil {
			t.Fatalf("completion without a principal was cached: entry=%+v found=%t", entry, found)
		}
	})

	// Test 5: MakeCompletionKey format
	t.Run("MakeCompletionKey_Format", func(t *testing.T) {
		key := MakeCompletionKey("bucket", "path/to/object", "upload-abc")
		expected := "complete:bucket|path/to/object|upload-abc"
		if key != expected {
			t.Errorf("expected key %q, got %q", expected, key)
		}
	})

	// Test 6: Components containing the legacy separator cannot alias.
	t.Run("MakeCompletionKey_DelimiterAliases", func(t *testing.T) {
		first := MakeCompletionKey("bucket", "object", "id|suffix")
		second := MakeCompletionKey("bucket", "object|id", "suffix")
		if first == second {
			t.Fatalf("distinct completion tuples share key %q", first)
		}
		if first != "complete:bucket:object:id%7Csuffix" {
			t.Errorf("unexpected encoded key for first tuple: %q", first)
		}
		if second != "complete:bucket:object%7Cid:suffix" {
			t.Errorf("unexpected encoded key for second tuple: %q", second)
		}
	})
}

func TestCompletionCache_DisabledCache(t *testing.T) {
	// Create a disabled cache
	cache := &Cache{enabled: false}

	ctx := context.Background()

	// GetCompletion should return not found without error
	entry, found, err := cache.GetCompletion(ctx, "bucket", "key", "uploadId", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("expected not found for disabled cache")
	}
	if entry != nil {
		t.Error("expected nil entry for disabled cache")
	}

	// PutCompletion should succeed without error (no-op)
	err = cache.PutCompletion(ctx, "bucket", "key", "uploadId", "client", 200, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error for disabled cache: %v", err)
	}
}
