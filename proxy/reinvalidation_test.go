package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tigrisdata/tag/cache"
)

// A GET that races an in-flight PUT can fetch the pre-PUT object and begin
// re-caching it. HandlePutObject must re-invalidate AFTER forwarding so that
// stale repopulation does not survive (read-after-write).
func TestHandlePutObject_ReinvalidatesAfterForward(t *testing.T) {
	var c *cache.Cache

	// Simulate the racing GET writing a stale entry mid-PUT (during Forward).
	repopulate := func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		b, k := ParseBucketKey(r)
		meta := &cache.CachedObjectMeta{Bucket: b, Key: k, ETag: `"stale"`, ContentLength: 5, StatusCode: 200}
		_ = c.PutWithMeta(context.Background(), b, k, meta, []byte("stale"), 60)
		w.WriteHeader(http.StatusOK)
		return nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{forwardFunc: repopulate}, true)

	r := httptest.NewRequest(http.MethodPut, "/test-bucket/test-key", strings.NewReader("new body"))
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, r); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	b, k := ParseBucketKey(r)
	if _, found, _ := c.GetMeta(context.Background(), b, k); found {
		t.Error("stale entry still cached after PUT — post-forward re-invalidation missing")
	}
}
