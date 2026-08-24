package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tigrisdata/tag/cache"
)

func TestHandleHeadObject_DecodedMetaTierUpdateAndInvalidation(t *testing.T) {
	const (
		bucket = "head-tier-bucket"
		key    = "head-tier-key"
	)

	service, store := newTestService(&mockForwarder{}, true)
	putMeta := func(etag, version string) {
		t.Helper()
		meta := &cache.CachedObjectMeta{
			Bucket:        bucket,
			Key:           key,
			ETag:          etag,
			ContentLength: 4,
			StatusCode:    http.StatusOK,
			UserMetadata: map[string]string{
				"x-amz-meta-version": version,
			},
		}
		if err := store.PutWithMeta(context.Background(), bucket, key, meta, []byte("body"), 60); err != nil {
			t.Fatalf("PutWithMeta %s: %v", etag, err)
		}
	}
	head := func() *httptest.ResponseRecorder {
		t.Helper()
		writer := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodHead, "/"+bucket+"/"+key, nil)
		if err := service.HandleHeadObject(writer, request); err != nil {
			t.Fatalf("HandleHeadObject: %v", err)
		}
		return writer
	}

	putMeta(`"v1"`, "one")
	first := head()
	if got := first.Header().Get("ETag"); got != `"v1"` {
		t.Fatalf("first HEAD ETag = %q, want v1", got)
	}
	if got := first.Header().Get("x-amz-meta-version"); got != "one" {
		t.Fatalf("first HEAD metadata header = %q, want one", got)
	}
	// The next two requests admit the decoded snapshot before the update below.
	for read := 2; read <= 3; read++ {
		warmed := head()
		if got := warmed.Header().Get("ETag"); got != `"v1"` {
			t.Fatalf("warmed HEAD %d ETag = %q, want v1", read, got)
		}
	}

	putMeta(`"v2"`, "two")
	second := head()
	if got := second.Header().Get("ETag"); got != `"v2"` {
		t.Fatalf("HEAD ETag after metadata update = %q, want v2", got)
	}
	if got := second.Header().Get("x-amz-meta-version"); got != "two" {
		t.Fatalf("HEAD metadata header after update = %q, want two", got)
	}

	if err := store.DeleteWithMeta(context.Background(), bucket, key); err != nil {
		t.Fatalf("DeleteWithMeta: %v", err)
	}
	third := head()
	if got := third.Header().Get(XCacheHeader); got != XCacheMiss {
		t.Fatalf("X-Cache after invalidation = %q, want %q", got, XCacheMiss)
	}
	if got := third.Header().Get("ETag"); got != "" {
		t.Fatalf("HEAD served stale ETag after invalidation: %q", got)
	}
	if got := third.Header().Get("x-amz-meta-version"); got != "" {
		t.Fatalf("HEAD served stale metadata header after invalidation: %q", got)
	}
}
