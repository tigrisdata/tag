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

// A rejected CopyObject leaves the destination unchanged, so the post-forward
// re-invalidation must NOT fire — otherwise a racing refill of the still-valid
// destination is discarded, causing an unnecessary later miss.
func TestHandleCopyObject_FailedCopyKeepsRacingRefill(t *testing.T) {
	var c *cache.Cache
	repopulateThenFail := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		b, k := ParseBucketKey(r)
		meta := &cache.CachedObjectMeta{Bucket: b, Key: k, ETag: `"refill"`, ContentLength: 6, StatusCode: 200}
		_ = c.PutWithMeta(context.Background(), b, k, meta, []byte("refill"), 60)
		body := []byte(`<Error><Code>AccessDenied</Code></Error>`)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write(body)
		return &ResponseCapture{StatusCode: http.StatusForbidden, Body: body, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenFail}, true)

	r := httptest.NewRequest(http.MethodPut, "/dst-bucket/dst-key", nil)
	r.Header.Set("X-Amz-Copy-Source", "/src-bucket/src-key")
	w := httptest.NewRecorder()
	if err := svc.HandleCopyObject(w, r); err != nil {
		t.Fatalf("HandleCopyObject: %v", err)
	}

	b, k := ParseBucketKey(r)
	if _, found, _ := c.GetMeta(context.Background(), b, k); !found {
		t.Error("racing refill discarded after a FAILED copy — re-invalidation must be gated on success")
	}
}

// A CopyObject that returns 200 OK but an <Error> body actually failed, so the
// destination is unchanged and must not be re-invalidated.
func TestHandleCopyObject_200ErrorBodyKeepsRacingRefill(t *testing.T) {
	var c *cache.Cache
	repopulateThen200Error := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		b, k := ParseBucketKey(r)
		meta := &cache.CachedObjectMeta{Bucket: b, Key: k, ETag: `"refill"`, ContentLength: 6, StatusCode: 200}
		_ = c.PutWithMeta(context.Background(), b, k, meta, []byte("refill"), 60)
		body := []byte(`<Error><Code>InternalError</Code></Error>`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return &ResponseCapture{StatusCode: http.StatusOK, Body: body, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThen200Error}, true)

	r := httptest.NewRequest(http.MethodPut, "/dst-bucket/dst-key", nil)
	w := httptest.NewRecorder()
	if err := svc.HandleCopyObject(w, r); err != nil {
		t.Fatalf("HandleCopyObject: %v", err)
	}

	b, k := ParseBucketKey(r)
	if _, found, _ := c.GetMeta(context.Background(), b, k); !found {
		t.Error("racing refill discarded after a 200-with-error-body copy — that copy did not succeed")
	}
}

// A successful CopyObject changed the destination, so the post-forward
// re-invalidation must fire to drop any stale racing refill.
func TestHandleCopyObject_SuccessReinvalidates(t *testing.T) {
	var c *cache.Cache
	repopulateThenSucceed := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		b, k := ParseBucketKey(r)
		meta := &cache.CachedObjectMeta{Bucket: b, Key: k, ETag: `"stale"`, ContentLength: 5, StatusCode: 200}
		_ = c.PutWithMeta(context.Background(), b, k, meta, []byte("stale"), 60)
		body := []byte(`<CopyObjectResult><ETag>"new"</ETag></CopyObjectResult>`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return &ResponseCapture{StatusCode: http.StatusOK, Body: body, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenSucceed}, true)

	r := httptest.NewRequest(http.MethodPut, "/dst-bucket/dst-key", nil)
	w := httptest.NewRecorder()
	if err := svc.HandleCopyObject(w, r); err != nil {
		t.Fatalf("HandleCopyObject: %v", err)
	}

	b, k := ParseBucketKey(r)
	if _, found, _ := c.GetMeta(context.Background(), b, k); found {
		t.Error("stale refill survived a SUCCESSFUL copy — post-forward re-invalidation should have removed it")
	}
}

// A bulk delete with a per-object failure must re-invalidate only the keys that
// were actually deleted; a racing refill of a FAILED key must survive.
func TestHandleDeleteObjects_PartialFailureKeepsFailedKeyRefill(t *testing.T) {
	var c *cache.Cache
	const okKey = "ok-key"
	const failKey = "fail-key"
	const responseBody = `<DeleteResult><Deleted><Key>ok-key</Key></Deleted><Error><Key>fail-key</Key><Code>AccessDenied</Code></Error></DeleteResult>`
	headers := make(http.Header)
	headers.Set("Content-Type", "application/xml")
	headers.Set("X-Upstream-Delete-Result", "partial")

	repopulateThenPartial := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		b, _ := ParseBucketKey(r)
		for _, k := range []string{okKey, failKey} {
			meta := &cache.CachedObjectMeta{Bucket: b, Key: k, ETag: `"refill"`, ContentLength: 6, StatusCode: 200}
			_ = c.PutWithMeta(context.Background(), b, k, meta, []byte("refill"), 60)
		}
		body := []byte(responseBody)
		for key, values := range headers {
			w.Header()[key] = append([]string(nil), values...)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return &ResponseCapture{StatusCode: http.StatusOK, Headers: headers.Clone(), Body: body, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenPartial}, true)

	reqBody := `<Delete><Object><Key>ok-key</Key></Object><Object><Key>fail-key</Key></Object></Delete>`
	r := httptest.NewRequest(http.MethodPost, "/bulk-bucket?delete", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	if err := svc.HandleDeleteObjects(w, r); err != nil {
		t.Fatalf("HandleDeleteObjects: %v", err)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("DeleteObjects status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get("X-Upstream-Delete-Result"); got != "partial" {
		t.Fatalf("DeleteObjects response header = %q, want partial", got)
	}
	if got := w.Body.String(); got != responseBody {
		t.Fatalf("DeleteObjects response body = %q, want byte-exact upstream body %q", got, responseBody)
	}

	b, _ := ParseBucketKey(r)
	if _, found, _ := c.GetMeta(context.Background(), b, okKey); found {
		t.Error("deleted key's stale refill survived — it should have been re-invalidated")
	}
	if _, found, _ := c.GetMeta(context.Background(), b, failKey); !found {
		t.Error("failed key's valid refill was discarded — failed keys must not be re-invalidated")
	}
}

// A DeleteObjects request can list the same key under multiple version IDs. If one
// version's delete succeeds and another fails, the key's current object may have
// changed, so a racing refill must be dropped. Matching failures by key alone would
// wrongly treat the key as fully failed and keep the stale refill.
func TestHandleDeleteObjects_MixedVersionSuccessReinvalidatesKey(t *testing.T) {
	var c *cache.Cache
	const key = "versioned-key"

	repopulateThenMixed := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		b, _ := ParseBucketKey(r)
		meta := &cache.CachedObjectMeta{Bucket: b, Key: key, ETag: `"refill"`, ContentLength: 6, StatusCode: 200}
		_ = c.PutWithMeta(context.Background(), b, key, meta, []byte("refill"), 60)
		body := []byte(`<DeleteResult><Deleted><Key>versioned-key</Key><VersionId>v1</VersionId></Deleted><Error><Key>versioned-key</Key><VersionId>v2</VersionId><Code>AccessDenied</Code></Error></DeleteResult>`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return &ResponseCapture{StatusCode: http.StatusOK, Body: body, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenMixed}, true)

	reqBody := `<Delete><Object><Key>versioned-key</Key><VersionId>v1</VersionId></Object><Object><Key>versioned-key</Key><VersionId>v2</VersionId></Object></Delete>`
	r := httptest.NewRequest(http.MethodPost, "/bulk-bucket?delete", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	if err := svc.HandleDeleteObjects(w, r); err != nil {
		t.Fatalf("HandleDeleteObjects: %v", err)
	}

	b, _ := ParseBucketKey(r)
	if _, found, _ := c.GetMeta(context.Background(), b, key); found {
		t.Error("key with a succeeded version-delete kept its stale refill — should be re-invalidated")
	}
}

// A failed unqualified delete may be reported with a VersionId the request never
// sent ("null" on a version-suspended bucket, or a concrete id the server
// resolved). Counting errors by key (not matching version tuples) keeps such a
// failed key's valid refill instead of over-invalidating it.
func TestHandleDeleteObjects_VersionIdMismatchFailureKeepsRefill(t *testing.T) {
	for _, respVersion := range []string{"null", "concrete-v3"} {
		t.Run(respVersion, func(t *testing.T) {
			var c *cache.Cache
			const key = "unqualified-key"

			repopulateThenFail := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
				b, _ := ParseBucketKey(r)
				meta := &cache.CachedObjectMeta{Bucket: b, Key: key, ETag: `"refill"`, ContentLength: 6, StatusCode: 200}
				_ = c.PutWithMeta(context.Background(), b, key, meta, []byte("refill"), 60)
				// Request sent no VersionId; response errors with a different representation.
				body := []byte(`<DeleteResult><Error><Key>unqualified-key</Key><VersionId>` + respVersion + `</VersionId><Code>AccessDenied</Code></Error></DeleteResult>`)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(body)
				return &ResponseCapture{StatusCode: http.StatusOK, Body: body, Complete: true}, nil
			}

			var svc *Service
			svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenFail}, true)

			// Request omits VersionId entirely.
			reqBody := `<Delete><Object><Key>unqualified-key</Key></Object></Delete>`
			r := httptest.NewRequest(http.MethodPost, "/bulk-bucket?delete", strings.NewReader(reqBody))
			w := httptest.NewRecorder()
			if err := svc.HandleDeleteObjects(w, r); err != nil {
				t.Fatalf("HandleDeleteObjects: %v", err)
			}

			b, _ := ParseBucketKey(r)
			if _, found, _ := c.GetMeta(context.Background(), b, key); !found {
				t.Errorf("failed delete (response VersionId=%q) over-invalidated the key — valid refill discarded", respVersion)
			}
		})
	}
}

// If the upstream result cannot be parsed, bulk delete conservatively invalidates
// every requested key and still returns the upstream status, headers, and bytes.
func TestHandleDeleteObjects_UnparseableResultInvalidatesAllAndPreservesResponse(t *testing.T) {
	var c *cache.Cache
	const firstKey = "first-key"
	const secondKey = "second-key"
	const responseBody = `<Error><Code>InternalError</Code><Message>result could not be parsed</Message></Error>`
	headers := make(http.Header)
	headers.Set("Content-Type", "application/xml")
	headers.Set("X-Upstream-Delete-Result", "unparseable")

	repopulateThenReturnUnparseable := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		bucket, _ := ParseBucketKey(r)
		for _, key := range []string{firstKey, secondKey} {
			meta := &cache.CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"refill"`, ContentLength: 6, StatusCode: http.StatusOK}
			if err := c.PutWithMeta(context.Background(), bucket, key, meta, []byte("refill"), 60); err != nil {
				return nil, err
			}
		}
		for key, values := range headers {
			w.Header()[key] = append([]string(nil), values...)
		}
		body := []byte(responseBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return &ResponseCapture{StatusCode: http.StatusOK, Headers: headers.Clone(), Body: body, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenReturnUnparseable}, true)
	requestBody := `<Delete><Object><Key>first-key</Key></Object><Object><Key>second-key</Key></Object></Delete>`
	r := httptest.NewRequest(http.MethodPost, "/bulk-bucket?delete", strings.NewReader(requestBody))
	w := httptest.NewRecorder()
	if err := svc.HandleDeleteObjects(w, r); err != nil {
		t.Fatalf("HandleDeleteObjects: %v", err)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("DeleteObjects status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get("X-Upstream-Delete-Result"); got != "unparseable" {
		t.Fatalf("DeleteObjects response header = %q, want unparseable", got)
	}
	if got := w.Body.String(); got != responseBody {
		t.Fatalf("DeleteObjects response body = %q, want byte-exact upstream body %q", got, responseBody)
	}
	bucket, _ := ParseBucketKey(r)
	for _, key := range []string{firstKey, secondKey} {
		if _, found, err := c.GetMeta(context.Background(), bucket, key); err != nil || found {
			t.Errorf("unparseable DeleteResult left %q cached: found=%t err=%v", key, found, err)
		}
	}
}

// A forwarded HTTP failure is not a successful delete: it retains any refill
// and preserves the upstream error response without running the success fence.
func TestHandleDeleteObjects_UpstreamFailurePreservesResponseAndRefill(t *testing.T) {
	var c *cache.Cache
	const key = "failed-key"
	const responseBody = `<Error><Code>AccessDenied</Code><Message>delete denied</Message></Error>`
	headers := make(http.Header)
	headers.Set("Content-Type", "application/xml")
	headers.Set("X-Upstream-Delete-Result", "failed")

	repopulateThenFail := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		bucket, _ := ParseBucketKey(r)
		meta := &cache.CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"refill"`, ContentLength: 6, StatusCode: http.StatusOK}
		if err := c.PutWithMeta(context.Background(), bucket, key, meta, []byte("refill"), 60); err != nil {
			return nil, err
		}
		for header, values := range headers {
			w.Header()[header] = append([]string(nil), values...)
		}
		body := []byte(responseBody)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write(body)
		return &ResponseCapture{StatusCode: http.StatusForbidden, Headers: headers.Clone(), Body: body, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenFail}, true)
	r := httptest.NewRequest(http.MethodPost, "/bulk-bucket?delete", strings.NewReader(`<Delete><Object><Key>failed-key</Key></Object></Delete>`))
	w := httptest.NewRecorder()
	if err := svc.HandleDeleteObjects(w, r); err != nil {
		t.Fatalf("HandleDeleteObjects: %v", err)
	}
	if w.Code != http.StatusForbidden {
		t.Fatalf("DeleteObjects status = %d, want %d", w.Code, http.StatusForbidden)
	}
	if got := w.Header().Get("X-Upstream-Delete-Result"); got != "failed" {
		t.Fatalf("DeleteObjects response header = %q, want failed", got)
	}
	if got := w.Body.String(); got != responseBody {
		t.Fatalf("DeleteObjects response body = %q, want byte-exact upstream body %q", got, responseBody)
	}
	bucket, _ := ParseBucketKey(r)
	if _, found, err := c.GetMeta(context.Background(), bucket, key); err != nil || !found {
		t.Fatalf("failed delete discarded the refill: found=%t err=%v", found, err)
	}
}
