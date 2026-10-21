package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tigrisdata/tag/cache"
)

const (
	mpBucket = "mp-bucket"
	mpKey    = "mymultipart"
	mpUpload = "upload-123"
)

// completeMultipartRequest builds a completion signed as the mock-validated
// principal "access": the Authorization header identifies the caller and the
// content hash lets the request be validated locally (the mock accepts any).
func completeMultipartRequest() *http.Request {
	return completeMultipartRequestAs("access")
}

func completeMultipartRequestAs(accessKey string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/"+mpBucket+"/"+mpKey+"?uploadId="+mpUpload, nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+accessKey+"/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=deadbeef")
	r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	r.Header.Set("X-Amz-Date", "20260101T000000Z")
	return r
}

// completion result body (a genuine success — not an <Error> document).
var mpSuccessBody = []byte(`<CompleteMultipartUploadResult><ETag>"new-etag"</ETag></CompleteMultipartUploadResult>`)

func seedStale(t *testing.T, c *cache.Cache) {
	t.Helper()
	meta := &cache.CachedObjectMeta{Bucket: mpBucket, Key: mpKey, ETag: `"stale"`, ContentLength: 5, StatusCode: 200}
	if err := c.PutWithMeta(context.Background(), mpBucket, mpKey, meta, []byte("stale"), 60); err != nil {
		t.Fatalf("seed stale object: %v", err)
	}
}

func staleCached(c *cache.Cache) bool {
	_, found, _ := c.GetMeta(context.Background(), mpBucket, mpKey)
	return found
}

// The core of issue #110: completing a multipart upload overwrites the object, so a
// previously cached version must not survive to be served on a later GET. A GET that
// raced the in-flight completion can also re-cache the pre-overwrite object; a
// successful completion must re-invalidate it (read-after-write).
func TestHandleCompleteMultipartUpload_SuccessInvalidatesOverwrittenObject(t *testing.T) {
	var c *cache.Cache
	repopulateThenSucceed := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		// Racing GET re-caches the pre-overwrite object mid-completion.
		meta := &cache.CachedObjectMeta{Bucket: mpBucket, Key: mpKey, ETag: `"stale"`, ContentLength: 5, StatusCode: 200}
		_ = c.PutWithMeta(context.Background(), mpBucket, mpKey, meta, []byte("stale"), 60)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(mpSuccessBody)
		return &ResponseCapture{StatusCode: http.StatusOK, Headers: http.Header{}, Body: mpSuccessBody, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenSucceed}, true)

	w := httptest.NewRecorder()
	if err := svc.HandleCompleteMultipartUpload(w, completeMultipartRequest()); err != nil {
		t.Fatalf("HandleCompleteMultipartUpload: %v", err)
	}
	if staleCached(c) {
		t.Error("pre-overwrite object still cached after a successful completion — read-after-write invalidation missing")
	}
}

// A completion that fails as 200 OK with an <Error> body did not overwrite the
// object, so a racing refill of the still-current object must survive — mirroring
// CopyObject's 200-with-error-body handling.
func TestHandleCompleteMultipartUpload_200ErrorBodyKeepsRacingRefill(t *testing.T) {
	var c *cache.Cache
	errorBody := []byte(`<Error><Code>InternalError</Code></Error>`)
	repopulateThen200Error := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		meta := &cache.CachedObjectMeta{Bucket: mpBucket, Key: mpKey, ETag: `"refill"`, ContentLength: 6, StatusCode: 200}
		_ = c.PutWithMeta(context.Background(), mpBucket, mpKey, meta, []byte("refill"), 60)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(errorBody)
		return &ResponseCapture{StatusCode: http.StatusOK, Headers: http.Header{}, Body: errorBody, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThen200Error}, true)

	w := httptest.NewRecorder()
	if err := svc.HandleCompleteMultipartUpload(w, completeMultipartRequest()); err != nil {
		t.Fatalf("HandleCompleteMultipartUpload: %v", err)
	}
	if !staleCached(c) {
		t.Error("racing refill discarded after a 200-with-error-body completion — that completion did not overwrite the object")
	}

	// A failed completion must not be cached as a successful idempotent replay.
	if _, found, _ := c.GetCompletion(context.Background(), mpBucket, mpKey, mpUpload, "access"); found {
		t.Error("200-with-error-body completion was cached as a successful completion — an idempotent replay would return the error as success")
	}
}

// The pre-forward invalidation must fire even when the forward itself errors out:
// the object may already have been overwritten upstream, so a stale cached copy must
// not be left behind. This isolates the before-forward invalidation from the
// after-forward one (which does not run when the forward returns an error).
func TestHandleCompleteMultipartUpload_InvalidatesBeforeForward(t *testing.T) {
	var c *cache.Cache
	forwardErr := errors.New("upstream unreachable")
	failForward := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		return nil, forwardErr
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: failForward}, true)
	seedStale(t, c)

	w := httptest.NewRecorder()
	if err := svc.HandleCompleteMultipartUpload(w, completeMultipartRequest()); !errors.Is(err, forwardErr) {
		t.Fatalf("HandleCompleteMultipartUpload err = %v, want %v", err, forwardErr)
	}
	if staleCached(c) {
		t.Error("stale object survived a completion whose forward errored — pre-forward invalidation missing")
	}
}

// A successful completion is cached for idempotent replay: a second call with the
// same uploadId by the same validated principal must return the stored response
// without forwarding upstream again. The entry is bound to that principal, so a
// different caller for the same uploadId is forwarded upstream instead.
func TestHandleCompleteMultipartUpload_SuccessCachedForIdempotentReplay(t *testing.T) {
	var forwards int
	succeed := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		forwards++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(mpSuccessBody)
		return &ResponseCapture{StatusCode: http.StatusOK, Headers: http.Header{}, Body: mpSuccessBody, Complete: true}, nil
	}

	svc, _ := newTestService(&mockForwarder{captureFunc: succeed}, true)

	for i := range 2 {
		w := httptest.NewRecorder()
		if err := svc.HandleCompleteMultipartUpload(w, completeMultipartRequest()); err != nil {
			t.Fatalf("HandleCompleteMultipartUpload call %d: %v", i, err)
		}
		if got := w.Body.Bytes(); string(got) != string(mpSuccessBody) {
			t.Errorf("call %d body = %q, want %q", i, got, mpSuccessBody)
		}
	}
	if forwards != 1 {
		t.Errorf("upstream forwarded %d times; the second identical completion must be served from the idempotency cache", forwards)
	}

	w := httptest.NewRecorder()
	if err := svc.HandleCompleteMultipartUpload(w, completeMultipartRequestAs("other")); err != nil {
		t.Fatalf("HandleCompleteMultipartUpload as another principal: %v", err)
	}
	if forwards != 2 {
		t.Errorf("upstream forwarded %d times; a completion by another principal must not be served from a caller-bound cache entry", forwards)
	}
}
