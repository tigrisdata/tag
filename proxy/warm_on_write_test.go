package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const wowBucket, wowKey = "wow-bucket", "wow-key"

// authedPut builds a PUT that carries an Authorization header, so warmOnWrite takes
// the authenticated (signed-warm) path rather than the anonymous one.
func authedPut(bucket, key, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+key, strings.NewReader(body))
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request, Signature=deadbeef")
	return r
}

// warmObjectResponder is a DoFullObjectRequest (signed-warm) hook: it serves a small
// cacheable object and counts how many signed warms reached upstream.
func warmObjectResponder(count *atomic.Int32, body string) func(context.Context, string, string, string, string) (*http.Response, error) {
	return func(_ context.Context, _, _, _, _ string) (*http.Response, error) {
		count.Add(1)
		h := http.Header{}
		h.Set("ETag", `"warm-etag"`)
		h.Set("Content-Type", "application/octet-stream")
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        h,
			Body:          io.NopCloser(strings.NewReader(body)),
			ContentLength: int64(len(body)),
		}, nil
	}
}

// anonWarmResponder is a DoAnonymousFullObjectRequest hook returning the given status
// and counting anonymous warms. On 200 it omits X-Amz-Acl, so fetchFullObjectToCache
// injects public-read (as it does for a genuine anonymous read that succeeded).
func anonWarmResponder(count *atomic.Int32, body string, status int) func(context.Context, string, string) (*http.Response, error) {
	return func(_ context.Context, _, _ string) (*http.Response, error) {
		count.Add(1)
		h := http.Header{}
		if status == http.StatusOK {
			h.Set("ETag", `"anon-etag"`)
			h.Set("Content-Type", "application/octet-stream")
		}
		return &http.Response{
			StatusCode:    status,
			Header:        h,
			Body:          io.NopCloser(strings.NewReader(body)),
			ContentLength: int64(len(body)),
		}, nil
	}
}

// A successful authenticated PUT with warm_on_write enabled triggers a signed
// background fetch that repopulates the cache (not public-read), so a following read
// is a hit.
func TestHandlePutObject_WarmOnWrite_Authenticated(t *testing.T) {
	var signed, anon atomic.Int32
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusOK) // upstream PUT succeeds
			return nil
		},
		doFullObjectFunc:          warmObjectResponder(&signed, "warm-body"),
		doAnonymousFullObjectFunc: anonWarmResponder(&anon, "warm-body", http.StatusOK),
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, "new-body")); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("object was not warmed into cache after a successful authenticated PUT")
	}
	if got := signed.Load(); got != 1 {
		t.Errorf("signed warm fetches = %d, want 1", got)
	}
	if got := anon.Load(); got != 0 {
		t.Errorf("anonymous warm fetches = %d, want 0 (authenticated write)", got)
	}
	// Authenticated warm must not mark the entry public-read.
	if meta, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey); found && meta.IsPublicRead() {
		t.Error("authenticated warm cached the object as public-read")
	}
}

// An anonymous PUT warms with an UNSIGNED fetch; when that anonymous read succeeds
// (object is publicly readable), the object is cached WITH public-read so anonymous
// reads can hit.
func TestHandlePutObject_WarmOnWrite_AnonymousPublic(t *testing.T) {
	var signed, anon atomic.Int32
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusOK)
			return nil
		},
		doFullObjectFunc:          warmObjectResponder(&signed, "warm-body"),
		doAnonymousFullObjectFunc: anonWarmResponder(&anon, "public-body", http.StatusOK),
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true

	// No Authorization header → anonymous write.
	r := httptest.NewRequest(http.MethodPut, "/"+wowBucket+"/"+wowKey, strings.NewReader("new-body"))
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, r); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("publicly-readable object was not warmed after an anonymous PUT")
	}
	if got := anon.Load(); got != 1 {
		t.Errorf("anonymous warm fetches = %d, want 1", got)
	}
	if got := signed.Load(); got != 0 {
		t.Errorf("signed warm fetches = %d, want 0 (anonymous write must not sign)", got)
	}
	// The anonymous read succeeded, so the entry must be cached public-read.
	meta, found, _ := c.GetMeta(context.Background(), wowBucket, wowKey)
	if !found || !meta.IsPublicRead() {
		t.Errorf("anonymous warm did not cache public-read (found=%v, publicRead=%v)", found, found && meta.IsPublicRead())
	}
}

// An anonymous PUT to a bucket that is publicly writable but NOT publicly readable:
// the anonymous warm fetch gets 403, so nothing is cached — a private object is never
// exposed to anonymous readers via the cache.
func TestHandlePutObject_WarmOnWrite_AnonymousPrivateNotCached(t *testing.T) {
	var anon atomic.Int32
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusOK) // the anonymous WRITE succeeds (public-write)
			return nil
		},
		doAnonymousFullObjectFunc: anonWarmResponder(&anon, "", http.StatusForbidden), // but reads are private
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true

	r := httptest.NewRequest(http.MethodPut, "/"+wowBucket+"/"+wowKey, strings.NewReader("new-body"))
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, r); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("object was cached from an anonymous warm that got 403 (private object exposed)")
	}
	if got := anon.Load(); got != 1 {
		t.Errorf("anonymous warm fetches = %d, want 1 (the probe must have run)", got)
	}
}

// With warm_on_write disabled (the default), a successful PUT must not trigger any
// background fetch — behavior is unchanged from cache-on-read only.
func TestHandlePutObject_WarmOnWrite_DisabledByDefault(t *testing.T) {
	var signed, anon atomic.Int32
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusOK)
			return nil
		},
		doFullObjectFunc:          warmObjectResponder(&signed, "warm-body"),
		doAnonymousFullObjectFunc: anonWarmResponder(&anon, "warm-body", http.StatusOK),
	}
	svc, c := newTestService(mock, true) // WarmOnWrite defaults to false

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, "new-body")); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	// A negative assertion must exhaust the poll window to be meaningful.
	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("object was warmed despite warm_on_write=false")
	}
	if got := signed.Load() + anon.Load(); got != 0 {
		t.Errorf("warm fetches = %d, want 0 (warm_on_write disabled)", got)
	}
}

// A failed PUT (non-2xx) must not warm — there is no new object to cache.
func TestHandlePutObject_WarmOnWrite_SkippedOnFailure(t *testing.T) {
	var signed, anon atomic.Int32
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusForbidden) // upstream rejects the PUT
			return nil
		},
		doFullObjectFunc:          warmObjectResponder(&signed, "warm-body"),
		doAnonymousFullObjectFunc: anonWarmResponder(&anon, "warm-body", http.StatusOK),
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true

	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, "new-body")); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("object was warmed after a FAILED PUT")
	}
	if got := signed.Load() + anon.Load(); got != 0 {
		t.Errorf("warm fetches = %d, want 0 (PUT failed)", got)
	}
}

// An authenticated write whose credentials don't validate (e.g. signing mode, unknown
// key) must not warm — there is nothing to sign the background read with.
func TestHandlePutObject_WarmOnWrite_SkippedWhenCredsInvalid(t *testing.T) {
	var signed, anon atomic.Int32
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusOK)
			return nil
		},
		doFullObjectFunc:          warmObjectResponder(&signed, "warm-body"),
		doAnonymousFullObjectFunc: anonWarmResponder(&anon, "warm-body", http.StatusOK),
		validateFunc: func(*http.Request) (AuthResult, string, string, error) {
			return AuthNotValidated, "", "", nil // credentials present but not usable
		},
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true

	// Authorization present (not anonymous), but validation yields no credentials.
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, authedPut(wowBucket, wowKey, "new-body")); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("object was warmed with unusable credentials")
	}
	if got := signed.Load() + anon.Load(); got != 0 {
		t.Errorf("warm fetches = %d, want 0 (invalid credentials)", got)
	}
}

// CompleteMultipartUpload is the case a write-through tee cannot serve (TAG never
// sees the assembled body), so warm-on-write is the only way to make it hot.
func TestHandleCompleteMultipartUpload_WarmOnWrite(t *testing.T) {
	var signed atomic.Int32
	successBody := []byte(`<CompleteMultipartUploadResult><ETag>"mp-etag"</ETag></CompleteMultipartUploadResult>`)
	mock := &mockForwarder{
		captureFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) (*ResponseCapture, error) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(successBody)
			return &ResponseCapture{StatusCode: http.StatusOK, Headers: http.Header{}, Body: successBody, Complete: true}, nil
		},
		doFullObjectFunc: warmObjectResponder(&signed, "assembled-object"),
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true

	r := httptest.NewRequest(http.MethodPost, "/"+wowBucket+"/"+wowKey+"?uploadId=u1", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request, Signature=deadbeef")
	w := httptest.NewRecorder()
	if err := svc.HandleCompleteMultipartUpload(w, r); err != nil {
		t.Fatalf("HandleCompleteMultipartUpload: %v", err)
	}

	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("multipart-completed object was not warmed into cache")
	}
	if got := signed.Load(); got != 1 {
		t.Errorf("signed warm fetches = %d, want 1", got)
	}
}
