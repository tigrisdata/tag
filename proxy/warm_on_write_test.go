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

// warmObjectResponder returns a DoFullObjectRequest hook that serves a small,
// cacheable object and counts how many times it was called (i.e. how many warms
// actually reached upstream).
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

// A successful PUT with warm_on_write enabled triggers a background full-object
// fetch that repopulates the cache, so a following read is a hit.
func TestHandlePutObject_WarmOnWrite_Enabled(t *testing.T) {
	var warms atomic.Int32
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusOK) // upstream PUT succeeds
			return nil
		},
		doFullObjectFunc: warmObjectResponder(&warms, "warm-body"),
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true

	r := httptest.NewRequest(http.MethodPut, "/"+wowBucket+"/"+wowKey, strings.NewReader("new-body"))
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, r); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("object was not warmed into cache after a successful PUT with warm_on_write=true")
	}
	if got := warms.Load(); got != 1 {
		t.Errorf("upstream warm fetches = %d, want 1", got)
	}
}

// With warm_on_write disabled (the default), a successful PUT must not trigger any
// background fetch — behavior is unchanged from cache-on-read only.
func TestHandlePutObject_WarmOnWrite_DisabledByDefault(t *testing.T) {
	var warms atomic.Int32
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusOK)
			return nil
		},
		doFullObjectFunc: warmObjectResponder(&warms, "warm-body"),
	}
	svc, c := newTestService(mock, true) // WarmOnWrite defaults to false

	r := httptest.NewRequest(http.MethodPut, "/"+wowBucket+"/"+wowKey, strings.NewReader("new-body"))
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, r); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	// A negative assertion must exhaust the poll window to be meaningful.
	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("object was warmed despite warm_on_write=false")
	}
	if got := warms.Load(); got != 0 {
		t.Errorf("upstream warm fetches = %d, want 0 (warm_on_write disabled)", got)
	}
}

// A failed PUT (non-2xx) must not warm — there is no new object to cache.
func TestHandlePutObject_WarmOnWrite_SkippedOnFailure(t *testing.T) {
	var warms atomic.Int32
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusForbidden) // upstream rejects the PUT
			return nil
		},
		doFullObjectFunc: warmObjectResponder(&warms, "warm-body"),
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true

	r := httptest.NewRequest(http.MethodPut, "/"+wowBucket+"/"+wowKey, strings.NewReader("new-body"))
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, r); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("object was warmed after a FAILED PUT")
	}
	if got := warms.Load(); got != 0 {
		t.Errorf("upstream warm fetches = %d, want 0 (PUT failed)", got)
	}
}

// Signing mode has no independent read identity (BackgroundFetchCredentials ok=false),
// so a successful write must not warm — TAG must not issue a background read with the
// client's credentials.
func TestHandlePutObject_WarmOnWrite_SkippedInSigningMode(t *testing.T) {
	var warms atomic.Int32
	mock := &mockForwarder{
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusOK)
			return nil
		},
		doFullObjectFunc:    warmObjectResponder(&warms, "warm-body"),
		backgroundCredsFunc: func() (string, string, bool) { return "", "", false }, // signing mode
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true

	r := httptest.NewRequest(http.MethodPut, "/"+wowBucket+"/"+wowKey, strings.NewReader("new-body"))
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, r); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	if metaCached(c, wowBucket, wowKey, 300*time.Millisecond) {
		t.Error("object was warmed in signing mode (no independent read identity)")
	}
	if got := warms.Load(); got != 0 {
		t.Errorf("upstream warm fetches = %d, want 0 (signing mode)", got)
	}
}

// CompleteMultipartUpload is the case a write-through tee cannot serve (TAG never
// sees the assembled body), so warm-on-write is the only way to make it hot.
func TestHandleCompleteMultipartUpload_WarmOnWrite(t *testing.T) {
	var warms atomic.Int32
	successBody := []byte(`<CompleteMultipartUploadResult><ETag>"mp-etag"</ETag></CompleteMultipartUploadResult>`)
	mock := &mockForwarder{
		captureFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) (*ResponseCapture, error) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(successBody)
			return &ResponseCapture{StatusCode: http.StatusOK, Headers: http.Header{}, Body: successBody, Complete: true}, nil
		},
		doFullObjectFunc: warmObjectResponder(&warms, "assembled-object"),
	}
	svc, c := newTestService(mock, true)
	svc.config.Cache.WarmOnWrite = true

	r := httptest.NewRequest(http.MethodPost, "/"+wowBucket+"/"+wowKey+"?uploadId=u1", nil)
	w := httptest.NewRecorder()
	if err := svc.HandleCompleteMultipartUpload(w, r); err != nil {
		t.Fatalf("HandleCompleteMultipartUpload: %v", err)
	}

	if !metaCached(c, wowBucket, wowKey, 2*time.Second) {
		t.Fatal("multipart-completed object was not warmed into cache")
	}
	if got := warms.Load(); got != 1 {
		t.Errorf("upstream warm fetches = %d, want 1", got)
	}
}
