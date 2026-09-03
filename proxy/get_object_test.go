package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

func TestWriteChunksToResponse_ErrorBeforeData(t *testing.T) {
	// When broadcast errors before sending any data, headers should NOT be
	// committed so the caller can write a proper error response.
	b := broadcast.NewBroadcaster(16)
	listener := b.Subscribe()
	b.SetHeaders(http.StatusOK, http.Header{
		"Content-Length": []string{"3"},
		"Content-Type":   []string{"application/octet-stream"},
	})

	// Complete with error before any data chunks
	go func() {
		b.Complete(errors.New("upstream fetch failed"))
	}()

	w := httptest.NewRecorder()
	svc := &Service{}
	err := svc.writeChunksToResponse(context.Background(), w, listener, "MISS")

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "upstream fetch failed" {
		t.Errorf("unexpected error: %v", err)
	}

	// Headers should NOT be committed — status code should still be default (200)
	// and no explicit WriteHeader call should have been made
	if w.Code != http.StatusOK {
		// httptest.ResponseRecorder defaults to 200 if WriteHeader was never called.
		// We verify no body was written, which is the key behavior.
		t.Errorf("unexpected status code: %d", w.Code)
	}
	if w.Body.Len() != 0 {
		t.Errorf("expected empty body, got %d bytes", w.Body.Len())
	}
	// Content-Length should NOT have been set on the response
	if w.Header().Get("Content-Length") != "" {
		t.Error("Content-Length should not be set when error occurs before data")
	}
	if w.Header().Get("X-Cache") != "" {
		t.Error("X-Cache should not be set when error occurs before data")
	}
}

func TestWriteChunksToResponse_NormalDataFlow(t *testing.T) {
	// Normal case: headers committed on first data chunk, body written correctly.
	b := broadcast.NewBroadcaster(16)
	listener := b.Subscribe()
	b.SetHeaders(http.StatusOK, http.Header{
		"Content-Length": []string{"6"},
		"Content-Type":   []string{"text/plain"},
	})

	go func() {
		b.Broadcast([]byte("foo"))
		b.Broadcast([]byte("bar"))
		b.Complete(nil)
	}()

	w := httptest.NewRecorder()
	svc := &Service{}
	err := svc.writeChunksToResponse(context.Background(), w, listener, "HIT")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Body.String(); got != "foobar" {
		t.Errorf("body = %q, want %q", got, "foobar")
	}
	if got := w.Header().Get("Content-Type"); got != "text/plain" {
		t.Errorf("Content-Type = %q, want %q", got, "text/plain")
	}
	if got := w.Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("X-Cache = %q, want %q", got, "HIT")
	}
}

func TestProbeMatchesCachedObject(t *testing.T) {
	meta := &cache.CachedObjectMeta{
		ETag:          `"etag-v1"`,
		ContentLength: 100,
		VersionID:     "version-1",
	}
	response := func(etag, contentRange, versionID string) *http.Response {
		headers := http.Header{}
		headers.Set("ETag", etag)
		headers.Set("Content-Range", contentRange)
		headers.Set("x-amz-version-id", versionID)
		return &http.Response{
			StatusCode: http.StatusPartialContent,
			Header:     headers,
		}
	}

	if !probeMatchesCachedObject(response(`"etag-v1"`, "bytes 0-0/100", "version-1"), meta) {
		t.Error("matching probe was rejected")
	}
	if probeMatchesCachedObject(response(`"etag-v2"`, "bytes 0-0/100", "version-1"), meta) {
		t.Error("ETag mismatch was accepted")
	}
	if probeMatchesCachedObject(response(`W/"etag-v1"`, "bytes 0-0/100", "version-1"), meta) {
		t.Error("weak ETag was accepted as an exact identity match")
	}
	if probeMatchesCachedObject(response(`"etag-v1"`, "bytes 0-0/101", "version-1"), meta) {
		t.Error("size mismatch was accepted")
	}
	if probeMatchesCachedObject(response(`"etag-v1"`, "bytes 0-0/100", "version-2"), meta) {
		t.Error("version mismatch was accepted")
	}
	if probeMatchesCachedObject(response(`"etag-v1"`, "", "version-1"), meta) {
		t.Error("missing Content-Range was accepted")
	}
	if probeMatchesCachedObject(response(`"etag-v1"`, "bytes 0-0/100", ""), meta) {
		t.Error("missing authoritative version was accepted")
	}

	zeroMeta := &cache.CachedObjectMeta{ETag: `"zero"`, ContentLength: 0}
	zeroResponse := &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"Etag": {`"zero"`}},
		ContentLength: 0,
	}
	if !probeMatchesCachedObject(zeroResponse, zeroMeta) {
		t.Error("matching zero-byte object probe was rejected")
	}
}

func TestPresignedCoalescingKeySeparatesCapabilities(t *testing.T) {
	req1 := httptest.NewRequest(http.MethodGet, "/bucket/key?X-Amz-Signature=one", nil)
	req1.Host = "bucket.t3.tigrisfiles.io"
	req2 := httptest.NewRequest(http.MethodGet, "/bucket/key?X-Amz-Signature=two", nil)
	req2.Host = req1.Host
	req3 := httptest.NewRequest(http.MethodGet, req1.URL.RequestURI(), nil)
	req3.Host = "other.t3.tigrisfiles.io"
	req4 := httptest.NewRequest(
		http.MethodGet,
		"/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2F20260720%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20260720T120000Z&X-Amz-Expires=3600&X-Amz-Signature=one&X-Amz-SignedHeaders=accept%3Bhost",
		nil,
	)
	req4.Host = req1.Host
	req4.Header.Set("Accept", "application/zip")
	req5 := req4.Clone(req4.Context())
	req5.Header.Set("Accept", "application/octet-stream")

	if presignedCoalescingKey(req1) == presignedCoalescingKey(req2) {
		t.Error("different signatures share a coalescing key")
	}
	if presignedCoalescingKey(req1) == presignedCoalescingKey(req3) {
		t.Error("different hosts share a coalescing key")
	}
	if presignedCoalescingKey(req4) == presignedCoalescingKey(req5) {
		t.Error("different signed header values share a coalescing key")
	}
}

func TestWriteChunksToResponse_ZeroByteResponse(t *testing.T) {
	// Zero-byte response: no data chunks, complete with nil error.
	// Headers should still be committed after the loop.
	b := broadcast.NewBroadcaster(16)
	listener := b.Subscribe()
	b.SetHeaders(http.StatusOK, http.Header{
		"Content-Length": []string{"0"},
		"Content-Type":   []string{"application/octet-stream"},
	})

	go func() {
		b.Complete(nil)
	}()

	w := httptest.NewRecorder()
	svc := &Service{}
	err := svc.writeChunksToResponse(context.Background(), w, listener, "MISS")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if w.Body.Len() != 0 {
		t.Errorf("expected empty body, got %d bytes", w.Body.Len())
	}
	if got := w.Header().Get("X-Cache"); got != "MISS" {
		t.Errorf("X-Cache = %q, want %q", got, "MISS")
	}
}

func TestWriteChunksToResponse_ErrorAfterPartialData(t *testing.T) {
	// Error after partial data: headers are committed (can't undo), error returned.
	b := broadcast.NewBroadcaster(16)
	listener := b.Subscribe()
	b.SetHeaders(http.StatusOK, http.Header{
		"Content-Length": []string{"6"},
		"Content-Type":   []string{"text/plain"},
	})

	go func() {
		b.Broadcast([]byte("foo"))
		b.Complete(errors.New("upstream connection reset"))
	}()

	w := httptest.NewRecorder()
	svc := &Service{}
	err := svc.writeChunksToResponse(context.Background(), w, listener, "MISS")

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "upstream connection reset" {
		t.Errorf("unexpected error: %v", err)
	}
	// Headers should have been committed because data was written
	if got := w.Header().Get("Content-Type"); got != "text/plain" {
		t.Errorf("Content-Type = %q, want %q", got, "text/plain")
	}
	// Partial body should have been written
	if got := w.Body.String(); got != "foo" {
		t.Errorf("body = %q, want %q", got, "foo")
	}
}

func TestWriteChunksToResponse_WaitForHeadersTimeout(t *testing.T) {
	// If context is canceled before headers arrive, should return context error.
	b := broadcast.NewBroadcaster(16)
	listener := b.Subscribe()
	// Don't call SetHeaders — context will cancel first

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	w := httptest.NewRecorder()
	svc := &Service{}
	err := svc.writeChunksToResponse(ctx, w, listener, "MISS")

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}
