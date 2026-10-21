package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

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

func TestWriteChunksToResponse_CompletedEmptyBroadcastWithLateListener(t *testing.T) {
	b := broadcast.NewBroadcaster(16)
	headers := http.Header{
		"Content-Length": []string{"0"},
		"Content-Type":   []string{"application/octet-stream"},
	}
	b.SetHeaders(http.StatusOK, headers)
	b.Complete(nil)

	listener := b.Subscribe()
	if listener == nil {
		t.Fatal("Subscribe returned nil after an empty response completed")
	}
	w := httptest.NewRecorder()
	if err := (&Service{}).writeChunksToResponse(context.Background(), w, listener, "MISS"); err != nil {
		t.Fatalf("writeChunksToResponse: %v", err)
	}
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if w.Body.Len() != 0 {
		t.Errorf("body length = %d, want 0", w.Body.Len())
	}
	if got := w.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type = %q, want application/octet-stream", got)
	}
	if got := w.Header().Get("X-Cache"); got != "MISS" {
		t.Errorf("X-Cache = %q, want MISS", got)
	}
}

func TestWriteChunksToResponse_CompletedHeaderErrorWithLateListener(t *testing.T) {
	upstreamErr := errors.New("upstream body read failed")
	b := broadcast.NewBroadcaster(16)
	b.SetHeaders(http.StatusOK, http.Header{
		"Content-Length": []string{"6"},
		"Content-Type":   []string{"text/plain"},
	})
	b.Complete(upstreamErr)

	listener := b.Subscribe()
	if listener == nil {
		t.Fatal("Subscribe returned nil after a completed response")
	}
	w := httptest.NewRecorder()
	err := (&Service{}).writeChunksToResponse(context.Background(), w, listener, "MISS")
	if err != upstreamErr {
		t.Fatalf("writeChunksToResponse error = %v, want original upstream error %v", err, upstreamErr)
	}
	if w.Body.Len() != 0 {
		t.Errorf("body length = %d, want 0 before any body data", w.Body.Len())
	}
	if got := w.Header().Get("Content-Type"); got != "" {
		t.Errorf("Content-Type = %q, want uncommitted headers", got)
	}
	if got := w.Header().Get("X-Cache"); got != "" {
		t.Errorf("X-Cache = %q, want no cache status before error response mapping", got)
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
