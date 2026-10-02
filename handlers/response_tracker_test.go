package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type responseTrackerDeadlineWriter struct {
	http.ResponseWriter
	deadline time.Time
}

func (w *responseTrackerDeadlineWriter) SetReadDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}

func TestResponseTrackerUnwrapsReadDeadline(t *testing.T) {
	writer := &responseTrackerDeadlineWriter{ResponseWriter: httptest.NewRecorder()}
	wrapped := &responseTracker{ResponseWriter: writer}
	deadline := time.Now()
	if err := http.NewResponseController(wrapped).SetReadDeadline(deadline); err != nil {
		t.Fatalf("set read deadline through responseTracker: %v", err)
	}
	if !writer.deadline.Equal(deadline) {
		t.Fatalf("underlying read deadline = %v, want %v", writer.deadline, deadline)
	}
}
