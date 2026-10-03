package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type responseTrackerDeadlineWriter struct {
	*httptest.ResponseRecorder
	readDeadline time.Time
}

func (w *responseTrackerDeadlineWriter) SetReadDeadline(deadline time.Time) error {
	w.readDeadline = deadline
	return nil
}

func TestResponseTrackerUnwrapsReadDeadline(t *testing.T) {
	writer := &responseTrackerDeadlineWriter{ResponseRecorder: httptest.NewRecorder()}
	deadline := time.Now().Add(time.Second)
	controller := http.NewResponseController(&responseTracker{ResponseWriter: writer})
	if err := controller.SetReadDeadline(deadline); err != nil {
		t.Fatalf("set request read deadline through responseTracker: %v", err)
	}
	if !writer.readDeadline.Equal(deadline) {
		t.Fatalf("read deadline = %v, want %v", writer.readDeadline, deadline)
	}
}
