package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestAdmissionMiddleware_ShedsWhenFull verifies that once the in-flight limit
// is reached, further S3 requests are shed with 503 SlowDown rather than being
// admitted (which is what converts overload into backpressure instead of
// unbounded goroutine/thread growth).
func TestAdmissionMiddleware_ShedsWhenFull(t *testing.T) {
	s := &Server{admissionSem: make(chan struct{}, 1)}

	block := make(chan struct{})
	entered := make(chan struct{})
	h := s.admissionMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{} // signal the slot is held
		<-block               // hold it until released
		w.WriteHeader(http.StatusOK)
	}))

	// First request occupies the only slot.
	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bucket/key", nil))
	<-entered

	// Second request must be shed with 503 SlowDown.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bucket/key2", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 (SlowDown) when admission full, got %d", rec.Code)
	}

	// Releasing the first frees the slot.
	close(block)
}

// TestAdmissionMiddleware_ExemptPathsBypass verifies operational endpoints are
// never shed, even when the admission limit is fully occupied.
func TestAdmissionMiddleware_ExemptPathsBypass(t *testing.T) {
	s := &Server{admissionSem: make(chan struct{}, 1)}
	s.admissionSem <- struct{}{} // fully occupied

	for _, p := range []string{"/health", "/metrics", "/debug/pprof/"} {
		called := false
		h := s.admissionMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, p, nil))
		if !called {
			t.Fatalf("exempt path %s must bypass admission even when full", p)
		}
	}
}

// TestAdmissionMiddleware_NilSemUnlimited verifies a nil semaphore (limit <= 0)
// disables admission control entirely.
func TestAdmissionMiddleware_NilSemUnlimited(t *testing.T) {
	s := &Server{} // admissionSem nil = unlimited
	calls := 0
	h := s.admissionMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++ }))
	for i := 0; i < 5; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/b/k", nil))
	}
	if calls != 5 {
		t.Fatalf("nil admissionSem must not limit; got %d calls", calls)
	}
}
