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
// never shed, even when the admission limit is fully occupied. pprof paths are
// only exempt when pprof is enabled.
func TestAdmissionMiddleware_ExemptPathsBypass(t *testing.T) {
	s := &Server{admissionSem: make(chan struct{}, 1), pprofEnabled: true}
	s.admissionSem <- struct{}{} // fully occupied

	for _, p := range []string{"/health", "/metrics", "/debug/pprof/", "/debug/pprof/heap"} {
		called := false
		h := s.admissionMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, p, nil))
		if !called {
			t.Fatalf("exempt path %s must bypass admission even when full", p)
		}
	}
}

// TestAdmissionMiddleware_DebugBucketNotExempt guards against the exemption
// being bypassed by S3 objects: path-style URLs are /{bucket}/{key}, so an
// object in a bucket named "debug" (/debug/my-key) must still be subject to
// admission and shed with 503 when full. It must also not be exempt as a pprof
// path when pprof is disabled.
func TestAdmissionMiddleware_DebugBucketNotExempt(t *testing.T) {
	for _, pprofEnabled := range []bool{false, true} {
		s := &Server{admissionSem: make(chan struct{}, 1), pprofEnabled: pprofEnabled}
		s.admissionSem <- struct{}{} // fully occupied

		rec := httptest.NewRecorder()
		h := s.admissionMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Fatalf("object in bucket 'debug' must not bypass admission (pprofEnabled=%v)", pprofEnabled)
		}))
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/my-key", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("bucket 'debug' object should be shed (503) when full, got %d (pprofEnabled=%v)", rec.Code, pprofEnabled)
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
