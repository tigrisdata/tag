package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

// buildAdmissionTestRouter builds a router mirroring a representative subset of
// the real route registration, with the admission middleware applied. Routing
// through real mux matching is what populates the {bucket} route variable that
// admission exemption keys off — so these tests exercise the exemption exactly
// as it behaves in production (rather than string-matching raw paths).
func buildAdmissionTestRouter(s *Server, sentinel http.HandlerFunc) http.Handler {
	r := mux.NewRouter()
	r.SkipClean(true)
	r.Use(s.admissionMiddleware)

	r.HandleFunc("/health", sentinel).Methods("GET")
	r.Handle("/metrics", sentinel).Methods("GET")
	if s.pprofEnabled {
		r.HandleFunc("/debug/pprof/", sentinel)
		r.HandleFunc("/debug/pprof/heap", sentinel)
	}
	r.HandleFunc("/{bucket}/{object:.+}", sentinel).Methods("GET", "HEAD", "PUT", "DELETE")
	r.HandleFunc("/{bucket}", sentinel).Methods("GET")
	return r
}

// TestAdmission_ShedsS3RequestWhenFull verifies an S3 object request is shed
// with 503 SlowDown once the in-flight limit is reached.
func TestAdmission_ShedsS3RequestWhenFull(t *testing.T) {
	s := &Server{admissionSem: make(chan struct{}, 1)}
	s.admissionSem <- struct{}{} // occupy the only slot

	router := buildAdmissionTestRouter(s, func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("S3 request must be shed, not served, when admission is full")
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/my-bucket/my-key", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 (SlowDown), got %d", rec.Code)
	}
}

// TestAdmission_ExemptOperationalEndpoints verifies /health, /metrics, and the
// pprof endpoints are never shed, even when the limit is fully occupied.
func TestAdmission_ExemptOperationalEndpoints(t *testing.T) {
	s := &Server{admissionSem: make(chan struct{}, 1), pprofEnabled: true}
	s.admissionSem <- struct{}{} // fully occupied

	served := 0
	router := buildAdmissionTestRouter(s, func(w http.ResponseWriter, r *http.Request) { served++ })

	for _, p := range []string{"/health", "/metrics", "/debug/pprof/", "/debug/pprof/heap"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Code == http.StatusServiceUnavailable {
			t.Fatalf("operational endpoint %s must not be shed", p)
		}
	}
	if served != 4 {
		t.Fatalf("expected 4 operational requests served, got %d", served)
	}
}

// TestAdmission_PprofLikeS3KeyNotExempt is the regression test for the pprof
// prefix bypass: with pprof enabled, an object key that looks like a pprof path
// (bucket "debug", key "pprof/large-object") does not match the exact pprof
// routes, so mux routes it to the object handler with a {bucket} var — it must
// be subject to admission, not exempt.
func TestAdmission_PprofLikeS3KeyNotExempt(t *testing.T) {
	s := &Server{admissionSem: make(chan struct{}, 1), pprofEnabled: true}
	s.admissionSem <- struct{}{} // fully occupied

	router := buildAdmissionTestRouter(s, func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("an S3 object with a pprof/-like key must be subject to admission")
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/large-object", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for S3 object /debug/pprof/large-object, got %d", rec.Code)
	}
}

// TestAdmission_NilSemUnlimited verifies a nil semaphore disables admission.
func TestAdmission_NilSemUnlimited(t *testing.T) {
	s := &Server{} // admissionSem nil = unlimited
	served := 0
	router := buildAdmissionTestRouter(s, func(w http.ResponseWriter, r *http.Request) { served++ })
	for i := 0; i < 5; i++ {
		router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/b/k", nil))
	}
	if served != 5 {
		t.Fatalf("nil admissionSem must not limit; got %d served", served)
	}
}
