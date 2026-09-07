package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
)

type admissionBenchmarkResponseWriter struct {
	header http.Header
	status int
}

func (w *admissionBenchmarkResponseWriter) Header() http.Header {
	return w.header
}

func (w *admissionBenchmarkResponseWriter) WriteHeader(status int) {
	w.status = status
}

func (w *admissionBenchmarkResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return len(p), nil
}

// BenchmarkAdmissionShed measures the saturated admission path for each route
// shape that can be rejected before a handler runs. The request and writer are
// reused so the benchmark focuses on routing, admission, and SlowDown response
// work rather than request parsing or test-writer allocation.
func BenchmarkAdmissionShed(b *testing.B) {
	previousLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(previousLogLevel) })
	cases := []struct {
		name   string
		method string
		path   string
	}{
		{name: "GETObject", method: http.MethodGet, path: "/cache-hit-bucket/cache-hit-object"},
		{name: "GETBucket", method: http.MethodGet, path: "/cache-hit-bucket"},
		{name: "GETBucketTrailingSlash", method: http.MethodGet, path: "/cache-hit-bucket/"},
		{name: "POSTMultipartQuery", method: http.MethodPost, path: "/cache-hit-bucket/cache-hit-object?uploadId=upload&partNumber=1"},
		{name: "GETRoot", method: http.MethodGet, path: "/"},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			server := NewServer(nil, "127.0.0.1", 0, false, 1)
			server.admissionSem <- struct{}{}
			handler := server.Router()
			req := httptest.NewRequest(tc.method, "http://localhost"+tc.path, nil)
			w := &admissionBenchmarkResponseWriter{header: make(http.Header)}

			b.ReportAllocs()
			for b.Loop() {
				w.status = 0
				handler.ServeHTTP(w, req)
			}
			if w.status != http.StatusServiceUnavailable {
				b.Fatalf("status = %d, want %d", w.status, http.StatusServiceUnavailable)
			}
		})
	}
}
