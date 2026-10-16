package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Benchmark the handler installed on http.Server; Server.Router bypasses the
// foreground request-lifetime tracking.
func BenchmarkServerHTTPHandler(b *testing.B) {
	server := NewServer(nil, "127.0.0.1", 0, false, 0)
	handler := server.httpServer.Handler
	request := httptest.NewRequest(http.MethodGet, "/health", nil)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		b.Fatalf("health response status = %v, want %d", response.Code, http.StatusOK)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
		}
	})
}
