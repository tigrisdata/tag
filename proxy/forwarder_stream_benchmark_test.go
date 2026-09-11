package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

const passthroughPacedPause = 5 * time.Millisecond

// BenchmarkPassthroughPacedFirstByte measures a client reading the first byte of
// a bodyless passthrough GET while the upstream pauses after its first fragment.
// The response shape models incremental S3 output: a sub-buffer fragment is
// available immediately, while the rest of the response is not.
func BenchmarkPassthroughPacedFirstByte(b *testing.B) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = w.Write([]byte("first-fragment"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(passthroughPacedPause)
		_, _ = w.Write([]byte("tail"))
	}))
	defer upstream.Close()

	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	forwarder := NewForwarder(
		nil,
		upstream.URL,
		"us-east-1",
		1,
		auth.NewProxySigner("benchmark-access-key", "benchmark-secret-key"),
		nil,
	)
	service := NewService(forwarder, cache.NewDisabledCache(), config.NewDefault())
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := service.HandlePassthrough(w, r); err != nil {
			b.Error(err)
		}
	}))
	defer proxy.Close()

	client := proxy.Client()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		resp, err := client.Get(proxy.URL + "/bucket/key")
		if err != nil {
			b.Fatal(err)
		}
		var firstByte [1]byte
		if _, err := io.ReadFull(resp.Body, firstByte[:]); err != nil {
			resp.Body.Close()
			b.Fatal(err)
		}
		if err := resp.Body.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
