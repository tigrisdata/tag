package proxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

const passthroughPacedPause = 5 * time.Millisecond

// BenchmarkPassthroughBufferedBody measures complete body passthrough at two
// response sizes while the upstream emits 8 KiB chunks without a pause.
func BenchmarkPassthroughBufferedBody(b *testing.B) {
	for _, test := range []struct {
		name string
		size int
	}{
		{name: "64KiB", size: 64 * 1024},
		{name: "1MiB", size: 1024 * 1024},
	} {
		b.Run(test.name, func(b *testing.B) {
			payload := bytes.Repeat([]byte("x"), test.size)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
				w.WriteHeader(http.StatusOK)
				for offset := 0; offset < len(payload); offset += 8 * 1024 {
					end := offset + 8*1024
					if end > len(payload) {
						end = len(payload)
					}
					if _, err := w.Write(payload[offset:end]); err != nil {
						return
					}
					w.(http.Flusher).Flush()
				}
			}))
			defer upstream.Close()

			oldLogger := log.Logger
			log.Logger = log.Logger.Level(zerolog.ErrorLevel)
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
				n, err := io.Copy(io.Discard, resp.Body)
				closeErr := resp.Body.Close()
				if err != nil {
					b.Fatal(err)
				}
				if closeErr != nil {
					b.Fatal(closeErr)
				}
				if n != int64(len(payload)) {
					b.Fatalf("body bytes = %d, want %d", n, len(payload))
				}
			}
		})
	}
}

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
	log.Logger = log.Logger.Level(zerolog.ErrorLevel)
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
