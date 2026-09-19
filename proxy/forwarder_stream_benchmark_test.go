package proxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

const passthroughPacedPause = 5 * time.Millisecond

type passthroughResponseStats struct {
	flushes atomic.Int64
	bytes   atomic.Int64
}

type countingResponseWriter struct {
	http.ResponseWriter
	stats *passthroughResponseStats
}

func (w *countingResponseWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.stats.bytes.Add(int64(n))
	return n, err
}

func (w *countingResponseWriter) Flush() {
	w.stats.flushes.Add(1)
	w.ResponseWriter.(http.Flusher).Flush()
}

func newCountingPassthroughServer(service *Service, stats *passthroughResponseStats) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := service.HandlePassthrough(&countingResponseWriter{ResponseWriter: w, stats: stats}, r); err != nil {
			log.Error().Err(err).Msg("passthrough benchmark request failed")
		}
	}))
}

func reportPassthroughResponseMetrics(b *testing.B, stats *passthroughResponseStats) {
	b.StopTimer()
	b.ReportMetric(float64(stats.flushes.Load())/float64(b.N), "flushes/op")
	b.ReportMetric(float64(stats.bytes.Load())/float64(b.N), "bytes/op")
}

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

// BenchmarkPassthroughStreamingBody measures a complete 32 MiB unknown-length
// passthrough response while the upstream emits 8 KiB chunks without a pause.
func BenchmarkPassthroughStreamingBody(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 32*1024*1024)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
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
	stats := &passthroughResponseStats{}
	proxy := newCountingPassthroughServer(service, stats)
	defer proxy.Close()

	client := proxy.Client()
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
	reportPassthroughResponseMetrics(b, stats)
}

// BenchmarkPassthroughPacedHeaders measures time to response headers while the
// upstream flushes its headers and pauses before its first body fragment.
func BenchmarkPassthroughPacedHeaders(b *testing.B) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(passthroughPacedPause)
		_, _ = w.Write([]byte("first-fragment"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
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
	stats := &passthroughResponseStats{}
	proxy := newCountingPassthroughServer(service, stats)
	defer proxy.Close()

	client := proxy.Client()
	b.ResetTimer()
	for b.Loop() {
		resp, err := client.Get(proxy.URL + "/bucket/key")
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			resp.Body.Close()
			b.Fatal(err)
		}
		if err := resp.Body.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	reportPassthroughResponseMetrics(b, stats)
}

// BenchmarkPassthroughPacedFirstByte measures a client reading the first byte of
// a request-without-body passthrough GET while the upstream pauses after its
// first fragment. The response shape models incremental S3 output: a sub-buffer
// fragment is available immediately, while the rest of the response is not.
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
	stats := &passthroughResponseStats{}
	proxy := newCountingPassthroughServer(service, stats)
	defer proxy.Close()

	client := proxy.Client()
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
		b.StopTimer()
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			resp.Body.Close()
			b.Fatal(err)
		}
		if err := resp.Body.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	reportPassthroughResponseMetrics(b, stats)
}
