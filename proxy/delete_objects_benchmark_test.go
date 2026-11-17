package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

type deleteObjectsBenchmarkWriter struct {
	header http.Header
	status int
	bytes  int
}

func (w *deleteObjectsBenchmarkWriter) Header() http.Header { return w.header }
func (w *deleteObjectsBenchmarkWriter) WriteHeader(status int) {
	w.status = status
}
func (w *deleteObjectsBenchmarkWriter) Write(p []byte) (int, error) {
	w.bytes += len(p)
	return len(p), nil
}

// BenchmarkHandleDeleteObjectsCacheDisabled measures complete bulk-delete
// forwarding to a local HTTP origin, without retaining the downstream body.
// The generated keys are 128 bytes long; these sizes are not a traffic model.
func BenchmarkHandleDeleteObjectsCacheDisabled(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	for _, entries := range []int{1, 1000} {
		b.Run(strconv.Itoa(entries), func(b *testing.B) {
			var request, response strings.Builder
			request.WriteString("<Delete>")
			response.WriteString("<DeleteResult>")
			for i := 0; i < entries; i++ {
				key := fmt.Sprintf("%0128d", i)
				fmt.Fprintf(&request, "<Object><Key>%s</Key></Object>", key)
				fmt.Fprintf(&response, "<Deleted><Key>%s</Key></Deleted>", key)
			}
			request.WriteString("</Delete>")
			response.WriteString("</DeleteResult>")
			requestBody, responseBody := request.String(), response.String()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					http.Error(w, "request read failed", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/xml")
				w.Header().Set("Content-Length", strconv.Itoa(len(responseBody)))
				_, _ = io.WriteString(w, responseBody)
			}))
			b.Cleanup(upstream.Close)
			cfg := config.NewDefault()
			cfg.Upstream.Endpoint = upstream.URL
			enabled := false
			cfg.Cache.Enabled = &enabled
			forwarder := NewForwarder(nil, upstream.URL, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost,
				auth.NewProxySigner(transparentForwarderBenchmarkAccessKey, "benchmark-secret-key"), nil)
			b.Cleanup(forwarder.(*transparentForwarder).httpClient.CloseIdleConnections)
			service := NewService(forwarder, cache.NewDisabledCache(), cfg)
			writer := deleteObjectsBenchmarkWriter{header: make(http.Header)}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				r, err := http.NewRequest(http.MethodPost, "http://localhost/benchmark-bucket?delete", strings.NewReader(requestBody))
				if err != nil {
					b.Fatal(err)
				}
				r.Header.Set("Authorization", transparentForwarderBenchmarkAuth)
				r.Header.Set("Content-Type", "application/xml")
				writer.status, writer.bytes = 0, 0
				if err := service.HandleDeleteObjects(&writer, r); err != nil {
					b.Fatal(err)
				}
				if writer.status != http.StatusOK || writer.bytes != len(responseBody) || writer.header.Get("Content-Type") != "application/xml" {
					b.Fatalf("response = %d, %d bytes, %q; want 200, %d bytes, application/xml", writer.status, writer.bytes, writer.header.Get("Content-Type"), len(responseBody))
				}
			}
		})
	}
}
