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

// BenchmarkDeleteObjectsRequest measures the cache-disabled handler and real
// transparent HTTP forwarding. The origin consumes the XML and returns a quiet
// result, keeping response size independent of the number of requested keys.
func BenchmarkDeleteObjectsRequest(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })
	for _, keys := range []int{1, 1000} {
		b.Run(strconv.Itoa(keys), func(b *testing.B) {
			var xml strings.Builder
			xml.WriteString("<Delete><Quiet>true</Quiet>")
			for i := 0; i < keys; i++ {
				fmt.Fprintf(&xml, "<Object><Key>%0128d</Key></Object>", i)
			}
			xml.WriteString("</Delete>")
			body := xml.String()
			const result = "<DeleteResult/>"
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n, err := io.Copy(io.Discard, r.Body)
				if err != nil || n != int64(len(body)) {
					http.Error(w, "incomplete request", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/xml")
				w.Header().Set("Content-Length", strconv.Itoa(len(result)))
				_, _ = io.WriteString(w, result)
			}))
			defer origin.Close()
			cfg := config.NewDefault()
			forwarder := NewForwarder(nil, origin.URL, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost,
				auth.NewProxySigner("request-benchmark", "request-benchmark-secret"), nil)
			defer forwarder.(*transparentForwarder).httpClient.CloseIdleConnections()
			service := NewService(forwarder, cache.NewDisabledCache(), cfg)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				// Hide strings.Reader's seek/copy shortcuts, as with an incoming HTTP body.
				r := httptest.NewRequest(http.MethodPost, "/benchmark-bucket?delete", io.NopCloser(struct{ io.Reader }{strings.NewReader(body)}))
				r.ContentLength = int64(len(body))
				r.Header.Set("Content-Type", "application/xml")
				w := httptest.NewRecorder()
				if err := service.HandleDeleteObjects(w, r); err != nil {
					b.Fatal(err)
				}
				if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/xml" || w.Body.String() != result {
					b.Fatalf("unexpected response: %d %v %q", w.Code, w.Header(), w.Body.String())
				}
			}
		})
	}
}
