package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

func TestHandleDeleteObjects_ConservativeThreshold(t *testing.T) {
	for _, mode := range []string{config.ModeTransparent, config.ModeSigning, config.ModeTiered} {
		for _, threshold := range []int{-1, 0, 1, 2, 3} {
			t.Run(fmt.Sprintf("%s/%d", mode, threshold), func(t *testing.T) {
				var c *cache.Cache
				const response = `<DeleteResult><Deleted><Key>ok</Key></Deleted><Error><Key>failed</Key><Code>AccessDenied</Code></Error></DeleteResult>`
				forwarder := &mockForwarder{captureFunc: func(_ context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
					for _, key := range []string{"ok", "failed"} {
						meta := &cache.CachedObjectMeta{Bucket: "bucket", Key: key, ETag: `"refill"`, ContentLength: 1, StatusCode: 200}
						if err := c.PutWithMeta(context.Background(), "bucket", key, meta, []byte("x"), 60); err != nil {
							t.Fatal(err)
						}
					}
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(200)
					_, _ = w.Write([]byte(response))
					return &ResponseCapture{StatusCode: 200, Body: []byte(response), Complete: true}, nil
				}}
				s, store := newTestService(forwarder, true)
				t.Cleanup(s.Stop)
				c = store
				s.config.Mode = mode
				s.config.Cache.BulkDeleteConservativeThreshold = threshold
				// The repeated successful key must not count toward the distinct-key threshold.
				r := httptest.NewRequest(http.MethodPost, "/bucket?delete", strings.NewReader(`<Delete><Object><Key>ok</Key></Object><Object><Key>ok</Key></Object><Object><Key>failed</Key></Object></Delete>`))
				w := httptest.NewRecorder()
				if err := s.HandleDeleteObjects(w, r); err != nil {
					t.Fatal(err)
				}
				if w.Code != 200 || w.Body.String() != response || w.Header().Get("Content-Type") != "application/xml" {
					t.Fatal("upstream response changed")
				}
				_, found, err := c.GetMeta(context.Background(), "bucket", "failed")
				if err != nil {
					t.Fatal(err)
				}
				conservative := mode != config.ModeTiered && threshold > 0 && threshold <= 2
				if found == conservative {
					t.Fatalf("failed-key refill found=%t, conservative=%t", found, conservative)
				}
			})
		}
	}
}
