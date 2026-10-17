package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

type lateFallbackTriggerGauge struct {
	prometheus.Gauge
	broadcaster *broadcast.Broadcaster
	once        sync.Once
}

func (g *lateFallbackTriggerGauge) Set(value float64) {
	g.once.Do(func() {
		g.broadcaster.Broadcast([]byte("first upstream chunk"))
	})
	g.Gauge.Set(value)
}

// BenchmarkHandleGetObjectLateSubscriberFallback measures one full GET after
// its original broadcaster starts streaming between GetOrCreate and Subscribe.
// It uses the existing active-broadcast metric call as a deterministic schedule
// seam; the original listener and headers are prepared outside timing.
func BenchmarkHandleGetObjectLateSubscriberFallback(b *testing.B) {
	const (
		bucket = "late-benchmark-bucket"
		body   = "late fallback response body"
		etag   = `"late-fallback-etag"`
	)

	forwarder := &mockForwarder{
		doRequestFunc: func(_ context.Context, _ *http.Request, _, _ string) (*http.Response, error) {
			response := cacheableGetResponse(body, etag)
			response.Header.Set("X-Upstream-ID", "late")
			return response, nil
		},
	}
	service, _ := newTestService(forwarder, false)
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		b.StopTimer()
		key := strconv.Itoa(i)
		broadcastKey := makeBroadcastKey(bucket, key, "")
		original, isFirst := service.broadcastManager.GetOrCreate(broadcastKey)
		if !isFirst {
			b.Fatalf("original broadcaster for %q was not created", key)
		}
		ownerListener := original.Subscribe()
		if ownerListener == nil {
			b.Fatal("original broadcaster did not accept its owner listener")
		}
		original.SetHeaders(http.StatusOK, http.Header{
			"Content-Type":  []string{"text/plain"},
			"X-Upstream-ID": []string{"original"},
		})

		previousGauge := metrics.ActiveBroadcasts
		trigger := &lateFallbackTriggerGauge{
			Gauge:       previousGauge,
			broadcaster: original,
		}
		metrics.ActiveBroadcasts = trigger
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)

		b.StartTimer()
		err := service.HandleGetObject(response, request)
		b.StopTimer()

		metrics.ActiveBroadcasts = previousGauge
		original.Complete(nil)
		for chunk := range ownerListener.Chunks() {
			chunk.Release()
		}
		service.broadcastManager.Remove(broadcastKey)
		metrics.SetActiveBroadcasts(service.broadcastManager.ActiveCount())

		if err != nil {
			b.Fatalf("HandleGetObject: %v", err)
		}
		if !original.IsStreaming() {
			b.Fatal("benchmark did not force the late-subscriber fallback")
		}
		if response.Code != http.StatusOK || response.Body.String() != body ||
			response.Header().Get("ETag") != etag || response.Header().Get("X-Upstream-ID") != "late" {
			b.Fatalf("response = status %d, ID %q, ETag %q, body %q", response.Code,
				response.Header().Get("X-Upstream-ID"), response.Header().Get("ETag"), response.Body.String())
		}

		b.StartTimer()
	}
}
