package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/ocache/embedded"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
	"google.golang.org/grpc"
)

// BenchmarkBulkDeleteInvalidation measures complete HTTP bulk deletes with live
// cached metadata. Remote runs send all cache operations to one disk-backed owner
// over loopback gRPC; local runs use the same embedded storage directly. There is
// no injected cache or origin delay. This isolates owner contention, not a WAN RTT.
func BenchmarkBulkDeleteInvalidation(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })
	for _, remote := range []bool{false, true} {
		for _, legacy := range []bool{true, false} {
			for _, keys := range []int{1, 64, 1000} {
				name := fmt.Sprintf("remote=%t/legacy=%t/keys=%d", remote, legacy, keys)
				b.Run(name, func(b *testing.B) { benchmarkBulkDeleteInvalidation(b, remote, legacy, keys) })
			}
		}
	}
}

func benchmarkBulkDeleteInvalidation(b *testing.B, remote, legacy bool, keys int) {
	cfg := config.NewDefault()
	cfg.Cache.SetLegacyCoordination(legacy)
	owner, err := embedded.New(&embedded.Config{DiskPath: b.TempDir(), TTL: cfg.Cache.TTL})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = owner.Close() })
	var client cacheclient.CacheClient = owner
	if remote {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatal(err)
		}
		server := grpc.NewServer()
		pb.RegisterCacheServiceServer(server, owner.Service())
		go func() { _ = server.Serve(listener) }()
		b.Cleanup(server.Stop)
		peer, err := cacheclient.NewSimpleClient(&cacheclient.ClientConfig{Addrs: []string{listener.Addr().String()}, Mode: cacheclient.ModeSimple, ConnectionPoolSize: 1})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = peer.Close() })
		client = peer
	}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	var request strings.Builder
	request.WriteString("<Delete>")
	for i := 0; i < keys; i++ {
		fmt.Fprintf(&request, "<Object><Key>key-%d</Key></Object>", i)
	}
	request.WriteString("</Delete>")
	requestBody := []byte(request.String())
	var response strings.Builder
	response.WriteString("<DeleteResult>")
	for i := 0; i < keys; i++ {
		fmt.Fprintf(&response, "<Deleted><Key>key-%d</Key></Deleted>", i)
	}
	response.WriteString("</DeleteResult>")
	result := []byte(response.String())
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, requestBody) {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("Content-Length", strconv.Itoa(len(result)))
		_, _ = w.Write(result)
	}))
	b.Cleanup(origin.Close)
	forwarder := proxy.NewForwarder(nil, origin.URL, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost, auth.NewProxySigner("benchmark", "benchmark-secret"), nil)
	service := proxy.NewService(forwarder, store, cfg)
	b.Cleanup(service.Stop)
	gateway := httptest.NewServer(NewServer(service, "127.0.0.1", 0, false, cfg.Server.MaxInflightRequests).Router())
	b.Cleanup(gateway.Close)
	httpClient := gateway.Client()
	b.Cleanup(httpClient.CloseIdleConnections)
	latencies := make([]time.Duration, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		// Restore the live metadata ordinary deletes invalidate. Do not bypass
		// the coordinator: seeding uses its normal unconditional publication.
		for i := 0; i < keys; i++ {
			key := fmt.Sprintf("key-%d", i)
			meta := &cache.CachedObjectMeta{Bucket: "benchmark", Key: key, ETag: `"old"`, StatusCode: http.StatusOK}
			if err := store.PutWithMeta(context.Background(), "benchmark", key, meta, nil, 60); err != nil {
				b.Fatal(err)
			}
		}
		r, err := http.NewRequest(http.MethodPost, gateway.URL+"/benchmark?delete", bytes.NewReader(requestBody))
		if err != nil {
			b.Fatal(err)
		}
		r.Header.Set("Authorization", bulkDeleteFenceAuthorization)
		r.Header.Set("Content-Type", "application/xml")
		b.StartTimer()
		start := time.Now()
		response, err := httpClient.Do(r)
		if err != nil {
			b.Fatal(err)
		}
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		latency := time.Since(start)
		b.StopTimer()
		if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK || !bytes.Equal(body, result) {
			b.Fatalf("bulk delete: status=%d body=%q read=%v close=%v", response.StatusCode, body, readErr, closeErr)
		}
		latencies = append(latencies, latency)
		b.StartTimer()
	}
	b.StopTimer()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	b.ReportMetric(float64(latencies[(len(latencies)-1)*50/100]), "p50-ns/op")
	b.ReportMetric(float64(latencies[(len(latencies)-1)*95/100]), "p95-ns/op")
}
