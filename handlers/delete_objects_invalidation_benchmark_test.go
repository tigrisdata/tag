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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/embedded"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

// BenchmarkBulkDeleteInvalidation measures complete HTTP bulk deletes with live
// cached metadata. Remote runs send all cache operations to one disk-backed owner
// through a two-node embedded ring over loopback gRPC, including streaming
// tombstones and inter-node authentication; local runs use embedded storage
// directly. There is
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
	client, objectKeys := newBulkDeleteCache(b, remote, keys, cfg.Cache.TTL)
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	var request strings.Builder
	request.WriteString("<Delete>")
	for _, key := range objectKeys {
		fmt.Fprintf(&request, "<Object><Key>%s</Key></Object>", key)
	}
	request.WriteString("</Delete>")
	requestBody := []byte(request.String())
	var response strings.Builder
	response.WriteString("<DeleteResult>")
	for _, key := range objectKeys {
		fmt.Fprintf(&response, "<Deleted><Key>%s</Key></Deleted>", key)
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
		for _, key := range objectKeys {
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

// newBulkDeleteCache uses the same embedded requester, routing and server setup
// as TAG. Selecting keys owned by the peer for both metadata and tombstones
// isolates the remote case without replacing any cache operation.
func newBulkDeleteCache(b *testing.B, remote bool, count int, ttl time.Duration) (*embedded.Client, []string) {
	b.Helper()
	address := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatal(err)
		}
		addr := listener.Addr().String()
		if err := listener.Close(); err != nil {
			b.Fatal(err)
		}
		return addr
	}
	token := auth.DeriveGRPCAuthToken("benchmark", "benchmark-secret")
	newNode := func(name string, seeds []string) (*embedded.Client, string) {
		cfg := &embedded.Config{DiskPath: b.TempDir(), TTL: ttl}
		if remote {
			cfg.NodeID, cfg.ClusterAddr, cfg.GRPCAddr = name, address(), address()
			cfg.AdvertiseAddr, cfg.SeedNodes = cfg.GRPCAddr, seeds
			cfg.Registerer = prometheus.NewRegistry()
			cfg.GRPCServerOptions = auth.GRPCServerOptions(token)
			cfg.GRPCDialOptions = auth.GRPCDialOptions(token)
		}
		client, err := embedded.New(cfg)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = client.Close() })
		if remote {
			if err := client.StartGRPCServer(); err != nil {
				b.Fatal(err)
			}
		}
		return client, cfg.ClusterAddr
	}
	owner, seed := newNode("owner", nil)
	client := owner
	if remote {
		client, _ = newNode("requester", []string{seed})
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, node := range []*embedded.Client{owner, client} {
			if err := node.WaitReady(ctx); err != nil {
				b.Fatal(err)
			}
		}
		for len(owner.Coordinator().GetRing().GetActiveNodes()) != 2 || len(client.Coordinator().GetRing().GetActiveNodes()) != 2 || owner.Coordinator().GetEpoch() != client.Coordinator().GetEpoch() {
			select {
			case <-ctx.Done():
				b.Fatal("two-node cache ring did not converge")
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	objectKeys := make([]string, 0, count)
	for i := 0; len(objectKeys) < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		meta, tomb := cache.MakeMetaKey("benchmark", key), cache.MakeTombstoneKey("benchmark", key)
		if remote && (client.IsLocal(meta) || client.IsLocal(tomb) || !owner.IsLocal(meta) || !owner.IsLocal(tomb)) {
			continue
		}
		objectKeys = append(objectKeys, key)
	}
	return client, objectKeys
}
