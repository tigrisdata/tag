package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/tigrisdata/ocache/embedded"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/handlers"
	"github.com/tigrisdata/tag/proxy"
)

const (
	headCacheBenchmarkBucket                       = "benchmark-bucket"
	headCacheBenchmarkKey                          = "metadata-heavy-object"
	headCacheBenchmarkAccessKey                    = "head-cache-benchmark-access"
	headCacheBenchmarkSecretKey                    = "head-cache-benchmark-secret"
	headCacheBenchmarkLowLocalityKeys              = 98304
	headCacheBenchmarkTwoHitKeys                   = 98304
	headCacheBenchmarkAdmissionChurnKeys           = 24576
	headCacheBenchmarkResidentWorkingSetKeys       = 4096
	headCacheBenchmarkMixedHotKeys                 = 128
	headCacheBenchmarkMixedColdKeys                = 8192
	headCacheBenchmarkMetadataFields               = 16
	headCacheBenchmarkStartupAttempts              = 3
	headCacheBenchmarkMetadataValueLen             = 96
	headCacheBenchmarkResidentMetadataEncodedBytes = 16 * 1024
	headCacheBenchmarkOversizedMetadataValueLen    = 20 * 1024
)

type headCacheBenchmarkFixture struct {
	client      *http.Client
	requests    []*http.Request
	store       *cache.Cache
	writerStore *cache.Cache
}

// benchmarkListenAddrs reserves distinct loopback ports long enough to obtain
// the embedded cache's coordinator and gRPC addresses. Keeping both reservations
// open prevents the kernel from assigning the just-released coordinator port to
// the gRPC listener in the same fixture setup.
func benchmarkListenAddrs(tb testing.TB) (clusterAddr, grpcAddr string) {
	tb.Helper()

	clusterListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("reserve coordinator loopback port: %v", err)
	}
	defer func() {
		if err := clusterListener.Close(); err != nil {
			tb.Fatalf("release coordinator loopback port: %v", err)
		}
	}()

	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("reserve gRPC loopback port: %v", err)
	}
	defer func() {
		if err := grpcListener.Close(); err != nil {
			tb.Fatalf("release gRPC loopback port: %v", err)
		}
	}()

	return clusterListener.Addr().String(), grpcListener.Addr().String()
}

func newHeadCacheBenchmarkBaseMeta(key string) *cache.CachedObjectMeta {
	return &cache.CachedObjectMeta{
		Bucket:        headCacheBenchmarkBucket,
		Key:           key,
		ETag:          `"head-cache-hit"`,
		ContentType:   "application/octet-stream",
		ContentLength: 4096,
		StatusCode:    http.StatusOK,
		CachedAt:      time.Now().Unix(),
	}
}

func newHeadCacheBenchmarkMeta(key string) *cache.CachedObjectMeta {
	meta := newHeadCacheBenchmarkBaseMeta(key)
	meta.UserMetadata = make(map[string]string, headCacheBenchmarkMetadataFields)
	metadataValue := strings.Repeat("m", headCacheBenchmarkMetadataValueLen)
	for i := 0; i < headCacheBenchmarkMetadataFields; i++ {
		meta.UserMetadata[fmt.Sprintf("x-amz-meta-field-%02d", i)] = metadataValue
	}
	return meta
}

func newHeadCacheBenchmarkMetadataLightMeta(key string) *cache.CachedObjectMeta {
	return newHeadCacheBenchmarkBaseMeta(key)
}

func newHeadCacheBenchmarkResidentAtCapMeta(key string) *cache.CachedObjectMeta {
	meta := newHeadCacheBenchmarkBaseMeta(key)
	const metadataKey = "x-amz-meta-at-cap"
	meta.UserMetadata = map[string]string{metadataKey: ""}
	encoded, err := meta.Encode()
	if err != nil {
		panic(fmt.Sprintf("encode at-cap benchmark metadata: %v", err))
	}
	metadataValueLen := headCacheBenchmarkResidentMetadataEncodedBytes - len(encoded)
	if metadataValueLen < 0 {
		panic("at-cap benchmark metadata base exceeds target size")
	}
	meta.UserMetadata[metadataKey] = strings.Repeat("m", metadataValueLen)
	return meta
}

func newHeadCacheBenchmarkOversizedMeta(key string) *cache.CachedObjectMeta {
	meta := newHeadCacheBenchmarkBaseMeta(key)
	meta.UserMetadata = map[string]string{
		"x-amz-meta-large": strings.Repeat("m", headCacheBenchmarkOversizedMetadataValueLen),
	}
	return meta
}

func headCacheBenchmarkObjectKey(index, keyCount int) string {
	if keyCount == 1 {
		return headCacheBenchmarkKey
	}
	return fmt.Sprintf("%s-%05d", headCacheBenchmarkKey, index)
}

// newHeadCacheBenchmarkFixture builds cached HEAD workloads through the public
// handler route. It uses a one-node embedded cache with the same node-ID
// configuration as the standalone deployment, not an in-memory client.
func newHeadCacheBenchmarkFixture(tb testing.TB, keyCount int) *headCacheBenchmarkFixture {
	return newHeadCacheBenchmarkFixtureWithMeta(tb, keyCount, newHeadCacheBenchmarkMeta)
}

// waitForHeadCacheBenchmarkWriteReady waits for the local replica to become
// healthy in the ring. WaitReady reaches ACTIVE before the first heartbeat can
// make that replica eligible for a write, so a fixture can otherwise fail while
// populating its untimed input.
func waitForHeadCacheBenchmarkWriteReady(embeddedCache *embedded.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const readyKey = "head-cache-benchmark-ready"
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		if err := embeddedCache.Put(ctx, readyKey, []byte("ready"), 1); err == nil {
			return nil
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for first embedded-cache write: %w", lastErr)
		case <-ticker.C:
		}
	}
}

// newHeadCacheBenchmarkEmbeddedCache retries only startup work that can lose a
// just-released loopback port to another benchmark process. A successful attempt
// is the sole cache used by the timed benchmark; retries never repeat a sample.
func newHeadCacheBenchmarkEmbeddedCache(tb testing.TB, cfg *config.Config) *embedded.Client {
	tb.Helper()

	var lastErr error
	for attempt := 1; attempt <= headCacheBenchmarkStartupAttempts; attempt++ {
		cfg.Cache.DiskPath = tb.TempDir()
		cfg.Cache.ClusterAddr, cfg.Cache.GRPCAddr = benchmarkListenAddrs(tb)
		embeddedCache, err := embedded.New(&embedded.Config{
			DiskPath:      cfg.Cache.DiskPath,
			TTL:           cfg.Cache.TTL,
			MaxDiskUsage:  cfg.Cache.MaxDiskUsageBytes,
			NodeID:        cfg.Cache.NodeID,
			ClusterAddr:   cfg.Cache.ClusterAddr,
			GRPCAddr:      cfg.Cache.GRPCAddr,
			AdvertiseAddr: cfg.Cache.AdvertiseAddr,
			SeedNodes:     cfg.Cache.SeedNodes,
			// Go's benchmark calibration can construct this fixture more than once.
			// Keep each embedded cache's collectors off the process-wide registry.
			Registerer: prometheus.NewRegistry(),
		})
		if err != nil {
			lastErr = fmt.Errorf("create embedded cache: %w", err)
			continue
		}
		if err := embeddedCache.StartGRPCServer(); err != nil {
			_ = embeddedCache.Close()
			lastErr = fmt.Errorf("start embedded cache: %w", err)
			continue
		}
		readyCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = embeddedCache.WaitReady(readyCtx)
		cancel()
		if err != nil {
			_ = embeddedCache.Close()
			lastErr = fmt.Errorf("wait for embedded cache: %w", err)
			continue
		}
		if err := waitForHeadCacheBenchmarkWriteReady(embeddedCache); err != nil {
			_ = embeddedCache.Close()
			lastErr = err
			continue
		}
		tb.Cleanup(func() { _ = embeddedCache.Close() })
		return embeddedCache
	}
	tb.Fatalf("start embedded cache after %d attempts: %v", headCacheBenchmarkStartupAttempts, lastErr)
	return nil
}

func newHeadCacheBenchmarkFixtureWithMeta(tb testing.TB, keyCount int, newMeta func(string) *cache.CachedObjectMeta) *headCacheBenchmarkFixture {
	tb.Helper()
	if keyCount < 1 {
		tb.Fatal("head-cache benchmark needs at least one key")
	}

	// The request path logs every cache operation at debug level. Keep that
	// output out of the benchmark's metric stream and measured work.
	previousLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	tb.Cleanup(func() {
		zerolog.SetGlobalLevel(previousLogLevel)
	})

	cfg := config.NewDefault()
	cfg.Cache.NodeID = "head-cache-benchmark"
	embeddedCache := newHeadCacheBenchmarkEmbeddedCache(tb, cfg)

	// Populate through a separate cache facade so the service's tier starts cold,
	// as it does after a process restart or when another gateway populated ocache.
	writerStore := cache.NewCacheWithClient(newEmbeddedBlockCacheClient(embeddedCache), &cfg.Cache)
	store := cache.NewCacheWithClient(newEmbeddedBlockCacheClient(embeddedCache), &cfg.Cache)
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(headCacheBenchmarkAccessKey, headCacheBenchmarkSecretKey)
	forwarder := proxy.NewForwarder(
		credentials,
		"http://127.0.0.1:1",
		cfg.Upstream.Region,
		cfg.Upstream.MaxIdleConnsPerHost,
		nil,
		nil,
	)
	service := proxy.NewService(forwarder, store, cfg)
	gateway := httptest.NewServer(handlers.NewServer(service, "127.0.0.1", 0, false, cfg.Server.MaxInflightRequests).Router())
	tb.Cleanup(gateway.Close)

	signer := auth.NewRequestSigner(gateway.URL, cfg.Upstream.Region)
	requests := make([]*http.Request, 0, keyCount)
	for index := range keyCount {
		meta := newMeta(headCacheBenchmarkObjectKey(index, keyCount))
		if err := writerStore.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, []byte("body"), 60); err != nil {
			tb.Fatalf("populate embedded cache: %v", err)
		}

		request, err := signer.SignRequest(
			context.Background(),
			http.MethodHead,
			"/"+meta.Bucket+"/"+meta.Key,
			nil,
			"",
			headCacheBenchmarkAccessKey,
			headCacheBenchmarkSecretKey,
			http.Header{},
		)
		if err != nil {
			tb.Fatalf("sign HEAD request: %v", err)
		}
		requests = append(requests, request)
	}

	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 16,
	}}
	tb.Cleanup(client.CloseIdleConnections)

	return &headCacheBenchmarkFixture{
		client:      client,
		requests:    requests,
		store:       store,
		writerStore: writerStore,
	}
}

func (f *headCacheBenchmarkFixture) head(index int) error {
	request := f.requests[index%len(f.requests)]
	response, err := f.client.Do(request.Clone(context.Background()))
	if err != nil {
		return err
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("HEAD status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	if got := response.Header.Get(proxy.XCacheHeader); got != proxy.XCacheHit {
		return fmt.Errorf("HEAD X-Cache = %q, want %q", got, proxy.XCacheHit)
	}
	return nil
}

// BenchmarkHeadObjectCacheHitEmbeddedSerial measures a repeated same-key
// cached HEAD through handlers.Server and a real one-node embedded ocache
// deployment. Cache population, decoded-tier admission, a resident validation,
// and connection warmup are excluded from the timed loop.
func BenchmarkHeadObjectCacheHitEmbeddedSerial(b *testing.B) {
	fixture := newHeadCacheBenchmarkFixture(b, 1)
	for range 4 {
		if err := fixture.head(0); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := fixture.head(0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetMetaCacheHitEmbedded isolates repeated metadata reads against the
// same real embedded cache used by the public HEAD benchmarks. It retains the
// public GetMeta ownership contract, so resident values are still cloned before
// returning to the caller.
func BenchmarkGetMetaCacheHitEmbedded(b *testing.B) {
	fixture := newHeadCacheBenchmarkFixture(b, 1)
	for range 4 {
		if _, found, err := fixture.store.GetMeta(context.Background(), headCacheBenchmarkBucket, headCacheBenchmarkKey); err != nil || !found {
			b.Fatalf("warm GetMeta = (found=%t, err=%v), want (true, nil)", found, err)
		}
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, found, err := fixture.store.GetMeta(ctx, headCacheBenchmarkBucket, headCacheBenchmarkKey); err != nil || !found {
			b.Fatalf("GetMeta = (found=%t, err=%v), want (true, nil)", found, err)
		}
	}
}

// BenchmarkHeadObjectCacheHitEmbeddedParallel measures the same deployment and
// key under four workers. Each request still goes through the public handler;
// the HTTP client and cache are shared as they are in the running gateway.
func BenchmarkHeadObjectCacheHitEmbeddedParallel(b *testing.B) {
	fixture := newHeadCacheBenchmarkFixture(b, 1)
	for range 4 {
		if err := fixture.head(0); err != nil {
			b.Fatal(err)
		}
	}

	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := fixture.head(0); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkHeadObjectCacheHitEmbeddedMetaShape(b *testing.B, newMeta func(string) *cache.CachedObjectMeta) {
	fixture := newHeadCacheBenchmarkFixtureWithMeta(b, 1, newMeta)
	for range 4 {
		if err := fixture.head(0); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := fixture.head(0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHeadObjectCacheHitEmbeddedMetadataLight bounds the common metadata
// shape with no user metadata, where decoder work is much smaller than the
// metadata-heavy hot-key workload.
func BenchmarkHeadObjectCacheHitEmbeddedMetadataLight(b *testing.B) {
	// Keep this distinct from an allocated-but-empty metadata map: it represents
	// the normal object shape with no user-metadata field in cached JSON.
	if meta := newHeadCacheBenchmarkMetadataLightMeta(headCacheBenchmarkKey); meta.UserMetadata != nil {
		b.Fatal("metadata-light benchmark unexpectedly has a user-metadata map")
	}
	benchmarkHeadObjectCacheHitEmbeddedMetaShape(b, newHeadCacheBenchmarkMetadataLightMeta)
}

// BenchmarkHeadObjectCacheHitEmbeddedResidentAtCapMetadata measures a
// 16 KiB encoded metadata value exactly at the decoded tier's size boundary.
func BenchmarkHeadObjectCacheHitEmbeddedResidentAtCapMetadata(b *testing.B) {
	meta := newHeadCacheBenchmarkResidentAtCapMeta(headCacheBenchmarkKey)
	encoded, err := meta.Encode()
	if err != nil {
		b.Fatal(err)
	}
	if len(encoded) != headCacheBenchmarkResidentMetadataEncodedBytes {
		b.Fatalf("encoded metadata length = %d, want %d", len(encoded), headCacheBenchmarkResidentMetadataEncodedBytes)
	}
	benchmarkHeadObjectCacheHitEmbeddedMetaShape(b, newHeadCacheBenchmarkResidentAtCapMeta)
}

// BenchmarkHeadObjectCacheHitEmbeddedOversizedMetadataBypass bounds metadata
// that is too large for the decoded tier and must retain its normal decode path.
func BenchmarkHeadObjectCacheHitEmbeddedOversizedMetadataBypass(b *testing.B) {
	benchmarkHeadObjectCacheHitEmbeddedMetaShape(b, newHeadCacheBenchmarkOversizedMeta)
}

// BenchmarkHeadObjectCacheHitEmbeddedAdmissionChurn fills the resident tier
// with more admitted keys than it can retain, then admits each timed key again.
// Its larger working set keeps every key below a fourth read during a ten-second
// sample while exercising public HEAD handling, resident eviction, and churn.
func BenchmarkHeadObjectCacheHitEmbeddedAdmissionChurn(b *testing.B) {
	fixture := newHeadCacheBenchmarkFixture(b, headCacheBenchmarkAdmissionChurnKeys)
	for index := range headCacheBenchmarkAdmissionChurnKeys {
		for range 3 {
			if err := fixture.head(index); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; b.Loop(); index++ {
		if index == headCacheBenchmarkAdmissionChurnKeys {
			b.Fatal("admission-churn benchmark exhausted its unique working set")
		}
		for range 3 {
			if err := fixture.head(index); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkHeadObjectCacheHitEmbeddedResidentWorkingSet fills each decoded-tier
// slot through public HEAD handling, then cycles through the exact 4096-key
// resident working set. It bounds collisions in the direct resident filter.
func BenchmarkHeadObjectCacheHitEmbeddedResidentWorkingSet(b *testing.B) {
	fixture := newHeadCacheBenchmarkFixture(b, headCacheBenchmarkResidentWorkingSetKeys)
	for index := range headCacheBenchmarkResidentWorkingSetKeys {
		for range 4 {
			if err := fixture.head(index); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; b.Loop(); index++ {
		if err := fixture.head(index); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHeadObjectCacheHitEmbeddedLowLocality measures cold and low-locality
// cache-eligible HEAD keys through the same public handler route. The working
// set exceeds both the decoded tier and its admission window. It is large
// enough for a ten-second timed sample and explicitly fails rather than
// revisiting a key, so every timed request retains the ordinary backend-read
// and decode behavior.
func BenchmarkHeadObjectCacheHitEmbeddedLowLocality(b *testing.B) {
	fixture := newHeadCacheBenchmarkFixture(b, headCacheBenchmarkLowLocalityKeys)
	if err := fixture.head(0); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 1; b.Loop(); index++ {
		if index == headCacheBenchmarkLowLocalityKeys {
			b.Fatal("low-locality benchmark exhausted its unique working set")
		}
		if err := fixture.head(index); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHeadObjectCacheHitEmbeddedTwoHitWorkingSet measures the ordinary
// burst where a key receives exactly two HEAD reads. Each operation performs
// both reads through the public handler; the working set is larger than a
// normal twenty-second sample so its keys do not reach a third read.
func BenchmarkHeadObjectCacheHitEmbeddedTwoHitWorkingSet(b *testing.B) {
	fixture := newHeadCacheBenchmarkFixture(b, headCacheBenchmarkTwoHitKeys)

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; b.Loop(); index++ {
		if index == headCacheBenchmarkTwoHitKeys {
			b.Fatal("two-hit benchmark exhausted its unique working set")
		}
		for range 2 {
			if err := fixture.head(index); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkHeadObjectCacheHitEmbeddedMixedTwoHitWorkingSet combines a warmed
// hot set with cold two-hit bursts through the ordinary public HEAD route. The
// cold working set is large enough that no cold key is read a third time.
func BenchmarkHeadObjectCacheHitEmbeddedMixedTwoHitWorkingSet(b *testing.B) {
	fixture := newHeadCacheBenchmarkFixture(b, headCacheBenchmarkMixedHotKeys+headCacheBenchmarkMixedColdKeys)
	for hotKey := range headCacheBenchmarkMixedHotKeys {
		for range 4 {
			if err := fixture.head(hotKey); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for coldKey := 0; b.Loop(); coldKey++ {
		if coldKey == headCacheBenchmarkMixedColdKeys {
			b.Fatal("mixed two-hit benchmark exhausted its cold working set")
		}
		if err := fixture.head(coldKey % headCacheBenchmarkMixedHotKeys); err != nil {
			b.Fatal(err)
		}
		for range 2 {
			if err := fixture.head(headCacheBenchmarkMixedHotKeys + coldKey); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkPutWithMetaEmbedded measures production cache population with the
// same metadata shape as the HEAD workloads. Constructing unique requests is
// setup; the timed operation is Cache.PutWithMeta against embedded ocache.
func BenchmarkPutWithMetaEmbedded(b *testing.B) {
	fixture := newHeadCacheBenchmarkFixture(b, 1)
	metas := make([]*cache.CachedObjectMeta, b.N)
	for index := range metas {
		meta := newHeadCacheBenchmarkMeta(fmt.Sprintf("write-%05d", index))
		meta.ETag = fmt.Sprintf(`"head-cache-write-%d"`, index)
		metas[index] = meta
	}

	ctx := context.Background()
	body := []byte("body")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		meta := metas[index]
		if err := fixture.writerStore.PutWithMeta(ctx, meta.Bucket, meta.Key, meta, body, 60); err != nil {
			b.Fatal(err)
		}
	}
}
