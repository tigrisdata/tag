package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

type bulkDeleteResponseCompletionBenchmark struct {
	gatewayURL   string
	client       *http.Client
	cacheStore   *cache.Cache
	handlerDone  chan struct{}
	requestBody  []byte
	responseBody []byte
}

func newBulkDeleteResponseCompletionBenchmark(tb testing.TB, keys int) *bulkDeleteResponseCompletionBenchmark {
	tb.Helper()

	var request, response strings.Builder
	request.WriteString("<Delete>")
	response.WriteString("<DeleteResult>")
	for i := 0; i < keys; i++ {
		key := bulkDeleteFenceKey
		if i > 0 {
			key = fmt.Sprintf("object-%04d", i)
		}
		fmt.Fprintf(&request, "<Object><Key>%s</Key></Object>", key)
		fmt.Fprintf(&response, "<Deleted><Key>%s</Key></Deleted>", key)
	}
	request.WriteString("</Delete>")
	if keys == 1 {
		response.WriteString(strings.Repeat(" ", 128*1024))
	}
	response.WriteString("</DeleteResult>")
	fixture := &bulkDeleteResponseCompletionBenchmark{
		handlerDone:  make(chan struct{}, 1),
		requestBody:  []byte(request.String()),
		responseBody: []byte(response.String()),
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !r.URL.Query().Has("delete") {
			http.NotFound(w, r)
			return
		}
		requestBody, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(requestBody, fixture.requestBody) {
			http.Error(w, "unexpected DeleteObjects request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set(bulkDeleteFenceHeader, bulkDeleteFenceHeaderValue)
		w.Header().Set("Content-Length", strconv.Itoa(len(fixture.responseBody)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fixture.responseBody)
	}))
	tb.Cleanup(upstream.Close)

	cfg := config.NewDefault()
	cfg.Upstream.Endpoint = upstream.URL
	cacheStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	if cfg.ResolvedMode() != config.ModeTransparent || cfg.IsTiered() || !cacheStore.IsEnabled() {
		tb.Fatal("benchmark requires the default enabled transparent proxy cache")
	}
	fixture.cacheStore = cacheStore
	forwarder := proxy.NewForwarder(
		nil,
		cfg.Upstream.Endpoint,
		cfg.Upstream.Region,
		cfg.Upstream.MaxIdleConnsPerHost,
		auth.NewProxySigner("bulk-delete-benchmark", "bulk-delete-benchmark-secret"),
		nil,
	)
	service := proxy.NewService(forwarder, cacheStore, cfg)
	router := NewServer(service, "127.0.0.1", 0, false, cfg.Server.MaxInflightRequests).Router()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		router.ServeHTTP(w, r)
		if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
			fixture.handlerDone <- struct{}{}
		}
	}))
	tb.Cleanup(gateway.Close)
	fixture.gatewayURL = gateway.URL
	fixture.client = gateway.Client()
	fixture.client.Timeout = 15 * time.Second
	tb.Cleanup(fixture.client.CloseIdleConnections)
	return fixture
}

func (f *bulkDeleteResponseCompletionBenchmark) seedCachedObject(tb testing.TB) {
	tb.Helper()
	meta := &cache.CachedObjectMeta{
		Bucket:        bulkDeleteFenceBucket,
		Key:           bulkDeleteFenceKey,
		ETag:          `"old-etag"`,
		ContentLength: int64(len(bulkDeleteFenceOldBody)),
		StatusCode:    http.StatusOK,
	}
	if err := f.cacheStore.PutWithMeta(context.Background(), bulkDeleteFenceBucket, bulkDeleteFenceKey, meta, []byte(bulkDeleteFenceOldBody), 60); err != nil {
		tb.Fatalf("seed cached object for DeleteObjects: %v", err)
	}
}

func (f *bulkDeleteResponseCompletionBenchmark) newRequest() (*http.Request, error) {
	r, err := http.NewRequest(http.MethodPost, f.gatewayURL+"/"+bulkDeleteFenceBucket+"?delete", bytes.NewReader(f.requestBody))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Authorization", bulkDeleteFenceAuthorization)
	r.Header.Set("Content-Type", "application/xml")
	return r, nil
}

func (f *bulkDeleteResponseCompletionBenchmark) readResponse() (int64, error) {
	r, err := f.newRequest()
	if err != nil {
		return 0, err
	}
	resp, err := f.client.Do(r)
	if err != nil {
		return 0, err
	}
	bodyBytes, readErr := io.Copy(io.Discard, resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return 0, readErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	if resp.StatusCode != http.StatusOK {
		return bodyBytes, fmt.Errorf("DeleteObjects status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if resp.Header.Get("Content-Type") != "application/xml" || resp.Header.Get(bulkDeleteFenceHeader) != bulkDeleteFenceHeaderValue {
		return bodyBytes, fmt.Errorf("DeleteObjects response headers differ from the upstream result")
	}
	if resp.ContentLength != int64(len(f.responseBody)) || bodyBytes != int64(len(f.responseBody)) {
		return bodyBytes, fmt.Errorf("DeleteObjects response bytes = %d/%d, want %d", bodyBytes, resp.ContentLength, len(f.responseBody))
	}
	return bodyBytes, nil
}

func (f *bulkDeleteResponseCompletionBenchmark) waitForHandler(tb testing.TB) {
	tb.Helper()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-f.handlerDone:
	case <-timer.C:
		tb.Fatal("DeleteObjects handler did not return after the client completed the response")
	}
}

// BenchmarkBulkDeleteResponseCompletion measures a successful cache-enabled
// DeleteObjects response through the registered route and transparent forwarder.
// The one-key control includes 128 KiB of padding to exercise response capture.
// The 1,000-key case uses an ordinary verbose DeleteResult at the S3 batch limit.
func BenchmarkBulkDeleteResponseCompletion(b *testing.B) {
	for _, keys := range []int{1, 1000} {
		b.Run(strconv.Itoa(keys), func(b *testing.B) {
			benchmarkBulkDeleteResponseCompletion(b, keys)
		})
	}
}

func benchmarkBulkDeleteResponseCompletion(b *testing.B, keys int) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	fixture := newBulkDeleteResponseCompletionBenchmark(b, keys)
	fixture.seedCachedObject(b)
	warmResponse, err := fixture.newRequest()
	if err != nil {
		b.Fatal(err)
	}
	response, err := doBulkDeleteFenceRequest(fixture.client, warmResponse)
	if err != nil {
		b.Fatalf("warm DeleteObjects response: %v", err)
	}
	if err := checkBulkDeleteFenceResponse(response, fixture.responseBody); err != nil {
		b.Fatalf("warm DeleteObjects response: %v", err)
	}
	fixture.waitForHandler(b)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		fixture.seedCachedObject(b)
		b.StartTimer()
		bodyBytes, err := fixture.readResponse()
		if err != nil {
			b.Fatal(err)
		}
		if bodyBytes != int64(len(fixture.responseBody)) {
			b.Fatalf("DeleteObjects response bytes = %d, want %d", bodyBytes, len(fixture.responseBody))
		}
		b.StopTimer()
		fixture.waitForHandler(b)
		b.StartTimer()
	}
	b.StopTimer()
}
