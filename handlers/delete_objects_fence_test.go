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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

const (
	bulkDeleteFenceBucket          = "bulk-delete-fence-bucket"
	bulkDeleteFenceKey             = "racing-key"
	bulkDeleteFenceOldBody         = "old-object-bytes"
	bulkDeleteFenceHeader          = "X-Bulk-Delete-Result"
	bulkDeleteFenceHeaderValue     = "upstream-response"
	bulkDeleteFenceViolationMarker = "DELETE_FENCE_ASSERTION_VIOLATED"
	bulkDeleteFenceAuthorization   = "AWS4-HMAC-SHA256 Credential=benchmark/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=deadbeef"
)

type bulkDeleteFenceCacheClient struct {
	cacheclient.CacheClient
	targetTombstone string
	postFence       chan struct{}
	releaseFence    <-chan struct{}
	putCount        atomic.Int32
	once            sync.Once
}

func (c *bulkDeleteFenceCacheClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	if key == c.targetTombstone && c.putCount.Add(1) == 2 {
		c.once.Do(func() { close(c.postFence) })
		<-c.releaseFence
	}
	return c.CacheClient.Put(ctx, key, data, ttlSeconds)
}

type bulkDeleteFenceResponseWriter struct {
	http.ResponseWriter
	committed chan struct{}
	once      sync.Once
}

func (w *bulkDeleteFenceResponseWriter) WriteHeader(statusCode int) {
	w.once.Do(func() { close(w.committed) })
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *bulkDeleteFenceResponseWriter) Write(body []byte) (int, error) {
	w.once.Do(func() { close(w.committed) })
	return w.ResponseWriter.Write(body)
}

type bulkDeleteFenceHTTPResponse struct {
	statusCode    int
	header        http.Header
	contentLength int64
	body          []byte
}

type bulkDeleteFenceObservation struct {
	committedBeforeFence    bool
	completedBeforeFence    bool
	staleRefillServedBefore bool
}

func TestBulkDeleteResponseWaitsForPostSuccessFence(t *testing.T) {
	observation, response, err := runBulkDeleteFenceScenario()
	if err != nil {
		t.Fatalf("could not complete the controlled delete/fence schedule: %v", err)
	}
	if observation.completedBeforeFence && observation.staleRefillServedBefore {
		t.Errorf("%s: the client completed the successful DeleteObjects response while the target key's post-success tombstone was held, then GET served the old refill; status=%d, X-Cache=%q, response_bytes=%d",
			bulkDeleteFenceViolationMarker, response.statusCode, "HIT", len(response.body))
		return
	}
	if observation.committedBeforeFence {
		t.Fatalf("response headers were committed before the fence but the complete-response counterexample was not established")
	}
	if observation.completedBeforeFence || !observation.staleRefillServedBefore {
		t.Fatalf("incomplete ordering observation: completed-before-fence=%t stale-refill-served-while-fence-held=%t",
			observation.completedBeforeFence, observation.staleRefillServedBefore)
	}
}

func runBulkDeleteFenceScenario() (bulkDeleteFenceObservation, bulkDeleteFenceHTTPResponse, error) {
	var observation bulkDeleteFenceObservation
	var deleteResponse bulkDeleteFenceHTTPResponse

	requestBody := []byte(`<Delete><Object><Key>racing-key</Key></Object></Delete>`)
	resultBody := []byte("<DeleteResult><Deleted><Key>" + bulkDeleteFenceKey + "</Key></Deleted>" + strings.Repeat(" ", 128*1024) + "</DeleteResult>")
	if len(resultBody) <= 128*1024 {
		return observation, deleteResponse, fmt.Errorf("fixed-length DeleteResult is only %d bytes; need more than 128 KiB to exercise early HTTP visibility", len(resultBody))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	releaseFence := make(chan struct{})
	var releaseFenceOnce sync.Once
	releasePostFence := func() { releaseFenceOnce.Do(func() { close(releaseFence) }) }
	allowDeleteReply := make(chan struct{})
	var allowDeleteReplyOnce sync.Once
	allowReply := func() { allowDeleteReplyOnce.Do(func() { close(allowDeleteReply) }) }

	deleteStarted := make(chan struct{})
	var deleteStartedOnce sync.Once
	upstreamErrors := make(chan error, 1)
	deleted := atomic.Bool{}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				upstreamErrors <- fmt.Errorf("read upstream DeleteObjects request: %w", err)
				http.Error(w, "request read failed", http.StatusBadRequest)
				return
			}
			if !bytes.Equal(body, requestBody) {
				upstreamErrors <- fmt.Errorf("forwarded DeleteObjects body differs: got %d bytes, want %d", len(body), len(requestBody))
				http.Error(w, "request body differs", http.StatusBadRequest)
				return
			}
			deleteStartedOnce.Do(func() { close(deleteStarted) })
			select {
			case <-allowDeleteReply:
			case <-r.Context().Done():
				return
			}
			deleted.Store(true)
			w.Header().Set("Content-Type", "application/xml")
			w.Header().Set(bulkDeleteFenceHeader, bulkDeleteFenceHeaderValue)
			w.Header().Set("Content-Length", strconv.Itoa(len(resultBody)))
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write(resultBody); err != nil {
				upstreamErrors <- fmt.Errorf("write upstream DeleteResult: %w", err)
			}
			return
		}

		if r.Method == http.MethodGet && r.URL.Path == "/"+bulkDeleteFenceBucket+"/"+bulkDeleteFenceKey {
			if deleted.Load() {
				writeBulkDeleteFenceResponse(w, http.StatusNotFound, "application/xml", []byte(`<Error><Code>NoSuchKey</Code></Error>`))
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.Header().Set("ETag", `"old-etag"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(bulkDeleteFenceOldBody)))
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, bulkDeleteFenceOldBody)
			return
		}

		http.NotFound(w, r)
	}))

	baseCache := cacheclient.NewMemoryCache()
	gateCache := &bulkDeleteFenceCacheClient{
		CacheClient:     baseCache,
		targetTombstone: cache.MakeTombstoneKey(bulkDeleteFenceBucket, bulkDeleteFenceKey),
		postFence:       make(chan struct{}),
		releaseFence:    releaseFence,
	}
	cfg := config.NewDefault()
	cfg.Upstream.Endpoint = upstream.URL
	cacheStore := cache.NewCacheWithClient(gateCache, &cfg.Cache)
	if cfg.IsTiered() || !cacheStore.IsEnabled() {
		releasePostFence()
		allowReply()
		upstream.Close()
		return observation, deleteResponse, fmt.Errorf("test requires the default enabled, non-tiered proxy cache")
	}

	forwarder := proxy.NewForwarder(
		nil,
		cfg.Upstream.Endpoint,
		cfg.Upstream.Region,
		cfg.Upstream.MaxIdleConnsPerHost,
		auth.NewProxySigner("bulk-delete-test", "bulk-delete-test-secret"),
		nil,
	)
	service := proxy.NewService(forwarder, cacheStore, cfg)
	router := NewServer(service, "127.0.0.1", 0, false, cfg.Server.MaxInflightRequests).Router()
	deleteCommitted := make(chan struct{})
	deleteHandlerDone := make(chan struct{})
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
			defer close(deleteHandlerDone)
			router.ServeHTTP(&bulkDeleteFenceResponseWriter{ResponseWriter: w, committed: deleteCommitted}, r)
			return
		}
		router.ServeHTTP(w, r)
	}))
	client := &http.Client{Timeout: 15 * time.Second}
	probeClient := &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   15 * time.Second,
	}

	defer func() {
		releasePostFence()
		allowReply()
		gateway.Close()
		upstream.Close()
		client.CloseIdleConnections()
		probeClient.CloseIdleConnections()
	}()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gateway.URL+"/"+bulkDeleteFenceBucket+"?delete", bytes.NewReader(requestBody))
	if err != nil {
		return observation, deleteResponse, fmt.Errorf("create DeleteObjects request: %w", err)
	}
	request.Header.Set("Authorization", bulkDeleteFenceAuthorization)
	request.Header.Set("Content-Type", "application/xml")
	deleteDone := make(chan struct {
		response bulkDeleteFenceHTTPResponse
		err      error
	}, 1)
	go func() {
		response, err := doBulkDeleteFenceRequest(client, request)
		deleteDone <- struct {
			response bulkDeleteFenceHTTPResponse
			err      error
		}{response: response, err: err}
	}()

	select {
	case <-deleteStarted:
	case err := <-upstreamErrors:
		return observation, deleteResponse, fmt.Errorf("upstream did not receive the expected DeleteObjects body: %w", err)
	case <-ctx.Done():
		return observation, deleteResponse, fmt.Errorf("waiting for upstream DeleteObjects request: %w", ctx.Err())
	}
	if got := gateCache.putCount.Load(); got != 1 {
		return observation, deleteResponse, fmt.Errorf("target tombstone puts before upstream request = %d, want exactly one pre-forward invalidation", got)
	}

	oldRequest, err := newBulkDeleteFenceGetRequest(ctx, gateway.URL)
	if err != nil {
		return observation, deleteResponse, fmt.Errorf("create racing GET: %w", err)
	}
	oldResponse, err := doBulkDeleteFenceRequest(probeClient, oldRequest)
	if err != nil {
		return observation, deleteResponse, fmt.Errorf("racing GET: %w", err)
	}
	if oldResponse.statusCode != http.StatusOK || oldResponse.header.Get(proxy.XCacheHeader) != proxy.XCacheMiss || string(oldResponse.body) != bulkDeleteFenceOldBody {
		return observation, deleteResponse, fmt.Errorf("racing GET did not refill the old object: status=%d X-Cache=%q body=%q", oldResponse.statusCode, oldResponse.header.Get(proxy.XCacheHeader), oldResponse.body)
	}
	if err := waitBulkDeleteFenceCacheMeta(ctx, cacheStore, bulkDeleteFenceBucket, bulkDeleteFenceKey); err != nil {
		return observation, deleteResponse, err
	}

	allowReply()
	if err := waitBulkDeleteFenceSignal(ctx, gateCache.postFence, "post-success cache fence"); err != nil {
		return observation, deleteResponse, err
	}
	if _, found, err := cacheStore.GetMeta(ctx, bulkDeleteFenceBucket, bulkDeleteFenceKey); err != nil || !found {
		return observation, deleteResponse, fmt.Errorf("old refill is not present while post-success fence is held: found=%t err=%v", found, err)
	}

	select {
	case <-deleteCommitted:
		observation.committedBeforeFence = true
	default:
	}
	var gotDeleteResponse bool
	if observation.committedBeforeFence {
		select {
		case result := <-deleteDone:
			if result.err != nil {
				return observation, deleteResponse, fmt.Errorf("client could not finish the response while fence was held: %w", result.err)
			}
			deleteResponse = result.response
			gotDeleteResponse = true
			observation.completedBeforeFence = true
		case <-ctx.Done():
			return observation, deleteResponse, fmt.Errorf("response headers committed before the fence but the client did not finish the fixed-length body: %w", ctx.Err())
		}
	} else {
		select {
		case <-deleteDone:
			return observation, deleteResponse, fmt.Errorf("client completed the delete response without the tracked ResponseWriter being committed")
		default:
		}
	}

	staleRequest, err := newBulkDeleteFenceGetRequest(ctx, gateway.URL)
	if err != nil {
		return observation, deleteResponse, fmt.Errorf("create GET while the post-success fence is held: %w", err)
	}
	staleResponse, err := doBulkDeleteFenceRequest(probeClient, staleRequest)
	if err != nil {
		return observation, deleteResponse, fmt.Errorf("GET while the post-success fence is held: %w", err)
	}
	if staleResponse.statusCode != http.StatusOK || staleResponse.header.Get(proxy.XCacheHeader) != proxy.XCacheHit || string(staleResponse.body) != bulkDeleteFenceOldBody {
		return observation, deleteResponse, fmt.Errorf("held-fence GET did not serve the racing old refill: status=%d X-Cache=%q body=%q", staleResponse.statusCode, staleResponse.header.Get(proxy.XCacheHeader), staleResponse.body)
	}
	observation.staleRefillServedBefore = true

	// The upstream has confirmed deletion, the stale refill was available while the
	// fence was held, and only a response that has already completed is a counterexample.
	releasePostFence()
	if !gotDeleteResponse {
		select {
		case result := <-deleteDone:
			if result.err != nil {
				return observation, deleteResponse, fmt.Errorf("read completed DeleteObjects response: %w", result.err)
			}
			deleteResponse = result.response
		case <-ctx.Done():
			return observation, deleteResponse, fmt.Errorf("DeleteObjects response did not complete after releasing the fence: %w", ctx.Err())
		}
	}
	if err := waitBulkDeleteFenceSignal(ctx, deleteHandlerDone, "DeleteObjects handler completion"); err != nil {
		return observation, deleteResponse, err
	}
	if err := checkBulkDeleteFenceResponse(deleteResponse, resultBody); err != nil {
		return observation, deleteResponse, err
	}

	if _, found, err := cacheStore.GetMeta(ctx, bulkDeleteFenceBucket, bulkDeleteFenceKey); err != nil || found {
		return observation, deleteResponse, fmt.Errorf("post-success fence did not remove the stale refill: found=%t err=%v", found, err)
	}
	postDeleteRequest, err := newBulkDeleteFenceGetRequest(ctx, gateway.URL)
	if err != nil {
		return observation, deleteResponse, fmt.Errorf("create GET after completed DeleteObjects response: %w", err)
	}
	postDeleteGet, err := doBulkDeleteFenceRequest(probeClient, postDeleteRequest)
	if err != nil {
		return observation, deleteResponse, fmt.Errorf("GET after completed DeleteObjects response: %w", err)
	}
	if postDeleteGet.statusCode != http.StatusNotFound || postDeleteGet.header.Get(proxy.XCacheHeader) == proxy.XCacheHit {
		return observation, deleteResponse, fmt.Errorf("GET after completed DeleteObjects response served stale cache: status=%d X-Cache=%q body=%q", postDeleteGet.statusCode, postDeleteGet.header.Get(proxy.XCacheHeader), postDeleteGet.body)
	}

	return observation, deleteResponse, nil
}

func writeBulkDeleteFenceResponse(w http.ResponseWriter, statusCode int, contentType string, body []byte) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(statusCode)
	_, _ = w.Write(body)
}

func newBulkDeleteFenceGetRequest(ctx context.Context, gatewayURL string) (*http.Request, error) {
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, gatewayURL+"/"+bulkDeleteFenceBucket+"/"+bulkDeleteFenceKey, nil)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Authorization", bulkDeleteFenceAuthorization)
	return r, nil
}

func doBulkDeleteFenceRequest(client *http.Client, request *http.Request) (bulkDeleteFenceHTTPResponse, error) {
	resp, err := client.Do(request)
	if err != nil {
		return bulkDeleteFenceHTTPResponse{}, err
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return bulkDeleteFenceHTTPResponse{}, readErr
	}
	if closeErr != nil {
		return bulkDeleteFenceHTTPResponse{}, closeErr
	}
	return bulkDeleteFenceHTTPResponse{
		statusCode:    resp.StatusCode,
		header:        resp.Header.Clone(),
		contentLength: resp.ContentLength,
		body:          body,
	}, nil
}

func waitBulkDeleteFenceSignal(ctx context.Context, signal <-chan struct{}, name string) error {
	select {
	case <-signal:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for %s: %w", name, ctx.Err())
	}
}

func waitBulkDeleteFenceCacheMeta(ctx context.Context, c *cache.Cache, bucket, key string) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, found, err := c.GetMeta(ctx, bucket, key)
		if err != nil {
			return fmt.Errorf("read refilled cache metadata: %w", err)
		}
		if found {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("waiting for old-object cache refill: %w", ctx.Err())
		}
	}
}

func checkBulkDeleteFenceResponse(response bulkDeleteFenceHTTPResponse, wantBody []byte) error {
	if response.statusCode != http.StatusOK {
		return fmt.Errorf("DeleteObjects status = %d, want %d", response.statusCode, http.StatusOK)
	}
	if got := response.header.Get("Content-Type"); got != "application/xml" {
		return fmt.Errorf("DeleteObjects Content-Type = %q, want application/xml", got)
	}
	if got := response.header.Get(bulkDeleteFenceHeader); got != bulkDeleteFenceHeaderValue {
		return fmt.Errorf("DeleteObjects %s = %q, want %q", bulkDeleteFenceHeader, got, bulkDeleteFenceHeaderValue)
	}
	if response.contentLength != int64(len(wantBody)) {
		return fmt.Errorf("DeleteObjects content length = %d, want %d", response.contentLength, len(wantBody))
	}
	if !bytes.Equal(response.body, wantBody) {
		return fmt.Errorf("DeleteObjects body differs: got %d bytes, want %d", len(response.body), len(wantBody))
	}
	return nil
}

func TestBulkDeleteWithCacheDisabledKeepsStreaming(t *testing.T) {
	body := []byte("<DeleteResult><Deleted><Key>racing-key</Key></Deleted>" + strings.Repeat(" ", 256*1024) + "</DeleteResult>")
	requestBody := []byte(`<Delete><Object><Key>racing-key</Key></Object></Delete>`)
	responseWritten := make(chan struct{})
	releaseUpstream := make(chan struct{})
	var releaseOnce sync.Once
	finishUpstream := func() { releaseOnce.Do(func() { close(releaseUpstream) }) }

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !r.URL.Query().Has("delete") {
			http.NotFound(w, r)
			return
		}
		if _, err := io.ReadAll(r.Body); err != nil {
			http.Error(w, "request read failed", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set(bulkDeleteFenceHeader, bulkDeleteFenceHeaderValue)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body[:len(body)/2])
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		} else {
			return
		}
		close(responseWritten)
		<-releaseUpstream
		_, _ = w.Write(body[len(body)/2:])
	}))
	defer upstream.Close()

	cfg := config.NewDefault()
	cfg.Upstream.Endpoint = upstream.URL
	disabled := false
	cfg.Cache.Enabled = &disabled
	forwarder := proxy.NewForwarder(
		nil,
		cfg.Upstream.Endpoint,
		cfg.Upstream.Region,
		cfg.Upstream.MaxIdleConnsPerHost,
		auth.NewProxySigner("bulk-delete-test", "bulk-delete-test-secret"),
		nil,
	)
	service := proxy.NewService(forwarder, cache.NewDisabledCache(), cfg)
	gateway := httptest.NewServer(NewServer(service, "127.0.0.1", 0, false, cfg.Server.MaxInflightRequests).Router())
	defer gateway.Close()
	defer finishUpstream()
	client := &http.Client{Timeout: 10 * time.Second}
	defer client.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gateway.URL+"/"+bulkDeleteFenceBucket+"?delete", bytes.NewReader(requestBody))
	if err != nil {
		t.Fatalf("create DeleteObjects request: %v", err)
	}
	request.Header.Set("Authorization", bulkDeleteFenceAuthorization)
	request.Header.Set("Content-Type", "application/xml")

	responseDone := make(chan struct {
		response bulkDeleteFenceHTTPResponse
		err      error
	}, 1)
	firstByteRead := make(chan error, 1)
	go func() {
		resp, err := client.Do(request)
		if err != nil {
			firstByteRead <- err
			responseDone <- struct {
				response bulkDeleteFenceHTTPResponse
				err      error
			}{err: err}
			return
		}
		firstByte := make([]byte, 1)
		_, err = io.ReadFull(resp.Body, firstByte)
		if err != nil {
			_ = resp.Body.Close()
			firstByteRead <- err
			responseDone <- struct {
				response bulkDeleteFenceHTTPResponse
				err      error
			}{err: err}
			return
		}
		firstByteRead <- nil
		bodyTail, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			err = readErr
		} else if closeErr != nil {
			err = closeErr
		}
		responseDone <- struct {
			response bulkDeleteFenceHTTPResponse
			err      error
		}{
			response: bulkDeleteFenceHTTPResponse{
				statusCode:    resp.StatusCode,
				header:        resp.Header.Clone(),
				contentLength: resp.ContentLength,
				body:          append(firstByte, bodyTail...),
			},
			err: err,
		}
	}()
	if err := waitBulkDeleteFenceSignal(ctx, responseWritten, "first upstream response chunk"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-firstByteRead:
		if err != nil {
			t.Fatalf("read first response byte before upstream finished: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("cache-disabled response did not stream its first byte while upstream was held: %v", ctx.Err())
	}
	finishUpstream()
	select {
	case result := <-responseDone:
		if result.err != nil {
			t.Fatalf("read streamed DeleteObjects response: %v", result.err)
		}
		if err := checkBulkDeleteFenceResponse(result.response, body); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatalf("cache-disabled response did not finish after the remaining upstream body was released: %v", ctx.Err())
	}
}
