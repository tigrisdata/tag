package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

const (
	broadcastErrorChunkSize       = 512
	broadcastErrorCacheQueue      = 64
	broadcastErrorInlineThreshold = 64 * 1024 // OCache v1.13.0 storage default
	broadcastErrorBucket          = "cache-errors"
	broadcastErrorKey             = "whole-object"
)

func TestBroadcastErrorsDoNotPublishIncompleteWholeObject(t *testing.T) {
	t.Run("slow cache listener", func(t *testing.T) {
		runFullGetSlowListenerFailure(t)
	})
	t.Run("upstream body error", func(t *testing.T) {
		runFullGetUpstreamFailure(t)
	})
}

func TestBroadcastCompletionKeepsFullQueuedBody(t *testing.T) {
	runFullGetFullQueueSuccess(t)
}

func runFullGetSlowListenerFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const chunks = broadcastErrorCacheQueue + 3
		fullBody := makeBroadcastErrorBody(chunks)
		originBody := newBroadcastErrorBodyReader()
		healthyBody := newBroadcastErrorBodyReader()
		cacheClient := newBroadcastErrorCacheClient()
		defer cacheClient.releaseFirst()

		forwarder := &broadcastErrorForwarder{responses: []*http.Response{
			broadcastErrorResponse(originBody, int64(len(fullBody))),
			broadcastErrorResponse(healthyBody, int64(len(fullBody))),
		}}
		router, cacheStore := newBroadcastErrorRouter(forwarder, cacheClient)

		firstResponse, firstDone := serveBroadcastErrorGET(router)
		waitBroadcastErrorSignal(t, cacheClient.firstStarted, "first cache PutStream")

		// The cache writer is held before its first read. Waiting after each
		// chunk lets the listener fill its 64-entry relay queue, block while
		// transferring the next chunk, and leave one chunk in its one-slot channel.
		feedBroadcastErrorBody(t, originBody, fullBody)
		finishBroadcastErrorBody(t, originBody, nil)
		waitBroadcastErrorSignal(t, firstDone, "first GET handler")
		if firstResponse.Code != http.StatusOK || !bytes.Equal(firstResponse.Body.Bytes(), fullBody) {
			t.Errorf("client response after a slow cache-listener drop is incomplete: status=%d body-bytes=%d, want status=%d body-bytes=%d", firstResponse.Code, firstResponse.Body.Len(), http.StatusOK, len(fullBody))
		}

		cacheClient.releaseFirst()
		cacheWriteErr := waitBroadcastErrorResult(t, cacheClient.firstDone, "first cache PutStream")
		synctest.Wait()
		if !errors.Is(cacheWriteErr, broadcast.ErrSlowConsumer) {
			t.Errorf("cache writer did not receive terminal error %v: got %v", broadcast.ErrSlowConsumer, cacheWriteErr)
		}

		found := checkBroadcastErrorCacheEntry(t, cacheStore, fullBody)
		secondResponse, secondDone := serveBroadcastErrorGET(router)
		if !found {
			waitBroadcastErrorSignal(t, cacheClient.secondStarted, "healthy GET cache PutStream")
			feedBroadcastErrorBody(t, healthyBody, fullBody)
			finishBroadcastErrorBody(t, healthyBody, nil)
		}
		waitBroadcastErrorSignal(t, secondDone, "healthy GET handler")
		if secondResponse.Code != http.StatusOK || !bytes.Equal(secondResponse.Body.Bytes(), fullBody) {
			t.Errorf("healthy GET after a cache-listener error is incomplete: status=%d body-bytes=%d, want status=%d body-bytes=%d", secondResponse.Code, secondResponse.Body.Len(), http.StatusOK, len(fullBody))
		}

		if !found {
			if err := waitBroadcastErrorResult(t, cacheClient.secondDone, "healthy GET cache PutStream"); err != nil {
				t.Fatalf("healthy cache populate: %v", err)
			}
			synctest.Wait()
			checkBroadcastErrorCacheEntry(t, cacheStore, fullBody)
			thirdResponse, thirdDone := serveBroadcastErrorGET(router)
			waitBroadcastErrorSignal(t, thirdDone, "cache-hit GET handler")
			if thirdResponse.Code != http.StatusOK || thirdResponse.Header().Get("X-Cache") != proxy.XCacheHit || !bytes.Equal(thirdResponse.Body.Bytes(), fullBody) {
				t.Errorf("GET after the healthy populate did not return the complete cached object: status=%d X-Cache=%q body-bytes=%d", thirdResponse.Code, thirdResponse.Header().Get("X-Cache"), thirdResponse.Body.Len())
			}
			if got := forwarder.calls.Load(); got != 2 {
				t.Errorf("origin GET calls after a cache miss and repair = %d, want 2", got)
			}
		}
	})
}

func runFullGetUpstreamFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			fullChunks    = broadcastErrorCacheQueue + 3
			partialChunks = fullChunks - 1
		)
		fullBody := makeBroadcastErrorBody(fullChunks)
		partialBody := newBroadcastErrorBodyReader()
		healthyBody := newBroadcastErrorBodyReader()
		upstreamErr := io.ErrUnexpectedEOF
		cacheClient := newBroadcastErrorCacheClient()
		defer cacheClient.releaseFirst()

		forwarder := &broadcastErrorForwarder{responses: []*http.Response{
			broadcastErrorResponse(partialBody, int64(len(fullBody))),
			broadcastErrorResponse(healthyBody, int64(len(fullBody))),
		}}
		router, cacheStore := newBroadcastErrorRouter(forwarder, cacheClient)

		firstResponse, firstDone := serveBroadcastErrorGET(router)
		waitBroadcastErrorSignal(t, cacheClient.firstStarted, "first cache PutStream")
		feedBroadcastErrorBody(t, partialBody, fullBody[:partialChunks*broadcastErrorChunkSize])
		finishBroadcastErrorBody(t, partialBody, upstreamErr)
		waitBroadcastErrorSignal(t, firstDone, "first GET handler")
		if !bytes.Equal(firstResponse.Body.Bytes(), fullBody[:partialChunks*broadcastErrorChunkSize]) {
			t.Fatalf("upstream failure response body = %d bytes, want %d-byte prefix", firstResponse.Body.Len(), partialChunks*broadcastErrorChunkSize)
		}

		cacheClient.releaseFirst()
		cacheWriteErr := waitBroadcastErrorResult(t, cacheClient.firstDone, "first cache PutStream")
		synctest.Wait()
		if !errors.Is(cacheWriteErr, upstreamErr) {
			t.Errorf("cache writer did not receive terminal error %v: got %v", upstreamErr, cacheWriteErr)
		}

		found := checkBroadcastErrorCacheEntry(t, cacheStore, fullBody)
		secondResponse, secondDone := serveBroadcastErrorGET(router)
		if !found {
			waitBroadcastErrorSignal(t, cacheClient.secondStarted, "healthy GET cache PutStream")
			feedBroadcastErrorBody(t, healthyBody, fullBody)
			finishBroadcastErrorBody(t, healthyBody, nil)
		}
		waitBroadcastErrorSignal(t, secondDone, "healthy GET handler")
		if secondResponse.Code != http.StatusOK || !bytes.Equal(secondResponse.Body.Bytes(), fullBody) {
			t.Errorf("healthy GET after an upstream body error is incomplete: status=%d body-bytes=%d, want status=%d body-bytes=%d", secondResponse.Code, secondResponse.Body.Len(), http.StatusOK, len(fullBody))
		}

		if !found {
			if err := waitBroadcastErrorResult(t, cacheClient.secondDone, "healthy GET cache PutStream"); err != nil {
				t.Fatalf("healthy cache populate: %v", err)
			}
			synctest.Wait()
			checkBroadcastErrorCacheEntry(t, cacheStore, fullBody)
			thirdResponse, thirdDone := serveBroadcastErrorGET(router)
			waitBroadcastErrorSignal(t, thirdDone, "cache-hit GET handler")
			if thirdResponse.Code != http.StatusOK || thirdResponse.Header().Get("X-Cache") != proxy.XCacheHit || !bytes.Equal(thirdResponse.Body.Bytes(), fullBody) {
				t.Errorf("GET after the healthy populate did not return the complete cached object: status=%d X-Cache=%q body-bytes=%d", thirdResponse.Code, thirdResponse.Header().Get("X-Cache"), thirdResponse.Body.Len())
			}
			if got := forwarder.calls.Load(); got != 2 {
				t.Errorf("origin GET calls after a cache miss and repair = %d, want 2", got)
			}
		}
	})
}

func runFullGetFullQueueSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const chunks = broadcastErrorCacheQueue + 2
		bodyBytes := makeBroadcastErrorBody(chunks)
		body := newBroadcastErrorBodyReader()
		cacheClient := newBroadcastErrorCacheClient()
		defer cacheClient.releaseFirst()

		forwarder := &broadcastErrorForwarder{responses: []*http.Response{
			broadcastErrorResponse(body, int64(len(bodyBytes))),
		}}
		router, cacheStore := newBroadcastErrorRouter(forwarder, cacheClient)

		firstResponse, firstDone := serveBroadcastErrorGET(router)
		waitBroadcastErrorSignal(t, cacheClient.firstStarted, "first cache PutStream")
		feedBroadcastErrorBody(t, body, bodyBytes)
		finishBroadcastErrorBody(t, body, nil)
		waitBroadcastErrorSignal(t, firstDone, "first GET handler")
		if firstResponse.Code != http.StatusOK || !bytes.Equal(firstResponse.Body.Bytes(), bodyBytes) {
			t.Fatalf("successful GET response = status %d, %d bytes; want status %d, %d bytes", firstResponse.Code, firstResponse.Body.Len(), http.StatusOK, len(bodyBytes))
		}

		cacheClient.releaseFirst()
		if err := waitBroadcastErrorResult(t, cacheClient.firstDone, "first cache PutStream"); err != nil {
			t.Fatalf("cache PutStream: %v", err)
		}
		synctest.Wait()
		if !checkBroadcastErrorCacheEntry(t, cacheStore, bodyBytes) {
			t.Fatal("successful full-queue stream did not publish its complete cache entry")
		}

		secondResponse, secondDone := serveBroadcastErrorGET(router)
		waitBroadcastErrorSignal(t, secondDone, "cache-hit GET handler")
		if secondResponse.Code != http.StatusOK || secondResponse.Header().Get("X-Cache") != proxy.XCacheHit || !bytes.Equal(secondResponse.Body.Bytes(), bodyBytes) {
			t.Fatalf("cache-hit GET = status %d X-Cache=%q body-bytes=%d; want complete cached response", secondResponse.Code, secondResponse.Header().Get("X-Cache"), secondResponse.Body.Len())
		}
		if got := forwarder.calls.Load(); got != 1 {
			t.Fatalf("origin GET calls = %d, want 1", got)
		}
	})
}

func newBroadcastErrorRouter(forwarder *broadcastErrorForwarder, cacheClient *broadcastErrorCacheClient) (http.Handler, *cache.Cache) {
	cfg := config.NewDefault()
	enabled := true
	cfg.Cache.Enabled = &enabled
	cfg.Cache.SizeThreshold = 1 << 20
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Broadcast.ChunkSize = broadcastErrorChunkSize
	cfg.Broadcast.ChannelBuffer = 1

	cacheStore := cache.NewCacheWithClient(cacheClient, &cfg.Cache)
	service := proxy.NewService(forwarder, cacheStore, cfg)
	return NewServer(service, "127.0.0.1", 0, false, 0).Router(), cacheStore
}

func serveBroadcastErrorGET(router http.Handler) (*httptest.ResponseRecorder, <-chan struct{}) {
	response := httptest.NewRecorder()
	done := make(chan struct{})
	request := httptest.NewRequest(http.MethodGet, "/"+broadcastErrorBucket+"/"+broadcastErrorKey, nil)
	go func() {
		defer close(done)
		// A committed response whose stream fails is aborted with
		// http.ErrAbortHandler, which a real http.Server recovers; this helper
		// drives the router directly, so recover it here the same way.
		defer func() {
			if r := recover(); r != nil && r != http.ErrAbortHandler {
				panic(r)
			}
		}()
		router.ServeHTTP(response, request)
	}()
	return response, done
}

func makeBroadcastErrorBody(chunks int) []byte {
	body := make([]byte, chunks*broadcastErrorChunkSize)
	for i := range body {
		body[i] = byte((i * 31) % 251)
	}
	return body
}

func feedBroadcastErrorBody(t *testing.T, body *broadcastErrorBodyReader, data []byte) {
	t.Helper()
	for offset := 0; offset < len(data); offset += broadcastErrorChunkSize {
		end := offset + broadcastErrorChunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := append([]byte(nil), data[offset:end]...)
		body.chunks <- chunk
		select {
		case n := <-body.reads:
			if n != len(chunk) {
				t.Fatalf("upstream body read %d bytes, want %d", n, len(chunk))
			}
		case <-time.After(time.Second):
			t.Fatalf("upstream did not consume body chunk at offset %d", offset)
		}
		synctest.Wait()
	}
}

func finishBroadcastErrorBody(t *testing.T, body *broadcastErrorBodyReader, err error) {
	t.Helper()
	body.terminal <- err
	select {
	case got := <-body.terminalReads:
		if !errors.Is(got, err) {
			t.Fatalf("upstream body terminal error = %v, want %v", got, err)
		}
	case <-time.After(time.Second):
		t.Fatalf("upstream body did not finish with %v", err)
	}
	synctest.Wait()
}

func waitBroadcastErrorSignal(t *testing.T, ch <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", operation)
	}
}

func waitBroadcastErrorResult(t *testing.T, ch <-chan error, operation string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", operation)
		return nil
	}
}

func checkBroadcastErrorCacheEntry(t *testing.T, cacheStore *cache.Cache, want []byte) bool {
	t.Helper()
	meta, found, err := cacheStore.GetMeta(context.Background(), broadcastErrorBucket, broadcastErrorKey)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if !found {
		return false
	}
	var cached bytes.Buffer
	bodyErr := cacheStore.GetBodyStream(context.Background(), broadcastErrorBucket, broadcastErrorKey, meta.ETag, &cached)
	if bodyErr != nil || meta.ContentLength != int64(len(want)) || !bytes.Equal(cached.Bytes(), want) {
		t.Errorf("visible cache entry is incomplete: metadata Content-Length=%d body-bytes=%d body-read-error=%v, want %d complete bytes", meta.ContentLength, cached.Len(), bodyErr, len(want))
	}
	return true
}

func newBroadcastErrorCacheClient() *broadcastErrorCacheClient {
	release := make(chan struct{})
	client := &broadcastErrorCacheClient{
		CacheClient:   &inlineStorageCacheClient{CacheClient: cacheclient.NewMemoryCache()},
		firstStarted:  make(chan struct{}),
		release:       release,
		firstDone:     make(chan error, 1),
		secondStarted: make(chan struct{}),
		secondDone:    make(chan error, 1),
	}
	return client
}

// inlineStorageCacheClient mirrors embedded storage's small-value read: it treats
// io.ErrUnexpectedEOF as a normal short value while probing the inline threshold.
type inlineStorageCacheClient struct {
	cacheclient.CacheClient
}

func (c *inlineStorageCacheClient) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	data, err := io.ReadAll(r)
	if err != nil && (err != io.ErrUnexpectedEOF || len(data) > broadcastErrorInlineThreshold) {
		return err
	}
	return c.CacheClient.Put(ctx, key, data, ttlSeconds)
}

type broadcastErrorCacheClient struct {
	cacheclient.CacheClient
	firstStarted  chan struct{}
	release       chan struct{}
	firstDone     chan error
	secondStarted chan struct{}
	secondDone    chan error
	puts          atomic.Int32
	releaseOnce   sync.Once
}

func (c *broadcastErrorCacheClient) releaseFirst() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func (c *broadcastErrorCacheClient) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	switch c.puts.Add(1) {
	case 1:
		close(c.firstStarted)
		select {
		case <-c.release:
		case <-ctx.Done():
			err := ctx.Err()
			c.firstDone <- err
			return err
		}
		err := c.CacheClient.PutStream(ctx, key, r, ttlSeconds)
		c.firstDone <- err
		return err
	case 2:
		close(c.secondStarted)
		err := c.CacheClient.PutStream(ctx, key, r, ttlSeconds)
		c.secondDone <- err
		return err
	default:
		return c.CacheClient.PutStream(ctx, key, r, ttlSeconds)
	}
}

type broadcastErrorForwarder struct {
	handlerPrefetchForwarder
	responses []*http.Response
	calls     atomic.Int32
}

func (f *broadcastErrorForwarder) DoRequestWithCreds(ctx context.Context, _ *http.Request, _, _ string) (*http.Response, error) {
	index := int(f.calls.Add(1)) - 1
	if index >= len(f.responses) {
		return nil, fmt.Errorf("unexpected upstream GET %d", index+1)
	}
	if body, ok := f.responses[index].Body.(*broadcastErrorBodyReader); ok {
		body.ctx = ctx
	}
	return f.responses[index], nil
}

func broadcastErrorResponse(body io.ReadCloser, contentLength int64) *http.Response {
	header := make(http.Header)
	header.Set("Content-Length", strconv.FormatInt(contentLength, 10))
	header.Set("Content-Type", "application/octet-stream")
	header.Set("ETag", `"whole-object"`)
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        header,
		Body:          body,
		ContentLength: contentLength,
	}
}

type broadcastErrorBodyReader struct {
	ctx           context.Context
	chunks        chan []byte
	terminal      chan error
	reads         chan int
	terminalReads chan error
}

func newBroadcastErrorBodyReader() *broadcastErrorBodyReader {
	return &broadcastErrorBodyReader{
		chunks:        make(chan []byte, 1),
		terminal:      make(chan error, 1),
		reads:         make(chan int, 1),
		terminalReads: make(chan error, 1),
	}
}

func (b *broadcastErrorBodyReader) Read(p []byte) (int, error) {
	select {
	case chunk := <-b.chunks:
		n := copy(p, chunk)
		b.reads <- n
		if n != len(chunk) {
			return n, fmt.Errorf("upstream chunk has %d bytes; read buffer has %d", len(chunk), len(p))
		}
		return n, nil
	case err := <-b.terminal:
		b.terminalReads <- err
		if err == nil {
			return 0, io.EOF
		}
		return 0, err
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	}
}

func (b *broadcastErrorBodyReader) Close() error { return nil }
