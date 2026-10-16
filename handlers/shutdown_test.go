package handlers

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
	"github.com/tigrisdata/tag/s3err"
)

type shutdownCacheClient struct {
	cacheclient.CacheClient

	deleteCalls     atomic.Int32
	blockDeleteCall int32
	cleanupStarted  chan struct{}
	releaseCleanup  <-chan struct{}
	cleanupDone     chan struct{}
	backendClosed   chan struct{}
	handlerDone     <-chan struct{}
	closedEarly     atomic.Bool
	afterClose      atomic.Int32
	closeOnce       sync.Once
}

func (c *shutdownCacheClient) Delete(ctx context.Context, key string) error {
	block := c.deleteCalls.Add(1) == c.blockDeleteCall
	if block {
		close(c.cleanupStarted)
		<-c.releaseCleanup
	}
	select {
	case <-c.backendClosed:
		c.afterClose.Add(1)
		if block && c.cleanupDone != nil {
			close(c.cleanupDone)
		}
		return errors.New("cache client closed before delete")
	default:
	}
	err := c.CacheClient.Delete(ctx, key)
	if block && c.cleanupDone != nil {
		close(c.cleanupDone)
	}
	return err
}

func (c *shutdownCacheClient) Close() error {
	c.closeOnce.Do(func() {
		select {
		case <-c.handlerDone:
		default:
			c.closedEarly.Store(true)
		}
		close(c.backendClosed)
	})
	return c.CacheClient.Close()
}

func TestStopWaitsForActiveDeleteCleanupAfterDrainExpiry(t *testing.T) {
	origin := newHeldShutdownOrigin()
	defer origin.server.Close()
	releaseCleanup := make(chan struct{})
	var releaseCleanupOnce sync.Once
	unblockCleanup := func() { releaseCleanupOnce.Do(func() { close(releaseCleanup) }) }
	cacheClient := &shutdownCacheClient{
		CacheClient:     cacheclient.NewMemoryCache(),
		blockDeleteCall: 2,
		cleanupStarted:  make(chan struct{}),
		releaseCleanup:  releaseCleanup,
		backendClosed:   make(chan struct{}),
	}
	server := newShutdownServer(origin.server.URL, cacheClient)
	_, handlerDone := observeShutdownHandlers(server, cacheClient)
	shutdownStarted := make(chan struct{})
	addr, serveDone := startShutdownTestServer(t, server, func() { close(shutdownStarted) })
	defer func() {
		origin.releaseResponse()
		unblockCleanup()
		_ = server.httpServer.Close()
		if !closed(cacheClient.backendClosed) {
			_ = cacheClient.Close()
		}
	}()

	requestDone := sendShutdownDelete(t, addr, &http.Client{Timeout: 5 * time.Second})
	select {
	case <-origin.started:
	case <-time.After(5 * time.Second):
		t.Fatal("DELETE did not reach the origin")
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stopReturned := make(chan struct{})
	stopDone := make(chan error, 1)
	go func() {
		stopErr := server.Stop(drainCtx)
		close(stopReturned)
		_ = cacheClient.Close()
		stopDone <- stopErr
	}()
	select {
	case <-shutdownStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP shutdown did not start")
	}
	origin.releaseResponse()
	select {
	case <-cacheClient.cleanupStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("successful DELETE did not reach its post-forward cache cleanup")
	}

	<-drainCtx.Done()
	prematureClose := closed(cacheClient.backendClosed)
	returnedBeforeCleanup := closed(stopReturned)
	unblockCleanup()
	select {
	case <-requestDone:
	case <-time.After(5 * time.Second):
		t.Error("DELETE client did not observe completion or abort")
	}
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Error("DELETE handler did not exit after cache cleanup was released")
	}
	var stopErr error
	select {
	case stopErr = <-stopDone:
	case <-time.After(5 * time.Second):
		t.Error("Stop did not return after the handler exited")
	}
	if !errors.Is(stopErr, context.DeadlineExceeded) {
		t.Errorf("Stop error = %v, want drain deadline", stopErr)
	}
	if prematureClose || cacheClient.closedEarly.Load() || returnedBeforeCleanup || cacheClient.afterClose.Load() != 0 {
		t.Errorf("backend close crossed active handler cleanup: prematureClose=%v closedEarly=%v stopReturnedEarly=%v cacheCallsAfterClose=%d",
			prematureClose, cacheClient.closedEarly.Load(), returnedBeforeCleanup, cacheClient.afterClose.Load())
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("Serve returned %v, want http.ErrServerClosed", err)
	}
}

type heldShutdownOrigin struct {
	server      *httptest.Server
	started     chan struct{}
	canceled    chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	cancelOnce  sync.Once
}

func newHeldShutdownOrigin() *heldShutdownOrigin {
	origin := &heldShutdownOrigin{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}
	origin.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(origin.started)
		select {
		case <-origin.release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
			origin.cancelOnce.Do(func() { close(origin.canceled) })
		}
	}))
	return origin
}

func (o *heldShutdownOrigin) releaseResponse() {
	o.releaseOnce.Do(func() { close(o.release) })
}

type shutdownRequestResult struct {
	status int
	err    error
}

func newShutdownServer(originURL string, cacheClient *shutdownCacheClient) *Server {
	cfg := config.NewDefault()
	cfg.Upstream.Endpoint = originURL
	cfg.Cache.SetEnabled(true)
	cfg.Cache.SetLegacyCoordination(true)
	objectCache := cache.NewCacheWithClient(cacheClient, &cfg.Cache)
	forwarder := proxy.NewForwarder(
		auth.NewCredentialStore(), originURL, cfg.Upstream.Region, 1,
		auth.NewProxySigner("proxy-key", "proxy-secret"), nil,
	)
	return NewServer(proxy.NewService(forwarder, objectCache, cfg), "127.0.0.1", 0, false, 0)
}

func observeShutdownHandlers(server *Server, cacheClient *shutdownCacheClient) (<-chan context.Context, <-chan struct{}) {
	requestContext := make(chan context.Context, 1)
	handlerDone := make(chan struct{})
	var handlerDoneOnce sync.Once
	server.router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestContext <- r.Context()
			defer handlerDoneOnce.Do(func() { close(handlerDone) })
			next.ServeHTTP(w, r)
		})
	})
	if cacheClient != nil {
		cacheClient.handlerDone = handlerDone
	}
	return requestContext, handlerDone
}

func startShutdownTestServer(t *testing.T, server *Server, onShutdown func()) (string, <-chan error) {
	t.Helper()
	if server.httpServer == nil {
		server.httpServer = &http.Server{Handler: server.router}
	}
	if onShutdown != nil {
		server.httpServer.RegisterOnShutdown(onShutdown)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.httpServer.Serve(listener) }()
	return "http://" + listener.Addr().String(), serveDone
}

func sendShutdownDelete(t *testing.T, url string, client *http.Client) <-chan shutdownRequestResult {
	t.Helper()
	request, err := http.NewRequest(http.MethodDelete, url+"/bucket/key", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan shutdownRequestResult, 1)
	go func() {
		resp, err := client.Do(request)
		if err != nil {
			result <- shutdownRequestResult{err: err}
			return
		}
		_, copyErr := io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		result <- shutdownRequestResult{status: resp.StatusCode, err: copyErr}
	}()
	return result
}

func TestStopDrainsActiveDeleteWithinGrace(t *testing.T) {
	origin := newHeldShutdownOrigin()
	defer origin.server.Close()
	cacheClient := &shutdownCacheClient{
		CacheClient:    cacheclient.NewMemoryCache(),
		cleanupStarted: make(chan struct{}),
		backendClosed:  make(chan struct{}),
	}
	server := newShutdownServer(origin.server.URL, cacheClient)
	_, handlerDone := observeShutdownHandlers(server, cacheClient)
	shutdownStarted := make(chan struct{})
	addr, serveDone := startShutdownTestServer(t, server, func() { close(shutdownStarted) })
	defer func() {
		origin.releaseResponse()
		_ = server.httpServer.Close()
		if !closed(cacheClient.backendClosed) {
			_ = cacheClient.Close()
		}
	}()

	requestDone := sendShutdownDelete(t, addr, &http.Client{Timeout: 5 * time.Second})
	select {
	case <-origin.started:
	case <-time.After(5 * time.Second):
		t.Fatal("DELETE did not reach the origin")
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() {
		stopErr := server.Stop(drainCtx)
		_ = cacheClient.Close()
		stopDone <- stopErr
	}()
	select {
	case <-shutdownStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP shutdown did not start")
	}
	origin.releaseResponse()

	select {
	case result := <-requestDone:
		if result.err != nil || result.status != http.StatusNoContent {
			t.Errorf("DELETE response = status %d, error %v; want 204", result.status, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DELETE did not finish within the drain grace")
	}
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("DELETE handler did not exit")
	}
	if err := <-stopDone; err != nil {
		t.Errorf("Stop returned %v for a request completed inside the grace", err)
	}
	if calls := cacheClient.deleteCalls.Load(); calls != 2 {
		t.Errorf("cache Delete calls = %d, want pre-forward and post-success invalidation", calls)
	}
	if cacheClient.closedEarly.Load() || cacheClient.afterClose.Load() != 0 {
		t.Errorf("cache closed before the successful DELETE completed: early=%v callsAfterClose=%d",
			cacheClient.closedEarly.Load(), cacheClient.afterClose.Load())
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("Serve returned %v, want http.ErrServerClosed", err)
	}
}
func TestStopCancelsForwardingAfterDrainExpiry(t *testing.T) {
	origin := newHeldShutdownOrigin()
	defer origin.server.Close()
	cacheClient := &shutdownCacheClient{
		CacheClient:    cacheclient.NewMemoryCache(),
		cleanupStarted: make(chan struct{}),
		backendClosed:  make(chan struct{}),
	}
	server := newShutdownServer(origin.server.URL, cacheClient)
	requestContext, handlerDone := observeShutdownHandlers(server, cacheClient)
	shutdownStarted := make(chan struct{})
	addr, serveDone := startShutdownTestServer(t, server, func() { close(shutdownStarted) })
	defer func() {
		origin.releaseResponse()
		_ = server.httpServer.Close()
		if !closed(cacheClient.backendClosed) {
			_ = cacheClient.Close()
		}
	}()

	requestDone := sendShutdownDelete(t, addr, &http.Client{Timeout: 5 * time.Second})
	select {
	case <-origin.started:
	case <-time.After(5 * time.Second):
		t.Fatal("DELETE did not reach the origin")
	}
	handlerCtx := <-requestContext
	drainCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() {
		stopErr := server.Stop(drainCtx)
		_ = cacheClient.Close()
		stopDone <- stopErr
	}()
	select {
	case <-shutdownStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP shutdown did not start")
	}
	stopErr := <-stopDone
	if !errors.Is(stopErr, context.DeadlineExceeded) {
		t.Errorf("Stop error = %v, want drain deadline", stopErr)
	}
	select {
	case <-origin.canceled:
	case <-time.After(5 * time.Second):
		t.Error("origin did not observe forwarding cancellation after shutdown")
	}
	if !closed(handlerCtx.Done()) || !closed(handlerDone) || cacheClient.closedEarly.Load() {
		t.Errorf("expiry did not cancel and join forwarding before close: requestCanceled=%v handlerExited=%v closedEarly=%v",
			closed(handlerCtx.Done()), closed(handlerDone), cacheClient.closedEarly.Load())
	}

	origin.releaseResponse()
	select {
	case <-requestDone:
	case <-time.After(5 * time.Second):
		t.Error("DELETE client did not observe completion or abort")
	}
	if !closed(handlerDone) {
		select {
		case <-handlerDone:
		case <-time.After(5 * time.Second):
			t.Error("DELETE handler did not exit after the origin was released")
		}
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("Serve returned %v, want http.ErrServerClosed", err)
	}
}
func TestStopRejectsRequestsAfterClosingAdmission(t *testing.T) {
	var originCalls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer origin.Close()
	cacheClient := &shutdownCacheClient{
		CacheClient:    cacheclient.NewMemoryCache(),
		cleanupStarted: make(chan struct{}),
		backendClosed:  make(chan struct{}),
	}
	handlerDone := make(chan struct{})
	close(handlerDone)
	cacheClient.handlerDone = handlerDone
	server := newShutdownServer(origin.URL, cacheClient)
	if server.httpServer == nil {
		server.httpServer = &http.Server{Handler: server.router}
	}
	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodDelete, "/bucket/key", nil)
	server.httpServer.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Errorf("request after shutdown status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if response.Header().Get("Content-Type") != "application/xml" {
		t.Errorf("shutdown S3 error content type = %q, want application/xml", response.Header().Get("Content-Type"))
	}
	var errorResponse s3err.ErrorResponse
	if err := xml.Unmarshal(response.Body.Bytes(), &errorResponse); err != nil {
		t.Errorf("decode shutdown S3 error: %v", err)
	} else if errorResponse.Code != s3err.ErrServiceUnavailable {
		t.Errorf("shutdown S3 error code = %q, want %q", errorResponse.Code, s3err.ErrServiceUnavailable)
	}
	operational := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(operational, httptest.NewRequest(http.MethodGet, "/health", nil))
	if operational.Code != http.StatusServiceUnavailable || operational.Header().Get("Content-Type") != "text/plain; charset=utf-8" {
		t.Errorf("health endpoint during shutdown = status %d, content type %q; want plain HTTP 503",
			operational.Code, operational.Header().Get("Content-Type"))
	}
	if originCalls.Load() != 0 || cacheClient.deleteCalls.Load() != 0 {
		t.Errorf("post-stop request reached dependencies: origin calls=%d cache deletes=%d", originCalls.Load(), cacheClient.deleteCalls.Load())
	}
	_ = cacheClient.Close()
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

type flushOnWriteHeader struct {
	http.ResponseWriter
}

func (w flushOnWriteHeader) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w flushOnWriteHeader) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
	return n, err
}

func (w flushOnWriteHeader) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func TestStopJoinsDisconnectedHTTP2DeleteCleanup(t *testing.T) {
	origin := newHeldShutdownOrigin()
	defer origin.server.Close()

	releaseCleanup := make(chan struct{})
	var releaseCleanupOnce sync.Once
	unblockCleanup := func() { releaseCleanupOnce.Do(func() { close(releaseCleanup) }) }
	cacheClient := &shutdownCacheClient{
		CacheClient:     cacheclient.NewMemoryCache(),
		blockDeleteCall: 2,
		cleanupStarted:  make(chan struct{}),
		releaseCleanup:  releaseCleanup,
		cleanupDone:     make(chan struct{}),
		backendClosed:   make(chan struct{}),
	}
	server := newShutdownServer(origin.server.URL, cacheClient)
	requestContext, handlerDone := observeShutdownHandlers(server, cacheClient)
	server.router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(flushOnWriteHeader{ResponseWriter: w}, r)
		})
	})
	if server.httpServer == nil {
		server.httpServer = &http.Server{Handler: server.router}
	}

	shutdownStarted := make(chan struct{})
	var shutdownOnce sync.Once
	server.httpServer.RegisterOnShutdown(func() {
		shutdownOnce.Do(func() { close(shutdownStarted) })
	})
	testServer := httptest.NewUnstartedServer(server.httpServer.Handler)
	testServer.Config = server.httpServer
	testServer.EnableHTTP2 = true
	testServer.StartTLS()
	defer func() {
		origin.releaseResponse()
		unblockCleanup()
		_ = server.httpServer.Close()
		testServer.Close()
		if !closed(cacheClient.backendClosed) {
			_ = cacheClient.Close()
		}
	}()

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodDelete, testServer.URL+"/bucket/key", nil)
	if err != nil {
		t.Fatal(err)
	}
	responseDone := make(chan struct {
		proto  int
		status int
		err    error
	}, 1)
	go func() {
		resp, err := testServer.Client().Do(request)
		if err != nil {
			responseDone <- struct {
				proto  int
				status int
				err    error
			}{err: err}
			return
		}
		responseDone <- struct {
			proto  int
			status int
			err    error
		}{proto: resp.ProtoMajor, status: resp.StatusCode, err: resp.Body.Close()}
	}()
	select {
	case <-origin.started:
	case <-time.After(5 * time.Second):
		t.Fatal("DELETE did not reach the origin")
	}
	handlerCtx := <-requestContext
	origin.releaseResponse()
	select {
	case <-cacheClient.cleanupStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("successful DELETE did not reach post-forward cache cleanup")
	}
	response := <-responseDone
	if response.err != nil || response.status != http.StatusNoContent || response.proto != 2 {
		t.Fatalf("HTTP/2 DELETE response = protocol %d, status %d, error %v; want HTTP/2 204", response.proto, response.status, response.err)
	}

	cancelRequest()
	testServer.CloseClientConnections()
	select {
	case <-handlerCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("disconnected HTTP/2 request context was not canceled")
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	shutdownErr := server.httpServer.Shutdown(shutdownCtx)
	cancelShutdown()
	if shutdownErr != nil {
		t.Fatalf("Shutdown after the HTTP/2 stream closed: %v", shutdownErr)
	}
	if closed(handlerDone) {
		t.Fatal("DELETE handler exited before its blocked consistency cleanup was released")
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), time.Second)
	defer cancelStop()
	stopDone := make(chan error, 1)
	go func() {
		stopErr := server.Stop(stopCtx)
		_ = cacheClient.Close()
		stopDone <- stopErr
	}()
	go func() {
		<-stopCtx.Done()
		unblockCleanup()
	}()
	stopErr := <-stopDone
	if !errors.Is(stopErr, context.DeadlineExceeded) {
		t.Errorf("Stop error while joining disconnected handler cleanup = %v, want drain deadline", stopErr)
	}
	if !closed(cacheClient.cleanupDone) || !closed(handlerDone) || cacheClient.closedEarly.Load() || cacheClient.afterClose.Load() != 0 {
		t.Errorf("Stop did not join disconnected handler cleanup before close: cleanupDone=%v handlerDone=%v closedEarly=%v callsAfterClose=%d",
			closed(cacheClient.cleanupDone), closed(handlerDone), cacheClient.closedEarly.Load(), cacheClient.afterClose.Load())
	}
	select {
	case <-shutdownStarted:
	default:
		t.Error("HTTP server shutdown callback was not observed")
	}
}

func TestStopCancelsIncompletePutBodyAfterDrainExpiry(t *testing.T) {
	originStarted := make(chan struct{})
	originReadBody := make(chan struct{})
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(originStarted)
		var oneByte [1]byte
		n, err := r.Body.Read(oneByte[:])
		if n > 0 {
			close(originReadBody)
		}
		if err == nil {
			_, _ = io.Copy(io.Discard, r.Body)
		}
	}))
	defer origin.Close()

	cacheClient := &shutdownCacheClient{
		CacheClient:    cacheclient.NewMemoryCache(),
		cleanupStarted: make(chan struct{}),
		backendClosed:  make(chan struct{}),
	}
	server := newShutdownServer(origin.URL, cacheClient)
	requestContext, handlerDone := observeShutdownHandlers(server, cacheClient)
	shutdownStarted := make(chan struct{})
	addr, serveDone := startShutdownTestServer(t, server, func() { close(shutdownStarted) })
	var closeBodyOnce sync.Once
	bodyReader, bodyWriter := io.Pipe()
	closeBody := func() { closeBodyOnce.Do(func() { _ = bodyWriter.Close() }) }
	defer func() {
		closeBody()
		_ = server.httpServer.Close()
		if !closed(cacheClient.backendClosed) {
			_ = cacheClient.Close()
		}
	}()

	request, err := http.NewRequest(http.MethodPut, addr+"/bucket/key", bodyReader)
	if err != nil {
		t.Fatal(err)
	}
	request.ContentLength = 10
	request.Header.Set("Content-Length", "10")
	requestDone := make(chan error, 1)
	go func() {
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
		if err == nil {
			_ = resp.Body.Close()
		}
		requestDone <- err
	}()
	select {
	case <-originStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("PUT did not reach the origin")
	}
	handlerCtx := <-requestContext
	writeDone := make(chan error, 1)
	go func() {
		_, err := bodyWriter.Write([]byte("x"))
		writeDone <- err
	}()
	select {
	case <-originReadBody:
	case <-time.After(5 * time.Second):
		t.Fatal("origin did not receive the partial PUT body")
	}

	drainCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() {
		stopErr := server.Stop(drainCtx)
		_ = cacheClient.Close()
		stopDone <- stopErr
	}()
	select {
	case <-shutdownStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP shutdown did not start")
	}
	stopErr := <-stopDone
	if !errors.Is(stopErr, context.DeadlineExceeded) {
		t.Errorf("Stop error = %v, want drain deadline", stopErr)
	}
	if !closed(handlerCtx.Done()) || !closed(handlerDone) || cacheClient.closedEarly.Load() {
		t.Errorf("incomplete PUT was not canceled and joined before close: requestCanceled=%v handlerDone=%v closedEarly=%v",
			closed(handlerCtx.Done()), closed(handlerDone), cacheClient.closedEarly.Load())
	}
	closeBody()
	select {
	case <-writeDone:
	case <-time.After(5 * time.Second):
		t.Error("partial body writer did not stop after the request was aborted")
	}
	select {
	case <-requestDone:
	case <-time.After(5 * time.Second):
		t.Error("PUT client did not observe completion or abort")
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("Serve returned %v, want http.ErrServerClosed", err)
	}
}

func TestStartUsesHandlerLifetimeBarrier(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	server := NewServer(nil, "127.0.0.1", port, false, 0)
	handlerStarted := make(chan struct{})
	handlerDone := make(chan struct{})
	releaseHandler := make(chan struct{})
	var releaseOnce sync.Once
	unblockHandler := func() { releaseOnce.Do(func() { close(releaseHandler) }) }
	server.router.HandleFunc("/shutdown-test", func(w http.ResponseWriter, r *http.Request) {
		close(handlerStarted)
		select {
		case <-r.Context().Done():
		case <-releaseHandler:
		}
		close(handlerDone)
	}).Methods(http.MethodPatch)
	startDone := make(chan error, 1)
	go func() { startDone <- server.Start() }()
	defer func() {
		unblockHandler()
		_ = server.httpServer.Close()
	}()

	client := &http.Client{Timeout: 5 * time.Second}
	address := fmt.Sprintf("http://127.0.0.1:%d", port)
	readyDeadline := time.NewTimer(5 * time.Second)
	poll := time.NewTicker(10 * time.Millisecond)
	defer readyDeadline.Stop()
	defer poll.Stop()
	for {
		select {
		case err := <-startDone:
			t.Fatalf("Start returned before Stop: %v", err)
		case <-readyDeadline.C:
			t.Fatal("HTTP server did not become ready")
		case <-poll.C:
			resp, err := client.Get(address + "/health")
			if err != nil {
				continue
			}
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				goto ready
			}
		}
	}

ready:
	request, err := http.NewRequest(http.MethodPatch, address+"/shutdown-test", nil)
	if err != nil {
		t.Fatal(err)
	}
	requestDone := make(chan error, 1)
	go func() {
		resp, err := client.Do(request)
		if err == nil {
			_ = resp.Body.Close()
		}
		requestDone <- err
	}()
	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("test handler did not start")
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stopErr := server.Stop(drainCtx)
	if !errors.Is(stopErr, context.DeadlineExceeded) {
		t.Errorf("Stop error = %v, want drain deadline", stopErr)
	}
	if !closed(handlerDone) {
		t.Error("Stop returned before the Start-served handler exited")
	}
	select {
	case err := <-startDone:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Start returned %v, want http.ErrServerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Start did not return after Stop")
	}
	unblockHandler()
	select {
	case <-requestDone:
	case <-time.After(5 * time.Second):
		t.Error("request client did not observe completion or abort")
	}
}

func TestStopBeforeStartPreventsHTTPAndHTTPSListen(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		name := "http"
		if useTLS {
			name = "https"
		}
		t.Run(name, func(t *testing.T) {
			server := NewServer(nil, "not a valid bind address", 0, false, 0)
			if useTLS {
				server.SetTLS("missing-cert.pem", "missing-key.pem")
			}
			if err := server.Stop(context.Background()); err != nil {
				t.Fatalf("Stop before Start: %v", err)
			}
			if err := server.Start(); !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("Start after Stop error = %v, want http.ErrServerClosed", err)
			}
		})
	}
}
