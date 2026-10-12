package handlers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

type routedBodyReadGate struct {
	io.ReadCloser
	stopAfter int64
	read      int64
	blocked   chan struct{}
	release   <-chan struct{}
	once      sync.Once
}

func (r *routedBodyReadGate) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.read >= r.stopAfter {
		r.once.Do(func() { close(r.blocked) })
		<-r.release
	} else if int64(len(p)) > r.stopAfter-r.read {
		p = p[:r.stopAfter-r.read]
	}
	n, err := r.ReadCloser.Read(p)
	r.read += int64(n)
	return n, err
}

type routedCommitWriter struct {
	http.ResponseWriter
	committed chan int
	once      sync.Once
}

func (w *routedCommitWriter) WriteHeader(status int) {
	w.once.Do(func() { w.committed <- status })
	w.ResponseWriter.WriteHeader(status)
}

func (w *routedCommitWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { w.committed <- http.StatusOK })
	return w.ResponseWriter.Write(p)
}

func (w *routedCommitWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *routedCommitWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestRoutedAWSChunkedExtentWaitsForEarlyUpstreamResponse(t *testing.T) {
	for _, tc := range []struct {
		name           string
		overrun        bool
		warmOnWrite    bool
		firstChunkSize int
	}{
		{name: "warm-off overrun", overrun: true, firstChunkSize: 1 << 20},
		{name: "warm-off valid control", firstChunkSize: 1 << 20},
		{name: "warm-on tee overrun", overrun: true, warmOnWrite: true, firstChunkSize: 8 * 1024},
		{name: "warm-on tee valid control", warmOnWrite: true, firstChunkSize: 8 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runRoutedEarlyChunkResponse(t, tc.overrun, tc.warmOnWrite, tc.firstChunkSize)
		})
	}
}

func runRoutedEarlyChunkResponse(t *testing.T, overrun, warmOnWrite bool, firstChunkSize int) {
	chunkHeader := fmt.Sprintf("%x\r\n", firstChunkSize)
	secondChunk := "4\r\nPASS\r\n"
	if overrun {
		secondChunk = "5\r\nFAIL!\r\n"
	}
	wireBody := chunkHeader + strings.Repeat("A", firstChunkSize) + "\r\n" + secondChunk + "0\r\n\r\n"
	decodedLength := int64(firstChunkSize + 4)

	bodyBlocked := make(chan struct{})
	releaseBody := make(chan struct{})
	var releaseBodyOnce sync.Once
	defer releaseBodyOnce.Do(func() { close(releaseBody) })

	prefixRead := make(chan routedChunkExtentObservation, 1)
	upstreamMethods := make(chan string, 8)
	allowUpstreamResponse := make(chan struct{})
	var allowResponseOnce sync.Once
	defer allowResponseOnce.Do(func() { close(allowUpstreamResponse) })
	upstreamResponded := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamMethods <- r.Method
		if r.Method == http.MethodHead {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Length", fmt.Sprint(decodedLength))
			w.Header().Set("ETag", `"early-etag"`)
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var prefix [1]byte
		n, readErr := io.ReadFull(r.Body, prefix[:])
		prefixRead <- routedChunkExtentObservation{
			body:          append([]byte(nil), prefix[:n]...),
			bodyReadErr:   readErr,
			contentLength: r.ContentLength,
		}
		<-allowUpstreamResponse
		w.Header().Set("Connection", "close")
		w.Header().Set("ETag", `"early-etag"`)
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		_, _ = io.WriteString(w, "stored")
		close(upstreamResponded)
	}))
	t.Cleanup(upstream.Close)

	cfg := config.NewDefault()
	cfg.Mode = config.ModeSigning
	cfg.Upstream.Endpoint = upstream.URL
	cfg.Upstream.Region = routedChunkExtentRegion
	cfg.Cache.WarmOnWrite = warmOnWrite
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(routedChunkExtentAccessKey, routedChunkExtentSecretKey)
	forwarder := proxy.NewForwarder(credentials, cfg.Upstream.Endpoint, cfg.Upstream.Region, 1, nil, nil)
	gatewayCache := cache.NewDisabledCache()
	if warmOnWrite {
		gatewayCache = cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	}
	service := proxy.NewService(forwarder, gatewayCache, cfg)
	router := NewServer(service, "127.0.0.1", 0, false, 0).Router()
	committed := make(chan int, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = &routedBodyReadGate{
			ReadCloser: r.Body,
			stopAfter:  int64(len(chunkHeader) + firstChunkSize/2),
			blocked:    bodyBlocked,
			release:    releaseBody,
		}
		router.ServeHTTP(&routedCommitWriter{ResponseWriter: w, committed: committed}, r)
	}))
	t.Cleanup(gateway.Close)
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)

	headers := make(http.Header)
	headers.Set("Content-Encoding", "aws-chunked")
	headers.Set("X-Amz-Content-Sha256", routedUnsignedChunkHash)
	headers.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(decodedLength))
	request, err := auth.NewRequestSigner(gateway.URL, routedChunkExtentRegion).SignRequest(
		context.Background(),
		http.MethodPut,
		"/bucket/object",
		strings.NewReader(wireBody),
		routedUnsignedChunkHash,
		routedChunkExtentAccessKey,
		routedChunkExtentSecretKey,
		headers,
	)
	if err != nil {
		t.Fatal(err)
	}
	if request.ContentLength != int64(len(wireBody)) {
		t.Fatalf("incoming Content-Length = %d, want complete wire length %d", request.ContentLength, len(wireBody))
	}
	if _, err := auth.NewRequestValidator(credentials).ValidateRequest(request); err != nil {
		t.Fatalf("incoming SigV4 request did not validate: %v", err)
	}
	responseDone := make(chan routedChunkExtentResponse, 1)
	go func() {
		response, err := client.Do(request)
		if err != nil {
			responseDone <- routedChunkExtentResponse{err: err}
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		responseDone <- routedChunkExtentResponse{status: response.StatusCode, body: body, err: err}
	}()

	// Do not wait for the client-side WroteRequest callback here. The gateway
	// intentionally stops reading the body at this gate, so a large request can
	// fill TCP buffers and prevent that callback until the gate is released.
	select {
	case <-bodyBlocked:
	case <-time.After(3 * time.Second):
		t.Fatal("gateway did not reach the controlled body-read gate")
	}
	select {
	case observation := <-prefixRead:
		if observation.bodyReadErr != nil || string(observation.body) != "A" {
			t.Fatalf("upstream prefix observation = body %q, err %v; want one valid-frame byte", observation.body, observation.bodyReadErr)
		}
		if observation.contentLength != decodedLength {
			t.Fatalf("upstream content length = %d, want %d", observation.contentLength, decodedLength)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upstream did not read a prefix of the valid first frame")
	}

	allowResponseOnce.Do(func() { close(allowUpstreamResponse) })
	select {
	case <-upstreamResponded:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream did not send its early 2xx response")
	}

	var committedEarly bool
	select {
	case status := <-committed:
		committedEarly = status >= http.StatusOK && status < http.StatusMultipleChoices
	case <-time.After(250 * time.Millisecond):
	}
	releaseBodyOnce.Do(func() { close(releaseBody) })

	select {
	case response := <-responseDone:
		if response.err != nil {
			t.Fatalf("reading routed response: %v", response.err)
		}
		if overrun {
			if committedEarly || (response.status >= http.StatusOK && response.status < http.StatusMultipleChoices) {
				t.Fatalf("early upstream response made the overrun PUT successful: warm-on-write=%t early-commit=%t final-status=%d body=%q", warmOnWrite, committedEarly, response.status, response.body)
			}
			if warmOnWrite {
				methods := make([]string, 0, 2)
				for {
					select {
					case method := <-upstreamMethods:
						methods = append(methods, method)
					default:
						if len(methods) != 1 || methods[0] != http.MethodPut {
							t.Fatalf("overrun triggered unexpected upstream/cache requests: %v", methods)
						}
						return
					}
				}
			}
		} else if response.status < http.StatusOK || response.status >= http.StatusMultipleChoices {
			t.Fatalf("early upstream response for a valid body returned status %d, body=%q", response.status, response.body)
		} else if warmOnWrite {
			headObserved := false
			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			for !headObserved {
				select {
				case method := <-upstreamMethods:
					headObserved = method == http.MethodHead
					if method != http.MethodPut && method != http.MethodHead {
						t.Fatalf("valid early write-through sent unexpected upstream method %q", method)
					}
				case <-timer.C:
					t.Fatal("valid early write-through did not perform its authoritative HEAD")
				}
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("gateway did not finish the early-response upload")
	}
}
