package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

const broadcastBenchmarkObjectSize = 4 << 20

type recordedReadBody struct {
	reader       *bytes.Reader
	mu           sync.Mutex
	sourceBuffer []*byte
}

func newRecordedReadBody(body []byte) *recordedReadBody {
	return &recordedReadBody{
		reader:       bytes.NewReader(body),
		sourceBuffer: make([]*byte, 0, broadcastBenchmarkObjectSize/broadcast.DefaultChunkSize+1),
	}
}

func (b *recordedReadBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if n > 0 {
		b.mu.Lock()
		b.sourceBuffer = append(b.sourceBuffer, &p[0])
		b.mu.Unlock()
	}
	return n, err
}

func (b *recordedReadBody) Close() error { return nil }

func (b *recordedReadBody) sourceBufferAt(index int) *byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	if index < 0 || index >= len(b.sourceBuffer) {
		return nil
	}
	return b.sourceBuffer[index]
}

type broadcastBenchmarkResult struct {
	err        error
	cloneBytes int64
}

// runBroadcastFullGetBenchmark measures the cache-miss full-GET path through a
// real HTTP server. The upstream response is a fresh reader over one immutable
// object so both cache-disabled and cache-listener arms consume the same body.
func runBroadcastFullGetBenchmark(b *testing.B, cacheEnabled bool) {
	b.Helper()

	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	body := bytes.Repeat([]byte{'b'}, broadcastBenchmarkObjectSize)
	forwarder := &mockForwarder{
		doRequestFunc: func(_ context.Context, _ *http.Request, _, _ string) (*http.Response, error) {
			headers := make(http.Header)
			headers.Set("Content-Length", strconv.Itoa(len(body)))
			headers.Set("Content-Type", "application/octet-stream")
			headers.Set("ETag", `"broadcast-benchmark"`)
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        headers,
				Body:          io.NopCloser(bytes.NewReader(body)),
				ContentLength: int64(len(body)),
			}, nil
		},
	}
	svc, cacheStore := newTestService(forwarder, cacheEnabled)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := svc.HandleGetObject(w, r); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
		}
	}))
	client := &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: 1},
		Timeout:   30 * time.Second,
	}
	b.Cleanup(func() {
		client.CloseIdleConnections()
		server.Close()
	})

	const bucket = "broadcast-benchmark"
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		key := strconv.Itoa(i)
		resp, err := client.Get(server.URL + "/" + bucket + "/" + key)
		if err != nil {
			b.Fatalf("GET %s: %v", key, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			b.Fatalf("GET %s status = %d, want %d", key, resp.StatusCode, http.StatusOK)
		}
		got, readErr := io.Copy(io.Discard, resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			b.Fatalf("read %s: %v", key, readErr)
		}
		if closeErr != nil {
			b.Fatalf("close %s: %v", key, closeErr)
		}
		if got != int64(len(body)) {
			b.Fatalf("GET %s body bytes = %d, want %d", key, got, len(body))
		}

		if cacheEnabled {
			// Keep cache population and eviction outside the measured request. The
			// wait also proves that the cache listener consumed the complete body.
			b.StopTimer()
			waitForCacheMeta(b, cacheStore, bucket, key, 30*time.Second)
			if err := cacheStore.Delete(context.Background(), bucket, key); err != nil {
				b.Fatalf("delete cached %s: %v", key, err)
			}
			b.StartTimer()
		}
	}
}

// runBroadcastStreamBenchmark measures the same upstream streaming loop without
// network and cache-writer work. It keeps all listeners subscribed before the
// first read so the second arm is a pre-stream coalesced listener.
func runBroadcastStreamBenchmark(b *testing.B, listenerCount int) {
	b.Helper()

	body := bytes.Repeat([]byte{'b'}, broadcastBenchmarkObjectSize)
	bodyCh := make(chan *recordedReadBody, 1)
	forwarder := &mockForwarder{
		doRequestFunc: func(_ context.Context, _ *http.Request, _, _ string) (*http.Response, error) {
			streamBody := <-bodyCh
			headers := make(http.Header)
			headers.Set("Content-Length", strconv.Itoa(len(body)))
			headers.Set("Content-Type", "application/octet-stream")
			headers.Set("ETag", `"broadcast-stream-benchmark"`)
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        headers,
				Body:          streamBody,
				ContentLength: int64(len(body)),
			}, nil
		},
	}
	svc, _ := newTestService(forwarder, false)
	request := httptest.NewRequest(http.MethodGet, "http://example.test/bucket/key", nil)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	var totalCloneBytes int64
	for b.Loop() {
		streamBody := newRecordedReadBody(body)
		bodyCh <- streamBody

		broadcaster := broadcast.NewBroadcaster(broadcast.DefaultChannelBuffer)
		listeners := make([]*broadcast.Listener, listenerCount)
		for i := range listeners {
			listeners[i] = broadcaster.Subscribe()
			if listeners[i] == nil {
				b.Fatal("Subscribe returned nil before streaming started")
			}
		}

		results := make(chan broadcastBenchmarkResult, listenerCount)
		for i, listener := range listeners {
			go func(i int, listener *broadcast.Listener, streamBody *recordedReadBody) {
				result := broadcastBenchmarkResult{}
				if _, _, err := listener.WaitForHeaders(context.Background()); err != nil {
					result.err = fmt.Errorf("listener %d headers: %w", i, err)
					results <- result
					return
				}
				var got int64
				readIndex := 0
				for chunk := range listener.Chunks() {
					if chunk.Err != nil {
						result.err = fmt.Errorf("listener %d: %w", i, chunk.Err)
						results <- result
						return
					}
					if len(chunk.Data) > 0 {
						got += int64(len(chunk.Data))
						if &chunk.Data[0] != streamBody.sourceBufferAt(readIndex) {
							result.cloneBytes += int64(len(chunk.Data))
						}
						readIndex++
					}
					chunk.Release()
				}
				if got != int64(len(body)) {
					result.err = fmt.Errorf("listener %d body bytes = %d, want %d", i, got, len(body))
				}
				results <- result
			}(i, listener, streamBody)
		}

		if err := svc.streamFromUpstream(context.Background(), request, "bucket", "key", "access", "secret", broadcaster); err != nil {
			b.Fatalf("streamFromUpstream: %v", err)
		}
		broadcaster.Complete(nil)
		for range listeners {
			result := <-results
			if result.err != nil {
				b.Fatal(result.err)
			}
			totalCloneBytes += result.cloneBytes
		}
	}

	if b.N > 0 {
		cloneBytesPerOp := float64(totalCloneBytes) / float64(b.N)
		// Broadcast's listener clones are made by copy(chunk.Data, data), the
		// source-level memmove under test. Upstream Read's fill is excluded.
		b.ReportMetric(cloneBytesPerOp, "clone-bytes/op")
		b.ReportMetric(cloneBytesPerOp, "memmove-bytes/op")
	}
}

// BenchmarkBroadcastFullGetSingleClient measures a full-object cache miss with
// only the fetching client listening to the broadcast.
func BenchmarkBroadcastFullGetSingleClient(b *testing.B) {
	runBroadcastFullGetBenchmark(b, false)
}

// BenchmarkBroadcastStreamSingleListener measures the upstream streaming loop
// with only the fetching listener.
func BenchmarkBroadcastStreamSingleListener(b *testing.B) {
	runBroadcastStreamBenchmark(b, 1)
}

// BenchmarkBroadcastStreamCoalescedListeners measures the upstream streaming
// loop with one pre-stream coalesced listener.
func BenchmarkBroadcastStreamCoalescedListeners(b *testing.B) {
	runBroadcastStreamBenchmark(b, 2)
}

// BenchmarkBroadcastFullGetCacheListener measures a full-object cache miss with
// the inline cache writer subscribed before body streaming begins.
func BenchmarkBroadcastFullGetCacheListener(b *testing.B) {
	runBroadcastFullGetBenchmark(b, true)
}
