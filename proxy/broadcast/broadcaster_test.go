package broadcast

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBroadcasterBasicStreaming(t *testing.T) {
	b := NewBroadcaster(DefaultChannelBuffer)

	// Subscribe a listener
	listener := b.Subscribe()
	if listener == nil {
		t.Fatal("Subscribe returned nil before streaming started")
	}

	// Set headers
	headers := http.Header{}
	headers.Set("Content-Type", "application/octet-stream")
	b.SetHeaders(http.StatusOK, headers)

	// Broadcast some data
	testData := []byte("hello world")
	go func() {
		b.Broadcast(testData)
		b.Complete(nil)
	}()

	// Receive headers
	ctx := context.Background()
	status, h, err := listener.WaitForHeaders(ctx)
	if err != nil {
		t.Fatalf("WaitForHeaders failed: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("Expected status 200, got %d", status)
	}
	if h.Get("Content-Type") != "application/octet-stream" {
		t.Errorf("Expected Content-Type header, got %s", h.Get("Content-Type"))
	}

	// Receive chunks
	var received []byte
	for chunk := range listener.Chunks() {
		if chunk.Err != nil {
			break
		}
		received = append(received, chunk.Data...)
	}

	if string(received) != string(testData) {
		t.Errorf("Expected %q, got %q", testData, received)
	}
}

func TestBroadcasterMultipleListeners(t *testing.T) {
	b := NewBroadcaster(DefaultChannelBuffer)

	// Subscribe multiple listeners
	numListeners := 10
	listeners := make([]*Listener, numListeners)
	for i := 0; i < numListeners; i++ {
		listeners[i] = b.Subscribe()
		if listeners[i] == nil {
			t.Fatalf("Subscribe returned nil for listener %d", i)
		}
	}

	// Set headers and broadcast data
	headers := http.Header{}
	b.SetHeaders(http.StatusOK, headers)

	testData := []byte("test data for multiple listeners")
	go func() {
		b.Broadcast(testData)
		b.Complete(nil)
	}()

	// All listeners should receive the same data
	var wg sync.WaitGroup
	results := make([][]byte, numListeners)

	for i, l := range listeners {
		wg.Add(1)
		go func(idx int, listener *Listener) {
			defer wg.Done()
			ctx := context.Background()
			_, _, err := listener.WaitForHeaders(ctx)
			if err != nil {
				t.Errorf("Listener %d: WaitForHeaders failed: %v", idx, err)
				return
			}

			for chunk := range listener.Chunks() {
				if chunk.Err != nil {
					break
				}
				results[idx] = append(results[idx], chunk.Data...)
			}
		}(i, l)
	}

	wg.Wait()

	// Verify all listeners got the same data
	for i, result := range results {
		if string(result) != string(testData) {
			t.Errorf("Listener %d: expected %q, got %q", i, testData, result)
		}
	}
}

func TestBroadcasterNoLateJoiners(t *testing.T) {
	b := NewBroadcaster(DefaultChannelBuffer)

	// Subscribe initial listener
	listener1 := b.Subscribe()
	if listener1 == nil {
		t.Fatal("Initial subscribe failed")
	}

	// Set headers
	b.SetHeaders(http.StatusOK, http.Header{})

	// Start streaming (this marks streaming as started)
	b.Broadcast([]byte("first chunk"))

	// Try to subscribe after streaming started - should fail
	listener2 := b.Subscribe()
	if listener2 != nil {
		t.Error("Subscribe should return nil after streaming started")
	}

	// Verify IsStreaming
	if !b.IsStreaming() {
		t.Error("IsStreaming should return true after Broadcast")
	}

	b.Complete(nil)
}

func TestBroadcasterSlowConsumerDisconnect(t *testing.T) {
	// Use a very small buffer to easily trigger slow consumer
	b := NewBroadcaster(2)

	// Subscribe listeners
	fastListener := b.Subscribe()
	slowListener := b.Subscribe()

	b.SetHeaders(http.StatusOK, http.Header{})

	// Fast listener consumes data immediately
	var fastReceived []byte
	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		ctx := context.Background()
		fastListener.WaitForHeaders(ctx)
		for chunk := range fastListener.Chunks() {
			if chunk.Err != nil {
				return
			}
			fastReceived = append(fastReceived, chunk.Data...)
		}
	}()

	// Slow listener doesn't consume at all until we signal - truly blocked
	slowReadyToReceive := make(chan struct{})
	slowResult := make(chan struct {
		err          error
		receivedData int
	}, 1)
	go func() {
		ctx := context.Background()
		slowListener.WaitForHeaders(ctx)
		// Wait for signal before consuming anything
		<-slowReadyToReceive
		var received int
		for chunk := range slowListener.Chunks() {
			if chunk.Err != nil {
				slowResult <- struct {
					err          error
					receivedData int
				}{chunk.Err, received}
				return
			}
			received += len(chunk.Data)
		}
		// Channel closed without error chunk (buffer was full)
		slowResult <- struct {
			err          error
			receivedData int
		}{nil, received}
	}()

	// Broadcast many chunks quickly to overflow slow consumer's buffer
	// Buffer is 2, so after 2 chunks the slow consumer will be disconnected
	for i := 0; i < 10; i++ {
		b.Broadcast([]byte("chunk"))
	}
	b.Complete(nil)

	// Now let slow listener try to read
	close(slowReadyToReceive)

	<-fastDone

	// Slow consumer should either:
	// 1. Get ErrSlowConsumer if error chunk was sent before buffer filled
	// 2. Get channel closed with partial data (buffer was completely full when disconnected)
	select {
	case result := <-slowResult:
		if result.err != nil && result.err != ErrSlowConsumer {
			t.Errorf("Slow consumer got unexpected error: %v", result.err)
		}
		// Either ErrSlowConsumer or nil (channel closed) is acceptable
		// The important thing is that slow consumer was disconnected (received less data than fast)
		t.Logf("Slow consumer received %d bytes, err=%v", result.receivedData, result.err)
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for slow consumer to finish")
	}

	// Fast consumer should have received all data
	if len(fastReceived) == 0 {
		t.Error("Fast consumer received no data")
	}

	// Verify broadcaster reports slow consumer was disconnected
	// (listener count should be reduced after disconnect)
	if b.ListenerCount() > 1 {
		t.Errorf("Expected listener count <= 1 after slow consumer disconnect, got %d", b.ListenerCount())
	}
}

func TestBroadcasterContextCancellation(t *testing.T) {
	b := NewBroadcaster(DefaultChannelBuffer)

	listener := b.Subscribe()

	// Create a cancelable context
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	// WaitForHeaders should return context error
	_, _, err := listener.WaitForHeaders(ctx)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestManagerGetOrCreate(t *testing.T) {
	m := NewManager(DefaultChannelBuffer)

	// First call should create new broadcaster
	b1, isFirst := m.GetOrCreate("key1")
	if !isFirst {
		t.Error("First call should return isFirst=true")
	}
	if b1 == nil {
		t.Fatal("First call should return broadcaster")
	}

	// Second call with same key should return same broadcaster
	b2, isFirst := m.GetOrCreate("key1")
	if isFirst {
		t.Error("Second call should return isFirst=false")
	}
	if b2 != b1 {
		t.Error("Second call should return same broadcaster")
	}

	// Different key should create new broadcaster
	b3, isFirst := m.GetOrCreate("key2")
	if !isFirst {
		t.Error("New key should return isFirst=true")
	}
	if b3 == b1 {
		t.Error("New key should return different broadcaster")
	}
}

func TestManagerRemove(t *testing.T) {
	m := NewManager(DefaultChannelBuffer)

	// Create and remove
	b1, _ := m.GetOrCreate("key1")
	m.Remove("key1")

	// Should create new broadcaster after remove
	b2, isFirst := m.GetOrCreate("key1")
	if !isFirst {
		t.Error("After remove, should return isFirst=true")
	}
	if b2 == b1 {
		t.Error("After remove, should return new broadcaster")
	}
}

func TestManagerActiveCount(t *testing.T) {
	m := NewManager(DefaultChannelBuffer)

	if m.ActiveCount() != 0 {
		t.Errorf("Expected 0 active, got %d", m.ActiveCount())
	}

	m.GetOrCreate("key1")
	if m.ActiveCount() != 1 {
		t.Errorf("Expected 1 active, got %d", m.ActiveCount())
	}

	m.GetOrCreate("key2")
	if m.ActiveCount() != 2 {
		t.Errorf("Expected 2 active, got %d", m.ActiveCount())
	}

	m.Remove("key1")
	if m.ActiveCount() != 1 {
		t.Errorf("Expected 1 active after remove, got %d", m.ActiveCount())
	}
}

// TestBroadcasterCompleteWithSlowConsumer tests that Complete() and Broadcast()
// work correctly when there are slow consumers that get disconnected.
func TestBroadcasterCompleteWithSlowConsumer(t *testing.T) {
	// Use small buffer to easily trigger slow consumer disconnect
	b := NewBroadcaster(1)

	// Subscribe a slow listener that never reads
	slowListener := b.Subscribe()
	_ = slowListener // We won't read from this one

	b.SetHeaders(http.StatusOK, http.Header{})

	// Fill the buffer
	b.Broadcast([]byte("chunk1"))

	// This should disconnect the slow consumer immediately (buffer full)
	b.Broadcast([]byte("chunk2"))

	// Complete should work fine even with disconnected consumers
	b.Complete(nil)

	// Verify slow consumer was disconnected
	if b.ListenerCount() != 0 {
		t.Errorf("Expected 0 listeners after slow consumer disconnect, got %d", b.ListenerCount())
	}
}

func TestConcurrentRequestsCoalescing(t *testing.T) {
	m := NewManager(DefaultChannelBuffer)

	key := "test/object"
	numRequests := 100
	var fetchCount int32

	// Two-phase barrier:
	// 1. startBarrier: all goroutines block until released together
	// 2. subscribedWg: fetcher waits until all listeners have subscribed
	startBarrier := make(chan struct{})
	var subscribedWg sync.WaitGroup
	// We expect numRequests-1 subscribers (1 goroutine becomes the fetcher).
	// Pre-add the count; the fetcher will adjust if needed.
	subscribedWg.Add(numRequests - 1)

	var wg sync.WaitGroup
	var subscribedCount int32

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Wait for all goroutines to be ready
			<-startBarrier

			b, isFirst := m.GetOrCreate(key)
			if isFirst {
				// I'm the fetcher - wait for all others to subscribe before broadcasting
				atomic.AddInt32(&fetchCount, 1)
				defer m.Remove(key)

				// Wait for all listeners to subscribe
				subscribedWg.Wait()

				// Simulate fetching with some delay
				b.SetHeaders(http.StatusOK, http.Header{})
				b.Broadcast([]byte("test data"))
				b.Complete(nil)
			} else {
				// I'm a listener
				listener := b.Subscribe()
				atomic.AddInt32(&subscribedCount, 1)
				subscribedWg.Done()
				if listener == nil {
					// Late joiner - would need own fetch in real code
					return
				}

				ctx := context.Background()
				listener.WaitForHeaders(ctx)
				for chunk := range listener.Chunks() {
					if chunk.Err != nil {
						break
					}
				}
			}
		}()
	}

	// Start all goroutines at once
	close(startBarrier)

	wg.Wait()

	// Should have exactly 1 fetch since we controlled timing
	if fetchCount != 1 {
		t.Errorf("Expected exactly 1 fetch with controlled timing, got %d", fetchCount)
	}

	t.Logf("Coalesced %d requests into %d fetches (%d subscribed)", numRequests, fetchCount, subscribedCount)
}
