package broadcast

import (
	"bytes"
	"context"
	"errors"
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

func TestBroadcasterBroadcastOwnedTransfersAndCopies(t *testing.T) {
	b := NewBroadcaster(DefaultChannelBuffer)
	first := b.Subscribe()
	second := b.Subscribe()
	if first == nil || second == nil {
		t.Fatal("Subscribe failed")
	}
	b.SetHeaders(http.StatusOK, http.Header{})

	const payload = "owned broadcast payload"
	data := GetChunkBuf(len(payload))
	copy(data, payload)
	if !b.BroadcastOwned(data) {
		t.Fatal("BroadcastOwned reported that no listener accepted the data")
	}
	b.Complete(nil)

	firstChunk := <-first.Chunks()
	secondChunk := <-second.Chunks()
	if firstChunk.Err != nil || secondChunk.Err != nil {
		t.Fatalf("unexpected terminal chunk: first=%v second=%v", firstChunk.Err, secondChunk.Err)
	}
	if &firstChunk.Data[0] != &data[0] {
		t.Fatal("first listener did not receive the owned input buffer")
	}
	if &secondChunk.Data[0] == &data[0] {
		t.Fatal("second listener shares the first listener's input buffer")
	}
	firstChunk.Data[0] = 'O'
	if got := secondChunk.Data[0]; got != payload[0] {
		t.Fatalf("second listener data changed with first listener: got %q, want %q", got, payload[0])
	}
	firstChunk.Release()
	secondChunk.Release()
	for chunk := range first.Chunks() {
		chunk.Release()
	}
	for chunk := range second.Chunks() {
		chunk.Release()
	}
}

func TestBroadcasterBroadcastOwnedPreservesInputForLaterListener(t *testing.T) {
	b := NewBroadcaster(1)
	slow := b.Subscribe()
	fast := b.Subscribe()
	if slow == nil || fast == nil {
		t.Fatal("Subscribe failed")
	}
	b.SetHeaders(http.StatusOK, http.Header{})

	// Fill both listeners, then consume only the second one. The first listener
	// must be disconnected without taking ownership of the next input buffer.
	b.Broadcast([]byte("queued"))
	queued := <-fast.Chunks()
	queued.Release()

	const payload = "later listener owns this"
	data := GetChunkBuf(len(payload))
	copy(data, payload)
	if !b.BroadcastOwned(data) {
		t.Fatal("BroadcastOwned did not transfer to the available listener")
	}
	b.Complete(nil)

	owned := <-fast.Chunks()
	if owned.Err != nil {
		t.Fatalf("fast listener got terminal chunk before data: %v", owned.Err)
	}
	if &owned.Data[0] != &data[0] {
		t.Fatal("available listener did not receive the caller-owned buffer")
	}
	if string(owned.Data) != payload {
		t.Fatalf("fast listener data = %q, want %q", owned.Data, payload)
	}
	owned.Release()
	for chunk := range fast.Chunks() {
		chunk.Release()
	}
	for chunk := range slow.Chunks() {
		chunk.Release()
	}
	if got := b.ListenerCount(); got != 1 {
		t.Fatalf("active listener count = %d, want 1", got)
	}
}

func TestBroadcasterBroadcastOwnedReturnsInputWhenUnaccepted(t *testing.T) {
	b := NewBroadcaster(DefaultChannelBuffer)
	const payload = "no listeners"
	data := GetChunkBuf(len(payload))
	copy(data, payload)

	if b.BroadcastOwned(data) {
		t.Fatal("BroadcastOwned reported ownership transfer without a listener")
	}
	if !b.IsStreaming() {
		t.Fatal("BroadcastOwned did not mark the broadcast as streaming")
	}
	if string(data) != payload {
		t.Fatalf("caller-owned data changed: got %q, want %q", data, payload)
	}
	PutChunkBuf(data)
	b.Complete(nil)
}

func TestBroadcasterBroadcastCopiesCallerData(t *testing.T) {
	b := NewBroadcaster(DefaultChannelBuffer)
	listener := b.Subscribe()
	if listener == nil {
		t.Fatal("Subscribe failed")
	}
	b.SetHeaders(http.StatusOK, http.Header{})

	data := []byte("copy before caller reuse")
	b.Broadcast(data)
	data[0] = 'X'
	b.Complete(nil)

	chunk := <-listener.Chunks()
	if chunk.Err != nil {
		t.Fatalf("unexpected terminal chunk: %v", chunk.Err)
	}
	if got := string(chunk.Data); got != "copy before caller reuse" {
		t.Fatalf("listener data = %q, want original caller data", got)
	}
	chunk.Release()
	for chunk := range listener.Chunks() {
		chunk.Release()
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
			chunk.Release()
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
			chunk.Release()
		}
		// The queue was full when the broadcaster disconnected this listener, so
		// there was no room for an error chunk: the terminal error is only
		// recorded on the listener. A nil Err() here is the failure this test
		// is meant to catch.
		slowResult <- struct {
			err          error
			receivedData int
		}{slowListener.Err(), received}
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

	// A slow consumer must receive the terminal error even when its queue was full.
	select {
	case result := <-slowResult:
		if result.err != ErrSlowConsumer {
			t.Errorf("Slow consumer error = %v, want %v", result.err, ErrSlowConsumer)
		}
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

	// WaitForHeaders should return context error before headers arrive.
	_, _, err := listener.WaitForHeaders(ctx)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestBroadcasterHeadersWinOverConcurrentCancellation(t *testing.T) {
	b := NewBroadcaster(DefaultChannelBuffer)
	listener := b.Subscribe()
	if listener == nil {
		t.Fatal("Subscribe returned nil")
	}

	headers := make(http.Header)
	headers.Set("ETag", `"ready"`)
	b.SetHeaders(http.StatusOK, headers)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	status, gotHeaders, err := listener.WaitForHeaders(ctx)
	if err != nil {
		t.Fatalf("WaitForHeaders: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("status = %d, want %d", status, http.StatusOK)
	}
	if got := gotHeaders.Get("ETag"); got != `"ready"` {
		t.Errorf("ETag = %q, want %q", got, `"ready"`)
	}
}

type observedHeaderWaitContext struct {
	done        chan struct{}
	entered     chan struct{}
	cancelOnce  sync.Once
	enteredOnce sync.Once
}

func newObservedHeaderWaitContext() *observedHeaderWaitContext {
	return &observedHeaderWaitContext{done: make(chan struct{}), entered: make(chan struct{})}
}

func (c *observedHeaderWaitContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *observedHeaderWaitContext) Done() <-chan struct{} {
	c.enteredOnce.Do(func() { close(c.entered) })
	return c.done
}

func (c *observedHeaderWaitContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func (c *observedHeaderWaitContext) Value(any) any { return nil }

func (c *observedHeaderWaitContext) cancel() { c.cancelOnce.Do(func() { close(c.done) }) }

func TestBroadcasterCancellationBeforePreHeaderCompletion(t *testing.T) {
	b := NewBroadcaster(DefaultChannelBuffer)
	listener := b.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b.Complete(errors.New("upstream transport failed"))

	_, _, err := listener.WaitForHeaders(ctx)
	if err != context.Canceled {
		t.Errorf("WaitForHeaders error = %v, want context.Canceled after cancellation before completion", err)
	}
}

func TestBroadcasterCompletionBeforeHeadersWakesExistingAndLateListeners(t *testing.T) {
	b := NewBroadcaster(DefaultChannelBuffer)
	first := b.Subscribe()
	second := b.Subscribe()
	if first == nil || second == nil {
		t.Fatal("Subscribe returned nil before streaming")
	}

	upstreamErr := errors.New("upstream transport failed")
	type headerWait struct {
		name   string
		ctx    *observedHeaderWaitContext
		result chan error
	}
	startHeaderWait := func(name string, listener *Listener) headerWait {
		ctx := newObservedHeaderWaitContext()
		t.Cleanup(ctx.cancel)
		result := make(chan error, 1)
		go func() {
			_, _, err := listener.WaitForHeaders(ctx)
			result <- err
		}()
		return headerWait{name: name, ctx: ctx, result: result}
	}
	waitUntilWaiting := func(wait headerWait) {
		t.Helper()
		select {
		case <-wait.ctx.entered:
		case <-time.After(time.Second):
			wait.ctx.cancel()
			t.Fatalf("%s WaitForHeaders did not start", wait.name)
		}
	}
	waitForPreHeaderError := func(wait headerWait) {
		t.Helper()
		select {
		case err := <-wait.result:
			if err != upstreamErr {
				t.Errorf("%s WaitForHeaders error = %v, want original error %v", wait.name, err, upstreamErr)
			}
			if wait.ctx.Err() != nil {
				t.Errorf("%s context was canceled before WaitForHeaders returned", wait.name)
			}
		case <-time.After(time.Second):
			if wait.ctx.Err() != nil {
				t.Errorf("%s context was canceled before the bounded wait completed", wait.name)
			}
			wait.ctx.cancel()
			if err := <-wait.result; !errors.Is(err, context.Canceled) {
				t.Errorf("%s cleanup WaitForHeaders error = %v, want context.Canceled", wait.name, err)
			}
			t.Errorf("%s WaitForHeaders did not return the upstream error while its context remained live", wait.name)
		}
		wait.ctx.cancel()
	}

	firstWait := startHeaderWait("first", first)
	secondWait := startHeaderWait("second", second)
	waitUntilWaiting(firstWait)
	waitUntilWaiting(secondWait)
	b.Complete(upstreamErr)

	for i, listener := range []*Listener{first, second} {
		chunk, ok := <-listener.Chunks()
		if !ok {
			t.Fatalf("listener %d chunk channel closed without the completion error", i)
		}
		if chunk.Err != upstreamErr {
			t.Errorf("listener %d chunk error = %v, want original error %v", i, chunk.Err, upstreamErr)
		}
		if _, ok := <-listener.Chunks(); ok {
			t.Errorf("listener %d chunk channel remained open after completion", i)
		}
	}

	waitForPreHeaderError(firstWait)
	waitForPreHeaderError(secondWait)

	late := b.Subscribe()
	if late == nil {
		t.Fatal("Subscribe returned nil after an unstreamed completion")
	}
	lateWait := startHeaderWait("late", late)
	waitForPreHeaderError(lateWait)
	select {
	case chunk, ok := <-late.Chunks():
		if !ok || chunk.Err != upstreamErr {
			t.Errorf("late listener chunk = %#v, open=%v, want the original completion error", chunk, ok)
		}
		if _, ok := <-late.Chunks(); ok {
			t.Error("late listener chunk channel remained open after completion")
		}
	case <-time.After(time.Second):
		t.Error("late listener did not receive the completed broadcast's terminal chunk")
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

// A slow-consumer disconnect against a full queue must never cost the consumer
// a data chunk: every chunk that was queued is delivered, in order, and the
// disconnect is then surfaced via Err(). The old behaviour displaced the OLDEST
// queued chunk to make room for the error chunk, which handed the consumer
// chunk k+1 at chunk k's offset (a hole in the body) before it learned anything
// was wrong.
func TestBroadcasterSlowConsumerDeliversQueuedChunksInOrder(t *testing.T) {
	const buf = 4
	b := NewBroadcaster(buf)
	l := b.Subscribe()
	if l == nil {
		t.Fatal("Subscribe failed")
	}
	b.SetHeaders(http.StatusOK, http.Header{})

	// Fill the queue exactly, then overflow it by one: the listener is now
	// disconnected with a full buffer and no room for an error chunk.
	for i := 0; i < buf+1; i++ {
		b.Broadcast([]byte{byte(i)})
	}
	if n := b.ListenerCount(); n != 0 {
		t.Fatalf("ListenerCount = %d after overflow, want 0 (listener disconnected)", n)
	}

	var got []byte
	var errChunk error
	for chunk := range l.Chunks() {
		if chunk.Err != nil {
			errChunk = chunk.Err
			continue
		}
		got = append(got, chunk.Data...)
		chunk.Release()
	}

	want := []byte{0, 1, 2, 3}
	if !bytes.Equal(got, want) {
		t.Fatalf("queued chunks delivered = %v, want %v (no hole, no reorder)", got, want)
	}
	if errChunk != nil {
		t.Fatalf("unexpected error chunk %v: the queue was full, so none could be sent", errChunk)
	}
	if err := l.Err(); err != ErrSlowConsumer {
		t.Fatalf("Err() = %v, want %v", err, ErrSlowConsumer)
	}

	// Completing afterwards must not resurrect or re-close the disconnected listener.
	b.Complete(nil)
	if err := l.Err(); err != ErrSlowConsumer {
		t.Fatalf("Err() after Complete = %v, want %v", err, ErrSlowConsumer)
	}
}

// A failed completion against a full queue keeps every queued chunk and
// surfaces the completion error via Err(); when the queue has room the error
// also arrives as a chunk. A clean completion leaves Err() nil either way.
func TestBroadcasterCompleteErrorSurvivesFullQueue(t *testing.T) {
	upstreamErr := errors.New("upstream cut")

	t.Run("full queue", func(t *testing.T) {
		b := NewBroadcaster(2)
		l := b.Subscribe()
		b.SetHeaders(http.StatusOK, http.Header{})
		b.Broadcast([]byte("a"))
		b.Broadcast([]byte("b"))
		b.Complete(upstreamErr)

		var got []byte
		var errChunk error
		for chunk := range l.Chunks() {
			if chunk.Err != nil {
				errChunk = chunk.Err
				continue
			}
			got = append(got, chunk.Data...)
			chunk.Release()
		}
		if string(got) != "ab" {
			t.Fatalf("data = %q, want %q", got, "ab")
		}
		if errChunk != nil {
			t.Fatalf("unexpected error chunk %v on a full queue", errChunk)
		}
		if err := l.Err(); err != upstreamErr {
			t.Fatalf("Err() = %v, want %v", err, upstreamErr)
		}
	})

	t.Run("queue has room", func(t *testing.T) {
		b := NewBroadcaster(4)
		l := b.Subscribe()
		b.SetHeaders(http.StatusOK, http.Header{})
		b.Broadcast([]byte("a"))
		b.Complete(upstreamErr)

		var got []byte
		var errChunk error
		for chunk := range l.Chunks() {
			if chunk.Err != nil {
				errChunk = chunk.Err
				continue
			}
			got = append(got, chunk.Data...)
			chunk.Release()
		}
		if string(got) != "a" {
			t.Fatalf("data = %q, want %q", got, "a")
		}
		if errChunk != upstreamErr {
			t.Fatalf("error chunk = %v, want %v", errChunk, upstreamErr)
		}
		if err := l.Err(); err != upstreamErr {
			t.Fatalf("Err() = %v, want %v", err, upstreamErr)
		}
	})

	t.Run("clean completion", func(t *testing.T) {
		b := NewBroadcaster(2)
		l := b.Subscribe()
		b.SetHeaders(http.StatusOK, http.Header{})
		b.Broadcast([]byte("a"))
		b.Broadcast([]byte("b"))
		b.Complete(nil)

		var got []byte
		for chunk := range l.Chunks() {
			if chunk.Err != nil {
				t.Fatalf("unexpected error chunk %v", chunk.Err)
			}
			got = append(got, chunk.Data...)
			chunk.Release()
		}
		if string(got) != "ab" {
			t.Fatalf("data = %q, want %q", got, "ab")
		}
		if err := l.Err(); err != nil {
			t.Fatalf("Err() = %v, want nil", err)
		}
	})
}

// An owned chunk that overflows the only candidate owner's queue is never
// queued: BroadcastOwned reports no transfer, the caller keeps the buffer, and
// the disconnected listener still drains every chunk it already owns in order.
// This pins the merge of ownership transfer with closeWithError: the disconnect
// path neither displaces a queued chunk nor releases a buffer it does not own.
func TestBroadcasterBroadcastOwnedSlowConsumerKeepsCallerBuffer(t *testing.T) {
	const buf = 4
	b := NewBroadcaster(buf)
	l := b.Subscribe()
	if l == nil {
		t.Fatal("Subscribe failed")
	}
	b.SetHeaders(http.StatusOK, http.Header{})

	for i := 0; i < buf; i++ {
		data := GetChunkBuf(1)
		data[0] = byte(i)
		if !b.BroadcastOwned(data) {
			t.Fatalf("BroadcastOwned(%d) did not transfer ownership with room in the queue", i)
		}
	}

	overflow := GetChunkBuf(1)
	overflow[0] = 0xff
	if b.BroadcastOwned(overflow) {
		t.Fatal("BroadcastOwned reported a transfer into a full queue")
	}
	if n := b.ListenerCount(); n != 0 {
		t.Fatalf("ListenerCount = %d after overflow, want 0 (listener disconnected)", n)
	}
	if overflow[0] != 0xff {
		t.Fatalf("caller-owned overflow buffer changed: got %#x, want 0xff", overflow[0])
	}
	// The caller still owns the rejected buffer and releases it exactly once.
	PutChunkBuf(overflow)

	var got []byte
	var errChunk error
	for chunk := range l.Chunks() {
		if chunk.Err != nil {
			errChunk = chunk.Err
			continue
		}
		got = append(got, chunk.Data...)
		chunk.Release()
	}
	want := []byte{0, 1, 2, 3}
	if !bytes.Equal(got, want) {
		t.Fatalf("queued owned chunks delivered = %v, want %v (no hole, no reorder)", got, want)
	}
	if errChunk != nil {
		t.Fatalf("unexpected error chunk %v: the queue was full, so none could be sent", errChunk)
	}
	if err := l.Err(); err != ErrSlowConsumer {
		t.Fatalf("Err() = %v, want %v", err, ErrSlowConsumer)
	}

	b.Complete(nil)
	if err := l.Err(); err != ErrSlowConsumer {
		t.Fatalf("Err() after Complete = %v, want %v", err, ErrSlowConsumer)
	}
}
