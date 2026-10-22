// Package broadcast provides streaming request coalescing for concurrent requests.
// It implements a Varnish-style broadcaster pattern that streams bytes to ALL
// waiting clients simultaneously as they arrive from upstream.
package broadcast

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/rs/zerolog/log"
)

// chunkPool provides reusable byte buffers for broadcast chunk data.
// Eliminates per-chunk allocations (make+copy) that cause GC pressure
// with large objects (e.g., 4MB object = 64 chunks × 64KB = 192+ allocs).
var chunkPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, DefaultChunkSize)
		return &b
	},
}

// GetChunkBuf returns a byte slice of exactly the given size from the pool.
// If size exceeds DefaultChunkSize, a fresh allocation is returned (not pooled).
// Returns nil for zero or negative sizes.
func GetChunkBuf(size int) []byte {
	if size <= 0 {
		return nil
	}
	if size > DefaultChunkSize {
		return make([]byte, size)
	}
	bp := chunkPool.Get().(*[]byte)
	return (*bp)[:size]
}

// PutChunkBuf returns a byte slice to the pool for reuse.
// Only slices with cap == DefaultChunkSize are pooled; others are left for GC.
func PutChunkBuf(b []byte) {
	if cap(b) != DefaultChunkSize {
		return
	}
	b = b[:cap(b)]
	chunkPool.Put(&b)
}

const (
	// DefaultChunkSize is the default size of chunks for streaming (64 KB).
	DefaultChunkSize = 64 * 1024
	// DefaultChannelBuffer is the default number of chunks to buffer per listener.
	// With 64KB chunks, this is ~64MB buffer per listener, providing sufficient
	// tolerance for temporary slowdowns before disconnecting slow consumers.
	// Increased from 64 to 1024 to handle large objects (64MB+) where cache writes
	// are slower than origin streaming.
	DefaultChannelBuffer = 1024
)

// ErrSlowConsumer indicates a listener was disconnected for being too slow.
// When this occurs, headers are already sent so no S3 error can be returned.
// The client will see a truncated response and S3 SDKs will automatically retry.
var ErrSlowConsumer = errors.New("slow consumer")

var errCompletedBeforeHeaders = errors.New("broadcast completed before response headers")

// Chunk represents a piece of streamed data.
// Consumers should call Release() after processing Data to return the
// underlying buffer to the pool. If Release() is not called, the buffer
// is garbage collected normally (safe, but less efficient).
type Chunk struct {
	Data []byte
	Err  error // Non-nil on final chunk if error occurred
}

// Release returns the chunk's data buffer to the pool for reuse.
// Safe to call multiple times or on chunks with nil Data.
//
// Thread safety: Release is not synchronized with a mutex because chunks flow
// through channels, which guarantee exclusive ownership — only one goroutine
// holds a given Chunk value at any time. DrainAndRelease only processes chunks
// still in the channel, never ones already received by a consumer.
func (c *Chunk) Release() {
	if c.Data != nil {
		PutChunkBuf(c.Data)
		c.Data = nil
	}
}

// Listener receives chunks from a broadcast.
type Listener struct {
	ch           chan Chunk
	headers      http.Header
	status       int
	headerCh     chan struct{} // Closed when headers or a pre-header completion are available
	headerErr    error         // Set when the broadcast completes before response headers
	disconnected bool          // True if disconnected due to slow consumption
	termErr      error         // Terminal error, written under Broadcaster.mu before ch is closed
}

// WaitForHeaders blocks until headers are available, the broadcast completes
// before headers, or the context is canceled. Successful headers still win over
// concurrent cancellation; a pre-header error is returned only while the
// request context remains live. A cache listener can be scheduled after its
// request handler returns, when headers and cancellation are both ready; it
// must still consume the completed response and publish the cache entry.
func (l *Listener) WaitForHeaders(ctx context.Context) (int, http.Header, error) {
	select {
	case <-l.headerCh:
		if l.headerErr != nil && ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		return l.status, l.headers, l.headerErr
	case <-ctx.Done():
		// A select chooses randomly when both channels are ready. Re-check the
		// headers so a completed response wins over cancellation, but keep
		// cancellation authoritative when the broadcast completed before headers.
		select {
		case <-l.headerCh:
			if l.headerErr != nil {
				return 0, nil, ctx.Err()
			}
			return l.status, l.headers, nil
		default:
			return 0, nil, ctx.Err()
		}
	}
}

// Chunks returns the channel to receive chunks.
//
// The channel delivers every data chunk the broadcaster managed to queue, in
// order, followed by at most one Chunk{Err: ...}. When the listener's queue was
// full at the moment it failed there is no room for that error chunk, so the
// channel simply closes after the last data chunk. Consumers that see the
// channel close without an error chunk must call Err() to learn whether the
// stream ended cleanly or was cut short.
func (l *Listener) Chunks() <-chan Chunk {
	return l.ch
}

// Err returns the listener's terminal error: nil when the broadcast completed
// successfully, otherwise the error that ended the stream (ErrSlowConsumer for
// a slow-consumer disconnect, or the broadcaster's completion error).
//
// It is only meaningful once Chunks() has been closed, i.e. after a range loop
// over Chunks() has ended. The field is written under Broadcaster.mu before the
// channel is closed, and a receive that returns because the channel is closed
// happens-after that close, so a consumer that has drained Chunks() reads it
// without further synchronization. Calling it earlier is a data race.
func (l *Listener) Err() error {
	return l.termErr
}

// closeWithError records the terminal error and closes the listener. The caller
// must hold Broadcaster.mu, which excludes other senders.
//
// The error is published two ways: it is stored in termErr (always), and a
// Chunk{Err: err} is queued on a best-effort basis (only when the buffer has
// room). A full buffer is never disturbed: channels are FIFO, so making room by
// displacing a queued chunk would drop the OLDEST unread chunk and hand the
// consumer the next one at the wrong offset, i.e. wrong bytes on the wire. No
// data chunk is ever dropped or reordered; a consumer that drains to a close
// without seeing an error chunk must consult Err().
func (l *Listener) closeWithError(err error) {
	l.termErr = err
	select {
	case l.ch <- Chunk{Err: err}:
	default:
	}
	close(l.ch)
}

// DrainAndRelease drains remaining chunks from the listener channel,
// returning pooled buffers. Runs in a background goroutine since the
// channel may not yet be closed (broadcaster still streaming).
// Call this after breaking out of a Chunks() range loop early.
// Safe to call on normal completion (no-op if channel is already drained/closed).
func (l *Listener) DrainAndRelease() {
	go func() {
		for chunk := range l.ch {
			chunk.Release()
		}
	}()
}

// Broadcaster streams data to multiple listeners simultaneously.
type Broadcaster struct {
	mu         sync.RWMutex
	listeners  []*Listener
	headers    http.Header
	status     int
	headerSet  bool
	streaming  bool // True once first chunk is broadcast (no late joiners)
	done       bool
	err        error
	doneCh     chan struct{}
	channelBuf int
}

// NewBroadcaster creates a new broadcaster.
func NewBroadcaster(channelBuf int) *Broadcaster {
	if channelBuf <= 0 {
		channelBuf = DefaultChannelBuffer
	}
	return &Broadcaster{
		doneCh:     make(chan struct{}),
		channelBuf: channelBuf,
	}
}

// IsStreaming returns true if broadcast has started streaming data.
// Used by Manager to implement "no late joiners" policy.
func (b *Broadcaster) IsStreaming() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.streaming
}

// Subscribe adds a new listener to receive chunks.
// Returns nil if streaming has already started (no late joiners).
func (b *Broadcaster) Subscribe() *Listener {
	b.mu.Lock()
	defer b.mu.Unlock()

	// No late joiners - if streaming started, caller must start their own broadcast
	if b.streaming {
		return nil
	}

	l := &Listener{
		ch:       make(chan Chunk, b.channelBuf),
		headerCh: make(chan struct{}),
	}

	if b.headerSet {
		l.headers = b.headers.Clone()
		l.status = b.status
	}

	if b.done {
		// A no-body success or a pre-header failure leaves streaming false. A
		// subscriber arriving before Manager removes that completed broadcast
		// must receive its terminal result rather than wait on an already-finished
		// producer.
		if !b.headerSet {
			l.headerErr = b.err
		}
		close(l.headerCh)
		l.termErr = b.err
		if b.err != nil {
			l.ch <- Chunk{Err: b.err}
		}
		close(l.ch)
		return l
	}

	// If headers are already set, this listener is immediately ready.
	if b.headerSet {
		close(l.headerCh)
	}

	b.listeners = append(b.listeners, l)
	return l
}

// SetHeaders sets response headers (called by fetcher before streaming body).
func (b *Broadcaster) SetHeaders(status int, headers http.Header) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.status = status
	b.headers = headers.Clone()
	b.headerSet = true

	// Notify all listeners
	for _, l := range b.listeners {
		l.status = status
		l.headers = headers.Clone()
		select {
		case <-l.headerCh:
			// Already closed
		default:
			close(l.headerCh)
		}
	}
}

// Broadcast sends a chunk to all listeners, copying the caller's data for each
// listener. Slow consumers (buffer full) are disconnected immediately.
func (b *Broadcaster) Broadcast(data []byte) {
	b.broadcast(data, false)
}

// BroadcastOwned sends a chunk to all listeners and transfers ownership of data
// to the first listener that accepts it. The data is copied for every subsequent
// listener so each listener still owns an independent buffer.
//
// It returns true when a listener accepted data and therefore owns the input
// buffer. When it returns false, no listener owns data and the caller may reuse
// or release it. The caller must not reuse or release data after a true result.
// Slow consumers (buffer full) are disconnected immediately.
func (b *Broadcaster) BroadcastOwned(data []byte) bool {
	return b.broadcast(data, true)
}

// broadcast delivers data through either the copying or ownership-taking path.
// Keeping the copying Broadcast API separate from the ownership-taking
// BroadcastOwned API prevents callers with reusable input slices from
// accidentally racing with listeners.
func (b *Broadcaster) broadcast(data []byte, owned bool) bool {
	if len(data) == 0 {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Mark as streaming on first chunk - no more subscribers allowed
	if !b.streaming {
		b.streaming = true
	}
	if owned {
		return b.broadcastOwnedLocked(data)
	}

	// Process listeners, removing slow ones.
	activeListeners := b.listeners[:0] // Reuse slice
	for _, l := range b.listeners {
		if l.disconnected {
			continue
		}

		chunk := Chunk{Data: GetChunkBuf(len(data))}
		copy(chunk.Data, data)

		// Non-blocking send - disconnect immediately if buffer is full
		select {
		case l.ch <- chunk:
			activeListeners = append(activeListeners, l)
		default:
			// Buffer full - disconnect slow consumer.
			chunk.Release()
			disconnectSlowConsumer(l)
		}
	}
	b.listeners = activeListeners
	return false
}

// broadcastOwnedLocked selects the first listener with room as the owner,
// sends independent copies to every other listener first, and transfers the
// input buffer to the owner last. Preparing copies first prevents a fast owner
// from releasing the input back to the pool while it is still needed as the
// source for a later copy. The caller must hold b.mu.
func (b *Broadcaster) broadcastOwnedLocked(data []byte) bool {
	ownerIndex := -1
	for i, l := range b.listeners {
		if !l.disconnected && len(l.ch) < cap(l.ch) {
			ownerIndex = i
			break
		}
	}
	activeListeners := b.listeners[:0]
	ownerPosition := 0
	var owner *Listener
	if ownerIndex >= 0 {
		owner = b.listeners[ownerIndex]
	}

	// Send independent copies while the input is still exclusively owned by the
	// producer. The order of activeListeners remains the same as b.listeners so
	// the fetcher's listener stays the owner candidate on subsequent chunks.
	for i, l := range b.listeners {
		if l.disconnected || i == ownerIndex {
			continue
		}

		chunk := Chunk{Data: GetChunkBuf(len(data))}
		copy(chunk.Data, data)
		select {
		case l.ch <- chunk:
			if ownerIndex >= 0 && i < ownerIndex {
				ownerPosition++
			}
			activeListeners = append(activeListeners, l)
		default:
			chunk.Release()
			disconnectSlowConsumer(l)
		}
	}

	if owner == nil {
		b.listeners = activeListeners
		return false
	}

	// The owner was observed with room before copies were sent. Since this
	// broadcaster is the only sender and consumers can only drain, this send
	// remains non-blocking. If it nevertheless fails, the caller retains data.
	select {
	case owner.ch <- Chunk{Data: data}:
		activeListeners = append(activeListeners, nil)
		copy(activeListeners[ownerPosition+1:], activeListeners[ownerPosition:])
		activeListeners[ownerPosition] = owner
		b.listeners = activeListeners
		return true
	default:
		disconnectSlowConsumer(owner)
		b.listeners = activeListeners
		return false
	}
}

// disconnectSlowConsumer marks l as disconnected and closes it through
// closeWithError so the terminal error is recorded in termErr and no queued data
// chunk is displaced. Callers must hold Broadcaster.mu and must not have handed
// the current chunk to l: on this path the chunk was never queued, so the
// caller still owns (and is responsible for releasing or retaining) it.
func disconnectSlowConsumer(l *Listener) {
	l.disconnected = true
	l.closeWithError(ErrSlowConsumer)
	log.Warn().Msg("Disconnecting slow consumer from broadcast (buffer full)")
}

// Complete marks the broadcast as done.
func (b *Broadcaster) Complete(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.done {
		return
	}

	b.done = true
	b.err = err
	if !b.headerSet {
		if b.err == nil {
			b.err = errCompletedBeforeHeaders
		}
		// A pre-header completion is the listener's terminal result. Publish it
		// before waking header waiters so they can return the original error.
		for _, l := range b.listeners {
			l.headerErr = b.err
			close(l.headerCh)
		}
	}
	close(b.doneCh)

	// Record the terminal result on every listener and close its channel. The
	// error chunk is best-effort (skipped when the buffer is full); queued data
	// chunks are always retained. A consumer that drains to the close without an
	// error chunk learns the outcome from Listener.Err(), so a failed completion
	// can never be mistaken for a clean EOF.
	for _, l := range b.listeners {
		if !l.disconnected {
			l.closeWithError(b.err)
		}
	}
}

// Done returns a channel that's closed when broadcast is complete.
func (b *Broadcaster) Done() <-chan struct{} {
	return b.doneCh
}

// Error returns the error if broadcast failed.
func (b *Broadcaster) Error() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.err
}

// ListenerCount returns the current number of active listeners.
func (b *Broadcaster) ListenerCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.listeners)
}
