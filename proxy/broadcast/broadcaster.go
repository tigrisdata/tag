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
func GetChunkBuf(size int) []byte {
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
	headerCh     chan struct{} // Closed when headers are available
	disconnected bool          // True if disconnected due to slow consumption
}

// WaitForHeaders blocks until headers are available.
func (l *Listener) WaitForHeaders(ctx context.Context) (int, http.Header, error) {
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-l.headerCh:
		return l.status, l.headers, nil
	}
}

// Chunks returns the channel to receive chunks.
func (l *Listener) Chunks() <-chan Chunk {
	return l.ch
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

	// If headers already set, copy them and close headerCh
	if b.headerSet {
		l.headers = b.headers.Clone()
		l.status = b.status
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

// Broadcast sends a chunk to all listeners.
// Slow consumers (buffer full) are disconnected immediately.
func (b *Broadcaster) Broadcast(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Mark as streaming on first chunk - no more subscribers allowed
	if !b.streaming {
		b.streaming = true
	}

	// Process listeners, removing slow ones
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
			// Return pooled buffer since this chunk won't be consumed.
			chunk.Release()
			l.disconnected = true
			select {
			case l.ch <- Chunk{Err: ErrSlowConsumer}:
			default:
			}
			close(l.ch)
			log.Warn().Msg("Disconnecting slow consumer from broadcast (buffer full)")
		}
	}
	b.listeners = activeListeners
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
	close(b.doneCh)

	// Send final chunk and close all listener channels.
	// Non-blocking send: if buffer is full, skip the error chunk.
	// Correctness is guaranteed by the tombstone mechanism in cache writes,
	// which prevents stale data from being cached even if error delivery fails.
	for _, l := range b.listeners {
		if !l.disconnected {
			select {
			case l.ch <- Chunk{Err: err}:
			default:
			}
			close(l.ch)
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
