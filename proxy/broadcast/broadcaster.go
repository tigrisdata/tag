// Package broadcast provides streaming request coalescing for concurrent requests.
// It implements a Varnish-style broadcaster pattern that streams bytes to ALL
// waiting clients simultaneously as they arrive from upstream.
package broadcast

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	// DefaultChunkSize is the default size of chunks for streaming (64 KB).
	DefaultChunkSize = 64 * 1024
	// DefaultChannelBuffer is the default number of chunks to buffer per listener.
	// With 64KB chunks, this is ~4MB buffer per listener.
	DefaultChannelBuffer = 64
	// SlowConsumerGracePeriod is how long to wait for a slow consumer before disconnecting.
	SlowConsumerGracePeriod = 5 * time.Second
)

// ErrSlowConsumer indicates a listener was disconnected for being too slow.
// When this occurs, headers are already sent so no S3 error can be returned.
// The client will see a truncated response and S3 SDKs will automatically retry.
var ErrSlowConsumer = errors.New("slow consumer")

// Chunk represents a piece of streamed data.
type Chunk struct {
	Data []byte
	Err  error // Non-nil on final chunk if error occurred
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
// Slow consumers (channel full) are given a grace period before being disconnected.
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

		chunk := Chunk{Data: make([]byte, len(data))}
		copy(chunk.Data, data)

		// Try non-blocking send first (fast path)
		select {
		case l.ch <- chunk:
			// Sent successfully - keep this listener
			activeListeners = append(activeListeners, l)
		default:
			// Channel full - give grace period before disconnecting
			// Release lock during blocking wait to avoid blocking other operations
			b.mu.Unlock()
			select {
			case l.ch <- chunk:
				// Sent after waiting - keep this listener
				b.mu.Lock()
				activeListeners = append(activeListeners, l)
			case <-time.After(SlowConsumerGracePeriod):
				// Still can't send after grace period - disconnect
				b.mu.Lock()
				l.disconnected = true
				// Try to send error, but don't block
				select {
				case l.ch <- Chunk{Err: ErrSlowConsumer}:
				default:
				}
				close(l.ch)
				// Don't add to activeListeners - listener is removed
				log.Warn().Msg("Disconnecting slow consumer from broadcast after timeout")
			}
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

	// Send final chunk with error status and close channels
	for _, l := range b.listeners {
		if !l.disconnected {
			l.ch <- Chunk{Err: err}
			close(l.ch)
		}
	}

	close(b.doneCh)
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
