package broadcast

import (
	"sync"
)

// Manager coordinates broadcasts for concurrent requests.
// It implements a "no late joiners" policy: once streaming starts,
// new requests must create their own broadcast.
type Manager struct {
	mu         sync.Mutex
	active     map[string]*Broadcaster
	channelBuf int
}

// NewManager creates a new broadcast manager.
func NewManager(channelBuf int) *Manager {
	if channelBuf <= 0 {
		channelBuf = DefaultChannelBuffer
	}
	return &Manager{
		active:     make(map[string]*Broadcaster),
		channelBuf: channelBuf,
	}
}

// GetOrCreate returns existing broadcaster or creates new one.
// Returns (broadcaster, isFirstCaller).
// If an existing broadcast has already started streaming, returns a NEW broadcaster
// (no late joiners policy - late arrivals start their own fetch).
func (m *Manager) GetOrCreate(key string) (*Broadcaster, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if b, exists := m.active[key]; exists {
		// Check if streaming has started (need to release manager lock first to avoid deadlock)
		// We do this check without holding broadcaster lock since IsStreaming uses RLock
		if !b.IsStreaming() {
			// Can still join this broadcast
			return b, false
		}
		// Streaming started - late joiner must start own broadcast
		// Fall through to create new broadcaster
	}

	// Create new broadcaster
	b := NewBroadcaster(m.channelBuf)
	m.active[key] = b
	return b, true
}

// Remove removes a broadcaster when done.
func (m *Manager) Remove(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.active, key)
}

// ActiveCount returns number of active broadcasts (for metrics).
func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.active)
}
