package proxy

import (
	"hash/maphash"
	"sync/atomic"

	"github.com/tigrisdata/tag/metrics"
)

// blockWriteFailureEntries is the slot count of a Service's block write-failure
// table (8 bytes each: 8 MiB).
const blockWriteFailureEntries = 1 << 20

// blockWriteFailure is why a fetched block was not cached. Codes fit in the low
// 3 bits of a table word; 0 is reserved for an empty slot.
type blockWriteFailure uint64

const (
	blockWriteNone blockWriteFailure = iota
	blockWriteSkipped
	blockWriteFailed
)

const blockWriteFailureMask = 7

var blockWriteFailureSeed = maphash.MakeSeed()

// blockWriteFailureTable remembers, per block key, that its last populate was
// skipped or failed on this pod. It is a fixed-size direct-mapped array of
// atomics: slot = hash & (len-1), word = (hash with the low 3 bits cleared) |
// reason. A colliding key overwrites the slot and a forgotten key reads as none,
// so refetch counts are a lower bound. Lock-free; a nil table records nothing.
type blockWriteFailureTable struct {
	slots []atomic.Uint64
	mask  uint64
}

// newBlockWriteFailureTable sizes the table to n, which must be a power of two.
func newBlockWriteFailureTable(n int) *blockWriteFailureTable {
	return &blockWriteFailureTable{slots: make([]atomic.Uint64, n), mask: uint64(n - 1)}
}

func (t *blockWriteFailureTable) record(blockKey string, r blockWriteFailure) {
	if t == nil {
		return
	}
	h := maphash.String(blockWriteFailureSeed, blockKey)
	t.slots[h&t.mask].Store(h&^blockWriteFailureMask | uint64(r))
}

func (t *blockWriteFailureTable) lookup(blockKey string) blockWriteFailure {
	if t == nil {
		return blockWriteNone
	}
	h := maphash.String(blockWriteFailureSeed, blockKey)
	w := t.slots[h&t.mask].Load()
	if w != 0 && w&^blockWriteFailureMask == h&^blockWriteFailureMask {
		return blockWriteFailure(w & blockWriteFailureMask)
	}
	return blockWriteNone
}

// clear empties blockKey's slot if it still holds blockKey.
func (t *blockWriteFailureTable) clear(blockKey string) {
	if t == nil {
		return
	}
	h := maphash.String(blockWriteFailureSeed, blockKey)
	slot := &t.slots[h&t.mask]
	if w := slot.Load(); w != 0 && w&^blockWriteFailureMask == h&^blockWriteFailureMask {
		slot.CompareAndSwap(w, 0)
	}
}

// countBlockRefetch increments tag_cache_block_refetch_total when blockKey's
// previous populate on this pod was skipped or failed.
func (t *blockWriteFailureTable) countBlockRefetch(blockKey string) {
	switch t.lookup(blockKey) {
	case blockWriteSkipped:
		metrics.CacheBlockRefetch.WithLabelValues("write_skipped").Inc()
	case blockWriteFailed:
		metrics.CacheBlockRefetch.WithLabelValues("write_failed").Inc()
	}
}
