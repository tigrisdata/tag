package auth

import (
	"hash/maphash"
	"time"

	expirable "github.com/hashicorp/golang-lru/v2/expirable"
)

const readMostlyTTLShardCount = 64

var readMostlyTTLSeed = maphash.MakeSeed()

// readMostlyTTLTable distributes its total capacity across fixed expirable-LRU
// shards. Reads use Peek, so a cache hit checks the stored deadline without
// changing recency; each shard owns its own expiry sweeper.
type readMostlyTTLTable[V any] struct {
	shards []*expirable.LRU[string, V]
}

func newReadMostlyTTLTable[V any](maxEntries int, ttl time.Duration) *readMostlyTTLTable[V] {
	shardCount := readMostlyTTLShardCount
	if maxEntries > 0 && maxEntries < shardCount {
		// An expirable LRU size of zero means unlimited, so small tables use
		// fewer shards rather than creating unbounded zero-capacity shards.
		shardCount = maxEntries
	}

	table := &readMostlyTTLTable[V]{shards: make([]*expirable.LRU[string, V], shardCount)}
	perShard, remainder := maxEntries/shardCount, maxEntries%shardCount
	for i := range table.shards {
		capacity := perShard
		if i < remainder {
			capacity++
		}
		table.shards[i] = expirable.NewLRU[string, V](capacity, nil, ttl)
	}
	return table
}

func (t *readMostlyTTLTable[V]) shard(key string) *expirable.LRU[string, V] {
	index := maphash.String(readMostlyTTLSeed, key) % uint64(len(t.shards))
	return t.shards[index]
}

func (t *readMostlyTTLTable[V]) Get(key string) (V, bool) {
	return t.shard(key).Peek(key)
}

func (t *readMostlyTTLTable[V]) Store(key string, value V) {
	t.shard(key).Add(key, value)
}

func (t *readMostlyTTLTable[V]) Remove(key string) {
	t.shard(key).Remove(key)
}

// Len returns the sum of each shard's current entry count.
func (t *readMostlyTTLTable[V]) Len() int {
	count := 0
	for _, shard := range t.shards {
		count += shard.Len()
	}
	return count
}
