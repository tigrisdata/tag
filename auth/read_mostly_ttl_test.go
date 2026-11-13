package auth

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestReadMostlyTTLTableGetDoesNotPromoteLRU(t *testing.T) {
	table := newReadMostlyTTLTable[string](128, time.Minute)
	first := "key-0"
	firstShard := table.shard(first)
	keys := []string{first}
	for i := 1; len(keys) < 3 && i < 10_000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if table.shard(key) == firstShard {
			keys = append(keys, key)
		}
	}
	if len(keys) != 3 {
		t.Fatal("could not find three keys in one shard")
	}

	table.Store(keys[0], "first")
	table.Store(keys[1], "second")
	if got, ok := table.Get(keys[0]); !ok || got != "first" {
		t.Fatalf("Get(first) = (%q, %t), want (first, true)", got, ok)
	}
	table.Store(keys[2], "third")

	if _, ok := table.Get(keys[0]); ok {
		t.Fatal("read promoted first entry; the next write should evict it")
	}
	if got, ok := table.Get(keys[1]); !ok || got != "second" {
		t.Fatalf("Get(second) = (%q, %t), want (second, true)", got, ok)
	}
	if got, ok := table.Get(keys[2]); !ok || got != "third" {
		t.Fatalf("Get(third) = (%q, %t), want (third, true)", got, ok)
	}
}

func TestReadMostlyTTLTableCapacityIsBoundedAcrossShards(t *testing.T) {
	const capacity = 10
	table := newReadMostlyTTLTable[int](capacity, time.Minute)
	for i := 0; i < 100; i++ {
		table.Store(fmt.Sprintf("key-%d", i), i)
		if got := table.Len(); got > capacity {
			t.Fatalf("Len() = %d after %d stores, want at most %d", got, i+1, capacity)
		}
	}
}

func TestReadMostlyTTLTableConcurrentStoreAndRead(t *testing.T) {
	table := newReadMostlyTTLTable[int](32, time.Minute)
	start := make(chan struct{})
	var workers sync.WaitGroup

	for reader := 0; reader < 16; reader++ {
		workers.Add(1)
		go func(reader int) {
			defer workers.Done()
			<-start
			for i := 0; i < 500; i++ {
				table.Get(fmt.Sprintf("key-%d", (i+reader)%64))
			}
		}(reader)
	}
	for writer := 0; writer < 4; writer++ {
		workers.Add(1)
		go func(writer int) {
			defer workers.Done()
			<-start
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("key-%d", (i+writer)%64)
				table.Store(key, i)
				if i%5 == 0 {
					table.Remove(key)
				}
			}
		}(writer)
	}

	close(start)
	workers.Wait()
}
