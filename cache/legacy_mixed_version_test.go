package cache

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

type legacyPeerDecisionGate struct {
	cacheclient.CacheClient
	metaKey     string
	armed       atomic.Bool
	readStarted chan struct{}
	resumeRead  chan struct{}
	resumeOnce  sync.Once
}

func (g *legacyPeerDecisionGate) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := g.CacheClient.Get(ctx, key)
	if key == g.metaKey && g.armed.Swap(false) {
		close(g.readStarted)
		<-g.resumeRead
	}
	return data, err
}

func (g *legacyPeerDecisionGate) resume() {
	g.resumeOnce.Do(func() { close(g.resumeRead) })
}

func TestLegacyDecisionTimePrecedesLegacyPeerInvalidation(t *testing.T) {
	ctx := context.Background()
	const bucket, key = "mixed-version-bucket", "deleted-key"
	base := cacheclient.NewMemoryCache()
	gate := &legacyPeerDecisionGate{
		CacheClient: base,
		metaKey:     MakeMetaKey(bucket, key),
		readStarted: make(chan struct{}),
		resumeRead:  make(chan struct{}),
	}
	cfg := config.NewDefault()
	cfg.Cache.SetLegacyCoordination(true)
	c := NewCacheWithClient(gate, &cfg.Cache)
	defer gate.resume()

	old := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"old"`, StatusCode: 200}
	if err := c.PutWithMeta(ctx, bucket, key, old, []byte("old body"), 60); err != nil {
		t.Fatalf("seed old metadata: %v", err)
	}

	type decision struct {
		found   bool
		err     error
		publish func() (bool, error)
	}
	captured := make(chan decision, 1)
	gate.armed.Store(true)
	go func() {
		_, token, found, err := c.GetMetaWithVersion(ctx, bucket, key)
		captured <- decision{
			found: found,
			err:   err,
			publish: func() (bool, error) {
				return c.PutWithMetaStreamIfVersion(ctx, bucket, key, old, strings.NewReader("old body"), 60, token)
			},
		}
	}()

	select {
	case <-gate.readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("decision read did not reach the metadata snapshot")
	}

	// Simulate an older v1.20 peer: it writes the timestamp tombstone and plain
	// metadata delete, but knows nothing about the new generation sidecar.
	var tombstone [8]byte
	binary.BigEndian.PutUint64(tombstone[:], uint64(time.Now().UnixNano()))
	if err := base.Put(ctx, MakeTombstoneKey(bucket, key), tombstone[:], TombstoneTTLSeconds(cfg.Cache.SizeThreshold)); err != nil {
		t.Fatalf("older peer tombstone: %v", err)
	}
	if err := base.Delete(ctx, MakeMetaKey(bucket, key)); err != nil {
		t.Fatalf("older peer metadata delete: %v", err)
	}
	gate.resume()

	var result decision
	select {
	case result = <-captured:
	case <-time.After(5 * time.Second):
		t.Fatal("decision token read did not finish")
	}
	if result.err != nil {
		t.Fatalf("capture decision token: %v", result.err)
	}
	if !result.found {
		t.Fatal("decision snapshot did not include the old metadata")
	}

	wrote, err := result.publish()
	if err != nil {
		t.Fatalf("late old-object publish: %v", err)
	}
	if wrote {
		t.Fatal("late metadata commit crossed an older-peer tombstone")
	}
	if meta, found, err := c.GetMeta(ctx, bucket, key); err != nil {
		t.Fatalf("read metadata after late publish: %v", err)
	} else if found {
		t.Fatalf("old metadata remained readable after the older-peer delete: %+v", meta)
	}
}
