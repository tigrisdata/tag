package cache

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

// recordingShedClient notes which keys went through the sheddable body path.
type recordingShedClient struct {
	cacheclient.CacheClient
	mu        sync.Mutex
	sheddable []string
}

func (r *recordingShedClient) PutStreamSheddable(ctx context.Context, key string, body io.Reader, ttl int64) error {
	r.mu.Lock()
	r.sheddable = append(r.sheddable, key)
	r.mu.Unlock()
	return r.CacheClient.PutStream(ctx, key, body, ttl)
}

// Bodies and blocks take the sheddable path; metadata never does.
func TestBodyWritesAreSheddableMetadataIsNot(t *testing.T) {
	cfg := &config.CacheConfig{}
	cfg.SetEnabled(true)
	client := &recordingShedClient{CacheClient: cacheclient.NewMemoryCache()}
	c := NewCacheWithClient(client, cfg)
	ctx := context.Background()

	meta := &CachedObjectMeta{ETag: `"e1"`, ContentLength: 5, StatusCode: 200}
	_, token, _, err := c.GetMetaWithVersion(ctx, "b", "k") // decision-time token for the meta commit
	if err != nil {
		t.Fatal(err)
	}
	wrote, err := c.PutWithMetaStreamIfVersion(ctx, "b", "k", meta, bytes.NewReader([]byte("hello")), 60, token)
	if err != nil || !wrote {
		t.Fatalf("PutWithMetaStreamIfVersion wrote=%v err=%v", wrote, err)
	}
	if err := c.PutBodyStream(ctx, "b", "k2", "ref", bytes.NewReader([]byte("hello")), 60); err != nil {
		t.Fatal(err)
	}
	if err := c.PutBlock(ctx, "b", "k3", `"e3"`, 4, 0, []byte("blk"), 60); err != nil {
		t.Fatal(err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.sheddable) != 3 {
		t.Fatalf("sheddable writes = %v, want 3 (body, body, block)", client.sheddable)
	}
	for _, k := range client.sheddable {
		if strings.HasPrefix(k, "meta|") {
			t.Fatalf("metadata key %q must never take the sheddable path", k)
		}
	}
}
