package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
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

// refusingShedClient is an owner over budget: every sheddable write is refused.
type refusingShedClient struct{ cacheclient.CacheClient }

func (refusingShedClient) PutStreamSheddable(context.Context, string, io.Reader, int64) error {
	return ErrPopulateShed
}

// A refused body or block surfaces as ErrPopulateShed and is counted as a shed
// under its own source; nothing is logged or counted as a cache failure.
func TestOwnerRefusalIsCountedAsShed(t *testing.T) {
	cfg := &config.CacheConfig{}
	cfg.SetEnabled(true)
	c := NewCacheWithClient(refusingShedClient{cacheclient.NewMemoryCache()}, cfg)
	ctx := context.Background()

	populateBefore := testutil.ToFloat64(metrics.CachePopulateDiskShed.WithLabelValues("populate_owner"))
	blockBefore := testutil.ToFloat64(metrics.CachePopulateDiskShed.WithLabelValues("block_owner"))

	meta := &CachedObjectMeta{ETag: `"e1"`, ContentLength: 5, StatusCode: 200}
	_, token, _, err := c.GetMetaWithVersion(ctx, "b", "k")
	if err != nil {
		t.Fatal(err)
	}
	wrote, err := c.PutWithMetaStreamIfVersion(ctx, "b", "k", meta, bytes.NewReader([]byte("hello")), 60, token)
	if wrote || !errors.Is(err, ErrPopulateShed) {
		t.Fatalf("PutWithMetaStreamIfVersion wrote=%v err=%v, want ErrPopulateShed", wrote, err)
	}
	if _, found, _ := c.GetMeta(ctx, "b", "k"); found {
		t.Fatal("a shed body must not leave metadata behind")
	}
	if err := c.PutBlock(ctx, "b", "k3", `"e3"`, 4, 0, []byte("blk"), 60); !errors.Is(err, ErrPopulateShed) {
		t.Fatalf("PutBlock err = %v, want ErrPopulateShed", err)
	}
	if err := c.PutBlockStream(ctx, "b", "k4", `"e4"`, 4, 0, bytes.NewReader([]byte("blk")), 60); !errors.Is(err, ErrPopulateShed) {
		t.Fatalf("PutBlockStream err = %v, want ErrPopulateShed", err)
	}

	if d := testutil.ToFloat64(metrics.CachePopulateDiskShed.WithLabelValues("populate_owner")) - populateBefore; d != 1 {
		t.Fatalf("populate_owner shed delta = %v, want 1", d)
	}
	if d := testutil.ToFloat64(metrics.CachePopulateDiskShed.WithLabelValues("block_owner")) - blockBefore; d != 2 {
		t.Fatalf("block_owner shed delta = %v, want 2", d)
	}
}

// A client whose classifier recognises the engine's raw refusal gets the same
// ErrPopulateShed without returning it itself.
type classifyingShedClient struct{ cacheclient.CacheClient }

var errRawRefusal = errors.New("rpc error: owner refused")

func (classifyingShedClient) PutStreamSheddable(context.Context, string, io.Reader, int64) error {
	return errRawRefusal
}
func (classifyingShedClient) IsOverBudget(err error) bool { return errors.Is(err, errRawRefusal) }

func TestOverBudgetClassifierMapsToErrPopulateShed(t *testing.T) {
	cfg := &config.CacheConfig{}
	cfg.SetEnabled(true)
	c := NewCacheWithClient(classifyingShedClient{cacheclient.NewMemoryCache()}, cfg)
	err := c.PutBodyStream(context.Background(), "b", "k", "ref", bytes.NewReader([]byte("x")), 60)
	if !errors.Is(err, ErrPopulateShed) {
		t.Fatalf("PutBodyStream err = %v, want ErrPopulateShed via classifier", err)
	}
}
