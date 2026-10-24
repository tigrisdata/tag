package proxy

import (
	"context"
	"net/http"
	"strconv"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// A write-through tee whose body the owning node refuses must not fall back
// to a warm: the owner has already said not to store it.
func TestWriteThroughTee_OwnerShedIsFinal(t *testing.T) {
	body := []byte("0123456789")
	hdr := http.Header{}
	hdr.Set("ETag", `"e1"`)
	hdr.Set("Content-Length", strconv.Itoa(len(body)))
	hdr.Set("Content-Type", "application/octet-stream")
	fwd := &mockForwarder{conditionalResp: &http.Response{StatusCode: http.StatusOK, Header: hdr}}

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	client := &ownerShedClient{CacheClient: cacheclient.NewMemoryCache()}
	c := cache.NewCacheWithClient(client, &cfg.Cache)
	svc := NewService(fwd, c, cfg)
	ctx := context.Background()

	client.shed.Store(true)
	if got := svc.cacheTeedBodyFromHead(ctx, "b", "k", `"e1"`, body, "ak", "sk"); got != teeShed {
		t.Fatalf("tee outcome with owner shedding = %v, want teeShed", got)
	}
	if _, found, _ := c.GetMeta(ctx, "b", "k"); found {
		t.Fatal("a shed tee must not leave metadata behind")
	}

	client.shed.Store(false)
	if got := svc.cacheTeedBodyFromHead(ctx, "b", "k", `"e1"`, body, "ak", "sk"); got != teeCached {
		t.Fatalf("tee outcome with owner accepting = %v, want teeCached", got)
	}
}
