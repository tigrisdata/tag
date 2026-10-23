package cache

import (
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

// diskUsageClient is a memory cache that also reports a fixed local usage.
type diskUsageClient struct {
	cacheclient.CacheClient
	used int64
}

func (d *diskUsageClient) DiskUsageBytes() int64 { return d.used }

func TestDiskUsageRatio(t *testing.T) {
	cfg := &config.CacheConfig{MaxDiskUsageBytes: 1000}
	cfg.SetEnabled(true)

	// A client that cannot report usage never yields a ratio.
	plain := NewCacheWithClient(cacheclient.NewMemoryCache(), cfg)
	if _, ok := plain.DiskUsageRatio(); ok {
		t.Fatal("DiskUsageRatio ok = true for a client without DiskUsageBytes")
	}

	reporting := &diskUsageClient{CacheClient: cacheclient.NewMemoryCache(), used: 1100}
	c := NewCacheWithClient(reporting, cfg)
	ratio, ok := c.DiskUsageRatio()
	if !ok || ratio < 1.09 || ratio > 1.11 {
		t.Fatalf("DiskUsageRatio = %v, %v; want ~1.1, true", ratio, ok)
	}

	// No cap configured: nothing to measure against.
	uncapped := NewCacheWithClient(reporting, &config.CacheConfig{})
	if _, ok := uncapped.DiskUsageRatio(); ok {
		t.Fatal("DiskUsageRatio ok = true with no disk cap")
	}
}
