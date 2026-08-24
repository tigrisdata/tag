// Package config provides configuration management for TAG.
package config

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

// Default configuration values.
const (
	// DefaultHTTPPort is the default S3 API port.
	DefaultHTTPPort = 8080

	// DefaultBindIP is the default bind address.
	DefaultBindIP = "0.0.0.0"

	// DefaultServerMaxInflightRequests is the default ceiling on concurrently-served
	// S3 requests. Sized so that, with streaming (per-request memory ~MiB), the
	// worst case stays well within typical container memory limits.
	DefaultServerMaxInflightRequests = 1024

	// DefaultUpstreamEndpoint is the default Tigris S3 endpoint.
	DefaultUpstreamEndpoint = "https://t3.storage.dev"

	// DefaultUpstreamRegion is the default AWS region for signing.
	DefaultUpstreamRegion = "auto"

	// DefaultCacheTTL is the default cache TTL.
	DefaultCacheTTL = 24 * time.Hour

	// DefaultCacheSizeThreshold is the max object size to cache (1GB).
	DefaultCacheSizeThreshold = 1024 * 1024 * 1024

	// DefaultCacheBlockSize (1 MiB) is the block granularity AND the read-side whole-vs-block
	// boundary: a read miss for an object SMALLER than one block is cached as a whole blob
	// (block-caching a sub-block object is identical to whole-caching it), while an object
	// this size or LARGER is cached at block granularity. Sized to typical analytics read
	// granularity (Parquet footers/row-groups, SST blocks); tune it to your workload's read
	// size, since an oversized block over-fetches on every miss and amplifies upstream. It must
	// stay below ocache's 64 MB CompactThreshold so blocks pack into shared segments rather than
	// each becoming a standalone file (see RFC 0001).
	DefaultCacheBlockSize = 1 * 1024 * 1024

	// DefaultCacheDiskPath is the default disk path for embedded cache storage.
	DefaultCacheDiskPath = "/var/cache/tag"

	// DefaultCacheGRPCAddr is the default gRPC address for embedded cache cluster routing.
	DefaultCacheGRPCAddr = ":9000"

	// DefaultCacheClusterAddr is the default cluster gossip address for embedded cache.
	DefaultCacheClusterAddr = ":7000"

	// DefaultLogLevel is the default log level.
	DefaultLogLevel = "info"

	// DefaultLogFormat is the default log format.
	// Use "json" for production (fast) or "console" for development (human-readable).
	DefaultLogFormat = "json"

	// DefaultBroadcastChunkSize is the default chunk size for streaming (64KB).
	DefaultBroadcastChunkSize = 64 * 1024

	// DefaultBroadcastChannelBuffer is the default buffer size per listener (1024 chunks = ~64MB).
	DefaultBroadcastChannelBuffer = 1024

	// DefaultMaxIdleConnsPerHost is the default HTTP connection pool size per upstream host.
	// Higher values improve throughput for cache miss scenarios with high concurrency.
	DefaultMaxIdleConnsPerHost = 100

	// DefaultCacheDeleteBatchSize is the default number of file deletions processed
	// per deletion-queue batch. Mirrors ocache storage.DefaultDeleteBatchSize; kept
	// as a literal so the config package stays free of the cgo/RocksDB dependency.
	DefaultCacheDeleteBatchSize = 1000

	// DefaultCacheRecoveryWorkers is the default number of parallel workers for
	// startup file recovery. Mirrors ocache storage.DefaultRecoveryWorkers; kept
	// as a literal so the config package stays free of the cgo/RocksDB dependency.
	DefaultCacheRecoveryWorkers = 16

	// EvictionPolicyLRU evicts least-recently-used entries first (default).
	// EvictionPolicyFIFO evicts oldest-written entries first — better for
	// write-once workloads (e.g. dated parquet) where a rare read of an old object
	// should not keep it resident at the expense of newer, hotter data. Both mirror
	// ocache storage.EvictionPolicy{LRU,FIFO} and only take effect when
	// max_disk_usage_bytes > 0 (eviction runs only under a disk cap).
	EvictionPolicyLRU  = "lru"
	EvictionPolicyFIFO = "fifo"

	// DefaultCacheEvictionPolicy preserves the historical behavior (LRU).
	DefaultCacheEvictionPolicy = EvictionPolicyLRU

	// DefaultCacheMaxConcurrentWrites is the default ceiling on concurrent
	// cache-populate operations.
	DefaultCacheMaxConcurrentWrites = 256

	// DefaultCacheMaxPopulateMemoryBytes is the default for MaxPopulateMemoryBytes (2 GiB); see
	// that field for the budget's contract. A byte-unaware count alone (MaxConcurrentWrites) can
	// pin gigabytes under large-object fan-out — e.g. 256 large populates × ~80 MB ≈ 20 GB — so a
	// byte budget, not the count, is what actually bounds this memory.
	DefaultCacheMaxPopulateMemoryBytes = 2 << 30

	// DefaultWarmOnWriteReservedFraction is the default cap on the fraction of the
	// cache-populate memory budget reserved for warm-on-write populates (when
	// warm_on_write is enabled). The reservation is demand-driven and elastic:
	// read-miss warms use the whole budget when no warm-on-write is pending, and
	// back off only by what pending warm-on-writes actually need, up to this cap.
	// This protects warm-on-write from being starved by the read-miss full-object
	// warm flood for read-recently-written workloads (issue #100).
	DefaultWarmOnWriteReservedFraction = 0.5
)

// Config holds all configuration for TAG.
type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Upstream    UpstreamConfig    `yaml:"upstream"`
	Credentials CredentialsConfig `yaml:"credentials"`
	Cache       CacheConfig       `yaml:"cache"`
	Broadcast   BroadcastConfig   `yaml:"broadcast"`
	Log         LogConfig         `yaml:"log"`
}

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	HTTPPort     int    `yaml:"http_port"`     // S3 API port (default: 8080)
	BindIP       string `yaml:"bind_ip"`       // Bind address (default: 0.0.0.0)
	PprofEnabled bool   `yaml:"pprof_enabled"` // Enable pprof endpoints (default: false)
	TLSCertFile  string `yaml:"tls_cert_file"` // Path to TLS certificate file (PEM format)
	TLSKeyFile   string `yaml:"tls_key_file"`  // Path to TLS private key file (PEM format)
	// MaxInflightRequests bounds concurrently-served S3 requests; excess requests
	// are shed with 503 SlowDown so overload becomes backpressure rather than
	// unbounded goroutine/thread/memory growth. 0 or unset uses
	// DefaultServerMaxInflightRequests; a negative value disables the limit.
	MaxInflightRequests int `yaml:"max_inflight_requests"`
}

// TLSEnabled returns whether TLS is configured.
// TLS is enabled when both TLSCertFile and TLSKeyFile are set.
func (s *ServerConfig) TLSEnabled() bool {
	return s.TLSCertFile != "" && s.TLSKeyFile != ""
}

// UpstreamConfig holds Tigris endpoint configuration.
type UpstreamConfig struct {
	Endpoint            string `yaml:"endpoint"`                // Tigris S3 endpoint (e.g., https://fly.storage.tigris.dev)
	Region              string `yaml:"region"`                  // AWS region for signing (default: auto)
	MaxIdleConnsPerHost int    `yaml:"max_idle_conns_per_host"` // HTTP connection pool size per host (default: 100)
	TransparentProxy    *bool  `yaml:"transparent_proxy"`       // Forward client requests as-is with proxy headers (default: true when nil)

	// Disabled runs TAG with no upstream at all: a cache miss is the final answer
	// (NoSuchKey) rather than something to fetch, and writes land only in local
	// storage. For a tier whose contents are pushed in by its client rather than
	// pulled from an origin.
	//
	// Mutually exclusive with Endpoint. When set, applyDefaults leaves Endpoint
	// empty, which is what every downstream consumer keys off — Endpoint is the
	// single source of truth for whether an origin exists, and this flag only
	// suppresses the default that would otherwise invent one.
	Disabled bool `yaml:"disabled"`
}

// HasOrigin reports whether an upstream is configured. False only in origin-less
// mode, which applyDefaults guarantees by leaving Endpoint empty.
func (u *UpstreamConfig) HasOrigin() bool {
	return u.Endpoint != ""
}

// IsTransparentProxy returns whether transparent proxy mode is enabled.
// Returns true by default if not explicitly set.
func (u *UpstreamConfig) IsTransparentProxy() bool {
	if u.TransparentProxy == nil {
		return true // Default to enabled
	}
	return *u.TransparentProxy
}

// SetTransparentProxy sets the TransparentProxy field to the given value.
func (u *UpstreamConfig) SetTransparentProxy(enabled bool) {
	u.TransparentProxy = &enabled
}

// CredentialsConfig holds credential store configuration.
// Credentials are loaded from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.
type CredentialsConfig struct {
	AuthzCacheTTL time.Duration `yaml:"authz_cache_ttl"` // TTL for authorization cache entries (default: 10m)
}

// CacheConfig holds cache configuration.
// These fields map to github.com/tigrisdata/ocache/embedded.Config.
type CacheConfig struct {
	Enabled       *bool         `yaml:"enabled"`        // Enable caching (default: true when nil)
	TTL           time.Duration `yaml:"ttl"`            // Default cache TTL (default: 24h)
	SizeThreshold int64         `yaml:"size_threshold"` // Max object size to cache in bytes (default: 1GB)

	// OCache embedded configuration (see github.com/tigrisdata/ocache/embedded)
	DiskPath          string   `yaml:"disk_path"`            // Path to cache data directory (default: /var/cache/tag)
	MaxDiskUsageBytes int64    `yaml:"max_disk_usage_bytes"` // Max disk usage in bytes (0 = unlimited)
	NodeID            string   `yaml:"node_id"`              // Unique node identifier for cluster mode
	ClusterAddr       string   `yaml:"cluster_addr"`         // Address for memberlist gossip (default: :7000)
	GRPCAddr          string   `yaml:"grpc_addr"`            // Address for gRPC server (default: :9000)
	AdvertiseAddr     string   `yaml:"advertise_addr"`       // Address advertised to other nodes (defaults to GRPCAddr)
	SeedNodes         []string `yaml:"seed_nodes"`           // Seed nodes for cluster discovery
	GRPCAuth          *bool    `yaml:"grpc_auth"`            // Enable gRPC auth using proxy credentials (default: true when nil)

	// Advanced storage tuning (maps to ocache stor.StorageConfig).
	DeleteBatchSize int `yaml:"delete_batch_size"` // File deletions processed per deletion-queue batch (default: DefaultCacheDeleteBatchSize)
	RecoveryWorkers int `yaml:"recovery_workers"`  // Parallel workers for startup file recovery (default: DefaultCacheRecoveryWorkers)
	// EvictionPolicy selects the order entries are evicted when the disk cap is hit:
	// "lru" (default) or "fifo" (oldest-written first). Only takes effect when
	// MaxDiskUsageBytes > 0 — with no disk cap nothing is evicted regardless.
	EvictionPolicy string `yaml:"eviction_policy"`
	// ParquetOptimization enables format-aware prefetching for objects whose key
	// ends in ".parquet". A parquet reader always reads the file footer before any
	// data, and the footer is at the end of the object, so a read that touches the
	// object's tail is a reliable signal that the whole metadata region is about to
	// be read. When the metadata spans more than the (partial) tail block, TAG
	// fetches the remaining metadata blocks in the background instead of letting the
	// reader discover them as misses. Measured on production parseable data, footers
	// run ~1.25% of object size, so a 300 MB object carries ~3.5 MB of metadata --
	// several blocks at a 1 MiB block_size. Off by default: it reads 8 bytes of
	// object content to size the footer, which is format-specific behavior an
	// operator should opt into.
	ParquetOptimization bool `yaml:"parquet_optimization"`

	// MetaOnWrite caches an object's METADATA when TAG proxies its write, without
	// caching the body. TAG otherwise invalidates on write and caches nothing, so
	// the first read of a freshly written object pays an upstream round trip just to
	// establish metadata before block mode can engage. Multipart uploads are the gap
	// this closes: write-through already caches meta+body for a teed PutObject, but
	// TAG never sees a multipart body and so caches nothing at all. Prototype.
	MetaOnWrite bool `yaml:"meta_on_write"`

	// CompactionBytesPerSecond bounds the shared source-read budget for ocache's
	// background file compaction and segment recompaction (bytes/second). It
	// protects serving reads on throughput-capped volumes: unthrottled
	// compaction bursts saturate the disk and stall foreground GETs. 0 or unset
	// leaves compaction unthrottled (ocache's library default); set it to a
	// small fraction of the volume's throughput cap (e.g. 16-32 MiB/s on a
	// 240 MB/s volume).
	CompactionBytesPerSecond int64 `yaml:"compaction_bytes_per_second"`
	// MaxConcurrentWrites bounds concurrent cache-populate operations (upstream
	// fetch + streaming write). When saturated, the object is still served from
	// upstream but not cached, so the memory/I/O-heavy write path can't grow
	// unbounded. 0 or unset uses DefaultCacheMaxConcurrentWrites; a negative
	// value disables the limit.
	MaxConcurrentWrites int `yaml:"max_concurrent_writes"`
	// MaxPopulateMemoryBytes bounds the aggregate memory buffered by ALL cache buffering —
	// cache-populate and block-serve staging together — as one honest total: buffering never
	// exceeds this value. Each populate reserves its object size, capped at the per-populate
	// buffer ceiling (~(channel_buffer + max(channel_buffer/4, 64)) × chunk_size); when it can't
	// fit, the object is served from upstream uncached. Small objects reserve little (high
	// concurrency) while a burst of large objects is throttled — this is what actually bounds
	// populate memory, since a byte-unaware count can pin many GB under large-object fan-out.
	// Applied independently of MaxConcurrentWrites (both limits apply). 0 or unset uses
	// DefaultCacheMaxPopulateMemoryBytes; a negative value disables the budget (count-only).
	//
	// Block-serve staging buffers (see proxy.NewService) draw from this SAME budget but are
	// capped at half of it, so warm block serves — which hold staging bytes for a whole response —
	// can never starve cold-miss populates, while populates can still use the entire budget when
	// no serve is staging.
	MaxPopulateMemoryBytes int64 `yaml:"max_populate_memory_bytes"`
	// WarmOnWrite, when true, repopulates the cache after a successful write
	// (PutObject / CompleteMultipartUpload / CopyObject) by triggering a background
	// full-object fetch — so a read soon after a write hits cache. This is
	// cache-warm-on-write (write-around plus async warming), not strict
	// write-through: the write still invalidates, and the warm is a separate,
	// best-effort background GET (deduplicated and shed under the populate budget).
	// It costs one extra upstream GET per write, so it defaults to false.
	WarmOnWrite bool `yaml:"warm_on_write"`
	// BlockCachingEnabled turns on block-aligned caching for large objects (RFC 0001):
	// objects at or above BlockSize are cached at BlockSize granularity on read, so a range
	// read (e.g. a Parquet footer) populates and serves only the blocks it touches instead of
	// the whole object. Defaults to true when nil; set false to disable. Read via
	// IsBlockCachingEnabled(). Size BlockSize to your workload's read granularity — an
	// oversized block amplifies upstream traffic on every miss.
	BlockCachingEnabled *bool `yaml:"block_caching_enabled"`
	// BlockSize is the block granularity AND the read-side whole-vs-block boundary: a read
	// miss for an object smaller than one block is whole-cached, an object this size or larger
	// is block-cached. It must stay below ocache's 64 MB CompactThreshold so blocks pack into
	// shared segments. 0 or unset uses DefaultCacheBlockSize (1 MiB). Only meaningful when
	// BlockCachingEnabled is true.
	BlockSize int64 `yaml:"block_size"`
	// WarmOnWriteReservedFraction caps the fraction of the populate memory budget
	// that warm-on-write populates may reserve ahead of read-miss warms, so
	// warm-on-write is never starved by the read-miss full-object warm flood. The
	// reservation is demand-driven and elastic: read-miss warms use the whole
	// budget when no warm-on-write is pending, and back off only by what pending
	// warm-on-writes need (up to this cap). Only applied when WarmOnWrite is true.
	// 0 or unset uses DefaultWarmOnWriteReservedFraction; a negative value disables
	// the reservation (read-miss and warm-on-write compete equally). Clamped to
	// [0, 1].
	WarmOnWriteReservedFraction float64 `yaml:"warm_on_write_reserved_fraction"`
}

// IsEnabled returns whether caching is enabled.
// Returns true by default if not explicitly set.
func (c *CacheConfig) IsEnabled() bool {
	if c.Enabled == nil {
		return true // Default to enabled
	}
	return *c.Enabled
}

// IsBlockCachingEnabled returns whether block-aligned caching is enabled (default: true when nil).
func (c *CacheConfig) IsBlockCachingEnabled() bool {
	if c.BlockCachingEnabled == nil {
		return true // Default to enabled
	}
	return *c.BlockCachingEnabled
}

// SetBlockCachingEnabled sets the BlockCachingEnabled field to the given value.
func (c *CacheConfig) SetBlockCachingEnabled(enabled bool) {
	c.BlockCachingEnabled = &enabled
}

// SetEnabled sets the Enabled field to the given value.
func (c *CacheConfig) SetEnabled(enabled bool) {
	c.Enabled = &enabled
}

// IsGRPCAuthEnabled returns whether gRPC auth is enabled for cache cluster communication.
// Returns true by default if not explicitly set.
func (c *CacheConfig) IsGRPCAuthEnabled() bool {
	if c.GRPCAuth == nil {
		return true // Default to enabled
	}
	return *c.GRPCAuth
}

// SetGRPCAuth sets the GRPCAuth field to the given value.
func (c *CacheConfig) SetGRPCAuth(enabled bool) {
	c.GRPCAuth = &enabled
}

// BroadcastConfig holds streaming broadcast configuration for request coalescing.
type BroadcastConfig struct {
	ChunkSize     int `yaml:"chunk_size"`     // Size of chunks for streaming (default: 64KB)
	ChannelBuffer int `yaml:"channel_buffer"` // Buffer size per listener in chunks (default: 1024, ~64MB at 64KB chunks)
}

// LogConfig holds logging configuration.
type LogConfig struct {
	Level  string `yaml:"level"`  // Log level: debug, info, warn, error
	Format string `yaml:"format"` // Log format: json (default, fast) or console (human-readable)
}

// Load reads configuration from a YAML file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Read the deployment mode first: it decides whether an endpoint default applies.
	applyUpstreamModeEnv(&cfg)

	// Apply defaults
	applyDefaults(&cfg)

	// Override from environment variables
	applyEnvOverrides(&cfg)

	// Resolve mode-dependent defaults once both file and environment have been read.
	resolveClusterAuthDefault(&cfg)

	// Validate configuration
	if err := validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// NewDefault creates a Config with default values.
// Panics if the resulting configuration is invalid (e.g., disallowed upstream endpoint).
func NewDefault() *Config {
	cfg := &Config{}
	applyUpstreamModeEnv(cfg)
	applyDefaults(cfg)
	applyEnvOverrides(cfg)
	resolveClusterAuthDefault(cfg)
	if err := validate(cfg); err != nil {
		panic(fmt.Sprintf("invalid default configuration: %v", err))
	}
	return cfg
}

// applyUpstreamModeEnv reads the origin-less switch. It runs before applyDefaults
// so that no endpoint is ever invented in this mode — which means any non-empty
// Endpoint later on is one the operator supplied, and validate() can reject the
// combination without having to guess whether a value was defaulted.
//
// Honors an explicit false so the environment can override a YAML
// upstream.disabled: true, matching TAG_TRANSPARENT_PROXY. Unrecognized values
// leave the setting untouched rather than silently flipping the deployment mode.
func applyUpstreamModeEnv(cfg *Config) {
	val := strings.ToLower(strings.TrimSpace(os.Getenv("TAG_UPSTREAM_DISABLED")))
	switch val {
	case "":
		return
	case "true", "1":
		cfg.Upstream.Disabled = true
	case "false", "0":
		cfg.Upstream.Disabled = false
	}
}

// applyDefaults sets default values for unset configuration fields.
func applyDefaults(cfg *Config) {
	// Server defaults
	if cfg.Server.HTTPPort == 0 {
		cfg.Server.HTTPPort = DefaultHTTPPort
	}
	if cfg.Server.BindIP == "" {
		cfg.Server.BindIP = DefaultBindIP
	}
	if cfg.Server.MaxInflightRequests == 0 {
		cfg.Server.MaxInflightRequests = DefaultServerMaxInflightRequests
	}
	// PprofEnabled defaults to false (disabled for security)
	// Use TAG_PPROF_ENABLED=true to enable

	// Upstream defaults. Origin-less mode must not pick up the default endpoint —
	// Endpoint being empty is precisely how the rest of the process recognises that
	// there is no origin. Validate() rejects the contradictory combination, so an
	// endpoint surviving here means the operator did not ask for origin-less.
	if !cfg.Upstream.Disabled && cfg.Upstream.Endpoint == "" {
		cfg.Upstream.Endpoint = DefaultUpstreamEndpoint
	}
	if cfg.Upstream.Region == "" {
		cfg.Upstream.Region = DefaultUpstreamRegion
	}
	if cfg.Upstream.MaxIdleConnsPerHost == 0 {
		cfg.Upstream.MaxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	}

	// Cache defaults - enabled by default for embedded mode.
	// Can be disabled via config file (cache.enabled: false) or TAG_CACHE_DISABLED=true env var.
	// Note: cfg.Cache.IsEnabled() returns true by default if Enabled is nil.
	if cfg.Cache.TTL == 0 {
		cfg.Cache.TTL = DefaultCacheTTL
	}
	if cfg.Cache.SizeThreshold == 0 {
		cfg.Cache.SizeThreshold = DefaultCacheSizeThreshold
	}
	// Block size must be positive (it is a divisor in block arithmetic, and the read-side
	// whole-vs-block boundary); a zero or negative value — from YAML or a programmatic config —
	// falls back to the default.
	if cfg.Cache.BlockSize <= 0 {
		cfg.Cache.BlockSize = DefaultCacheBlockSize
	}
	if cfg.Cache.DiskPath == "" {
		cfg.Cache.DiskPath = DefaultCacheDiskPath
	}
	// Note: MaxDiskUsageBytes defaults to 0 (unlimited), so no default assignment needed
	if cfg.Cache.ClusterAddr == "" {
		cfg.Cache.ClusterAddr = DefaultCacheClusterAddr
	}
	if cfg.Cache.GRPCAddr == "" {
		cfg.Cache.GRPCAddr = DefaultCacheGRPCAddr
	}
	if cfg.Cache.DeleteBatchSize == 0 {
		cfg.Cache.DeleteBatchSize = DefaultCacheDeleteBatchSize
	}
	if cfg.Cache.RecoveryWorkers == 0 {
		cfg.Cache.RecoveryWorkers = DefaultCacheRecoveryWorkers
	}
	cfg.Cache.EvictionPolicy = strings.ToLower(strings.TrimSpace(cfg.Cache.EvictionPolicy))
	if cfg.Cache.EvictionPolicy == "" {
		cfg.Cache.EvictionPolicy = DefaultCacheEvictionPolicy
	}
	if cfg.Cache.MaxConcurrentWrites == 0 {
		cfg.Cache.MaxConcurrentWrites = DefaultCacheMaxConcurrentWrites
	}
	if cfg.Cache.MaxPopulateMemoryBytes == 0 {
		cfg.Cache.MaxPopulateMemoryBytes = DefaultCacheMaxPopulateMemoryBytes
	}
	if cfg.Cache.WarmOnWriteReservedFraction == 0 {
		cfg.Cache.WarmOnWriteReservedFraction = DefaultWarmOnWriteReservedFraction
	}

	// Broadcast defaults
	if cfg.Broadcast.ChunkSize == 0 {
		cfg.Broadcast.ChunkSize = DefaultBroadcastChunkSize
	}
	if cfg.Broadcast.ChannelBuffer == 0 {
		cfg.Broadcast.ChannelBuffer = DefaultBroadcastChannelBuffer
	}

	// Log defaults
	if cfg.Log.Level == "" {
		cfg.Log.Level = DefaultLogLevel
	}
	if cfg.Log.Format == "" {
		cfg.Log.Format = DefaultLogFormat
	}
}

// envInt64 reads env var `key` as a base-10 int64, tolerating surrounding whitespace. It
// returns ok=false when unset/blank. A non-empty but unparseable value (stray space that
// isn't just leading/trailing, a typo, "4GB") is logged and treated as unset — the override
// falls back to YAML/default instead of silently no-opping, so a bad override surfaces.
func envInt64(key string) (int64, bool) {
	raw, present := os.LookupEnv(key)
	if !present {
		return 0, false
	}
	val := strings.TrimSpace(raw)
	if val == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		log.Warn().Str("env", key).Str("value", raw).Msg("Ignoring malformed integer env override; using config/default")
		return 0, false
	}
	return n, true
}

// envInt is envInt64 narrowed to int (for count-style knobs).
func envInt(key string) (int, bool) {
	n, ok := envInt64(key)
	return int(n), ok
}

// envFloat is envInt64's float64 counterpart (trim + warn-on-malformed).
func envFloat(key string) (float64, bool) {
	raw, present := os.LookupEnv(key)
	if !present {
		return 0, false
	}
	val := strings.TrimSpace(raw)
	if val == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		log.Warn().Str("env", key).Str("value", raw).Msg("Ignoring malformed float env override; using config/default")
		return 0, false
	}
	return f, true
}

// envBool is envInt64's bool counterpart (trim + warn-on-malformed). Note: security-sensitive
// booleans that must fail closed do their own explicit parsing and are NOT routed through here.
func envBool(key string) (bool, bool) {
	raw, present := os.LookupEnv(key)
	if !present {
		return false, false
	}
	val := strings.TrimSpace(raw)
	if val == "" {
		return false, false
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		log.Warn().Str("env", key).Str("value", raw).Msg("Ignoring malformed boolean env override; using config/default")
		return false, false
	}
	return b, true
}

// applyEnvOverrides applies environment variable overrides to configuration.
func applyEnvOverrides(cfg *Config) {
	// Override upstream endpoint from environment
	if endpoint := os.Getenv("TAG_UPSTREAM_ENDPOINT"); endpoint != "" {
		cfg.Upstream.Endpoint = endpoint
	}

	// Override upstream region (SigV4 signing scope) from environment
	if region := os.Getenv("TAG_UPSTREAM_REGION"); region != "" {
		cfg.Upstream.Region = region
	}

	// Override upstream HTTP connection pool size from environment
	if poolSize := os.Getenv("TAG_MAX_IDLE_CONNS_PER_HOST"); poolSize != "" {
		if size, err := strconv.Atoi(poolSize); err == nil && size > 0 {
			cfg.Upstream.MaxIdleConnsPerHost = size
		}
	}

	// Disable cache from environment (explicit disable takes precedence)
	if disabled := os.Getenv("TAG_CACHE_DISABLED"); disabled == "true" || disabled == "1" {
		cfg.Cache.SetEnabled(false)
	}

	// Embedded cache configuration from environment (only if cache is enabled)
	if cfg.Cache.IsEnabled() {
		if diskPath := os.Getenv("TAG_CACHE_DISK_PATH"); diskPath != "" {
			cfg.Cache.DiskPath = diskPath
		}
		if maxDisk := os.Getenv("TAG_CACHE_MAX_DISK_USAGE"); maxDisk != "" {
			if size, err := strconv.ParseInt(maxDisk, 10, 64); err == nil && size >= 0 {
				cfg.Cache.MaxDiskUsageBytes = size
			}
		}
		if nodeID := os.Getenv("TAG_CACHE_NODE_ID"); nodeID != "" {
			cfg.Cache.NodeID = nodeID
		}
		if clusterAddr := os.Getenv("TAG_CACHE_CLUSTER_ADDR"); clusterAddr != "" {
			cfg.Cache.ClusterAddr = clusterAddr
		}
		if grpcAddr := os.Getenv("TAG_CACHE_GRPC_ADDR"); grpcAddr != "" {
			cfg.Cache.GRPCAddr = grpcAddr
		}
		if advertiseAddr := os.Getenv("TAG_CACHE_ADVERTISE_ADDR"); advertiseAddr != "" {
			cfg.Cache.AdvertiseAddr = advertiseAddr
		}
		if seedNodes := os.Getenv("TAG_CACHE_SEED_NODES"); seedNodes != "" {
			cfg.Cache.SeedNodes = splitEndpoints(seedNodes)
		}
		// Override gRPC auth from environment (enabled by default, only explicit "false"/"0" disables)
		if val := os.Getenv("TAG_CACHE_GRPC_AUTH"); val != "" {
			disabled := val == "false" || val == "0"
			cfg.Cache.SetGRPCAuth(!disabled)
		}
		// Override cache TTL from environment
		if val := os.Getenv("TAG_CACHE_TTL"); val != "" {
			if ttl, err := time.ParseDuration(val); err == nil && ttl > 0 {
				cfg.Cache.TTL = ttl
			}
		}
		// Override deletion-queue batch size from environment
		if val := os.Getenv("TAG_CACHE_DELETE_BATCH_SIZE"); val != "" {
			if size, err := strconv.Atoi(val); err == nil && size > 0 {
				cfg.Cache.DeleteBatchSize = size
			}
		}
		// Override startup recovery worker count from environment
		if workers, ok := envInt("TAG_CACHE_RECOVERY_WORKERS"); ok && workers > 0 {
			cfg.Cache.RecoveryWorkers = workers
		}
		// Enable parquet-aware footer prefetching from environment.
		if val := strings.ToLower(strings.TrimSpace(os.Getenv("TAG_CACHE_PARQUET_OPTIMIZATION"))); val != "" {
			cfg.Cache.ParquetOptimization = val == "true" || val == "1"
		}
		// Enable metadata caching on write from environment.
		if val := strings.ToLower(strings.TrimSpace(os.Getenv("TAG_CACHE_META_ON_WRITE"))); val != "" {
			cfg.Cache.MetaOnWrite = val == "true" || val == "1"
		}
		// Override the compaction source-read budget from environment
		// (bytes/second). An explicit 0 disables the throttle even when YAML
		// enabled it (ocache 0-disables convention); a negative value is
		// ignored so it cannot wipe a valid YAML setting.
		if bps, ok := envInt64("TAG_CACHE_COMPACTION_BPS"); ok && bps >= 0 {
			cfg.Cache.CompactionBytesPerSecond = bps
		}
		// Override eviction policy from environment ("lru" or "fifo").
		// Ignore a blank/whitespace-only value so it can't wipe a valid YAML or
		// default setting; an unrecognized non-empty value is caught by validate().
		if val := strings.ToLower(strings.TrimSpace(os.Getenv("TAG_CACHE_EVICTION_POLICY"))); val != "" {
			cfg.Cache.EvictionPolicy = val
		}
		// Override concurrent cache-write limit from environment
		if n, ok := envInt("TAG_CACHE_MAX_CONCURRENT_WRITES"); ok && n > 0 {
			cfg.Cache.MaxConcurrentWrites = n
		}
		// Override cache-populate memory budget from environment (negative disables)
		if n, ok := envInt64("TAG_CACHE_MAX_POPULATE_MEMORY"); ok && n != 0 {
			cfg.Cache.MaxPopulateMemoryBytes = n
		}
		// Override cache-warm-on-write from environment (accepts true/false/1/0)
		if b, ok := envBool("TAG_CACHE_WARM_ON_WRITE"); ok {
			cfg.Cache.WarmOnWrite = b
		}
		// Override block-aligned caching from environment (accepts true/false/1/0).
		if b, ok := envBool("TAG_CACHE_BLOCK_CACHING_ENABLED"); ok {
			cfg.Cache.SetBlockCachingEnabled(b)
		}
		// Override the block granularity from environment (0/unset keeps the default).
		if n, ok := envInt64("TAG_CACHE_BLOCK_SIZE"); ok && n > 0 {
			cfg.Cache.BlockSize = n
		}
		// Override the warm-on-write populate reservation fraction from environment.
		// f != 0 mirrors the sibling budget overrides: an env "0" means "use the
		// default" (per the documented 0-or-unset contract), not "disable" — a
		// negative value disables.
		if f, ok := envFloat("TAG_CACHE_WARM_ON_WRITE_RESERVED_FRACTION"); ok && f != 0 {
			cfg.Cache.WarmOnWriteReservedFraction = f
		}
	}
	// Clamp the reservation fraction to [0, 1] (negative disables the reservation).
	// Applied after defaults + env so both a stray config value and an env override
	// land in range. NaN is unordered — it slips past both comparisons — so a
	// malformed non-finite value (e.g. env/YAML "NaN") falls back to the default
	// rather than propagating into the integer budget cap as garbage.
	if math.IsNaN(cfg.Cache.WarmOnWriteReservedFraction) {
		cfg.Cache.WarmOnWriteReservedFraction = DefaultWarmOnWriteReservedFraction
	} else if cfg.Cache.WarmOnWriteReservedFraction < 0 {
		cfg.Cache.WarmOnWriteReservedFraction = 0
	} else if cfg.Cache.WarmOnWriteReservedFraction > 1 {
		cfg.Cache.WarmOnWriteReservedFraction = 1
	}

	// Override log level from environment
	if logLevel := os.Getenv("TAG_LOG_LEVEL"); logLevel != "" {
		cfg.Log.Level = logLevel
	}

	// Override log format from environment (json or console)
	if logFormat := os.Getenv("TAG_LOG_FORMAT"); logFormat != "" {
		cfg.Log.Format = logFormat
	}

	// Override HTTP port from environment
	if port := os.Getenv("TAG_HTTP_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil && p > 0 {
			cfg.Server.HTTPPort = p
		}
	}

	// Override the ingress in-flight request limit from environment
	if val := os.Getenv("TAG_MAX_INFLIGHT_REQUESTS"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			cfg.Server.MaxInflightRequests = n
		}
	}

	// Enable pprof from environment (disabled by default for security)
	if enabled := os.Getenv("TAG_PPROF_ENABLED"); enabled == "true" || enabled == "1" {
		cfg.Server.PprofEnabled = true
	}

	// Override TLS certificate from environment
	if certFile := os.Getenv("TAG_TLS_CERT_FILE"); certFile != "" {
		cfg.Server.TLSCertFile = certFile
	}
	if keyFile := os.Getenv("TAG_TLS_KEY_FILE"); keyFile != "" {
		cfg.Server.TLSKeyFile = keyFile
	}

	// Override transparent proxy from environment (enabled by default)
	if val := os.Getenv("TAG_TRANSPARENT_PROXY"); val != "" {
		enabled := val == "true" || val == "1"
		cfg.Upstream.SetTransparentProxy(enabled)
	}

	// Override authz cache TTL from environment
	if val := os.Getenv("TAG_AUTHZ_CACHE_TTL"); val != "" {
		if ttl, err := time.ParseDuration(val); err == nil && ttl > 0 {
			cfg.Credentials.AuthzCacheTTL = ttl
		}
	}
}

// validate checks that the final configuration is valid.
func validate(cfg *Config) error {
	if err := validateOrigin(&cfg.Upstream); err != nil {
		return err
	}
	// Skipped in origin-less mode: there is no endpoint to validate, and an empty
	// one is the state that signals the mode rather than a misconfiguration.
	if cfg.Upstream.HasOrigin() {
		if err := validateUpstreamEndpoint(cfg.Upstream.Endpoint, cfg.Upstream.IsTransparentProxy()); err != nil {
			return err
		}
	}
	if err := validateTLS(&cfg.Server); err != nil {
		return err
	}
	if err := validateEvictionPolicy(cfg.Cache.EvictionPolicy); err != nil {
		return err
	}
	return nil
}

// resolveClusterAuthDefault turns cross-node gRPC auth off by default in
// origin-less mode.
//
// gRPC auth derives its token from the AWS credentials TAG uses upstream. An
// origin-less deployment has no upstream and therefore no such credentials, so
// leaving the default on means either refusing to start or inventing dummy keys —
// and dummy keys are worse than no auth, because they look like authentication
// while proving nothing.
//
// The mode also carries its own trust model: origin-less TAG is an internal tier
// whose boundary is the network, and its peers sit inside that same boundary.
// Authenticating a node to its neighbour there adds ceremony, not safety.
//
// Only the *default* moves. An explicit true is still honoured (and still fails
// fast without credentials), so an operator who wants it can have it — the
// resolved value also lands in the startup config log, so the choice is visible
// rather than implied.
func resolveClusterAuthDefault(cfg *Config) {
	if !cfg.Upstream.HasOrigin() && cfg.Cache.GRPCAuth == nil {
		cfg.Cache.SetGRPCAuth(false)
	}
}

// validateOrigin rejects asking for origin-less mode while also configuring an
// upstream. The two are contradictory, and resolving it silently either way would
// pick a behaviour the operator did not ask for: honouring the endpoint turns a
// cache-only tier into a proxy, and honouring the flag discards a configured
// origin. Fail at startup instead.
func validateOrigin(u *UpstreamConfig) error {
	if u.Disabled && u.Endpoint != "" {
		return fmt.Errorf(
			"upstream.disabled cannot be combined with upstream.endpoint %q: origin-less mode has no upstream, so configure one or the other",
			u.Endpoint,
		)
	}
	return nil
}

// validateEvictionPolicy rejects an unrecognized eviction policy so a typo (e.g.
// "fif0") fails fast at startup with a clear error, instead of being forwarded to
// ocache where it silently degrades to the LRU fallback — quietly not applying the
// policy the operator asked for.
func validateEvictionPolicy(policy string) error {
	switch policy {
	case EvictionPolicyLRU, EvictionPolicyFIFO:
		return nil
	default:
		return fmt.Errorf("invalid cache.eviction_policy %q: must be %q or %q", policy, EvictionPolicyLRU, EvictionPolicyFIFO)
	}
}

// validateTLS checks that TLS configuration is consistent.
// Both cert and key must be provided together, or neither.
func validateTLS(server *ServerConfig) error {
	hasCert := server.TLSCertFile != ""
	hasKey := server.TLSKeyFile != ""
	if hasCert != hasKey {
		if hasCert {
			return fmt.Errorf("tls_cert_file is set but tls_key_file is missing")
		}
		return fmt.Errorf("tls_key_file is set but tls_cert_file is missing")
	}
	return nil
}

// IsTigrisEndpoint reports whether the endpoint host is localhost or a Tigris
// domain (*.tigris.dev, *.storage.dev). Transparent proxy mode requires a Tigris
// endpoint; signing mode works with any S3-compatible endpoint. Returns false if
// the endpoint cannot be parsed.
func IsTigrisEndpoint(endpoint string) bool {
	u, err := url.Parse(endpoint)
	if err != nil {
		return false
	}

	host := u.Hostname() // strips port if present
	if host == "localhost" {
		return true
	}
	return strings.HasSuffix(host, ".tigris.dev") || strings.HasSuffix(host, ".storage.dev")
}

// validateUpstreamEndpoint ensures the upstream endpoint is a well-formed
// absolute http(s) URL with a host (checked in every mode, since the signing
// path builds upstream requests from it). In transparent proxy mode it must
// additionally be a Tigris domain or localhost, because the X-Tigris-Proxy-*
// identity headers are meaningful only to Tigris; signing mode permits any
// S3-compatible host.
func validateUpstreamEndpoint(endpoint string, transparent bool) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("invalid upstream endpoint %q: %w", endpoint, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("upstream endpoint %q must be an absolute http:// or https:// URL", endpoint)
	}
	if u.Hostname() == "" {
		return fmt.Errorf("upstream endpoint %q must include a host", endpoint)
	}
	if transparent && !IsTigrisEndpoint(endpoint) {
		return fmt.Errorf("upstream endpoint %q is not allowed in transparent proxy mode: host must be localhost, *.tigris.dev, or *.storage.dev (use signing mode for other S3-compatible services)", endpoint)
	}
	return nil
}

// splitEndpoints splits a comma-separated string into a slice of endpoints.
func splitEndpoints(s string) []string {
	var endpoints []string
	for _, ep := range strings.Split(s, ",") {
		ep = strings.TrimSpace(ep)
		if ep != "" {
			endpoints = append(endpoints, ep)
		}
	}
	return endpoints
}
