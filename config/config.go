// Package config provides configuration management for TAG.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Default configuration values.
const (
	// DefaultHTTPPort is the default S3 API port.
	DefaultHTTPPort = 8080

	// DefaultBindIP is the default bind address.
	DefaultBindIP = "0.0.0.0"

	// DefaultUpstreamEndpoint is the default Tigris S3 endpoint.
	DefaultUpstreamEndpoint = "https://t3.storage.dev"

	// DefaultUpstreamRegion is the default AWS region for signing.
	DefaultUpstreamRegion = "auto"

	// DefaultCacheTTL is the default cache TTL.
	DefaultCacheTTL = 60 * time.Minute

	// DefaultCacheSizeThreshold is the max object size to cache (1GB).
	DefaultCacheSizeThreshold = 1024 * 1024 * 1024

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
}

// UpstreamConfig holds Tigris endpoint configuration.
type UpstreamConfig struct {
	Endpoint            string `yaml:"endpoint"`                // Tigris S3 endpoint (e.g., https://fly.storage.tigris.dev)
	Region              string `yaml:"region"`                  // AWS region for signing (default: auto)
	MaxIdleConnsPerHost int    `yaml:"max_idle_conns_per_host"` // HTTP connection pool size per host (default: 100)
	TransparentProxy    *bool  `yaml:"transparent_proxy"`       // Forward client requests as-is with proxy headers (default: true when nil)
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
	TTL           time.Duration `yaml:"ttl"`            // Default cache TTL (default: 60m)
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
}

// IsEnabled returns whether caching is enabled.
// Returns true by default if not explicitly set.
func (c *CacheConfig) IsEnabled() bool {
	if c.Enabled == nil {
		return true // Default to enabled
	}
	return *c.Enabled
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
	ChannelBuffer int `yaml:"channel_buffer"` // Buffer size per listener in chunks (default: 32)
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

	// Apply defaults
	applyDefaults(&cfg)

	// Override from environment variables
	applyEnvOverrides(&cfg)

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
	applyDefaults(cfg)
	applyEnvOverrides(cfg)
	if err := validate(cfg); err != nil {
		panic(fmt.Sprintf("invalid default configuration: %v", err))
	}
	return cfg
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
	// PprofEnabled defaults to false (disabled for security)
	// Use TAG_PPROF_ENABLED=true to enable

	// Upstream defaults
	if cfg.Upstream.Endpoint == "" {
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

// applyEnvOverrides applies environment variable overrides to configuration.
func applyEnvOverrides(cfg *Config) {
	// Override upstream endpoint from environment
	if endpoint := os.Getenv("TAG_UPSTREAM_ENDPOINT"); endpoint != "" {
		cfg.Upstream.Endpoint = endpoint
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

	// Enable pprof from environment (disabled by default for security)
	if enabled := os.Getenv("TAG_PPROF_ENABLED"); enabled == "true" || enabled == "1" {
		cfg.Server.PprofEnabled = true
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
	if err := validateUpstreamEndpoint(cfg.Upstream.Endpoint); err != nil {
		return err
	}
	return nil
}

// validateUpstreamEndpoint ensures the upstream endpoint is an allowed Tigris domain or localhost.
func validateUpstreamEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("invalid upstream endpoint %q: %w", endpoint, err)
	}

	host := u.Hostname() // strips port if present
	if host == "localhost" {
		return nil
	}
	if strings.HasSuffix(host, ".tigris.dev") || strings.HasSuffix(host, ".storage.dev") {
		return nil
	}

	return fmt.Errorf("upstream endpoint %q is not allowed: host must be localhost, *.tigris.dev, or *.storage.dev", endpoint)
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
