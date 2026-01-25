// Package config provides configuration management for TAG.
package config

import (
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

	// DefaultCacheConnectionPoolSize is the default number of gRPC connections per ocache node.
	// Higher values improve throughput for high-concurrency workloads.
	DefaultCacheConnectionPoolSize = 4

	// DefaultLogLevel is the default log level.
	DefaultLogLevel = "info"

	// DefaultLogFormat is the default log format.
	// Use "json" for production (fast) or "console" for development (human-readable).
	DefaultLogFormat = "json"

	// DefaultBroadcastChunkSize is the default chunk size for streaming (64KB).
	DefaultBroadcastChunkSize = 64 * 1024

	// DefaultBroadcastChannelBuffer is the default buffer size per listener (32 chunks = ~2MB).
	DefaultBroadcastChannelBuffer = 32

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
	Endpoint            string `yaml:"endpoint"`               // Tigris S3 endpoint (e.g., https://fly.storage.tigris.dev)
	Region              string `yaml:"region"`                 // AWS region for signing (default: auto)
	MaxIdleConnsPerHost int    `yaml:"max_idle_conns_per_host"` // HTTP connection pool size per host (default: 500)
}

// CredentialsConfig holds credential store configuration.
// Credentials are loaded from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.
type CredentialsConfig struct {
	// Reserved for future configuration options
}

// CacheConfig holds ocache client configuration.
type CacheConfig struct {
	Enabled            bool          `yaml:"enabled"`              // Enable caching (auto-enabled if endpoints configured)
	Endpoints          []string      `yaml:"endpoints"`            // ocache cluster endpoints (e.g., ["ocache-0:9000", "ocache-1:9000"])
	TTL                time.Duration `yaml:"ttl"`                  // Default cache TTL (default: 60m)
	SizeThreshold      int64         `yaml:"size_threshold"`       // Max object size to cache in bytes (default: 1GB)
	ConnectionPoolSize int           `yaml:"connection_pool_size"` // Number of gRPC connections per ocache node (default: 64)
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

	return &cfg, nil
}

// NewDefault creates a Config with default values.
func NewDefault() *Config {
	cfg := &Config{}
	applyDefaults(cfg)
	applyEnvOverrides(cfg)
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

	// Cache defaults - auto-enabled when endpoints are configured.
	// Use TAG_CACHE_DISABLED=true environment variable to disable.
	if len(cfg.Cache.Endpoints) > 0 {
		cfg.Cache.Enabled = true
	}
	if cfg.Cache.TTL == 0 {
		cfg.Cache.TTL = DefaultCacheTTL
	}
	if cfg.Cache.SizeThreshold == 0 {
		cfg.Cache.SizeThreshold = DefaultCacheSizeThreshold
	}
	if cfg.Cache.ConnectionPoolSize == 0 {
		cfg.Cache.ConnectionPoolSize = DefaultCacheConnectionPoolSize
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

	// Override ocache endpoints from environment (comma-separated)
	if endpoints := os.Getenv("TAG_OCACHE_ENDPOINTS"); endpoints != "" {
		cfg.Cache.Endpoints = splitEndpoints(endpoints)
	}

	// Auto-enable cache when endpoints are configured via environment
	// This must run AFTER endpoints are set from environment
	if len(cfg.Cache.Endpoints) > 0 && !cfg.Cache.Enabled {
		cfg.Cache.Enabled = true
	}

	// Disable cache from environment (explicit disable takes precedence)
	if disabled := os.Getenv("TAG_CACHE_DISABLED"); disabled == "true" || disabled == "1" {
		cfg.Cache.Enabled = false
	}

	// Override ocache connection pool size from environment
	if poolSize := os.Getenv("TAG_OCACHE_CONNECTION_POOL_SIZE"); poolSize != "" {
		if size, err := strconv.Atoi(poolSize); err == nil && size > 0 {
			cfg.Cache.ConnectionPoolSize = size
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

	// Enable pprof from environment (disabled by default for security)
	if enabled := os.Getenv("TAG_PPROF_ENABLED"); enabled == "true" || enabled == "1" {
		cfg.Server.PprofEnabled = true
	}
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
