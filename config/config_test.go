package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoad_ValidYAML(t *testing.T) {
	// Create a temporary config file
	content := `
server:
  http_port: 9000
  bind_ip: "127.0.0.1"
upstream:
  endpoint: "https://custom.endpoint.com"
  region: "us-west-2"
cache:
  enabled: true
  endpoints:
    - "cache-0:9000"
    - "cache-1:9000"
  ttl: 10m
  size_threshold: 104857600
log:
  level: "debug"
`

	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify server config
	if cfg.Server.HTTPPort != 9000 {
		t.Errorf("HTTPPort = %d, want 9000", cfg.Server.HTTPPort)
	}
	if cfg.Server.BindIP != "127.0.0.1" {
		t.Errorf("BindIP = %q, want 127.0.0.1", cfg.Server.BindIP)
	}

	// Verify upstream config
	if cfg.Upstream.Endpoint != "https://custom.endpoint.com" {
		t.Errorf("Upstream.Endpoint = %q, want https://custom.endpoint.com", cfg.Upstream.Endpoint)
	}
	if cfg.Upstream.Region != "us-west-2" {
		t.Errorf("Upstream.Region = %q, want us-west-2", cfg.Upstream.Region)
	}

	// Verify cache config
	if !cfg.Cache.Enabled {
		t.Error("Cache.Enabled = false, want true")
	}
	if len(cfg.Cache.Endpoints) != 2 {
		t.Errorf("Cache.Endpoints length = %d, want 2", len(cfg.Cache.Endpoints))
	}
	if cfg.Cache.TTL != 10*time.Minute {
		t.Errorf("Cache.TTL = %v, want 10m", cfg.Cache.TTL)
	}
	if cfg.Cache.SizeThreshold != 104857600 {
		t.Errorf("Cache.SizeThreshold = %d, want 104857600", cfg.Cache.SizeThreshold)
	}

	// Verify log config
	if cfg.Log.Level != "debug" {
		t.Errorf("Log.Level = %q, want debug", cfg.Log.Level)
	}
}

func TestLoad_Defaults(t *testing.T) {
	// Create a minimal config file (empty YAML)
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte("{}"), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify default values are applied
	if cfg.Server.HTTPPort != DefaultHTTPPort {
		t.Errorf("HTTPPort = %d, want %d", cfg.Server.HTTPPort, DefaultHTTPPort)
	}
	if cfg.Server.BindIP != DefaultBindIP {
		t.Errorf("BindIP = %q, want %q", cfg.Server.BindIP, DefaultBindIP)
	}
	if cfg.Upstream.Endpoint != DefaultUpstreamEndpoint {
		t.Errorf("Upstream.Endpoint = %q, want %q", cfg.Upstream.Endpoint, DefaultUpstreamEndpoint)
	}
	if cfg.Upstream.Region != DefaultUpstreamRegion {
		t.Errorf("Upstream.Region = %q, want %q", cfg.Upstream.Region, DefaultUpstreamRegion)
	}
	if cfg.Cache.TTL != DefaultCacheTTL {
		t.Errorf("Cache.TTL = %v, want %v", cfg.Cache.TTL, DefaultCacheTTL)
	}
	if cfg.Cache.SizeThreshold != DefaultCacheSizeThreshold {
		t.Errorf("Cache.SizeThreshold = %d, want %d", cfg.Cache.SizeThreshold, DefaultCacheSizeThreshold)
	}
	if cfg.Log.Level != DefaultLogLevel {
		t.Errorf("Log.Level = %q, want %q", cfg.Log.Level, DefaultLogLevel)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	// Create a config file
	content := `
server:
  http_port: 8080
upstream:
  endpoint: "https://default.endpoint.com"
log:
  level: "info"
`

	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	// Set environment variables
	t.Setenv("TAG_UPSTREAM_ENDPOINT", "https://env.endpoint.com")
	t.Setenv("TAG_OCACHE_ENDPOINTS", "env-cache-0:9000,env-cache-1:9000")
	t.Setenv("TAG_LOG_LEVEL", "warn")

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify env overrides
	if cfg.Upstream.Endpoint != "https://env.endpoint.com" {
		t.Errorf("Upstream.Endpoint = %q, want https://env.endpoint.com", cfg.Upstream.Endpoint)
	}
	if len(cfg.Cache.Endpoints) != 2 {
		t.Errorf("Cache.Endpoints length = %d, want 2", len(cfg.Cache.Endpoints))
	}
	if cfg.Cache.Endpoints[0] != "env-cache-0:9000" {
		t.Errorf("Cache.Endpoints[0] = %q, want env-cache-0:9000", cfg.Cache.Endpoints[0])
	}
	if cfg.Log.Level != "warn" {
		t.Errorf("Log.Level = %q, want warn", cfg.Log.Level)
	}
}

func TestLoad_CacheDisabledByEnv(t *testing.T) {
	// Create a config file with cache enabled
	content := `
cache:
  enabled: true
  endpoints:
    - "cache-0:9000"
`

	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	// Set environment variable to disable cache
	t.Setenv("TAG_CACHE_DISABLED", "true")

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify cache is disabled
	if cfg.Cache.Enabled {
		t.Error("Cache.Enabled = true, want false (disabled by env)")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	// Create an invalid YAML file
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte("invalid: yaml: content: ["), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	_, err := Load(tmpFile)
	if err == nil {
		t.Error("Load() expected error for invalid YAML, got nil")
	}
}

func TestLoad_NonexistentFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("Load() expected error for nonexistent file, got nil")
	}
}

func TestNewDefault(t *testing.T) {
	cfg := NewDefault()

	// Verify default values
	if cfg.Server.HTTPPort != DefaultHTTPPort {
		t.Errorf("HTTPPort = %d, want %d", cfg.Server.HTTPPort, DefaultHTTPPort)
	}
	if cfg.Upstream.Endpoint != DefaultUpstreamEndpoint {
		t.Errorf("Upstream.Endpoint = %q, want %q", cfg.Upstream.Endpoint, DefaultUpstreamEndpoint)
	}
	if cfg.Cache.TTL != DefaultCacheTTL {
		t.Errorf("Cache.TTL = %v, want %v", cfg.Cache.TTL, DefaultCacheTTL)
	}
}

func TestSplitEndpoints(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single endpoint",
			input:    "cache-0:9000",
			expected: []string{"cache-0:9000"},
		},
		{
			name:     "multiple endpoints",
			input:    "cache-0:9000,cache-1:9000,cache-2:9000",
			expected: []string{"cache-0:9000", "cache-1:9000", "cache-2:9000"},
		},
		{
			name:     "endpoints with spaces",
			input:    "cache-0:9000, cache-1:9000 , cache-2:9000",
			expected: []string{"cache-0:9000", "cache-1:9000", "cache-2:9000"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "only commas",
			input:    ",,,",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitEndpoints(tt.input)

			if len(result) != len(tt.expected) {
				t.Errorf("splitEndpoints(%q) length = %d, want %d", tt.input, len(result), len(tt.expected))
				return
			}

			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("splitEndpoints(%q)[%d] = %q, want %q", tt.input, i, v, tt.expected[i])
				}
			}
		})
	}
}

func TestCacheAutoEnabled(t *testing.T) {
	// Create a config file with cache endpoints but enabled not explicitly set
	content := `
cache:
  endpoints:
    - "cache-0:9000"
    - "cache-1:9000"
`

	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify cache is auto-enabled when endpoints are configured
	if !cfg.Cache.Enabled {
		t.Error("Cache.Enabled = false, want true (auto-enabled with endpoints)")
	}
}
