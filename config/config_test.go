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
  endpoint: "https://fly.storage.tigris.dev"
  region: "us-west-2"
cache:
  enabled: true
  ttl: 10m
  size_threshold: 104857600
  disk_path: "/var/cache/custom"
  node_id: "test-node-1"
  seed_nodes:
    - "node-0:7000"
    - "node-1:7000"
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
	if cfg.Upstream.Endpoint != "https://fly.storage.tigris.dev" {
		t.Errorf("Upstream.Endpoint = %q, want https://fly.storage.tigris.dev", cfg.Upstream.Endpoint)
	}
	if cfg.Upstream.Region != "us-west-2" {
		t.Errorf("Upstream.Region = %q, want us-west-2", cfg.Upstream.Region)
	}

	// Verify cache config
	if !cfg.Cache.IsEnabled() {
		t.Error("Cache.IsEnabled() = false, want true")
	}
	if cfg.Cache.TTL != 10*time.Minute {
		t.Errorf("Cache.TTL = %v, want 10m", cfg.Cache.TTL)
	}
	if cfg.Cache.SizeThreshold != 104857600 {
		t.Errorf("Cache.SizeThreshold = %d, want 104857600", cfg.Cache.SizeThreshold)
	}
	if cfg.Cache.DiskPath != "/var/cache/custom" {
		t.Errorf("Cache.DiskPath = %q, want /var/cache/custom", cfg.Cache.DiskPath)
	}
	if cfg.Cache.NodeID != "test-node-1" {
		t.Errorf("Cache.NodeID = %q, want test-node-1", cfg.Cache.NodeID)
	}
	if len(cfg.Cache.SeedNodes) != 2 {
		t.Errorf("Cache.SeedNodes length = %d, want 2", len(cfg.Cache.SeedNodes))
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
  endpoint: "https://fly.storage.tigris.dev"
log:
  level: "info"
`

	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	// Set environment variables
	t.Setenv("TAG_UPSTREAM_ENDPOINT", "https://t3.storage.dev")
	t.Setenv("TAG_CACHE_NODE_ID", "env-node-1")
	t.Setenv("TAG_CACHE_DISK_PATH", "/env/cache/path")
	t.Setenv("TAG_CACHE_SEED_NODES", "env-node-0:7000,env-node-1:7000")
	t.Setenv("TAG_LOG_LEVEL", "warn")

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify env overrides
	if cfg.Upstream.Endpoint != "https://t3.storage.dev" {
		t.Errorf("Upstream.Endpoint = %q, want https://t3.storage.dev", cfg.Upstream.Endpoint)
	}
	if cfg.Cache.NodeID != "env-node-1" {
		t.Errorf("Cache.NodeID = %q, want env-node-1", cfg.Cache.NodeID)
	}
	if cfg.Cache.DiskPath != "/env/cache/path" {
		t.Errorf("Cache.DiskPath = %q, want /env/cache/path", cfg.Cache.DiskPath)
	}
	if len(cfg.Cache.SeedNodes) != 2 {
		t.Errorf("Cache.SeedNodes length = %d, want 2", len(cfg.Cache.SeedNodes))
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
  node_id: "test-node"
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
	if cfg.Cache.IsEnabled() {
		t.Error("Cache.IsEnabled() = true, want false (disabled by env)")
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

func TestCacheEnabledByDefault(t *testing.T) {
	// Create a minimal config file
	content := `
server:
  http_port: 8080
`

	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify cache is enabled by default (embedded mode)
	if !cfg.Cache.IsEnabled() {
		t.Error("Cache.IsEnabled() = false, want true (enabled by default)")
	}
}

func TestCacheDisabledByConfig(t *testing.T) {
	// Create a config file with cache explicitly disabled
	content := `
cache:
  enabled: false
`

	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify cache is disabled when explicitly set to false in config
	if cfg.Cache.IsEnabled() {
		t.Error("Cache.IsEnabled() = true, want false (explicitly disabled in config)")
	}
}

func TestTransparentProxy_EnabledByDefault(t *testing.T) {
	cfg := NewDefault()
	if !cfg.Upstream.IsTransparentProxy() {
		t.Error("IsTransparentProxy() = false, want true (enabled by default)")
	}
}

func TestTransparentProxy_DisabledByYAML(t *testing.T) {
	content := `
upstream:
  transparent_proxy: false
`
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Upstream.IsTransparentProxy() {
		t.Error("IsTransparentProxy() = true, want false")
	}
}

func TestTransparentProxy_DisabledByEnv(t *testing.T) {
	t.Setenv("TAG_TRANSPARENT_PROXY", "false")

	cfg := NewDefault()
	if cfg.Upstream.IsTransparentProxy() {
		t.Error("IsTransparentProxy() = true, want false (disabled by env)")
	}
}

func TestTransparentProxy_DisabledByEnv_NumericZero(t *testing.T) {
	t.Setenv("TAG_TRANSPARENT_PROXY", "0")

	cfg := NewDefault()
	if cfg.Upstream.IsTransparentProxy() {
		t.Error("IsTransparentProxy() = true, want false (disabled by env with '0')")
	}
}

func TestValidateUpstreamEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		wantErr  bool
	}{
		{"tigris.dev domain", "https://fly.storage.tigris.dev", false},
		{"storage.dev domain", "https://t3.storage.dev", false},
		{"localhost", "http://localhost:8080", false},
		{"localhost no port", "http://localhost", false},
		{"disallowed domain", "https://evil.example.com", true},
		{"subdomain of tigris.dev", "https://sub.tigris.dev", false},
		{"not a suffix match", "https://nottrigris.dev", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUpstreamEndpoint(tt.endpoint)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateUpstreamEndpoint(%q) error = %v, wantErr %v", tt.endpoint, err, tt.wantErr)
			}
		})
	}
}

func TestLoad_InvalidUpstreamEndpoint(t *testing.T) {
	content := `
upstream:
  endpoint: "https://evil.example.com"
`
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	_, err := Load(tmpFile)
	if err == nil {
		t.Error("Load() should return error for disallowed upstream endpoint")
	}
}

func TestCacheDefaultValues(t *testing.T) {
	cfg := NewDefault()

	// Verify cache is enabled by default
	if !cfg.Cache.IsEnabled() {
		t.Error("Cache.IsEnabled() = false, want true (enabled by default)")
	}

	// Verify default disk path
	if cfg.Cache.DiskPath != DefaultCacheDiskPath {
		t.Errorf("Cache.DiskPath = %q, want %q", cfg.Cache.DiskPath, DefaultCacheDiskPath)
	}

	// Verify default cluster and gRPC addresses
	if cfg.Cache.ClusterAddr != DefaultCacheClusterAddr {
		t.Errorf("Cache.ClusterAddr = %q, want %q", cfg.Cache.ClusterAddr, DefaultCacheClusterAddr)
	}
	if cfg.Cache.GRPCAddr != DefaultCacheGRPCAddr {
		t.Errorf("Cache.GRPCAddr = %q, want %q", cfg.Cache.GRPCAddr, DefaultCacheGRPCAddr)
	}

	// Verify gRPC auth is enabled by default
	if !cfg.Cache.IsGRPCAuthEnabled() {
		t.Error("Cache.IsGRPCAuthEnabled() = false, want true (enabled by default)")
	}
}

func TestGRPCAuth_DisabledByYAML(t *testing.T) {
	content := `
cache:
  grpc_auth: false
`
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Cache.IsGRPCAuthEnabled() {
		t.Error("IsGRPCAuthEnabled() = true, want false")
	}
}

func TestGRPCAuth_DisabledByEnv(t *testing.T) {
	t.Setenv("TAG_CACHE_GRPC_AUTH", "false")

	cfg := NewDefault()
	if cfg.Cache.IsGRPCAuthEnabled() {
		t.Error("IsGRPCAuthEnabled() = true, want false (disabled by env)")
	}
}

func TestGRPCAuth_EnabledByEnv(t *testing.T) {
	t.Setenv("TAG_CACHE_GRPC_AUTH", "true")

	cfg := NewDefault()
	if !cfg.Cache.IsGRPCAuthEnabled() {
		t.Error("IsGRPCAuthEnabled() = false, want true (enabled by env)")
	}
}

func TestGRPCAuth_UnrecognizedValueKeepsEnabled(t *testing.T) {
	// Unrecognized values like "True", "yes", "TRUE" must not silently disable auth
	for _, val := range []string{"True", "TRUE", "yes", "enabled", "typo"} {
		t.Run(val, func(t *testing.T) {
			t.Setenv("TAG_CACHE_GRPC_AUTH", val)
			cfg := NewDefault()
			if !cfg.Cache.IsGRPCAuthEnabled() {
				t.Errorf("IsGRPCAuthEnabled() = false with TAG_CACHE_GRPC_AUTH=%q, want true (only 'false'/'0' should disable)", val)
			}
		})
	}
}

func TestHTTPPort_OverrideByEnv(t *testing.T) {
	t.Setenv("TAG_HTTP_PORT", "9999")
	cfg := NewDefault()
	if cfg.Server.HTTPPort != 9999 {
		t.Errorf("Server.HTTPPort = %d, want 9999", cfg.Server.HTTPPort)
	}
}

func TestTLSEnabled(t *testing.T) {
	tests := []struct {
		name     string
		cert     string
		key      string
		expected bool
	}{
		{"both set", "/path/cert.pem", "/path/key.pem", true},
		{"neither set", "", "", false},
		{"only cert", "/path/cert.pem", "", false},
		{"only key", "", "/path/key.pem", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := ServerConfig{TLSCertFile: tt.cert, TLSKeyFile: tt.key}
			if got := s.TLSEnabled(); got != tt.expected {
				t.Errorf("TLSEnabled() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTLS_FromYAML(t *testing.T) {
	content := `
server:
  tls_cert_file: "/etc/tag/tls/cert.pem"
  tls_key_file: "/etc/tag/tls/key.pem"
`
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Server.TLSCertFile != "/etc/tag/tls/cert.pem" {
		t.Errorf("TLSCertFile = %q, want /etc/tag/tls/cert.pem", cfg.Server.TLSCertFile)
	}
	if cfg.Server.TLSKeyFile != "/etc/tag/tls/key.pem" {
		t.Errorf("TLSKeyFile = %q, want /etc/tag/tls/key.pem", cfg.Server.TLSKeyFile)
	}
	if !cfg.Server.TLSEnabled() {
		t.Error("TLSEnabled() = false, want true")
	}
}

func TestTLS_FromEnv(t *testing.T) {
	t.Setenv("TAG_TLS_CERT_FILE", "/env/cert.pem")
	t.Setenv("TAG_TLS_KEY_FILE", "/env/key.pem")

	cfg := NewDefault()
	if cfg.Server.TLSCertFile != "/env/cert.pem" {
		t.Errorf("TLSCertFile = %q, want /env/cert.pem", cfg.Server.TLSCertFile)
	}
	if cfg.Server.TLSKeyFile != "/env/key.pem" {
		t.Errorf("TLSKeyFile = %q, want /env/key.pem", cfg.Server.TLSKeyFile)
	}
}

func TestTLS_EnvOverridesFile(t *testing.T) {
	content := `
server:
  tls_cert_file: "/file/cert.pem"
  tls_key_file: "/file/key.pem"
`
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	t.Setenv("TAG_TLS_CERT_FILE", "/env/cert.pem")
	t.Setenv("TAG_TLS_KEY_FILE", "/env/key.pem")

	cfg, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Server.TLSCertFile != "/env/cert.pem" {
		t.Errorf("TLSCertFile = %q, want /env/cert.pem (env override)", cfg.Server.TLSCertFile)
	}
	if cfg.Server.TLSKeyFile != "/env/key.pem" {
		t.Errorf("TLSKeyFile = %q, want /env/key.pem (env override)", cfg.Server.TLSKeyFile)
	}
}

func TestTLS_ValidationOnlyCert(t *testing.T) {
	content := `
server:
  tls_cert_file: "/path/cert.pem"
`
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	_, err := Load(tmpFile)
	if err == nil {
		t.Error("Load() should return error when only tls_cert_file is set")
	}
}

func TestTLS_ValidationOnlyKey(t *testing.T) {
	content := `
server:
  tls_key_file: "/path/key.pem"
`
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	_, err := Load(tmpFile)
	if err == nil {
		t.Error("Load() should return error when only tls_key_file is set")
	}
}

func TestTLS_DisabledByDefault(t *testing.T) {
	cfg := NewDefault()
	if cfg.Server.TLSEnabled() {
		t.Error("TLSEnabled() = true, want false (disabled by default)")
	}
}
