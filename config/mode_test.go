package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeModeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMode_DefaultIsTransparent(t *testing.T) {
	cfg := NewDefault()
	if got := cfg.ResolvedMode(); got != ModeTransparent {
		t.Fatalf("ResolvedMode() = %q, want %q", got, ModeTransparent)
	}
	if cfg.IsTiered() {
		t.Fatal("IsTiered() = true for default config")
	}
}

func TestMode_TransparentProxyFalseResolvesSigning(t *testing.T) {
	cfg := NewDefault()
	cfg.Upstream.SetTransparentProxy(false)
	if got := cfg.ResolvedMode(); got != ModeSigning {
		t.Fatalf("ResolvedMode() = %q, want %q", got, ModeSigning)
	}
}

func TestMode_OverrideByEnv(t *testing.T) {
	t.Setenv("TAG_MODE", ModeTiered)
	var cfg Config
	applyDefaults(&cfg)
	applyEnvOverrides(&cfg)
	if err := validate(&cfg); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !cfg.IsTiered() {
		t.Fatal("IsTiered() = false with TAG_MODE=tiered")
	}
}

func TestMode_InvalidRejected(t *testing.T) {
	var cfg Config
	applyDefaults(&cfg)
	cfg.Mode = "sideways"
	if err := validate(&cfg); err == nil {
		t.Fatal("validate accepted an invalid mode")
	}
}

func TestMode_ContradictionWithTransparentProxyRejected(t *testing.T) {
	cases := []struct {
		mode string
		tp   bool
	}{
		{ModeTransparent, false},
		{ModeSigning, true},
		{ModeTiered, true},
		{ModeTiered, false},
	}
	for _, tc := range cases {
		var cfg Config
		applyDefaults(&cfg)
		cfg.Mode = tc.mode
		cfg.Upstream.SetTransparentProxy(tc.tp)
		if err := validate(&cfg); err == nil {
			t.Fatalf("validate accepted mode=%q with transparent_proxy=%v", tc.mode, tc.tp)
		}
	}
}

func TestMode_ConsistentTransparentProxyAllowed(t *testing.T) {
	cases := []struct {
		mode string
		tp   bool
	}{
		{ModeTransparent, true},
		{ModeSigning, false},
	}
	for _, tc := range cases {
		var cfg Config
		applyDefaults(&cfg)
		cfg.Mode = tc.mode
		cfg.Upstream.SetTransparentProxy(tc.tp)
		if err := validate(&cfg); err != nil {
			t.Fatalf("validate rejected consistent mode=%q transparent_proxy=%v: %v", tc.mode, tc.tp, err)
		}
	}
}

func TestMode_TieredRejectsBlockCaching(t *testing.T) {
	var cfg Config
	applyDefaults(&cfg)
	cfg.Mode = ModeTiered
	cfg.Cache.SetBlockCachingEnabled(true)
	if err := validate(&cfg); err == nil {
		t.Fatal("validate accepted tiered mode with block caching enabled")
	}
}

func TestMode_TieredDefaultsBlockCachingOff(t *testing.T) {
	var cfg Config
	applyDefaults(&cfg)
	cfg.Mode = ModeTiered
	if err := validate(&cfg); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Cache.IsBlockCachingEnabled() {
		t.Fatal("block caching still enabled in tiered mode")
	}
}

func TestMode_TieredSelectsCASCoordination(t *testing.T) {
	var cfg Config
	applyDefaults(&cfg)
	cfg.Mode = ModeTiered
	if err := validate(&cfg); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Cache.IsLegacyCoordination() {
		t.Fatal("tiered mode did not auto-select CAS coordination")
	}
}

func TestMode_TieredRejectsExplicitLegacyCoordination(t *testing.T) {
	var cfg Config
	applyDefaults(&cfg)
	cfg.Mode = ModeTiered
	cfg.Cache.SetLegacyCoordination(true)
	if err := validate(&cfg); err == nil {
		t.Fatal("validate accepted tiered mode with legacy coordination forced on")
	}
}

// The env override applies before validation, so forcing legacy coordination
// through the environment is the same contradiction as forcing it in yaml.
func TestMode_TieredRejectsLegacyCoordinationEnv(t *testing.T) {
	t.Setenv("TAG_MODE", ModeTiered)
	t.Setenv("TAG_CACHE_LEGACY_COORDINATION", "true")
	path := writeModeConfig(t, `
upstream:
  endpoint: "https://t3.storage.dev"
`)
	if _, err := Load(path); err == nil {
		t.Fatal("Load accepted tiered mode with legacy coordination forced by env")
	}
}

func TestMode_TieredRequiresCache(t *testing.T) {
	var cfg Config
	applyDefaults(&cfg)
	cfg.Mode = ModeTiered
	disabled := false
	cfg.Cache.Enabled = &disabled
	if err := validate(&cfg); err == nil {
		t.Fatal("validate accepted tiered mode with the cache disabled")
	}
}

func TestMode_LoadFromYAML(t *testing.T) {
	path := writeModeConfig(t, `
mode: tiered
upstream:
  endpoint: "https://t3.storage.dev"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.IsTiered() {
		t.Fatal("IsTiered() = false for mode: tiered YAML")
	}
	if cfg.Cache.IsBlockCachingEnabled() {
		t.Fatal("block caching enabled in tiered mode")
	}
}

// Tiering is a caching topology, not a forwarding flavor: a tiered deployment
// may front any S3-compatible endpoint, forwarding by signing there and
// transparently on Tigris. Only transparent mode itself is pinned to Tigris.
func TestMode_TieredAllowsNonTigrisEndpoint(t *testing.T) {
	var cfg Config
	applyDefaults(&cfg)
	cfg.Mode = ModeTiered
	cfg.Upstream.Endpoint = "https://ns.compat.objectstorage.us-ashburn-1.oraclecloud.com"
	if err := validate(&cfg); err != nil {
		t.Fatalf("validate rejected tiered mode on a non-Tigris endpoint: %v", err)
	}
	if cfg.ForwardsTransparently() {
		t.Fatal("tiered on a non-Tigris endpoint must forward by signing")
	}
}

func TestMode_TransparentRejectsNonTigrisEndpoint(t *testing.T) {
	var cfg Config
	applyDefaults(&cfg)
	cfg.Mode = ModeTransparent
	cfg.Upstream.Endpoint = "https://ns.compat.objectstorage.us-ashburn-1.oraclecloud.com"
	if err := validate(&cfg); err == nil {
		t.Fatal("validate accepted transparent mode on a non-Tigris endpoint")
	}
}

func TestMode_ForwardsTransparently(t *testing.T) {
	tigris, oci := "https://t3.storage.dev", "https://ns.compat.objectstorage.us-ashburn-1.oraclecloud.com"
	cases := []struct {
		name     string
		mode     string
		endpoint string
		want     bool
	}{
		{"transparent/tigris", ModeTransparent, tigris, true},
		{"signing/tigris", ModeSigning, tigris, false},
		{"signing/oci", ModeSigning, oci, false},
		{"tiered/tigris", ModeTiered, tigris, true},
		{"tiered/oci", ModeTiered, oci, false},
		{"default(transparent)/tigris", "", tigris, true},
		{"tiered/tigris mixed-case host", ModeTiered, "https://T3.Storage.Dev", true},
		{"tiered/localhost is NOT trusted → signing", ModeTiered, "http://localhost:9000", false},
		{"tiered/Localhost mixed-case → signing", ModeTiered, "http://Localhost:9000", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config
			applyDefaults(&cfg)
			cfg.Mode = tc.mode
			cfg.Upstream.Endpoint = tc.endpoint
			if got := cfg.ForwardsTransparently(); got != tc.want {
				t.Fatalf("ForwardsTransparently() = %v, want %v", got, tc.want)
			}
		})
	}
}

// Transparent mode keeps its localhost allowance for local testing; tiered
// treats the same endpoint as untrusted (signing flavor, cleanup disabled).
func TestMode_TransparentAllowsLocalhost(t *testing.T) {
	var cfg Config
	applyDefaults(&cfg)
	cfg.Mode = ModeTransparent
	cfg.Upstream.Endpoint = "http://Localhost:9000"
	if err := validate(&cfg); err != nil {
		t.Fatalf("validate rejected transparent mode on localhost: %v", err)
	}
	if !cfg.ForwardsTransparently() {
		t.Fatal("transparent mode on localhost must forward transparently")
	}
}
