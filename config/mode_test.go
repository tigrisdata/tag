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
