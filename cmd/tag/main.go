// Package main is the entry point for TAG (Tigris Access Gateway).
package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/handlers"
	"github.com/tigrisdata/tag/proxy"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "", "Path to configuration file")
	disableCache := flag.Bool("disable-cache", false, "Disable caching (pass-through mode)")
	flag.Parse()

	// Load configuration first (before setting up logger, so we can use log format from config)
	var cfg *config.Config
	var err error

	if *configPath != "" {
		cfg, err = config.Load(*configPath)
		if err != nil {
			// Use console writer for startup errors since config isn't loaded yet
			log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
			log.Fatal().Err(err).Str("path", *configPath).Msg("Failed to load configuration")
		}
	} else {
		cfg = config.NewDefault()
	}

	// Initialize logger based on configuration
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if cfg.Log.Format == "console" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	// Default is JSON format (zerolog's default), which is much faster

	// Set log level
	switch cfg.Log.Level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	if *configPath == "" {
		log.Info().Msg("Using default configuration")
	}

	// Override cache enabled from command line flag
	if *disableCache {
		cfg.Cache.Enabled = false
	}

	log.Info().
		Int("http_port", cfg.Server.HTTPPort).
		Str("upstream", cfg.Upstream.Endpoint).
		Bool("cache_enabled", cfg.Cache.Enabled).
		Strs("ocache_endpoints", cfg.Cache.Endpoints).
		Msg("Starting TAG (Tigris Access Gateway)")

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize credential store
	credStore := auth.NewCredentialStore()

	// Load credentials from environment
	if err := credStore.LoadFromEnv(); err != nil {
		log.Warn().Err(err).Msg("Failed to load credentials from environment")
	}

	if credStore.Count() == 0 {
		log.Warn().Msg("No credentials loaded - TAG will reject all requests")
	}

	// 2. Initialize cache (connects to external ocache cluster)
	objectCache, err := cache.NewCache(&cfg.Cache)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to connect to ocache cluster - running in pass-through mode")
		// Create a disabled cache
		objectCache = &cache.Cache{}
	} else {
		defer objectCache.Close()
		if objectCache.IsEnabled() {
			log.Info().
				Str("mode", objectCache.GetMode()).
				Strs("nodes", objectCache.GetConnectedNodes()).
				Msg("Connected to ocache cluster")
		} else {
			log.Info().Msg("Cache disabled - running in pass-through mode")
		}
	}

	// 3. Initialize forwarder
	forwarder := proxy.NewForwarder(credStore, cfg.Upstream.Endpoint, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost)

	// 4. Initialize proxy service
	service := proxy.NewService(forwarder, objectCache, cfg)

	// 5. Initialize HTTP server
	server := handlers.NewServer(service, cfg.Server.BindIP, cfg.Server.HTTPPort, cfg.Server.PprofEnabled)

	// Start HTTP server in goroutine
	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server failed")
		}
	}()

	log.Info().
		Str("addr", cfg.Server.BindIP).
		Int("port", cfg.Server.HTTPPort).
		Msg("TAG is ready")

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	select {
	case sig := <-sigCh:
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
	case <-ctx.Done():
		log.Info().Msg("Context cancelled")
	}

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Stop(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("Error during server shutdown")
	}

	log.Info().Msg("TAG shutdown complete")
}
