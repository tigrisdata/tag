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

	"github.com/tigrisdata/ocache/embedded"
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
		Str("node_id", cfg.Cache.NodeID).
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

	// 2. Initialize cache (embedded or disabled based on config)
	var objectCache *cache.Cache

	if cfg.Cache.Enabled {
		log.Info().
			Str("node_id", cfg.Cache.NodeID).
			Str("disk_path", cfg.Cache.DiskPath).
			Strs("seed_nodes", cfg.Cache.SeedNodes).
			Msg("Initializing embedded cache")

		embeddedCache, err := embedded.New(&embedded.Config{
			DiskPath:      cfg.Cache.DiskPath,
			TTL:           cfg.Cache.TTL,
			MaxDiskUsage:  cfg.Cache.MaxDiskUsageBytes,
			NodeID:        cfg.Cache.NodeID,
			ClusterAddr:   cfg.Cache.ClusterAddr,
			GRPCAddr:      cfg.Cache.GRPCAddr,
			AdvertiseAddr: cfg.Cache.AdvertiseAddr,
			SeedNodes:     cfg.Cache.SeedNodes,
		})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize embedded cache")
		}
		defer embeddedCache.Close()

		// Start gRPC server for cluster routing
		if err := embeddedCache.StartGRPCServer(); err != nil {
			log.Fatal().Err(err).Msg("Failed to start embedded cache gRPC server")
		}

		// Wait for cluster to be ready
		readyCtx, readyCancel := context.WithTimeout(ctx, 30*time.Second)
		if err := embeddedCache.WaitReady(readyCtx); err != nil {
			readyCancel()
			log.Warn().Err(err).Msg("Embedded cache not fully ready, continuing anyway")
		}
		readyCancel()

		// Wrap embedded cache with the cache.Cache interface
		objectCache = cache.NewCacheWithClient(embeddedCache, &cfg.Cache)

		log.Info().
			Str("node_id", cfg.Cache.NodeID).
			Strs("nodes", embeddedCache.GetConnectedNodes()).
			Msg("Embedded cache ready")
	} else {
		log.Info().Msg("Cache disabled, running in pass-through mode")
		objectCache = cache.NewDisabledCache()
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
