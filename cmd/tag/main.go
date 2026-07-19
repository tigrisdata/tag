// Package main is the entry point for TAG (Tigris Acceleration Gateway).
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/tigrisdata/ocache/embedded"
	ocachestorage "github.com/tigrisdata/ocache/storage"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/handlers"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/proxy"
)

// Build-time variables set via ldflags.
var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

const (
	// Cluster ready timeout for embedded cache
	clusterReadyTimeout = 30 * time.Second

	// Shutdown timeout for graceful shutdown
	shutdownTimeout = 30 * time.Second

	// How often the local cache size is sampled into tag_cache_size_bytes.
	cacheSizeSampleInterval = 30 * time.Second
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "", "Path to configuration file")
	disableCache := flag.Bool("disable-cache", false, "Disable caching (pass-through mode)")
	showVersion := flag.Bool("version", false, "Print version information and exit")
	httpPort := flag.Int("http-port", 0, "HTTP port (env: TAG_HTTP_PORT)")
	logLevel := flag.String("log-level", "", "Log level: debug, info, warn, error (env: TAG_LOG_LEVEL)")
	logFormat := flag.String("log-format", "", "Log format: json or console (env: TAG_LOG_FORMAT)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: tag [options]\n\n")
		fmt.Fprintf(os.Stderr, "TAG (Tigris Acceleration Gateway) - S3-compatible caching proxy for Tigris\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fmt.Fprintf(os.Stderr, "  --version              Print version information and exit\n")
		fmt.Fprintf(os.Stderr, "  --config PATH          Path to YAML configuration file\n")
		fmt.Fprintf(os.Stderr, "  --disable-cache        Disable caching (pass-through mode)\n")
		fmt.Fprintf(os.Stderr, "  --http-port PORT       HTTP port (default: 8080, env: TAG_HTTP_PORT)\n")
		fmt.Fprintf(os.Stderr, "  --log-level LEVEL      Log level: debug, info, warn, error (default: info, env: TAG_LOG_LEVEL)\n")
		fmt.Fprintf(os.Stderr, "  --log-format FORMAT    Log format: json or console (default: json, env: TAG_LOG_FORMAT)\n")
		fmt.Fprintf(os.Stderr, "\nConfiguration precedence: defaults < config file < environment variables < CLI flags\n")
		fmt.Fprintf(os.Stderr, "See documentation for full list of environment variables (TAG_*).\n")
	}

	flag.Parse()

	// Handle --version before any config loading
	if *showVersion {
		fmt.Printf("TAG (Tigris Acceleration Gateway)\n")
		fmt.Printf("  Version:    %s\n", Version)
		fmt.Printf("  Build Time: %s\n", BuildTime)
		fmt.Printf("  Git Commit: %s\n", GitCommit)
		os.Exit(0)
	}

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

	// Override cache enabled from command line flag
	if *disableCache {
		cfg.Cache.SetEnabled(false)
	}

	// Apply convenience CLI flag overrides (only when explicitly set)
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "http-port":
			cfg.Server.HTTPPort = *httpPort
		case "log-level":
			cfg.Log.Level = *logLevel
		case "log-format":
			cfg.Log.Format = *logFormat
		}
	})

	// Initialize logger based on configuration (after CLI overrides)
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

	log.Info().
		Str("version", Version).
		Str("build_time", BuildTime).
		Str("git_commit", GitCommit).
		Dict("server", zerolog.Dict().
			Int("http_port", cfg.Server.HTTPPort).
			Str("bind_ip", cfg.Server.BindIP).
			Bool("tls_enabled", cfg.Server.TLSEnabled()).
			Str("tls_cert_file", cfg.Server.TLSCertFile).
			Str("tls_key_file", cfg.Server.TLSKeyFile).
			Bool("pprof_enabled", cfg.Server.PprofEnabled),
		).
		Dict("upstream", zerolog.Dict().
			Str("endpoint", cfg.Upstream.Endpoint).
			Str("region", cfg.Upstream.Region).
			Int("max_idle_conns_per_host", cfg.Upstream.MaxIdleConnsPerHost).
			Bool("transparent_proxy", cfg.Upstream.IsTransparentProxy()),
		).
		Dict("cache", zerolog.Dict().
			Bool("enabled", cfg.Cache.IsEnabled()).
			Str("ttl", cfg.Cache.TTL.String()).
			Int64("size_threshold", cfg.Cache.SizeThreshold).
			Str("disk_path", cfg.Cache.DiskPath).
			Int64("max_disk_usage_bytes", cfg.Cache.MaxDiskUsageBytes).
			Str("node_id", cfg.Cache.NodeID).
			Str("cluster_addr", cfg.Cache.ClusterAddr).
			Str("grpc_addr", cfg.Cache.GRPCAddr).
			Str("advertise_addr", cfg.Cache.AdvertiseAddr).
			Strs("seed_nodes", cfg.Cache.SeedNodes).
			Bool("grpc_auth", cfg.Cache.IsGRPCAuthEnabled()),
		).
		Dict("credentials", zerolog.Dict().
			Str("authz_cache_ttl", cfg.Credentials.AuthzCacheTTL.String()),
		).
		Dict("broadcast", zerolog.Dict().
			Int("chunk_size", cfg.Broadcast.ChunkSize).
			Int("channel_buffer", cfg.Broadcast.ChannelBuffer),
		).
		Dict("log", zerolog.Dict().
			Str("level", cfg.Log.Level).
			Str("format", cfg.Log.Format),
		).
		Msg("Starting TAG (Tigris Acceleration Gateway)")

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize credential store
	credStore := auth.NewCredentialStore()

	// Load credentials from environment
	if err := credStore.LoadFromEnv(); err != nil {
		log.Warn().Err(err).Msg("Failed to load credentials from environment")
	}

	// Initialize proxy signer if transparent proxy is enabled
	var proxySigner *auth.ProxySigner
	if cfg.Upstream.IsTransparentProxy() {
		accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		if accessKey == "" || secretKey == "" {
			log.Fatal().Msg("Transparent proxy requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
		}
		proxySigner = auth.NewProxySigner(accessKey, secretKey)
		log.Info().Msg("Transparent proxy mode enabled")
	} else {
		log.Info().Str("endpoint", cfg.Upstream.Endpoint).Msg("Signing mode enabled")
		if !config.IsTigrisEndpoint(cfg.Upstream.Endpoint) {
			log.Warn().Str("endpoint", cfg.Upstream.Endpoint).
				Msg("Running against a non-Tigris S3 endpoint; transparent-proxy features are unavailable and third-party backends are community-supported")
		}
	}

	if credStore.Count() == 0 && !cfg.Upstream.IsTransparentProxy() {
		log.Warn().Msg("No credentials loaded - TAG will reject all requests")
	}

	// 2. Initialize cache (embedded or disabled based on config)
	var objectCache *cache.Cache

	if cfg.Cache.IsEnabled() {
		log.Info().
			Str("node_id", cfg.Cache.NodeID).
			Str("disk_path", cfg.Cache.DiskPath).
			Strs("seed_nodes", cfg.Cache.SeedNodes).
			Bool("grpc_auth", cfg.Cache.IsGRPCAuthEnabled()).
			Msg("Initializing embedded cache")

		embeddedCfg := &embedded.Config{
			DiskPath:      cfg.Cache.DiskPath,
			TTL:           cfg.Cache.TTL,
			MaxDiskUsage:  cfg.Cache.MaxDiskUsageBytes,
			NodeID:        cfg.Cache.NodeID,
			ClusterAddr:   cfg.Cache.ClusterAddr,
			GRPCAddr:      cfg.Cache.GRPCAddr,
			AdvertiseAddr: cfg.Cache.AdvertiseAddr,
			SeedNodes:     cfg.Cache.SeedNodes,
		}

		// Advanced storage tuning. Defaults are applied by config.applyDefaults;
		// the top-level embedded.Config fields (DiskPath, TTL, etc.) still take
		// precedence over anything set here.
		embeddedCfg.Storage = &ocachestorage.StorageConfig{
			DeleteBatchSize: cfg.Cache.DeleteBatchSize,
			RecoveryWorkers: cfg.Cache.RecoveryWorkers,
			EvictionPolicy:  cfg.Cache.EvictionPolicy,
		}

		// Configure gRPC auth for cache cluster communication
		if cfg.Cache.IsGRPCAuthEnabled() {
			accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
			secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
			if accessKey == "" || secretKey == "" {
				log.Fatal().Msg("Cache gRPC auth requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (set TAG_CACHE_GRPC_AUTH=false to disable)")
			}
			grpcToken := auth.DeriveGRPCAuthToken(accessKey, secretKey)
			embeddedCfg.GRPCServerOptions = auth.GRPCServerOptions(grpcToken)
			embeddedCfg.GRPCDialOptions = auth.GRPCDialOptions(grpcToken)
			log.Info().Msg("Cache gRPC auth enabled")
		}

		embeddedCache, err := embedded.New(embeddedCfg)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize embedded cache")
		}
		defer embeddedCache.Close()

		// Start gRPC server for cluster routing
		if err := embeddedCache.StartGRPCServer(); err != nil {
			log.Fatal().Err(err).Msg("Failed to start embedded cache gRPC server")
		}

		// Wait for cluster to be ready
		readyCtx, readyCancel := context.WithTimeout(ctx, clusterReadyTimeout)
		if err := embeddedCache.WaitReady(readyCtx); err != nil {
			readyCancel()
			log.Warn().Err(err).Msg("Embedded cache not fully ready, continuing anyway")
		}
		readyCancel()

		// Wrap embedded cache with the cache.Cache interface
		objectCache = cache.NewCacheWithClient(embeddedCache, &cfg.Cache)

		// Publish this node's local cache size as tag_cache_size_bytes. ocache keeps
		// the total live (an atomic), so sampling it is cheap. Stops on ctx cancel.
		go metrics.SampleCacheSize(ctx, cacheSizeSampleInterval, embeddedCache.Storage().TotalSize)

		log.Info().
			Str("node_id", cfg.Cache.NodeID).
			Strs("nodes", embeddedCache.GetConnectedNodes()).
			Msg("Embedded cache ready")
	} else {
		log.Info().Msg("Cache disabled, running in pass-through mode")
		objectCache = cache.NewDisabledCache()
	}

	// 3. Initialize local auth for transparent proxy
	var localAuth *proxy.LocalAuthConfig
	if cfg.Upstream.IsTransparentProxy() {
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		derivedKeyStore := auth.NewDerivedKeyStore(auth.DefaultDerivedKeyTTL)
		keyUnwrapper, err := auth.NewKeyUnwrapper(secretKey)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize key unwrapper for local auth")
		}

		authzCacheTTL := cfg.Credentials.AuthzCacheTTL
		if authzCacheTTL == 0 {
			authzCacheTTL = auth.DefaultAuthzCacheTTL
		}

		localAuth = &proxy.LocalAuthConfig{
			DerivedKeyStore: derivedKeyStore,
			Validator:       auth.NewRequestValidator(derivedKeyStore),
			KeyUnwrapper:    keyUnwrapper,
			AuthzCache:      auth.NewAuthzCache(authzCacheTTL),
		}
		log.Info().Msg("Local auth validation enabled for transparent proxy (derived signing keys)")
	}

	// 4. Initialize forwarder
	forwarder := proxy.NewForwarder(credStore, cfg.Upstream.Endpoint, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost, proxySigner, localAuth)

	// 5. Initialize proxy service
	service := proxy.NewService(forwarder, objectCache, cfg)

	// 6. Initialize HTTP server
	server := handlers.NewServer(service, cfg.Server.BindIP, cfg.Server.HTTPPort, cfg.Server.PprofEnabled, cfg.Server.MaxInflightRequests)
	if cfg.Server.TLSEnabled() {
		server.SetTLS(cfg.Server.TLSCertFile, cfg.Server.TLSKeyFile)
	}

	// Start HTTP server in goroutine
	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server failed")
		}
	}()

	protocol := "http"
	if cfg.Server.TLSEnabled() {
		protocol = "https"
	}
	log.Info().
		Str("addr", cfg.Server.BindIP).
		Int("port", cfg.Server.HTTPPort).
		Str("protocol", protocol).
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
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	if err := server.Stop(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("Error during server shutdown")
	}

	log.Info().Msg("TAG shutdown complete")
}
