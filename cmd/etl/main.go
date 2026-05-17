package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go-etl/config"
	"go-etl/iputil"
	"go-etl/metrics"
	"go-etl/pipeline"
	"go-etl/store"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to YAML config file")
	storePath := flag.String("store", "data/filestatus.db", "Path to file status bolt DB")
	logLevel := flag.String("log", "info", "Log level: debug, info, warn, error")
	flag.Parse()

	// Setup logger
	logger := newLogger(*logLevel)
	defer logger.Sync()

	logger.Info("go-etl starting", zap.String("config", *configPath))

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}
	var metricsServer interface {
		Shutdown(context.Context) error
	}
	if cfg.Metrics.Enabled {
		metricsServer = metrics.StartServer(cfg.Metrics.Addr)
		logger.Info("metrics server started", zap.String("addr", cfg.Metrics.Addr), zap.String("path", "/debug/vars"))
	}

	// Open file status store
	fileStore, err := store.NewFileStore(*storePath)
	if err != nil {
		logger.Fatal("failed to open file store", zap.Error(err))
	}
	defer fileStore.Close()

	// Load IP database if configured
	var ipdb *iputil.IPDB
	if cfg.IPDB.Path != "" {
		logger.Info("loading IP database", zap.String("path", cfg.IPDB.Path))
		ipdb, err = iputil.LoadCSV(cfg.IPDB.Path, cfg.IPDB.Columns)
		if err != nil {
			logger.Fatal("failed to load IP database", zap.Error(err))
		}
		logger.Info("IP database loaded", zap.Int("ranges", ipdb.Count()))
	}

	// Start all pipelines
	var wg sync.WaitGroup
	pipelines := make([]*pipeline.Pipeline, len(cfg.Pipelines))

	for i, pCfg := range cfg.Pipelines {
		p, err := pipeline.New(pCfg, cfg.ClickHouse, ipdb, fileStore, logger)
		if err != nil {
			logger.Fatal("failed to create pipeline",
				zap.String("name", pCfg.Name),
				zap.Error(err),
			)
		}
		pipelines[i] = p

		wg.Add(1)
		go func(p *pipeline.Pipeline) {
			defer wg.Done()
			if err := p.Run(); err != nil {
				logger.Error("pipeline exited with error",
					zap.String("name", pCfg.Name),
					zap.Error(err),
				)
			}
		}(p)
	}

	// Wait for termination signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	logger.Info("received signal, shutting down", zap.String("signal", sig.String()))

	// Graceful shutdown
	for _, p := range pipelines {
		p.Shutdown()
	}
	wg.Wait()
	if metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(ctx); err != nil {
			logger.Error("metrics server shutdown failed", zap.Error(err))
		}
	}

	logger.Info("go-etl stopped")
}

func newLogger(level string) *zap.Logger {
	var l zapcore.Level
	switch level {
	case "debug":
		l = zapcore.DebugLevel
	case "warn":
		l = zapcore.WarnLevel
	case "error":
		l = zapcore.ErrorLevel
	default:
		l = zapcore.InfoLevel
	}

	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(l)
	cfg.Encoding = "console"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, _ := cfg.Build()
	return logger
}
