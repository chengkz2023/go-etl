package watcher

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"go-etl/store"

	"go.uber.org/zap"
)

// Poller periodically scans a directory for new files that haven't been processed.
type Poller struct {
	watchDir    string
	filePattern string
	store       *store.FileStore
	pipeline    string
	interval    time.Duration
	logger      *zap.Logger
}

// NewPoller creates a new directory poller.
func NewPoller(pipeline, watchDir, filePattern string, s *store.FileStore, interval time.Duration, logger *zap.Logger) *Poller {
	if filePattern == "" {
		filePattern = "*"
	}
	return &Poller{
		watchDir:    watchDir,
		filePattern: filePattern,
		store:       s,
		pipeline:    pipeline,
		interval:    interval,
		logger:      logger,
	}
}

// Start begins polling. Sends discovered files to the channel.
// Runs until stopCh is closed.
func (p *Poller) Start(stopCh <-chan struct{}) <-chan string {
	fileCh := make(chan string, 100)

	go func() {
		defer close(fileCh)

		// Do an immediate full scan on start
		p.scan(fileCh)

		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				p.scan(fileCh)
			}
		}
	}()

	return fileCh
}

// scan performs a single directory scan and sends new files to the channel.
func (p *Poller) scan(fileCh chan<- string) {
	entries, err := os.ReadDir(p.watchDir)
	if err != nil {
		p.logger.Error("poller scan directory failed",
			zap.String("dir", p.watchDir),
			zap.Error(err),
		)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		matched, err := filepath.Match(p.filePattern, entry.Name())
		if err != nil || !matched {
			continue
		}

		fullPath := filepath.Join(p.watchDir, entry.Name())
		// Skip temporary / in-progress files
		if strings.HasSuffix(entry.Name(), ".tmp") || strings.HasSuffix(entry.Name(), ".writing") {
			continue
		}

		// Check bolt: only enqueue new/unknown files
		status, err := p.store.GetStatus(p.pipeline, fullPath)
		if err != nil {
			p.logger.Error("poller check file status failed",
				zap.String("file", fullPath),
				zap.Error(err),
			)
			continue
		}

		if status != "unknown" && status != "pending" {
			continue // already processing, done, or failed
		}

		info, err := entry.Info()
		if err != nil {
			p.logger.Error("poller get file info failed",
				zap.String("file", fullPath),
				zap.Error(err),
			)
			continue
		}

		// Register as pending in bolt (atomic check-then-set)
		if err := p.store.SetPending(p.pipeline, fullPath, info.Size(), info.ModTime()); err != nil {
			p.logger.Error("poller set pending failed",
				zap.String("file", fullPath),
				zap.Error(err),
			)
			continue
		}

		p.logger.Info("poller discovered new file",
			zap.String("file", fullPath),
			zap.String("pipeline", p.pipeline),
		)

		select {
		case fileCh <- fullPath:
		case <-time.After(5 * time.Second):
			p.logger.Warn("poller file channel full, dropping file", zap.String("file", fullPath))
		}
	}
}
