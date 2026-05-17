package watcher

import (
	"os"
	"path/filepath"
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
	ready       ReadyConfig
	logger      *zap.Logger
}

// NewPoller creates a new directory poller.
func NewPoller(pipeline, watchDir string, ready ReadyConfig, s *store.FileStore, interval time.Duration, logger *zap.Logger) *Poller {
	ready = ready.normalize()
	return &Poller{
		watchDir:    watchDir,
		filePattern: ready.FilePattern,
		store:       s,
		pipeline:    pipeline,
		interval:    interval,
		ready:       ready,
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

	now := time.Now()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		matched, err := p.entryMayBeReady(entry.Name())
		if err != nil || !matched {
			continue
		}

		fullPath := filepath.Join(p.watchDir, entry.Name())
		readyFile, err := p.ready.Check(fullPath, now)
		if err != nil {
			p.logger.Error("poller ready check failed",
				zap.String("file", fullPath),
				zap.Error(err),
			)
			continue
		}
		if !readyFile.Ready {
			continue
		}

		enqueued, err := p.store.EnqueueReadyFile(p.pipeline, readyFile.Path, readyFile.Info.Size(), readyFile.Info.ModTime())
		if err != nil {
			p.logger.Error("poller enqueue file failed",
				zap.String("file", readyFile.Path),
				zap.Error(err),
			)
			continue
		}
		if !enqueued {
			continue
		}

		p.logger.Info("poller discovered new file",
			zap.String("file", readyFile.Path),
			zap.String("pipeline", p.pipeline),
		)

		select {
		case fileCh <- readyFile.Path:
		case <-time.After(5 * time.Second):
			p.logger.Warn("poller file channel full, dropping file", zap.String("file", readyFile.Path))
		}
	}
}

func (p *Poller) entryMayBeReady(name string) (bool, error) {
	if p.ready.Strategy == ReadyMarker {
		if p.ready.MarkerSuffix == "" {
			return false, nil
		}
		return filepath.Match(p.filePattern+p.ready.MarkerSuffix, name)
	}
	return filepath.Match(p.filePattern, name)
}
