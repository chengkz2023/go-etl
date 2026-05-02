package watcher

import (
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"go-etl/store"

	"go.uber.org/zap"
)

// Watcher combines fsnotify and a periodic poller for reliable file discovery.
// fsnotify handles real-time events; the poller catches anything missed.
// bolt-backed FileStore deduplicates across both channels.
type Watcher struct {
	fsWatcher  *fsnotify.Watcher
	poller     *Poller
	store      *store.FileStore
	pipeline   string
	watchDir   string
	logger     *zap.Logger

	stopCh chan struct{}
	fileCh chan string
	once   sync.Once
}

// New creates a dual-channel Watcher for a pipeline's directory.
func New(pipeline, watchDir, filePattern string, s *store.FileStore, pollInterval time.Duration, logger *zap.Logger) (*Watcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	if err := fsw.Add(watchDir); err != nil {
		fsw.Close()
		return nil, err
	}

	poller := NewPoller(pipeline, watchDir, filePattern, s, pollInterval, logger)

	return &Watcher{
		fsWatcher: fsw,
		poller:    poller,
		store:     s,
		pipeline:  pipeline,
		watchDir:  watchDir,
		logger:    logger,
		stopCh:    make(chan struct{}),
		fileCh:    make(chan string, 200),
	}, nil
}

// Start begins watching. Returns a channel of discovered file paths.
func (w *Watcher) Start() <-chan string {
	w.once.Do(func() {
		// Start poller (handles initial full scan + periodic rescans)
		pollerCh := w.poller.Start(w.stopCh)

		// Start fsnotify listener
		go w.runFsnotify()

		// Merge both channels into fileCh
		go w.merge(pollerCh)
	})

	return w.fileCh
}

func (w *Watcher) runFsnotify() {
	for {
		select {
		case <-w.stopCh:
			return
		case event, ok := <-w.fsWatcher.Events:
			if !ok {
				return
			}
			// Only react to file creation and rename (atomic write patterns)
			if event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) {
				w.handleFsEvent(event.Name)
			}
		case err, ok := <-w.fsWatcher.Errors:
			if !ok {
				return
			}
			w.logger.Error("fsnotify error", zap.Error(err))
		}
	}
}

func (w *Watcher) handleFsEvent(fullPath string) {
	// Filter: only files in the watched directory
	dir := filepath.Dir(fullPath)
	if dir != w.watchDir {
		return
	}

	// Skip temp files
	if strings.HasSuffix(fullPath, ".tmp") || strings.HasSuffix(fullPath, ".writing") {
		return
	}

	// Check bolt to avoid duplicates
	status, err := w.store.GetStatus(w.pipeline, fullPath)
	if err != nil {
		w.logger.Error("fsnotify check status failed",
			zap.String("file", fullPath),
			zap.Error(err),
		)
		return
	}
	if status != "unknown" && status != "pending" {
		return // already handled
	}

	// Register as pending
	// Use time.Time zero since we don't have FileInfo here (poller does it better)
	if err := w.store.SetPending(w.pipeline, fullPath, 0, time.Time{}); err != nil {
		w.logger.Error("fsnotify set pending failed",
			zap.String("file", fullPath),
			zap.Error(err),
		)
		return
	}

	w.logger.Info("fsnotify discovered file",
		zap.String("file", fullPath),
		zap.String("pipeline", w.pipeline),
	)

	select {
	case w.fileCh <- fullPath:
	case <-time.After(5 * time.Second):
		w.logger.Warn("fsnotify file channel full", zap.String("file", fullPath))
	}
}

// merge reads from pollerCh and forwards to fileCh (dedup is handled by bolt).
func (w *Watcher) merge(pollerCh <-chan string) {
	for file := range pollerCh {
		select {
		case w.fileCh <- file:
		case <-w.stopCh:
			return
		}
	}
}

// Stop shuts down the watcher.
func (w *Watcher) Stop() {
	close(w.stopCh)
	w.fsWatcher.Close()
}
