package watcher

import (
	"path/filepath"
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
	fsWatcher *fsnotify.Watcher
	poller    *Poller
	store     *store.FileStore
	pipeline  string
	watchDir  string
	ready     ReadyConfig
	logger    *zap.Logger

	stopCh   chan struct{}
	fileCh   chan string
	once     sync.Once
	stopOnce sync.Once
}

// New creates a dual-channel Watcher for a pipeline's directory.
func New(pipeline, watchDir string, ready ReadyConfig, s *store.FileStore, pollInterval time.Duration, logger *zap.Logger) (*Watcher, error) {
	ready = ready.normalize()
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	if err := fsw.Add(watchDir); err != nil {
		fsw.Close()
		return nil, err
	}

	poller := NewPoller(pipeline, watchDir, ready, s, pollInterval, logger)

	return &Watcher{
		fsWatcher: fsw,
		poller:    poller,
		store:     s,
		pipeline:  pipeline,
		watchDir:  watchDir,
		ready:     ready,
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

	readyFile, err := w.ready.Check(fullPath, time.Now())
	if err != nil {
		w.logger.Error("fsnotify ready check failed",
			zap.String("file", fullPath),
			zap.Error(err),
		)
		return
	}
	if !readyFile.Ready {
		return
	}

	enqueued, err := w.store.EnqueueReadyFile(w.pipeline, readyFile.Path, readyFile.Info.Size(), readyFile.Info.ModTime())
	if err != nil {
		w.logger.Error("fsnotify enqueue file failed",
			zap.String("file", readyFile.Path),
			zap.Error(err),
		)
		return
	}
	if !enqueued {
		return
	}

	w.logger.Info("fsnotify discovered file",
		zap.String("file", readyFile.Path),
		zap.String("pipeline", w.pipeline),
	)

	select {
	case w.fileCh <- readyFile.Path:
	case <-time.After(5 * time.Second):
		w.logger.Warn("fsnotify file channel full", zap.String("file", readyFile.Path))
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
	w.stopOnce.Do(func() {
		close(w.stopCh)
		_ = w.fsWatcher.Close()
	})
}
