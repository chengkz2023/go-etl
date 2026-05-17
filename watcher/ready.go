package watcher

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	ReadyAtomicRename = "atomic_rename"
	ReadyMarker       = "marker"
	ReadyStableSize   = "stable_size"
)

// ReadyConfig controls how the watcher decides a discovered path is complete.
type ReadyConfig struct {
	Strategy     string
	FilePattern  string
	TempSuffixes []string
	MarkerSuffix string
	StableDelay  time.Duration
}

// ReadyFile describes a data file that is ready to enqueue.
type ReadyFile struct {
	Path     string
	Info     os.FileInfo
	Ready    bool
	IsMarker bool
}

func (c ReadyConfig) normalize() ReadyConfig {
	if c.Strategy == "" {
		c.Strategy = ReadyAtomicRename
	}
	if c.FilePattern == "" {
		c.FilePattern = "*"
	}
	if len(c.TempSuffixes) == 0 {
		c.TempSuffixes = []string{".tmp", ".writing"}
	}
	if c.MarkerSuffix == "" {
		c.MarkerSuffix = ".ok"
	}
	if c.StableDelay == 0 {
		c.StableDelay = 10 * time.Second
	}
	return c
}

func (c ReadyConfig) DataPathFromEvent(path string) (string, bool) {
	c = c.normalize()
	if c.Strategy != ReadyMarker {
		return path, false
	}
	if !strings.HasSuffix(path, c.MarkerSuffix) {
		return "", false
	}
	return strings.TrimSuffix(path, c.MarkerSuffix), true
}

func (c ReadyConfig) Check(path string, now time.Time) (ReadyFile, error) {
	c = c.normalize()

	dataPath := path
	isMarker := false
	if c.Strategy == ReadyMarker {
		var ok bool
		dataPath, ok = c.DataPathFromEvent(path)
		if !ok {
			return ReadyFile{}, nil
		}
		isMarker = true
	}

	name := filepath.Base(dataPath)
	matched, err := filepath.Match(c.FilePattern, name)
	if err != nil || !matched {
		return ReadyFile{}, err
	}
	if hasAnySuffix(name, c.TempSuffixes) {
		return ReadyFile{}, nil
	}

	info, err := os.Stat(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ReadyFile{}, nil
		}
		return ReadyFile{}, err
	}
	if info.IsDir() {
		return ReadyFile{}, nil
	}

	switch c.Strategy {
	case ReadyStableSize:
		if now.Sub(info.ModTime()) < c.StableDelay {
			return ReadyFile{}, nil
		}
	case ReadyMarker:
		markerInfo, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				return ReadyFile{}, nil
			}
			return ReadyFile{}, err
		}
		if markerInfo.IsDir() {
			return ReadyFile{}, nil
		}
	}

	return ReadyFile{Path: dataPath, Info: info, Ready: true, IsMarker: isMarker}, nil
}

func hasAnySuffix(name string, suffixes []string) bool {
	for _, suffix := range suffixes {
		if suffix != "" && strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}
