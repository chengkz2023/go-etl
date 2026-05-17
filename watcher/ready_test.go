package watcher

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadyConfigMarkerRequiresMarkerFile(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "cdr.csv")
	markerPath := dataPath + ".ok"

	if err := os.WriteFile(dataPath, []byte("row\n"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := ReadyConfig{
		Strategy:     ReadyMarker,
		FilePattern:  "*.csv",
		MarkerSuffix: ".ok",
	}

	ready, err := cfg.Check(dataPath, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if ready.Ready {
		t.Fatal("data file without marker event must not be ready in marker mode")
	}

	if err := os.WriteFile(markerPath, nil, 0644); err != nil {
		t.Fatal(err)
	}

	ready, err = cfg.Check(markerPath, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !ready.Ready {
		t.Fatal("marker file should make data file ready")
	}
	if ready.Path != dataPath {
		t.Fatalf("ready path = %q, want %q", ready.Path, dataPath)
	}
}

func TestReadyConfigAtomicRenameIgnoresTempSuffix(t *testing.T) {
	dir := t.TempDir()
	tmpPath := filepath.Join(dir, "cdr.csv.tmp")
	dataPath := filepath.Join(dir, "cdr.csv")

	if err := os.WriteFile(tmpPath, []byte("row\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, []byte("row\n"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := ReadyConfig{
		Strategy:     ReadyAtomicRename,
		FilePattern:  "*.csv*",
		TempSuffixes: []string{".tmp"},
	}

	ready, err := cfg.Check(tmpPath, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if ready.Ready {
		t.Fatal("temp file must not be ready")
	}

	ready, err = cfg.Check(dataPath, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !ready.Ready {
		t.Fatal("final renamed file should be ready")
	}
}

func TestReadyConfigStableSizeWaitsForDelay(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "cdr.csv")
	if err := os.WriteFile(dataPath, []byte("row\n"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := ReadyConfig{
		Strategy:    ReadyStableSize,
		FilePattern: "*.csv",
		StableDelay: time.Hour,
	}

	ready, err := cfg.Check(dataPath, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if ready.Ready {
		t.Fatal("recent file should not be ready before stable delay")
	}
}
