package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"go-etl/config"
	"go-etl/model"
	"go-etl/store"
)

func TestMoveToDeadLetter(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "a.csv")
	deadDir := filepath.Join(dir, "dead")
	if err := os.WriteFile(sourcePath, []byte("row\n"), 0644); err != nil {
		t.Fatal(err)
	}

	p := &Pipeline{cfg: config.PipelineConfig{DeadLetterDir: deadDir}}
	targetPath, err := p.moveToDeadLetter(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if targetPath != filepath.Join(deadDir, "a.csv") {
		t.Fatalf("target = %q", targetPath)
	}
	if _, err := os.Stat(targetPath); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sourcePath); !os.IsNotExist(err) {
		t.Fatalf("source still exists or stat failed unexpectedly: %v", err)
	}
}

func TestMoveToDeadLetterWithoutDir(t *testing.T) {
	p := &Pipeline{}
	targetPath, err := p.moveToDeadLetter("a.csv")
	if err != nil {
		t.Fatal(err)
	}
	if targetPath != "" {
		t.Fatalf("target = %q, want empty", targetPath)
	}
}

func TestMoveFileToDirRenamesOnConflict(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "a.csv")
	targetDir := filepath.Join(dir, "archive")
	targetPath := filepath.Join(targetDir, "a.csv")

	if err := os.MkdirAll(targetDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sourcePath, []byte("new\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, []byte("old\n"), 0644); err != nil {
		t.Fatal(err)
	}

	movedPath, err := moveFileToDir(sourcePath, targetDir)
	if err != nil {
		t.Fatal(err)
	}
	if movedPath == targetPath {
		t.Fatal("expected conflict-safe target path")
	}
	if _, err := os.Stat(movedPath); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(targetPath); err != nil || string(data) != "old\n" {
		t.Fatalf("original target changed: data=%q err=%v", data, err)
	}
}

func TestCleanupMarker(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "a.csv")
	markerPath := dataPath + ".ok"
	if err := os.WriteFile(dataPath, []byte("row\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(markerPath, nil, 0644); err != nil {
		t.Fatal(err)
	}

	p := &Pipeline{cfg: config.PipelineConfig{
		ReadyStrategy: "marker",
		MarkerSuffix:  ".ok",
	}}
	if err := p.cleanupMarker(dataPath); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("marker still exists or stat failed unexpectedly: %v", err)
	}
}

func TestCleanupMarkerIgnoresNonMarkerStrategy(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "a.csv")
	markerPath := dataPath + ".ok"
	if err := os.WriteFile(markerPath, nil, 0644); err != nil {
		t.Fatal(err)
	}

	p := &Pipeline{cfg: config.PipelineConfig{
		ReadyStrategy: "atomic_rename",
		MarkerSuffix:  ".ok",
	}}
	if err := p.cleanupMarker(dataPath); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatal(err)
	}
}

func TestInputFieldNamesSkipsGeneratedFields(t *testing.T) {
	got := inputFieldNames([]model.FieldDef{
		{Name: "event_time", Type: "DateTime"},
		{Name: "src_ip", Type: "IPv4"},
		{Name: "src_geo_city", Type: "String", Generated: true},
		{Name: "method_name", Source: "method", Type: "String"},
	})
	want := []string{"event_time", "src_ip", "method"}

	if len(got) != len(want) {
		t.Fatalf("field count = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("field[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRetryDispatcherDispatchesRetryableFailedFiles(t *testing.T) {
	s, err := store.NewFileStore(filepath.Join(t.TempDir(), "status.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	filePath := "/data/a.csv"
	if _, err := s.EnqueueReadyFile("dns", filePath, 12, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimPending("dns", filePath); err != nil {
		t.Fatal(err)
	}
	if err := s.SetFailedForRetry("dns", filePath, "boom", 0); err != nil {
		t.Fatal(err)
	}

	p := &Pipeline{
		cfg: config.PipelineConfig{
			Name:          "dns",
			RetryFailed:   true,
			MaxRetries:    3,
			RetryInterval: time.Millisecond,
		},
		store:  s,
		logger: zaptest.NewLogger(t),
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	defer p.cancel()

	workCh := make(chan string, 1)
	p.wg.Add(1)
	go p.retryDispatcher(workCh)

	select {
	case got := <-workCh:
		if got != filePath {
			t.Fatalf("retry file = %q, want %q", got, filePath)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retry file")
	}
}
