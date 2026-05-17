package store

import (
	"path/filepath"
	"testing"
	"time"

	"go-etl/model"
)

func TestEnqueueReadyFileAndClaimPending(t *testing.T) {
	s, err := NewFileStore(filepath.Join(t.TempDir(), "status.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	enqueued, err := s.EnqueueReadyFile("dns", "/data/a.csv", 12, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !enqueued {
		t.Fatal("first enqueue should succeed")
	}

	enqueued, err = s.EnqueueReadyFile("dns", "/data/a.csv", 12, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !enqueued {
		t.Fatal("pending file may be re-enqueued for recovery")
	}

	claimed, err := s.ClaimPending("dns", "/data/a.csv")
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Fatal("pending file should be claimed")
	}

	claimed, err = s.ClaimPending("dns", "/data/a.csv")
	if err != nil {
		t.Fatal(err)
	}
	if claimed {
		t.Fatal("processing file must not be claimed twice")
	}

	status, err := s.GetStatus("dns", "/data/a.csv")
	if err != nil {
		t.Fatal(err)
	}
	if status != model.StatusProcessing {
		t.Fatalf("status = %q, want %q", status, model.StatusProcessing)
	}
}

func TestEnqueueReadyFileSkipsDoneFile(t *testing.T) {
	s, err := NewFileStore(filepath.Join(t.TempDir(), "status.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	enqueued, err := s.EnqueueReadyFile("dns", "/data/a.csv", 12, time.Now())
	if err != nil || !enqueued {
		t.Fatalf("enqueue = %v, %v", enqueued, err)
	}
	claimed, err := s.ClaimPending("dns", "/data/a.csv")
	if err != nil || !claimed {
		t.Fatalf("claim = %v, %v", claimed, err)
	}
	if err := s.SetDone("dns", "/data/a.csv", 10); err != nil {
		t.Fatal(err)
	}

	enqueued, err = s.EnqueueReadyFile("dns", "/data/a.csv", 12, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if enqueued {
		t.Fatal("done file must not be enqueued again")
	}
}

func TestResetProcessingToPending(t *testing.T) {
	s, err := NewFileStore(filepath.Join(t.TempDir(), "status.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	enqueued, err := s.EnqueueReadyFile("dns", "/data/a.csv", 12, time.Now())
	if err != nil || !enqueued {
		t.Fatalf("enqueue = %v, %v", enqueued, err)
	}
	claimed, err := s.ClaimPending("dns", "/data/a.csv")
	if err != nil || !claimed {
		t.Fatalf("claim = %v, %v", claimed, err)
	}

	count, err := s.ResetProcessingToPending("dns")
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("reset count = %d, want 1", count)
	}

	status, err := s.GetStatus("dns", "/data/a.csv")
	if err != nil {
		t.Fatal(err)
	}
	if status != model.StatusPending {
		t.Fatalf("status = %q, want %q", status, model.StatusPending)
	}
}

func TestSetFailedForRetryAndResetRetryable(t *testing.T) {
	s, err := NewFileStore(filepath.Join(t.TempDir(), "status.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	enqueued, err := s.EnqueueReadyFile("dns", "/data/a.csv", 12, time.Now())
	if err != nil || !enqueued {
		t.Fatalf("enqueue = %v, %v", enqueued, err)
	}
	claimed, err := s.ClaimPending("dns", "/data/a.csv")
	if err != nil || !claimed {
		t.Fatalf("claim = %v, %v", claimed, err)
	}
	if err := s.SetFailedForRetry("dns", "/data/a.csv", "boom", time.Minute); err != nil {
		t.Fatal(err)
	}

	rec, err := s.GetRecord("dns", "/data/a.csv")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Attempts != 1 || rec.Status != model.StatusFailed {
		t.Fatalf("record = %#v", rec)
	}

	count, err := s.ResetRetryableFailedToPending("dns", 3, rec.NextRetryAt.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("early retry count = %d, want 0", count)
	}

	count, err = s.ResetRetryableFailedToPending("dns", 3, rec.NextRetryAt)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("retry count = %d, want 1", count)
	}
}

func TestResetRetryableHonorsMaxRetries(t *testing.T) {
	s, err := NewFileStore(filepath.Join(t.TempDir(), "status.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.EnqueueReadyFile("dns", "/data/a.csv", 12, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimPending("dns", "/data/a.csv"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetFailedForRetry("dns", "/data/a.csv", "boom", 0); err != nil {
		t.Fatal(err)
	}

	count, err := s.ResetRetryableFailedToPending("dns", 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("retry count = %d, want 0", count)
	}
}

func TestMarkDead(t *testing.T) {
	s, err := NewFileStore(filepath.Join(t.TempDir(), "status.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.EnqueueReadyFile("dns", "/data/a.csv", 12, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimPending("dns", "/data/a.csv"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetFailedForRetry("dns", "/data/a.csv", "boom", 0); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDead("dns", "/data/a.csv", "/dead/a.csv"); err != nil {
		t.Fatal(err)
	}

	rec, err := s.GetRecord("dns", "/data/a.csv")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != model.StatusDead || rec.DeadLetterTo != "/dead/a.csv" {
		t.Fatalf("record = %#v", rec)
	}
}
