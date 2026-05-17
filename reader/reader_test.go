package reader

import (
	"strings"
	"testing"

	"go-etl/model"
)

func TestReadBatchesSkipsLinesAndMergesHeaderMeta(t *testing.T) {
	r := NewReader("|", []string{"ts", "ip"}, false, model.Row{"device": "probe-1"})
	r.SetSkipLines(1)

	var batches [][]model.Row
	err := r.ReadBatches(strings.NewReader("meta=ignored\n1|10.0.0.1\n2|10.0.0.2\n3|10.0.0.3\n"), 2, func(batch []model.Row) error {
		copied := append([]model.Row(nil), batch...)
		batches = append(batches, copied)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(batches) != 2 {
		t.Fatalf("batch count = %d, want 2", len(batches))
	}
	if len(batches[0]) != 2 || len(batches[1]) != 1 {
		t.Fatalf("batch sizes = %d/%d, want 2/1", len(batches[0]), len(batches[1]))
	}
	if batches[0][0]["ts"] != "1" || batches[0][0]["ip"] != "10.0.0.1" {
		t.Fatalf("unexpected first row: %#v", batches[0][0])
	}
	if batches[0][0]["device"] != "probe-1" {
		t.Fatalf("header meta was not merged: %#v", batches[0][0])
	}
}

func TestReadAllUsesBatchReader(t *testing.T) {
	r := NewReader("|++|", []string{"a", "b"}, false, nil)

	rows, err := r.ReadAll(strings.NewReader("x|++|y\nm|++|n\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("row count = %d, want 2", len(rows))
	}
	if rows[1]["a"] != "m" || rows[1]["b"] != "n" {
		t.Fatalf("unexpected second row: %#v", rows[1])
	}
}
