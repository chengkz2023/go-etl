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
	err := r.ReadBatches(strings.NewReader("probe-1\n1|10.0.0.1\n2|10.0.0.2\n3|10.0.0.3\n"), 2, func(batch []model.Row) error {
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

func TestReadBatchesWithMetaLineNumberSkipsHeader(t *testing.T) {
	r := NewReader("|", []string{"ts", "ip"}, false, nil)
	r.SetSkipLines(1)

	var got Batch
	err := r.ReadBatchesWithMeta(strings.NewReader("probe|south\n1|10.0.0.1\n2|10.0.0.2\n"), 10, func(batch Batch) error {
		got = batch
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(got.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(got.Rows))
	}
	if got.Meta[0].LineNumber != 2 || got.Meta[1].LineNumber != 3 {
		t.Fatalf("line numbers = %#v", got.Meta)
	}
	if got.Meta[0].RawHash == "" || got.Meta[0].RawHash == got.Meta[1].RawHash {
		t.Fatalf("unexpected hashes: %#v", got.Meta)
	}
}

func TestParseHeaderMetaUsesConfiguredFieldNames(t *testing.T) {
	meta := ParseHeaderMeta("probe-1|south", "|", []string{"probe_id", "region", "vendor"})

	if meta["probe_id"] != "probe-1" || meta["region"] != "south" || meta["vendor"] != "" {
		t.Fatalf("unexpected header meta: %#v", meta)
	}
	if _, ok := meta["meta_0"]; ok {
		t.Fatalf("header meta should not create positional meta_N fields: %#v", meta)
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
