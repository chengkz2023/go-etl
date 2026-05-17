package metrics

import (
	"expvar"
	"testing"
	"time"
)

func TestIncCreatesSanitizedCounter(t *testing.T) {
	Inc("http-cdr", "files.done", 2)

	v := expvar.Get("go_etl_http_cdr_files_done")
	if v == nil {
		t.Fatal("counter was not registered")
	}
	if got := v.String(); got != "2" {
		t.Fatalf("counter = %s, want 2", got)
	}
}

func TestObserveDuration(t *testing.T) {
	ObserveDuration("dns", "file_process", 1500*time.Millisecond)

	v := expvar.Get("go_etl_dns_file_process_ms_total")
	if v == nil {
		t.Fatal("duration counter was not registered")
	}
	if got := v.String(); got != "1500" {
		t.Fatalf("duration = %s, want 1500", got)
	}
}
