package metrics

import (
	"expvar"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestPrometheusHandler(t *testing.T) {
	Inc("prom-test", "rows_written_total", 3)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	prometheusHandler(rr, req)

	body := rr.Body.String()
	if !strings.Contains(body, "go_etl_prom_test_rows_written_total 3") {
		t.Fatalf("unexpected prometheus body: %s", body)
	}
}
