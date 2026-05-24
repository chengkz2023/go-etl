package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadAppliesReadyDefaults(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	data := []byte(`
clickhouse:
  hosts: ["127.0.0.1:9000"]
  database: cdr
metrics:
  enabled: true
pipelines:
  - name: dns
    watch_dir: /data/dns
    clickhouse_table: raw.dns
    fields:
      - name: timestamp
        type: DateTime
      - name: src_ip
        type: IPv4
`)
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.Pipelines[0]
	if p.ReadyStrategy != "atomic_rename" {
		t.Fatalf("ReadyStrategy = %q", p.ReadyStrategy)
	}
	if p.MarkerSuffix != ".ok" {
		t.Fatalf("MarkerSuffix = %q", p.MarkerSuffix)
	}
	if p.StableDelay != 10*time.Second {
		t.Fatalf("StableDelay = %s", p.StableDelay)
	}
	if p.MaxRetries != 3 {
		t.Fatalf("MaxRetries = %d", p.MaxRetries)
	}
	if p.RetryInterval != time.Minute {
		t.Fatalf("RetryInterval = %s", p.RetryInterval)
	}
	if cfg.Metrics.Addr != ":9090" {
		t.Fatalf("Metrics.Addr = %q", cfg.Metrics.Addr)
	}
}

func TestLoadRejectsInvalidReadyStrategy(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	data := []byte(`
clickhouse:
  hosts: ["127.0.0.1:9000"]
  database: cdr
pipelines:
  - name: dns
    watch_dir: /data/dns
    clickhouse_table: raw.dns
    ready_strategy: guess
    fields:
      - name: timestamp
        type: DateTime
`)
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := Load(configPath); err == nil {
		t.Fatal("expected invalid ready_strategy error")
	}
}

func TestLoadValidatesFields(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	data := []byte(`
clickhouse:
  hosts: ["127.0.0.1:9000"]
  database: cdr
pipelines:
  - name: dns
    watch_dir: /data/dns
    clickhouse_table: raw.dns
    fields:
      - name: timestamp
`)
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := Load(configPath); err == nil {
		t.Fatal("expected missing field type error")
	}
}

func TestLoadRejectsInvalidRetryConfig(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	data := []byte(`
clickhouse:
  hosts: ["127.0.0.1:9000"]
  database: cdr
pipelines:
  - name: dns
    watch_dir: /data/dns
    clickhouse_table: raw.dns
    max_retries: -1
    fields:
      - name: timestamp
        type: DateTime
`)
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := Load(configPath); err == nil {
		t.Fatal("expected invalid max_retries error")
	}
}

func TestLoadRequiresHeaderFields(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	data := []byte(`
clickhouse:
  hosts: ["127.0.0.1:9000"]
  database: cdr
pipelines:
  - name: dns
    watch_dir: /data/dns
    clickhouse_table: raw.dns
    has_header_meta: true
    fields:
      - name: timestamp
        type: DateTime
`)
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := Load(configPath); err == nil {
		t.Fatal("expected missing header_fields error")
	}
}

func TestLoadFullFixture(t *testing.T) {
	cfg, err := Load("../examples/full-test/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Pipelines) != 2 {
		t.Fatalf("pipeline count = %d, want 2", len(cfg.Pipelines))
	}
	if cfg.Pipelines[0].Name != "dns_cdr" || cfg.Pipelines[1].Name != "http_cdr" {
		t.Fatalf("unexpected pipelines: %#v", cfg.Pipelines)
	}
}

func TestLoadStressFixture(t *testing.T) {
	cfg, err := Load("../examples/stress-test/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Pipelines) != 2 {
		t.Fatalf("pipeline count = %d, want 2", len(cfg.Pipelines))
	}
	if cfg.Pipelines[0].Name != "stress_dns_cdr" || cfg.Pipelines[1].Name != "stress_http_cdr" {
		t.Fatalf("unexpected pipelines: %#v", cfg.Pipelines)
	}
	if len(cfg.Pipelines[0].HeaderFields) == 0 {
		t.Fatal("stress DNS pipeline should define header_fields")
	}
}
