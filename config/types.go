package config

import (
	"time"

	"go-etl/model"
)

// Config is the root configuration.
type Config struct {
	ClickHouse  ClickHouseConfig `yaml:"clickhouse"`
	IPDB        IPDBConfig       `yaml:"ip_db"`
	Metrics     MetricsConfig    `yaml:"metrics"`
	PipelineDir string           `yaml:"pipeline_dir"` // directory containing per-pipeline YAML files
	Pipelines   []PipelineConfig `yaml:"pipelines"`    // inline pipelines (merged with pipeline_dir)
}

// MetricsConfig controls the built-in expvar metrics endpoint.
type MetricsConfig struct {
	Enabled           bool   `yaml:"enabled"`
	Addr              string `yaml:"addr"`               // default ":9090"
	PrometheusEnabled bool   `yaml:"prometheus_enabled"` // expose /metrics in Prometheus text format
}

// ClickHouseConfig holds ClickHouse connection parameters.
type ClickHouseConfig struct {
	Hosts         []string          `yaml:"hosts"`
	Database      string            `yaml:"database"`
	Username      string            `yaml:"username"`
	Password      string            `yaml:"password"`
	MaxOpenConns  int               `yaml:"max_open_conns"` // default 10
	MaxIdleConns  int               `yaml:"max_idle_conns"` // default 5
	Debug         bool              `yaml:"debug"`
	BatchSize     int               `yaml:"batch_size"`     // default 10000
	FlushInterval time.Duration     `yaml:"flush_interval"` // default 5s
	WriteTimeout  time.Duration     `yaml:"write_timeout"`  // default 60s
	AsyncInsert   AsyncInsertConfig `yaml:"async_insert"`
}

// AsyncInsertConfig controls ClickHouse server-side async insert buffering.
type AsyncInsertConfig struct {
	Enabled bool  `yaml:"enabled"`
	Wait    *bool `yaml:"wait"` // default true when enabled
}

// IPDBConfig holds IP database configuration.
type IPDBConfig struct {
	Type           string        `yaml:"type"`            // "csv"
	Path           string        `yaml:"path"`            // path to IP CSV file
	Columns        []string      `yaml:"columns"`         // column names in CSV
	ReloadInterval time.Duration `yaml:"reload_interval"` // hot reload interval
	CacheSize      int           `yaml:"cache_size"`      // optional lookup cache size, 0 disables cache
}

// DedupConfig controls optional stable row metadata generation.
type DedupConfig struct {
	Enabled         bool   `yaml:"enabled"`
	RecordIDField   string `yaml:"record_id_field"`
	SourceFileField string `yaml:"source_file_field"`
	LineNumberField string `yaml:"line_number_field"`
	RowHashField    string `yaml:"row_hash_field"`
}

// PipelineConfig defines one ETL pipeline (one directory → one table).
type PipelineConfig struct {
	Name            string              `yaml:"name"`
	WatchDir        string              `yaml:"watch_dir"`
	FilePattern     string              `yaml:"file_pattern"`      // "*" or "*.csv"
	ReadyStrategy   string              `yaml:"ready_strategy"`    // atomic_rename, marker, or stable_size
	TempSuffixes    []string            `yaml:"temp_suffixes"`     // ignored in-progress suffixes
	MarkerSuffix    string              `yaml:"marker_suffix"`     // marker file suffix, usually ".ok"
	StableDelay     time.Duration       `yaml:"stable_delay"`      // used by stable_size strategy
	Delimiter       string              `yaml:"delimiter"`         // "|" or "|++|"
	HasHeaderMeta   bool                `yaml:"has_header_meta"`   // first line is common info?
	HeaderFields    []model.FieldDef    `yaml:"header_fields"`     // fields read from first-line common info
	SkipHeaderLines int                 `yaml:"skip_header_lines"` // skip N lines at top (usually 1 if has_header_meta)
	Fields          []model.FieldDef    `yaml:"fields"`            // output ClickHouse schema
	Transformers    []TransformerConfig `yaml:"transformers"`
	ClickHouseTable string              `yaml:"clickhouse_table"`
	Workers         int                 `yaml:"workers"`    // transform worker count
	BatchSize       int                 `yaml:"batch_size"` // override global batch size
	RetryFailed     bool                `yaml:"retry_failed"`
	MaxRetries      int                 `yaml:"max_retries"`
	RetryInterval   time.Duration       `yaml:"retry_interval"`
	DeadLetterDir   string              `yaml:"dead_letter_dir"`
	ArchiveDir      string              `yaml:"archive_dir"`
	CleanupMarker   bool                `yaml:"cleanup_marker"`
	Dedup           DedupConfig         `yaml:"dedup"`
}

// TransformerConfig defines a transformer in the pipeline.
type TransformerConfig struct {
	Type        string                 `yaml:"type"`
	Fields      []string               `yaml:"fields,omitempty"`       // IP matching: which fields
	LabelFields []string               `yaml:"label_fields,omitempty"` // IP matching: output geo fields
	Field       string                 `yaml:"field,omitempty"`        // Dict mapping: source field
	Dict        map[string]string      `yaml:"dict,omitempty"`         // Dict mapping: value → mapped value
	DictFile    string                 `yaml:"dict_file,omitempty"`    // Dict mapping: external CSV dict file
	Config      map[string]interface{} `yaml:"config,omitempty"`       // Custom transformer config
}

// DefaultClickHouseConfig returns sensible defaults.
func DefaultClickHouseConfig() ClickHouseConfig {
	return ClickHouseConfig{
		MaxOpenConns:  10,
		MaxIdleConns:  5,
		BatchSize:     10000,
		FlushInterval: 5 * time.Second,
		WriteTimeout:  60 * time.Second,
	}
}
