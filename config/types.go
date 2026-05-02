package config

import "time"

// Config is the root configuration.
type Config struct {
	ClickHouse  ClickHouseConfig `yaml:"clickhouse"`
	IPDB        IPDBConfig       `yaml:"ip_db"`
	PipelineDir string           `yaml:"pipeline_dir"` // directory containing per-pipeline YAML files
	Pipelines   []PipelineConfig `yaml:"pipelines"`    // inline pipelines (merged with pipeline_dir)
}

// ClickHouseConfig holds ClickHouse connection parameters.
type ClickHouseConfig struct {
	Hosts         []string      `yaml:"hosts"`
	Database      string        `yaml:"database"`
	Username      string        `yaml:"username"`
	Password      string        `yaml:"password"`
	MaxOpenConns  int           `yaml:"max_open_conns"`  // default 10
	MaxIdleConns  int           `yaml:"max_idle_conns"`  // default 5
	Debug         bool          `yaml:"debug"`
	BatchSize     int           `yaml:"batch_size"`       // default 10000
	FlushInterval time.Duration `yaml:"flush_interval"`   // default 5s
}

// IPDBConfig holds IP database configuration.
type IPDBConfig struct {
	Type           string        `yaml:"type"`            // "csv"
	Path           string        `yaml:"path"`            // path to IP CSV file
	Columns        []string      `yaml:"columns"`         // column names in CSV
	ReloadInterval time.Duration `yaml:"reload_interval"` // hot reload interval
}

// PipelineConfig defines one ETL pipeline (one directory → one table).
type PipelineConfig struct {
	Name            string              `yaml:"name"`
	WatchDir        string              `yaml:"watch_dir"`
	FilePattern     string              `yaml:"file_pattern"` // "*" or "*.csv"
	Delimiter       string              `yaml:"delimiter"`    // "|" or "|++|"
	HasHeaderMeta   bool                `yaml:"has_header_meta"`   // first line is common info?
	HeaderMetaKey   string              `yaml:"header_meta_key"`   // key name for first-line data
	SkipHeaderLines int                 `yaml:"skip_header_lines"` // skip N lines at top (usually 1 if has_header_meta)
	FieldNames      []string            `yaml:"field_names"`       // field names if no header in data
	Transformers    []TransformerConfig `yaml:"transformers"`
	ClickHouseTable string              `yaml:"clickhouse_table"`
	Workers         int                 `yaml:"workers"`    // transform worker count
	BatchSize       int                 `yaml:"batch_size"` // override global batch size
}

// TransformerConfig defines a transformer in the pipeline.
type TransformerConfig struct {
	Type        string            `yaml:"type"`
	Fields      []string          `yaml:"fields,omitempty"`      // IP matching: which fields
	LabelFields []string          `yaml:"label_fields,omitempty"` // IP matching: output geo fields
	Field       string            `yaml:"field,omitempty"`       // Dict mapping: source field
	Dict        map[string]string `yaml:"dict,omitempty"`        // Dict mapping: value → mapped value
	DictFile    string            `yaml:"dict_file,omitempty"`   // Dict mapping: external CSV dict file
	Config      map[string]interface{} `yaml:"config,omitempty"` // Custom transformer config
}

// DefaultClickHouseConfig returns sensible defaults.
func DefaultClickHouseConfig() ClickHouseConfig {
	return ClickHouseConfig{
		MaxOpenConns:  10,
		MaxIdleConns:  5,
		BatchSize:     10000,
		FlushInterval: 5 * time.Second,
	}
}
