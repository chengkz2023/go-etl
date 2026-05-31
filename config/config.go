package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"gopkg.in/yaml.v3"
)

// Load reads and parses a YAML config file, including any pipeline files
// found in the pipeline_dir subdirectory.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Load per-pipeline files from pipeline_dir
	if cfg.PipelineDir != "" {
		// Resolve relative paths from the config file's directory
		baseDir := filepath.Dir(path)
		pipelineDir := cfg.PipelineDir
		if !filepath.IsAbs(pipelineDir) {
			pipelineDir = filepath.Join(baseDir, pipelineDir)
		}

		entries, err := os.ReadDir(pipelineDir)
		if err != nil {
			return nil, fmt.Errorf("read pipeline dir %s: %w", pipelineDir, err)
		}

		// Sort for deterministic load order
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if filepath.Ext(entry.Name()) != ".yaml" && filepath.Ext(entry.Name()) != ".yml" {
				continue
			}

			filePath := filepath.Join(pipelineDir, entry.Name())
			pData, err := os.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("read pipeline file %s: %w", filePath, err)
			}

			var pCfg PipelineConfig
			if err := yaml.Unmarshal(pData, &pCfg); err != nil {
				return nil, fmt.Errorf("parse pipeline file %s: %w", filePath, err)
			}
			cfg.Pipelines = append(cfg.Pipelines, pCfg)
		}
	}

	// Apply defaults
	cfg.applyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Metrics.Enabled && c.Metrics.Addr == "" {
		c.Metrics.Addr = ":9090"
	}
	if c.ClickHouse.MaxOpenConns == 0 {
		c.ClickHouse.MaxOpenConns = 10
	}
	if c.ClickHouse.MaxIdleConns == 0 {
		c.ClickHouse.MaxIdleConns = 5
	}
	if c.ClickHouse.BatchSize == 0 {
		c.ClickHouse.BatchSize = 10000
	}
	if c.ClickHouse.FlushInterval == 0 {
		c.ClickHouse.FlushInterval = 5_000_000_000 // 5s in ns
	}
	if c.ClickHouse.WriteTimeout == 0 {
		c.ClickHouse.WriteTimeout = 60_000_000_000 // 60s in ns
	}
	if c.ClickHouse.AsyncInsert.Wait == nil {
		wait := true
		c.ClickHouse.AsyncInsert.Wait = &wait
	}

	for i := range c.Pipelines {
		if c.Pipelines[i].Workers == 0 {
			c.Pipelines[i].Workers = 4
		}
		if c.Pipelines[i].BatchSize == 0 {
			c.Pipelines[i].BatchSize = c.ClickHouse.BatchSize
		}
		if c.Pipelines[i].FilePattern == "" {
			c.Pipelines[i].FilePattern = "*"
		}
		if c.Pipelines[i].Delimiter == "" {
			c.Pipelines[i].Delimiter = "|"
		}
		if c.Pipelines[i].ReadyStrategy == "" {
			c.Pipelines[i].ReadyStrategy = "atomic_rename"
		}
		if len(c.Pipelines[i].TempSuffixes) == 0 {
			c.Pipelines[i].TempSuffixes = []string{".tmp", ".writing"}
		}
		if c.Pipelines[i].MarkerSuffix == "" {
			c.Pipelines[i].MarkerSuffix = ".ok"
		}
		if c.Pipelines[i].StableDelay == 0 {
			c.Pipelines[i].StableDelay = 10_000_000_000 // 10s in ns
		}
		if c.Pipelines[i].MaxRetries == 0 {
			c.Pipelines[i].MaxRetries = 3
		}
		if c.Pipelines[i].RetryInterval == 0 {
			c.Pipelines[i].RetryInterval = 60_000_000_000 // 60s in ns
		}
		if c.Pipelines[i].Dedup.RecordIDField == "" {
			c.Pipelines[i].Dedup.RecordIDField = "_etl_record_id"
		}
		if c.Pipelines[i].Dedup.SourceFileField == "" {
			c.Pipelines[i].Dedup.SourceFileField = "_etl_source_file"
		}
		if c.Pipelines[i].Dedup.LineNumberField == "" {
			c.Pipelines[i].Dedup.LineNumberField = "_etl_line_number"
		}
		if c.Pipelines[i].Dedup.RowHashField == "" {
			c.Pipelines[i].Dedup.RowHashField = "_etl_row_hash"
		}
	}
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	if len(c.ClickHouse.Hosts) == 0 {
		return fmt.Errorf("clickhouse.hosts is required")
	}
	if c.ClickHouse.Database == "" {
		return fmt.Errorf("clickhouse.database is required")
	}
	if c.ClickHouse.WriteTimeout < 0 {
		return fmt.Errorf("clickhouse.write_timeout must be >= 0")
	}
	if c.IPDB.CacheSize < 0 {
		return fmt.Errorf("ip_db.cache_size must be >= 0")
	}
	if len(c.Pipelines) == 0 {
		return fmt.Errorf("at least one pipeline is required (configure inline or via pipeline_dir)")
	}

	names := make(map[string]bool)
	for i, p := range c.Pipelines {
		if p.Name == "" {
			return fmt.Errorf("pipeline[%d]: name is required", i)
		}
		if names[p.Name] {
			return fmt.Errorf("pipeline[%d]: duplicate name %q", i, p.Name)
		}
		names[p.Name] = true

		if p.WatchDir == "" {
			return fmt.Errorf("pipeline %q: watch_dir is required", p.Name)
		}
		if p.ClickHouseTable == "" {
			return fmt.Errorf("pipeline %q: clickhouse_table is required", p.Name)
		}
		if p.MaxRetries < 0 {
			return fmt.Errorf("pipeline %q: max_retries must be >= 0", p.Name)
		}
		if p.RetryInterval < 0 {
			return fmt.Errorf("pipeline %q: retry_interval must be >= 0", p.Name)
		}
		if len(p.Fields) == 0 {
			return fmt.Errorf("pipeline %q: fields is required", p.Name)
		}
		if p.HasHeaderMeta && len(p.HeaderFields) == 0 {
			return fmt.Errorf("pipeline %q: header_fields is required when has_header_meta is true", p.Name)
		}
		for j, f := range p.HeaderFields {
			if f.Name == "" {
				return fmt.Errorf("pipeline %q: header_fields[%d].name is required", p.Name, j)
			}
			if f.Type == "" {
				return fmt.Errorf("pipeline %q: header_fields[%d].type is required", p.Name, j)
			}
		}
		for j, f := range p.Fields {
			if f.Name == "" {
				return fmt.Errorf("pipeline %q: fields[%d].name is required", p.Name, j)
			}
			if f.Type == "" {
				return fmt.Errorf("pipeline %q: fields[%d].type is required", p.Name, j)
			}
		}
		switch p.ReadyStrategy {
		case "atomic_rename", "marker", "stable_size":
		default:
			return fmt.Errorf("pipeline %q: ready_strategy must be atomic_rename, marker, or stable_size", p.Name)
		}
	}
	return nil
}
