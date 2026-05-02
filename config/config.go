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
	}
	return nil
}
