package writer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"go-etl/model"
)

// ClickHouseWriter batches rows and writes them to ClickHouse.
type ClickHouseWriter struct {
	conn         driver.Conn
	table        string
	fieldNames   []string
	converter    *rowConverter
	writeTimeout time.Duration
	insertFunc   func([]model.Row) error
}

// NewClickHouseWriter creates a new writer connected to ClickHouse.
func NewClickHouseWriter(cfg ClickHouseConfig, table string, fieldNames []string, fields []model.FieldDef) (*ClickHouseWriter, error) {
	settings := clickhouse.Settings{}
	if cfg.AsyncInsertEnabled {
		settings["async_insert"] = 1
		if cfg.AsyncInsertWait {
			settings["wait_for_async_insert"] = 1
		} else {
			settings["wait_for_async_insert"] = 0
		}
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: cfg.Hosts,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		MaxOpenConns: cfg.MaxOpenConns,
		MaxIdleConns: cfg.MaxIdleConns,
		Debug:        cfg.Debug,
		Settings:     settings,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to ClickHouse: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("ping ClickHouse: %w", err)
	}

	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 60 * time.Second
	}

	w := &ClickHouseWriter{
		conn:         conn,
		table:        table,
		fieldNames:   fieldNames,
		writeTimeout: cfg.WriteTimeout,
	}
	if len(fields) > 0 {
		w.converter = newRowConverter(fields)
		w.fieldNames = w.converter.fieldNames()
	}

	return w, nil
}

// ClickHouseConfig is a simplified config for the writer.
type ClickHouseConfig struct {
	Hosts              []string
	Database           string
	Username           string
	Password           string
	MaxOpenConns       int
	MaxIdleConns       int
	Debug              bool
	WriteTimeout       time.Duration
	AsyncInsertEnabled bool
	AsyncInsertWait    bool
}

// Write writes one row immediately. Prefer WriteBatch for ClickHouse throughput.
func (w *ClickHouseWriter) Write(row model.Row) error {
	return w.WriteBatch([]model.Row{row})
}

// WriteBatch writes one independent ClickHouse batch.
func (w *ClickHouseWriter) WriteBatch(rows []model.Row) error {
	if len(rows) == 0 {
		return nil
	}
	if w.insertFunc != nil {
		return w.insertFunc(rows)
	}
	return w.insert(rows)
}

// insert performs a batch insert into ClickHouse.
func (w *ClickHouseWriter) insert(rows []model.Row) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.writeTimeout)
	defer cancel()

	batch, err := w.conn.PrepareBatch(ctx, w.insertSQL())
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, row := range rows {
		values, err := w.rowValues(row)
		if err != nil {
			return err
		}
		if err := batch.Append(values...); err != nil {
			return fmt.Errorf("append to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	return nil
}

func (w *ClickHouseWriter) insertSQL() string {
	if len(w.fieldNames) == 0 {
		return fmt.Sprintf("INSERT INTO %s", w.table)
	}
	return fmt.Sprintf("INSERT INTO %s (%s)", w.table, strings.Join(w.fieldNames, ", "))
}

func (w *ClickHouseWriter) rowValues(row model.Row) ([]interface{}, error) {
	if w.converter != nil {
		return w.converter.values(row)
	}

	values := make([]interface{}, len(w.fieldNames))
	for i, name := range w.fieldNames {
		values[i] = row[name]
	}
	return values, nil
}

// Close closes the ClickHouse connection.
func (w *ClickHouseWriter) Close() error {
	return w.conn.Close()
}
