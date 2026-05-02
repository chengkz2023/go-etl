package writer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"go-etl/model"
)

// ClickHouseWriter batches rows and writes them to ClickHouse.
type ClickHouseWriter struct {
	conn          driver.Conn
	table         string
	fieldNames    []string
	batchSize     int
	flushInterval time.Duration

	mu     sync.Mutex
	buffer []model.Row

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// NewClickHouseWriter creates a new writer connected to ClickHouse.
func NewClickHouseWriter(cfg ClickHouseConfig, table string, fieldNames []string, batchSize int, flushInterval time.Duration) (*ClickHouseWriter, error) {
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
	})
	if err != nil {
		return nil, fmt.Errorf("connect to ClickHouse: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("ping ClickHouse: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	w := &ClickHouseWriter{
		conn:          conn,
		table:         table,
		fieldNames:    fieldNames,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
	}

	// Start background flusher
	go w.flushLoop()

	return w, nil
}

// ClickHouseConfig is a simplified config for the writer.
type ClickHouseConfig struct {
	Hosts        []string
	Database     string
	Username     string
	Password     string
	MaxOpenConns int
	MaxIdleConns int
	Debug        bool
}

// Write adds a row to the buffer. Flushes if buffer is full.
func (w *ClickHouseWriter) Write(row model.Row) error {
	w.mu.Lock()
	w.buffer = append(w.buffer, row)
	shouldFlush := len(w.buffer) >= w.batchSize
	w.mu.Unlock()

	if shouldFlush {
		return w.Flush()
	}
	return nil
}

// WriteBatch adds multiple rows and flushes if buffer exceeds size.
func (w *ClickHouseWriter) WriteBatch(rows []model.Row) error {
	w.mu.Lock()
	w.buffer = append(w.buffer, rows...)
	shouldFlush := len(w.buffer) >= w.batchSize
	w.mu.Unlock()

	if shouldFlush {
		return w.Flush()
	}
	return nil
}

// Flush writes all buffered rows to ClickHouse.
func (w *ClickHouseWriter) Flush() error {
	w.mu.Lock()
	if len(w.buffer) == 0 {
		w.mu.Unlock()
		return nil
	}
	batch := w.buffer
	w.buffer = make([]model.Row, 0, w.batchSize)
	w.mu.Unlock()

	return w.insert(batch)
}

// insert performs a batch insert into ClickHouse.
func (w *ClickHouseWriter) insert(rows []model.Row) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := w.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", w.table))
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, row := range rows {
		values := make([]interface{}, len(w.fieldNames))
		for i, name := range w.fieldNames {
			values[i] = row[name]
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

// flushLoop periodically flushes the buffer.
func (w *ClickHouseWriter) flushLoop() {
	defer close(w.done)
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			// Final flush before exit
			w.Flush()
			return
		case <-ticker.C:
			w.Flush()
		}
	}
}

// Close flushes remaining data and closes the connection.
func (w *ClickHouseWriter) Close() error {
	w.cancel()
	<-w.done
	return w.conn.Close()
}
