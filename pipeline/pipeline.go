package pipeline

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"go-etl/config"
	"go-etl/metrics"
	"go-etl/model"
	"go-etl/reader"
	"go-etl/store"
	"go-etl/transform"
	"go-etl/watcher"
	"go-etl/writer"
)

// Pipeline orchestrates the full ETL flow for one directory/table pair.
type Pipeline struct {
	cfg    config.PipelineConfig
	store  *store.FileStore
	logger *zap.Logger

	chain       transform.Chain
	clickWriter *writer.ClickHouseWriter

	ctx    context.Context
	cancel context.CancelFunc
}

// New creates a new Pipeline.
func New(
	cfg config.PipelineConfig,
	chCfg config.ClickHouseConfig,
	ipdb interface {
		Lookup(ipStr string) map[string]string
	},
	fileStore *store.FileStore,
	logger *zap.Logger,
) (*Pipeline, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Build transform chain
	chain, err := buildTransformChain(cfg.Transformers, ipdb)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("build transform chain: %w", err)
	}

	// Build ClickHouse writer
	writeFields := append([]model.FieldDef{}, cfg.HeaderFields...)
	writeFields = append(writeFields, cfg.Fields...)
	chWriter, err := writer.NewClickHouseWriter(
		writer.ClickHouseConfig{
			Hosts:              chCfg.Hosts,
			Database:           chCfg.Database,
			Username:           chCfg.Username,
			Password:           chCfg.Password,
			MaxOpenConns:       chCfg.MaxOpenConns,
			MaxIdleConns:       chCfg.MaxIdleConns,
			Debug:              chCfg.Debug,
			WriteTimeout:       chCfg.WriteTimeout,
			AsyncInsertEnabled: chCfg.AsyncInsert.Enabled,
			AsyncInsertWait:    chCfg.AsyncInsert.Wait != nil && *chCfg.AsyncInsert.Wait,
		},
		cfg.ClickHouseTable,
		fieldNames(writeFields),
		writeFields,
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create ClickHouse writer: %w", err)
	}

	return &Pipeline{
		cfg:         cfg,
		store:       fileStore,
		logger:      logger.With(zap.String("pipeline", cfg.Name)),
		chain:       chain,
		clickWriter: chWriter,
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// Run starts the pipeline: watches directory, processes files.
func (p *Pipeline) Run() error {
	p.logger.Info("pipeline starting",
		zap.String("watch_dir", p.cfg.WatchDir),
		zap.String("table", p.cfg.ClickHouseTable),
	)

	recovered, err := p.store.ResetProcessingToPending(p.cfg.Name)
	if err != nil {
		return fmt.Errorf("recover processing files: %w", err)
	}
	if recovered > 0 {
		p.logger.Info("recovered interrupted files", zap.Int("files", recovered))
	}
	if p.cfg.RetryFailed {
		retryable, err := p.store.ResetRetryableFailedToPending(p.cfg.Name, p.cfg.MaxRetries, time.Now())
		if err != nil {
			return fmt.Errorf("recover failed files for retry: %w", err)
		}
		if retryable > 0 {
			p.logger.Info("recovered failed files for retry", zap.Int("files", retryable))
		}
	}

	w, err := watcher.New(
		p.cfg.Name,
		p.cfg.WatchDir,
		watcher.ReadyConfig{
			Strategy:     p.cfg.ReadyStrategy,
			FilePattern:  p.cfg.FilePattern,
			TempSuffixes: p.cfg.TempSuffixes,
			MarkerSuffix: p.cfg.MarkerSuffix,
			StableDelay:  p.cfg.StableDelay,
		},
		p.store,
		30*time.Second,
		p.logger,
	)
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	defer w.Stop()

	fileCh := w.Start()

	// Worker pool
	workCh := make(chan string, 100)
	var workerWG sync.WaitGroup
	for i := 0; i < p.cfg.Workers; i++ {
		workerWG.Add(1)
		go p.worker(workCh, &workerWG)
	}

	// Dispatch files to workers
	var producerWG sync.WaitGroup
	producerWG.Add(1)
	go func() {
		defer producerWG.Done()
		for {
			select {
			case <-p.ctx.Done():
				return
			case file, ok := <-fileCh:
				if !ok {
					return
				}
				select {
				case workCh <- file:
				case <-p.ctx.Done():
					return
				}
			}
		}
	}()

	if p.cfg.RetryFailed {
		producerWG.Add(1)
		go p.retryDispatcher(workCh, &producerWG)
	}

	<-p.ctx.Done()
	p.logger.Info("pipeline shutting down")
	w.Stop()

	// Stop producers first, then close the work channel so workers can drain safely.
	producerWG.Wait()
	close(workCh)
	workerWG.Wait()

	if err := p.clickWriter.Close(); err != nil {
		p.logger.Error("close ClickHouse writer", zap.Error(err))
	}
	return nil
}

func (p *Pipeline) worker(workCh <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	for filePath := range workCh {
		p.processFile(filePath)
	}
}

func (p *Pipeline) retryDispatcher(workCh chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()

	interval := p.cfg.RetryInterval
	if interval <= 0 || interval > 10*time.Second {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			retryable, err := p.store.ResetRetryableFailedToPending(p.cfg.Name, p.cfg.MaxRetries, time.Now())
			if err != nil {
				p.logger.Error("recover failed files for retry", zap.Error(err))
				metrics.Inc(p.cfg.Name, "retry_recover_errors_total", 1)
				continue
			}
			if retryable == 0 {
				continue
			}

			pending, err := p.store.ListPending(p.cfg.Name)
			if err != nil {
				p.logger.Error("list retry pending files", zap.Error(err))
				metrics.Inc(p.cfg.Name, "retry_list_errors_total", 1)
				continue
			}
			p.logger.Info("dispatching retry files", zap.Int("files", len(pending)))
			for _, rec := range pending {
				select {
				case workCh <- rec.FilePath:
				case <-p.ctx.Done():
					return
				}
			}
		}
	}
}

func (p *Pipeline) processFile(filePath string) {
	start := time.Now()
	logger := p.logger.With(zap.String("file", filePath))
	metrics.Inc(p.cfg.Name, "files_seen_total", 1)

	claimed, err := p.store.ClaimPending(p.cfg.Name, filePath)
	if err != nil {
		logger.Error("claim file failed", zap.Error(err))
		metrics.Inc(p.cfg.Name, "claim_errors_total", 1)
		return
	}
	if !claimed {
		logger.Debug("file was already claimed or completed")
		metrics.Inc(p.cfg.Name, "files_skipped_total", 1)
		return
	}
	metrics.Inc(p.cfg.Name, "files_processing_total", 1)

	f, err := os.Open(filePath)
	if err != nil {
		logger.Error("open file failed", zap.Error(err))
		p.failFile(filePath, err, logger)
		metrics.Inc(p.cfg.Name, "files_failed_total", 1)
		return
	}
	defer f.Close()

	// Determine header meta and skip header lines
	var headerMeta model.Row
	skipLines := p.cfg.SkipHeaderLines

	if p.cfg.HasHeaderMeta {
		headerMeta = p.readHeaderMeta(f)
		skipLines++ // the meta line itself counts as a skip
		f.Seek(0, 0)
	}

	rdr := reader.NewReader(p.cfg.Delimiter, inputFieldNames(p.cfg.Fields), false, headerMeta)
	rdr.SetSkipLines(skipLines)

	batchSize := p.cfg.BatchSize
	readRows := 0
	processed := 0
	transformErrors := 0
	err = rdr.ReadBatchesWithMeta(f, batchSize, func(batch reader.Batch) error {
		readRows += len(batch.Rows)
		p.applyDedupMeta(filePath, batch)
		transformed := p.chain.BatchTransform(batch.Rows, func(row model.Row, err error) {
			transformErrors++
			logger.Warn("transform row failed", zap.Error(err))
		})

		if len(transformed) > 0 {
			writeStart := time.Now()
			if err := p.clickWriter.WriteBatch(transformed); err != nil {
				metrics.Inc(p.cfg.Name, "batch_write_errors_total", 1)
				return err
			}
			metrics.Inc(p.cfg.Name, "batches_written_total", 1)
			metrics.Inc(p.cfg.Name, "batch_rows_written_total", int64(len(transformed)))
			metrics.ObserveDuration(p.cfg.Name, "batch_write", time.Since(writeStart))
		}
		processed += len(transformed)
		return nil
	})
	if err != nil {
		logger.Error("process file failed", zap.Error(err))
		p.failFile(filePath, err, logger)
		metrics.Inc(p.cfg.Name, "files_failed_total", 1)
		return
	}

	if processed == 0 {
		logger.Info("file has no data rows, skipping")
	}

	if err := p.store.SetDone(p.cfg.Name, filePath, int64(processed)); err != nil {
		logger.Error("mark done failed", zap.Error(err))
	}
	p.afterSuccess(filePath, logger)

	duration := time.Since(start)
	metrics.Inc(p.cfg.Name, "files_done_total", 1)
	metrics.Inc(p.cfg.Name, "rows_read_total", int64(readRows))
	metrics.Inc(p.cfg.Name, "rows_written_total", int64(processed))
	metrics.Inc(p.cfg.Name, "transform_errors_total", int64(transformErrors))
	metrics.ObserveDuration(p.cfg.Name, "file_process", duration)
	logger.Info("file processed",
		zap.Int("rows_read", readRows),
		zap.Int("rows_written", processed),
		zap.Int("transform_errors", transformErrors),
		zap.Duration("duration", duration),
	)
}

func (p *Pipeline) applyDedupMeta(filePath string, batch reader.Batch) {
	if !p.cfg.Dedup.Enabled {
		return
	}
	for i, row := range batch.Rows {
		if i >= len(batch.Meta) {
			continue
		}
		meta := batch.Meta[i]
		sourceFile := filepath.Base(filePath)
		lineNumber := strconv.Itoa(meta.LineNumber)
		rowHash := meta.RawHash
		recordID := stableRecordID(filePath, lineNumber, rowHash)

		row[p.cfg.Dedup.SourceFileField] = sourceFile
		row[p.cfg.Dedup.LineNumberField] = lineNumber
		row[p.cfg.Dedup.RowHashField] = rowHash
		row[p.cfg.Dedup.RecordIDField] = recordID
	}
}

func stableRecordID(parts ...string) string {
	h := sha1.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (p *Pipeline) afterSuccess(filePath string, logger *zap.Logger) {
	if p.cfg.CleanupMarker {
		if err := p.cleanupMarker(filePath); err != nil {
			logger.Warn("cleanup marker failed", zap.Error(err))
			metrics.Inc(p.cfg.Name, "marker_cleanup_errors_total", 1)
		} else {
			metrics.Inc(p.cfg.Name, "markers_cleaned_total", 1)
		}
	}
	if p.cfg.ArchiveDir == "" {
		return
	}
	targetPath, err := moveFileToDir(filePath, p.cfg.ArchiveDir)
	if err != nil {
		logger.Warn("archive file failed", zap.Error(err))
		metrics.Inc(p.cfg.Name, "archive_errors_total", 1)
		return
	}
	p.moveMarkerWithData(filePath, targetPath, logger)
	metrics.Inc(p.cfg.Name, "files_archived_total", 1)
	logger.Info("file archived", zap.String("target", targetPath))
}

func (p *Pipeline) cleanupMarker(filePath string) error {
	if p.cfg.ReadyStrategy != watcher.ReadyMarker {
		return nil
	}
	markerPath := filePath + p.cfg.MarkerSuffix
	if err := os.Remove(markerPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (p *Pipeline) failFile(filePath string, cause error, logger *zap.Logger) {
	if !p.cfg.RetryFailed {
		if err := p.store.SetFailed(p.cfg.Name, filePath, cause.Error()); err != nil {
			logger.Error("mark failed failed", zap.Error(err))
		}
		metrics.Inc(p.cfg.Name, "files_failed_no_retry_total", 1)
		return
	}

	if err := p.store.SetFailedForRetry(p.cfg.Name, filePath, cause.Error(), p.cfg.RetryInterval); err != nil {
		logger.Error("mark failed for retry failed", zap.Error(err))
		metrics.Inc(p.cfg.Name, "retry_mark_errors_total", 1)
		return
	}
	metrics.Inc(p.cfg.Name, "files_scheduled_retry_total", 1)

	rec, err := p.store.GetRecord(p.cfg.Name, filePath)
	if err != nil {
		logger.Error("load failed record failed", zap.Error(err))
		return
	}
	if rec == nil || rec.Attempts < p.cfg.MaxRetries {
		return
	}

	targetPath, moveErr := p.moveToDeadLetter(filePath)
	if moveErr != nil {
		logger.Error("move dead-letter file failed", zap.Error(moveErr))
		return
	}
	p.moveMarkerWithData(filePath, targetPath, logger)
	if err := p.store.MarkDead(p.cfg.Name, filePath, targetPath); err != nil {
		logger.Error("mark dead-letter failed", zap.Error(err))
		return
	}
	metrics.Inc(p.cfg.Name, "files_dead_total", 1)
	logger.Error("file moved to dead-letter", zap.String("target", targetPath), zap.Int("attempts", rec.Attempts))
}

func (p *Pipeline) moveToDeadLetter(filePath string) (string, error) {
	if p.cfg.DeadLetterDir == "" {
		return "", nil
	}
	return moveFileToDir(filePath, p.cfg.DeadLetterDir)
}

func (p *Pipeline) moveMarkerWithData(sourcePath, targetPath string, logger *zap.Logger) {
	if p.cfg.ReadyStrategy != watcher.ReadyMarker || p.cfg.MarkerSuffix == "" || targetPath == "" {
		return
	}
	sourceMarker := sourcePath + p.cfg.MarkerSuffix
	if _, err := os.Stat(sourceMarker); os.IsNotExist(err) {
		return
	} else if err != nil {
		logger.Warn("stat marker failed", zap.Error(err))
		return
	}

	targetMarker := targetPath + p.cfg.MarkerSuffix
	if err := os.MkdirAll(filepath.Dir(targetMarker), 0755); err != nil {
		logger.Warn("create marker target dir failed", zap.Error(err))
		return
	}
	if err := os.Rename(sourceMarker, targetMarker); err != nil {
		logger.Warn("move marker failed", zap.String("marker", sourceMarker), zap.Error(err))
	}
}

func moveFileToDir(filePath, targetDir string) (string, error) {
	if targetDir == "" {
		return "", nil
	}
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return "", err
	}

	targetPath := filepath.Join(targetDir, filepath.Base(filePath))
	if _, err := os.Stat(targetPath); err == nil {
		ext := filepath.Ext(targetPath)
		base := strings.TrimSuffix(filepath.Base(targetPath), ext)
		targetPath = filepath.Join(targetDir, fmt.Sprintf("%s.%d%s", base, time.Now().UnixNano(), ext))
	} else if !os.IsNotExist(err) {
		return "", err
	}

	return targetPath, os.Rename(filePath, targetPath)
}

// readHeaderMeta reads the first non-empty line and parses it as header meta.
func (p *Pipeline) readHeaderMeta(f *os.File) model.Row {
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		return reader.ParseHeaderMeta(line, p.cfg.Delimiter, fieldNames(p.cfg.HeaderFields))
	}
	return model.Row{}
}

func fieldNames(fields []model.FieldDef) []string {
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.Name
	}
	return names
}

func inputFieldNames(fields []model.FieldDef) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.Generated {
			continue
		}
		if field.Source != "" {
			names = append(names, field.Source)
			continue
		}
		names = append(names, field.Name)
	}
	return names
}

// Shutdown gracefully stops the pipeline.
func (p *Pipeline) Shutdown() {
	p.cancel()
}

// buildTransformChain creates a transform.Chain from config.
func buildTransformChain(tConfigs []config.TransformerConfig, ipdb interface {
	Lookup(ipStr string) map[string]string
}) (transform.Chain, error) {
	var chain transform.Chain
	for _, tc := range tConfigs {
		switch tc.Type {
		case "ip_matcher":
			if ipdb == nil {
				return nil, fmt.Errorf("ip_matcher requires IP database but none configured")
			}
			chain = append(chain, &ipMatcherAdapter{
				db:          ipdb,
				fields:      tc.Fields,
				labelFields: tc.LabelFields,
			})

		case "dict_mapper":
			var dm *transform.DictMapper
			var err error
			if tc.DictFile != "" {
				dm, err = transform.NewDictMapperFromFile(tc.Field, tc.DictFile)
			} else {
				dm = transform.NewDictMapper(tc.Field, tc.Dict)
			}
			if err != nil {
				return nil, fmt.Errorf("create dict_mapper: %w", err)
			}
			chain = append(chain, dm)

		case "custom":
			// Placeholder for user-registered custom transformers
			continue

		default:
			return nil, fmt.Errorf("unknown transformer type: %s", tc.Type)
		}
	}
	return chain, nil
}

// ipMatcherAdapter wraps IP lookup as a Transformer.
type ipMatcherAdapter struct {
	db interface {
		Lookup(ipStr string) map[string]string
	}
	fields      []string
	labelFields []string
}

func (m *ipMatcherAdapter) Name() string { return "ip_matcher" }

func (m *ipMatcherAdapter) Transform(row model.Row) (model.Row, error) {
	for _, field := range m.fields {
		ip, ok := row[field]
		if !ok || ip == "" {
			continue
		}

		attrs := m.db.Lookup(ip)
		if attrs == nil {
			continue
		}

		fieldIdx := indexOfStr(m.fields, field)
		prefix := field
		if fieldIdx >= 0 && fieldIdx < len(m.labelFields) {
			prefix = m.labelFields[fieldIdx]
		}

		for k, v := range attrs {
			row[prefix+"_"+k] = v
		}
	}
	return row, nil
}

func indexOfStr(slice []string, item string) int {
	for i, s := range slice {
		if s == item {
			return i
		}
	}
	return -1
}
