package pipeline

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"go-etl/config"
	"go-etl/model"
	"go-etl/reader"
	"go-etl/store"
	"go-etl/transform"
	"go-etl/watcher"
	"go-etl/writer"
)

// Pipeline orchestrates the full ETL flow for one directory/table pair.
type Pipeline struct {
	cfg     config.PipelineConfig
	store   *store.FileStore
	logger  *zap.Logger

	chain       transform.Chain
	clickWriter *writer.ClickHouseWriter

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a new Pipeline.
func New(
	cfg config.PipelineConfig,
	chCfg config.ClickHouseConfig,
	ipdb interface{ Lookup(ipStr string) map[string]string },
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
	chWriter, err := writer.NewClickHouseWriter(
		writer.ClickHouseConfig{
			Hosts:        chCfg.Hosts,
			Database:     chCfg.Database,
			Username:     chCfg.Username,
			Password:     chCfg.Password,
			MaxOpenConns: chCfg.MaxOpenConns,
			MaxIdleConns: chCfg.MaxIdleConns,
			Debug:        chCfg.Debug,
		},
		cfg.ClickHouseTable,
		cfg.FieldNames,
		cfg.BatchSize,
		chCfg.FlushInterval,
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

	w, err := watcher.New(p.cfg.Name, p.cfg.WatchDir, p.cfg.FilePattern, p.store, 30*time.Second, p.logger)
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	defer w.Stop()

	fileCh := w.Start()

	// Worker pool
	workCh := make(chan string, 100)
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(workCh)
	}

	// Dispatch files to workers
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(workCh)
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

	<-p.ctx.Done()
	p.logger.Info("pipeline shutting down")
	w.Stop()
	p.wg.Wait()

	if err := p.clickWriter.Close(); err != nil {
		p.logger.Error("close ClickHouse writer", zap.Error(err))
	}
	return nil
}

func (p *Pipeline) worker(workCh <-chan string) {
	defer p.wg.Done()
	for filePath := range workCh {
		p.processFile(filePath)
	}
}

func (p *Pipeline) processFile(filePath string) {
	logger := p.logger.With(zap.String("file", filePath))

	if err := p.store.SetProcessing(p.cfg.Name, filePath); err != nil {
		logger.Error("mark processing failed", zap.Error(err))
	}

	f, err := os.Open(filePath)
	if err != nil {
		logger.Error("open file failed", zap.Error(err))
		p.store.SetFailed(p.cfg.Name, filePath, err.Error())
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

	// Build reader for this file
	rdr := reader.NewReader(p.cfg.Delimiter, p.cfg.FieldNames, false, headerMeta)
	// We handle skip ourselves since we already advanced
	// Actually, pass skipLines through the reader's internal counter
	// Since the reader doesn't expose skipLines after construction, we read manually:

	rows, err := p.readRowsWithSkip(f, rdr, skipLines)
	if err != nil {
		logger.Error("read file failed", zap.Error(err))
		p.store.SetFailed(p.cfg.Name, filePath, err.Error())
		return
	}

	if len(rows) == 0 {
		logger.Info("file has no data rows, skipping")
		p.store.SetDone(p.cfg.Name, filePath, 0)
		return
	}

	// Transform and write
	batchSize := p.cfg.BatchSize
	processed := 0

	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		transformed := p.chain.BatchTransform(batch, func(row model.Row, err error) {
			logger.Warn("transform row failed", zap.Error(err))
		})

		if len(transformed) > 0 {
			if err := p.clickWriter.WriteBatch(transformed); err != nil {
				logger.Error("write batch failed", zap.Error(err))
				p.store.SetFailed(p.cfg.Name, filePath, err.Error())
				return
			}
		}
		processed += len(transformed)
	}

	if err := p.clickWriter.Flush(); err != nil {
		logger.Error("flush failed", zap.Error(err))
		p.store.SetFailed(p.cfg.Name, filePath, err.Error())
		return
	}

	if err := p.store.SetDone(p.cfg.Name, filePath, int64(processed)); err != nil {
		logger.Error("mark done failed", zap.Error(err))
	}

	logger.Info("file processed", zap.Int("rows", processed))
}

// readHeaderMeta reads the first non-empty line and parses it as header meta.
func (p *Pipeline) readHeaderMeta(f *os.File) model.Row {
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		return reader.ParseHeaderMeta(line, p.cfg.Delimiter)
	}
	return model.Row{}
}

// readRowsWithSkip reads all data rows, skipping the first skipLines non-empty lines.
func (p *Pipeline) readRowsWithSkip(f *os.File, rdr *reader.Reader, skipLines int) ([]model.Row, error) {
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 256*1024), 16*1024*1024)

	var allLines []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		allLines = append(allLines, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Skip header lines
	start := skipLines
	if start > len(allLines) {
		return nil, nil
	}
	if start < 0 {
		start = 0
	}

	dataLines := strings.Join(allLines[start:], "\n")
	return rdr.ReadAll(strings.NewReader(dataLines))
}

// Shutdown gracefully stops the pipeline.
func (p *Pipeline) Shutdown() {
	p.cancel()
	p.wg.Wait()
}

// buildTransformChain creates a transform.Chain from config.
func buildTransformChain(tConfigs []config.TransformerConfig, ipdb interface{ Lookup(ipStr string) map[string]string }) (transform.Chain, error) {
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
	db          interface{ Lookup(ipStr string) map[string]string }
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
