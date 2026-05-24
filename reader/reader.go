package reader

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"go-etl/model"
)

// Reader parses delimited text files into Row slices.
type Reader struct {
	delimiter    string
	skipLines    int
	fieldNames   []string
	hasHeaderRow bool
	headerMeta   model.Row

	// fast path: single-char delimiter
	singleChar byte
	isSingle   bool
}

// NewReader creates a new Reader.
// delimiter: the field separator ("|" or "|++|")
// fieldNames: field names for each column
// hasHeaderRow: whether the file has a column header line (after header meta)
// headerMeta: pre-parsed header meta row (from first line), merged into every row
func NewReader(delimiter string, fieldNames []string, hasHeaderRow bool, headerMeta model.Row) *Reader {
	r := &Reader{
		delimiter:    delimiter,
		fieldNames:   fieldNames,
		hasHeaderRow: hasHeaderRow,
		headerMeta:   headerMeta,
	}
	if len(delimiter) == 1 {
		r.isSingle = true
		r.singleChar = delimiter[0]
	}
	return r
}

// SetSkipLines configures how many non-empty lines are skipped before parsing.
func (r *Reader) SetSkipLines(skipLines int) {
	if skipLines < 0 {
		skipLines = 0
	}
	r.skipLines = skipLines
}

// ReadAll reads and parses all rows from an io.Reader.
func (r *Reader) ReadAll(rd io.Reader) ([]model.Row, error) {
	var rows []model.Row
	err := r.ReadBatches(rd, 10000, func(batch []model.Row) error {
		rows = append(rows, batch...)
		return nil
	})
	return rows, err
}

// ReadBatches reads rows from an io.Reader and calls onBatch for each batch.
func (r *Reader) ReadBatches(rd io.Reader, batchSize int, onBatch func([]model.Row) error) error {
	if batchSize <= 0 {
		batchSize = 10000
	}

	scanner := bufio.NewScanner(rd)
	// Increase buffer for long lines (max 16MB per line)
	scanner.Buffer(make([]byte, 0, 256*1024), 16*1024*1024)

	batch := make([]model.Row, 0, batchSize)
	lineNum := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		lineNum++

		// Skip configured header lines
		if lineNum <= r.skipLines {
			continue
		}

		// Skip column header row if present
		if r.hasHeaderRow && lineNum == r.skipLines+1 {
			continue
		}

		row, err := r.parseLine(line)
		if err != nil {
			return fmt.Errorf("line %d: %w", lineNum, err)
		}

		// Merge header meta into every data row
		for k, v := range r.headerMeta {
			row[k] = v
		}

		batch = append(batch, row)
		if len(batch) >= batchSize {
			if err := onBatch(batch); err != nil {
				return err
			}
			batch = make([]model.Row, 0, batchSize)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan file: %w", err)
	}

	if len(batch) > 0 {
		if err := onBatch(batch); err != nil {
			return err
		}
	}

	return nil
}

func (r *Reader) parseLine(line string) (model.Row, error) {
	var parts []string
	if r.isSingle {
		parts = splitQuoted(line, r.singleChar)
	} else {
		parts = strings.Split(line, r.delimiter)
	}

	row := make(model.Row, len(r.fieldNames))
	for i, name := range r.fieldNames {
		if i < len(parts) {
			row[name] = strings.TrimSpace(parts[i])
		} else {
			row[name] = ""
		}
	}
	return row, nil
}

// splitQuoted splits a line by a single-char delimiter, handling basic quoting.
func splitQuoted(line string, delim byte) []string {
	if delim == 0 {
		return []string{line}
	}

	var fields []string
	fieldStart := 0
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(line); i++ {
		c := line[i]
		if inQuotes {
			if c == quoteChar {
				inQuotes = false
			}
		} else {
			if c == '"' || c == '\'' {
				inQuotes = true
				quoteChar = c
			} else if c == delim {
				fields = append(fields, line[fieldStart:i])
				fieldStart = i + 1
			}
		}
	}
	fields = append(fields, line[fieldStart:])

	// Trim surrounding quotes
	for i, f := range fields {
		f = strings.TrimSpace(f)
		if len(f) >= 2 {
			if (f[0] == '"' && f[len(f)-1] == '"') || (f[0] == '\'' && f[len(f)-1] == '\'') {
				f = f[1 : len(f)-1]
			}
		}
		fields[i] = f
	}

	return fields
}

// ParseHeaderMeta parses the first line as positional header meta fields.
// The line uses the same delimiter as data rows.
func ParseHeaderMeta(line, delimiter string, fieldNames []string) model.Row {
	meta := make(model.Row)
	if line == "" {
		return meta
	}

	var parts []string
	if len(delimiter) == 1 {
		parts = splitQuoted(line, delimiter[0])
	} else {
		parts = strings.Split(line, delimiter)
	}

	for i, name := range fieldNames {
		if i < len(parts) {
			meta[name] = strings.TrimSpace(parts[i])
		} else {
			meta[name] = ""
		}
	}
	return meta
}
