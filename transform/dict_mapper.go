package transform

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"go-etl/model"
)

// DictMapper maps values in a field using a lookup dictionary.
type DictMapper struct {
	Field   string            // source field name
	Dict    map[string]string // value → mapped value
	Target  string            // output field name (defaults to Field if empty)
	Default string            // default value if no match (empty = keep original)
}

// NewDictMapper creates a dict mapper from an inline map.
func NewDictMapper(field string, dict map[string]string) *DictMapper {
	return &DictMapper{
		Field: field,
		Dict:  dict,
	}
}

// NewDictMapperFromFile creates a dict mapper from a CSV file.
// File format: key,value (one per line)
func NewDictMapperFromFile(field, filePath string) (*DictMapper, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open dict file: %w", err)
	}
	defer f.Close()

	dict := make(map[string]string)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ",", 2)
		if len(parts) == 2 {
			dict[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan dict file: %w", err)
	}

	return &DictMapper{Field: field, Dict: dict}, nil
}

// Name returns the transformer name.
func (m *DictMapper) Name() string { return "dict_mapper" }

// Transform maps the configured field through the dictionary.
func (m *DictMapper) Transform(row model.Row) (model.Row, error) {
	val, ok := row[m.Field]
	if !ok {
		return row, nil
	}

	mapped, ok := m.Dict[val]
	if ok {
		target := m.Target
		if target == "" {
			target = m.Field
		}
		row[target] = mapped
	} else if m.Default != "" {
		target := m.Target
		if target == "" {
			target = m.Field
		}
		row[target] = m.Default
	}
	return row, nil
}
