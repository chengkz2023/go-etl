package writer

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"go-etl/model"
)

type rowConverter struct {
	fields []model.FieldDef
}

func newRowConverter(fields []model.FieldDef) *rowConverter {
	return &rowConverter{fields: fields}
}

func (c *rowConverter) fieldNames() []string {
	names := make([]string, len(c.fields))
	for i, f := range c.fields {
		names[i] = f.Name
	}
	return names
}

func (c *rowConverter) values(row model.Row) ([]interface{}, error) {
	values := make([]interface{}, len(c.fields))
	for i, field := range c.fields {
		value, err := convertField(row, field)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field.Name, err)
		}
		values[i] = value
	}
	return values, nil
}

func convertField(row model.Row, field model.FieldDef) (interface{}, error) {
	source := field.Source
	if source == "" {
		source = field.Name
	}

	raw := strings.TrimSpace(row[source])
	if raw == "" {
		raw = field.Default
	}
	if raw == "" && field.Nullable {
		return nil, nil
	}

	return convertValue(raw, field.Type, field.Layout)
}

func convertValue(raw, typ, layout string) (interface{}, error) {
	baseType := normalizeType(typ)

	switch baseType {
	case "", "string", "fixedstring", "enum8", "enum16", "uuid":
		return raw, nil
	case "bool":
		if raw == "" {
			return false, nil
		}
		return strconv.ParseBool(raw)
	case "uint8":
		v, err := parseUint(raw, 8)
		return uint8(v), err
	case "uint16":
		v, err := parseUint(raw, 16)
		return uint16(v), err
	case "uint32":
		v, err := parseUint(raw, 32)
		return uint32(v), err
	case "uint64":
		return parseUint(raw, 64)
	case "int8":
		v, err := parseInt(raw, 8)
		return int8(v), err
	case "int16":
		v, err := parseInt(raw, 16)
		return int16(v), err
	case "int32":
		v, err := parseInt(raw, 32)
		return int32(v), err
	case "int64", "int":
		return parseInt(raw, 64)
	case "float32":
		v, err := strconv.ParseFloat(raw, 32)
		return float32(v), err
	case "float64", "float":
		return strconv.ParseFloat(raw, 64)
	case "date", "datetime", "datetime64":
		return parseTime(raw, layout)
	case "ipv4", "ipv6":
		ip := net.ParseIP(raw)
		if ip == nil {
			return nil, fmt.Errorf("invalid IP %q", raw)
		}
		return ip, nil
	default:
		return raw, nil
	}
}

func normalizeType(typ string) string {
	typ = strings.TrimSpace(strings.ToLower(typ))
	for {
		switch {
		case strings.HasPrefix(typ, "nullable(") && strings.HasSuffix(typ, ")"):
			typ = strings.TrimSuffix(strings.TrimPrefix(typ, "nullable("), ")")
		case strings.HasPrefix(typ, "lowcardinality(") && strings.HasSuffix(typ, ")"):
			typ = strings.TrimSuffix(strings.TrimPrefix(typ, "lowcardinality("), ")")
		default:
			if idx := strings.IndexByte(typ, '('); idx > 0 {
				return typ[:idx]
			}
			return typ
		}
	}
}

func parseUint(raw string, bitSize int) (uint64, error) {
	if raw == "" {
		raw = "0"
	}
	return strconv.ParseUint(raw, 10, bitSize)
}

func parseInt(raw string, bitSize int) (int64, error) {
	if raw == "" {
		raw = "0"
	}
	return strconv.ParseInt(raw, 10, bitSize)
}

func parseTime(raw, layout string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	if layout != "" {
		return time.ParseInLocation(layout, raw, time.Local)
	}
	for _, candidate := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999",
		"2006-01-02",
	} {
		t, err := time.ParseInLocation(candidate, raw, time.Local)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time %q", raw)
}
