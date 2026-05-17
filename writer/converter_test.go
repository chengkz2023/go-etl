package writer

import (
	"net"
	"testing"
	"time"

	"go-etl/model"
)

func TestRowConverterValues(t *testing.T) {
	converter := newRowConverter([]model.FieldDef{
		{Name: "ts", Type: "DateTime", Layout: "2006-01-02 15:04:05"},
		{Name: "status", Type: "UInt16"},
		{Name: "bytes", Type: "UInt64", Default: "0"},
		{Name: "ok", Type: "Bool"},
		{Name: "ip", Type: "IPv4"},
		{Name: "method", Source: "method_name", Type: "LowCardinality(String)"},
	})

	values, err := converter.values(model.Row{
		"ts":          "2026-05-17 10:11:12",
		"status":      "200",
		"ok":          "true",
		"ip":          "192.168.1.10",
		"method_name": "GET",
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := values[0].(time.Time); !ok {
		t.Fatalf("timestamp type = %T, want time.Time", values[0])
	}
	if values[1] != uint16(200) {
		t.Fatalf("status = %#v", values[1])
	}
	if values[2] != uint64(0) {
		t.Fatalf("bytes default = %#v", values[2])
	}
	if values[3] != true {
		t.Fatalf("ok = %#v", values[3])
	}
	if ip, ok := values[4].(net.IP); !ok || ip.String() != "192.168.1.10" {
		t.Fatalf("ip = %#v", values[4])
	}
	if values[5] != "GET" {
		t.Fatalf("method = %#v", values[5])
	}
}

func TestRowConverterNullableEmpty(t *testing.T) {
	converter := newRowConverter([]model.FieldDef{
		{Name: "deleted_at", Type: "Nullable(DateTime)", Nullable: true},
	})

	values, err := converter.values(model.Row{})
	if err != nil {
		t.Fatal(err)
	}
	if values[0] != nil {
		t.Fatalf("nullable empty = %#v, want nil", values[0])
	}
}

func TestRowConverterInvalidNumber(t *testing.T) {
	converter := newRowConverter([]model.FieldDef{
		{Name: "status", Type: "UInt16"},
	})

	_, err := converter.values(model.Row{"status": "abc"})
	if err == nil {
		t.Fatal("expected invalid number error")
	}
}

func TestClickHouseWriterRowValuesUsesSchema(t *testing.T) {
	w := &ClickHouseWriter{
		fieldNames: []string{"raw"},
		converter: newRowConverter([]model.FieldDef{
			{Name: "status", Type: "UInt16"},
		}),
	}

	values, err := w.rowValues(model.Row{"raw": "ignored", "status": "404"})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0] != uint16(404) {
		t.Fatalf("values = %#v", values)
	}
}

func TestClickHouseWriterInsertSQLUsesExplicitColumns(t *testing.T) {
	w := &ClickHouseWriter{
		table:      "cdr.http_cdr",
		fieldNames: []string{"event_time", "client_ip", "status_code"},
	}

	got := w.insertSQL()
	want := "INSERT INTO cdr.http_cdr (event_time, client_ip, status_code)"
	if got != want {
		t.Fatalf("insert sql = %q, want %q", got, want)
	}
}
