# ETL Stress Test

This example creates repeatable DNS and HTTP CDR input files for end-to-end ETL pressure testing.

It exercises:

- file discovery with marker and atomic rename modes
- first-line header fields
- delimited parsing with `|` and `|++|`
- IP matching
- dictionary mapping
- ClickHouse batch writes
- file status persistence
- expvar metrics

## 1. Prepare ClickHouse

Run from the repository root:

```bash
clickhouse-client --multiquery < examples/stress-test/schema.sql
```

The script recreates:

```text
cdr.stress_dns_cdr
cdr.stress_http_cdr
```

## 2. Generate Input Files

Default generation creates 20 DNS files and 20 HTTP files, each with 50,000 data rows.

```bash
go run ./examples/stress-test/generator
```

That produces 2,000,000 total rows:

```text
DNS:  20 * 50,000 = 1,000,000 rows
HTTP: 20 * 50,000 = 1,000,000 rows
```

You can change the scale:

```bash
go run ./examples/stress-test/generator -dns-files 100 -http-files 100 -rows 100000
```

Useful options:

```text
-dns-files    number of DNS files
-http-files   number of HTTP files
-rows         data rows per file
-clean        clean watch/archive/dead directories and file_status.db before generating
-root         stress-test directory
```

## 3. Start ETL

Use the current build:

```bash
go build -o dist/go-etl-stress ./cmd/etl
./dist/go-etl-stress -config examples/stress-test/config.yaml -store examples/stress-test/file_status.db -log info
```

On Windows PowerShell:

```powershell
go build -o dist\go-etl-stress.exe .\cmd\etl
.\dist\go-etl-stress.exe -config examples\stress-test\config.yaml -store examples\stress-test\file_status.db -log info
```

For a Linux one-command run, build `dist/go-etl-linux-amd64` and execute:

```bash
cd examples/stress-test
chmod +x run_stress.sh bin/go-etl-linux-amd64 bin/stress-generator-linux-amd64
DNS_FILES=20 HTTP_FILES=20 ROWS_PER_FILE=50000 ./run_stress.sh
```

The script recreates ClickHouse tables, generates input files, starts ETL, polls ClickHouse row counts, prints elapsed time and approximate rows per second, then stops ETL.

## 4. Watch Metrics

Metrics are exposed through expvar:

```bash
curl http://127.0.0.1:9090/debug/vars
```

Important counters:

```text
go_etl_stress_dns_cdr_files_done_total
go_etl_stress_dns_cdr_rows_read_total
go_etl_stress_dns_cdr_rows_written_total
go_etl_stress_dns_cdr_file_process_ms_total
go_etl_stress_http_cdr_files_done_total
go_etl_stress_http_cdr_rows_read_total
go_etl_stress_http_cdr_rows_written_total
go_etl_stress_http_cdr_file_process_ms_total
```

Approximate throughput can be calculated as:

```text
rows_written_total / (file_process_ms_total / 1000)
```

This is per-file processing time summed across files, so it is useful for relative comparisons between runs rather than exact wall-clock throughput.

## 5. Verify Results

```sql
SELECT count() FROM cdr.stress_dns_cdr;
SELECT count() FROM cdr.stress_http_cdr;

SELECT
    min(event_time),
    max(event_time),
    count()
FROM cdr.stress_dns_cdr;

SELECT
    min(event_time),
    max(event_time),
    count()
FROM cdr.stress_http_cdr;
```

For the default generator settings, each table should contain 1,000,000 rows.

## 6. Baseline Result

Stable baseline from a local VM test:

```text
OS: CentOS 7.9
CPU: 4 cores
Memory: 8 GiB
Disk: 40 GiB
ClickHouse: local instance, 127.0.0.1:9000
Data scale: 20,000,000 rows
DNS rows: 10,000,000
HTTP rows: 10,000,000
Elapsed: 91s
Throughput: about 210,000-220,000 rows/s
Dead-letter files: 0
Failed row/file logs: 0
```

Stable configuration:

```yaml
clickhouse:
  max_open_conns: 16
  max_idle_conns: 8
  batch_size: 50000
  flush_interval: 1s
```

Pipeline configuration:

```yaml
workers: 2
batch_size: 50000
```

Observed tuning notes:

- `workers: 2` was stable on a 4-core VM and reached about 210k-220k rows/s.
- `batch_size: 50000` and `batch_size: 100000` had similar peak throughput.
- `batch_size: 50000` is preferred because it has lower memory pressure and lower retry cost.
- `workers: 10` overloaded the VM/ClickHouse write path and caused `send batch: context deadline exceeded`.
- Disk free space matters. ClickHouse failed with `Cannot reserve 1.00 MiB, not enough space` when the root filesystem was full.

## 7. Reset

```bash
rm -rf examples/stress-test/watch
rm -rf examples/stress-test/archive
rm -rf examples/stress-test/dead
rm -f examples/stress-test/file_status.db
```

Then regenerate input files and rerun the ETL.
