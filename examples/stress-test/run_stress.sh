#!/usr/bin/env bash
set -euo pipefail

STRESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$STRESS_DIR"

DNS_FILES="${DNS_FILES:-20}"
HTTP_FILES="${HTTP_FILES:-20}"
ROWS_PER_FILE="${ROWS_PER_FILE:-50000}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-1800}"
POLL_SECONDS="${POLL_SECONDS:-5}"
NO_PROGRESS_SECONDS="${NO_PROGRESS_SECONDS:-120}"
ETL_BIN="${ETL_BIN:-./bin/go-etl-linux-amd64}"
GENERATOR_BIN="${GENERATOR_BIN:-./bin/stress-generator-linux-amd64}"
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT:-clickhouse-client}"
CONFIG_PATH="${CONFIG_PATH:-./config.yaml}"
STORE_PATH="${STORE_PATH:-./file_status.db}"
LOG_PATH="${LOG_PATH:-./stress-etl.log}"

EXPECTED_DNS=$((DNS_FILES * ROWS_PER_FILE))
EXPECTED_HTTP=$((HTTP_FILES * ROWS_PER_FILE))
EXPECTED_TOTAL=$((EXPECTED_DNS + EXPECTED_HTTP))

ETL_PID=""
LAST_TOTAL_COUNT=0
LAST_PROGRESS_TS=0

cleanup() {
  if [[ -n "$ETL_PID" ]] && kill -0 "$ETL_PID" >/dev/null 2>&1; then
    echo "stopping etl pid=$ETL_PID"
    kill "$ETL_PID" >/dev/null 2>&1 || true
    wait "$ETL_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "missing command: $cmd" >&2
    exit 1
  fi
}

query_count() {
  local table="$1"
  "$CLICKHOUSE_CLIENT" --query "SELECT count() FROM $table" 2>/dev/null
}

query_scalar() {
  local sql="$1"
  "$CLICKHOUSE_CLIENT" --query "$sql" 2>/dev/null
}

print_usage() {
  cat <<EOF
Usage:
  DNS_FILES=20 HTTP_FILES=20 ROWS_PER_FILE=50000 $0

Environment:
  DNS_FILES            DNS input file count, default 20
  HTTP_FILES           HTTP input file count, default 20
  ROWS_PER_FILE        rows per generated file, default 50000
  TIMEOUT_SECONDS      max wait time, default 1800
  POLL_SECONDS         ClickHouse polling interval, default 5
  NO_PROGRESS_SECONDS  fail if row count does not grow for this long, default 120
  ETL_BIN              ETL binary path, default ./bin/go-etl-linux-amd64
  GENERATOR_BIN        generator binary path, default ./bin/stress-generator-linux-amd64
  CLICKHOUSE_CLIENT    clickhouse client command, default clickhouse-client
  CONFIG_PATH          ETL config path, default ./config.yaml
  STORE_PATH           ETL file status DB, default ./file_status.db
  LOG_PATH             ETL log path, default ./stress-etl.log
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  print_usage
  exit 0
fi

require_cmd "$CLICKHOUSE_CLIENT"

if [[ -f "$ETL_BIN" && ! -x "$ETL_BIN" ]]; then
  chmod +x "$ETL_BIN"
fi
if [[ ! -x "$ETL_BIN" ]]; then
  echo "ETL binary not found: $ETL_BIN" >&2
  echo "Upload bin/go-etl-linux-amd64 or set ETL_BIN=/path/to/binary" >&2
  exit 1
fi

if [[ -f "$GENERATOR_BIN" && ! -x "$GENERATOR_BIN" ]]; then
  chmod +x "$GENERATOR_BIN"
fi
if [[ ! -x "$GENERATOR_BIN" ]]; then
  echo "generator binary not found: $GENERATOR_BIN" >&2
  echo "Upload bin/stress-generator-linux-amd64 or set GENERATOR_BIN=/path/to/binary" >&2
  exit 1
fi

echo "stress test settings:"
echo "  dns_files=$DNS_FILES"
echo "  http_files=$HTTP_FILES"
echo "  rows_per_file=$ROWS_PER_FILE"
echo "  expected_dns_rows=$EXPECTED_DNS"
echo "  expected_http_rows=$EXPECTED_HTTP"
echo "  expected_total_rows=$EXPECTED_TOTAL"
echo "  etl_bin=$ETL_BIN"
echo "  generator_bin=$GENERATOR_BIN"
echo "  log_path=$LOG_PATH"
echo "  no_progress_seconds=$NO_PROGRESS_SECONDS"

echo "checking ClickHouse connectivity"
"$CLICKHOUSE_CLIENT" --query "SELECT 1" >/dev/null

echo "recreating stress tables"
"$CLICKHOUSE_CLIENT" --multiquery < ./schema.sql

echo "generating input files"
"$GENERATOR_BIN" \
  -root . \
  -dns-files "$DNS_FILES" \
  -http-files "$HTTP_FILES" \
  -rows "$ROWS_PER_FILE" \
  -clean true

rm -f "$LOG_PATH"

echo "starting etl"
"$ETL_BIN" -config "$CONFIG_PATH" -store "$STORE_PATH" -log info >"$LOG_PATH" 2>&1 &
ETL_PID="$!"
START_TS="$(date +%s)"
LAST_PROGRESS_TS="$START_TS"

echo "etl pid=$ETL_PID"
echo "waiting for ClickHouse row counts to reach expected values"

while true; do
  if ! kill -0 "$ETL_PID" >/dev/null 2>&1; then
    echo "etl exited before completing stress test" >&2
    echo "last 80 log lines:" >&2
    tail -n 80 "$LOG_PATH" >&2 || true
    exit 1
  fi

  DNS_COUNT="$(query_count cdr.stress_dns_cdr || echo 0)"
  HTTP_COUNT="$(query_count cdr.stress_http_cdr || echo 0)"
  NOW_TS="$(date +%s)"
  ELAPSED=$((NOW_TS - START_TS))
  TOTAL_COUNT=$((DNS_COUNT + HTTP_COUNT))
  if [[ "$TOTAL_COUNT" -gt "$LAST_TOTAL_COUNT" ]]; then
    LAST_TOTAL_COUNT="$TOTAL_COUNT"
    LAST_PROGRESS_TS="$NOW_TS"
  fi

  printf 'elapsed=%ss dns=%s/%s http=%s/%s total=%s/%s\n' \
    "$ELAPSED" "$DNS_COUNT" "$EXPECTED_DNS" "$HTTP_COUNT" "$EXPECTED_HTTP" "$TOTAL_COUNT" "$EXPECTED_TOTAL"

  if [[ "$DNS_COUNT" -ge "$EXPECTED_DNS" && "$HTTP_COUNT" -ge "$EXPECTED_HTTP" ]]; then
    break
  fi

  if [[ "$ELAPSED" -ge "$TIMEOUT_SECONDS" ]]; then
    echo "timeout after ${TIMEOUT_SECONDS}s" >&2
    echo "last 80 log lines:" >&2
    tail -n 80 "$LOG_PATH" >&2 || true
    exit 1
  fi

  NO_PROGRESS_ELAPSED=$((NOW_TS - LAST_PROGRESS_TS))
  if [[ "$NO_PROGRESS_ELAPSED" -ge "$NO_PROGRESS_SECONDS" ]]; then
    echo "no row-count progress for ${NO_PROGRESS_ELAPSED}s" >&2
    echo "remaining files:" >&2
    find watch archive dead -type f 2>/dev/null | sort >&2 || true
    echo "last 120 log lines:" >&2
    tail -n 120 "$LOG_PATH" >&2 || true
    exit 1
  fi

  sleep "$POLL_SECONDS"
done

END_TS="$(date +%s)"
ELAPSED=$((END_TS - START_TS))
if [[ "$ELAPSED" -le 0 ]]; then
  ELAPSED=1
fi

ROWS_PER_SECOND=$((EXPECTED_TOTAL / ELAPSED))

DNS_MIN_MAX="$(query_scalar "SELECT concat(toString(min(event_time)), ' -> ', toString(max(event_time))) FROM cdr.stress_dns_cdr")"
HTTP_MIN_MAX="$(query_scalar "SELECT concat(toString(min(event_time)), ' -> ', toString(max(event_time))) FROM cdr.stress_http_cdr")"

echo "stress test completed"
echo "  elapsed_seconds=$ELAPSED"
echo "  total_rows=$EXPECTED_TOTAL"
echo "  approx_rows_per_second=$ROWS_PER_SECOND"
echo "  dns_event_time=$DNS_MIN_MAX"
echo "  http_event_time=$HTTP_MIN_MAX"
echo "  etl_log=$LOG_PATH"

cleanup
ETL_PID=""
