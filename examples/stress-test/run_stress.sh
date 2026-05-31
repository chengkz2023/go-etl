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
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-127.0.0.1}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
CLICKHOUSE_CONNECT_TIMEOUT="${CLICKHOUSE_CONNECT_TIMEOUT:-5}"
CLICKHOUSE_RECEIVE_TIMEOUT="${CLICKHOUSE_RECEIVE_TIMEOUT:-30}"
CLICKHOUSE_QUERY_TIMEOUT="${CLICKHOUSE_QUERY_TIMEOUT:-10}"
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
    echo "停止 ETL pid=$ETL_PID"
    kill "$ETL_PID" >/dev/null 2>&1 || true
    wait "$ETL_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "缺少命令: $cmd" >&2
    exit 1
  fi
}

query_count() {
  local table="$1"
  query_scalar "SELECT count() FROM $table"
}

query_scalar() {
  local sql="$1"
  RUN_WITH_TIMEOUT="$CLICKHOUSE_QUERY_TIMEOUT" clickhouse_query --query "$sql" 2>/dev/null
}

run_sql_file() {
  local file="$1"
  local sql
  sql="$(cat "$file")"
  clickhouse_query --multiquery --query "$sql"
}

clickhouse_query() {
  local args=(
    --host "$CLICKHOUSE_HOST"
    --port "$CLICKHOUSE_PORT"
    --user "$CLICKHOUSE_USER"
    --connect_timeout "$CLICKHOUSE_CONNECT_TIMEOUT"
    --receive_timeout "$CLICKHOUSE_RECEIVE_TIMEOUT"
  )
  if [[ -n "$CLICKHOUSE_PASSWORD" ]]; then
    args+=(--password "$CLICKHOUSE_PASSWORD")
  fi
  if [[ -n "${RUN_WITH_TIMEOUT:-}" ]] && command -v timeout >/dev/null 2>&1; then
    timeout "$RUN_WITH_TIMEOUT" "$CLICKHOUSE_CLIENT" "${args[@]}" "$@"
    return
  fi
  "$CLICKHOUSE_CLIENT" "${args[@]}" "$@"
}

print_usage() {
  cat <<EOF
用法:
  DNS_FILES=20 HTTP_FILES=20 ROWS_PER_FILE=50000 $0

环境变量:
  DNS_FILES            DNS 输入文件数，默认 20
  HTTP_FILES           HTTP 输入文件数，默认 20
  ROWS_PER_FILE        每个文件的数据行数，默认 50000
  TIMEOUT_SECONDS      最大等待秒数，默认 1800
  POLL_SECONDS         ClickHouse 轮询间隔秒数，默认 5
  NO_PROGRESS_SECONDS  行数无增长超时秒数，默认 120
  ETL_BIN              ETL 二进制路径，默认 ./bin/go-etl-linux-amd64
  GENERATOR_BIN        数据生成器二进制路径，默认 ./bin/stress-generator-linux-amd64
  CLICKHOUSE_CLIENT    clickhouse 客户端命令，默认 clickhouse-client
  CLICKHOUSE_HOST      ClickHouse 地址，默认 127.0.0.1
  CLICKHOUSE_PORT      ClickHouse TCP 端口，默认 9000
  CLICKHOUSE_USER      ClickHouse 用户名，默认 default
  CLICKHOUSE_PASSWORD  ClickHouse 密码，默认空
  CLICKHOUSE_QUERY_TIMEOUT 单次 ClickHouse 查询超时秒数，默认 10
  CONFIG_PATH          ETL 配置路径，默认 ./config.yaml
  STORE_PATH           ETL 文件状态库路径，默认 ./file_status.db
  LOG_PATH             ETL 日志路径，默认 ./stress-etl.log
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
  echo "未找到 ETL 二进制: $ETL_BIN" >&2
  echo "请上传 bin/go-etl-linux-amd64，或设置 ETL_BIN=/path/to/binary" >&2
  exit 1
fi

if [[ -f "$GENERATOR_BIN" && ! -x "$GENERATOR_BIN" ]]; then
  chmod +x "$GENERATOR_BIN"
fi
if [[ ! -x "$GENERATOR_BIN" ]]; then
  echo "未找到数据生成器二进制: $GENERATOR_BIN" >&2
  echo "请上传 bin/stress-generator-linux-amd64，或设置 GENERATOR_BIN=/path/to/binary" >&2
  exit 1
fi

echo "压测配置:"
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

echo "检查 ClickHouse 连接"
clickhouse_query --query "SELECT 1" >/dev/null

echo "重建压测表"
run_sql_file ./schema.sql

echo "生成输入文件"
"$GENERATOR_BIN" \
  -root . \
  -dns-files "$DNS_FILES" \
  -http-files "$HTTP_FILES" \
  -rows "$ROWS_PER_FILE" \
  -clean true

rm -f "$LOG_PATH"

echo "启动 ETL"
"$ETL_BIN" -config "$CONFIG_PATH" -store "$STORE_PATH" -log info >"$LOG_PATH" 2>&1 &
ETL_PID="$!"
START_TS="$(date +%s)"
LAST_PROGRESS_TS="$START_TS"

echo "etl pid=$ETL_PID"
echo "等待 ClickHouse 行数达到预期"

while true; do
  if ! kill -0 "$ETL_PID" >/dev/null 2>&1; then
    echo "ETL 在压测完成前退出" >&2
    echo "最近 80 行日志:" >&2
    tail -n 80 "$LOG_PATH" >&2 || true
    exit 1
  fi

  DNS_COUNT="$(query_count cdr.stress_dns_cdr || true)"
  HTTP_COUNT="$(query_count cdr.stress_http_cdr || true)"
  DNS_COUNT="${DNS_COUNT//$'\r'/}"
  HTTP_COUNT="${HTTP_COUNT//$'\r'/}"
  if [[ ! "$DNS_COUNT" =~ ^[0-9]+$ ]]; then
    echo "DNS 行数查询失败或超时，按 0 处理" >&2
    DNS_COUNT=0
  fi
  if [[ ! "$HTTP_COUNT" =~ ^[0-9]+$ ]]; then
    echo "HTTP 行数查询失败或超时，按 0 处理" >&2
    HTTP_COUNT=0
  fi
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
    echo "等待超过 ${TIMEOUT_SECONDS}s" >&2
    echo "最近 80 行日志:" >&2
    tail -n 80 "$LOG_PATH" >&2 || true
    exit 1
  fi

  NO_PROGRESS_ELAPSED=$((NOW_TS - LAST_PROGRESS_TS))
  if [[ "$NO_PROGRESS_ELAPSED" -ge "$NO_PROGRESS_SECONDS" ]]; then
    echo "行数连续 ${NO_PROGRESS_ELAPSED}s 无增长" >&2
    echo "剩余文件:" >&2
    find watch archive dead -type f 2>/dev/null | sort >&2 || true
    echo "最近 120 行日志:" >&2
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

echo "压测完成"
echo "  elapsed_seconds=$ELAPSED"
echo "  total_rows=$EXPECTED_TOTAL"
echo "  approx_rows_per_second=$ROWS_PER_SECOND"
echo "  dns_event_time=$DNS_MIN_MAX"
echo "  http_event_time=$HTTP_MIN_MAX"
echo "  etl_log=$LOG_PATH"

cleanup
ETL_PID=""
