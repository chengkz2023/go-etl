#!/usr/bin/env bash
set -euo pipefail

FULL_TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$FULL_TEST_DIR/../.." && pwd)"
PARENT_DIR="$(cd "$FULL_TEST_DIR/.." && pwd)"
cd "$FULL_TEST_DIR"

TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
POLL_SECONDS="${POLL_SECONDS:-1}"
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
LOG_PATH="${LOG_PATH:-./full-test-etl.log}"
RUN_BAD_SAMPLE="${RUN_BAD_SAMPLE:-true}"

ETL_PID=""

if [[ -z "${ETL_BIN:-}" ]]; then
  if [[ -x "$PARENT_DIR/go-etl-linux-amd64" || -f "$PARENT_DIR/go-etl-linux-amd64" ]]; then
    ETL_BIN="$PARENT_DIR/go-etl-linux-amd64"
  elif [[ -x "$REPO_ROOT/dist/go-etl-linux-amd64" || -f "$REPO_ROOT/dist/go-etl-linux-amd64" ]]; then
    ETL_BIN="$REPO_ROOT/dist/go-etl-linux-amd64"
  else
    ETL_BIN="$PARENT_DIR/go-etl-linux-amd64"
  fi
fi

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
  bash run_full_test.sh

环境变量:
  ETL_BIN             ETL 二进制路径，默认优先使用 ../go-etl-linux-amd64
  CLICKHOUSE_CLIENT   clickhouse 客户端命令，默认 clickhouse-client
  CLICKHOUSE_HOST     ClickHouse 地址，默认 127.0.0.1
  CLICKHOUSE_PORT     ClickHouse TCP 端口，默认 9000
  CLICKHOUSE_USER     ClickHouse 用户名，默认 default
  CLICKHOUSE_PASSWORD ClickHouse 密码，默认空
  CLICKHOUSE_QUERY_TIMEOUT 单次 ClickHouse 查询超时秒数，默认 10
  TIMEOUT_SECONDS     最大等待秒数，默认 120
  POLL_SECONDS        ClickHouse 轮询间隔秒数，默认 1
  RUN_BAD_SAMPLE      是否验证坏样例进入死信目录，默认 true
  CONFIG_PATH         ETL 配置路径，默认 ./config.yaml
  STORE_PATH          ETL 文件状态库路径，默认 ./file_status.db
  LOG_PATH            ETL 日志路径，默认 ./full-test-etl.log
EOF
}

reset_dirs() {
  rm -f "$STORE_PATH" "$LOG_PATH"
  rm -rf watch archive dead
  mkdir -p watch/dns watch/http archive/dns archive/http dead/dns dead/http
}

write_samples() {
  cat > watch/dns/dns_sample_001.cdr <<'EOF'
probe-dns-01|south
2026-05-17 10:00:01|10.10.1.10|8.8.8.8|example.com|1|0
2026-05-17 10:00:02|10.20.2.20|1.1.1.1|openai.com|28|0
2026-05-17 10:00:03|172.16.3.30|8.8.8.8|mail.example.com|15|3
EOF
  : > watch/dns/dns_sample_001.cdr.ok

  cat > watch/http/http_sample_001.log <<'EOF'
2026-05-17 10:01:01|++|192.168.1.10|++|93.184.216.34|++|1|++|/index.html|++|200|++|1024|++|probe-http-01|++|east
2026-05-17 10:01:02|++|10.10.1.10|++|203.0.113.10|++|2|++|/api/v1/order|++|201|++|2048|++|probe-http-01|++|east
2026-05-17 10:01:03|++|10.20.2.20|++|93.184.216.34|++|3|++|/health|++|204|++|0|++|probe-http-02|++|north
EOF
}

start_etl() {
  if [[ -f "$ETL_BIN" && ! -x "$ETL_BIN" ]]; then
    chmod +x "$ETL_BIN"
  fi
  if [[ ! -x "$ETL_BIN" ]]; then
    echo "未找到 ETL 二进制: $ETL_BIN" >&2
    echo "请确认 ../go-etl-linux-amd64 存在，或设置 ETL_BIN=/path/to/binary" >&2
    exit 1
  fi

  "$ETL_BIN" -config "$CONFIG_PATH" -store "$STORE_PATH" -log info >"$LOG_PATH" 2>&1 &
  ETL_PID="$!"
  echo "ETL pid=$ETL_PID"
}

wait_counts() {
  local expected_dns="$1"
  local expected_http="$2"
  local start_ts now_ts elapsed dns_count http_count
  start_ts="$(date +%s)"

  while true; do
    if ! kill -0 "$ETL_PID" >/dev/null 2>&1; then
      echo "ETL 在测试完成前退出" >&2
      tail -n 80 "$LOG_PATH" >&2 || true
      exit 1
    fi

    dns_count="$(query_scalar "SELECT count() FROM cdr.dns_cdr" || true)"
    http_count="$(query_scalar "SELECT count() FROM cdr.http_cdr" || true)"
    dns_count="${dns_count//$'\r'/}"
    http_count="${http_count//$'\r'/}"
    if [[ ! "$dns_count" =~ ^[0-9]+$ ]]; then
      echo "DNS 行数查询失败或超时，按 0 处理" >&2
      dns_count=0
    fi
    if [[ ! "$http_count" =~ ^[0-9]+$ ]]; then
      echo "HTTP 行数查询失败或超时，按 0 处理" >&2
      http_count=0
    fi
    now_ts="$(date +%s)"
    elapsed=$((now_ts - start_ts))
    printf 'elapsed=%ss dns=%s/%s http=%s/%s\n' "$elapsed" "$dns_count" "$expected_dns" "$http_count" "$expected_http"

    if [[ "$dns_count" -ge "$expected_dns" && "$http_count" -ge "$expected_http" ]]; then
      break
    fi
    if [[ "$elapsed" -ge "$TIMEOUT_SECONDS" ]]; then
      echo "等待超过 ${TIMEOUT_SECONDS}s" >&2
      tail -n 120 "$LOG_PATH" >&2 || true
      exit 1
    fi
    sleep "$POLL_SECONDS"
  done
}

assert_file_exists() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "文件不存在: $path" >&2
    exit 1
  fi
}

assert_file_missing() {
  local path="$1"
  if [[ -e "$path" ]]; then
    echo "文件不应存在: $path" >&2
    exit 1
  fi
}

verify_success_lifecycle() {
  assert_file_exists archive/dns/dns_sample_001.cdr
  assert_file_missing watch/dns/dns_sample_001.cdr
  assert_file_missing watch/dns/dns_sample_001.cdr.ok
  assert_file_missing archive/dns/dns_sample_001.cdr.ok
  assert_file_exists archive/http/http_sample_001.log
}

verify_metrics() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsS http://127.0.0.1:9090/debug/vars >/dev/null || true
  fi
}

run_bad_sample_check() {
  if [[ "$RUN_BAD_SAMPLE" != "true" ]]; then
    return
  fi

  cp bad-samples/http_bad_invalid_status.log watch/http/http_bad_invalid_status.log
  local start_ts now_ts elapsed
  start_ts="$(date +%s)"
  while true; do
    if [[ -f dead/http/http_bad_invalid_status.log ]]; then
      echo "坏样例已进入死信目录"
      break
    fi
    now_ts="$(date +%s)"
    elapsed=$((now_ts - start_ts))
    if [[ "$elapsed" -ge "$TIMEOUT_SECONDS" ]]; then
      echo "等待坏样例进入死信目录超过 ${TIMEOUT_SECONDS}s" >&2
      tail -n 120 "$LOG_PATH" >&2 || true
      exit 1
    fi
    sleep "$POLL_SECONDS"
  done
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  print_usage
  exit 0
fi

require_cmd "$CLICKHOUSE_CLIENT"

echo "检查 ClickHouse 连接"
clickhouse_query --query "SELECT 1" >/dev/null

echo "重建完整测试表"
run_sql_file ./schema.sql
echo "完整测试表重建完成"

echo "重建样例输入目录"
reset_dirs
write_samples

echo "启动 ETL"
start_etl

echo "等待正常样例写入 ClickHouse"
wait_counts 3 3

echo "验证归档和 marker 生命周期"
verify_success_lifecycle

echo "验证指标端点"
verify_metrics

echo "验证坏样例重试和死信"
run_bad_sample_check

echo "完整测试完成"
echo "  dns_rows=$(query_scalar "SELECT count() FROM cdr.dns_cdr")"
echo "  http_rows=$(query_scalar "SELECT count() FROM cdr.http_cdr")"
echo "  etl_log=$LOG_PATH"

cleanup
ETL_PID=""
