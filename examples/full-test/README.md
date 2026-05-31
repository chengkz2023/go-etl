# ETL 完整测试样例

这个目录提供一套小规模端到端功能测试，用来验证 `go-etl` 的主要能力：文件发现、首行公共字段、分隔符解析、IP 匹配、字典映射、ClickHouse 批量写入、归档、重试、死信和指标。

## 1. 一键运行

如果只上传完整测试目录和二进制到服务器，推荐目录结构如下：

```text
/home/go-etl/
  go-etl-linux-amd64
  full-test/
    config.yaml
    schema.sql
    run_full_test.sh
    pipelines/
    ipdb/
    dict/
    bad-samples/
```

在 Linux 测试环境执行：

```bash
cd /home/go-etl/full-test
bash run_full_test.sh
```

脚本默认会优先使用上一级目录的 `../go-etl-linux-amd64`。

如果在源码仓库中运行，也可以先准备 ETL 二进制：

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/go-etl-linux-amd64 ./cmd/etl
cd examples/full-test
bash run_full_test.sh
```

如果 ETL 二进制不在默认位置，可以指定：

```bash
ETL_BIN=/path/to/go-etl-linux-amd64 bash run_full_test.sh
```

脚本会自动：

- 检查 ClickHouse 连接。
- 重建 `cdr.dns_cdr` 和 `cdr.http_cdr`。
- 清理并重建 `watch/archive/dead` 目录。
- 写入正常样例文件。
- 启动 ETL。
- 等待两张表都写入 3 行。
- 验证成功文件归档和 DNS marker 清理。
- 验证指标端点。
- 默认投递一个坏 HTTP 样例，确认它重试后进入死信目录。

常用环境变量：

```text
ETL_BIN             ETL 二进制路径，默认优先使用 ../go-etl-linux-amd64
CLICKHOUSE_CLIENT   clickhouse 客户端命令，默认 clickhouse-client
TIMEOUT_SECONDS     最大等待秒数，默认 120
POLL_SECONDS        ClickHouse 轮询间隔秒数，默认 1
RUN_BAD_SAMPLE      是否验证坏样例进入死信目录，默认 true
CONFIG_PATH         ETL 配置路径，默认 ./config.yaml
STORE_PATH          ETL 文件状态库路径，默认 ./file_status.db
LOG_PATH            ETL 日志路径，默认 ./full-test-etl.log
```

只验证正常样例：

```bash
RUN_BAD_SAMPLE=false bash run_full_test.sh
```

## 2. 手动运行

准备 ClickHouse 表：

```bash
cd /home/go-etl/full-test
clickhouse-client --multiquery < schema.sql
```

从完整测试目录启动 ETL：

```bash
../go-etl-linux-amd64 \
  -config config.yaml \
  -store file_status.db \
  -log info
```

样例输入文件：

```text
watch/dns/dns_sample_001.cdr
watch/dns/dns_sample_001.cdr.ok
watch/http/http_sample_001.log
```

DNS 管道使用 `.ok` marker 就绪模式；HTTP 管道使用 atomic rename 就绪模式，忽略 `.tmp` 和 `.writing` 后缀。

## 3. 验证数据

```sql
SELECT
    event_time,
    src_ip,
    dst_ip,
    domain,
    dns_type,
    response_code,
    probe_id,
    region,
    src_geo_city,
    dst_geo_isp
FROM cdr.dns_cdr
ORDER BY event_time;

SELECT
    event_time,
    client_ip,
    server_ip,
    method,
    url,
    status_code,
    bytes_sent,
    probe_id,
    region,
    client_geo_city,
    server_geo_isp
FROM cdr.http_cdr
ORDER BY event_time;
```

预期行数：

```sql
SELECT count() FROM cdr.dns_cdr;  -- 3
SELECT count() FROM cdr.http_cdr; -- 3
```

## 4. 验证文件生命周期

成功处理后，源文件会进入归档目录：

```bash
ls archive/dns
ls archive/http
```

DNS 管道配置了 `cleanup_marker: true`，成功归档后原始 `.ok` marker 会被清理，不会留在 `watch/dns` 或 `archive/dns`。

坏样例路径：

```text
bad-samples/http_bad_invalid_status.log
```

这个文件的 `status_code=not_a_number`，类型转换会失败。HTTP 管道配置了：

```yaml
retry_failed: true
max_retries: 2
retry_interval: 5s
dead_letter_dir: ./dead/http
```

达到最大重试次数后，文件会进入：

```text
dead/http/
```

## 5. 验证指标

expvar 指标：

```bash
curl http://127.0.0.1:9090/debug/vars
```

如果设置 `metrics.prometheus_enabled: true`，也可以查看：

```bash
curl http://127.0.0.1:9090/metrics
```

重点关注：

```text
go_etl_dns_cdr_files_done_total
go_etl_dns_cdr_rows_written_total
go_etl_http_cdr_files_done_total
go_etl_http_cdr_rows_written_total
go_etl_http_cdr_files_dead_total
```

## 6. 配置覆盖点

完整测试样例覆盖这些配置能力：

- `clickhouse.write_timeout`
- `clickhouse.async_insert`
- `metrics.prometheus_enabled`
- `ip_db.cache_size`
- `header_fields`
- `fields.generated`
- marker 和 atomic rename 两种 ready strategy
- retry 和 dead-letter

默认没有开启 dedup 元字段，因为样例表没有声明 `_etl_record_id` 等列。需要验证去重元字段时，先在目标表和 pipeline `fields` 中增加对应 generated 字段，再设置：

```yaml
dedup:
  enabled: true
```

## 7. 重置环境

一键脚本每次都会自动重置。手动重置可以执行：

```bash
rm -f file_status.db full-test-etl.log
rm -rf watch archive dead
mkdir -p watch/dns watch/http archive/dns archive/http dead/dns dead/http
```
