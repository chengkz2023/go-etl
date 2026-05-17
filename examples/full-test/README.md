# 完整 ETL 测试样例

这个目录提供一套完整的 ClickHouse 集成测试样例，用来验证 go-etl 的主要功能。

## 1. 准备 ClickHouse 表

执行建库建表脚本：

```bash
clickhouse-client --multiquery < examples/full-test/schema.sql
```

## 2. 检查配置路径

样例配置默认从仓库根目录运行，主要路径如下：

```bash
examples/full-test/config.yaml
examples/full-test/pipelines/*.yaml
examples/full-test/watch/*
```

如果把这套样例复制到其他目录，需要保持相同的相对目录结构，或者同步修改配置里的路径。

## 3. 启动 ETL

在仓库根目录执行：

```bash
./go-etl-linux-amd64 -config examples/full-test/config.yaml -store examples/full-test/file_status.db -log info
```

样例目录已经包含可处理的话单文件：

```text
watch/dns/dns_sample_001.cdr
watch/dns/dns_sample_001.cdr.ok
watch/http/http_sample_001.log
```

DNS 管道使用 `.ok` 标记文件模式；HTTP 管道使用最终文件名模式，也就是生产方写完后再原子重命名为正式文件名。

## 4. 验证 ClickHouse 数据

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

## 5. 验证文件生命周期

处理成功后，源文件会进入归档目录：

```bash
ls examples/full-test/archive/dns
ls examples/full-test/archive/http
```

DNS 管道配置了 `cleanup_marker: true`，所以对应的 `.ok` 标记文件应被删除。

## 6. 验证指标

指标通过标准库 expvar 暴露：

```bash
curl http://127.0.0.1:9090/debug/vars
```

可以关注这些计数器：

```text
go_etl_dns_cdr_files_done_total
go_etl_dns_cdr_rows_written_total
go_etl_http_cdr_files_done_total
go_etl_http_cdr_rows_written_total
```

## 7. 验证重试和死信目录

把坏样例复制到 HTTP 监听目录：

```bash
cp examples/full-test/bad-samples/http_bad_invalid_status.log examples/full-test/watch/http/http_bad_invalid_status.log
```

这个文件的 `status_code=not_a_number`，类型转换会失败。HTTP 管道配置如下：

```yaml
retry_failed: true
max_retries: 2
retry_interval: 5s
dead_letter_dir: ./examples/full-test/dead/http
```

达到最大重试次数后，文件应进入死信目录：

```text
examples/full-test/dead/http/
```

## 8. 重置样例环境

```bash
rm -f examples/full-test/file_status.db
rm -f examples/full-test/watch/dns/*
rm -f examples/full-test/watch/http/*
rm -f examples/full-test/archive/dns/*
rm -f examples/full-test/archive/http/*
rm -f examples/full-test/dead/dns/*
rm -f examples/full-test/dead/http/*
```

然后从版本控制中恢复样例话单文件即可再次测试。
