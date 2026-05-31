# ETL 压测样例

这个目录提供一套可重复生成 DNS 和 HTTP 话单文件的端到端压测样例。

覆盖的能力包括：

- marker 和 atomic rename 两种文件就绪模式
- 文件首行公共字段
- `|` 和 `|++|` 分隔符解析
- IP 归属地匹配
- 字典映射
- ClickHouse 批量写入
- 文件处理状态持久化
- expvar 指标
- 可配置写入超时
- 可选 IP 查询缓存

## 1. 准备 ClickHouse

如果在仓库根目录执行：

```bash
clickhouse-client --multiquery < examples/stress-test/schema.sql
```

如果只上传了 `stress-test` 目录到服务器，则在 `stress-test` 目录内执行：

```bash
clickhouse-client --multiquery < schema.sql
```

脚本会重建两张压测表：

```text
cdr.stress_dns_cdr
cdr.stress_http_cdr
```

## 2. 生成输入文件

默认生成 20 个 DNS 文件和 20 个 HTTP 文件，每个文件 50,000 行数据。

在仓库根目录执行：

```bash
go run ./examples/stress-test/generator
```

默认总行数为 2,000,000：

```text
DNS:  20 * 50,000 = 1,000,000 行
HTTP: 20 * 50,000 = 1,000,000 行
```

可以调整规模：

```bash
go run ./examples/stress-test/generator -dns-files 100 -http-files 100 -rows 100000
```

常用参数：

```text
-dns-files    生成的 DNS 文件数
-http-files   生成的 HTTP 文件数
-rows         每个文件的数据行数
-clean        生成前清理 watch/archive/dead 目录和 file_status.db
-root         stress-test 目录
```

## 3. 启动 ETL

使用当前源码构建后运行：

```bash
go build -o dist/go-etl-stress ./cmd/etl
./dist/go-etl-stress -config examples/stress-test/config.yaml -store examples/stress-test/file_status.db -log info
```

Windows PowerShell：

```powershell
go build -o dist\go-etl-stress.exe .\cmd\etl
.\dist\go-etl-stress.exe -config examples\stress-test\config.yaml -store examples\stress-test\file_status.db -log info
```

Linux 一键压测方式：

```bash
cd examples/stress-test
chmod +x run_stress.sh bin/go-etl-linux-amd64 bin/stress-generator-linux-amd64
DNS_FILES=20 HTTP_FILES=20 ROWS_PER_FILE=50000 ./run_stress.sh
```

如果只上传 `stress-test` 目录到服务器：

```bash
cd stress-test
bash run_stress.sh
```

一键脚本会重建 ClickHouse 表、生成输入文件、启动 ETL、轮询 ClickHouse 行数、输出耗时和近似吞吐，然后停止 ETL。

## 4. 查看指标

指标通过 expvar 暴露：

```bash
curl http://127.0.0.1:9090/debug/vars
```

如果把 `config.yaml` 中的 `metrics.prometheus_enabled` 改为 `true`，也可以查看 Prometheus 文本格式：

```bash
curl http://127.0.0.1:9090/metrics
```

重点关注：

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

近似吞吐可以按下面方式计算：

```text
rows_written_total / (file_process_ms_total / 1000)
```

这个指标使用的是各文件处理耗时之和，适合做不同配置之间的相对比较；一键脚本输出的 `approx_rows_per_second` 使用整轮压测的墙钟时间计算。

## 5. 验证结果

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

默认生成参数下，每张表应有 1,000,000 行。

## 6. 基准结果

本地虚拟机稳定压测基准：

```text
操作系统: CentOS 7.9
CPU: 4 核
内存: 8 GiB
磁盘: 40 GiB
ClickHouse: 本机实例，127.0.0.1:9000
数据规模: 20,000,000 行
DNS 行数: 10,000,000
HTTP 行数: 10,000,000
耗时: 91s
吞吐: 约 210,000-220,000 行/秒
死信文件数: 0
失败行/文件日志: 0
```

稳定配置：

```yaml
clickhouse:
  max_open_conns: 16
  max_idle_conns: 8
  batch_size: 50000
  flush_interval: 1s
  write_timeout: 60s
  async_insert:
    enabled: false
    wait: true
```

IP 库缓存：

```yaml
ip_db:
  cache_size: 4096
```

Pipeline 配置：

```yaml
workers: 2
batch_size: 50000
```

调优观察：

- `workers: 2` 在 4 核虚拟机上稳定，吞吐约 210k-220k 行/秒。
- `batch_size: 50000` 和 `batch_size: 100000` 峰值吞吐接近。
- 推荐 `batch_size: 50000`，因为内存压力更低，失败重试成本也更低。
- `workers: 10` 会压垮虚拟机和 ClickHouse 写入链路，出现 `send batch: context deadline exceeded`。
- `write_timeout` 可按 ClickHouse 写入延迟调大；当前压测基准使用 `60s`。
- 磁盘可用空间很关键。根分区写满时，ClickHouse 会报 `Cannot reserve 1.00 MiB, not enough space`。

## 7. 重置环境

在仓库根目录执行：

```bash
rm -rf examples/stress-test/watch
rm -rf examples/stress-test/archive
rm -rf examples/stress-test/dead
rm -f examples/stress-test/file_status.db
```

在 `stress-test` 目录内执行：

```bash
rm -rf watch archive dead
rm -f file_status.db stress-etl.log
```

然后重新生成输入文件并启动 ETL。
