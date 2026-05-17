# go-etl 项目开发文档

## 1. 项目定位

`go-etl` 是一个面向文件型话单/日志数据的轻量 ETL 服务。它从一个或多个目录发现文件，按管道配置解析文本行，执行字段转换、IP 归属地匹配、字典映射，然后批量写入 ClickHouse。

项目目标不是替代 Flink、Spark、DataX 这类平台，而是提供一个部署简单、行为可控、适合运营商/大数据中台文件落地场景的单机 ETL 进程。

典型场景：

- 探针、网关、采集机按目录投递 DNS/HTTP/其他话单文件。
- 生产方使用 `.ok` 标记文件或临时文件重命名表达“文件已写完”。
- ETL 需要保证重启不丢文件、不重复处理已完成文件。
- 话单落入 ClickHouse，供后续检索、统计、宽表分析使用。

## 2. 总体架构

核心链路如下：

```text
文件目录
  -> Watcher(fsnotify + Poller)
  -> Ready Detector(.ok / rename / stable_size)
  -> bbolt 文件状态队列
  -> Pipeline Worker
  -> Reader 流式批读取
  -> Transform Chain
  -> Type Converter
  -> ClickHouse Batch Writer
  -> Done / Archive / Retry / Dead Letter
```

模块职责：

```text
cmd/etl       程序入口、日志、配置加载、启动 pipeline、信号退出
config        YAML 配置结构、加载、默认值、校验
watcher       文件发现，组合 fsnotify 和定时扫描
store         bbolt 文件状态库，提供原子入队、认领、失败重试状态
reader        分隔文本解析，支持流式批读取、header meta 合并
transform     转换链，目前支持 IP 匹配、字典映射
iputil        CSV IP 库加载和 IPv4 范围二分查找
writer        ClickHouse 连接、批量写入、字段类型转换
metrics       expvar 指标
pipeline      单个目录到单个 ClickHouse 表的完整 ETL 编排
examples      完整集成测试样例
scripts       Windows 下交叉编译脚本
```

## 3. 程序启动流程

入口文件：[cmd/etl/main.go](../cmd/etl/main.go)

启动参数：

```bash
./go-etl-linux-amd64 -config config.yaml -store data/filestatus.db -log info
```

参数说明：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `-config` | `config.yaml` | 主配置文件路径 |
| `-store` | `data/filestatus.db` | bbolt 文件状态库路径 |
| `-log` | `info` | 日志级别：`debug/info/warn/error` |

启动步骤：

1. 初始化 zap 日志。
2. 加载主配置和 `pipeline_dir` 下的管道配置。
3. 打开 bbolt 文件状态库。
4. 如果配置了 IP 库，加载 CSV IP 范围。
5. 如果启用 metrics，启动 `/debug/vars`。
6. 为每条 pipeline 创建并启动 goroutine。
7. 等待 `SIGINT/SIGTERM`。
8. 调用每条 pipeline 的 `Shutdown`，flush writer 并退出。

## 4. 配置模型

配置结构定义在：[config/types.go](../config/types.go)

主配置示例：

```yaml
clickhouse:
  hosts:
    - "127.0.0.1:9000"
  database: cdr
  username: default
  password: ""
  max_open_conns: 5
  max_idle_conns: 2
  debug: false
  batch_size: 10000
  flush_interval: 5s

metrics:
  enabled: true
  addr: ":9090"

ip_db:
  type: csv
  path: ./data/ipdb.csv
  columns: [country, province, city, isp]
  reload_interval: 3600s

pipeline_dir: pipelines
```

注意事项：

- `pipeline_dir` 相对主配置文件所在目录解析。
- 当前 `ip_db.path`、`dict_file`、`watch_dir` 按进程工作目录解析。
- `clickhouse.batch_size` 是全局默认值，pipeline 可用 `batch_size` 覆盖。

## 5. Pipeline 配置

每条 pipeline 表示“一个监听目录 -> 一张 ClickHouse 表”。

关键字段：

| 字段 | 说明 |
|---|---|
| `name` | 管道名，也是状态库 key 前缀和指标名前缀 |
| `watch_dir` | 文件监听目录 |
| `file_pattern` | 数据文件匹配规则，如 `*.cdr`、`*.log` |
| `ready_strategy` | 文件就绪策略：`marker`、`atomic_rename`、`stable_size` |
| `marker_suffix` | marker 模式下的标记后缀，通常是 `.ok` |
| `temp_suffixes` | atomic rename 模式下忽略的临时后缀 |
| `delimiter` | 字段分隔符，支持单字符和多字符 |
| `has_header_meta` | 首个非空行是否作为公共元数据 |
| `skip_header_lines` | 额外跳过的非空行数 |
| `field_names` | 输入文件字段名，按文件列顺序配置 |
| `fields` | ClickHouse 输出字段 schema，决定写入列和类型转换 |
| `transformers` | 转换链配置 |
| `clickhouse_table` | 目标表名，建议写完整库表名 |
| `workers` | 文件级 worker 数 |
| `batch_size` | 单次读取/转换/写入批大小 |
| `retry_failed` | 是否对失败文件重试 |
| `max_retries` | 最大失败次数 |
| `retry_interval` | 失败后多久允许重试 |
| `dead_letter_dir` | 超过重试次数后的死信目录 |
| `archive_dir` | 成功处理后的归档目录 |
| `cleanup_marker` | 成功后是否清理 `.ok` 文件 |

示例见：[examples/full-test/pipelines](../examples/full-test/pipelines)

## 6. 文件就绪策略

实现位置：[watcher/ready.go](../watcher/ready.go)

### 6.1 marker 模式

配置：

```yaml
ready_strategy: marker
marker_suffix: ".ok"
file_pattern: "*.cdr"
```

生产方先写数据文件：

```text
dns_sample_001.cdr
```

写完后生成空标记文件：

```text
dns_sample_001.cdr.ok
```

ETL 只处理真实数据文件 `dns_sample_001.cdr`，状态库也只记录真实数据文件路径。

适用场景：

- 文件来自上游批处理系统。
- 上游可以明确生成完成标记。
- 对“文件是否写完”要求强确定性。

### 6.2 atomic_rename 模式

配置：

```yaml
ready_strategy: atomic_rename
temp_suffixes:
  - ".tmp"
  - ".writing"
```

生产方先写：

```text
http_sample_001.log.tmp
```

写完后重命名为：

```text
http_sample_001.log
```

ETL 忽略临时后缀，只处理最终文件名。

### 6.3 stable_size 模式

配置：

```yaml
ready_strategy: stable_size
stable_delay: 10s
```

当文件 `modTime` 超过 `stable_delay` 后才认为可处理。这是兜底方案，优先级低于 marker 和 atomic rename，因为它本质上依赖时间猜测。

## 7. 文件状态与可靠队列

状态库实现：[store/filestore.go](../store/filestore.go)

状态定义：[model/file_status.go](../model/file_status.go)

状态流转：

```text
unknown
  -> pending
  -> processing
  -> done

processing
  -> failed
  -> pending       重试恢复
  -> dead          超过最大重试次数
```

关键方法：

| 方法 | 说明 |
|---|---|
| `EnqueueReadyFile` | 原子入队 ready 文件，避免 fsnotify 和 poller 重复入队 |
| `ClaimPending` | worker 原子认领 pending 文件 |
| `ResetProcessingToPending` | 启动时恢复上次中断的 processing 文件 |
| `SetDone` | 成功完成 |
| `SetFailedForRetry` | 标记失败并记录下次重试时间 |
| `ResetRetryableFailedToPending` | 启动时恢复到期可重试文件 |
| `MarkDead` | 标记死信 |

状态 key：

```text
pipeline_name:file_path
```

因此同一个物理文件如果被多个 pipeline 处理，状态互不影响。

## 8. Reader 设计

实现位置：[reader/reader.go](../reader/reader.go)

功能：

- 按配置分隔符解析行。
- 单字符分隔符支持基础引号处理。
- 多字符分隔符使用 `strings.Split`。
- 支持跳过前 N 个非空行。
- 支持 header meta 合并到每一行。
- 支持流式批读取，避免大文件一次性加载进内存。

读取模型：

```go
rdr.ReadBatches(file, batchSize, func(batch []model.Row) error {
    // 转换并写入
    return nil
})
```

`model.Row` 当前是：

```go
type Row map[string]string
```

也就是说 reader 和 transform 阶段都以字符串为主，真正写入 ClickHouse 前再做类型转换。

## 9. Transform Chain

转换链实现：

- [transform/chain.go](../transform/chain.go)
- [transform/dict_mapper.go](../transform/dict_mapper.go)
- [pipeline/pipeline.go](../pipeline/pipeline.go) 中的 `buildTransformChain`

当前支持两类转换器。

### 9.1 IP 匹配

配置：

```yaml
transformers:
  - type: ip_matcher
    fields: [src_ip, dst_ip]
    label_fields: [src_geo, dst_geo]
```

IP 库 CSV 格式：

```csv
start_ip,end_ip,country,province,city,isp
10.10.0.0,10.10.255.255,CN,Guangdong,Shenzhen,PrivateNet
```

输出字段示例：

```text
src_geo_country
src_geo_province
src_geo_city
src_geo_isp
```

IP 查询实现位于 [iputil/loader.go](../iputil/loader.go)，加载后按起始 IP 排序，查询时二分查找。

### 9.2 字典映射

内联字典：

```yaml
  - type: dict_mapper
    field: dns_type
    dict:
      "1": "A"
      "28": "AAAA"
```

外部字典：

```yaml
  - type: dict_mapper
    field: method
    dict_file: ./examples/full-test/dict/http_method.csv
```

字典文件格式：

```csv
1,GET
2,POST
```

## 10. ClickHouse 写入设计

写入实现：

- [writer/clickhouse.go](../writer/clickhouse.go)
- [writer/converter.go](../writer/converter.go)

### 10.1 显式列写入

writer 使用显式列插入：

```sql
INSERT INTO cdr.http_cdr (event_time, client_ip, server_ip, ...)
```

这样 ClickHouse 表中带 `DEFAULT` 的字段，例如：

```sql
ingest_time DateTime DEFAULT now()
```

可以由 ClickHouse 自动填充，不要求 ETL 传入。

### 10.2 类型转换

`fields` 配置决定 ClickHouse 输出列和类型转换：

```yaml
fields:
  - name: event_time
    type: DateTime
    layout: "2006-01-02 15:04:05"
  - name: client_ip
    type: IPv4
  - name: status_code
    type: UInt16
  - name: method
    type: LowCardinality(String)
```

支持类型：

- `String`
- `LowCardinality(String)`
- `UInt8/UInt16/UInt32/UInt64`
- `Int8/Int16/Int32/Int64`
- `Float32/Float64`
- `Bool`
- `Date/DateTime/DateTime64`
- `IPv4/IPv6`
- `Nullable(...)`

字段配置补充：

| 字段 | 说明 |
|---|---|
| `name` | 输出列名 |
| `source` | 输入 row 字段名，默认等于 `name` |
| `type` | ClickHouse 类型 |
| `layout` | 时间解析格式 |
| `default` | 输入为空时使用的默认字符串 |
| `nullable` | 输入为空且 nullable=true 时写入 nil |

### 10.3 ClickHouse 设计依据

本项目样例表设计参考以下 ClickHouse 规则：

- Per `schema-types-native-types`：时间、整数、IP 不应全部按 String 存储，应使用 DateTime、UInt、IPv4 等原生类型。
- Per `schema-types-lowcardinality`：方法、地域、运营商、DNS 类型等低基数字符串适合 `LowCardinality(String)`。
- Per `schema-pk-prioritize-filters`：`ORDER BY` 应优先选择常用过滤列，样例中按时间、IP、域名或状态码排序。
- Per `insert-batch-size`：ClickHouse 推荐批量写入，理想批大小通常在 10K 到 100K 行；测试样例为了快速观察设置得较小，生产应调大。

## 11. 成功归档、失败重试与死信

### 11.1 成功归档

配置：

```yaml
archive_dir: ./archive/http
cleanup_marker: true
```

处理成功后：

1. 状态标记为 `done`。
2. 如果是 marker 模式且 `cleanup_marker=true`，删除 `.ok` 文件。
3. 如果配置了 `archive_dir`，移动数据文件到归档目录。
4. 归档目标重名时追加纳秒时间戳，避免覆盖。

归档失败只记录 warn，不重新标记 failed，避免 ClickHouse 已写入后重复处理。

### 11.2 失败重试

配置：

```yaml
retry_failed: true
max_retries: 2
retry_interval: 5s
```

处理失败后：

1. 状态变为 `failed`。
2. `attempts` 加 1。
3. 写入 `next_retry_at`。
4. 服务启动时调用 `ResetRetryableFailedToPending` 恢复到期文件。

### 11.3 死信目录

配置：

```yaml
dead_letter_dir: ./dead/http
```

当 `attempts >= max_retries` 后：

1. 如果配置了死信目录，移动文件到该目录。
2. 状态标记为 `dead`。
3. 记录 `dead_letter_at` 和 `dead_letter_to`。

## 12. 指标与日志

指标实现：[metrics/metrics.go](../metrics/metrics.go)

启用配置：

```yaml
metrics:
  enabled: true
  addr: ":9090"
```

访问：

```bash
curl http://127.0.0.1:9090/debug/vars
```

主要指标：

| 指标后缀 | 说明 |
|---|---|
| `files_seen_total` | worker 收到的文件数 |
| `files_processing_total` | 成功认领处理的文件数 |
| `files_done_total` | 成功完成文件数 |
| `files_failed_total` | 处理失败文件数 |
| `files_skipped_total` | 未能认领、已被处理或跳过的文件数 |
| `rows_read_total` | 读取行数 |
| `rows_written_total` | 写入行数 |
| `transform_errors_total` | 转换失败行数 |
| `file_process_ms_total` | 文件处理耗时累计毫秒 |
| `files_archived_total` | 成功归档文件数 |
| `archive_errors_total` | 归档失败次数 |
| `markers_cleaned_total` | 清理 marker 次数 |
| `files_scheduled_retry_total` | 已安排重试次数 |
| `files_dead_total` | 进入死信文件数 |

指标名格式：

```text
go_etl_<pipeline_name>_<metric_name>
```

处理成功日志包含：

- `rows_read`
- `rows_written`
- `transform_errors`
- `duration`

## 13. 完整测试样例

目录：[examples/full-test](../examples/full-test)

包含：

- `schema.sql`：ClickHouse 建库建表脚本。
- `config.yaml`：完整主配置。
- `pipelines/dns_cdr.yaml`：DNS 管道，使用 `.ok` marker。
- `pipelines/http_cdr.yaml`：HTTP 管道，使用多字符分隔符和外部字典。
- `ipdb/ipdb.csv`：测试 IP 库。
- `dict/http_method.csv`：HTTP 方法字典。
- `watch/dns/*`、`watch/http/*`：样例话单。
- `bad-samples/*`：用于测试失败重试和死信。

运行：

```bash
clickhouse-client --multiquery < examples/full-test/schema.sql

./go-etl-linux-amd64 \
  -config examples/full-test/config.yaml \
  -store examples/full-test/file_status.db \
  -log info
```

验证：

```sql
SELECT count() FROM cdr.dns_cdr;
SELECT count() FROM cdr.http_cdr;
```

预期都是 `3`。

## 14. 构建与部署

Windows 下交叉编译脚本：[scripts/build.ps1](../scripts/build.ps1)

示例：

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\build.ps1 -Target linux-amd64 -Clean
powershell.exe -ExecutionPolicy Bypass -File scripts\build.ps1 -All -Clean
```

支持目标：

- `windows-amd64`
- `windows-arm64`
- `linux-amd64`
- `linux-arm64`
- `darwin-amd64`
- `darwin-arm64`

产物输出到：

```text
dist/
```

部署建议：

```text
/opt/go-etl/
  go-etl-linux-amd64
  config.yaml
  pipelines/
  data/
  logs/
```

运行示例：

```bash
./go-etl-linux-amd64 -config config.yaml -store data/file_status.db -log info
```

## 15. 测试

运行全部测试：

```bash
go test ./...
```

目前测试覆盖：

- 配置加载和默认值。
- 完整测试 fixture 配置加载。
- ready strategy。
- reader 批读取。
- bbolt 状态流转。
- 重试和死信状态。
- 归档和 marker 清理。
- 类型转换。
- ClickHouse 显式列 INSERT SQL。
- metrics 计数器。

## 16. 当前限制

需要开发人员注意的限制：

- 目前是单进程架构，不是多实例分布式消费。同一个目录如果启动多个 ETL 实例，bbolt 状态库不能跨进程共享协调。
- 多字符分隔符目前不处理引号内分隔符，只做 `strings.Split`。
- `ip_db.reload_interval` 已在配置中存在，但当前没有实现热加载。
- `HeaderMetaKey` 字段存在，但当前 header meta 直接按 key=value 或 `meta_N` 合并，没有用该字段包裹。
- writer 没有实现 ClickHouse async insert 配置。
- 失败重试恢复发生在服务启动时；当前没有后台定时把到期 failed 自动重新入队。
- 指标是 expvar 计数器，不是 Prometheus 原生格式。
- 状态库记录路径字符串；如果 watch_dir 路径写法变化，同一文件可能被视为不同 key。

## 17. 后续演进建议

优先级建议：

1. 实现 IP 库热加载，真正使用 `reload_interval`。
2. 增加 failed 到期文件的后台定时恢复，而不仅是启动恢复。
3. 增加配置项控制 ClickHouse async insert。
4. 增加管理接口，查询 pending/processing/failed/dead 文件列表。
5. 增加 Prometheus `/metrics` 输出。
6. 增加更严格的 CSV/多字符分隔符解析器，支持引号和转义。
7. 增加按日期或 pipeline 分层归档策略。
8. 增加表结构生成工具，由 pipeline `fields` 生成 ClickHouse DDL。

## 18. 开发约定

- 新增文档和代码注释默认使用中文。
- 保持 pipeline 配置向后兼容，已有 `field_names` 语义不要破坏。
- 文件处理成功后避免重新标记 failed，防止 ClickHouse 重复入库。
- 对文件状态变更优先使用 `FileStore` 的原子方法。
- 修改 ClickHouse 写入字段时，必须同步检查 `fields` 顺序和目标表列类型。
- 涉及 ClickHouse schema 或写入策略时，应检查 ClickHouse 最佳实践规则，尤其是原生类型、LowCardinality、ORDER BY 和批量写入。
