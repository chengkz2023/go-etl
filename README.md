# go-etl

轻量级自研ETL框架，专为网安/运营商大数据中台场景设计。

## 功能特性

- **多目录监控**：同时监控多个目录，每个目录对应一种话单schema
- **自定义分隔符**：支持 `|`、`|++|` 等任意分隔符
- **首行元数据处理**：自动将文件首行设备公共信息合并到所有数据行
- **可靠文件发现**：fsnotify + 定时轮询双通道，bolt持久化状态，重启不丢文件
- **IP段匹配**：自定义CSV IP库，二分查找 + 前缀树，O(log N)查询
- **字典映射**：字段值映射转换，支持内联配置和外部CSV文件
- **可插拔转换链**：Transformer接口，支持自定义转换逻辑
- **批量写入ClickHouse**：按文件批次同步提交，连接池管理，支持写入超时配置
- **并发处理**：可配置Worker数，适配不同量级

## 快速开始

### 安装

```bash
git clone <repo-url> go-etl
cd go-etl
go build -o etl ./cmd/etl/
```

### 配置

编辑 `config.yaml`：

```yaml
clickhouse:
  hosts: ["127.0.0.1:9000"]
  database: cdr
  username: default
  password: ""

ip_db:
  path: ./data/ipdb.csv
  columns: [country, province, city, isp]

pipelines:
  - name: dns_cdr
    watch_dir: /data/probe/dns
    delimiter: "|"
    has_header_meta: true
    header_fields:
      - name: probe_id
        type: LowCardinality(String)
      - name: region
        type: LowCardinality(String)
    fields:
      - name: timestamp
        type: DateTime
      - name: src_ip
        type: IPv4
      - name: dst_ip
        type: IPv4
      - name: domain
        type: String
      - name: dns_type
        type: LowCardinality(String)
    transformers:
      - type: ip_matcher
        fields: [src_ip, dst_ip]
        label_fields: [src_geo, dst_geo]
      - type: dict_mapper
        field: dns_type
        dict:
          "1": "A"
          "28": "AAAA"
    clickhouse_table: raw.dns_log
    workers: 4
```

### 运行

```bash
./etl -config config.yaml -store data/filestatus.db -log info
```

## 目录结构

```
go-etl/
├── cmd/etl/main.go           # 入口
├── config/                    # YAML配置加载
├── watcher/                   # 双通道文件监控(fsnotify + poller)
├── reader/                    # 分隔符解析器
├── transform/                 # 可插拔转换器(IP/字典/自定义)
├── writer/                    # ClickHouse批量写入
├── pipeline/                  # 完整ETL管道编排
├── store/                     # 文件处理状态持久化(bolt)
├── model/                     # 公共数据类型
├── iputil/                    # IP库加载与查找
└── config.yaml               # 配置示例
```

## 数据流

```
Watcher(fsnotify+poller) → Reader → Transform Chain → ClickHouse Writer
                                    ↓
                           IPMatcher → DictMapper → ...
```

## 文件处理可靠性

- **双通道**：fsnotify 负责实时，定时 poller(30s) 扫描兜底
- **bolt持久化**：每个文件处理状态 `unknown → pending → processing → done/failed`
- **重启恢复**：启动时全量扫描 + bolt去重，已处理文件自动跳过
- **防重保证**：bolt key = `pipeline:filepath`，原子性检查

## IP库格式

CSV格式，前两列为IP范围，后续列为属性：

```
start_ip,end_ip,country,province,city,isp
1.0.0.0,1.0.0.255,中国,福建,福州,电信
1.0.1.0,1.0.1.255,中国,福建,福州,联通
```

## 扩展自定义转换器

实现 `Transformer` 接口：

```go
type MyTransformer struct{}

func (t *MyTransformer) Name() string { return "my_logic" }

func (t *MyTransformer) Transform(row model.Row) (model.Row, error) {
    // 自定义逻辑
    return row, nil
}
```

然后在 `pipeline/pipeline.go` 的 `buildTransformChain` 中注册。

## License

Internal use.
