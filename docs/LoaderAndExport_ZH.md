# Loader 与 Export 配置解析

本文档说明 Data-Juicer 当前支持的数据加载（loader）和导出（export）配置。内容以实际代码路径为准：

- Loader：`data_juicer/core/data/load_strategy.py`
- Export：`data_juicer/core/export_manager.py`、`data_juicer/core/exporter.py`、`data_juicer/core/ray_exporter.py`

## Loader 配置总览

Loader 配置写在 `dataset.configs` 下。每一项代表一个数据源：

```yaml
dataset:
  configs:
    - type: local
      path: ./data/input.jsonl
      weight: 1.0
      load_kwargs: {}
```

公共字段：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `type` | 推荐 | 无 | 数据位置类型。常用值：`local`、`remote`。 |
| `source` | 视 loader 而定 | `*` | 数据源类型。远程数据必须配置，例如 `huggingface`、`s3`、`hdfs`、`tqs`、`hive`、`duckdb`、`lark`、`magnus`。Ray 本地读取时也可用它指定格式。 |
| `path` | 视 loader 而定 | 无 | 文件路径、目录路径、远程 URI 或远程数据集名称。 |
| `weight` | 否 | `1.0` | 多数据源混合时的采样权重。 |
| `load_kwargs` | 否 | `{}` | 透传给具体读取实现的额外参数。默认 executor 多数会传给 formatter/HuggingFace `load_dataset`；Ray 部分 loader 只会转发白名单参数。 |

与 loader 相关的顶层配置：

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `executor_type` | `default` | 决定使用默认 loader 还是 Ray loader。Ray 模式需要 `executor_type: ray`。 |
| `text_keys` | `text` | Data-Juicer 统一格式化时使用的文本字段。 |
| `suffixes` | `[]` | 本地文件读取时的后缀过滤。 |
| `np` | `4` | 默认模式的加载/处理进程数；Ray 模式下通常不是 Ray 读取并发的主控制项。 |
| `load_dataset_kwargs` | `{}` | 旧式 `dataset_path` 路径使用的额外加载参数；结构化 `dataset.configs` 更推荐使用每个数据源自己的 `load_kwargs`。 |

## Loader 选择规则

Data-Juicer 使用 `(executor_type, type, source)` 查找 loader：

1. 先匹配最具体的注册项。
2. 再匹配带 `*` 的通配注册项。
3. 找不到时会报 “No data load strategy found”。

常见例子：

| executor_type | type | source | 实际 loader |
| --- | --- | --- | --- |
| `default` | `local` | 任意或省略 | 默认本地 loader |
| `ray` | `local` | 任意或省略 | Ray 本地 loader |
| `default` | `remote` | `lark` | 默认 Lark Sheet loader |
| `ray` | `remote` | `lark` | Ray Lark Sheet loader |
| `ray` | `remote` | `hive` | Ray Hive loader |

## 本地文件 Loader

### 默认模式

```yaml
dataset:
  configs:
    - type: local
      path: ./data/input.jsonl
```

参数：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `path` | 是 | 本地文件或目录。支持格式由 formatter 自动识别，常见为 JSON、JSONL、CSV、TSV、TXT、Parquet 等。 |
| `load_kwargs` | 否 | 传给 formatter 的额外参数。 |

行为：

- 使用 `data_juicer.format.load.load_formatter` 加载。
- 会根据顶层 `text_keys`、`suffixes` 做统一格式化。
- 如果 pipeline 中包含 `suffix_filter`，loader 会为样本增加后缀信息。

### Ray 模式

```yaml
executor_type: ray
dataset:
  configs:
    - type: local
      path: ./data/input.parquet
      source: parquet
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `path` | 是 | 无 | 本地文件或目录。相对路径会尝试按当前目录、仓库根目录、用户目录、`work_dir` 解析。 |
| `source` | 否 | 自动识别 | 可显式指定格式：`json`、`text`、`csv`、`parquet`、`numpy`、`tfrecords`、`lance`。 |

行为：

- 使用 `RayDataset.read(data_format, path)`。
- 目录自动识别时，以目录中遇到的第一个文件后缀决定读取格式。
- 不能识别时默认按 JSON 读取。

## HuggingFace Loader

默认模式支持 HuggingFace；Ray 模式当前未实现。

```yaml
dataset:
  configs:
    - type: remote
      source: huggingface
      path: HuggingFaceFW/fineweb
      name: CC-MAIN-2024-10
      split: train
      data_dir: null
      data_files: null
      limit: 1000
```

参数：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `path` | 是 | HuggingFace 数据集名称或本地/远程数据集脚本路径。 |
| `name` | 否 | 数据集配置名。 |
| `split` | 否 | 数据集 split，例如 `train`。 |
| `data_files` | 否 | 传给 `datasets.load_dataset` 的数据文件。 |
| `data_dir` | 否 | 传给 `datasets.load_dataset` 的数据目录。 |
| `limit` | 否 | 传给 `datasets.load_dataset` 的限制参数。 |
| `load_kwargs` | 否 | 透传给 `datasets.load_dataset` 的额外参数。 |

## S3 Loader

### 默认模式

```yaml
dataset:
  configs:
    - type: remote
      source: s3
      path: s3://bucket/path/data.jsonl
      aws_access_key_id: <access_key>
      aws_secret_access_key: <secret_key>
      aws_session_token: null
      aws_region: us-east-1
      endpoint_url: https://s3.example.com
```

参数：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `path` | 是 | 必须以 `s3://` 开头。 |
| `aws_access_key_id` | 否 | S3 access key。 |
| `aws_secret_access_key` | 否 | S3 secret key。 |
| `aws_session_token` | 否 | 临时凭证 token。 |
| `aws_region` | 否 | 区域；默认模式下 HuggingFace/fsspec 通常可从路径或环境推断。 |
| `endpoint_url` | 否 | S3 兼容存储 endpoint。 |
| `anon` | 否 | 公开桶匿名访问。 |
| `load_kwargs` | 否 | 传给 `datasets.load_dataset` 的额外参数。 |

行为：

- 按文件后缀识别 JSON、JSONL、TXT、CSV、TSV、Parquet；不能识别时默认 JSON。
- 凭证优先级由 `get_aws_credentials` 处理，通常显式配置和环境变量都可用。

### Ray 模式

```yaml
executor_type: ray
dataset:
  configs:
    - type: remote
      source: s3
      path: s3://bucket/path/data.parquet
      format: parquet
      aws_access_key_id: <access_key>
      aws_secret_access_key: <secret_key>
      endpoint_url: https://s3.example.com
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `path` | 是 | 必须以 `s3://` 开头。 |
| `format` | 否 | 自动识别；不能识别时默认 `parquet` | 支持 `json`、`text`、`csv`、`parquet`、`numpy`、`tfrecords`、`lance`。 |
| `aws_access_key_id` / `aws_secret_access_key` / `aws_session_token` / `aws_region` / `endpoint_url` | 否 | 无 | 用于创建 PyArrow S3 filesystem。 |

## HDFS Loader

### 默认模式

```yaml
dataset:
  configs:
    - type: remote
      source: hdfs
      path: hdfs://cluster/path/data.jsonl
```

参数：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `path` | 是 | 必须以 `hdfs://` 开头。 |
| `load_kwargs` | 否 | 复制到本地暂存后，透传给本地 loader。 |

行为：

- 先将 HDFS 文件或目录复制到 `work_dir/.io_cache/load/...`。
- 再复用本地 loader 读取。

### Ray 模式

```yaml
executor_type: ray
dataset:
  configs:
    - type: remote
      source: hdfs
      path: hdfs://cluster/path/table_or_file
      format: parquet
      filesystem: pyarrow
      columns: ["text", "label"]
      override_num_blocks: 128
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `path` | 是 | 无 | 必须以 `hdfs://` 开头。 |
| `format` | 否 | `parquet` | Ray HDFS 直读当前只支持 Parquet。 |
| `filesystem` | 否 | `pyarrow` | 支持 `pyarrow` 或 `webhdfs`。 |
| `webhdfs` | 否 | `{}` | `filesystem: webhdfs` 时传给 fsspec 的参数，例如 `host`、`port`、`user`。 |
| `load_kwargs` | 否 | `{}` | 读取参数。 |
| `columns`、`parallelism`、`num_cpus`、`num_gpus`、`memory`、`ray_remote_args`、`tensor_column_schema`、`partition_filter`、`partitioning`、`shuffle`、`include_paths`、`file_extensions`、`concurrency`、`override_num_blocks` | 否 | 无 | 会转发给 Ray Parquet reader 的白名单参数。 |

## TQS Loader

```yaml
dataset:
  configs:
    - type: remote
      source: tqs
      query: SELECT text, label FROM db.table
      tqs_app_id: <app_id>
      tqs_app_key: <app_key>
      user_name: <user_name>
      read_mode: materialized
      output_uri: hdfs://cluster/tmp/dj_tqs_result
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `query` | 是 | 无 | 要执行的 SQL。 |
| `tqs_app_id` | 是 | 无 | TQS app id。 |
| `tqs_app_key` | 是 | 无 | TQS app key。 |
| `user_name` | 是 | 无 | TQS 执行用户。 |
| `read_mode` | 否 | `materialized` | `materialized`：SQL 写到 `output_uri` 后再加载；`client_result`：直接拉取查询结果。 |
| `output_uri` / `tqs_output_uri` | materialized 必填 | 无 | 查询结果物化位置。 |
| `cluster` | 否 | `""` | materialized 模式下的 Yarn cluster。 |
| `queue_name` | 否 | `""` | materialized 模式下的队列。 |
| `priority` | 否 | `5` | materialized 模式下的任务优先级。 |
| `memory` | 否 | `0` | materialized 模式下的 executor memory，单位 GB。 |
| `max_result_rows` | 否 | `10000` | client_result 模式最多拉取行数。 |
| `tqs_cluster` | 否 | `cn` | client_result 模式 TQS cluster。 |
| `tqs_enable_domain` | 否 | 无 | client_result 模式 domain 开关。 |
| `tqs_timeout` | 否 | `120` | client_result 模式超时秒数。 |

默认 executor 和 Ray executor 都支持 TQS。Ray 的 `client_result` 会用 `ray.data.from_items` 构造 Ray Dataset；`materialized` 会先落地再走 staged loader。

## Hive Loader

Hive loader 只支持 Ray 模式，并依赖内部 byted-ray。

```yaml
executor_type: ray
dataset:
  configs:
    - type: remote
      source: hive
      table_name: db.table
      columns:
        - text
        - label
        - name: user_id
          cast: BIGINT
      filter: "date='20260515'"
      concurrency: 64
      override_num_blocks: 128
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `table_name` | 是 | 无 | Hive 表名。 |
| `columns` | 否 | 全部列 | 可以是字符串列表，也可以包含 `{name, cast}` 的对象列表。`cast` 当前支持 `STRING`、`BIGINT`。 |
| `filter` | 否 | 无 | Hive/Parquet reader 的过滤条件。 |
| `concurrency` | 否 | 无 | 读取并发。 |
| `override_num_blocks` | 否 | 无 | Ray block 数。 |
| `ray_remote_args` | 否 | 无 | Ray remote 参数。 |
| `arrow_parquet_args` | 否 | `{}` | 透传给底层 Arrow/Parquet reader 的参数。 |
| `load_kwargs` | 否 | `{}` | 先合入 read kwargs，再被显式字段覆盖。 |

明确不再支持的旧字段：`sql`、`table`、`output_uri`、`tqs_output_uri`、`read_mode`、`max_result_rows`、`tqs_app_id`、`tqs_app_key`、`user_name`、`tqs_cluster`、`tqs_enable_domain`、`tqs_timeout`、`catalog`、`cast_columns`。

## DuckDB Loader

```yaml
dataset:
  configs:
    - type: remote
      source: duckdb
      sql: SELECT * FROM read_parquet('/path/to/*.parquet')
      path_mapping:
        /remote/prefix: /local/prefix
```

参数：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `sql` | 是 | DuckDB SQL。查询结果会物化为暂存 Parquet，再走 staged loader。 |
| `path_mapping` | 否 | 路径映射，供 `materialize_duckdb_query` 使用。 |

默认模式和 Ray 模式都支持 DuckDB staged loading。

## Lark Sheet Loader

```yaml
dataset:
  configs:
    - type: remote
      source: lark
      lark_path: https://bytedance.larkoffice.com/sheets/<spreadsheet_token>?sheet=<sheet_id>
      lark_app_id: <lark_app_id>
      lark_app_secret: <lark_app_secret>
      file_extension: csv
      document_type: sheet
      wait_export_time_seconds: 60
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `lark_path` | 是 | 无 | 飞书表格 URL 或 bare spreadsheet token。URL 中可带 `?sheet=<sheet_id>`。 |
| `sheet_id` | URL 中无 `sheet` 时必填 | URL query | 目标 sheet id。如果 URL 和字段同时设置，必须一致。 |
| `lark_app_id` | 是 | 无 | 飞书应用 app id。 |
| `lark_app_secret` | 是 | 无 | 飞书应用 app secret。不要提交真实 secret。 |
| `file_extension` | 否 | `csv` | 当前只支持 `csv`。 |
| `document_type` | 否 | `sheet` | 当前只支持 `sheet`。 |
| `wait_export_time_seconds` | 否 | `60` | 等待 Drive 导出任务完成的最长秒数。 |

读取行为：

1. 优先创建 Drive export task，将 sheet 导出为 CSV。
2. 如果导出创建失败且错误是权限类 `1069902/no permission`，则降级为 Sheets values read。
3. values read 成功后，会把二维单元格写成暂存 CSV，再复用普通 CSV loader。
4. 如果 app 对表格没有读权限，values read 会失败，常见错误是 `91403 Forbidden`。

权限要求：

- loader 使用 `lark_app_id + lark_app_secret` 创建 tenant/app client，不使用本机用户登录态。
- 应用需要开通对应 API scope。
- 具体表格也需要分享给应用/bot；用户自己能读，不代表应用能读。

## Magnus Loader

```yaml
dataset:
  configs:
    - type: remote
      source: magnus
      table_name: db.table
      filter: "date = '20260515'"
      magnus_conf:
        catalog: <catalog>
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `table_name` | 是 | 无 | Magnus 表名。 |
| `filter` | 否 | 无 | 下推过滤条件。 |
| `magnus_conf` | 否 | `{}` | 传给 Magnus/PyIceberg 的配置。 |

默认模式会读成 pandas 再转 HuggingFace Dataset；Ray 模式会读成 Ray Dataset。

## 当前注册但不可用的 Loader

以下 loader 有注册类或配置校验，但 `load_data` 当前直接抛出 `NotImplementedError`：

| source | executor_type | 状态 |
| --- | --- | --- |
| `modelscope` | `default` | 未实现 |
| `arxiv` | `default` | 未实现 |
| `wiki` | `default` | 未实现 |
| `commoncrawl` | `default` | 未实现 |
| `huggingface` | `ray` | 未实现 |

## Export 配置总览

推荐使用结构化 `export` 配置；旧式 `export_path`、`export_type` 等字段仍可用。

旧式配置：

```yaml
export_path: ./outputs/result.jsonl
export_type: jsonl
export_shard_size: 0
export_in_parallel: false
export_extra_args: {}
export_aws_credentials: null
keep_stats_in_res_ds: false
keep_hashes_in_res_ds: false
```

结构化配置：

```yaml
export:
  target: local
  path: ./outputs/result.jsonl
  type: jsonl
  shard_size: 0
  in_parallel: false
  extra_args: {}
```

公共字段：

| 字段 | 旧式字段 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `target` | 从 `export_path` 推断 | 自动推断 | 导出目标：`local`、`s3`、`hdfs`、`hive`、`lark`、`tos`、`magnus`。 |
| `path` | `export_path` | 无 | 输出路径。local/s3/hdfs 使用；部分远端目标作为暂存类型推断来源。 |
| `type` | `export_type` | 从路径后缀推断 | 输出格式。 |
| `shard_size` | `export_shard_size` | `0` | 分片大小，字节。`0` 表示单文件。 |
| `in_parallel` | `export_in_parallel` | `false` | 默认模式单文件导出是否并行。 |
| `extra_args` | `export_extra_args` | `{}` | 传给底层导出函数的额外参数。 |
| `aws_credentials` | `export_aws_credentials` | `{}` | S3 导出凭证。 |

导出前，默认会移除中间字段：

- `keep_stats_in_res_ds: false` 时移除 `__dj__stats__`、`__dj__meta__`。
- `keep_hashes_in_res_ds: false` 时移除 `__dj__hash__`、`__dj__minhash__`、`__dj__simhash__`、`__dj__imagehash__`、`__dj__videohash__`。

## Export target 推断规则

如果没有显式设置 `export.target`：

1. `hive_table` 存在时推断为 `hive`。
2. `table_name` 存在且 `magnus_conf` 不为 `null` 时推断为 `magnus`。
3. `lark_path` 存在时推断为 `lark`。
4. `bucket_name` 和 `object_key` 同时存在时推断为 `tos`。
5. 否则根据 `path`/`export_path` 推断：`s3://` 为 `s3`，`hdfs://` 为 `hdfs`，其他为 `local`。

## Local 与 S3 Export

```yaml
export:
  target: local
  path: ./outputs/result.jsonl
  type: jsonl
```

```yaml
export:
  target: s3
  path: s3://bucket/outputs/result.parquet
  type: parquet
  aws_credentials:
    aws_access_key_id: <access_key>
    aws_secret_access_key: <secret_key>
    aws_session_token: null
    endpoint_url: https://s3.example.com
```

默认模式支持：`jsonl`、`json`、`parquet`、`csv`。

Ray 模式支持：`jsonl`、`json`、`parquet`、`csv`、`tfrecords`、`webdataset`、`lance`。

## HDFS Export

```yaml
export:
  target: hdfs
  path: hdfs://cluster/path/result.parquet
  type: parquet
```

行为：

- 先导出到本地 `work_dir/.io_cache/export/...`。
- 再通过 PyArrow filesystem 复制到 HDFS。
- `type` 可显式设置；不设置时依次从 `path` 后缀、默认值推断。

## Hive Export

Hive export 只支持 Ray 模式，并依赖内部 byted-ray。

```yaml
executor_type: ray
export:
  target: hive
  table_name: db.table
  partition:
    date: "20260515"
  mode: append
  auto_cast_schema: true
  concurrency: 64
```

参数：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `table_name` | 是 | 目标 Hive 表。 |
| `partition` | 否 | 分区，可以是字符串或字典。 |
| `mode` | 否 | 写入模式，透传给 `write_hive_table`。 |
| `auto_cast_schema` | 否 | 是否自动 cast 到目标表 schema。 |
| `concurrency` | 否 | 写入并发。 |
| `ray_remote_args` | 否 | Ray remote 参数。 |
| `arrow_parquet_args` | 否 | 透传给底层 parquet writer 的参数。 |

## Lark Export

```yaml
export:
  target: lark
  lark_path: https://bytedance.larkoffice.com/sheets/<spreadsheet_token>?sheet=<sheet_id>
  lark_app_id: <lark_app_id>
  lark_app_secret: <lark_app_secret>
  range: A1
  type: csv
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `lark_path` | 是 | 无 | 目标飞书表格 URL，必须能解析出 spreadsheet token 和 sheet id。 |
| `lark_app_id` | 是 | 无 | 飞书应用 app id。 |
| `lark_app_secret` | 是 | 无 | 飞书应用 app secret。不要提交真实 secret。 |
| `range` | 是 | 无 | 写入起始区域，例如 `A1`。 |
| `type` | 否 | `csv` | 暂存导出格式。当前 Lark 上传链路按文件上传并更新单元格。 |

行为：

- 先把结果数据集导出为本地暂存文件，默认 `dataset.csv`。
- 再上传文件到 Lark，并把文件对象写入目标 sheet 的 `range`。
- 与 Lark loader 一样，使用 app/tenant 身份，不使用本机 user 登录态。

## TOS Export

```yaml
export:
  target: tos
  bucket_name: <bucket>
  object_key: outputs/result.jsonl
  type: jsonl
  endpoint: https://tos-cn-beijing.volces.com
  region: cn-beijing
  access_key: <access_key>
  secret_key: <secret_key>
  session_token: null
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `bucket_name` | 是 | 无 | TOS bucket。 |
| `object_key` | 是 | 无 | TOS object key。 |
| `type` | 否 | 从 `object_key` 或 `path` 推断 | 暂存文件格式。 |
| `endpoint` | 否 | `https://tos-cn-beijing.volces.com` | TOS endpoint。 |
| `region` | 否 | `cn-beijing` | TOS region。 |
| `access_key` / `secret_key` / `session_token` | 否 | 无 | TOS 凭证。 |

限制：当前 TOS export 要求暂存结果是单文件；目录分片会报错。

## Magnus Export

```yaml
export:
  target: magnus
  table_name: db.table
  magnus_conf:
    write_options:
      write.format.default: lance
  partition_columns: ["date"]
  partition_values:
    date: "20260515"
  operation: APPEND
  create_table_if_not_exists: false
  infer_schema_on_create: false
  schema: null
  magnus_failure_policy: abort
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `table_name` | 是 | 无 | 目标 Magnus 表。 |
| `magnus_conf` | 否 | `{}` | 传给 Magnus/PyIceberg 写入的配置。 |
| `partition_columns` | 否 | 无 | 分区列名列表。 |
| `partition_values` | 否 | 无 | 当前写入对应的分区值。 |
| `schema` | 否 | 无 | 创建表或对齐写入时使用的显式 schema。 |
| `create_table_if_not_exists` | 否 | `false` | 表不存在时是否自动创建。 |
| `infer_schema_on_create` | 否 | `false` | 配合 `create_table_if_not_exists: true` 使用；未配置 `schema` 时从数据集推断 schema。 |
| `magnus_failure_policy` | 否 | `abort` | 支持 `abort` 和 `commit_completed_unsafe`。后者仅适合 Ray Data checkpoint 场景。 |
| `batch_size` | 否 | `2000` | 默认模式 HuggingFace Dataset 写入时的 batch size。 |
| `operation` | Ray 模式可选 | `APPEND` | Ray Magnus 写入操作，例如 `APPEND`、`OVERWRITE` 等，具体取决于目标 SDK。 |
| `validate_overwrite_partition_before_write` | Ray 模式可选 | `false` | OVERWRITE 分区写入前是否提前 materialize/count 并校验分区值。 |

注意：

- `partition_columns` 和 `partition_values` 用于分区写入和覆盖保护。需要覆盖指定分区，或创建分区表时，应配置它们。
- `create_table_if_not_exists: true` 且没有 `schema` 时，只有 `infer_schema_on_create: true` 才会自动推断 schema。
- Ray 模式会尽量保持 Ray Data lazy；开启 `validate_overwrite_partition_before_write` 会提前触发计算。

## WebDataset Export

Ray 模式支持 WebDataset：

```yaml
executor_type: ray
export:
  target: local
  path: ./outputs/webdataset
  type: webdataset
  extra_args:
    field_mapping:
      txt: text
      png: images
      json: metadata
```

`field_mapping` 用于把数据集字段映射到 WebDataset 样本扩展名。

## 旧式 `dataset_path`

旧式配置仍可用：

```yaml
dataset_path: ./data/input.jsonl
```

它简单但表达能力弱，不适合多数据源、远程数据源、权重、凭证、read mode 等场景。新配置应优先使用：

```yaml
dataset:
  configs:
    - type: local
      path: ./data/input.jsonl
```
