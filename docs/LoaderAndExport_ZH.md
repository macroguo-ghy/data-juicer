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
      limit: 1000
      on_bad_files: error
      skip_zero_row_group_files: true
      override_num_blocks: 128
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `path` | 是 | 无 | 必须以 `hdfs://` 开头。 |
| `format` | 否 | `parquet` | Ray HDFS 直读当前只支持 Parquet。 |
| `filesystem` | 否 | `pyarrow` | HDFS filesystem 实现。生产和线上 Ray 集群使用 `pyarrow`；`webhdfs` 仅用于本地或测试环境验证。 |
| `webhdfs` | 否 | `{}` | 仅在 `filesystem: webhdfs` 的测试场景生效，传给 fsspec 的参数，例如 `host`、`port`、`user`。 |
| `limit` | 否 | 无 | Ray HDFS 直读 Parquet 后立即应用 `Dataset.limit(limit)`，用于限制进入后续 process/export 的行数。 |
| `on_bad_files` | 否 | `error` | 坏 parquet 文件处理策略。`error` 保持 fail-fast；`skip` 会在 Data-Juicer 调用 Ray reader 前预检并跳过 zero-byte、`0 row groups`、metadata 读取失败的文件。如果所有文件都被跳过，则返回空 Ray Dataset。该配置不透传给 Ray，也不覆盖 worker 读取 data page 时才暴露的深层损坏。 |
| `skip_zero_row_group_files` | 否 | `true` | 是否在调用 Ray Parquet reader 前预检 Ray 采样候选文件，并跳过会导致 `row_group_ids=[0]` 采样失败的 `0 row groups` 文件。默认开启；如需完全跳过该预检，可显式设为 `false`。 |
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
  in_parallel: false
  max_rows: null
  max_rows_mode: limit
  max_rows_quota_batch_size: null
  extra_args: {}
```

公共字段：

| 字段 | 旧式字段 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `target` | 从 `export_path` 推断 | 自动推断 | 导出目标：`local`、`s3`、`hdfs`、`hive`、`lark`、`tos`、`magnus`。 |
| `path` | `export_path` | 无 | 输出路径。local/s3/hdfs 使用；部分远端目标作为暂存类型推断来源。 |
| `type` | `export_type` | 从路径后缀推断 | 输出格式。 |
| `in_parallel` | `export_in_parallel` | `false` | 默认模式单文件导出是否并行。 |
| `max_rows` | 无 | `null` | 控制传给导出 sink 的行数，必须是正整数。 |
| `max_rows_mode` | 无 | `limit` | `limit`：默认实现，导出行数不超过 `max_rows`，Ray 模式会在写入前应用 `Dataset.limit(max_rows)` 并尽量保留 lazy limit 下推能力。`quota_reservation`：Ray-only，按 pyarrow batch 整批放行直到至少达到 `max_rows`，随后 materialize quota 过滤后的 Ray Dataset 再交给 sink，成功写入时行数可超过 `max_rows`。 |
| `max_rows_quota_batch_size` | 无 | 算子默认 batch size | 仅 `max_rows_mode: quota_reservation` 生效。batch 越大，actor 调用越少，但超出 `max_rows` 的行数可能越多。 |
| `shard_size` | `export_shard_size` | `0` | 废弃字段。旧式本地文件导出仍可识别；新配置不要继续使用。Ray HDFS 分布式导出不支持该字段，需要控制文件大小或行数时使用目标 sink 的 `extra_args`。 |
| `extra_args` | `export_extra_args` | `{}` | 传给底层导出函数的额外参数。 |
| `aws_credentials` | `export_aws_credentials` | `{}` | S3 导出凭证。 |

`export.targets` 用于 Ray 文件 fan-out 写出，不能和 `export.target` 同时配置。第一版支持 `executor_type: ray`、`target: hdfs/local`、`type: parquet/jsonl`，且同一个 `targets` 列表里的所有 target 的 `target` 与 `type` 必须一致。HDFS path 必须以 `hdfs://` 开头；local path 支持相对路径、绝对路径或 `file://`，但该目录必须对所有 Ray worker 可见。每个 target 可配置 `filter_condition`，语法复用 `general_field_filter` 的字段比较表达式；同一行可命中多个 target。任一写出失败都会让任务失败；`mode: append` 仍是 at-least-once，task retry 或用户重跑可能产生重复 part 文件。启用 `ray_data_checkpoint.enabled: true` 时，fan-out 只支持所有 target 都显式配置 `mode: append`；未配置 `mode` 仍按默认 `error_if_exists` 处理并被拒绝。

```yaml
executor_type: ray
export:
  targets:
    - target: hdfs
      type: parquet
      path: hdfs://cluster/path/high_score
      mode: overwrite
      filter_condition: "score >= 0.8"
      filesystem: pyarrow
    - target: hdfs
      type: parquet
      path: hdfs://cluster/path/zh
      mode: overwrite
      filter_condition: "lang == 'zh'"
      filesystem: pyarrow
```

```yaml
executor_type: ray
export:
  targets:
    - target: local
      type: jsonl
      path: ./outputs/fanout/high_score
      mode: overwrite
      filter_condition: "score >= 0.8"
    - target: local
      type: jsonl
      path: file:///tmp/data-juicer/fanout/zh
      mode: overwrite
      filter_condition: "lang == 'zh'"
```

`export.max_rows` 只控制传给 sink 的数据规模，不改变写入模式；例如 `OVERWRITE` 仍会覆盖目标，只是写入受控后的数据。`limit` 模式下，Ray limit 可能被下推并减少兼容 lazy pipeline 的上游执行量，但这是 best-effort：需要全量输入的算子、all-to-all 算子、filter、已 materialize 的 dataset 都可能执行超过 `max_rows` 行的上游工作。`quota_reservation` 控制的是进入 sink 输入流的数据，不是写入提交计数器；它会在真正 sink 前增加一次 materialize 屏障，用来避免 Ray 写入前的 schema/sample 动作重复执行带状态的 quota reservation。任务重试或 sink 失败时不提供 exactly-once 计数保证。`ray_collect_real_metrics: true` 不能和 `export.max_rows` 同时配置，因为导出前的 eager `materialize()` / `count()` 会破坏 lazy limit 路径。

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

### 默认模式

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

### Ray 分布式模式

Ray 模式下，`parquet` 和 `jsonl` 可以直接由 Ray workers 分布式写入 HDFS 目录，避免先写到 driver 本地再复制到 HDFS。该路径适合大规模输出，也是 `ray_data_checkpoint.enabled: true` 与 HDFS export 配合时的要求。

```yaml
executor_type: ray
export:
  target: hdfs
  type: parquet
  path: hdfs://cluster/path/output_dir
  filesystem: pyarrow
  mode: error_if_exists
  extra_args:
    max_rows_per_file: 10000
    concurrency: 64
```

```yaml
executor_type: ray
export:
  target: hdfs
  type: jsonl
  path: hdfs://cluster/path/output_dir
  filesystem: pyarrow
  mode: error_if_exists
  extra_args:
    num_rows_per_file: 10000
    concurrency: 64
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `path` | 是 | 无 | 目标 HDFS 目录，必须以 `hdfs://` 开头。Ray 分布式 HDFS export 要求目录路径，不能是 `*.parquet`、`*.json`、`*.jsonl`、`*.csv` 这类文件路径。 |
| `type` | 否 | 从路径后缀推断，否则 `jsonl` | Ray 分布式 HDFS export 当前支持 `parquet`、`jsonl`。其他格式仍走默认 staging copy 路径；开启 `ray_data_checkpoint` 时会提前报错。 |
| `filesystem` | 否 | `pyarrow` | HDFS filesystem 实现。生产和线上 Ray 集群使用 `pyarrow`；`webhdfs` 仅用于本地或测试环境验证。 |
| `webhdfs` | 否 | `{}` | 仅在 `filesystem: webhdfs` 的测试场景生效，传给 fsspec 的参数，例如 `host`、`port`、`user`。 |
| `mode` | 否 | `error_if_exists` | 写入模式。`error_if_exists`：目标已存在时失败；`overwrite`：写入前删除已有目标；`append`：直接追加 part 文件，重试或重跑可能产生重复文件。single-target checkpoint 的可启用性不依赖具体 `mode`，但 fan-out checkpoint 只允许显式 `append`。 |
| `extra_args` | 否 | `{}` | 传给 Ray writer / datasink 的参数，例如 `concurrency`、`ray_remote_args`、`min_rows_per_file`、`num_rows_per_file`、`max_rows_per_file`。Parquet 的 `max_rows_per_file` 只有在当前 Ray writer 支持该参数时才会生效；旧版 Ray 不支持时会被参数过滤逻辑丢弃。JSONL 推荐使用 `num_rows_per_file` 或 `min_rows_per_file`。 |

限制：

- Ray 分布式 HDFS export 不支持 `export.shard_size`；需要控制文件大小或行数时使用 `export.extra_args` 中的 Ray writer 参数。
- `ray_data_checkpoint.enabled: true` 的硬要求是 Ray 文件 source 到文件 sink 的 lazy 路径；HDFS export 场景需要使用上述 Ray 分布式 `parquet/jsonl` 写入路径。
- single-target checkpoint 与 `export.mode` 没有硬绑定，但恢复语义不同。`error_if_exists` / `overwrite` 主要适合同一次 Ray job 内部 task retry；如果失败后由用户重新提交任务，`error_if_exists` 可能因输出目录已存在而失败，`overwrite` 会删除上次输出进度，因此二者对跨任务恢复意义有限。启用 checkpoint 且使用这两个 mode 时会输出 warning。
- fan-out checkpoint 只允许 `export.targets[].mode: append`。`ray_data_checkpoint.delete_no_checkpoint_files: true` 可配置，但不会把 fan-out 输出升级成 exactly-once；custom fan-out datasink 走 Ray 的 post-write checkpoint，多个目标目录之间没有原子清理或原子可见性保证。
- `mode: append` 才可能保留已经写出的 part 文件，但第一版只提供 at-least-once 语义，不提供 exactly-once；同一次任务重试、用户重跑或局部失败后再次提交都可能产生重复 part。

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
  mode: file
```

参数：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `lark_path` | 视模式而定 | 无 | 目标飞书表格 URL，必须能解析出 spreadsheet token 和 sheet id。`create_spreadsheet: true` 时可不写。 |
| `lark_app_id` | 是 | 无 | 飞书应用 app id。 |
| `lark_app_secret` | 是 | 无 | 飞书应用 app secret。不要提交真实 secret。 |
| `range` | 仅 `file`/`upload` 必填 | `append` 从 `lark_path` 的 `sheet` 推断；`overwrite` 从 `A1` 开始 | 写入范围。`append` 可不写，会默认追加到 `lark_path` 指定的 sheet 当前表尾；`overwrite` 可不写，会按暂存 CSV 的行列数从 `A1` 覆盖写入。 |
| `type` | 否 | `csv` | 暂存导出格式。`append` 和 `overwrite` 只支持 `csv`。 |
| `mode` | 否 | `file` | `file`/`upload`：上传暂存文件并把文件对象写入目标单元格；`append`：把暂存 CSV 的行追加写入目标 sheet；`overwrite`：把暂存 CSV 覆盖写入目标 sheet。 |
| `skip_header` | 否 | `append` 默认为 `true`；`overwrite` 默认为 `false` | values 写入时是否跳过暂存 CSV 表头。 |
| `clear_sheet` | 否 | `true` | 仅 `overwrite` 且未配置 `range` 时生效。写入前删除本次输出行数之后的旧行，避免历史尾行残留。 |
| `create_spreadsheet` | 否 | `false` | 为 `true` 且未配置 `lark_path` 时，先创建一个新的飞书表格，再写入。 |
| `spreadsheet_title` / `title` | 否 | `data-juicer-export` | `create_spreadsheet: true` 时的新表标题。 |

行为：

- 先把结果数据集导出为本地暂存文件，默认 `dataset.csv`。
- `mode: file`/`upload`：再上传文件到 Lark，并把文件对象写入目标 sheet 的 `range`。
- `mode: append`：读取暂存 CSV，并调用 Lark Sheets append 接口把数据行追加到目标 sheet。不写 `range` 时使用 `lark_path` 里的 `sheet` 作为 append range；写单个起始单元格时会按暂存 CSV 的行列数扩展为矩形范围。
- `mode: overwrite`：读取暂存 CSV，并调用 Lark Sheets values PUT 接口覆盖写入。未配置 `range` 时从 `A1` 开始，自动扩展到暂存 CSV 的行列范围，并默认清理旧的尾行。
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

## 从 Demo 抽取的配置案例

本节从 `demos/` 中抽取常见 loader/export 组合，并对内部路径、表名、凭证做了占位符化。真实任务中应替换为自己的资源。

### 案例 1：Ray 本地 JSONL 读取，导出到本地目录

来源：`demos/process_on_ray/configs/demo-new-config.yaml`

```yaml
project_name: ray-local-jsonl-demo
executor_type: ray
ray_address: auto

dataset:
  configs:
    - type: local
      path: ./demos/process_on_ray/data/demo-dataset.jsonl
      weight: 1.0

process:
  - text_length_filter:
      min_len: 10

export_path: ./outputs/demo/demo-processed
```

适用场景：

- 本地文件作为输入。
- 需要在 Ray 上跑算子。
- 结果写到本地路径；Ray export 格式由 `export_path` 或 `export_type` 推断。

### 案例 2：S3 读取，结果元信息导出回 S3

来源：`demos/process_video_on_ray/configs/s3_video_processing_config.yaml`

```yaml
project_name: s3-video-processing-demo
executor_type: ray
ray_address: auto
work_dir: ./outputs/s3_demo

dataset:
  configs:
    - type: remote
      source: s3
      path: s3://<bucket>/dj/dataset/input.jsonl
      aws_region: us-east-1
      endpoint_url: https://<s3-compatible-endpoint>

export_path: s3://<bucket>/dj/dataset/demo-processed
export_type: jsonl
export_aws_credentials:
  aws_access_key_id: <aws_access_key_id>
  aws_secret_access_key: <aws_secret_access_key>
  aws_region: us-east-1
  endpoint_url: https://<s3-compatible-endpoint>
```

适用场景：

- 数据集文件在 S3 或 S3 兼容存储。
- 默认 loader 使用 fsspec/s3fs；Ray loader 使用 PyArrow S3 filesystem。
- 输入凭证在 `dataset.configs` 中配置，输出凭证在 `export_aws_credentials` 或结构化 `export.aws_credentials` 中配置。

### 案例 3：Lark Sheet 读取，导出 JSONL

来源：`demos/bytedance/lark_sheet_loader/lark_sheet_loader_default.yaml`

```yaml
project_name: lark-sheet-loader-default

dataset:
  configs:
    - type: remote
      source: lark
      lark_path: https://bytedance.larkoffice.com/sheets/<spreadsheet_token>?sheet=<sheet_id>
      lark_app_id: <lark_app_id>
      lark_app_secret: <lark_app_secret>
      file_extension: csv

process:
  - text_length_filter:
      min_len: 0

export_path: ./outputs/lark_sheet_loader_default.jsonl
```

适用场景：

- 一个飞书电子表格作为输入。
- 表格中需要有 Data-Juicer 后续算子要用的字段，例如 `text`。
- 应用需要有目标表的读取权限；如果 Drive 导出没权限但 values read 有权限，会自动 fallback 到 read 后写临时 CSV。

### 案例 4：Lark Sheet 读取、处理并追加回同一 Sheet

来源：`demos/bytedance/lark_sheet_loader/lark_sheet_transform_append.yaml`

```yaml
project_name: lark_sheet_transform_append
text_keys: null

dataset:
  configs:
    - type: remote
      source: lark
      lark_path: "https://bytedance.larkoffice.com/sheets/<spreadsheet_token>?sheet=<sheet_id>"
      lark_app_id: "<lark_app_id>"
      lark_app_secret: "<lark_app_secret>"
      file_extension: csv

process:
  - python_lambda_mapper:
      lambda_str: 'lambda d: {k: ("empty" if v is None or v == "" else (v + 1 if isinstance(v, (int, float)) and not isinstance(v, bool) else (v + "_process_by_dj" if isinstance(v, str) else v))) for k, v in d.items()}'

export:
  target: lark
  mode: append
  lark_path: "https://bytedance.larkoffice.com/sheets/<spreadsheet_token>?sheet=<sheet_id>"
  lark_app_id: "<lark_app_id>"
  lark_app_secret: "<lark_app_secret>"
  type: csv
  skip_header: true
```

适用场景：

- 不要求输入表有 `text` 列；`text_keys: null` 会关闭默认文本列校验。
- mapper 会遍历每一列：数字加 1，字符串追加 `_process_by_dj`，空值写成 `empty`，其他 Python 值类型保持不变。
- `skip_header: true` 会避免把暂存 CSV 的表头再次追加进原表。

### 案例 5：Ray Hive 读取，写入 Magnus Lance 表

来源：`demos/bytedance/process_landing_page_on_ray/configs/preloads_demo.yaml`

```yaml
project_name: hive-to-magnus-ray
executor_type: ray
ray_address: auto
min_common_dep_num_to_combine: 0

dataset:
  configs:
    - type: remote
      source: hive
      table_name: <hive_db.hive_table>
      columns:
        p_date: STRING
        site_id: BIGINT
        text: STRING
      filter: |
        p_date = '<partition_date>'
        AND text IS NOT NULL
      override_num_blocks: 1024
      concurrency: 32
      ray_remote_args:
        num_cpus: 1

export:
  target: magnus
  table_name: <catalog.db.output_table>
  create_table_if_not_exists: true
  operation: OVERWRITE
  partition_columns: ["p_date"]
  partition_values:
    p_date: <partition_date>
  schema:
    fields:
      - {name: "id", type: "string"}
      - {name: "text", type: "string"}
      - {name: "p_date", type: "string"}
      - {name: "site_id", type: "int64"}
  magnus_conf:
    concurrency: 8
    ray_remote_args:
      num_cpus: 1
    write_options:
      write.format.default: lance
      magnus.ray.write.disable_repartition: "true"
      magnus.ray.write.disable_sort: "true"
```

适用场景：

- 内部 byted-ray 直接读 Hive。
- 结果写到 Magnus，底层格式使用 Lance。
- 覆盖分区时应同时配置 `partition_columns` 和 `partition_values`，并确保数据中分区列存在。

### 案例 6：TQS client_result 小样本读取，写入 Magnus

来源：`demos/bytedance/process_landing_page_on_ray/configs/preloads_demo_tqs_100.yaml`

```yaml
project_name: tqs-client-result-to-magnus
executor_type: ray
ray_address: auto

dataset:
  configs:
    - type: remote
      source: tqs
      read_mode: client_result
      max_result_rows: 1000
      tqs_app_id: <tqs_app_id>
      tqs_app_key: <tqs_app_key>
      user_name: <user_name>
      tqs_cluster: cn
      tqs_timeout: 120
      query: |
        SELECT
          CAST(p_date AS STRING) AS p_date,
          CAST(id AS BIGINT) AS id,
          CAST(text AS STRING) AS text
        FROM <db.table>
        WHERE p_date = '<partition_date>'

export:
  target: magnus
  table_name: <catalog.db.output_table>
  operation: OVERWRITE
  partition_columns: ["p_date"]
  partition_values:
    p_date: <partition_date>
  schema:
    fields:
      - {name: "id", type: "int64"}
      - {name: "text", type: "string"}
      - {name: "p_date", type: "string"}
  magnus_conf:
    write_options:
      write.format.default: lance
```

适用场景：

- 只需要小批量查询结果，不想先物化到 HDFS。
- `max_result_rows` 控制 client_result 读取上限。
- 大结果集更适合 `read_mode: materialized` 并配置 `output_uri`。

### 案例 7：本地 JSONL 创建/覆盖 Magnus Lance 表

来源：`demos/bytedance/process_magnus/configs/lance_create_smoke.yaml`

```yaml
project_name: magnus-lance-create-smoke
executor_type: ray
ray_address: auto
np: 1
work_dir: ./outputs/magnus_lance_create_smoke

dataset:
  configs:
    - type: local
      path: ./demos/bytedance/process_magnus/data/lance_create_smoke.jsonl

export:
  target: magnus
  table_name: <catalog.db.output_table>
  operation: OVERWRITE
  schema:
    fields:
      - {name: "id", type: "int64"}
      - {name: "text", type: "string"}
      - {name: "score", type: "double"}
  magnus_conf:
    concurrency: 1
    ray_remote_args:
      num_cpus: 1
    write_options:
      write.format.default: lance
      magnus.ray.write.disable_repartition: "true"
      magnus.ray.write.disable_sort: "true"

process: []
```

适用场景：

- 用本地小数据做 Magnus 写入 smoke test。
- 通过显式 `schema` 固定建表/写入字段。
- 已存在但格式不符合预期的表应失败，而不是被静默覆盖成另一种物理格式。

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
