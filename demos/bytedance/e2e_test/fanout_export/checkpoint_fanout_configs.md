# Ray File Fan-out Checkpoint Configs

本文档说明本目录新增的 Ray fan-out append checkpoint E2E 配置：

- `13_parquet_append_checkpoint.yaml`
- `14_jsonl_append_checkpoint_delete_no_checkpoint_files.yaml`

这两个配置用于验证 `export.targets` 在 Ray HDFS fan-out append 场景下可以开启 `ray_data_checkpoint.enabled: true`。同一套 fan-out 配置入口也支持 `target: local`，但 online E2E 使用 HDFS，避免 Ray worker 依赖某台机器的本地目录。语义是 at-least-once：失败重试或重新提交时，Ray Data checkpoint 可以跳过已 checkpoint 的输入行；但如果某些目标目录已经写入 part 文件，而 checkpoint 元数据尚未保存，后续重试仍可能产生重复 part 文件。

## 共同约束

两个配置都满足以下条件：

- `executor_type: ray`
- 输入来自 HDFS parquet，避免 online Ray worker 依赖 repo-local 文件。
- `ray_data_checkpoint.enabled: true`
- `export.targets` 只写 HDFS；local fan-out 也可配置，但 local path 必须对所有 Ray worker 可见。
- 所有 `export.targets[].type` 相同。
- 所有 `export.targets[].mode` 都显式设置为 `append`。
- 每个 target 的 `path` 都包含 `{job_id}`，避免不同 E2E run 互相污染。

启用 checkpoint 的 fan-out 配置必须显式写 `mode: append`。不写 `mode` 会按默认 `error_if_exists` 处理，并在配置校验阶段被拒绝。

## export.targets 参数说明

`export.targets` 是一个非空列表，每个元素表示一个 fan-out 输出目标。当前 checkpoint fan-out E2E 中每个 target 都使用下面这些字段：

| 字段 | 必填 | 当前取值 | 说明 |
| --- | --- | --- | --- |
| `target` | 是 | `hdfs` | 输出目标类型。fan-out 支持 `hdfs` 和 `local`，但同一个 `targets` 列表中的所有 target 必须使用相同 `target`。本 E2E 使用 HDFS。 |
| `type` | 是 | `parquet` 或 `jsonl` | 输出文件格式。fan-out 第一版只支持 `parquet` 和 `jsonl`，且同一个 `export.targets` 列表里的所有 target 必须使用相同 `type`。 |
| `path` | 是 | `hdfs://haruna/.../{job_id}/...` | 目标输出目录。`target: hdfs` 时必须以 `hdfs://` 开头；`target: local` 时可使用相对路径、绝对路径或 `file://`，但目录必须对所有 Ray worker 可见。path 必须是目录路径，不能是 `*.parquet`、`*.jsonl` 这类文件路径。所有 target 的 path 必须互不相同。本 E2E 使用 `{job_id}` 隔离每次运行的输出。 |
| `filesystem` | 否 | `pyarrow` | HDFS filesystem 实现。仅 `target: hdfs` 使用。线上 Ray E2E 使用 `pyarrow`，使 Ray worker 通过 PyArrow HDFS filesystem 直接写入 HDFS。`target: local` 使用 PyArrow `LocalFileSystem`，不需要配置该字段。 |
| `webhdfs` | 否 | 未配置 | 仅在 `filesystem: webhdfs` 的本地或测试场景使用，传给 fsspec/WebHDFS 的连接参数，例如 `host`、`port`、`user`。这两个 online E2E 不使用。 |
| `mode` | 否，但 checkpoint fan-out 必须显式写 | `append` | 写入模式。fan-out 通用支持 `error_if_exists`、`overwrite`、`append`，但启用 `ray_data_checkpoint.enabled: true` 时只允许所有 target 都显式 `append`。`append` 不清理已有目录，会追加新的 part 文件，因此重试或重跑可能产生重复 part。 |
| `filter_condition` | 否 | `id != ''` 或 `source != ''` | 行级过滤表达式，语法复用 `general_field_filter` 的字段比较表达式。一个输入行可以命中多个 target；任一 target 写失败都会让整个 sink action 失败。空字符串或不配置表示该 target 接收所有行。 |
| `extra_args` | 否 | 见下文 | 传给 fan-out datasink 的 target 级额外参数。custom fan-out datasink 不等同于 Ray 内置 file writer，只有被当前写出实现识别的参数才会生效。 |

`extra_args` 在这两个配置中使用的字段如下：

| 字段 | 适用格式 | 当前取值 | 说明 |
| --- | --- | --- | --- |
| `min_rows_per_file` | `parquet` | `2` | 当前 custom fan-out parquet 写出直接调用 `pyarrow.parquet.write_table`，该字段不会控制 fan-out part 文件切分；实际 part 数由 Ray task/block 和 fan-out datasink 文件命名决定。 |
| `num_rows_per_file` | `jsonl` | `2` | 当前 custom fan-out jsonl 写出不会按该字段拆分文件；实际 part 数由 Ray task/block 和 fan-out datasink 文件命名决定。 |
| `concurrency` | `parquet` / `jsonl` | `2` | 该字段放在 target 内时不是 sink action 并发参数，当前 custom fan-out datasink 不使用它控制并发。需要控制 `dataset.write_datasink(...)` 并发时，应配置顶层 `export.extra_args.concurrency`。 |
| `ray_remote_args` | `parquet` / `jsonl` | 未配置 | 可选 Ray sink action 资源参数。需要生效时应配置顶层 `export.extra_args.ray_remote_args`，而不是 target 内的 `extra_args`。 |

当前实现中，target 级 `extra_args` 的有效范围是格式写出函数实际识别的参数：parquet 会过滤并传递 `pyarrow.parquet.write_table(...)` 支持的参数；jsonl 只使用 `force_ascii` / `ensure_ascii` 控制 JSON 编码。`concurrency` 和 `ray_remote_args` 是 sink action 级参数，应放在顶层 `export.extra_args`。

## 13_parquet_append_checkpoint.yaml

用途：验证 parquet fan-out append sink 与 Ray Data checkpoint 同时开启。

关键配置：

```yaml
ray_data_checkpoint:
  enabled: true
  dir: "hdfs://haruna/.../fanout_export/{job_id}/checkpoints/parquet"
  delete_no_checkpoint_files: false

export:
  targets:
    - type: "parquet"
      mode: "append"
      filter_condition: "id != ''"
    - type: "parquet"
      mode: "append"
      filter_condition: "source != ''"
```

预期输出：

- `.../{job_id}/parquet_checkpoint/high_id/`
- `.../{job_id}/parquet_checkpoint/from_source/`
- `.../{job_id}/checkpoints/parquet/`

## 14_jsonl_append_checkpoint_delete_no_checkpoint_files.yaml

用途：验证 jsonl fan-out append sink 与 Ray Data checkpoint 同时开启，并覆盖 `delete_no_checkpoint_files: true` 的配置入口。

关键配置：

```yaml
ray_data_checkpoint:
  enabled: true
  dir: "hdfs://haruna/.../fanout_export/{job_id}/checkpoints/jsonl"
  delete_no_checkpoint_files: true

export:
  targets:
    - type: "jsonl"
      mode: "append"
      filter_condition: "id != ''"
    - type: "jsonl"
      mode: "append"
      filter_condition: "source != ''"
```

`delete_no_checkpoint_files: true` 在 fan-out 场景下只表示把 Ray Data 的对应开关传给 checkpoint context；它不会把 fan-out 输出升级成 exactly-once，也不会对多个目标目录提供原子清理或原子可见性。当前 fan-out sink 是 custom `Datasink`，不是 `_FileDatasink` 事务路径。

预期输出：

- `.../{job_id}/jsonl_checkpoint/non_empty_id/`
- `.../{job_id}/jsonl_checkpoint/from_source/`
- `.../{job_id}/checkpoints/jsonl/`

## Online E2E 命令

从 repo 根目录执行，`--username` 需要和 `bytedcli --json auth status` 中的 ByteCloud 登录用户一致。

```bash
python3 demos/bytedance/e2e_test/online_ray_job.py launch \
  --username <username> \
  --config demos/bytedance/e2e_test/fanout_export/13_parquet_append_checkpoint.yaml \
  --allow-placeholder-api-key \
  --worker-num 2 \
  --job-id fanout_parquet_checkpoint_$(date +%Y%m%d_%H%M) \
  --out-dir /tmp/data_juicer_e2e/fanout_parquet_checkpoint
```

```bash
python3 demos/bytedance/e2e_test/online_ray_job.py launch \
  --username <username> \
  --config demos/bytedance/e2e_test/fanout_export/14_jsonl_append_checkpoint_delete_no_checkpoint_files.yaml \
  --allow-placeholder-api-key \
  --worker-num 2 \
  --job-id fanout_jsonl_checkpoint_$(date +%Y%m%d_%H%M) \
  --out-dir /tmp/data_juicer_e2e/fanout_jsonl_checkpoint
```

任务完成或确认输出后必须 stop one-off Federal job：

```bash
python3 demos/bytedance/e2e_test/online_ray_job.py stop \
  --username <username> \
  --run-dir /tmp/data_juicer_e2e/fanout_parquet_checkpoint
```

```bash
python3 demos/bytedance/e2e_test/online_ray_job.py stop \
  --username <username> \
  --run-dir /tmp/data_juicer_e2e/fanout_jsonl_checkpoint
```

## HDFS 验证点

使用 `ad.ai.data_forge.ExecuteHdfsCommand` 执行只读 `hdfs dfs -ls`，分别检查：

- 每个 fan-out target 目录至少有一个 `part-*` 文件。
- checkpoint 目录包含 `_metadata` 和 `checkpoint-*` 文件。
- stop 后 Federal job 状态为 `STOPPED`。

示例命令体：

```json
{
  "command_line": "hdfs dfs -ls hdfs://haruna/ad_base/addrd_core/addrd_stats/data_juicer/e2e/fanout_export/<job_id>/jsonl_checkpoint/non_empty_id",
  "user_context": {
    "username": "<username>",
    "user_role": "",
    "user_email": "<email>"
  },
  "base": {
    "log_id": "",
    "caller": "",
    "addr": "",
    "client": "",
    "traffic_env": {"open": false, "env": ""},
    "extra": {}
  }
}
```
