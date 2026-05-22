# 线上 Ray E2E 提交与定位 Runbook

本文档记录 Data-Juicer 线上 Ray 任务的闭环：

```text
提交 Ray 任务 -> 查看任务状态 -> 任务失败 -> 定位失败层 -> 修复代码或配置 -> 重新提交 -> 直到成功
```

提交入口使用 BITS RPC 调用 `ad.ai.data_forge` 的 `LaunchMerlinFederalJob`。接口结构以 `../ai_data_forge/idl/ai_data_forge.thrift` 为准。

## 固定 RPC 参数

默认使用下面这组 BITS 调用参数：

```bash
PSM=ad.ai.data_forge
IDL_VERSION=codex/use-python-311
ENV=ppe_terranova
IDC=hl
CLUSTER=default
```

命令模板：

```bash
bytedcli --json bits rpc-call "$PSM" <MethodName> \
  --idl-version "$IDL_VERSION" \
  --idl-source branch \
  --zone CN \
  --idc "$IDC" \
  --env "$ENV" \
  --cluster "$CLUSTER" \
  --body-file request.json
```

如果本地 CLI 要求填写控制面，使用 BITS 页面里的 `China-online`。

## 第 0 步：确认代码、镜像和入口

提交前先确认这些字段：

- `job_def_version.git_repo.repo_name`：通常是 `ad/data-juicer`。
- `job_def_version.git_repo.branch_name`：Merlin 挂载的分支，例如 `dev/local-20260514-162151`。
- `job_def_version.git_repo.use_latest_commit`：迭代分支 head 时保持 `true`。
- `job_def_version.image_meta.image_url`：运行镜像，例如 `hub.byted.org/ad_stats/data_juicer:<tag>`。
- `job_def_version.entrypoint_full_script`：真实执行的 Data-Juicer 命令。

线上 Ray E2E 是分布式执行，不要把仓库内样例文件作为真实输入数据，例如 `demos/.../*.jsonl`。driver 能看到当前仓库，不代表 worker 的 runtime working directory 一定有同一份文件；这类配置容易在线上失败为 `FileNotFoundError`。线上 E2E 尽可能使用 HDFS 中的小样本数据，提交前可用 `ExecuteHdfsCommand` 确认路径存在和文件大小。仓库本地样例只适合本地 dry-run 或本地单机 smoke test。

调试时建议显式指定 `job_id`，方便把 Ray UI、Data-Juicer 日志和导出路径串起来：

```bash
python tools/process_data.py \
  --config demos/bytedance/process_landing_page_on_ray/configs/preloads_demo.yaml \
  --job_id online_e2e_20260518_000000
```

如果只是临时改 YAML，不想提交到分支，可以在 RPC 请求里传 `operator_yaml`。服务会把入口替换为：

```bash
python tools/process_data_base64.py --config-base64 <encoded-yaml>
```

请求 JSON 只作为本地临时文件保存，不要把包含真实 token、密钥或临时业务配置的请求文件提交到仓库。

如果 YAML 里包含 `api_key`、token 或其他敏感字段，不要在群聊、issue、PR 或最终报告里粘贴完整 `operator_yaml`、base64 entrypoint、Ray job metadata 或签名日志 URL。Ray History 的 job entrypoint、stdout/stderr 和 log proxy 链接都可能间接包含提交请求里的敏感内容。排查时只把原始内容落到本机 `/tmp`，对外只摘录非敏感字段和错误栈。

## 第 1 步：提交 CPU Ray 任务

无 GPU 的任务只保留 `YARN` resource。一般只需要改：

- `caption`
- `job_def_version.git_repo.branch_name`
- `job_def_version.entrypoint_full_script`
- `resources[0].yarn_config.roles` 里的 worker 数量

请求示例：

```bash
cat >/tmp/dj_ray_launch.json <<'JSON'
{
  "namespace_name": "/topic/790e3ece1131c882",
  "caption": "landing_page_online_e2e",
  "description": "",
  "tags": [],
  "type": "RayCluster",
  "sub_type": "SingleJob",
  "job_def_version": {
    "image_meta": {
      "image_sid": "",
      "image_vid": "",
      "image_source": "url",
      "image_url": "hub.byted.org/ad_stats/data_juicer:ea784a3ddfc181e1c6b1dc717f3250b4",
      "task_id": 0,
      "need_build": false
    },
    "git_repo": {
      "repo_name": "ad/data-juicer",
      "branch_name": "dev/local-20260514-162151",
      "tag": "",
      "mnt": "/opt/tiger/data-juicer",
      "commit_sha": "",
      "use_latest_commit": true
    },
    "env": {
      "BYTED_RAY_ray_io_dont_shutdown_cluster_after_job_finished": "true",
      "BYTED_RAY_ray_io_param_head_no_cpu": "true",
      "RAY_max_lineage_bytes": "5368709120",
      "RAY_memory_monitor_refresh_ms": "0"
    },
    "entrypoint_full_script": "python tools/process_data.py --config demos/bytedance/process_landing_page_on_ray/configs/preloads_demo.yaml --job_id online_e2e_20260518_000000"
  },
  "resources": [
    {
      "backend": "YARN",
      "yarn_config": {
        "queue_name": "root.panda_hl_ad_stats_general_h",
        "cluster_name": "rabbit-hl",
        "idc": "hl",
        "project_id": "paubxt82r1tu",
        "roles": [
          {
            "name": "worker",
            "num": 100,
            "memory": 32768,
            "gpu": 0,
            "cpu": 4
          },
          {
            "name": "head",
            "num": 1,
            "memory": 65536,
            "gpu": 0,
            "cpu": 8
          }
        ]
      }
    }
  ],
  "user_context": {
    "username": "<your-username>",
    "user_role": "",
    "user_email": "<your-email>"
  },
  "base": {
    "log_id": "",
    "caller": "",
    "addr": "",
    "client": "",
    "traffic_env": {
      "open": false,
      "env": ""
    },
    "extra": {}
  }
}
JSON
```

提交：

```bash
bytedcli --json bits rpc-call ad.ai.data_forge LaunchMerlinFederalJob \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_ray_launch.json | tee /tmp/dj_ray_launch.response.json
```

记录返回的 `federal_job.merlin_federal_job_sid`。如果不同版本 CLI 的 JSON 外层结构不同，可以用递归提取：

```bash
SID=$(jq -r '.. | objects | .merlin_federal_job_sid? // empty' /tmp/dj_ray_launch.response.json | head -n1)
echo "$SID"
```

## 推荐脚本提交方式

手写 RPC JSON 适合看清完整请求结构。日常 E2E 更推荐使用仓库里的 helper：

```bash
ARK_API_KEY="<ark-api-key>" \
PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py launch \
  --username "<your-username>" \
  --model "<model-endpoint>" \
  --worker-num 10
```

脚本默认会：

- 使用 `demos/bytedance/e2e_test/e2e_test.yaml`；
- 用 `operator_yaml` 发送本地 YAML，适合验证未提交或刚修改的配置；
- 注入本次 `job_id` 和 `work_dir`；
- 把请求和响应归档到 `/tmp/data_juicer_e2e/<job_id>`；
- 默认在归档请求里隐藏 YAML 内的 `api_key`。

常用命令：

```bash
# 只生成请求，不真实提交。
PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py launch \
  --username "<your-username>" \
  --allow-placeholder-api-key \
  --dry-run

# 查看 Federal Job 高层状态。
PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py status \
  --username "<your-username>" \
  --run-dir /tmp/data_juicer_e2e/<job_id>

# 获取 Ray UI URL。
PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py ray-ui \
  --username "<your-username>" \
  --run-dir /tmp/data_juicer_e2e/<job_id>

# 停止错误任务。
PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py stop \
  --username "<your-username>" \
  --run-dir /tmp/data_juicer_e2e/<job_id>
```

如果需要 GPU，在 `launch` 上加 `--with-arnold`。无 GPU 任务不要加 Arnold resource。

## 单样本算子 Debug

当只想用一条样本排查线上 Ray 算子行为时，使用独立 debug 入口，不要复用正常 `tools/process_data.py` 的业务 export 流程：

```bash
PYTHONPATH="$PWD" python tools/debug_operator_pipeline.py \
  --config demos/bytedance/operator_debug/single_sample_debug.yaml
```

线上 `operator_yaml` / base64 entrypoint 场景使用：

```bash
PYTHONPATH="$PWD" python tools/debug_operator_pipeline_base64.py \
  --config-base64 "$CONFIG_B64"
```

最小配置形态：

```yaml
executor_type: ray
ray_address: auto
job_id: debug_demo
export_path: ./outputs/debug_demo/unused.jsonl

debug:
  enabled: true
  sample_json: '{"text":"hello","sample_id":"demo-1"}'
  output:
    path: hdfs://haruna/tmp/data-juicer-debug/{job_id}/{debug_run_id}/trace.jsonl
    type: jsonl
  bytes_output:
    mode: summary      # summary | full_base64
    preview_bytes: 64

process:
  - text_length_filter:
      min_len: 1
```

输出是单个 JSONL 文件，事件顺序固定：

```text
input -> op_step -> op_step -> ... -> summary
```

每条 `op_step` 会记录算子下标、算子名、算子类型、脱敏后的 `op_config`、状态、耗时、时间戳、`row_count`、schema、执行后 `data` 快照，以及顶层冗余的 `stats` / `meta`。不做 diff；如果需要对比变化，按相邻 step 的 `data` 快照比较。

二进制输入不能直接写 raw bytes。需要显式 wrapper，并在 `decode_fields` 中声明顶层字段：

```yaml
debug:
  sample:
    text: "image sample"
    image_bytes:
      __dj_bytes__:
        encoding: base64
        data: "/9j/4AAQSkZJRgABAQ..."
  decode_fields:
    image_bytes: bytes
```

二进制输出默认只写摘要，避免 debug JSONL 过大：

```json
{"__dj_bytes_summary__":{"length":123,"sha256":"...","preview_base64":"...","truncated":true}}
```

如果确实需要可还原 bytes，设置：

```yaml
debug:
  bytes_output:
    mode: full_base64
```

限制和语义：

- 第一版只支持 `executor_type: ray`。
- 这是 sequential single-sample debug：逐个算子执行，每步触发一次 Ray materialize/take 快照；不复现 DAG、op fusion、partitioned Ray 的调度形态。
- `start_index` / `end_index` 可按 `process` 的 0-based 下标只跑一段算子链，`end_index` 为包含式。
- Filter 过滤掉这条样本时，会写 `dropped: true`，停止后续算子，`summary.status=dropped`。
- debug 工具会对执行的算子强制 `skip_op_error=false`，避免算子异常被样本级容错吞掉。
- 算子失败时，会写 failed `op_step` 和 failed `summary`，并尽量上传 debug JSONL；工具进程仍正常退出，便于常驻集群把诊断运行视为成功产出结果。
- 只有 `debug.output.path` 缺失或非法、无法交付诊断产物这类错误会让工具非 0 退出。
- Deduplicator / Pipeline 会尝试执行，但 `op_step.single_sample_semantics=true`；单样本结果不代表全量数据语义。
- debug 工具永远不执行原 pipeline 的业务 export，只写 `debug.output`。
- `debug.output.path` 支持 `{job_id}`、`{debug_run_id}`、`{timestamp}` 模板。默认覆盖最终路径；保留多次运行结果时推荐路径包含 `{debug_run_id}`。

## GPU 资源写法

只有需要 GPU 时才加 `ARNOLD` resource。除非切换队列或 quota，其他 Arnold 参数通常不改。

```json
{
  "backend": "ARNOLD",
  "arnold_config": {
    "group_ids": [955],
    "cluster_id": 17,
    "quota_pool": "default",
    "roles": [
      {
        "name": "worker",
        "num": 1,
        "memory": 45056,
        "gpu": 1,
        "gpuv": "A100_SXM_80GB",
        "queue_name": "compute-1190-lq-cloudnative-ai-life.alg.genai-guarantee",
        "scheduling_options": "{}",
        "cpu": 11,
        "ports": 10,
        "preemptible": false,
        "resource_pool": ""
      }
    ]
  }
}
```

## 第 2 步：查看 Federal Job 状态

用提交返回的 SID 调 `GetMerlinFederalJob`：

```bash
cat >/tmp/dj_ray_get.json <<JSON
{
  "merlin_federal_job_sid": "$SID",
  "user_context": {
    "username": "<your-username>",
    "user_role": "",
    "user_email": "<your-email>"
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
JSON

bytedcli --json bits rpc-call ad.ai.data_forge GetMerlinFederalJob \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_ray_get.json | tee /tmp/dj_ray_get.response.json
```

重点看：

- `federal_job.status`：Merlin/Federal Job 的高层状态。
- `federal_job.detail_json`：Merlin 侧详情。排查队列、镜像、代码分支、运行时问题时先解析它。
- `status_code` 和 `status_message`：RPC 本身是否通过校验。

`GetMerlinFederalJob` 是高层状态，可能比 Ray driver 终态滞后。只要 Ray UI Jobs 里 driver 已经进入 `FAILED`、`SUCCEEDED` 或 `STOPPED`，定位时以 Ray job 终态和 driver log 为准，不要因为 Federal Job 仍显示 `RUNNING` 就继续等待。

## 第 3 步：打开 Ray UI

查询 Ray UI URL：

```bash
bytedcli --json bits rpc-call ad.ai.data_forge GetMerlinFederalJobRayUI \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_ray_get.json | tee /tmp/dj_ray_ui.response.json
```

打开响应里的 `ray_ui.url`。如果 History Server API 返回 `Select a logfile first`，先在浏览器打开一次 Ray UI。页面可能会把 URL 从：

```text
https://ray-history-server.byted.org/#/new/history/<cluster>/overview
```

重定向到带日志后缀的形式：

```text
https://ray-history-server.byted.org/#/new/history/<cluster>:<log_suffix>/overview
```

后续 API 调用要使用带后缀的 key：

```text
https://ray-history-server.byted.org/history/<cluster>:<log_suffix>
```

建议按这个顺序看：

1. Jobs：确认 driver 是否创建，先看 driver stdout/stderr。
2. Ray Data：确认失败的 dataset、stage、operator，是卡在 read、map、shuffle、write 还是 schema discovery。
3. Logs：先看 driver log，再看失败 task/actor 对应的 worker log。
4. Cluster/resources：确认是否是资源压力、worker crash、镜像启动、依赖安装或用户代码错误。

Ray job 信息可以直接查 History Server API：

```bash
curl -s 'https://ray-history-server.byted.org/history/<cluster>:<log_suffix>/api/jobs/<ray_job_id>' | jq
```

driver 日志优先顺序：

1. Ray UI `Jobs -> <ray_job_id> -> stdout/stderr`。
2. job API 返回的 driver log 字段或 log proxy 链接，仅保存到本机临时文件，不要粘贴签名 URL。
3. 如果 driver log 没有完整 traceback，再从失败 stage 的 worker/task log 继续追。

下载到本机后先查第一个明确异常，而不是只看文件尾部：

```bash
rg -n "Traceback|Error|Exception|FAILED|CRITICAL" /tmp/driver.log | head -n 40
```

Ray Data 失败时，driver log 中通常会打印完整 physical plan。先记录失败 operator 链，再看 traceback 里最靠近用户代码或 Data-Juicer helper 的异常。Ray 自身的反序列化、debugger 或 pickling 报错可能只是原始异常之后的二次错误。

## HDFS 元数据和样例数据探查

需要确认线上输入/输出路径是否存在、文件大小是否符合预期，或读取文本文件头尾时，可以通过 `ai_data_forge.ExecuteHdfsCommand` 调 HDFS 只读命令。当前接口定义见 `../ai_data_forge/idl/ai_data_forge.thrift`，只接受：

```text
hdfs dfs <read_command> <hdfs_uri>
```

其中 `<read_command>` 只能是 `-ls`、`-cat`、`-get`、`-tail`，路径必须是 `hdfs://` URI，不能带 glob、shell 展开、query 或 fragment。

请求示例：

```bash
cat >/tmp/dj_hdfs_cmd.json <<'JSON'
{
  "command_line": "hdfs dfs -ls hdfs://haruna/path/to/file-or-dir",
  "user_context": {
    "username": "<your-username>",
    "user_role": "",
    "user_email": "<your-email>"
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
JSON

bytedcli --json bits rpc-call ad.ai.data_forge ExecuteHdfsCommand \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_hdfs_cmd.json | tee /tmp/dj_hdfs_cmd.response.json
```

重点看：

- `status_code` / `status_message`：命令是否通过服务端校验和执行。
- `command`：服务实际执行的规范化命令。
- `output_encoding`：`utf-8` 表示可直接读；`base64` 表示输出是二进制或非 UTF-8 内容。
- `bytes_read`：本次返回的输出字节数。

常用检查：

```bash
# 路径或文件是否存在、大小和 owner。
hdfs dfs -ls hdfs://haruna/path/to/file-or-dir

# 文本日志或小文本文件尾部。
hdfs dfs -tail hdfs://haruna/path/to/text-file

# 小文本文件内容。不要对大 parquet/orc 直接 cat。
hdfs dfs -cat hdfs://haruna/path/to/text-file

# 小文件下载后由服务返回内容；二进制会以 base64 返回。
hdfs dfs -get hdfs://haruna/path/to/small-file
```

## 本地修复验证

配置或代码修复后，先做本地验证再重提线上任务。优先级是：

1. 能跑小样本 Ray + HDFS 时，跑 Ray HDFS loader 验证；
2. 目标 pipeline 依赖线上 OCR、VLM、Magnus 或生产 HDFS 凭证时，至少跑 Ray dry-run plan；
3. 线上生产 HDFS 的文件存在性和大小，用 `ExecuteHdfsCommand` 确认。

本地命令必须从 repo root 运行，并强制使用当前工作树源码：

```bash
PYTHONPATH="$PWD" RAY_ENABLE_UV_RUN_RUNTIME_ENV=0 \
  ./.venv/bin/python tools/process_data.py \
  --config demos/bytedance/e2e_test/e2e_test.yaml \
  --ray_address local \
  --ray_dry_run_plan True \
  --job_id e2e_schema_fix_dry_run
```

`PYTHONPATH="$PWD"` 很重要。本机 `.venv` 里可能装过旧版 `data_juicer`，直接执行 `./.venv/bin/python tools/process_data.py` 时会导入旧包，导致和当前源码不一致的配置解析或运行行为。

`ray_dry_run_plan=True` 会构建 schema-only input dataset，打印 Ray Data logical/physical plan，并跳过真实 HDFS 读取、OCR/VLM 调用和 Magnus 写入。它适合验证：

- YAML 能被当前源码解析；
- operator 能通过真实 `load_ops(cfg.process, op_env_manager)` 路径加载；
- Ray Dataset 处理链能构建出预期 logical/physical plan；
- export schema 能被解析并参与 schema-only 输入构建。

它不能证明真实 HDFS 源数据可读、OCR/VLM 服务可用，或 Magnus 写入成功。

如果这次修复涉及 HDFS loader、HDFS parquet 字段类型、远端数据 schema，优先按 [Mac HDFS E2E Testing](AgentRunbooks.md#mac-hdfs-e2e-testing) 使用共享 `dj-arm-hdfs` 环境做 Ray + HDFS 验证。先确认共享 HDFS 可用：

```bash
docker ps --filter name=dj-arm-hdfs --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
curl -sS 'http://localhost:9870/webhdfs/v1/?op=GETFILESTATUS&user.name=root'
```

在 Mac 上优先用 WebHDFS，避免本地 `libhdfs.dylib` 缺失影响判断：

```bash
RAY_ENABLE_UV_RUN_RUNTIME_ENV=0 PYTHONPATH="$PWD" ./.venv/bin/python - <<'PY'
import ray

from data_juicer.config import get_default_cfg
from data_juicer.core.data.load_strategy import RayHDFSDataLoadStrategy

ray.init(address="local", num_cpus=1, include_dashboard=False, log_to_driver=False)
cfg = get_default_cfg()
cfg.text_keys = ["text"]

dataset = RayHDFSDataLoadStrategy(
    {
        "type": "remote",
        "source": "hdfs",
        "path": "hdfs://localhost:9000/datasets/demo",
        "format": "parquet",
        "filesystem": "webhdfs",
        "webhdfs": {"host": "localhost", "port": 9870, "user": "root"},
        "override_num_blocks": 1,
    },
    cfg,
).load_data()

rows = sorted(dataset.get(10), key=lambda row: row["id"])
print("rows", len(rows))
print("schema", dataset.data.schema())
print("data", rows)
assert len(rows) == 3
ray.shutdown()
PY
```

如果验证环境是 Linux 集群或线上镜像，优先使用默认 PyArrow HDFS 路径而不是 `filesystem: webhdfs`，并确认 Hadoop client、`libjvm`、HDFS 凭证在 driver 和 worker 上都可用。

## 第 4 步：定位失败层

按第一个出现明确错误的层来归因。

### RPC 校验失败

现象：`LaunchMerlinFederalJob` 返回非 0 `status_code`，且没有 `federal_job`。

检查：

- `user_context.username` 必须存在。
- `job_def_version.entrypoint_full_script` 和 `operator_yaml` 至少有一个。
- `job_def_version.env` 不能有空 key。
- resource backend 和配置要匹配：`YARN` 必须有 `yarn_config`，`ARNOLD` 必须有 `arnold_config`。

### 集群或镜像启动失败

现象：SID 已创建，但 Ray UI 不可用，或 driver 一直没有启动。

检查：

- `image_meta.image_url` 是否存在，镜像内是否有目标依赖。
- `git_repo.branch_name` 是否已推送目标代码。
- `git_repo.mnt` 是否为 `/opt/tiger/data-juicer`，并和 entrypoint 路径一致。
- YARN queue、project、worker 数、内存和 CPU 是否适配目标集群。

### Driver 失败

现象：Ray job 创建成功，但 Jobs 页面里 driver 很快失败。

优先在 driver log 里找：

- 配置解析错误；
- committed config 路径不存在；
- Python 包或运行时依赖缺失；
- Data-Juicer operator 加载失败；
- Hive/TQS/Magnus 凭证或 SDK 调用失败；
- export path/table 创建失败。

Data-Juicer 会把 config 备份到 `work_dir`，并在该目录下写 `job_summary.json`、`events_*.jsonl` 和日志。线上调试时尽量显式设置 `--job_id` 和稳定的 `work_dir`，这样可以从 driver log 反查到具体文件。

### Ray Data 执行失败

现象：driver 运行了一段时间后失败，Ray Data 页面有失败 stage。

检查：

- input stage：Hive/TQS SQL、分区条件、columns、schema cast；
- map stage：具体 operator 名称和最后的用户代码 traceback；
- shuffle/repartition stage：object store 压力、lineage size、worker crash；
- write/export stage：Magnus/HDFS/Lance SDK 参数和 completion callback 返回结构。

不要只停留在 Ray stage 名称。需要从 Ray Data 失败 stage 继续追到创建它的 Data-Juicer operator 或配置。

一次真实失败的最小记录格式：

```text
Ray job: 02000000
Ray job status: FAILED
Driver exit code: 1
Failed operator:
  MapBatches(OcrAnswerCategoryMapper.process_batched)
  -> MapBatches(add_partition_columns)
  -> MapBatches(validate_partition_values)
  -> Project
  -> MapBatches(align_batch_to_schema)
  -> WriteMagnusDataSink
First useful exception:
  pyarrow.lib.ArrowNotImplementedError:
  Unsupported cast from string to list using function cast_list
Owning layer:
  Data-Juicer export schema alignment
Likely fix:
  Make the YAML export schema match the produced field type, or add a mapper
  before export to normalize the field shape.
Secondary noise:
  Ray debugger/pickling errors after the original exception.
```

这类失败不要先调资源。先对照 YAML export schema 和 driver log 里打印的 Arrow schema，确认是数据字段类型、导出 schema，还是 exporter SDK 契约不一致。

## 第 5 步：停止错误任务

当前 RPC 契约提供 `OperateMerlinFederalJob`，已知可用 action 是 `Stop`。

```bash
cat >/tmp/dj_ray_stop.json <<JSON
{
  "merlin_federal_job_sid": "$SID",
  "action": "Stop",
  "user_context": {
    "username": "<your-username>",
    "user_role": "",
    "user_email": "<your-email>"
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
JSON

bytedcli --json bits rpc-call ad.ai.data_forge OperateMerlinFederalJob \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_ray_stop.json
```

## 第 6 步：修复并重新提交

每次失败至少记录：

- SID；
- Data-Juicer `job_id`；
- branch name 和 commit SHA，如果本次固定了 commit；
- image URL；
- entrypoint；
- resource spec；
- 第一个明确失败层和原始 traceback；
- 下一次要验证的改动。

然后只改拥有该问题的最小层：

- 配置问题：改 YAML 或 `operator_yaml`；
- Data-Juicer 代码问题：改代码，本地做针对性验证，推送分支，再用 `use_latest_commit=true` 重新提交；
- 镜像问题：换 image URL 或重建镜像；
- 资源问题：调整 worker `num`、CPU、memory，或按需增加/删除 Arnold。

除非日志已经证明存在多个独立 blocker，否则下一次提交只改变一个关键变量。

重提交流程：

1. 本地修复 YAML 或代码。
2. 跑本地验证，优先 Ray + HDFS，小样本不可行时至少跑 `ray_dry_run_plan=True`。
3. 如果修了代码或 committed config，提交并推送分支；如果只验证临时 YAML，可以继续用 helper 的 `operator_yaml` 提交。
4. 用 `online_ray_job.py launch` 重新提交，记录新的 `sid`、`job_id` 和 `/tmp/data_juicer_e2e/<job_id>`。
5. 用 `online_ray_job.py status` 和 `ray-ui` 获取状态与 Ray UI。
6. 以 Ray driver 终态和 driver stdout/stderr 为准，继续定位下一层失败或确认成功。

本次 `e2e_test.yaml` 的真实修复例子：

```yaml
export:
  schema:
    fields:
      - name: "texts"
        type: "string"
```

driver log 里的实际 Arrow schema 是 `texts: string`，原配置写成 `list<string>`，导致 Magnus export 前的 `align_batch_to_schema` 触发 `Unsupported cast from string to list`。修复后先确认 schema 解析和 operator 加载，再跑 Ray dry-run plan，最后重新提交线上任务。

## 成功判定

只有同时满足下面条件，才认为线上 E2E 成功：

- `GetMerlinFederalJob` 在 `federal_job.status` 或 `detail_json` 里显示成功终态；
- Ray UI Jobs 页面里 driver 成功结束；
- Ray Data 没有 failed stage；
- Data-Juicer `job_summary.json` 为 `completed`，或 driver log 有等价的完成信息；
- 配置的导出目标里存在预期 rows、partition 或 table 输出。

如果某一层不可访问，记录 blocker 和已经验证到的最深层，不要把任务描述为成功。
