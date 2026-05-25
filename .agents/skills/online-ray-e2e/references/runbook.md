# Online Ray E2E Reference

Use this reference only after the `online-ray-e2e` skill triggers and exact commands are needed.

## Submit With Helper

Run from the Data-Juicer repo root:

```bash
ARK_API_KEY="<ark-api-key>" \
PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py launch \
  --username "<your-username>" \
  --model "<model-endpoint>" \
  --worker-num 10
```

The helper defaults to:

- config: `demos/bytedance/e2e_test/e2e_test.yaml`
- image: `hub.byted.org/ad_stats/data_juicer:ea784a3ddfc181e1c6b1dc717f3250b4`
- repo mount: `/opt/tiger/data-juicer`
- branch: current Git branch
- YARN project: `paubxt82r1tu`
- YARN cluster: `rabbit-hl`
- output dir: `/tmp/data_juicer_e2e/<job_id>`

Input data rule:

- Online Ray E2E is distributed execution. Do not use repository-local sample files such as `demos/.../*.jsonl` as the real input source, because workers may not have those files in the runtime working directory even when the driver can see the repository.
- Prefer HDFS-backed sample data for online E2E. If the path is from production or shared test data, probe existence and size first with `ExecuteHdfsCommand`.
- Repo-local data is still acceptable for local dry-run or local single-process smoke checks, but it is not a reliable online Ray E2E data source.

Useful variants:

```bash
# Generate a request without real submission.
PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py launch \
  --username "<your-username>" \
  --allow-placeholder-api-key \
  --dry-run

# Use a fixed job id.
ARK_API_KEY="<ark-api-key>" \
PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py launch \
  --username "<your-username>" \
  --model "<model-endpoint>" \
  --job-id "<job-id>" \
  --worker-num 10

# Add Arnold only for GPU tasks.
ARK_API_KEY="<ark-api-key>" \
PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py launch \
  --username "<your-username>" \
  --model "<model-endpoint>" \
  --worker-num 10 \
  --with-arnold
```

## Status, Ray UI, Stop

Use the helper run directory when available:

```bash
PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py status \
  --username "<your-username>" \
  --run-dir /tmp/data_juicer_e2e/<job_id>

PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py ray-ui \
  --username "<your-username>" \
  --run-dir /tmp/data_juicer_e2e/<job_id>

PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py stop \
  --username "<your-username>" \
  --run-dir /tmp/data_juicer_e2e/<job_id>
```

Raw RPC defaults:

```bash
bytedcli --json bits rpc-call ad.ai.data_forge <MethodName> \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file request.json
```

Methods:

- `LaunchMerlinFederalJob`
- `GetMerlinFederalJob`
- `GetMerlinFederalJobRayUI`
- `OperateMerlinFederalJob` with `"action": "Stop"`
- `ExecuteHdfsCommand`

## Ray History And Driver Logs

If the History Server API returns `Select a logfile first`, or if `/history/<cluster>/api/jobs` returns an empty list while Federal/Ray UI suggests the job is running or finished, open the Ray UI URL once and capture the selected History key. The page may redirect from:

```text
https://ray-history-server.byted.org/#/new/history/<cluster>/overview
```

to:

```text
https://ray-history-server.byted.org/#/new/history/<cluster>:<log_suffix>/overview
```

Use the suffixed key for API calls:

```bash
curl -s 'https://ray-history-server.byted.org/history/<cluster>:<log_suffix>/api/jobs' | jq
curl -s 'https://ray-history-server.byted.org/history/<cluster>:<log_suffix>/api/jobs/<ray_job_id>' | jq
curl -s 'https://ray-history-server.byted.org/history/<cluster>:<log_suffix>/api/data/datasets' | jq
curl -s 'https://ray-history-server.byted.org/history/<cluster>:<log_suffix>/logical/actors' | jq
```

Do not treat an empty unsuffixed Jobs response as proof that the driver has not started. The unsuffixed key can return `[]` even when the suffixed key has a completed job, for example:

```text
unsuffixed API: /history/j-...-hl-rabbit/api/jobs -> []
suffixed API:   /history/j-...-hl-rabbit:20260525120725-.../api/jobs -> [{status: "SUCCEEDED", ...}]
```

Driver log priority:

1. Ray UI `Jobs -> <ray_job_id> -> stdout/stderr`.
2. Job API driver log fields or log proxy URL, saved locally under `/tmp`.
3. Failed task or worker logs only after driver logs are insufficient.

Search downloaded logs for the first useful failure:

```bash
rg -n "Traceback|Error|Exception|FAILED|CRITICAL" /tmp/driver.log | head -n 40
```

Ray driver terminal state is more authoritative than Federal status when Federal status still shows `RUNNING`.

## Local Validation

Use current source imports:

```bash
PYTHONPATH="$PWD" RAY_ENABLE_UV_RUN_RUNTIME_ENV=0 \
  ./.venv/bin/python tools/process_data.py \
  --config demos/bytedance/e2e_test/e2e_test.yaml \
  --ray_address local \
  --ray_dry_run_plan True \
  --job_id e2e_schema_fix_dry_run
```

`ray_dry_run_plan=True` validates config parsing, operator loading, Ray plan construction, and export schema parsing. It does not validate real HDFS reads, OCR/VLM calls, or Magnus writes.

For HDFS loader or parquet schema changes, prefer the repo's shared local HDFS flow in `docs/AgentRunbooks.md#mac-hdfs-e2e-testing`. Quick health check:

```bash
docker ps --filter name=dj-arm-hdfs --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
curl -sS 'http://localhost:9870/webhdfs/v1/?op=GETFILESTATUS&user.name=root'
```

On Mac, prefer WebHDFS to avoid local `libhdfs.dylib` issues:

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
assert len(rows) == 3
ray.shutdown()
PY
```

## ExecuteHdfsCommand

Use `ai_data_forge.ExecuteHdfsCommand` for production HDFS metadata or small sample checks. The service accepts exactly:

```text
hdfs dfs <read_command> <hdfs_uri>
```

Allowed commands: `-ls`, `-cat`, `-get`, `-tail`. The path must be an `hdfs://` URI and cannot contain glob, shell expansion, query, or fragment characters.

Request template:

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

Fields to inspect: `status_code`, `status_message`, `command`, `output_encoding`, `bytes_read`, `output`.

## Resubmit Loop

After a failure:

1. Record `sid`, Data-Juicer `job_id`, branch, commit, image, entrypoint, resources, first useful traceback, and next change.
2. Fix one owning layer only unless logs prove multiple independent blockers.
3. Validate locally with Ray + HDFS or dry-run plan.
4. Push committed code/config when needed.
5. Resubmit with `online_ray_job.py launch`.
6. Monitor Ray driver terminal state and driver logs.
7. Verify export rows, partition, or table output before calling the E2E successful.

Success requires Federal success, Ray driver success, no failed Ray Data stage, Data-Juicer completion, and expected exported output. If a layer cannot be checked, state the blocker and the deepest verified layer.
