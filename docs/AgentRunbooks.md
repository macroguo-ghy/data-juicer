# Agent Runbooks

## Online Ray E2E Submission And Debugging

Use [Online Ray E2E Submission And Debug Runbook](OnlineRayE2ERunbook.md) when submitting a Data-Juicer Ray job through `ad.ai.data_forge` `LaunchMerlinFederalJob`, checking Federal job status, opening Ray UI, and locating failures through Merlin, Ray, and Data-Juicer logs.

## Token-Efficient Ray Dashboard Inspection

Use the ray-helper scripts before pasting full Ray Dashboard, Ray History, or Godel API payloads into an agent conversation. They decode Data-Juicer `--config-base64`, redact secrets, keep only the Ray Data fields needed for diagnosis, and trim long values.

Summarize one job:

```bash
.agents/skills/ray-helper/scripts/ray_job_summary.py '<RAY_JOB_URL>' --format markdown
```

Compare two jobs:

```bash
.agents/skills/ray-helper/scripts/ray_compare_jobs.py '<OLD_RAY_JOB_URL>' '<CURRENT_RAY_JOB_URL>' --format markdown
```

If a payload has already been saved, use the compact JSON output files as inputs to `ray_compare_jobs.py` instead of re-fetching URLs.

For HDFS checkpoint metadata fetched through `ExecuteHdfsCommand`, summarize the saved response instead of printing binary/base64 output:

```bash
.agents/skills/ray-helper/scripts/hdfs_checkpoint_summary.py \
  --metadata-response /tmp/dj_hdfs_cmd.response.json \
  --format markdown
```

## Online HDFS Read-Only Inspection

Use this flow when inspecting production HDFS paths such as `hdfs://haruna/...` from a local Mac or another environment that cannot resolve the production HDFS nameservice directly. Do not rely on local `hdfs dfs` in that case; it may fail with errors such as `UnknownHostException: haruna` even when the file is valid online.

Call `ad.ai.data_forge.ExecuteHdfsCommand` through BITS so the HDFS command runs in the service environment:

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

Allowed command shape:

```text
hdfs dfs <read_command> <hdfs_uri>
```

Use only the supported read commands:

- `-ls`: existence, owner, size, and timestamp.
- `-tail`: tail of text logs or small text files.
- `-cat`: small text files only. Do not use this for parquet/orc.
- `-get`: download a file through the RPC response. Binary output is returned as base64. The service-side limit is 200 MB; do not use it for larger files.

Inspect the normalized service response:

```bash
jq -r '.data.resp_body_json | {
  status_code,
  status_message,
  output_encoding,
  bytes_read,
  command
}' /tmp/dj_hdfs_cmd.response.json
```

For a binary `-get` response, decode it explicitly:

```bash
jq -r '.data.resp_body_json.output' /tmp/dj_hdfs_cmd.response.json \
  | base64 -d > /tmp/input.parquet

python3 - <<'PY'
import pyarrow.parquet as pq

path = "/tmp/input.parquet"
pf = pq.ParquetFile(path)
print("rows", pf.metadata.num_rows)
print("row_groups", pf.num_row_groups)
print(pf.schema_arrow)
PY
```

`-get` is limited to files no larger than 200 MB. Large-but-allowed parquet files can still be too big for the default local Node heap used by `bytedcli` because the RPC response is JSON plus base64. If `-get` fails locally with `JavaScript heap out of memory`, retry with a larger heap:

```bash
NODE_OPTIONS='--max-old-space-size=12288' \
bytedcli --json bits rpc-call ad.ai.data_forge ExecuteHdfsCommand \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_hdfs_cmd.json > /tmp/dj_hdfs_cmd.response.json
```

Prefer `-ls` for large production files when you only need existence and size. For files over 200 MB, use Ray job logical plans, driver logs, or a small online sample job for schema investigation instead of `-get`.

## Local Ray E2E Testing

Use this flow when doing a small local end-to-end Ray/Data-Juicer run that reads a few rows from TQS/Hive and exports local parquet.

- Run from the repo root and force local source imports so the command uses the working tree rather than an installed `data_juicer` package:

```bash
PYTHONPATH="$PWD" RAY_ENABLE_UV_RUN_RUNTIME_ENV=0 \
  ./.venv/bin/python tools/process_data.py \
  --config /tmp/<local-e2e-config>.yaml \
  --ray_address local
```

- Prefer `--ray_address local` for Mac local debugging. In this environment, `ray start --head` can report success but leave no usable GCS listener, while `ray.init(address="local")` starts an in-process local Ray instance that works for small E2E checks.
- `ray_data_checkpoint` is available only for validated Ray file-source to sink paths. For single-target HDFS export, use Ray distributed HDFS parquet/jsonl export so the Ray Data plan stays lazy until the sink write. Single-target checkpoint validation is not tied to one `export.mode`, but cross-job recovery is: `error_if_exists` can fail on an existing partial output directory, `overwrite` drops previous progress, and `append` can preserve part files with at-least-once semantics. For `export.targets` fan-out, checkpoint is append-only: every target must explicitly set `mode: append`, and the custom fan-out datasink remains at-least-once even if `delete_no_checkpoint_files` is true.
- For TQS/Hive small-sample debugging, set the dataset config to `read_mode: "client_result"` and a small `max_result_rows`. This avoids writing TQS output to HDFS and is only appropriate for small result sets.
- If local Consul/service discovery fails with errors like `no available translator for data.olap...`, set `tqs_enable_domain: true` in the dataset config so `bytedtqs` uses the cluster domain directly.
- TQS partition checks may reject dynamic partition subqueries. Use a static partition predicate such as `p_date = 'YYYYMMDD'` for the local E2E YAML. The latest partition can be checked through the Hive catalog before the run.
- Keep local E2E YAMLs and outputs under `/tmp` unless the user asks otherwise. Do not commit real TQS credentials or generated parquet outputs.
- After the run, verify the exported parquet directly:

```bash
./.venv/bin/python - <<'PY'
import pyarrow.parquet as pq

path = "/tmp/<local-output>.parquet"
table = pq.read_table(path)
print("rows", table.num_rows)
print("columns", table.column_names)
PY
```

- Before and after local Ray experiments, check for Ray leftovers without killing unrelated processes:

```bash
ps -axo pid,ppid,stat,etime,command | grep -i '[r]ay' || true
```

Only terminate a process when its command line clearly belongs to the current test you started.

## Mac HDFS E2E Testing

Use this flow when validating HDFS parquet loading or HDFS export behavior on a Mac with Docker Desktop. Prefer the shared local HDFS container described below; do not tear it down as routine test cleanup, because multiple agents may share it for HDFS e2e validation.

### Shared Local HDFS Environment

The preferred shared environment is a single-node HDFS container:

- Container name: `dj-arm-hdfs`
- NameNode RPC: `hdfs://localhost:9000`
- WebHDFS / NameNode HTTP: `http://localhost:9870`
- DataNode HTTP: `http://localhost:9864`
- DataNode transfer / IPC: `localhost:9866` and `localhost:9867`
- Local Hadoop copy: `/Users/bytedance/tmp/dj_hadoop_libexec`
- Local Hadoop config: `/Users/bytedance/tmp/dj_mac_hdfs_conf`
- Local HDFS data dir: `/Users/bytedance/tmp/dj_mac_hdfs_data`

Do not run `docker rm -f dj-arm-hdfs` during normal cleanup. Keep per-test datasets under unique HDFS paths such as `/datasets/<test-name>-<timestamp>` or `/tmp/<test-name>-<timestamp>`, and delete only those paths when needed. Rebuild the shared container only when it is missing, unhealthy, or its mounted Hadoop files are no longer usable.

Check the shared environment before each HDFS e2e:

```bash
docker ps --filter name=dj-arm-hdfs --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
curl -sS 'http://localhost:9870/webhdfs/v1/?op=GETFILESTATUS&user.name=root'
```

Check WebHDFS write/read without relying on the container's `hdfs` CLI:

```bash
test_path="/tmp/dj_hdfs_write_check_$(date +%Y%m%d_%H%M%S)"
printf ok > /tmp/dj_hdfs_write_check_local
curl -sS -X PUT "http://localhost:9870/webhdfs/v1${test_path}?op=MKDIRS&user.name=root"
curl -sS -L -X PUT -T /tmp/dj_hdfs_write_check_local \
  "http://localhost:9870/webhdfs/v1${test_path}/ok.txt?op=CREATE&overwrite=true&user.name=root"
curl -sS -L "http://localhost:9870/webhdfs/v1${test_path}/ok.txt?op=OPEN&user.name=root"
```

On this Mac setup, WebHDFS is the reliable validation path. PyArrow's native HDFS filesystem still requires a local `libhdfs.dylib`; if `pyarrow.fs.FileSystem.from_uri("hdfs://localhost:9000/...")` fails with `Unable to load libhdfs`, treat native HDFS export/copy as blocked by local environment rather than by the shared HDFS service.

### Rebuild Only If The Shared Environment Is Invalid

Use the setup below only when the shared `dj-arm-hdfs` environment is missing or unhealthy. It starts a single-node HDFS inside Docker, writes a few parquet parts, and reads them through the Data-Juicer Ray HDFS loader.

- Prefer a native arm64 Java image on Apple Silicon. The public `apache/hadoop` image may be amd64-only; running Hadoop and Ray together under QEMU can make NameNode/DataNode or Ray workers fail for reasons unrelated to Data-Juicer.
- Keep all generated files under `/Users/bytedance/tmp` or `/tmp`. Do not commit Hadoop configs, HDFS data directories, parquet parts, or Docker scratch files.
- If Docker cannot mount `/opt/homebrew/...` directly, copy the Homebrew Hadoop libexec directory into `/Users/bytedance/tmp` first:

```bash
brew install hadoop

rm -rf /Users/bytedance/tmp/dj_hadoop_libexec
cp -R /opt/homebrew/opt/hadoop/libexec /Users/bytedance/tmp/dj_hadoop_libexec
```

- Create a minimal HDFS config for Docker. The NameNode/DataNode directories are inside the container-mounted `/hdfs` path, and WebHDFS is exposed on port `9870`:

```bash
rm -rf /Users/bytedance/tmp/dj_mac_hdfs_conf /Users/bytedance/tmp/dj_mac_hdfs_data
mkdir -p /Users/bytedance/tmp/dj_mac_hdfs_conf
mkdir -p /Users/bytedance/tmp/dj_mac_hdfs_data/name /Users/bytedance/tmp/dj_mac_hdfs_data/data
cp /Users/bytedance/tmp/dj_hadoop_libexec/etc/hadoop/*.xml \
  /Users/bytedance/tmp/dj_hadoop_libexec/etc/hadoop/log4j.properties \
  /Users/bytedance/tmp/dj_mac_hdfs_conf/

cat > /Users/bytedance/tmp/dj_mac_hdfs_conf/core-site.xml <<'XML'
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
</configuration>
XML

cat > /Users/bytedance/tmp/dj_mac_hdfs_conf/hdfs-site.xml <<'XML'
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///hdfs/name</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///hdfs/data</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
  <property><name>dfs.namenode.rpc-bind-host</name><value>0.0.0.0</value></property>
  <property><name>dfs.namenode.http-bind-host</name><value>0.0.0.0</value></property>
  <property><name>dfs.datanode.address</name><value>0.0.0.0:9866</value></property>
  <property><name>dfs.datanode.http.address</name><value>0.0.0.0:9864</value></property>
  <property><name>dfs.datanode.ipc.address</name><value>0.0.0.0:9867</value></property>
  <property><name>dfs.datanode.hostname</name><value>localhost</value></property>
  <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>
</configuration>
XML
```

- Start or rebuild the shared HDFS container. Only remove `dj-arm-hdfs` here after the health check above has shown it is invalid:

```bash
docker rm -f dj-arm-hdfs >/dev/null 2>&1 || true
docker pull eclipse-temurin:17-jdk
docker run -d --name dj-arm-hdfs \
  -p 9000:9000 -p 9870:9870 -p 9864:9864 -p 9866:9866 -p 9867:9867 \
  -v /Users/bytedance/tmp/dj_hadoop_libexec:/opt/hadoop:ro \
  -v /Users/bytedance/tmp/dj_mac_hdfs_conf:/etc/hadoop:ro \
  -v /Users/bytedance/tmp/dj_mac_hdfs_data:/hdfs \
  eclipse-temurin:17-jdk \
  bash -lc '
    export HADOOP_HOME=/opt/hadoop
    export HADOOP_CONF_DIR=/etc/hadoop
    export HADOOP_LOG_DIR=/tmp/hadoop-logs
    export HADOOP_PID_DIR=/tmp/hadoop-pids
    export PATH="$HADOOP_HOME/bin:$PATH"
    mkdir -p "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR" /hdfs/name /hdfs/data
    hdfs namenode -format -force -nonInteractive >/tmp/format.log 2>&1
    hdfs --daemon start namenode
    hdfs --daemon start datanode
    hdfs dfsadmin -safemode wait || true
    tail -F "$HADOOP_LOG_DIR"/*.log /tmp/format.log
  '
```

- Confirm HDFS health before testing Data-Juicer:

```bash
docker exec dj-arm-hdfs bash -lc '
  export HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/etc/hadoop PATH=/opt/hadoop/bin:$PATH
  jps
  hdfs dfsadmin -report | head -n 40
'
```

- Write a tiny parquet dataset into HDFS:

```bash
rm -rf /tmp/dj_hdfs_parts
mkdir -p /tmp/dj_hdfs_parts
./.venv/bin/python - <<'PY'
import pyarrow as pa
import pyarrow.parquet as pq

pq.write_table(
    pa.table({"id": [1, 2], "text": ["hello", "from hdfs"], "images": [[b"a"], [b"b", b"c"]]}),
    "/tmp/dj_hdfs_parts/part-00000.parquet",
)
pq.write_table(
    pa.table({"id": [3], "text": ["parquet"], "images": [[b"d"]]}),
    "/tmp/dj_hdfs_parts/part-00001.parquet",
)
PY

docker exec dj-arm-hdfs bash -lc 'rm -rf /tmp/dj_hdfs_parts && mkdir -p /tmp/dj_hdfs_parts'
docker cp /tmp/dj_hdfs_parts/. dj-arm-hdfs:/tmp/dj_hdfs_parts/
docker exec dj-arm-hdfs bash -lc '
  export HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/etc/hadoop PATH=/opt/hadoop/bin:$PATH
  hdfs dfs -rm -r -f /datasets/demo >/dev/null 2>&1 || true
  hdfs dfs -mkdir -p /datasets/demo
  hdfs dfs -put -f /tmp/dj_hdfs_parts/*.parquet /datasets/demo
  hdfs dfs -ls /datasets/demo
'
```

- For Mac validation, use the Ray HDFS loader with `filesystem: webhdfs`. This avoids requiring local `libhdfs.dylib`, which Homebrew Hadoop may not provide:

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
assert rows[0]["images"] == [b"a"]
assert rows[1]["images"] == [b"b", b"c"]
ray.shutdown()
PY
```

- Clean up only per-test resources. Do not remove the shared `dj-arm-hdfs` container unless it is invalid and you are rebuilding it:

```bash
./.venv/bin/ray stop --force || true
ps -axo pid,ppid,stat,etime,command | grep -i '[r]ay' || true
```

For production-like validation on a Linux cluster, prefer the default PyArrow HDFS path instead of `filesystem: webhdfs`, and make sure Hadoop client libraries, `libjvm`, and HDFS credentials are available on both the Ray driver and workers.
