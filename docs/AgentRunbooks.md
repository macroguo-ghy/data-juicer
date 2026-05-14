# Agent Runbooks

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
- Treat `ray_data_checkpoint` as unavailable for now. Do not design, test, or explain the Ray/Data-Juicer path assuming checkpoint is enabled unless the user explicitly says this limitation has changed.
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

Use this flow when validating HDFS parquet loading on a Mac with Docker Desktop. It starts a disposable single-node HDFS inside Docker, writes a few parquet parts, and reads them through the Data-Juicer Ray HDFS loader.

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

- Start the disposable HDFS container:

```bash
docker rm -f dj-mac-hdfs >/dev/null 2>&1 || true
docker pull eclipse-temurin:17-jdk
docker run -d --name dj-mac-hdfs \
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
docker exec dj-mac-hdfs bash -lc '
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

docker exec dj-mac-hdfs bash -lc 'rm -rf /tmp/dj_hdfs_parts && mkdir -p /tmp/dj_hdfs_parts'
docker cp /tmp/dj_hdfs_parts/. dj-mac-hdfs:/tmp/dj_hdfs_parts/
docker exec dj-mac-hdfs bash -lc '
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

- Clean up only the resources created for this test:

```bash
docker rm -f dj-mac-hdfs
./.venv/bin/ray stop --force || true
ps -axo pid,ppid,stat,etime,command | grep -i '[r]ay' || true
```

For production-like validation on a Linux cluster, prefer the default PyArrow HDFS path instead of `filesystem: webhdfs`, and make sure Hadoop client libraries, `libjvm`, and HDFS credentials are available on both the Ray driver and workers.
