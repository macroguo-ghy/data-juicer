# Long-Running Ray Cluster Submission

Use this reference when submitting a Data-Juicer job to an already-running shared Ray cluster instead of launching a new `LaunchMerlinFederalJob` single-job cluster.

## Source Of Truth

Primary reference:

- `/Users/bytedance/repo/ai_data_forge/docs/long-running-ray-cluster-changes.html`

The `../ai_data_forge` service exposes these RPCs for resident Ray clusters:

- `GetRayClusterInfo`
- `ListRayFederalClusters`
- `SubmitRayJobToLongRunningCluster`
- `GetRayJobStatus`
- `ListRayJobs`
- `StopRayJob`
- `GetRayJobLogs`
- `StopRayCluster`

The request/response structs are in `../ai_data_forge/idl/ai_data_forge.thrift`. The implementation path is:

```text
Euler RPC -> RayClusterService -> RayClusterHttpClient -> Ray dashboard /api/jobs
```

`SubmitRayJobToLongRunningCluster` resolves the target Ray dashboard URL, then posts a Ray Jobs payload to:

```text
<dashboard_url>/api/jobs/
```

## Critical Data-Juicer Entrypoint Rule

Long-running clusters do not default to the Data-Juicer repository as the process working directory. Always use absolute paths for Data-Juicer scripts and files that must exist on the cluster image or shared mount.

For YAML / base64 YAML submission, the service default entrypoint is:

```bash
python /opt/tiger/data-juicer/tools/process_data_base64.py --config_base64 <base64-yaml>
```

Do not write resident-cluster examples like:

```bash
python tools/process_data.py --config demos/...
```

unless the entrypoint first `cd`s to the repository and every path has been checked. Prefer the absolute `process_data_base64.py` entrypoint above.

## Submission Sources

`SubmitRayJobToLongRunningClusterRequest` requires exactly one of:

- `yaml`: raw Data-Juicer YAML. The service base64-encodes it and uses `/opt/tiger/data-juicer/tools/process_data_base64.py`.
- `base64_yaml`: pre-encoded Data-Juicer YAML. The service uses `/opt/tiger/data-juicer/tools/process_data_base64.py`.
- `entrypoint`: custom Ray job entrypoint. Use absolute paths for Data-Juicer scripts, configs, outputs, and any helper files.

It also requires:

- `submission_id`: stable Ray Jobs submission id chosen by the caller.
- `user_context.username`: the ByteDance user to submit as.
- `cluster_name`: optional. If omitted, ai_data_forge selects a RUNNING LongRunning Federal Ray cluster from namespace `/topic/790e3ece1131c882`. Prefer passing it explicitly when the target cluster matters.

The implementation sets Ray job metadata:

```json
{
  "owner": "<username>",
  "BYTED_RAY_TOKEN": "<identity-token>"
}
```

and sends the identity token as both `gdpr-token` and `byte-zti-token` headers to the Ray dashboard Jobs API.

## Submit With ai_data_forge RPC

Use `bytedcli bits rpc-call` and store raw request/response files under `/tmp`.

Raw YAML example:

```bash
cat >/tmp/dj_long_running_submit.json <<'JSON'
{
  "submission_id": "dj_lr_20260523_001",
  "cluster_name": "c-paubxt82r1tu-example-hl-rabbit",
  "yaml": "project_name: dj-long-running-demo\nexecutor_type: ray\nray_address: auto\nwork_dir: /mnt/shared/dj/work/dj_lr_20260523_001\nexport:\n  target: local\n  path: /mnt/shared/dj/out/dj_lr_20260523_001/result.parquet\n  type: parquet\nprocess: []\n",
  "user_context": {
    "username": "<your-username>",
    "user_role": "",
    "user_email": "<your-username>@bytedance.com"
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

bytedcli --json bits rpc-call ad.ai.data_forge SubmitRayJobToLongRunningCluster \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_long_running_submit.json | tee /tmp/dj_long_running_submit.response.json
```

Custom entrypoint example:

```json
{
  "submission_id": "dj_lr_20260523_002",
  "cluster_name": "c-paubxt82r1tu-example-hl-rabbit",
  "entrypoint": "python /opt/tiger/data-juicer/tools/process_data_base64.py --config_base64 <base64-yaml>",
  "user_context": {"username": "<your-username>"}
}
```

Use a custom entrypoint only when the command itself is the contract being tested. For normal Data-Juicer configs, prefer `yaml` or `base64_yaml` so the service applies the standard absolute script path.

## Recommended Call Loop

1. Create a temporary resident cluster when needed with `LaunchMerlinFederalJob(sub_type="LongRunning")`. LongRunning Federal jobs support an empty entrypoint; YARN `is_long_running` is forced true.
2. Get the cluster name and dashboard metadata with `ListRayFederalClusters` or `GetRayClusterInfo`.
3. Submit with `SubmitRayJobToLongRunningCluster(cluster_name=..., yaml/base64_yaml/entrypoint=...)`.
4. Poll with `GetRayJobStatus(cluster_name=..., submission_id=...)` until `SUCCEEDED`, `FAILED`, or `STOPPED`.
5. Confirm visibility with `ListRayJobs(cluster_name=...)` when needed.
6. Read stdout/stderr tail with `GetRayJobLogs(cluster_name=..., submission_id=...)`.
7. Stop only the job with `StopRayJob(cluster_name=..., submission_id=...)` for normal cancellation.
8. Stop the entire cluster with `StopRayCluster(cluster_name=...)` only when cleaning up a temporary resident cluster that this flow created.

For new integrations, always pass `cluster_name`. Only `SubmitRayJobToLongRunningCluster` and `GetRayJobStatus` can omit it for compatibility; omitting it lets ai_data_forge auto-select a running LongRunning cluster and can hit the wrong target.

## Inspect Cluster And Jobs

Get cluster dashboard and Federal metadata:

```bash
cat >/tmp/dj_long_running_cluster.json <<'JSON'
{
  "cluster_name": "c-paubxt82r1tu-example-hl-rabbit",
  "user_context": {"username": "<your-username>"}
}
JSON

bytedcli --json bits rpc-call ad.ai.data_forge GetRayClusterInfo \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_long_running_cluster.json | tee /tmp/dj_long_running_cluster.response.json
```

List running LongRunning Federal clusters:

```bash
cat >/tmp/dj_long_running_clusters.json <<'JSON'
{
  "page_number": 1,
  "page_size": 20,
  "sub_types": ["LongRunning"],
  "status": ["RUNNING"],
  "namespaces": ["/topic/790e3ece1131c882"],
  "tab_type": "All",
  "user_context": {"username": "<your-username>"}
}
JSON

bytedcli --json bits rpc-call ad.ai.data_forge ListRayFederalClusters \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_long_running_clusters.json | tee /tmp/dj_long_running_clusters.response.json
```

List Ray Jobs on a cluster:

```bash
cat >/tmp/dj_long_running_jobs.json <<'JSON'
{
  "cluster_name": "c-paubxt82r1tu-example-hl-rabbit",
  "user_context": {"username": "<your-username>"}
}
JSON

bytedcli --json bits rpc-call ad.ai.data_forge ListRayJobs \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_long_running_jobs.json | tee /tmp/dj_long_running_jobs.response.json
```

## Monitor, Logs, Stop

Poll a job:

```bash
cat >/tmp/dj_long_running_status.json <<'JSON'
{
  "submission_id": "dj_lr_20260523_001",
  "cluster_name": "c-paubxt82r1tu-example-hl-rabbit",
  "user_context": {"username": "<your-username>"}
}
JSON

bytedcli --json bits rpc-call ad.ai.data_forge GetRayJobStatus \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_long_running_status.json | tee /tmp/dj_long_running_status.response.json
```

Fetch driver logs:

```bash
cat >/tmp/dj_long_running_logs.json <<'JSON'
{
  "submission_id": "dj_lr_20260523_001",
  "cluster_name": "c-paubxt82r1tu-example-hl-rabbit",
  "max_bytes": 200000,
  "user_context": {"username": "<your-username>"}
}
JSON

bytedcli --json bits rpc-call ad.ai.data_forge GetRayJobLogs \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_long_running_logs.json | tee /tmp/dj_long_running_logs.response.json
```

`GetRayJobLogs` returns the tail of the logs. Default `max_bytes` is 20000 and the maximum is 200000. Preserve `truncated` and `bytes_returned` in diagnostics so the caller knows whether more logs may exist.

Stop a submitted job:

```bash
cat >/tmp/dj_long_running_stop.json <<'JSON'
{
  "submission_id": "dj_lr_20260523_001",
  "cluster_name": "c-paubxt82r1tu-example-hl-rabbit",
  "user_context": {"username": "<your-username>"}
}
JSON

bytedcli --json bits rpc-call ad.ai.data_forge StopRayJob \
  --idl-version codex/use-python-311 \
  --idl-source branch \
  --zone CN \
  --idc hl \
  --env ppe_terranova \
  --cluster default \
  --body-file /tmp/dj_long_running_stop.json | tee /tmp/dj_long_running_stop.response.json
```

## Validation And Caveats

- Validate the YAML locally with `ray_dry_run_plan=True` before submitting when changing Data-Juicer config or code.
- Use HDFS, S3, Magnus, or a shared mounted filesystem for distributed inputs and outputs. Repository-local paths are not reliable across resident-cluster workers.
- Use absolute paths for any config file, script, working directory, shared mount, or local sink path. Resident cluster jobs may start outside `/opt/tiger/data-juicer`.
- Use unique `submission_id`, `work_dir`, and export output directories for each run. Ray Jobs submission ids are reused for status/log/stop calls and should be stable.
- Do not put secrets in persisted request files. Prefer environment-backed secret injection or redact request files before sharing.
- Treat Ray Jobs terminal status and logs as authoritative for the submitted job. Federal cluster state only tells whether the resident cluster is alive.
- Check `status_code == 0` before trusting RPC payloads. Validation failures generally return non-zero status plus `status_message` / `base_resp.status_message`.
- Do not call `StopRayCluster` for ordinary job cancellation. It stops the whole Merlin Federal Ray cluster after resolving `merlin_federal_job_sid`; use `StopRayJob` for one job.
