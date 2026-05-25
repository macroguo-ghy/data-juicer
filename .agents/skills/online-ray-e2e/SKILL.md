---
name: online-ray-e2e
description: Submit, monitor, and debug Data-Juicer online Ray E2E jobs through ByteDance ai_data_forge, including LaunchMerlinFederalJob jobs and Ray Jobs submitted to long-running Ray clusters. Use when the user asks to run or close the loop for Ray job submission, resident/long-running cluster submission, Federal job status, Ray UI or Ray History inspection, driver stdout/stderr debugging, ExecuteHdfsCommand HDFS checks, local Ray/HDFS dry-run validation, or fixing and resubmitting Data-Juicer online Ray configs.
---

# Online Ray E2E

Use this skill to run the loop:

```text
submit Ray job -> monitor status -> inspect driver logs -> fix config/code -> validate -> resubmit until success
```

Work from the Data-Juicer repo root. Prefer the repo helper script over hand-written RPC JSON when possible:

```bash
PYTHONPATH="$PWD" ./.venv/bin/python demos/bytedance/e2e_test/online_ray_job.py <command>
```

The launch `user_context.username` must match the active ByteDance login identity from `bytedcli --json auth status`, specifically `data.bytecloud_auth.identity.username`. Do not rely on the local shell `USER`, which may be a machine account such as `bytedance`. If the helper cannot infer the identity, pass `--username <bytedcli-login-username>` explicitly.

For exact commands, request fields, HDFS probes, and validation snippets, read `references/runbook.md`.
For resident / long-running Ray cluster submission through Ray Jobs, read `references/long_running_cluster.md`.

## Workflow

1. Confirm the target config, branch, image, model, API key source, worker count, and whether GPU is needed.
2. Run local validation before submitting when the change is local:
   - Prefer small Ray + HDFS validation when the change touches HDFS loading, parquet schema, or IO behavior.
   - Otherwise run `ray_dry_run_plan=True` with `PYTHONPATH="$PWD"` to force current source imports.
3. For online/distributed E2E input data, do not use repository-local files such as `demos/.../*.jsonl`; Ray workers may not see those files in their runtime working directories. Prefer HDFS-backed sample data and probe it with `ExecuteHdfsCommand` when needed.
4. Submit through `online_ray_job.py launch` for one-off Federal jobs. For resident / long-running Ray clusters, use `SubmitRayJobToLongRunningCluster` from `references/long_running_cluster.md`.
5. Poll Federal status, but treat Ray driver terminal state as authoritative when Federal status lags.
6. Open Ray UI / Ray History and inspect Jobs first. Read driver stdout/stderr before worker logs.
7. Attribute failure to the first useful exception, not to secondary Ray debugger, pickling, or materialize noise.
8. Fix the smallest owning layer: YAML, Data-Juicer code, image, resource spec, or external service config.
9. Revalidate, push code when needed, resubmit, and continue monitoring until success or a new blocker is proven.

## Safety Rules

- Never paste real API keys, full `operator_yaml`, base64 entrypoints, cookies, JWTs, signed log URLs, or raw request files into final answers, docs, commits, or chat.
- Store raw requests and downloaded logs only under `/tmp` or the helper run directory.
- If `operator_yaml` contains secrets, use redacted summaries in reports.
- Do not call a job successful unless Federal/Ray/Data-Juicer/export evidence all agree, or clearly state the missing layer.

## Debug Priorities

- For live Ray UI, Ray History, Ray Data, Grafana, actor pressure, and bottleneck diagnosis, also use the `ray-helper` skill if it is available.
- For internal RPCs, prefer `bytedcli --json bits rpc-call` or the helper script.
- For HDFS metadata or small sample checks on production paths, use `ai_data_forge.ExecuteHdfsCommand`; it only supports read commands.
- For local Mac HDFS checks, prefer the shared `dj-arm-hdfs` WebHDFS flow from the repo runbook.

## Common Failure Pattern

When export fails in `WriteMagnusDataSink` or `align_batch_to_schema`, compare the driver log Arrow schema with YAML `export.schema` before changing resources. Example: an actual `texts: string` column with config `texts: list<string>` causes:

```text
pyarrow.lib.ArrowNotImplementedError: Unsupported cast from string to list
```

The fix is to align the YAML schema or add a mapper before export to normalize the field shape.
