---
name: ray-helper
description: >-
  Help with ByteDance Merlin/Arnold Ray jobs. Use when the user provides a Merlin job/instance/trial URL, Ray History Server URL, Ray Dashboard/RayUI URL, Grafana metrics URL, or asks for Ray tuning, Ray task debugging, actor pressure or custom actor-count anomalies, resource sizing, Ray Data tuning, Data-Juicer-on-Ray bottleneck analysis, CPU/GPU/memory/IO/disk diagnosis, or pipeline parameter/concurrency recommendations. The live diagnosis workflow starts from an existing Ray/Merlin job and correlates RayUI, Grafana, logs, and Data-Juicer config evidence. The concurrency-tuning workflow starts from a Data-Juicer YAML plus available cluster resources, validates the YAML, inspects input data size/files, builds a small demo YAML, submits online Ray E2E runs, observes Ray Data/Grafana metrics, and then proposes or applies only concurrency-related YAML changes.
---

# Ray Helper

## Modes

- `diagnose`: implemented. Use it to analyze an existing Ray/Merlin job's resource usage, Ray Data stages, Grafana/business metrics, and Data-Juicer pipeline parameters, then propose tuning experiments.
- `concurrency-tune`: implemented. Use it when the user provides a Data-Juicer YAML and available resources, and wants a closed-loop concurrency tuning run. Read [references/concurrency-tuning.md](references/concurrency-tuning.md) before taking action.
- `debug`: not implemented yet. If the user explicitly asks for debug mode, say that this skill currently only has the diagnose and concurrency-tune workflows and proceed with general investigation outside the skill if appropriate.

## Concurrency Tuning Entry Point

When the user gives a YAML plus resource budget such as `100 workers, 8c16g each`, switch to the `concurrency-tune` workflow. First validate the YAML for logic/config errors. If the YAML is wrong, stop and report those errors instead of tuning. If it is valid, only change concurrency-related parameters in the production YAML: source read concurrency/blocking knobs, operator `num_proc`/`num_cpus`/`batch_size`, per-task IO concurrency such as `max_concurrent`, and export/write concurrency. Do not change business logic, filters, schemas, selected fields, table names, operator order, or output semantics unless the user explicitly asks.

Use repo/service tools to inspect input dataset size, file count, schema, and sampleability before generating a demo YAML. For HDFS use `ExecuteHdfsCommand` or equivalent `hdfs dfs` checks; for Lance use `GetLanceTableSchema` and table metadata/read probes when available; for Hive/Magnus use the corresponding catalog/schema/location tools available in the environment. Build a small demo YAML that reads only a few representative files or a small bounded input and uses a small worker budget, then submit and monitor it through the `online-ray-e2e` skill. Iterate demo runs until there is no obvious bottleneck or the remaining limit is external, then scale the concurrency recommendations to the real resource budget and submit/observe the production run.

Detailed procedure, metrics, and report shape: [references/concurrency-tuning.md](references/concurrency-tuning.md).

## Live Diagnose Workflow

Use a named browser session. Keep all authentication state and page interactions in that session.

1. Open the Merlin task instance URL.
2. If the page redirects to ByteDance SSO or shows an unauthenticated shell, open a headed browser, ask the user to complete login, and pause until the user confirms.
3. Extract RayUI, cluster-level metrics, Ray Data metrics, and node-level metrics.
4. Open RayUI or call the History Server APIs for the selected cluster/job.
5. Analyze Ray Data Overview by dataset/operator stage.
6. Query Grafana via bytedcli, prioritizing business monitoring panels, for CPU/GPU/memory/IO/disk evidence.
7. Map the user's Data-Juicer pipeline/config parameters to Ray stages and Grafana signals.
8. Report links, evidence, bottleneck assessment, and concrete tuning proposals.

Do not include JWTs, cookies, authorization headers, log proxy codes, or other secrets in the final answer. If a tool output contains `x-jwt-token`, `authorization`, cookies, or signed log URLs, summarize only the non-secret fields.

Evidence completeness rules:

- If the user asks about actor pressure or whether actors are being created per task, read the config to get the expected actor/shard/pool count, count actors from Merlin `actor/list` or History Server `/logical/actors`, and check Grafana Ray core/Ray Data metrics before giving the final conclusion.
- Do not imply Grafana evidence was checked unless you fetched panel data, a screenshot, or `screenshot --only-data` output for the job-padded time range. If Grafana is blocked or intentionally skipped, state that boundary in the answer.
- For actor-count checks, compare the configured actor/shard/pool count with the actual actor class count, Ray core `actors`, Ray Data task/concurrency metrics, and spill/object-store metrics. A healthy run should not show actor count multiplying unexpectedly by Ray task or block count.

## Authentication Pauses

If any required ByteDance SSO session is missing, stop the workflow at that point and ask the user to finish the manual login step before continuing. Do not keep analyzing with partial links or stale metrics when the missing SSO session prevents fetching RayUI, Merlin, Grafana, or bytedcli data.

Use this pause rule for:

- Merlin pages that redirect to `sso.bytedance.com` or show an unauthenticated shell.
- Grafana commands that return `GRAFANA_AUTH_REQUIRED`, `SSO session not found`, or similar session errors.
- agent-browser sessions that cannot access the task page after opening a headed browser.

After the user says login is complete, retry the exact failed step in the same browser session or bytedcli context. Only proceed with degraded analysis if the user explicitly asks to continue without that data.

## Merlin Page

Start with `agent-browser`; use a unique session name such as `ray-debug`.

```bash
agent-browser --session ray-debug set viewport 1920 1080
agent-browser --session ray-debug open '<MERLIN_JOB_URL>'
agent-browser --session ray-debug get title
agent-browser --session ray-debug snapshot -i
```

If login is needed:

```bash
AGENT_BROWSER_HEADED=1 agent-browser --session ray-debug open '<MERLIN_JOB_URL>'
```

Ask the user to finish SSO in the browser window and reply when done. Continue with the same session:

```bash
agent-browser --session ray-debug get url
agent-browser --session ray-debug snapshot -i
```

Do not continue to RayUI or Grafana extraction until the user confirms the page is authenticated. If the page is still on SSO after confirmation, ask the user to finish the remaining login prompt and retry the same checks.

Useful visible controls on the Merlin run-info page usually include:

- `RayUI`
- `查看监控`
- `MetricCPU`
- `MetricRay`
- per-node rows for `head_0` and `worker_*`

## Link Extraction

Prefer stable API responses over clicking every button.

First inspect network calls:

```bash
agent-browser --session ray-debug network requests --type xhr,fetch
```

Look for these endpoints:

- `POST /api/training/ray_job/cluster/get`
- `POST /api/training/ray_job/actor/list`
- `POST /api/training/ray_job/job/list`
- `POST /api/training/ray_job/instance/list`
- `GET /arnold/api/v3/trials/<trial_id>/poll_instances/`
- `GET /arnold/api/v3/trials/<trial_id>/brief/`

Read only the response body from `ray_job/cluster/get`; do not print request headers.

```bash
agent-browser --session ray-debug network request <REQUEST_ID> --json \
  | jq -r '.data.responseBody' \
  | jq '.result | {
      cluster_name,
      ray_version,
      python_version,
      client_port,
      dashboard_port,
      frontend_config
    }'
```

Important fields:

- `result.frontend_config.dashboard`: live Ray dashboard/proxy if available.
- `result.frontend_config.ray_core_grafana`: cluster-level Ray core/Grafana metrics.
- `result.frontend_config.ray_data_grafana`: Ray Data Grafana metrics.
- `result.dashboard_port`: internal head-node dashboard URL.
- `result.cluster_name`: Ray History Server cluster key, often `trial-<id>-trialrun-<id>-<cluster>`.

To capture the `RayUI` button URL if the API does not expose it, intercept `window.open`, then click the button:

```bash
agent-browser --session ray-debug eval --stdin <<'EVALEOF'
window.__codexOpenedUrls = [];
window.__codexOldOpen = window.open;
window.open = function(url, target, features) {
  window.__codexOpenedUrls.push({url: String(url), target: target || "", features: features || ""});
  return {focus(){}, close(){}, closed:false};
};
EVALEOF
agent-browser --session ray-debug find text "RayUI" click
agent-browser --session ray-debug wait 1000
agent-browser --session ray-debug eval 'JSON.stringify(window.__codexOpenedUrls || [])'
```

Construct the History Server API base from the RayUI URL:

```text
RayUI:       https://ray-history-server.byted.org/#/new/history/<cluster>/jobs
API base:    https://ray-history-server.byted.org/history/<cluster>
```

If a History Server API call returns `Select a logfile first`, open the RayUI URL once. RayUI may redirect to a log-selected cluster key such as `<cluster>:<log_suffix>`. Use that full key for API calls:

```text
RayUI after redirect: https://ray-history-server.byted.org/#/new/history/<cluster>:<log_suffix>/overview
API base:             https://ray-history-server.byted.org/history/<cluster>:<log_suffix>
```

For per-node metrics, prefer the History Server node summary after RayUI is known:

```bash
curl -s 'https://ray-history-server.byted.org/history/<cluster>/nodes?view=summary' \
  | jq -r '.data.summary[] |
      [
        .raylet.nodeName,
        (.raylet.isHeadNode|tostring),
        .raylet.state,
        .raylet.resourcesTotal.CPU,
        (.raylet.resourcesTotal.memory / 1024 / 1024 / 1024),
        (.raylet.resourcesTotal.objectStoreMemory / 1024 / 1024 / 1024),
        (.metricCpu // ""),
        (.metricRay // "")
      ] | @tsv'
```

If `metricRay` is empty, still report `metricCpu` and the cluster-level `ray_core_grafana` / `ray_data_grafana` links.

## RayUI Job Analysis

Open RayUI and select a finished or running job relevant to the user request. If the job id is visible, use it directly; otherwise inspect the jobs page and choose by status, time, or entrypoint.

Prefer the compact helper scripts before pasting full RayUI API payloads into the conversation:

```bash
# One job: decode config-base64, redact secrets, and summarize Ray Data operators.
.agents/skills/ray-helper/scripts/ray_job_summary.py '<RAY_JOB_URL>' --format markdown

# Two jobs: compare config, dataset context, and operator block-size/output changes.
.agents/skills/ray-helper/scripts/ray_compare_jobs.py '<OLD_RAY_JOB_URL>' '<CURRENT_RAY_JOB_URL>' --format markdown
```

The scripts also accept compact JSON summaries as local files. Use raw `/api/jobs/<job_id>` or `/api/data/datasets` output only when the helper cannot parse the URL or when a missing field must be inspected directly.

For Godel live dashboard URLs, `ray_job_summary.py` and `ray_compare_jobs.py` automatically retry through Ray History Server event logs when the live API has been archived or still reports a stale `RUNNING` job after all Ray Data datasets are finished.

For HDFS output-directory growth checks, use `hdfs_ls_summary.py` instead of printing large `hdfs dfs -ls` output or hand-writing `jq` / `awk` diffs:

```bash
.agents/skills/ray-helper/scripts/hdfs_ls_summary.py \
  --path hdfs://haruna/path/to/output \
  --samples 2 \
  --interval 60 \
  --username <username> \
  --user-email <email>
```

If the `ExecuteHdfsCommand` responses are already saved locally, compare them directly:

```bash
.agents/skills/ray-helper/scripts/hdfs_ls_summary.py \
  --compare-response /tmp/dj_hdfs_ls_before.json /tmp/dj_hdfs_ls_after.json \
  --format markdown
```

Useful endpoints:

```bash
# Job metadata.
curl -s 'https://ray-history-server.byted.org/history/<cluster>/api/jobs/<job_id>' | jq

# Ray Data datasets/operators.
curl -s 'https://ray-history-server.byted.org/history/<cluster>/api/data/datasets' | jq

# Actors, useful for custom actor pressure and actor-count anomalies.
curl -s 'https://ray-history-server.byted.org/history/<cluster>/logical/actors' | jq
```

### Ray Data Worker Log Lookup

Ray Data operator logs from `map_batches`, filters, and Data-Juicer mappers usually come from worker tasks, not the driver. If the driver log is empty or does not show the mapper error, locate the task's `node_id` and `worker_pid`, then read the matching worker `.err` file on that node.

For live Ray Dashboard / Godel dashboard URLs, the API base is usually the dashboard URL prefix before `/#/...`, for example:

```text
Dashboard: https://godel-stream-applications.byted.org/<app>/<cluster>-batch-dashboard/#/jobs/<job_id>
API base:  https://godel-stream-applications.byted.org/<app>/<cluster>-batch-dashboard
```

Find the worker that executed a specific Ray Data operator:

```bash
curl -s '<API_BASE>/api/v0/tasks?filter_keys=job_id&filter_predicates=%3D&filter_values=<JOB_ID>&limit=1000&detail=1' \
  | jq -r '.data.result.result[]
    | select((.name // "") | test("<OPERATOR_OR_CLASS_NAME>"))
    | [.name, .node_id, .worker_pid] | @tsv'
```

For example, to find where `ImageOcrMapper.process_batched` logged:

```bash
curl -s '<API_BASE>/api/v0/tasks?filter_keys=job_id&filter_predicates=%3D&filter_values=<JOB_ID>&limit=1000&detail=1' \
  | jq -r '.data.result.result[]
    | select((.name // "") | test("ImageOcrMapper.process_batched"))
    | [.name, .node_id, .worker_pid] | @tsv'
```

Then list logs on that node and choose the worker stderr file containing the same pid:

```bash
curl -s '<API_BASE>/api/v0/logs?node_id=<NODE_ID>' \
  | jq -r '.data.result[]' \
  | grep '<WORKER_PID>.*\\.err'
```

Read that `.err` file:

```bash
curl -s '<API_BASE>/api/v0/logs/file?node_id=<NODE_ID>&filename=<WORKER_ERR_FILENAME>'
```

Use `.err` before `.out` for Python logger output and exceptions. Worker `.out` often only contains Ray task markers such as `:task_name:...`, while Data-Juicer `logger.error(...)` output commonly appears in the worker `.err` file.

To identify where a running Ray Data job is currently blocked in user code, inspect the dataset action stack:

```bash
curl -s 'https://ray-history-server.byted.org/history/<cluster>/api/data/datasets' \
  | jq -r '.datasets[] |
      "DATASET \(.dataset) state=\(.state) progress=\(.progress)/\(.total)",
      (.action_stack // "NO_ACTION_STACK")
    '
```

The same field is visible in the RayUI page: open `Jobs -> <job_id>`, find `Ray Data Overview`, then click the dataset name text such as `dataset_1` in the `Dataset / Operator Name` column. The dialog's `Call Stack` section renders `datasets[].action_stack`. Use this to cite the exact user-code file and line where the driver is waiting.

For Merlin's actor list response, summarize by class without printing request headers:

```bash
agent-browser --session ray-debug network request <ACTOR_LIST_REQUEST_ID> --json \
  | jq -r '.data.responseBody' \
  | jq -r '.result.data | group_by(.class_name)[] |
      "class=\(.[0].class_name) count=\(length) states=\(group_by(.state)|map("\(.[0].state):\(length)")|join(","))"'
```

Record the job time window from RayUI before analyzing Grafana. The job API usually returns millisecond timestamps such as `start_time` and `end_time`. Use `start_time - 5 minutes` as the Grafana `from` time and `end_time + 5 minutes` as the Grafana `to` time. If the job is still running, use the current time as the temporary `to` time and say that the final 5-minute tail is not available yet.

```bash
curl -s 'https://ray-history-server.byted.org/history/<cluster>/api/jobs/<job_id>' \
  | jq '{status, start_time, end_time, start_for_grafana_ms:(.start_time - 300000), end_for_grafana_ms:(if .end_time then (.end_time + 300000) else null end)}'
```

Compact dataset/operator summary:

```bash
curl -s 'https://ray-history-server.byted.org/history/<cluster>/api/data/datasets' \
  | jq -r '
    .datasets[] as $d |
      "DATASET \($d.dataset) state=\($d.state) progress=\($d.progress)/\($d.total) dur_s=\((($d.end_time-$d.start_time)*10|round)/10) rows=\($d.ray_data_output_rows.max) bytes=\($d.ray_data_output_bytes.max) spilled=\($d.ray_data_spilled_bytes.max)",
      ($d.operators[] |
        "  OP \(.operator) state=\(.state) progress=\(.progress)/\(.total) rows=\(.ray_data_output_rows.max) bytes=\(.ray_data_output_bytes.max) mem_max=\(.ray_data_current_bytes.max) spill=\(.ray_data_spilled_bytes.max) cpu_max=\(.ray_data_cpu_usage_cores.max) conc_run_max=\(.ray_data_num_concurrency_running.max) task_wall_max=\(.ray_data_task_duration_wall_time.max)"
      )
  '
```

For stage attribution, read each operator's `extra_metrics.transform_fns` and `logical_plan`. These often reveal the Data-Juicer operator inside fused Ray stages, for example `DownloadFileMapper.process_batched`, `SomeOperator.compute_stats_single`, or `WriteMagnusDataSink`.

## Grafana Analysis

Use `bytedcli grafana` for dashboard and panel inspection. Prefer business monitoring first, then Ray core/Ray Data dashboards as supporting evidence. In Merlin pages, business monitoring usually comes from the `查看监控` link or per-instance `监控`, `MetricCPU`, and `MetricRay` controls; cluster-level `ray_core_grafana` and `ray_data_grafana` are still useful for Ray internals.

Always set the Grafana time range to the job window plus padding:

- `from = RayUI job start_time - 5 minutes`
- `to = RayUI job end_time + 5 minutes`
- for running jobs, `to = now` until the job completes

Prefer adding standard Grafana query parameters to every dashboard/panel URL:

```text
<GRAFANA_URL>&from=<start_for_grafana_ms>&to=<end_for_grafana_ms>
```

If the URL has no query string, use `?from=...&to=...`; otherwise append `&from=...&to=...`. Prefer this explicit `from/to` range over a relative duration, because it aligns business monitoring with the RayUI job interval.

Start by confirming the command surface on the current machine:

```bash
bytedcli grafana --help
bytedcli grafana dashboard get --help
bytedcli grafana panel list --help
bytedcli grafana expr-parse --help
bytedcli grafana screenshot --help
```

Fetch dashboard metadata and variables from the time-bounded URL:

```bash
bytedcli --json grafana dashboard get --url "<GRAFANA_URL_WITH_FROM_TO>" > /tmp/ray-grafana-dashboard.json
bytedcli --json grafana variable get --url "<GRAFANA_URL_WITH_FROM_TO>" > /tmp/ray-grafana-vars.json
bytedcli --json grafana expr-parse --url "<GRAFANA_URL_WITH_FROM_TO>" > /tmp/ray-grafana-expr.json
```

If any Grafana command returns `GRAFANA_AUTH_REQUIRED` or `SSO session not found`, pause and ask the user to complete a browser-session login such as:

```bash
bytedcli auth login --session
```

After the user confirms, rerun the failed Grafana command before continuing. Do not substitute dashboard structure alone for time-series or panel data unless the user chooses to proceed without Grafana access.

When the user provides only a Ray cluster Grafana URL, use that URL as the
primary input and recover the cluster/node context before inspecting panels.
Start by extracting Grafana variables and the explicit time range:

```bash
GRAFANA_URL='<GRAFANA_URL>'
bytedcli --json grafana variable get --url "$GRAFANA_URL" > /tmp/ray-grafana-vars.json
bytedcli --json grafana expr-parse --url "$GRAFANA_URL" > /tmp/ray-grafana-expr.json

jq -r '
  .. | objects
  | to_entries[]
  | select(.key | test("RayClusterName|PodName|Host|JobID|SubmissionID|Component|from|to"))
  | "\(.key)=\(.value)"
' /tmp/ray-grafana-vars.json /tmp/ray-grafana-expr.json 2>/dev/null
```

Confirm at least:

- `from` / `to`: the metric window, preferably the Ray job window plus five minutes of padding.
- `var-RayClusterName`: the Ray cluster name for Ray History Server lookup.
- `var-PodName`: the currently selected pod. Do not assume it is the failing or hottest pod.
- `var-Host`: the selected host, if present. It may be `All` even when `PodName` is fixed.

Then query the Ray History Server node summary for the same cluster. If a RayUI
or History URL includes a log suffix such as `<cluster>:<log_suffix>`, use the
full suffixed key. If the unsuffixed API returns `Select a logfile first`, open
the RayUI URL and retry with the redirected suffixed key.

```bash
CLUSTER='<var-RayClusterName-or-suffixed-history-key>'
curl -s "https://ray-history-server.byted.org/history/${CLUSTER}/nodes?view=summary" \
  > /tmp/ray-history-nodes.json

jq -r '
  (.data.summary // .summary // [])[]
  | [
      (.raylet.nodeName // .nodeName // ""),
      (.raylet.nodeIp // .nodeIp // ""),
      ((.raylet.isHeadNode // .isHeadNode // false) | tostring),
      (.raylet.state // .state // ""),
      ((.raylet.resourcesTotal.CPU // 0) | tostring),
      (((.raylet.resourcesTotal.memory // 0) / 1024 / 1024 / 1024) | tostring),
      (((.raylet.resourcesTotal.objectStoreMemory // 0) / 1024 / 1024 / 1024) | tostring),
      (.metricCpu // ""),
      (.metricRay // "")
    ] | @tsv
' /tmp/ray-history-nodes.json
```

Use the node summary to build the cluster inventory:

- total nodes, alive/dead node counts, head node, and worker nodes.
- total Ray logical CPU, memory, and object store memory across alive nodes.
- each node's `nodeName`/pod, node IP, state, `metricCpu`, and `metricRay` links.
- the node selected by the incoming Grafana URL, matched by `var-PodName` or by host/IP when available.

For node-focused diagnosis, always compare the selected pod with the actual
failure or hottest pod from Ray logs, RayUI job errors, Ray Data Overview, or
host-memory panels. A user-shared Grafana URL often pins one pod for convenience;
it is not proof that this pod caused the job failure.

If you need to turn a Ray node IP into the hostname shown by Grafana, normalize
IPv6 by replacing `:` with `-` and compare it with host labels from panel rows.
For example, a Ray node IP shaped like `<prefix>:<region>:<rack>:<host>` may map
to a Grafana host label shaped like `<region>-p<rack>-<host>`. Treat this only
as a matching heuristic and verify it with Grafana `PodName`/`Host` variables or
panel row labels when possible.

When checking a concrete node, keep separate URLs or files for:

- the original Grafana URL's selected pod.
- the suspected failing/hottest pod from Ray History logs or host memory rows.
- cluster-level `All` pod/host view when you need to find the max node.

Example workflow for panel data by pod:

```bash
BASE_URL='<GRAFANA_URL_WITH_FROM_TO>'
PANEL_ID='<PANEL_ID>'
SELECTED_POD='<pod-from-input-url>'
SUSPECT_POD='<pod-from-ray-error-or-node-summary>'

bytedcli --json grafana screenshot --only-data \
  --url "${BASE_URL}&var-PodName=${SELECTED_POD}&viewPanel=${PANEL_ID}" \
  > /tmp/ray-panel-${PANEL_ID}-selected.json

bytedcli --json grafana screenshot --only-data \
  --url "${BASE_URL}&var-PodName=${SUSPECT_POD}&viewPanel=${PANEL_ID}" \
  > /tmp/ray-panel-${PANEL_ID}-suspect.json
```

Use this comparison before making node-specific claims such as "the selected
node was healthy" or "the OOM happened on another worker".

List relevant panels by keyword. Run several keyword passes because panel titles vary:

```bash
for kw in 业务 monitor 监控 CPU cpu GPU gpu Memory memory mem 内存 IO io Disk disk Network network Spill spill Object object Store store; do
  bytedcli --json grafana panel list --url "<GRAFANA_URL_WITH_FROM_TO>" --keyword "$kw"
done
```

Inspect a specific panel when the title or id looks relevant:

```bash
bytedcli --json grafana panel get --url "<GRAFANA_URL_WITH_FROM_TO>" --panel-id <PANEL_ID>
bytedcli grafana screenshot --url "<GRAFANA_URL_WITH_FROM_TO>&viewPanel=<PANEL_ID>" --width 1600 --height 900
bytedcli --json grafana screenshot --only-data --url "<GRAFANA_URL_WITH_FROM_TO>&viewPanel=<PANEL_ID>"
```

Prefer `screenshot --only-data` when you need numeric evidence. For Ray Data panels, summarize query series by metric, operator, and max value:

```bash
jq -r '
  .data.total[]
  | select((.url|contains("/query")) and (.data|type=="array"))
  | .data[]
  | [(.metric // ""), (.tags.operator // ""), ((.dps|to_entries|map(.value|tonumber)|max) // 0)]
  | @tsv
' /tmp/ray-grafana-panel.json
```

Some Grafana panels return SQL/ClickHouse rows under `.data.total[].data.data` instead of Bosun/TSDB series. Use those rows for distribution panels such as container memory usage or RSS:

```bash
jq -r '
  .data.total[]
  | select((.data|type)=="object" and (.data.data? != null))
  | .data.data[]
  | [.t, (.mem_usage_max // .mem_rss_max // ""), (.mem_usage_p95 // .mem_rss_p95 // ""), (.mem_usage_avg // .mem_rss_avg // ""), (.mem_limit // "")]
  | @tsv
' /tmp/ray-grafana-panel.json
```

Check business monitoring first:

- End-to-end throughput, request rate, processed rows/items/images, success/failure counts, retries, timeout rate, external API/RPC latency, and downstream service errors if such panels exist.
- Correlate business metric drops/spikes with Ray Data stages using the padded job time window.
- If business monitoring is absent or inaccessible, state that boundary and fall back to Ray core/Ray Data/system panels.

Then check system and Ray signal families:

- CPU: cluster/node CPU utilization, Ray worker CPU, raylet/GCS CPU, task CPU saturation, pending resource demands.
- GPU: GPU utilization and memory if the cluster has GPUs; explicitly say "not applicable" when the job is CPU-only.
- Memory: container usage versus limit, RSS/component RSS, Ray logical memory, object store memory, worker heap, Python idle worker memory, OOM/restart signals.
- IO/network: download or remote-call throughput, network RX/TX, RPC latency, retry spikes, external service backpressure.
- Disk: disk read/write, object spilling, local temp usage, filesystem pressure, write/export throughput.

Memory/OOM reading order:

- Treat container or pod memory usage divided by limit as the direct cgroup OOM-risk signal. Sustained values near 90% deserve attention; sustained values near 95% or above are high risk.
- Use RSS/component RSS to decide whether processes are truly holding memory. High container usage with much lower RSS often points to cache, shared memory, or object-store accounting rather than Python heap alone.
- Use object store usage, `SPILLED`, `PendingSpill`, and spill request counters to decide whether Ray object-store pressure is real.
- Use Ray Data per-operator `data_current_bytes`, pending task inputs/outputs, and queue metrics to locate which stage has the largest in-flight memory.
- If only distribution panels are available, report max and p95 distribution values and say that per-pod attribution was not available. If per-pod rows are available, name the specific head or worker pod with the highest usage.

Interpret Grafana together with Ray Data Overview:

- High Ray Data task wall time with low CPU and stable memory usually points to IO/network or remote service latency.
- High container usage without high RSS, spilling, OOM, or restarts is pressure to watch, but not enough by itself to call an imminent OOM.
- Object spilling, OOM, worker restarts, high RSS near the limit, or disk write spikes during Ray Data stages are stronger memory/object-store evidence.
- Node-level imbalance plus skewed Ray Data block distributions suggests partition/skew tuning.

If bytedcli can fetch panel definitions and screenshots but not raw time-series points for the target dashboard, state that boundary and use the panel expression, screenshot, RayUI metrics, and History Server API evidence together.

## Driver/Head Failure Diagnosis

When the user provides a RayUI driver node link such as
`#/cluster/nodes/<node_id>`, treat it as the primary node-level input. Driver
and head are different concepts: the driver is the Python job process, while the
head node hosts Ray control-plane services such as GCS, dashboard, and raylet.
They may be on the same pod or on different pods. Always verify this with the
job metadata and node page before attributing pressure to "head" or "driver".

Start from the driver node page and extract the exact metric links:

```bash
agent-browser --session ray-driver set viewport 1920 1080
agent-browser --session ray-driver open '<RAYUI_DRIVER_NODE_URL>'
agent-browser --session ray-driver snapshot -i
agent-browser --session ray-driver eval \
  'JSON.stringify(Array.from(document.querySelectorAll("a")).map(a=>({text:a.textContent.trim(),href:a.href,target:a.target})))'
```

Use the `MetricCPU` and `MetricRay` URLs from that page, and override their time
range to the job window plus five minutes of padding. If the dashboard URL has
`to=now`, replace it with the concrete `end_time + 300000`; do not compare a
failed job against an open-ended time window unless the user explicitly wants
post-failure behavior too.

Fetch raw Grafana data when possible:

```bash
bytedcli -j grafana screenshot --only-data --timeout 60s \
  --url '<METRIC_RAY_URL_WITH_JOB_PADDED_FROM_TO>' \
  > /tmp/ray-driver-metricray.json

bytedcli -j grafana screenshot --only-data --timeout 60s \
  --url '<METRIC_CPU_URL_WITH_JOB_PADDED_FROM_TO>' \
  > /tmp/ray-driver-metriccpu.json
```

Summarize the Ray core signals first:

- `ray.component_rss_mb` grouped by `Component`: especially `Driver`,
  `gcs_server`, `raylet`, and `agent`.
- `ray.component_cpu_percentage` for the same components.
- `ray.gcs_task_manager_task_events_stored`, `reported`, and `dropped`
  (`PROFILE_EVENT` and `STATUS_EVENT`). Stored events pinned at a cap, millions
  of reported/dropped events, or GCS log lines such as protobuf responses over
  2GB are strong Ray control-plane metadata-pressure evidence.
- `ray.object_store_memory`, `ray.resources` for logical memory/object store,
  and Ray Data operator metrics for in-flight bytes and backpressure.
- cluster active/dead node counts, to separate one driver/control-plane failure
  from a broad cluster failure.

Then summarize the CPU/container signals:

- container memory usage versus container limit for the driver pod. This is the
  direct cgroup OOM-risk signal.
- RSS/workingset/cache fields where available. High container usage plus high
  Driver/GCS RSS is stronger evidence than container usage alone.
- host memory used versus host total. If host memory is healthy but container
  usage is near the pod limit, call it pod/container memory pressure, not
  physical host OOM.
- OOM counters, memory pressure state, PSI memory, and direct reclaim counters
  such as `pgscan_direct` / `pgsteal_direct`.
- network RX/TX and disk IO only as supporting evidence unless the failing stage
  is clearly IO/download/export-bound.

Use timestamps near the driver death, not only full-window peaks. A useful
summary shape is:

```text
At <death_time>:
- driver/head pod memory: <usage>/<limit>
- Driver RSS: <rss>
- GCS RSS: <rss>
- raylet/agent RSS: <rss>
- GCS task events: stored=<n>, reported=<n>, dropped=<n>
- host memory: <used>/<total>
- OOM counters / memory pressure: <values>
```

Interpretation rules:

- If the driver process exits with code `-9`, there is no Python traceback, and
  the driver pod memory is near its limit while Driver/GCS RSS accounts for most
  of it, call the immediate failure "driver/head pod memory pressure". Do not
  call it host OOM if host memory is still healthy.
- If GCS task events are capped/dropped heavily, GCS logs mention huge protobuf
  replies, and driver RSS grows during `materialize()` or Ray Data execution,
  attribute the underlying cause to Ray Data/control-plane metadata pressure.
- If worker OOM messages identify another node or pod, inspect that worker's
  MetricCPU/MetricRay separately. Do not assume the driver node caused worker
  OOMs.
- If download/RPC operators log many failures but do not raise, treat them as
  contributing log/event pressure unless the stack trace shows a direct
  exception from that operator.

## Data-Juicer Pipeline Parameters

When the user provides a Data-Juicer config path or operator pipeline, read it before making tuning claims. Do not edit it unless the user asks for implementation.

Extract these config surfaces:

- Dataset loader: source, table/path, filters, selected columns, `override_num_blocks`, `concurrency`, `ray_remote_args`.
- Process chain: operator order, `num_proc`, `batch_size`, `num_cpus`, `max_concurrent`, retry/timeout fields, dedup settings.
- Export: target, write concurrency, `ray_remote_args`, partition/write options, table format.
- Cluster: head/worker count, CPU, memory, object store, GPU, autoscaling behavior.

Map Data-Juicer operators to Ray Data stages using `logical_plan` and `extra_metrics.transform_fns`. Fused stages can contain multiple Data-Juicer operators, so attribute bottlenecks to the transform functions, not only the Ray operator name.

Use these parameter checks:

- CPU budget: compare total usable CPU with per-op `num_proc * num_cpus` and Ray Data `cpu_max` / pending resources.
- Download pressure: estimate external concurrency as `num_proc * max_concurrent` for downloader-like operators.
- In-flight bytes: estimate memory pressure from `num_proc * batch_size * average row bytes`, especially for image/audio/video bytes and nested binary/list columns; compare this estimate with Ray Data per-operator current bytes and pending task input/output bytes.
- Blocks: compare `override_num_blocks`, actual blocks, rows/block, bytes/block, and skew percentiles.
- Actor-backed state: compare configured actor/shard/pool counts with actual actor counts from `/logical/actors`; unexpected multiplication can indicate per-task actor creation and possible shared-state correctness issues.
- Export: compare export concurrency with `Write*DataSink` wall time, CPU, pending concurrency, and disk/network metrics.
- Loader: if reads are fast and CPU/memory are low, do not tune Hive/HDFS first; focus on later expensive stages.

Parameter advice should be experimental and specific. Prefer "run A/B with these exact changes and compare duration, spilled bytes, CPU, memory, retries" over broad advice such as "increase resources".

## Bottleneck Heuristics

Use evidence from both Ray Data Overview and Grafana. Avoid declaring a bottleneck from a single chart.

- CPU bottleneck: CPU usage near cluster capacity, resource pending is non-zero, task concurrency is CPU-limited, and increasing CPU should reduce wall time.
- Memory/object-store bottleneck: container usage is persistently close to its limit and RSS is also high or rising, `ray_data_spilled_bytes > 0`, object-store pressure is high, workers restart/OOM, or large binary columns remain in flight across stages. Do not call a hard OOM risk from container usage alone when RSS is much lower and spill/restart signals are absent.
- IO/download bottleneck: CPU is low or moderate, memory is not spilling, but a download or remote-call `MapBatches` stage has high task wall time. Tune request concurrency/timeouts and upstream service pressure before adding CPU.
- Skew: output rows/bytes p95 or max is much larger than p50, or one block/task dominates wall time. Increase/repartition blocks or change partitioning before blindly adding workers.
- Actor pressure: actor count is unexpectedly high compared with config, actor memory is large, or actors restart. If actual actor count is much higher than the configured actor/shard/pool count, suspect per-task actor creation and possible shared-state correctness issues.
- Export bottleneck: `Write*DataSink` or `WriteMagnusDataSink` has high wall time, high pending concurrency, or high CPU. Tune export concurrency and write options separately from processing stages.

Common tuning levers:

- Lower `batch_size` when rows carry image/audio/video bytes or nested binary payloads.
- Lower per-operator `num_proc` if memory is tight and CPU is not saturated.
- Lower downloader `max_concurrent` when remote fetch stages dominate or retries grow.
- Increase `override_num_blocks` only when blocks are too coarse or skewed.
- Increase worker memory before CPU when CPU is low but memory/object-store pressure is high.
- Do not raise Magnus/write concurrency unless write stages are proven bottlenecks.

## Report Shape

Return a concise, evidence-backed report:

```text
RayUI:
<url>

Metrics:
- Ray core: <url>
- Ray data: <url>
- Per-node MetricCPU: head_0 <url>, worker_x <url>, ...

Grafana:
- job-padded time range used: start-5m to end+5m
- business monitoring panels checked first
- CPU/GPU/memory/IO/disk panels checked
- peaks, sustained usage, imbalance, spilling/OOM/restart evidence
- memory: container usage peak/limit, RSS peak/limit, object-store peak, spilled bytes, and whether the evidence is distribution-only or per-pod

Job:
- status / duration / entrypoint
- cluster resources: head + workers, CPU, memory, object store

Ray Data:
- largest duration stage
- largest memory/object-store stage
- bytes spilled
- rows and bytes before/after major filters
- actor anomalies, if any

Data-Juicer pipeline:
- loader, expensive operators, export path
- suspicious parameters and why they matter
- proposed A/B config changes

Conclusion:
- bottleneck classification
- config changes to try next
- what to compare in the next run
```

Mention any boundary clearly: unauthenticated page, missing request body, History Server API unavailable, Grafana not opened, or metrics links blank for terminated nodes.

## Future Extension

TODO: Add debug workflow for failed Ray jobs, stack traces, worker/node failures, logs, and exception root-cause analysis.
TODO: Add a fully automated A/B tuning runner for batch config generation and regression comparison across multiple Merlin trials.
