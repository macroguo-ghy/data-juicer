# Concurrency Tuning Workflow

Use this workflow when the user provides a Data-Juicer YAML and an available Ray resource budget, for example `100 workers, each 8c16g`. The goal is to validate the pipeline, tune only concurrency-related parameters, prove the changes on a small online Ray E2E demo, then scale and observe the production run.

Recommended loop:

```text
intake -> validate yaml -> inspect input -> build demo -> run baseline demo
       -> tune demo -> scale config -> production canary/full run -> observe
```

## Guardrails

- Start from the current checkout and read repository runbooks if present.
- Use `online-ray-e2e` for online Ray submissions and monitoring.
- Do not tune a YAML with logic/config errors. Report the errors and stop.
- Production YAML edits are limited to concurrency-related parameters: source `concurrency`, `override_num_blocks`, `ray_remote_args.num_cpus`; operator `num_proc`, `num_cpus`, `batch_size`; per-task IO concurrency such as `max_concurrent`; and export/write concurrency knobs.
- Do not change business filters, schemas, selected columns, field names, operator order, target tables, output semantics, credentials, or service endpoints unless the user explicitly asks.
- Demo YAMLs may change input paths, sampling/limits, output paths, and resource size to create a bounded representative run, but must preserve the same process chain and business semantics wherever possible.
- Keep each tuning iteration attributable: change one parameter family at a time unless a safety issue requires immediate rollback.
- Keep a written baseline for every run: YAML path, changed knobs, Ray job id, RayUI URL, resource budget, input size, output path, and the bottleneck conclusion.

## 0. Intake

Before editing or submitting jobs, extract or ask for only the missing facts that cannot be discovered locally:

- Production resource budget: head/worker count, cores, memory, GPU, autoscaling, and queue limits.
- Tuning objective: reduce wall time, reduce cost, avoid OOM/spill, meet SLA, or find the external cap.
- Allowed run budget: maximum demo iterations and whether a production canary is allowed before the full run.
- External budgets: RPC/download QPS, timeout/retry policy, downstream write limits, and expected failure-rate tolerance.
- Correctness invariants: expected output schema, target paths/tables, row-count expectations, and filters that must not change.

If these are missing, make conservative assumptions and state them in the run report. Do not block on information that can be measured from the YAML, source metadata, RayUI, or logs.

## 1. Validate The YAML First

Read the YAML and load the real operator path before proposing tuning:

- Parse config with the repo's current config loader.
- Load operators with the same path used by the Ray executor, not only `load_configs_only`.
- Check custom operator paths, import/runtime env, required fields, operator order, source/export config, target path/table conflicts, and invalid combinations such as unsupported checkpoint/export modes.
- Check resource requests: `num_proc * num_cpus`, actor pool sizes, GPU flags, object-store-heavy stages, and any per-task IO concurrency.
- Check obvious field-flow errors: filters before fields are produced, dedup keys missing, binary fields retained across global shuffle, target columns absent, or `resume_download` conflicts.
- Check output safety: production paths/tables are not used by demo runs, overwrite/append behavior is intentional, and fan-out targets or partition overwrites cannot corrupt existing data.

If validation finds a logic or low-level config error, answer with the issue and the exact evidence. Do not create tuning YAMLs until the pipeline is logically valid.

## 2. Inspect Input Dataset Size

Before generating a demo, inspect the source:

- HDFS/parquet/jsonl: use `ExecuteHdfsCommand` or `hdfs dfs -count -h`, `-du -h`, and `-ls` to get total bytes, file count, representative file sizes, and whether paths exist.
- Lance: use `GetLanceTableSchema`; when available, use metadata/table stats or a small `ReadLanceTable` probe for row count, fragments/files, and schema.
- Hive/Magnus/catalog sources: use catalog/schema/detail tools to find selected columns, partition filters, table location, file count/size when exposed, and whether the configured partition exists.
- Remote URL workloads: estimate row count and URL count from source columns if possible; treat external service QPS as a separate budget.

Record total size, file count, expected rows, selected columns, average file/block size, and any skew or very large files. If the input cannot be inspected due to auth/tooling, state that boundary and use the smallest safe probe.

Sampling guidance:

- Prefer representative files over the first files returned by `ls`: include small/median/large files or several partitions when skew is visible.
- If the pipeline depends on partition/date/source distribution, sample across those groups instead of taking adjacent files.
- If only a table source is available, create a bounded source using the platform's native filter/limit/sampling mechanism, and preserve selected columns and casts.
- For global shuffle/dedup workloads, a tiny 2-3 file demo is good for correctness and per-row cost, but not enough to prove shuffle scaling. Add a medium demo if the small run hides skew or all-to-all cost.

## 3. Build A Small Demo YAML

Create a sibling demo YAML with a clear suffix such as `_concurrency_demo.yaml`.

Demo rules:

- Read only a few representative files, usually 2-3 parquet files, or use a small bounded source that still exercises the real operators.
- Keep the same process chain, field mappings, dedup keys, RPC mappers, and export type when feasible.
- Use temporary output paths/tables that cannot overwrite production data.
- Use a small Ray resource request, typically 2-4 workers unless the operator requires more parallelism to be meaningful.
- Set `override_num_blocks` high enough to exercise parallelism on the demo, but bounded enough to avoid scheduler-only noise.
- Scale initial `num_proc` to the demo worker CPU budget and keep per-task IO concurrency conservative.
- Preserve correctness checks: output schema, required fields, expected filters, and representative row samples should match production semantics.

Useful initial sizing:

```text
usable_worker_cpus = workers * cores_per_worker * 0.7 to 0.85
cpu_bound_num_proc <= usable_worker_cpus / max(num_cpus, 1)
io_effective_concurrency ~= active_ray_tasks * max_concurrent
target_blocks ~= max(file_count, target_task_concurrency * 2 to 4)
binary_window ~= avg_binary_bytes_per_row * batch_size * active_tasks * amplification
```

For demo runs, prefer stable attribution over maximum throughput.

Create two demo configs when useful:

- Smoke demo: the smallest safe input, used to prove config/operator/export correctness.
- Tuning demo: enough rows/files/blocks to exercise the likely bottleneck and make Ray Data metrics meaningful.

## 4. Run A Baseline Demo

Submit the demo YAML through `online-ray-e2e` once without tuning changes beyond the minimum needed to fit the demo resource budget. This baseline is the comparison point for every later change.

During and after each demo run, capture:

- Ray job status, duration, and driver errors.
- Ray Data dataset/operator rows in/out, bytes in/out, block counts, progress, queued blocks, running/active concurrency, task duration average/p50/p95/max, current bytes, spilled bytes, and logical plan/transform functions.
- Cluster CPU usage and pending resources.
- Memory/container RSS, object store usage, spill, disk IO, and worker restarts/OOM.
- Network/RPC/download business metrics: QPS, latency, timeout, retry, success/failure counts, and known service limits.
- Export/write duration and throughput if export is part of the critical path.

Use stage-first attribution. Ray Data may fuse multiple Data-Juicer operators, so inspect `logical_plan`, `extra_metrics.transform_fns`, and logs before assigning a bottleneck.

Baseline gate:

- If the baseline demo fails due to logic, schema, permissions, missing dependencies, or output safety, stop and report the failure instead of tuning.
- If output rows/schema or required fields differ from expected production semantics, fix the demo sampling/config first.
- If metrics are missing, collect Ray job logs and worker logs before deciding that a stage is healthy.

## 5. Decide The Next Demo Iteration

Use these rules:

- CPU-bound: CPU near capacity, no spill/OOM, Ray Data tasks running at the configured limit, and task duration improves with more CPU. Increase `num_proc` or workers, or reduce `num_cpus` only if each task is over-reserving CPU.
- Under-parallelized: CPU low, memory healthy, few active tasks, and blocks are exhausted or too coarse. Increase `override_num_blocks` or the bottleneck operator's `num_proc`.
- IO/RPC-bound: CPU low/moderate, memory healthy, high task wall time, and latency/retries/QPS caps dominate. Tune `max_concurrent`, `num_proc`, timeout/backoff only if allowed, and respect service QPS limits.
- Memory/object-store-bound: high RSS/container usage, object store pressure, spill, worker restarts, or large binary columns in flight. Lower `batch_size`, `num_proc`, or `max_concurrent`; reduce data carried into shuffle; or require more memory per worker.
- Shuffle/dedup-bound: all-to-all stages such as sort/groupby dominate. Reduce pre-shuffle bytes, improve key distribution, tune block counts, and avoid carrying binary/list payloads across the shuffle.
- Export-bound: write sink dominates while processing is healthy. Tune export/write concurrency separately from mapper concurrency.

Iteration discipline:

- Change one parameter family per run: block/read concurrency, one operator's Ray task concurrency, per-task IO concurrency, batch size, or export concurrency.
- Prefer geometric changes such as `0.5x`, `1.5x`, or `2x` over small noisy edits.
- Roll back immediately when correctness changes, failure rate grows beyond tolerance, spill/OOM appears, or an external service cap is exceeded.
- Keep the best-known-good config, not only the latest config.

Stop demo iteration when the run has no obvious stage bottleneck, the next bottleneck is an external cap, or further improvement would require business-logic/operator changes outside the allowed scope. Also stop when the allowed iteration budget is exhausted and report the best-known-good setting.

## 6. Scale To The Production YAML

Do not blindly multiply demo settings by cluster size. Scale by bottleneck:

- Keep per-node memory/object-store pressure no higher than the healthy demo run.
- Production `override_num_blocks` should normally exceed the intended bottleneck-stage concurrency by `2x-4x`, while avoiding tiny blocks and excessive shuffle overhead.
- CPU-bound operators can scale with usable cluster cores until Ray Data or downstream stages stop improving.
- IO/RPC operators must respect global service budget:

```text
max_concurrent_per_task <= service_qps_budget / active_ray_tasks
```

- Binary-heavy operators should scale by memory window, not CPU count.
- Global shuffle/dedup stages need enough blocks for parallelism but should prioritize lower input bytes and lower skew.

Apply only concurrency-related diffs to the production YAML. Keep a separate demo YAML unless the user asks to delete it.

Production gate before submission:

- Show the exact production YAML diff and confirm every changed key is a concurrency/resource key.
- Re-run config/operator loading on the production YAML.
- Ensure demo output paths/tables did not leak into production.
- Ensure production output mode, partition filters, target tables, and credentials are unchanged unless explicitly requested.
- If demo evidence came only from a tiny smoke sample, label the production run as a canary or risk-managed first run rather than a fully validated scale-up.

## 7. Submit And Observe Production

Submit the production YAML through `online-ray-e2e` and monitor the same metrics as the demo. Prefer a production canary or bounded run first when the input is much larger than the demo or the pipeline has all-to-all/shuffle/export-heavy stages. If production exposes a new bottleneck, make another scoped concurrency adjustment and rerun or resume observation. Stop when:

- no stage has a clear avoidable bottleneck,
- resource use is stable with no spill/OOM/restart risk,
- external RPC/download limits are the remaining bottleneck,
- or further improvements require non-concurrency code/config changes.

Do not declare success only because the Ray job started. The run is healthy only when Ray Data progresses through the intended stages, output writes complete or reach the expected steady state, and the observed bottleneck classification is supported by Ray Data plus system/business metrics.

## Report Shape

Return a compact report:

```text
Validation:
- YAML logic/config status
- blocked issues, if any

Input:
- source, size, file count, row estimate, selected columns/schema

Resources:
- demo cluster budget
- production cluster budget

Demo runs:
- run id / RayUI
- YAML diff
- baseline versus tuned comparison
- stage bottleneck evidence
- CPU/memory/object-store/RPC/export evidence
- decision for next iteration

Production:
- YAML diff
- run id / RayUI
- canary/full-run status
- observed bottleneck or stable state

Conclusion:
- final concurrency parameters
- best-known-good settings and rejected settings
- remaining bottleneck or external cap
- what was not changed because it is outside concurrency scope
```
