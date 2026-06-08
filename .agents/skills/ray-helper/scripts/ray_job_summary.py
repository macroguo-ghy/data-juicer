#!/usr/bin/env python3
"""Token-efficient Ray job and Ray Data summary helper for agents."""

from __future__ import annotations

import argparse
import base64
import json
import re
import shlex
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, NamedTuple, Optional


SECRET_KEYS = {
    "ak",
    "sk",
    "token",
    "password",
    "secret",
    "private_key",
    "git_private_key",
    "authorization",
    "cookie",
    "byted_ray_token",
    "ray.io/identify-token",
}

FINAL_JOB_STATUSES = {"SUCCEEDED", "FAILED", "STOPPED"}

JOB_KEYS = [
    "type",
    "job_id",
    "submission_id",
    "status",
    "message",
    "error_type",
    "start_time",
    "end_time",
    "driver_exit_code",
    "byted_ray_user",
]

CONTEXT_KEYS = [
    "target_max_block_size",
    "target_shuffle_max_block_size",
    "target_min_block_size",
    "streaming_read_buffer_size",
    "scheduling_strategy",
    "large_args_threshold",
    "data_checkpoint_dir",
    "data_checkpoint_write_interval",
    "data_delete_no_checkpoint_files",
    "streaming_executor_split_num",
    "max_num_blocks_to_dispatch_once",
    "max_num_blocks_for_streaming_stats",
    "_max_num_blocks_in_streaming_gen_buffer",
]

DATASET_CONFIG_KEYS = [
    "type",
    "source",
    "path",
    "format",
    "read_mode",
    "max_result_rows",
    "override_num_blocks",
    "concurrency",
    "num_cpus",
    "skip_zero_row_group_files",
]

PROCESS_KEYS = [
    "batch_size",
    "num_proc",
    "num_cpus",
    "qps",
    "max_concurrent",
    "dedup_set_num",
    "condition",
    "filter_condition",
    "field_key",
    "id_key",
    "download_field",
    "save_field",
    "url_key",
    "bytes_key",
    "md5_key",
    "valid_count_key",
    "ak",
    "sk",
]


class ParsedRayJob(NamedTuple):
    api_base: str
    job_id: str


def _json_get(url: str, timeout: int = 30) -> Dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def _is_godel_url(url: str) -> bool:
    return "godel-stream-applications.byted.org" in urllib.parse.urlparse(url).netloc


def _godel_history_lookup_name(url: str) -> Optional[str]:
    parsed = urllib.parse.urlparse(url)
    parts = [urllib.parse.unquote(part) for part in parsed.path.strip("/").split("/") if part]
    if len(parts) < 2:
        return None

    app_segment, dashboard_segment = parts[0], parts[1]
    if "-" not in app_segment:
        return None
    cluster, dc = app_segment.rsplit("-", 1)

    if dashboard_segment.endswith("-dashboard"):
        dashboard_segment = dashboard_segment[: -len("-dashboard")]
    if dashboard_segment.endswith("-ray-on-godel"):
        name = dashboard_segment[: -len("-ray-on-godel")]
    elif "-" in dashboard_segment:
        name = dashboard_segment.rsplit("-", 1)[0]
    else:
        name = dashboard_segment

    if not cluster or not dc or not name:
        return None
    return f"{name}-{dc}-{cluster}"


def _event_log_candidates(payload: Dict[str, Any]) -> List[str]:
    logs = payload.get("data", {}).get("eventlogs") or payload.get("eventlogs") or []
    logs = [log for log in logs if isinstance(log, dict) and log.get("name")]
    logs.sort(key=lambda log: (log.get("lastUpdate") or 0, bool(log.get("isCurrent"))), reverse=True)
    return [str(log["name"]) for log in logs]


def _fetch_payloads(api_base: str, job_id: str, timeout: int) -> tuple[Dict[str, Any], Dict[str, Any]]:
    job_payload = _json_get(f"{api_base}/api/jobs/{job_id}", timeout=timeout)
    dataset_payload = _json_get(f"{api_base}/api/data/datasets", timeout=timeout)
    return job_payload, dataset_payload


def _resolve_godel_history_api_base(url: str, job_id: str, timeout: int) -> Optional[str]:
    lookup_name = _godel_history_lookup_name(url)
    if not lookup_name:
        return None

    event_logs = _json_get(
        f"https://ray-history-server.byted.org/v2/history/{lookup_name}/api/event_logs",
        timeout=timeout,
    )
    for history_key in _event_log_candidates(event_logs):
        api_base = f"https://ray-history-server.byted.org/history/{history_key}"
        try:
            job_payload = _json_get(f"{api_base}/api/jobs/{job_id}", timeout=timeout)
        except Exception:
            continue
        if str(job_payload.get("job_id")) == str(job_id):
            return api_base
    return None


def _datasets_finished(payload: Dict[str, Any]) -> bool:
    datasets = payload.get("datasets")
    if datasets is None and isinstance(payload.get("data"), dict):
        datasets = payload["data"].get("datasets")
    if not datasets:
        return False
    return all(isinstance(dataset, dict) and dataset.get("state") == "FINISHED" for dataset in datasets)


def _is_final_job_payload(payload: Dict[str, Any]) -> bool:
    return (
        payload.get("status") in FINAL_JOB_STATUSES
        or payload.get("end_time") is not None
        or payload.get("driver_exit_code") is not None
    )


def _is_stale_godel_running_job(job_payload: Dict[str, Any], dataset_payload: Dict[str, Any]) -> bool:
    return (
        job_payload.get("status") == "RUNNING"
        and not _is_final_job_payload(job_payload)
        and _datasets_finished(dataset_payload)
    )


def _strip_suffix_operator_index(name: str) -> str:
    return re.sub(r"_\d+$", "", name or "")


def parse_ray_job_url(url: str, job_id: Optional[str] = None) -> ParsedRayJob:
    parsed = urllib.parse.urlparse(url)
    scheme_netloc = f"{parsed.scheme}://{parsed.netloc}"

    if "ray-history-server.byted.org" in parsed.netloc:
        if parsed.fragment:
            parts = [urllib.parse.unquote(part) for part in parsed.fragment.strip("/").split("/")]
            if "history" not in parts:
                raise ValueError(f"Cannot find history segment in Ray History URL: {url}")
            history_idx = parts.index("history")
            cluster = parts[history_idx + 1]
            if job_id is None and "jobs" in parts:
                jobs_idx = parts.index("jobs")
                if jobs_idx + 1 < len(parts):
                    job_id = parts[jobs_idx + 1]
            if not job_id:
                raise ValueError(f"Cannot find job id in Ray History URL: {url}")
            return ParsedRayJob(f"{scheme_netloc}/history/{cluster}", job_id)

        path_parts = [urllib.parse.unquote(part) for part in parsed.path.strip("/").split("/")]
        if path_parts and path_parts[0] == "history":
            if "api" in path_parts:
                api_idx = path_parts.index("api")
                cluster = "/".join(path_parts[1:api_idx])
            else:
                cluster = "/".join(path_parts[1:])
            if job_id is None and "jobs" in path_parts:
                jobs_idx = path_parts.index("jobs")
                if jobs_idx + 1 < len(path_parts):
                    job_id = path_parts[jobs_idx + 1]
            if not cluster or not job_id:
                raise ValueError(f"Cannot parse Ray History API URL: {url}")
            return ParsedRayJob(f"{scheme_netloc}/history/{cluster}", job_id)

    if "godel-stream-applications.byted.org" in parsed.netloc:
        if job_id is None and parsed.fragment:
            parts = [urllib.parse.unquote(part) for part in parsed.fragment.strip("/").split("/")]
            if "jobs" in parts:
                jobs_idx = parts.index("jobs")
                if jobs_idx + 1 < len(parts):
                    job_id = parts[jobs_idx + 1]
        base_path = parsed.path
        if "/api/" in base_path:
            base_path = base_path.split("/api/", 1)[0]
        base_path = base_path.rstrip("/")
        if not job_id:
            raise ValueError(f"Cannot find job id in Godel Ray Dashboard URL: {url}")
        return ParsedRayJob(f"{scheme_netloc}{base_path}", job_id)

    raise ValueError(f"Unsupported Ray job URL: {url}")


def _is_secret_key(key: Any) -> bool:
    lowered = str(key).lower()
    return lowered in SECRET_KEYS or lowered.endswith("_token") or lowered.endswith("_secret")


def _truncate_string(value: str, limit: int = 300) -> str:
    if len(value) <= limit:
        return value
    return f"{value[:limit]}...<truncated {len(value) - limit} chars>"


def redact_value(value: Any, *, key: Any = None, max_list: int = 20) -> Any:
    if _is_secret_key(key):
        return "<redacted>"
    if isinstance(value, dict):
        return {k: redact_value(v, key=k, max_list=max_list) for k, v in value.items()}
    if isinstance(value, list):
        clipped = [redact_value(v, max_list=max_list) for v in value[:max_list]]
        if len(value) > max_list:
            clipped.append(f"<truncated {len(value) - max_list} items>")
        return clipped
    if isinstance(value, str):
        return _truncate_string(value)
    return value


def _select(data: Dict[str, Any], keys: Iterable[str]) -> Dict[str, Any]:
    return {key: redact_value(data[key], key=key) for key in keys if key in data}


def _yaml_load(raw: str) -> Dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("PyYAML is required to decode Data-Juicer YAML configs.") from exc
    loaded = yaml.safe_load(raw)
    return loaded or {}


def _extract_config_base64(entrypoint: str) -> Optional[str]:
    try:
        parts = shlex.split(entrypoint)
    except ValueError:
        parts = entrypoint.split()
    for idx, part in enumerate(parts):
        if part == "--config-base64" and idx + 1 < len(parts):
            return parts[idx + 1]
        if part.startswith("--config-base64="):
            return part.split("=", 1)[1]
    match = re.search(r"--config-base64\s+(\S+)", entrypoint)
    return match.group(1) if match else None


def decode_config_from_entrypoint(entrypoint: str) -> Optional[Dict[str, Any]]:
    encoded = _extract_config_base64(entrypoint or "")
    if not encoded:
        return None
    raw = base64.b64decode(encoded).decode("utf-8")
    return _yaml_load(raw)


def summarize_process_step(step: Any) -> Dict[str, Any]:
    if not isinstance(step, dict) or not step:
        return {"operator": type(step).__name__}
    operator, cfg = next(iter(step.items()))
    summary: Dict[str, Any] = {"operator": operator}
    if isinstance(cfg, dict):
        for key in PROCESS_KEYS:
            if key in cfg:
                summary[key] = redact_value(cfg[key], key=key)
    return summary


def summarize_export(export_cfg: Any) -> Any:
    if not isinstance(export_cfg, dict):
        return redact_value(export_cfg)
    summary = redact_value(export_cfg)
    schema = export_cfg.get("schema")
    if isinstance(schema, dict) and isinstance(schema.get("fields"), list):
        fields = schema["fields"]
        summary["schema"] = {
            "field_count": len(fields),
            "field_names": [
                field.get("name") for field in fields if isinstance(field, dict) and "name" in field
            ],
        }
    return summary


def summarize_config(config: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if config is None:
        return None
    dataset_cfg = config.get("dataset")
    dataset_summary = redact_value(dataset_cfg)
    if isinstance(dataset_cfg, dict) and isinstance(dataset_cfg.get("configs"), list):
        dataset_summary = {
            "configs": [
                _select(cfg, DATASET_CONFIG_KEYS) if isinstance(cfg, dict) else redact_value(cfg)
                for cfg in dataset_cfg["configs"]
            ]
        }

    return {
        "project_name": config.get("project_name"),
        "executor_type": config.get("executor_type"),
        "ray_address": config.get("ray_address"),
        "min_common_dep_num_to_combine": config.get("min_common_dep_num_to_combine"),
        "ray_data_checkpoint": redact_value(config.get("ray_data_checkpoint")),
        "ray_data_context": redact_value(config.get("ray_data_context")),
        "dataset": dataset_summary,
        "process": [summarize_process_step(step) for step in config.get("process", [])],
        "export": summarize_export(config.get("export")),
    }


def _metric_value(metric: Any, field: str = "value") -> Any:
    if isinstance(metric, dict):
        return metric.get(field)
    return None


def _dist_summary(dist: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(dist, dict):
        return None
    keys = ["count", "zeros", "zero_count", "avg", "min", "p50", "p95", "p99", "max", "drop"]
    return {key: dist[key] for key in keys if key in dist}


def summarize_operator(operator: Dict[str, Any]) -> Dict[str, Any]:
    rows_dist = _dist_summary(operator.get("ray_data_output_rows_dist"))
    bytes_dist = _dist_summary(operator.get("ray_data_output_bytes_dist"))
    extra = operator.get("extra_metrics") or {}
    summary: Dict[str, Any] = {
        "operator": operator.get("operator") or operator.get("name"),
        "name": operator.get("name"),
        "state": operator.get("state"),
        "progress": operator.get("progress"),
        "total": operator.get("total"),
        "total_rows": operator.get("total_rows"),
        "queued_blocks": operator.get("queued_blocks"),
        "avg_output_rows": rows_dist.get("avg") if rows_dist else None,
        "avg_output_bytes": bytes_dist.get("avg") if bytes_dist else None,
        "output_rows_dist": rows_dist,
        "output_bytes_dist": bytes_dist,
        "current_bytes": operator.get("ray_data_current_bytes"),
        "cpu_usage_cores": operator.get("ray_data_cpu_usage_cores"),
        "concurrency": {
            "running": _metric_value(operator.get("ray_data_num_concurrency_running")),
            "active": _metric_value(operator.get("ray_data_num_concurrency_active")),
            "pending": _metric_value(operator.get("ray_data_num_concurrency_pending")),
            "min": _metric_value(operator.get("ray_data_min_concurrency")),
            "max": _metric_value(operator.get("ray_data_max_concurrency")),
        },
        "class": extra.get("class"),
        "min_rows_per_bundle": extra.get("min_rows_per_bundle"),
        "ray_remote_args": redact_value(extra.get("ray_remote_args")),
        "transform_fns": extra.get("transform_fns"),
    }
    return {key: value for key, value in summary.items() if value is not None}


def summarize_dataset_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    datasets = payload.get("datasets")
    if datasets is None and isinstance(payload.get("data"), dict):
        datasets = payload["data"].get("datasets")
    if datasets is None:
        datasets = []

    result = []
    for dataset in datasets:
        context = dataset.get("context") if isinstance(dataset, dict) else None
        result.append(
            {
                "dataset": dataset.get("dataset"),
                "job_id": dataset.get("job_id"),
                "state": dataset.get("state"),
                "progress": dataset.get("progress"),
                "total": dataset.get("total"),
                "total_rows": dataset.get("total_rows"),
                "start_time": dataset.get("start_time"),
                "end_time": dataset.get("end_time"),
                "last_update_time": dataset.get("last_update_time"),
                "context": _select(context or {}, CONTEXT_KEYS),
                "operators": [
                    summarize_operator(operator)
                    for operator in dataset.get("operators", [])
                    if isinstance(operator, dict)
                ],
            }
        )
    return {"datasets": result}


def summarize_job_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    runtime_env = payload.get("runtime_env") or {}
    metadata = payload.get("metadata") or {}
    return {
        **_select(payload, JOB_KEYS),
        "runtime_env": {
            "working_dir": runtime_env.get("working_dir"),
            "env_vars": redact_value(runtime_env.get("env_vars")),
        },
        "metadata_keys": sorted(metadata.keys()),
    }


def build_summary(job_payload: Dict[str, Any], dataset_payload: Dict[str, Any]) -> Dict[str, Any]:
    config = decode_config_from_entrypoint(job_payload.get("entrypoint", ""))
    dataset_summary = summarize_dataset_payload(dataset_payload)
    return {
        "job": summarize_job_payload(job_payload),
        "config": summarize_config(config),
        **dataset_summary,
    }


def fetch_summary(url: str, job_id: Optional[str] = None, timeout: int = 30) -> Dict[str, Any]:
    parsed = parse_ray_job_url(url, job_id=job_id)
    if _is_godel_url(url):
        try:
            job_payload, dataset_payload = _fetch_payloads(parsed.api_base, parsed.job_id, timeout)
        except Exception as exc:
            try:
                history_api_base = _resolve_godel_history_api_base(url, parsed.job_id, timeout)
            except Exception:
                history_api_base = None
            if not history_api_base:
                raise exc
            job_payload, dataset_payload = _fetch_payloads(history_api_base, parsed.job_id, timeout)
            return build_summary(job_payload, dataset_payload)

        if _is_stale_godel_running_job(job_payload, dataset_payload):
            try:
                history_api_base = _resolve_godel_history_api_base(url, parsed.job_id, timeout)
            except Exception:
                history_api_base = None
            if history_api_base:
                history_job_payload, history_dataset_payload = _fetch_payloads(
                    history_api_base,
                    parsed.job_id,
                    timeout,
                )
                if _is_final_job_payload(history_job_payload):
                    return build_summary(history_job_payload, history_dataset_payload)

        return build_summary(job_payload, dataset_payload)

    job_payload, dataset_payload = _fetch_payloads(parsed.api_base, parsed.job_id, timeout)
    return build_summary(job_payload, dataset_payload)


def _format_bytes(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(value)
    for unit in units:
        if abs(size) < 1024 or unit == units[-1]:
            return f"{size:.1f}{unit}"
        size /= 1024
    return str(value)


def format_markdown(summary: Dict[str, Any]) -> str:
    lines: List[str] = []
    job = summary.get("job") or {}
    lines.append("# Ray Job Summary")
    lines.append("")
    lines.append(
        f"- job: `{job.get('job_id', '-')}` status=`{job.get('status', '-')}` "
        f"start=`{job.get('start_time', '-')}` end=`{job.get('end_time', '-')}`"
    )
    runtime = job.get("runtime_env") or {}
    if runtime.get("working_dir"):
        lines.append(f"- working_dir: `{runtime['working_dir']}`")
    config = summary.get("config") or {}
    if config:
        lines.append(f"- project: `{config.get('project_name')}` executor=`{config.get('executor_type')}`")
        lines.append(f"- checkpoint: `{config.get('ray_data_checkpoint')}`")
        export = config.get("export") or {}
        if isinstance(export, dict):
            lines.append(
                f"- export: target=`{export.get('target')}` type=`{export.get('type')}` "
                f"mode=`{export.get('mode')}` path=`{export.get('path')}`"
            )
    for dataset in summary.get("datasets", []):
        lines.append("")
        lines.append(f"## Dataset `{dataset.get('dataset')}` state=`{dataset.get('state')}`")
        context = dataset.get("context") or {}
        if context:
            lines.append(f"- context: `{json.dumps(context, ensure_ascii=False)}`")
        lines.append("")
        lines.append("| operator | state | progress/total | avg rows | avg bytes | min_rows_per_bundle |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for op in dataset.get("operators", []):
            lines.append(
                f"| `{op.get('operator')}` | {op.get('state', '-')} | "
                f"{op.get('progress', '-')}/{op.get('total', '-')} | "
                f"{op.get('avg_output_rows', '-')} | {_format_bytes(op.get('avg_output_bytes'))} | "
                f"{op.get('min_rows_per_bundle', '-')} |"
            )
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="Ray History or Godel Ray Dashboard job URL")
    parser.add_argument("--job-id", default=None, help="Override or provide job id")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--format", choices=["json", "markdown"], default="markdown")
    args = parser.parse_args(argv)

    summary = fetch_summary(args.url, job_id=args.job_id, timeout=args.timeout)
    if args.format == "json":
        print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True))
    else:
        print(format_markdown(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
