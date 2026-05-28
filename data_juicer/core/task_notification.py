from __future__ import annotations

import json
import os
import re
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable

from loguru import logger

from data_juicer.utils.http_utils import HttpClient
from data_juicer.utils.ray_task_kv_store import incr_task_kv, snapshot_task_kv

ADC_LARK_MESSAGE_PATH = "/openapi/lark/message/template-card/send-to-user"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_STATUS_CODES = (420, 429, 500, 502, 503, 504)
MIN_INTERVAL_SECONDS = 30
RUNTIME_STATS_NAMESPACE = "runtime_stats"
SNAPSHOT_FILENAME = "notification_snapshot.json"

_INTERVAL_RE = re.compile(r"^([1-9]\d*)(s|min|h)$")


@dataclass
class TaskProgressSnapshot:
    job_id: str | None
    project_name: str | None
    executor_type: str | None
    status: str
    phase: str
    start_time: float
    elapsed_seconds: float
    export_path: str | None
    output_rows: int | None
    output_bytes: int | None
    output_files: int | None
    custom_stats: dict[str, Any]
    export_targets: list[dict[str, Any]] | None = None
    error_summary: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RuntimeStatsCollector:
    def __init__(self, namespace: str = RUNTIME_STATS_NAMESPACE):
        self.namespace = namespace

    def increment(self, key: str, delta: int | float = 1) -> None:
        if delta == 0:
            return
        incr_task_kv(key, delta, namespace=self.namespace, wait=False)

    def snapshot(self) -> dict[str, Any]:
        return dict(snapshot_task_kv(namespace=self.namespace) or {})


class AdcLarkMessageNotificationHook:
    def __init__(self, hook_cfg: dict[str, Any]):
        self.hook_cfg = _plain_dict(hook_cfg)
        self.ctx = _require_dict(self.hook_cfg.get("ctx"), "notification_hooks[].ctx")
        self.api_base = str(_require_value(self.ctx.get("apiBase"), "ctx.apiBase")).rstrip("/")
        self.user_account = str(_require_value(self.ctx.get("userAccount"), "ctx.userAccount"))
        self.template_id = str(_require_value(self.hook_cfg.get("template_id"), "notification_hooks[].template_id"))
        self.timeout = float(self.hook_cfg.get("timeout", DEFAULT_TIMEOUT_SECONDS))
        self.retry_attempts = int(self.hook_cfg.get("retry_attempts", DEFAULT_RETRY_ATTEMPTS))
        if self.retry_attempts < 0:
            raise ValueError("notification_hooks[].retry_attempts must be non-negative")

    def send(self, snapshot: TaskProgressSnapshot, *, event: str) -> None:
        payload = {
            "userEmailOrAccount": self.user_account,
            "templateId": self.template_id,
            "templateVariable": self._build_template_variable(snapshot, event=event),
        }
        client = HttpClient(
            endpoint=f"{self.api_base}/{ADC_LARK_MESSAGE_PATH.lstrip('/')}",
            method="POST",
            headers=self._base_headers(),
            timeout=self.timeout,
            retry_attempts=self.retry_attempts,
            retry_status_codes=DEFAULT_RETRY_STATUS_CODES,
            retry_on_timeout=False,
        )
        result = client.request(json_body=payload)
        if not result["ok"]:
            raise ValueError(f"ADC lark message request failed: {result['error']}")
        self._validate_openapi_result(result.get("data"))

    def _build_template_variable(self, snapshot: TaskProgressSnapshot, *, event: str) -> dict[str, Any]:
        snapshot_dict = snapshot.to_dict()
        custom_fields = dict(self.hook_cfg.get("custom_fields") or {})
        custom_stats = dict(snapshot.custom_stats or {})
        custom_stats_for_template = self._custom_stats_for_template(custom_stats)
        custom_stats_text = _format_custom_stats_text(custom_stats_for_template)
        elapsed_text = _format_duration(snapshot.elapsed_seconds)
        output_bytes_text = _format_bytes(snapshot.output_bytes)
        output_rows_text = _format_optional_value(snapshot.output_rows)
        output_files_text = _format_optional_value(snapshot.output_files)
        export_path_text = _format_optional_value(snapshot.export_path)
        export_targets_text = _format_export_targets_text(snapshot.export_targets)
        error_summary_text = snapshot.error_summary or "无"
        ray_ui_url = _format_url_variable(custom_fields.get("rayUiUrl") or custom_fields.get("ray_ui_url"))
        driver_log_url = _format_url_variable(custom_fields.get("driverLogUrl") or custom_fields.get("driver_log_url"))
        output_url = _format_url_variable(custom_fields.get("outputUrl") or custom_fields.get("output_url"))
        template_variable = {
            "event": event,
            "status": snapshot.status,
            "statusText": _status_text(snapshot.status),
            "status_text": _status_text(snapshot.status),
            "phase": snapshot.phase,
            "phaseText": snapshot.phase,
            "phase_text": snapshot.phase,
            "phaseProgress": _phase_progress(snapshot.phase, snapshot.status),
            "phase_progress": _phase_progress(snapshot.phase, snapshot.status),
            "user": self.user_account,
            "jobId": snapshot.job_id,
            "job_id": snapshot.job_id,
            "projectName": snapshot.project_name,
            "project_name": snapshot.project_name,
            "executorType": snapshot.executor_type,
            "executor_type": snapshot.executor_type,
            "startTime": snapshot.start_time,
            "start_time": snapshot.start_time,
            "elapsedSeconds": snapshot.elapsed_seconds,
            "elapsed_seconds": snapshot.elapsed_seconds,
            "elapsed": elapsed_text,
            "elapsedText": elapsed_text,
            "elapsed_text": elapsed_text,
            "exportPath": snapshot.export_path,
            "export_path": snapshot.export_path,
            "exportPathText": export_path_text,
            "export_path_text": export_path_text,
            "exportTargets": snapshot.export_targets or [],
            "export_targets": snapshot.export_targets or [],
            "exportTargetsText": export_targets_text,
            "export_targets_text": export_targets_text,
            "outputRows": snapshot.output_rows,
            "output_rows": snapshot.output_rows,
            "outputRowsText": output_rows_text,
            "output_rows_text": output_rows_text,
            "outputFiles": snapshot.output_files,
            "output_files": snapshot.output_files,
            "outputFilesText": output_files_text,
            "output_files_text": output_files_text,
            "outputBytes": snapshot.output_bytes,
            "output_bytes": snapshot.output_bytes,
            "outputBytesText": output_bytes_text,
            "output_bytes_text": output_bytes_text,
            "errorSummary": snapshot.error_summary or "",
            "error_summary": snapshot.error_summary or "",
            "errorSummaryText": error_summary_text,
            "error_summary_text": error_summary_text,
            "hasError": bool(snapshot.error_summary),
            "has_error": bool(snapshot.error_summary),
            "rayUiUrl": ray_ui_url,
            "ray_ui_url": ray_ui_url,
            "rayUiUrlText": ray_ui_url.get("url", ""),
            "ray_ui_url_text": ray_ui_url.get("url", ""),
            "driverLogUrl": driver_log_url,
            "driver_log_url": driver_log_url,
            "driverLogUrlText": driver_log_url.get("url", ""),
            "driver_log_url_text": driver_log_url.get("url", ""),
            "outputUrl": output_url,
            "output_url": output_url,
            "outputUrlText": output_url.get("url", ""),
            "output_url_text": output_url.get("url", ""),
            "runtime": {
                "jobId": snapshot.job_id,
                "job_id": snapshot.job_id,
                "projectName": snapshot.project_name,
                "project_name": snapshot.project_name,
                "executorType": snapshot.executor_type,
                "executor_type": snapshot.executor_type,
                "startTime": snapshot.start_time,
                "start_time": snapshot.start_time,
                "elapsedSeconds": snapshot.elapsed_seconds,
                "elapsed_seconds": snapshot.elapsed_seconds,
            },
            "exportSummary": {
                "exportPath": snapshot.export_path,
                "export_path": snapshot.export_path,
                "exportPathText": export_path_text,
                "export_path_text": export_path_text,
                "exportTargets": snapshot.export_targets or [],
                "export_targets": snapshot.export_targets or [],
                "exportTargetsText": export_targets_text,
                "export_targets_text": export_targets_text,
                "outputRows": snapshot.output_rows,
                "output_rows": snapshot.output_rows,
                "outputRowsText": output_rows_text,
                "output_rows_text": output_rows_text,
                "outputFiles": snapshot.output_files,
                "output_files": snapshot.output_files,
                "outputFilesText": output_files_text,
                "output_files_text": output_files_text,
                "outputBytes": snapshot.output_bytes,
                "output_bytes": snapshot.output_bytes,
                "outputBytesText": output_bytes_text,
                "output_bytes_text": output_bytes_text,
            },
            "customFields": custom_fields,
            "customStats": custom_stats_for_template,
            "custom_stats": custom_stats_for_template,
            "customStatsMap": custom_stats,
            "custom_stats_map": custom_stats,
            "customStatsText": custom_stats_text,
            "custom_stats_text": custom_stats_text,
            "snapshot": snapshot_dict,
        }
        for key, value in custom_stats.items():
            safe_key = _safe_template_key(key)
            camel_key = _camel_template_key(key)
            template_variable.setdefault(safe_key, value)
            template_variable.setdefault(f"{safe_key}_text", _format_optional_value(value))
            template_variable.setdefault(camel_key, value)
            template_variable.setdefault(f"{camel_key}Text", _format_optional_value(value))
            template_variable.setdefault(f"stat_{safe_key}", value)
        for key, value in custom_fields.items():
            template_variable.setdefault(key, value)
        return template_variable

    def _custom_stats_for_template(self, stats: dict[str, Any]) -> list[dict[str, Any]]:
        configured_stats = self.hook_cfg.get("custom_stats") or []
        output = []
        seen = set()
        suppress_keys = _rollup_stat_keys_shadowed_by_configured(configured_stats, stats)
        for item in configured_stats:
            if isinstance(item, str):
                key = item
                label = item
                group = None
            elif isinstance(item, dict):
                key = item.get("key")
                label = item.get("label") or key
                group = item.get("group") or item.get("category")
            else:
                continue
            if not key:
                continue
            seen.add(key)
            value = stats.get(key)
            stat = {"key": key, "label": label, "value": value}
            if group:
                stat["group"] = group
            output.append(stat)
        for key in sorted(stats):
            if key in seen or key in suppress_keys:
                continue
            value = stats.get(key)
            output.append({"key": key, "label": key, "value": value})
        return output

    def _base_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "User-Account": self.user_account,
        }
        for key in ("x-tt-env", "x-use-ppe"):
            value = self.ctx.get(key)
            if value:
                headers[key] = str(value)
        return headers

    @staticmethod
    def _validate_openapi_result(data: Any) -> None:
        if not isinstance(data, dict) or "code" not in data:
            return
        if data.get("code") != 0:
            message = data.get("message") or data.get("msg") or ""
            raise ValueError(f"ADC lark message business failed: code={data.get('code')}, message={message}")


class TaskNotificationManager:
    def __init__(self, cfg: Any, stats_collector: RuntimeStatsCollector | None = None):
        self.cfg = cfg
        self.stats_collector = stats_collector or RuntimeStatsCollector()
        self.start_time = time.time()
        self.phase = "initializing"
        self.status = "running"
        self.export_summary: dict[str, Any] = {}
        self._export_summary_provider: Callable[[], dict[str, Any] | None] | None = None
        self.error_summary: str | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._hooks = []
        for hook_cfg in _plain_list(getattr(cfg, "notification_hooks", []) or []):
            if hook_cfg.get("enabled", True) is False:
                continue
            hook_type = hook_cfg.get("type")
            if hook_type == "adc_lark_message":
                self._hooks.append(
                    {
                        "cfg": hook_cfg,
                        "hook": AdcLarkMessageNotificationHook(hook_cfg),
                        "interval": parse_notification_interval_seconds(hook_cfg.get("interval")),
                    }
                )
            else:
                raise ValueError(f"Unsupported notification hook type: {hook_type}")

    @property
    def enabled(self) -> bool:
        return bool(self._hooks)

    def start(self) -> None:
        intervals = [entry["interval"] for entry in self._hooks if entry["interval"] is not None]
        if not intervals or self._thread is not None:
            return
        interval = min(intervals)
        self._thread = threading.Thread(target=self._heartbeat_loop, args=(interval,), daemon=True)
        self._thread.start()

    def update_phase(self, phase: str) -> None:
        self.phase = phase

    def set_export_summary_provider(self, provider: Callable[[], dict[str, Any] | None] | None) -> None:
        self._export_summary_provider = provider

    def send_heartbeat(self) -> None:
        if not self.enabled:
            return
        snapshot = self.build_snapshot()
        self._write_snapshot(snapshot)
        for entry in self._hooks:
            if entry["interval"] is None:
                continue
            try:
                entry["hook"].send(snapshot, event="heartbeat")
                logger.info("Sent task notification heartbeat snapshot for phase [{}]", snapshot.phase)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to send task notification heartbeat: {}", exc)

    def finish(
        self,
        *,
        success: bool,
        export_summary: dict[str, Any] | None = None,
        error: BaseException | None = None,
    ) -> None:
        if not self.enabled:
            return
        self._stop_timer()
        self.status = "success" if success else "failure"
        self.phase = "finished" if success else "failed"
        self.export_summary = dict(export_summary or {})
        self.error_summary = _summarize_error(error) if error is not None else None
        snapshot = self.build_snapshot()
        self._write_snapshot(snapshot)

        event = "success" if success else "failure"
        terminal_errors = []
        for entry in self._hooks:
            cfg = entry["cfg"]
            if success and cfg.get("on_success", True) is False:
                continue
            if not success and cfg.get("on_failure", True) is False:
                continue
            try:
                entry["hook"].send(snapshot, event=event)
                logger.info("Sent task terminal notification [{}]", event)
            except Exception as exc:  # noqa: BLE001
                terminal_errors.append((cfg, exc))
                logger.warning("Failed to send terminal task notification: {}", exc)

        for cfg, exc in terminal_errors:
            if cfg.get("fail_on_error", False):
                raise exc

    def build_snapshot(self) -> TaskProgressSnapshot:
        stats = self._selected_custom_stats()
        export_summary = self._current_export_summary()
        return TaskProgressSnapshot(
            job_id=getattr(self.cfg, "job_id", None),
            project_name=getattr(self.cfg, "project_name", None),
            executor_type=getattr(self.cfg, "executor_type", None),
            status=self.status,
            phase=self.phase,
            start_time=self.start_time,
            elapsed_seconds=time.time() - self.start_time,
            export_path=getattr(self.cfg, "export_path", None),
            output_rows=export_summary.get("output_rows"),
            output_bytes=export_summary.get("output_bytes"),
            output_files=export_summary.get("output_files"),
            custom_stats=stats,
            export_targets=export_summary.get("targets"),
            error_summary=self.error_summary,
        )

    def _current_export_summary(self) -> dict[str, Any]:
        export_summary = dict(self.export_summary or {})
        if self._export_summary_provider is None:
            return export_summary
        try:
            live_summary = self._export_summary_provider() or {}
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to collect live export summary for task notification: {}", exc)
            return export_summary
        for key, value in live_summary.items():
            if export_summary.get(key) is None:
                export_summary[key] = value
        return export_summary

    def _selected_custom_stats(self) -> dict[str, Any]:
        stats = dict(self.stats_collector.snapshot() or {})
        configured = self._configured_custom_stat_items()
        if not configured:
            return stats
        selected = dict(stats)
        for item in configured:
            key = item.get("key")
            if not key:
                continue
            if _is_ratio_custom_stat(item):
                selected[key] = _format_ratio_custom_stat(
                    stats.get(item.get("numerator") or item.get("numerator_key"), 0),
                    stats.get(item.get("denominator") or item.get("denominator_key"), 0),
                    item,
                )
            elif key not in selected:
                selected[key] = stats.get(key, 0)
        return selected

    def _configured_custom_stat_items(self) -> list[dict[str, Any]]:
        items = []
        seen = set()
        for entry in self._hooks:
            for item in entry["cfg"].get("custom_stats") or []:
                normalized = {"key": item} if isinstance(item, str) else dict(item) if isinstance(item, dict) else {}
                key = normalized.get("key")
                if key and key not in seen:
                    seen.add(key)
                    items.append(normalized)
        return items

    def _heartbeat_loop(self, interval: int) -> None:
        while not self._stop_event.wait(interval):
            self.send_heartbeat()

    def _stop_timer(self) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)

    def _write_snapshot(self, snapshot: TaskProgressSnapshot) -> None:
        work_dir = getattr(self.cfg, "work_dir", None)
        if not work_dir:
            return
        snapshot_dict = snapshot.to_dict()
        try:
            os.makedirs(work_dir, exist_ok=True)
            snapshot_path = os.path.join(work_dir, SNAPSHOT_FILENAME)
            with open(snapshot_path, "w", encoding="utf-8") as file:
                json.dump(snapshot_dict, file, indent=2, default=str)
            self._update_job_summary(snapshot_dict)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to write task notification snapshot: {}", exc)

    def _update_job_summary(self, snapshot: dict[str, Any]) -> None:
        summary_path = getattr(self.cfg, "job_summary_file", None)
        if not summary_path:
            work_dir = getattr(self.cfg, "work_dir", None)
            summary_path = os.path.join(work_dir, "job_summary.json") if work_dir else None
        if not summary_path or not os.path.exists(summary_path):
            return
        with open(summary_path, "r", encoding="utf-8") as file:
            summary = json.load(file)
        summary["notification_snapshot"] = snapshot
        with open(summary_path, "w", encoding="utf-8") as file:
            json.dump(summary, file, indent=2, default=str)


def parse_notification_interval_seconds(interval: Any) -> int | None:
    if interval is None:
        return None
    if not isinstance(interval, str):
        raise ValueError("notification_hooks[].interval must use a string like 30s, 10min, or 1h")
    match = _INTERVAL_RE.match(interval)
    if not match:
        raise ValueError("notification_hooks[].interval must use a string like 30s, 10min, or 1h")
    value = int(match.group(1))
    unit = match.group(2)
    multiplier = {"s": 1, "min": 60, "h": 3600}[unit]
    seconds = value * multiplier
    if seconds < MIN_INTERVAL_SECONDS:
        raise ValueError("notification_hooks[].interval must be at least 30s")
    return seconds


def validate_notification_hooks_config(notification_hooks: Any) -> None:
    hooks = _plain_list(notification_hooks or [])
    if not isinstance(hooks, list):
        raise ValueError("notification_hooks must be a list")
    for hook_cfg in hooks:
        if not isinstance(hook_cfg, dict):
            raise ValueError("notification_hooks[] item must be a dictionary")
        if hook_cfg.get("enabled", True) is False:
            continue
        if hook_cfg.get("type") != "adc_lark_message":
            raise ValueError("notification_hooks[].type must be adc_lark_message")
        hook_cfg.setdefault("interval", None)
        parse_notification_interval_seconds(hook_cfg.get("interval"))


def _plain_list(value: Any) -> list[Any]:
    value = _plain_value(value)
    return value if isinstance(value, list) else value


def _plain_dict(value: Any) -> dict[str, Any]:
    value = _plain_value(value)
    return value if isinstance(value, dict) else {}


def _plain_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _plain_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_plain_value(item) for item in value]
    as_dict = getattr(value, "as_dict", None)
    if callable(as_dict):
        return _plain_value(as_dict())
    if hasattr(value, "__dict__") and value.__class__.__name__ == "Namespace":
        return _plain_value(vars(value))
    return value


def _require_dict(value: Any, name: str) -> dict[str, Any]:
    value = _plain_dict(value)
    if not value:
        raise ValueError(f"{name} must be a dictionary")
    return value


def _require_value(value: Any, name: str) -> Any:
    if value in (None, ""):
        raise ValueError(f"{name} must be provided")
    return value


def _status_text(status: str | None) -> str:
    return {
        "running": "进行中",
        "success": "成功",
        "failure": "失败",
    }.get(str(status or ""), str(status or ""))


def _phase_progress(phase: str | None, status: str | None) -> str:
    phase = str(phase or "")
    status = str(status or "")
    if status == "success":
        return "load ✓ -> process ✓ -> export ✓ -> finished ✓"
    if status == "failure":
        return {
            "load": "load 失败 -> process 未开始 -> export 未开始",
            "process": "load ✓ -> process 失败 -> export 未开始",
            "export": "load ✓ -> process ✓ -> export 失败",
            "failed": "load ✓ -> process ✓ -> export 失败",
        }.get(phase, f"{phase or 'unknown'} 失败")
    return {
        "initializing": "initializing 进行中 -> load 未开始 -> process 未开始 -> export 未开始",
        "load": "load 进行中 -> process 未开始 -> export 未开始",
        "process": "load ✓ -> process 进行中 -> export 未开始",
        "export": "load ✓ -> process ✓ -> export 进行中",
        "finished": "load ✓ -> process ✓ -> export ✓ -> finished ✓",
    }.get(phase, f"{phase or 'unknown'} 进行中")


def _format_duration(seconds: float | int | None) -> str:
    if seconds is None:
        return "unknown"
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        return str(seconds)
    if seconds < 60:
        return f"{seconds:.1f}s" if seconds % 1 else f"{int(seconds)}s"
    minutes, remaining_seconds = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}min {remaining_seconds}s"
    hours, remaining_minutes = divmod(minutes, 60)
    return f"{hours}h {remaining_minutes}min"


def _format_bytes(num_bytes: int | float | None) -> str:
    if num_bytes is None:
        return "unknown"
    try:
        value = float(num_bytes)
    except (TypeError, ValueError):
        return str(num_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if abs(value) < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def _format_optional_value(value: Any) -> str:
    if value is None:
        return "unknown"
    return str(value)


def _format_custom_stats_text(items: list[dict[str, Any]]) -> str:
    if not items:
        return "暂无"
    lines = []
    current_group = None
    for item in items:
        group = item.get("group")
        if group and group != current_group:
            if lines:
                lines.append("")
            lines.append(f"**{group}**")
            current_group = group
        label = item.get("label") or item.get("key") or "stat"
        lines.append(f"• {label}：{_format_optional_value(item.get('value'))}")
    return "\n".join(lines)


def _format_export_targets_text(targets: list[dict[str, Any]] | None) -> str:
    if not targets:
        return "暂无"
    lines = []
    for index, target in enumerate(targets, 1):
        if not isinstance(target, dict):
            lines.append(f"{index}. {_format_optional_value(target)}")
            continue
        rows = _format_optional_value(_first_present(target, "rows", "output_rows"))
        files = _format_optional_value(target.get("output_files"))
        bytes_text = _format_bytes(target.get("output_bytes"))
        path = _format_optional_value(_first_present(target, "path", "original_uri", "uri"))
        lines.append(f"{index}. 行数：{rows}；文件数：{files}；大小：{bytes_text}")
        lines.append(path)
    return "\n".join(lines)


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping.get(key)
    return None


def _rollup_stat_keys_shadowed_by_configured(configured_stats: list[Any], stats: dict[str, Any]) -> set[str]:
    configured_keys = _configured_custom_stat_dependency_keys(configured_stats)
    suppressed = set()
    for key in stats:
        parts = str(key).split(".")
        if len(parts) != 2 or parts[1] not in {"total_count", "success_count", "failed_count"}:
            continue
        family, suffix = parts
        for configured_key in configured_keys:
            configured_parts = str(configured_key).split(".")
            if len(configured_parts) >= 3 and configured_parts[0] == family and configured_parts[-1] == suffix:
                suppressed.add(key)
                break
    return suppressed


def _configured_custom_stat_dependency_keys(configured_stats: list[Any]) -> set[str]:
    keys = set()
    for item in configured_stats:
        if isinstance(item, str):
            keys.add(item)
            continue
        if not isinstance(item, dict):
            continue
        for field in ("key", "numerator", "numerator_key", "denominator", "denominator_key"):
            value = item.get(field)
            if value:
                keys.add(str(value))
    return keys


def _is_ratio_custom_stat(item: dict[str, Any]) -> bool:
    return item.get("type") == "ratio" or (
        (item.get("numerator") or item.get("numerator_key"))
        and (item.get("denominator") or item.get("denominator_key"))
    )


def _format_ratio_custom_stat(numerator: Any, denominator: Any, item: dict[str, Any]) -> str | float:
    try:
        numerator = float(numerator or 0)
    except (TypeError, ValueError):
        numerator = 0.0
    try:
        denominator = float(denominator or 0)
    except (TypeError, ValueError):
        denominator = 0.0
    ratio = 0.0 if denominator <= 0 else numerator / denominator
    precision = int(item.get("precision", 2))
    if item.get("format", "percent") == "number":
        return round(ratio, precision)
    return f"{ratio * 100:.{precision}f}%"


def _format_url_variable(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        url = str(value.get("url") or value.get("default_url") or value.get("url_detail") or "")
        return {
            "url": url,
            "pc_url": str(value.get("pc_url") or value.get("pc") or url),
            "android_url": str(value.get("android_url") or value.get("android") or url),
            "ios_url": str(value.get("ios_url") or value.get("ios") or url),
        }
    url = str(value or "")
    return {
        "url": url,
        "pc_url": url,
        "android_url": url,
        "ios_url": url,
    }


def _safe_template_key(key: Any) -> str:
    value = re.sub(r"[^0-9A-Za-z_]", "_", str(key or "stat"))
    value = re.sub(r"_+", "_", value).strip("_") or "stat"
    if value[0].isdigit():
        value = f"stat_{value}"
    return value


def _camel_template_key(key: Any) -> str:
    parts = [part for part in _safe_template_key(key).split("_") if part]
    if not parts:
        return "stat"
    return parts[0] + "".join(part[:1].upper() + part[1:] for part in parts[1:])


def _summarize_error(error: BaseException | None) -> str | None:
    if error is None:
        return None
    message = str(error).replace("\n", " ").strip()
    if len(message) > 500:
        message = message[:497] + "..."
    return f"{type(error).__name__}: {message}" if message else type(error).__name__
