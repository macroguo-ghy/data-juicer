from __future__ import annotations

import os
from typing import Any

from loguru import logger

from data_juicer.utils.constant import (
    METRICS_JOB_ID_ENV_VAR,
    METRICS_RAY_ADDRESS_ENV_VAR,
)

METRICS_PREFIX = "ad.ai.data_forge"
RPC_QPS_METRIC = "rpc.qps"
DOWNLOAD_QPS_METRIC = "download.qps"
DOWNLOAD_BYTES_METRIC = "download.bytes"
DOWNLOAD_LATENCY_MS_METRIC = "download.latency_ms"
VLM_QPS_METRIC = "vlm.qps"
VLM_RATE_LIMIT_EVENT_METRIC = "vlm.rate_limit.event"
VLM_RATE_LIMIT_VALUE_METRIC = "vlm.rate_limit.value"
DEDUP_ROWS_METRIC = "dedup.rows"
UNKNOWN_TAG_VALUE = "unknown"

_metrics_client = None
_metrics_client_initialized = False
_metrics_warning_logged = False


def set_metrics_context(job_id: str | None = None, ray_address: str | None = None) -> None:
    if job_id:
        os.environ[METRICS_JOB_ID_ENV_VAR] = str(job_id)
    if ray_address:
        os.environ[METRICS_RAY_ADDRESS_ENV_VAR] = str(ray_address)


def metrics_context_tags() -> dict[str, str]:
    return {
        "job_id": _tag_value(os.environ.get(METRICS_JOB_ID_ENV_VAR)),
        "ray_address": _tag_value(os.environ.get(METRICS_RAY_ADDRESS_ENV_VAR)),
    }


def emit_rpc_qps(
    *,
    op_name: str,
    target: str,
    method: str,
    status: str,
    extra_tags: dict[str, Any] | None = None,
) -> None:
    _emit_qps(
        RPC_QPS_METRIC,
        {
            "op_name": op_name,
            "target": target,
            "method": method,
            "status": status,
            **(extra_tags or {}),
        },
    )


def emit_vlm_qps(
    *,
    op_name: str,
    target: str,
    method: str,
    status: str,
    extra_tags: dict[str, Any] | None = None,
) -> None:
    _emit_qps(
        VLM_QPS_METRIC,
        {
            "op_name": op_name,
            "target": target,
            "method": method,
            "status": status,
            **(extra_tags or {}),
        },
    )


def emit_download_qps(
    *,
    op_name: str,
    scheme: str,
    status: str,
    save_mode: str,
    extra_tags: dict[str, Any] | None = None,
) -> None:
    _emit_qps(
        DOWNLOAD_QPS_METRIC,
        {
            "op_name": op_name,
            "scheme": scheme,
            "status": status,
            "save_mode": save_mode,
            **(extra_tags or {}),
        },
    )


def emit_download_bytes(
    *,
    op_name: str,
    scheme: str,
    byte_count: int | float,
    save_mode: str,
    extra_tags: dict[str, Any] | None = None,
) -> None:
    _emit_store(
        DOWNLOAD_BYTES_METRIC,
        float(byte_count),
        {
            "op_name": op_name,
            "scheme": scheme,
            "save_mode": save_mode,
            **(extra_tags or {}),
        },
    )


def emit_download_latency_ms(
    *,
    op_name: str,
    scheme: str,
    status: str,
    latency_ms: int | float,
    save_mode: str,
    extra_tags: dict[str, Any] | None = None,
) -> None:
    _emit_store(
        DOWNLOAD_LATENCY_MS_METRIC,
        float(latency_ms),
        {
            "op_name": op_name,
            "scheme": scheme,
            "status": status,
            "save_mode": save_mode,
            **(extra_tags or {}),
        },
    )


def emit_vlm_rate_limit_event(
    *,
    event: str,
    op_name: str,
    model: str | None = None,
    target: str | None = None,
    method: str | None = None,
    extra_tags: dict[str, Any] | None = None,
) -> None:
    _emit_rate_counter(
        VLM_RATE_LIMIT_EVENT_METRIC,
        {
            "event": event,
            "op_name": op_name,
            "model": model,
            "target": target,
            "method": method,
            **(extra_tags or {}),
        },
    )


def emit_vlm_rate_limit_value(
    *,
    metric: str,
    value: float,
    op_name: str,
    model: str | None = None,
    target: str | None = None,
    method: str | None = None,
    extra_tags: dict[str, Any] | None = None,
) -> None:
    _emit_store(
        VLM_RATE_LIMIT_VALUE_METRIC,
        value,
        {
            "metric": metric,
            "op_name": op_name,
            "model": model,
            "target": target,
            "method": method,
            **(extra_tags or {}),
        },
    )


def emit_dedup_rows(
    *,
    op_name: str,
    field_key: str,
    event: str,
    count: int,
    extra_tags: dict[str, Any] | None = None,
) -> None:
    if count <= 0:
        return
    _emit_rate_counter(
        DEDUP_ROWS_METRIC,
        {
            "op_name": op_name,
            "field_key": field_key,
            "event": event,
            **(extra_tags or {}),
        },
        value=count,
    )


def _emit_qps(metric_name: str, tags: dict[str, Any]) -> None:
    _emit_rate_counter(metric_name, tags)


def _emit_rate_counter(metric_name: str, tags: dict[str, Any], value: int | float = 1) -> None:
    client = _get_metrics_client()
    if client is None:
        return
    try:
        client.emit_rate_counter(metric_name, value, tags=_normalize_tags({**tags, **metrics_context_tags()}))
    except Exception as err:  # noqa: BLE001
        _log_metrics_warning_once(f"Failed to emit metrics via bytedance.metrics: {err}")


def _emit_store(metric_name: str, value: float, tags: dict[str, Any]) -> None:
    client = _get_metrics_client()
    if client is None:
        return
    try:
        client.emit_store(metric_name, value, tags=_normalize_tags({**tags, **metrics_context_tags()}))
    except Exception as err:  # noqa: BLE001
        _log_metrics_warning_once(f"Failed to emit metrics via bytedance.metrics: {err}")


def _get_metrics_client():
    global _metrics_client
    global _metrics_client_initialized

    if _metrics_client_initialized:
        return _metrics_client

    _metrics_client_initialized = True
    try:
        from bytedance import metrics

        _metrics_client = metrics.Client(prefix=METRICS_PREFIX)
    except Exception as err:  # noqa: BLE001
        _metrics_client = None
        _log_metrics_warning_once(f"Failed to initialize bytedance.metrics client: {err}")
    return _metrics_client


def _normalize_tags(tags: dict[str, Any]) -> dict[str, str]:
    return {str(key): _tag_value(value) for key, value in tags.items()}


def _tag_value(value: Any) -> str:
    if value is None:
        return UNKNOWN_TAG_VALUE
    text = str(value)
    return text if text else UNKNOWN_TAG_VALUE


def _log_metrics_warning_once(message: str) -> None:
    global _metrics_warning_logged
    if _metrics_warning_logged:
        return
    _metrics_warning_logged = True
    logger.warning(message)


def _reset_metrics_client_for_test() -> None:
    global _metrics_client
    global _metrics_client_initialized
    global _metrics_warning_logged

    _metrics_client = None
    _metrics_client_initialized = False
    _metrics_warning_logged = False
