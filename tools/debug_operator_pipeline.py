import argparse
import base64
import copy
import datetime as dt
import hashlib
import json
import math
import os
import sys
import tempfile
import time
import traceback
import uuid
from pathlib import Path
from typing import Any

from loguru import logger

_REPO_ROOT = str(Path(__file__).resolve().parents[1])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from data_juicer.config import init_configs
from data_juicer.core.io_utils import copy_local_to_uri, namespace_to_plain_dict
from data_juicer.ops import Deduplicator, Filter, Mapper, Pipeline
from data_juicer.ops.load import load_ops
from data_juicer.utils.constant import Fields


BYTES_WRAPPER_KEY = "__dj_bytes__"
BYTES_SUMMARY_KEY = "__dj_bytes_summary__"
REPR_WRAPPER_KEY = "__dj_repr__"
DEFAULT_SENSITIVE_FIELD_NAMES = {
    "api_key",
    "app_key",
    "access_key",
    "authorization",
    "cookie",
    "credential",
    "password",
    "secret",
    "session",
    "token",
}
SENSITIVE_OP_CONFIG_PARTS = tuple(sorted(DEFAULT_SENSITIVE_FIELD_NAMES))


class DebugConfigError(ValueError):
    pass


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _plain(value: Any) -> Any:
    return namespace_to_plain_dict(value)


def _get_debug_cfg(cfg) -> dict:
    debug_cfg = _plain(getattr(cfg, "debug", None))
    if not isinstance(debug_cfg, dict):
        raise DebugConfigError("`debug` must be a mapping for operator pipeline debug.")
    if not debug_cfg.get("enabled", False):
        raise DebugConfigError("`debug.enabled` must be true.")
    return debug_cfg


def _normalize_debug_output(debug_cfg: dict) -> dict:
    output_cfg = debug_cfg.get("output")
    if not isinstance(output_cfg, dict):
        raise DebugConfigError("`debug.output` must be a mapping.")
    output_path = output_cfg.get("path")
    if not isinstance(output_path, str) or not output_path:
        raise DebugConfigError("`debug.output.path` is required.")
    output_type = output_cfg.get("type", "jsonl")
    if output_type != "jsonl":
        raise DebugConfigError("`debug.output.type` only supports `jsonl`.")
    return output_cfg


def _decode_bytes_value(value: Any) -> Any:
    if isinstance(value, dict) and set(value.keys()) == {BYTES_WRAPPER_KEY}:
        payload = value[BYTES_WRAPPER_KEY]
        if not isinstance(payload, dict):
            raise DebugConfigError(f"`{BYTES_WRAPPER_KEY}` must contain a mapping.")
        encoding = payload.get("encoding")
        data = payload.get("data")
        if not isinstance(data, str):
            raise DebugConfigError(f"`{BYTES_WRAPPER_KEY}.data` must be a string.")
        if encoding == "base64":
            normalized = "".join(data.split())
            padding = (-len(normalized)) % 4
            return base64.b64decode(normalized + ("=" * padding), validate=True)
        if encoding == "data_url":
            if ";base64," not in data:
                raise DebugConfigError("data_url bytes wrapper must contain `;base64,`.")
            encoded = data.split(",", 1)[1]
            normalized = "".join(encoded.split())
            padding = (-len(normalized)) % 4
            return base64.b64decode(normalized + ("=" * padding), validate=True)
        raise DebugConfigError(f"Unsupported bytes encoding [{encoding}].")
    if isinstance(value, list):
        return [_decode_bytes_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _decode_bytes_value(item) for key, item in value.items()}
    raise DebugConfigError(f"Declared bytes field must use `{BYTES_WRAPPER_KEY}` wrappers.")


def _load_debug_sample(debug_cfg: dict) -> dict:
    has_sample = "sample" in debug_cfg
    has_sample_json = "sample_json" in debug_cfg
    if has_sample and has_sample_json:
        raise DebugConfigError("`debug.sample` and `debug.sample_json` are mutually exclusive.")
    if has_sample:
        sample = _plain(debug_cfg["sample"])
    elif has_sample_json:
        try:
            sample = json.loads(debug_cfg["sample_json"])
        except json.JSONDecodeError as err:
            raise DebugConfigError(f"`debug.sample_json` is not valid JSON: {err}") from err
    else:
        raise DebugConfigError("One of `debug.sample` or `debug.sample_json` is required.")
    if not isinstance(sample, dict):
        raise DebugConfigError("Debug sample must be a JSON object.")

    sample = copy.deepcopy(sample)
    decode_fields = _plain(debug_cfg.get("decode_fields", {})) or {}
    if not isinstance(decode_fields, dict):
        raise DebugConfigError("`debug.decode_fields` must be a mapping.")
    for field, target_type in decode_fields.items():
        if "." in field or "[" in field or "]" in field:
            raise DebugConfigError("`debug.decode_fields` only supports top-level fields.")
        if target_type != "bytes":
            raise DebugConfigError("`debug.decode_fields` only supports value `bytes`.")
        if field not in sample:
            raise DebugConfigError(f"`debug.decode_fields` references missing field [{field}].")
        sample[field] = _decode_bytes_value(sample[field])
    return sample


def _normalize_bytes_output(debug_cfg: dict) -> dict:
    bytes_cfg = _plain(debug_cfg.get("bytes_output", {})) or {}
    if not isinstance(bytes_cfg, dict):
        raise DebugConfigError("`debug.bytes_output` must be a mapping.")
    mode = bytes_cfg.get("mode", "summary")
    if mode not in {"summary", "full_base64"}:
        raise DebugConfigError("`debug.bytes_output.mode` must be `summary` or `full_base64`.")
    preview_bytes = bytes_cfg.get("preview_bytes", 64)
    if not isinstance(preview_bytes, int) or preview_bytes < 0:
        raise DebugConfigError("`debug.bytes_output.preview_bytes` must be a non-negative integer.")
    return {"mode": mode, "preview_bytes": preview_bytes}


def _redact_field_name(key: Any, redact_fields: set[str]) -> bool:
    return isinstance(key, str) and key.lower() in redact_fields


def _redact_op_config(value: Any) -> Any:
    value = _plain(value)
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            if isinstance(key, str) and any(part in key.lower() for part in SENSITIVE_OP_CONFIG_PARTS):
                redacted[key] = "<redacted>"
            else:
                redacted[key] = _redact_op_config(item)
        return redacted
    if isinstance(value, list):
        return [_redact_op_config(item) for item in value]
    return value


def _bytes_to_jsonable(value: bytes, bytes_cfg: dict) -> dict:
    digest = hashlib.sha256(value).hexdigest()
    if bytes_cfg["mode"] == "full_base64":
        return {
            BYTES_WRAPPER_KEY: {
                "encoding": "base64",
                "data": base64.b64encode(value).decode("ascii"),
                "length": len(value),
                "sha256": digest,
            }
        }

    preview = value[: bytes_cfg["preview_bytes"]]
    return {
        BYTES_SUMMARY_KEY: {
            "length": len(value),
            "sha256": digest,
            "preview_base64": base64.b64encode(preview).decode("ascii"),
            "truncated": len(preview) < len(value),
        }
    }


def to_jsonable_debug_value(value: Any, *, bytes_cfg: dict, redact_fields: set[str]) -> Any:
    if isinstance(value, dict):
        converted = {}
        for key, item in value.items():
            if _redact_field_name(key, redact_fields):
                converted[key] = "<redacted>"
            else:
                converted[key] = to_jsonable_debug_value(item, bytes_cfg=bytes_cfg, redact_fields=redact_fields)
        return converted
    if isinstance(value, (bytes, bytearray, memoryview)):
        return _bytes_to_jsonable(bytes(value), bytes_cfg)
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable_debug_value(item, bytes_cfg=bytes_cfg, redact_fields=redact_fields) for item in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()

    try:
        import numpy as np

        if isinstance(value, np.generic):
            return to_jsonable_debug_value(value.item(), bytes_cfg=bytes_cfg, redact_fields=redact_fields)
        if isinstance(value, np.ndarray):
            return to_jsonable_debug_value(value.tolist(), bytes_cfg=bytes_cfg, redact_fields=redact_fields)
    except Exception:
        pass

    try:
        import pyarrow as pa

        if isinstance(value, pa.Scalar):
            return to_jsonable_debug_value(value.as_py(), bytes_cfg=bytes_cfg, redact_fields=redact_fields)
        if isinstance(value, (pa.Array, pa.ChunkedArray)):
            return to_jsonable_debug_value(value.to_pylist(), bytes_cfg=bytes_cfg, redact_fields=redact_fields)
    except Exception:
        pass

    return {REPR_WRAPPER_KEY: {"type": type(value).__name__, "repr": repr(value)}}


def _schema_to_jsonable(dataset) -> dict | None:
    try:
        schema = dataset.schema(fetch_if_missing=False)
    except TypeError:
        try:
            schema = dataset.schema()
        except Exception:
            return None
    except Exception:
        return None
    base_schema = getattr(schema, "base_schema", schema)
    names = getattr(base_schema, "names", None)
    types = getattr(base_schema, "types", None)
    if names is None or types is None:
        return None
    return {name: str(field_type) for name, field_type in zip(names, types)}


def _op_type(op) -> str:
    if isinstance(op, Mapper):
        return "mapper"
    if isinstance(op, Filter):
        return "filter"
    if isinstance(op, Deduplicator):
        return "deduplicator"
    if isinstance(op, Pipeline):
        return "pipeline"
    return type(op).__name__


def _extract_stats(data: dict | None) -> Any:
    if not isinstance(data, dict):
        return None
    return data.get(Fields.stats)


def _extract_meta(data: dict | None) -> Any:
    if not isinstance(data, dict):
        return None
    return data.get(Fields.meta)


def _build_redact_fields(debug_cfg: dict) -> set[str]:
    fields = set(DEFAULT_SENSITIVE_FIELD_NAMES)
    extra_fields = debug_cfg.get("redact_fields", []) or []
    if not isinstance(extra_fields, list):
        raise DebugConfigError("`debug.redact_fields` must be a list.")
    fields.update(str(field).lower() for field in extra_fields)
    return fields


def _format_output_path(path: str, *, cfg, debug_run_id: str, timestamp: str) -> str:
    return path.format(
        job_id=getattr(cfg, "job_id", ""),
        debug_run_id=debug_run_id,
        timestamp=timestamp,
    )


def _write_events_local(events: list[dict], local_path: str) -> None:
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as fout:
        for event in events:
            fout.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")


def _copy_trace_to_output(local_path: str, output_path: str, output_cfg: dict) -> None:
    copy_local_to_uri(
        local_path,
        output_path,
        filesystem=output_cfg.get("filesystem"),
        storage_options=output_cfg.get("webhdfs"),
    )


def _load_ops_for_debug(cfg, selected_process: list[dict]):
    op_env_manager = None
    if getattr(cfg, "min_common_dep_num_to_combine", -1) >= 0:
        from data_juicer.ops import OPEnvManager

        op_env_manager = OPEnvManager(
            min_common_dep_num_to_combine=cfg.min_common_dep_num_to_combine,
            conflict_resolve_strategy=getattr(cfg, "conflict_resolve_strategy", "latest"),
        )
    return load_ops(selected_process, op_env_manager)


def _force_fail_fast_ops(process_list: list[dict]) -> list[dict]:
    debug_process = copy.deepcopy(process_list)
    for process in debug_process:
        if not isinstance(process, dict) or not process:
            continue
        op_name, op_args = list(process.items())[0]
        if op_args is None:
            op_args = {}
        if isinstance(op_args, dict):
            op_args["skip_op_error"] = False
            process[op_name] = op_args
    return debug_process


def _build_initial_ray_dataset(sample: dict, cfg):
    import ray

    from data_juicer.core.data.ray_dataset import RayDataset

    return RayDataset(ray.data.from_items([sample]), cfg=cfg)


def _snapshot_dataset(dataset, *, bytes_cfg: dict, redact_fields: set[str]) -> tuple[dict | None, int, dict | None]:
    ray_dataset = dataset.data.materialize()
    dataset.data = ray_dataset
    rows = ray_dataset.take(1)
    schema = _schema_to_jsonable(ray_dataset)
    if not rows:
        return None, 0, schema
    data = to_jsonable_debug_value(rows[0], bytes_cfg=bytes_cfg, redact_fields=redact_fields)
    return data, 1, schema


def _base_event(debug_run_id: str, cfg, output_path: str) -> dict:
    return {
        "debug_run_id": debug_run_id,
        "job_id": getattr(cfg, "job_id", ""),
        "ray_job_id": None,
        "output_path": output_path,
    }


def _set_ray_job_id(event: dict) -> None:
    try:
        import ray

        event["ray_job_id"] = ray.get_runtime_context().get_job_id()
    except Exception:
        event["ray_job_id"] = None


def _summary_event(base: dict, *, status: str, executed_ops: int, error: dict | None = None) -> dict:
    event = {
        **base,
        "event": "summary",
        "status": status,
        "executed_ops": executed_ops,
        "finished_at": _utc_now_iso(),
    }
    if error:
        event.update(error)
    return event


def _validate_indices(debug_cfg: dict, process_list: list[dict]) -> tuple[int, int]:
    if not process_list:
        raise DebugConfigError("`process` must contain at least one operator.")
    start_index = debug_cfg.get("start_index", 0)
    end_index = debug_cfg.get("end_index", None)
    if not isinstance(start_index, int) or start_index < 0:
        raise DebugConfigError("`debug.start_index` must be a non-negative integer.")
    if end_index is None:
        end_index = len(process_list) - 1
    if not isinstance(end_index, int) or end_index < 0:
        raise DebugConfigError("`debug.end_index` must be a non-negative integer or null.")
    if start_index > end_index:
        raise DebugConfigError("`debug.start_index` must be <= `debug.end_index`.")
    if end_index >= len(process_list):
        raise DebugConfigError("`debug.end_index` is out of range for `process`.")
    return start_index, end_index


def _validate_ray_only(cfg) -> None:
    if getattr(cfg, "executor_type", None) != "ray":
        raise DebugConfigError("Operator pipeline debug currently requires `executor_type: ray`.")


def run_debug_pipeline(cfg) -> tuple[list[dict], str]:
    debug_cfg = _get_debug_cfg(cfg)
    _validate_ray_only(cfg)
    output_cfg = _normalize_debug_output(debug_cfg)
    sample = _load_debug_sample(debug_cfg)
    bytes_cfg = _normalize_bytes_output(debug_cfg)
    redact_fields = _build_redact_fields(debug_cfg)
    process_list = _plain(getattr(cfg, "process", [])) or []
    start_index, end_index = _validate_indices(debug_cfg, process_list)

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    debug_run_id = debug_cfg.get("run_id") or f"debug_{timestamp}_{uuid.uuid4().hex[:8]}"
    output_path = _format_output_path(output_cfg["path"], cfg=cfg, debug_run_id=debug_run_id, timestamp=timestamp)
    base = _base_event(debug_run_id, cfg, output_path)

    from data_juicer.utils.ray_utils import initialize_ray

    initialize_ray(cfg=cfg, force=True)
    _set_ray_job_id(base)

    events = [
        {
            **base,
            "event": "input",
            "status": "success",
            "created_at": _utc_now_iso(),
            "data": to_jsonable_debug_value(sample, bytes_cfg=bytes_cfg, redact_fields=redact_fields),
            "stats": to_jsonable_debug_value(_extract_stats(sample), bytes_cfg=bytes_cfg, redact_fields=redact_fields),
            "meta": to_jsonable_debug_value(_extract_meta(sample), bytes_cfg=bytes_cfg, redact_fields=redact_fields),
        }
    ]

    dataset = _build_initial_ray_dataset(sample, cfg)
    selected_process = _force_fail_fast_ops(process_list[start_index : end_index + 1])
    ops = _load_ops_for_debug(cfg, selected_process)
    include_op_config = debug_cfg.get("include_op_config", True)
    include_traceback = debug_cfg.get("include_traceback", True)
    final_status = "success"
    executed_ops = 0

    for relative_index, op in enumerate(ops):
        op_index = start_index + relative_index
        op_cfg = process_list[op_index]
        started = time.perf_counter()
        started_at = _utc_now_iso()
        event = {
            **base,
            "event": "op_step",
            "op_index": op_index,
            "op_name": getattr(op, "_name", type(op).__name__),
            "op_type": _op_type(op),
            "single_sample_semantics": isinstance(op, (Deduplicator, Pipeline)),
            "started_at": started_at,
        }
        if include_op_config:
            event["op_config"] = _redact_op_config(op_cfg)
        try:
            dataset = dataset.process([op], tracer=None)
            data, row_count, schema = _snapshot_dataset(dataset, bytes_cfg=bytes_cfg, redact_fields=redact_fields)
            duration_ms = (time.perf_counter() - started) * 1000
            event.update(
                {
                    "status": "success",
                    "dropped": row_count == 0,
                    "duration_ms": duration_ms,
                    "finished_at": _utc_now_iso(),
                    "row_count": row_count,
                    "data_schema": schema,
                    "data": data,
                    "stats": to_jsonable_debug_value(
                        _extract_stats(data), bytes_cfg=bytes_cfg, redact_fields=redact_fields
                    ),
                    "meta": to_jsonable_debug_value(
                        _extract_meta(data), bytes_cfg=bytes_cfg, redact_fields=redact_fields
                    ),
                }
            )
            events.append(event)
            executed_ops += 1
            if row_count == 0:
                final_status = "dropped"
                break
        except Exception as err:
            duration_ms = (time.perf_counter() - started) * 1000
            final_status = "failed"
            event.update(
                {
                    "status": "failed",
                    "dropped": None,
                    "duration_ms": duration_ms,
                    "finished_at": _utc_now_iso(),
                    "row_count": None,
                    "data_schema": None,
                    "data": None,
                    "stats": None,
                    "meta": None,
                    "error_type": type(err).__name__,
                    "error_message": str(err),
                }
            )
            if include_traceback:
                event["traceback"] = traceback.format_exc()
            events.append(event)
            executed_ops += 1
            break

    events.append(_summary_event(base, status=final_status, executed_ops=executed_ops))
    return events, output_path


def _write_failure_summary(cfg, error: Exception, *, include_traceback: bool = True) -> tuple[list[dict], str]:
    debug_cfg = _get_debug_cfg(cfg)
    output_cfg = _normalize_debug_output(debug_cfg)
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    debug_run_id = debug_cfg.get("run_id") or f"debug_{timestamp}_{uuid.uuid4().hex[:8]}"
    output_path = _format_output_path(output_cfg["path"], cfg=cfg, debug_run_id=debug_run_id, timestamp=timestamp)
    base = _base_event(debug_run_id, cfg, output_path)
    error_data = {
        "error_type": type(error).__name__,
        "error_message": str(error),
    }
    if include_traceback:
        error_data["traceback"] = traceback.format_exc()
    return [_summary_event(base, status="failed", executed_ops=0, error=error_data)], output_path


def _persist_events(cfg, events: list[dict], output_path: str, output_cfg: dict) -> str:
    work_dir = getattr(cfg, "work_dir", None) or tempfile.gettempdir()
    local_dir = os.path.join(work_dir, ".debug_operator_pipeline")
    local_path = os.path.join(local_dir, f"{events[-1]['debug_run_id']}.jsonl")
    _write_events_local(events, local_path)
    _copy_trace_to_output(local_path, output_path, output_cfg)
    return local_path


def run(args=None) -> int:
    # `debug` is an existing flat boolean config in the regular Data-Juicer
    # entrypoint. This tool uses a structured `debug` block, so avoid the
    # regular config display/log setup that treats any truthy `debug` value as
    # global DEBUG mode.
    cfg = init_configs(args=args, load_configs_only=True)
    try:
        debug_cfg = _get_debug_cfg(cfg)
        output_cfg = _normalize_debug_output(debug_cfg)
    except DebugConfigError as err:
        logger.error(str(err))
        return 2

    try:
        events, output_path = run_debug_pipeline(cfg)
    except DebugConfigError as err:
        include_traceback = (_plain(getattr(cfg, "debug", {})) or {}).get("include_traceback", True)
        try:
            events, output_path = _write_failure_summary(cfg, err, include_traceback=include_traceback)
        except DebugConfigError:
            logger.error(str(err))
            return 2
    except Exception as err:
        include_traceback = (_plain(getattr(cfg, "debug", {})) or {}).get("include_traceback", True)
        events, output_path = _write_failure_summary(cfg, err, include_traceback=include_traceback)

    try:
        local_path = _persist_events(cfg, events, output_path, output_cfg)
    except Exception as err:
        logger.error(f"Failed to persist debug trace: {err}")
        return 2

    logger.info(f"Wrote operator debug trace to {output_path} (local staging: {local_path})")
    return 0


@logger.catch(reraise=True)
def main():
    parser = argparse.ArgumentParser(
        description="Run a single-sample Data-Juicer Ray operator pipeline debug trace.",
        allow_abbrev=False,
    )
    parser.add_argument("--config", required=True, help="Data-Juicer YAML config with a `debug` block.")
    wrapper_args, dj_args = parser.parse_known_args()
    raise SystemExit(run(["--config", wrapper_args.config] + dj_args))


if __name__ == "__main__":
    main()
