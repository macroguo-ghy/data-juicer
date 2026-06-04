from __future__ import annotations

from typing import Any, Dict


_MODE_BASED_TARGETS = {"hdfs", "hive", "lark", "local"}
_OPERATION_BASED_TARGETS = {"magnus"}

_OPERATION_TO_MODE = {
    "APPEND": "append",
    "OVERWRITE": "overwrite",
    "ERROR_IF_EXISTS": "error_if_exists",
}
_MODE_TO_OPERATION = {
    "append": "APPEND",
    "overwrite": "OVERWRITE",
}
_OPERATION_EQUIVALENT_MODE = {
    "APPEND": "append",
    "OVERWRITE": "overwrite",
    "OVERWRITE_PARTITION": "overwrite",
}


def normalize_export_write_mode_aliases(
    export_cfg: Dict[str, Any],
    *,
    target: str | None = None,
    context: str = "export",
) -> Dict[str, Any]:
    """Normalize structured export `mode`/`operation` aliases.

    Data-Juicer file-like exporters use `mode`, while Magnus uses `operation`.
    This helper keeps the backend-facing field canonical and rejects ambiguous
    configs where both aliases are set to different write semantics.
    """
    normalized = dict(export_cfg)
    export_target = _infer_alias_target(normalized, target)
    if export_target in _OPERATION_BASED_TARGETS:
        return _normalize_operation_based_aliases(normalized, context=context)
    if export_target in _MODE_BASED_TARGETS:
        return _normalize_mode_based_aliases(normalized, context=context)
    return normalized


def _infer_alias_target(export_cfg: Dict[str, Any], target: str | None) -> str | None:
    if target:
        return str(target).lower()
    configured_target = export_cfg.get("target")
    if configured_target:
        return str(configured_target).lower()
    if export_cfg.get("hive_table"):
        return "hive"
    if export_cfg.get("table_name") and export_cfg.get("magnus_conf") is not None:
        return "magnus"
    if export_cfg.get("lark_path"):
        return "lark"
    path = export_cfg.get("path")
    if isinstance(path, str) and path.startswith("hdfs://"):
        return "hdfs"
    return None


def _normalize_mode_based_aliases(export_cfg: Dict[str, Any], *, context: str) -> Dict[str, Any]:
    has_mode = "mode" in export_cfg and export_cfg.get("mode") is not None
    has_operation = "operation" in export_cfg and export_cfg.get("operation") is not None
    if not has_mode and not has_operation:
        return export_cfg

    mode = _normalize_mode_value(export_cfg["mode"], context=context) if has_mode else None
    operation_mode = _operation_to_mode(export_cfg["operation"], context=context) if has_operation else None
    if mode is not None and operation_mode is not None and mode != operation_mode:
        _raise_alias_conflict(context, export_cfg["mode"], export_cfg["operation"])

    export_cfg["mode"] = operation_mode or mode
    export_cfg.pop("operation", None)
    return export_cfg


def _normalize_operation_based_aliases(export_cfg: Dict[str, Any], *, context: str) -> Dict[str, Any]:
    has_mode = "mode" in export_cfg and export_cfg.get("mode") is not None
    has_operation = "operation" in export_cfg and export_cfg.get("operation") is not None
    if not has_mode and not has_operation:
        return export_cfg

    mode_operation = _mode_to_operation(export_cfg["mode"], context=context) if has_mode else None
    operation = _normalize_operation_value(export_cfg["operation"], context=context) if has_operation else None
    if mode_operation is not None and operation is not None:
        operation_mode = _OPERATION_EQUIVALENT_MODE.get(operation)
        mode = _normalize_mode_value(export_cfg["mode"], context=context)
        if operation_mode != mode:
            _raise_alias_conflict(context, export_cfg["mode"], export_cfg["operation"])

    export_cfg["operation"] = operation or mode_operation
    export_cfg.pop("mode", None)
    return export_cfg


def _normalize_mode_value(value: Any, *, context: str) -> str:
    mode = str(value).strip().lower()
    if not mode:
        raise ValueError(f"`{context}.mode` must not be empty.")
    return mode


def _normalize_operation_value(value: Any, *, context: str) -> str:
    operation = str(value).strip().upper()
    if not operation:
        raise ValueError(f"`{context}.operation` must not be empty.")
    if operation == "OVERWRITE-PARTITION":
        operation = "OVERWRITE_PARTITION"
    return operation


def _operation_to_mode(value: Any, *, context: str) -> str:
    operation = _normalize_operation_value(value, context=context)
    try:
        return _OPERATION_TO_MODE[operation]
    except KeyError as exc:
        raise ValueError(
            f"`{context}.operation` must be one of APPEND, OVERWRITE, or ERROR_IF_EXISTS "
            "when used as an alias for `mode`."
        ) from exc


def _mode_to_operation(value: Any, *, context: str) -> str:
    mode = _normalize_mode_value(value, context=context)
    try:
        return _MODE_TO_OPERATION[mode]
    except KeyError as exc:
        raise ValueError(
            f"`{context}.mode` must be append or overwrite when used as an alias for Magnus `operation`."
        ) from exc


def _raise_alias_conflict(context: str, mode: Any, operation: Any) -> None:
    raise ValueError(
        f"`{context}` has conflicting `mode` ({mode!r}) and `operation` ({operation!r}) values."
    )
