from __future__ import annotations

from collections.abc import MutableMapping
from typing import Any

from data_juicer.core.io_utils import namespace_to_plain_dict


COMPACT_KEYS = {
    "target_bytes_per_file",
    "target_rows_per_file",
    "max_buffer_bytes",
}
DEFAULT_COMPACT_TARGET_BYTES_PER_FILE = 64 * 1024 * 1024
DEFAULT_COMPACT_TARGET_ROWS_PER_FILE = 200000


def _is_positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def normalize_fanout_compact(compact: Any, *, context: str = "export.targets[].compact") -> dict[str, int] | None:
    compact = namespace_to_plain_dict(compact)
    if compact is False:
        return None
    if not isinstance(compact, dict):
        raise ValueError(f"`{context}` must be a dictionary or false; `compact: true` is not supported.")

    unknown_keys = sorted(set(compact) - COMPACT_KEYS)
    if unknown_keys:
        raise ValueError(f"`{context}` does not support fields: {unknown_keys}.")

    target_bytes = compact.get("target_bytes_per_file", DEFAULT_COMPACT_TARGET_BYTES_PER_FILE)
    target_rows = compact.get("target_rows_per_file", DEFAULT_COMPACT_TARGET_ROWS_PER_FILE)
    if not _is_positive_int(target_bytes):
        raise ValueError(f"`{context}.target_bytes_per_file` must be a positive integer.")
    if not _is_positive_int(target_rows):
        raise ValueError(f"`{context}.target_rows_per_file` must be a positive integer.")

    target_bytes = int(target_bytes)
    target_rows = int(target_rows)
    max_buffer_bytes = compact.get("max_buffer_bytes", 2 * target_bytes)
    if not _is_positive_int(max_buffer_bytes):
        raise ValueError(f"`{context}.max_buffer_bytes` must be a positive integer when set.")
    if max_buffer_bytes < target_bytes:
        raise ValueError(f"`{context}.max_buffer_bytes` must be greater than or equal to target_bytes_per_file.")

    return {
        "target_bytes_per_file": target_bytes,
        "target_rows_per_file": target_rows,
        "max_buffer_bytes": int(max_buffer_bytes),
    }


def normalize_fanout_target_compacts(targets: list[MutableMapping[str, Any]]) -> None:
    expected_compact = None
    for index, target in enumerate(targets):
        extra_args = namespace_to_plain_dict(target.get("extra_args") or {})
        if isinstance(extra_args, dict) and "compact" in extra_args:
            raise ValueError(
                "`export.targets[].compact` must be configured at the target top level; "
                "`export.targets[].extra_args.compact` is not supported."
            )

        raw_compact = target.get("compact") if "compact" in target else False
        compact = normalize_fanout_compact(raw_compact, context=f"export.targets[{index}].compact")
        if compact is None:
            target["compact"] = False
            continue

        target["compact"] = compact
        if expected_compact is None:
            expected_compact = compact
            continue
        if compact != expected_compact:
            raise ValueError("All compact `export.targets[]` entries must use identical compact configs.")
