from __future__ import annotations

from collections.abc import MutableMapping
from typing import Any

from data_juicer.core.io_utils import namespace_to_plain_dict


COMPACT_KEYS = {
    "target_bytes_per_file",
    "target_rows_per_file",
    "max_buffer_bytes",
}
REQUIRED_COMPACT_KEYS = {
    "target_bytes_per_file",
    "target_rows_per_file",
}


def _is_positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def normalize_fanout_compact(compact: Any, *, context: str = "export.targets[].compact") -> dict[str, int]:
    compact = namespace_to_plain_dict(compact)
    if not isinstance(compact, dict):
        raise ValueError(f"`{context}` must be a dictionary; `compact: true` is not supported.")

    unknown_keys = sorted(set(compact) - COMPACT_KEYS)
    if unknown_keys:
        raise ValueError(f"`{context}` does not support fields: {unknown_keys}.")

    for field in sorted(REQUIRED_COMPACT_KEYS):
        if field not in compact:
            raise ValueError(f"`{context}.{field}` is required.")
        if not _is_positive_int(compact[field]):
            raise ValueError(f"`{context}.{field}` must be a positive integer.")

    target_bytes = int(compact["target_bytes_per_file"])
    max_buffer_bytes = compact.get("max_buffer_bytes", 2 * target_bytes)
    if not _is_positive_int(max_buffer_bytes):
        raise ValueError(f"`{context}.max_buffer_bytes` must be a positive integer when set.")
    if max_buffer_bytes < target_bytes:
        raise ValueError(f"`{context}.max_buffer_bytes` must be greater than or equal to target_bytes_per_file.")

    return {
        "target_bytes_per_file": target_bytes,
        "target_rows_per_file": int(compact["target_rows_per_file"]),
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

        if "compact" not in target:
            continue

        compact = normalize_fanout_compact(target.get("compact"), context=f"export.targets[{index}].compact")
        target["compact"] = compact
        if expected_compact is None:
            expected_compact = compact
            continue
        if compact != expected_compact:
            raise ValueError("All compact `export.targets[]` entries must use identical compact configs.")
