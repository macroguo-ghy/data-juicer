#!/usr/bin/env python3
"""Compare two compact Ray job summaries without pasting full RayUI payloads."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import ray_job_summary  # noqa: E402


DIFF_CONFIG_KEYS = {
    "project_name",
    "executor_type",
    "ray_address",
    "min_common_dep_num_to_combine",
    "ray_data_checkpoint",
    "ray_data_context",
    "dataset",
    "process",
    "export",
}

OPERATOR_COMPARE_FIELDS = [
    "state",
    "progress",
    "total",
    "total_rows",
    "avg_output_rows",
    "avg_output_bytes",
    "current_bytes",
    "min_rows_per_bundle",
]


def normalize_operator_name(name: Any) -> str:
    return re.sub(r"_\d+$", "", str(name or ""))


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, set):
        return sorted(_jsonable(v) for v in value)
    return value


def _diff_values(left: Any, right: Any, path: str = "", max_diffs: int = 80) -> List[Dict[str, Any]]:
    if max_diffs <= 0:
        return []
    if isinstance(left, dict) and isinstance(right, dict):
        diffs: List[Dict[str, Any]] = []
        for key in sorted(set(left) | set(right), key=str):
            next_path = f"{path}.{key}" if path else str(key)
            diffs.extend(_diff_values(left.get(key), right.get(key), next_path, max_diffs - len(diffs)))
            if len(diffs) >= max_diffs:
                break
        return diffs
    if isinstance(left, list) and isinstance(right, list):
        if left == right:
            return []
        return [{"path": path, "left": _jsonable(left), "right": _jsonable(right)}]
    if left != right:
        return [{"path": path, "left": _jsonable(left), "right": _jsonable(right)}]
    return []


def _dataset_key(dataset: Dict[str, Any], fallback: int) -> str:
    return str(dataset.get("dataset") or dataset.get("job_id") or f"dataset_{fallback}")


def _operator_index(summary: Dict[str, Any]) -> Dict[Tuple[str, str, int], Dict[str, Any]]:
    indexed: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
    seen: Dict[Tuple[str, str], int] = {}
    for dataset_idx, dataset in enumerate(summary.get("datasets") or []):
        if not isinstance(dataset, dict):
            continue
        dataset_name = _dataset_key(dataset, dataset_idx)
        for operator in dataset.get("operators") or []:
            if not isinstance(operator, dict):
                continue
            normalized = normalize_operator_name(operator.get("operator") or operator.get("name"))
            base_key = (dataset_name, normalized)
            occurrence = seen.get(base_key, 0)
            seen[base_key] = occurrence + 1
            indexed[(dataset_name, normalized, occurrence)] = operator
    return indexed


def _context_index(summary: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    indexed: Dict[str, Dict[str, Any]] = {}
    for idx, dataset in enumerate(summary.get("datasets") or []):
        if isinstance(dataset, dict):
            indexed[_dataset_key(dataset, idx)] = dataset.get("context") or {}
    return indexed


def _ratio_score(left: Any, right: Any) -> float:
    if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
        return 0.0
    if left == right:
        return 0.0
    smaller = max(min(abs(left), abs(right)), 1)
    return abs(right - left) / smaller


def _operator_diff_row(
    dataset: str,
    normalized_operator: str,
    left: Optional[Dict[str, Any]],
    right: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    left = left or {}
    right = right or {}
    changed_fields = [
        field for field in OPERATOR_COMPARE_FIELDS if left.get(field) != right.get(field)
    ]
    row = {
        "dataset": dataset,
        "operator": normalized_operator,
        "left_operator": left.get("operator") or left.get("name"),
        "right_operator": right.get("operator") or right.get("name"),
        "changed_fields": changed_fields,
        "left_total": left.get("total"),
        "right_total": right.get("total"),
        "left_avg_output_rows": left.get("avg_output_rows"),
        "right_avg_output_rows": right.get("avg_output_rows"),
        "left_avg_output_bytes": left.get("avg_output_bytes"),
        "right_avg_output_bytes": right.get("avg_output_bytes"),
        "left_min_rows_per_bundle": left.get("min_rows_per_bundle"),
        "right_min_rows_per_bundle": right.get("min_rows_per_bundle"),
    }
    return {key: value for key, value in row.items() if value is not None or key == "changed_fields"}


def build_comparison(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    left_config = left.get("config") or {}
    right_config = right.get("config") or {}
    focused_left_config = {key: left_config.get(key) for key in DIFF_CONFIG_KEYS if key in left_config}
    focused_right_config = {key: right_config.get(key) for key in DIFF_CONFIG_KEYS if key in right_config}

    left_context = _context_index(left)
    right_context = _context_index(right)
    context_diffs = []
    for dataset in sorted(set(left_context) | set(right_context)):
        for diff in _diff_values(left_context.get(dataset), right_context.get(dataset)):
            context_diffs.append({"dataset": dataset, **diff})

    left_ops = _operator_index(left)
    right_ops = _operator_index(right)
    operator_diffs = []
    for key in sorted(set(left_ops) | set(right_ops)):
        left_op = left_ops.get(key)
        right_op = right_ops.get(key)
        row = _operator_diff_row(key[0], key[1], left_op, right_op)
        if row.get("changed_fields") or left_op is None or right_op is None:
            operator_diffs.append(row)
    operator_diffs.sort(
        key=lambda row: max(
            _ratio_score(row.get("left_total"), row.get("right_total")),
            _ratio_score(row.get("left_avg_output_rows"), row.get("right_avg_output_rows")),
            _ratio_score(row.get("left_avg_output_bytes"), row.get("right_avg_output_bytes")),
        ),
        reverse=True,
    )

    return {
        "left_job": left.get("job") or {},
        "right_job": right.get("job") or {},
        "config_diffs": _diff_values(focused_left_config, focused_right_config),
        "context_diffs": context_diffs,
        "operator_diffs": operator_diffs,
    }


def _load_summary(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _summary_from_arg(value: str, *, timeout: int) -> Dict[str, Any]:
    candidate = Path(value)
    if candidate.exists():
        return _load_summary(str(candidate))
    return ray_job_summary.fetch_summary(value, timeout=timeout)


def _format_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def format_markdown(comparison: Dict[str, Any]) -> str:
    lines: List[str] = ["# Ray Job Comparison", ""]
    left_job = comparison.get("left_job") or {}
    right_job = comparison.get("right_job") or {}
    lines.append(f"- left: `{left_job.get('job_id', '-')}` status=`{left_job.get('status', '-')}`")
    lines.append(f"- right: `{right_job.get('job_id', '-')}` status=`{right_job.get('status', '-')}`")

    config_diffs = comparison.get("config_diffs") or []
    lines.append("")
    lines.append("## Config Diffs")
    if not config_diffs:
        lines.append("- none")
    else:
        for diff in config_diffs[:40]:
            lines.append(
                f"- `{diff['path']}`: left=`{_format_value(diff.get('left'))}` "
                f"right=`{_format_value(diff.get('right'))}`"
            )

    context_diffs = comparison.get("context_diffs") or []
    lines.append("")
    lines.append("## Dataset Context Diffs")
    if not context_diffs:
        lines.append("- none")
    else:
        for diff in context_diffs[:40]:
            lines.append(
                f"- `{diff['dataset']}.{diff['path']}`: left=`{_format_value(diff.get('left'))}` "
                f"right=`{_format_value(diff.get('right'))}`"
            )

    lines.append("")
    lines.append("## Operator Diffs")
    operator_diffs = comparison.get("operator_diffs") or []
    if not operator_diffs:
        lines.append("- none")
    else:
        lines.append("| dataset | operator | total | avg rows | avg bytes | changed |")
        lines.append("|---|---|---:|---:|---:|---|")
        for diff in operator_diffs[:80]:
            lines.append(
                f"| `{diff.get('dataset')}` | `{diff.get('operator')}` | "
                f"{diff.get('left_total', '-')}/{diff.get('right_total', '-')} | "
                f"{diff.get('left_avg_output_rows', '-')}/{diff.get('right_avg_output_rows', '-')} | "
                f"{diff.get('left_avg_output_bytes', '-')}/{diff.get('right_avg_output_bytes', '-')} | "
                f"{', '.join(diff.get('changed_fields') or [])} |"
            )
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left", help="Left Ray job URL or compact summary JSON file")
    parser.add_argument("right", help="Right Ray job URL or compact summary JSON file")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--format", choices=["json", "markdown"], default="markdown")
    args = parser.parse_args(argv)

    comparison = build_comparison(
        _summary_from_arg(args.left, timeout=args.timeout),
        _summary_from_arg(args.right, timeout=args.timeout),
    )
    if args.format == "json":
        print(json.dumps(comparison, indent=2, ensure_ascii=False, sort_keys=True))
    else:
        print(format_markdown(comparison))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
