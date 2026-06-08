#!/usr/bin/env python3
"""Summarize HDFS directory listings from ExecuteHdfsCommand compactly."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from typing import Any, Dict, List, Optional


def _body_from_response(response: Dict[str, Any]) -> Dict[str, Any]:
    body = response.get("data", {}).get("resp_body_json", response)
    if isinstance(body, str):
        body = json.loads(body)
    if not isinstance(body, dict):
        raise ValueError("ExecuteHdfsCommand response body must be a JSON object")
    return body


def _load_output_from_response(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        response = json.load(handle)
    body = _body_from_response(response)
    status_code = body.get("status_code", 0)
    if status_code not in (0, "0", None):
        message = body.get("status_message") or body.get("error") or "unknown error"
        raise ValueError(f"ExecuteHdfsCommand failed with status_code={status_code}: {message}")
    output = body.get("output")
    if not isinstance(output, str):
        raise ValueError(f"Cannot find string output in ExecuteHdfsCommand response: {path}")
    return {
        "output": output,
        "command": body.get("command"),
        "status_code": status_code,
        "status_message": body.get("status_message", ""),
        "bytes_read": body.get("bytes_read"),
    }


def _parse_ls_line(line: str) -> Optional[Dict[str, Any]]:
    if not line or line.startswith("Found "):
        return None
    if not (line.startswith("-") or line.startswith("d")):
        return None
    parts = line.split(maxsplit=7)
    if len(parts) < 8:
        return None
    kind = "directory" if parts[0].startswith("d") else "file"
    try:
        size = int(parts[4])
    except ValueError:
        return None
    return {
        "kind": kind,
        "permissions": parts[0],
        "replication": parts[1],
        "owner": parts[2],
        "group": parts[3],
        "size": size,
        "mtime": f"{parts[5]} {parts[6]}",
        "path": parts[7],
    }


def summarize_ls_output(output: str, *, sample_size: int = 10) -> Dict[str, Any]:
    entries = [entry for line in output.splitlines() if (entry := _parse_ls_line(line))]
    files = [entry for entry in entries if entry["kind"] == "file"]
    dirs = [entry for entry in entries if entry["kind"] == "directory"]
    total_bytes = sum(entry["size"] for entry in files)
    latest = max(files, key=lambda entry: (entry["mtime"], entry["path"]), default=None)
    largest = max(files, key=lambda entry: (entry["size"], entry["path"]), default=None)
    latest_files = sorted(files, key=lambda entry: (entry["mtime"], entry["path"]), reverse=True)[
        :sample_size
    ]
    largest_files = sorted(files, key=lambda entry: (entry["size"], entry["path"]), reverse=True)[
        :sample_size
    ]

    summary = {
        "file_count": len(files),
        "dir_count": len(dirs),
        "total_bytes": total_bytes,
        "latest_mtime": latest["mtime"] if latest else None,
        "latest_path": latest["path"] if latest else None,
        "largest_file_bytes": largest["size"] if largest else None,
        "largest_file_path": largest["path"] if largest else None,
        "latest_files": _compact_file_sample(latest_files),
        "largest_files": _compact_file_sample(largest_files),
        "_files": {entry["path"]: {"size": entry["size"], "mtime": entry["mtime"]} for entry in files},
    }
    return {key: value for key, value in summary.items() if value is not None}


def load_summary_from_response(path: str, *, sample_size: int = 10) -> Dict[str, Any]:
    loaded = _load_output_from_response(path)
    summary = summarize_ls_output(loaded["output"], sample_size=sample_size)
    for key in ["command", "status_code", "status_message", "bytes_read"]:
        if loaded.get(key) is not None:
            summary[key] = loaded[key]
    return summary


def _compact_file_sample(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "path": entry["path"],
            "size": entry["size"],
            "mtime": entry["mtime"],
        }
        for entry in entries
    ]


def compare_summaries(
    before: Dict[str, Any],
    after: Dict[str, Any],
    *,
    sample_size: int = 20,
) -> Dict[str, Any]:
    before_files = before.get("_files", {})
    after_files = after.get("_files", {})
    before_paths = set(before_files)
    after_paths = set(after_files)

    changed = []
    for path in sorted(before_paths & after_paths):
        old = before_files[path]
        new = after_files[path]
        if old["size"] == new["size"] and old["mtime"] == new["mtime"]:
            continue
        changed.append(
            {
                "path": path,
                "old_size": old["size"],
                "new_size": new["size"],
                "delta_bytes": new["size"] - old["size"],
                "old_mtime": old["mtime"],
                "new_mtime": new["mtime"],
            }
        )

    changed.sort(key=lambda item: (abs(item["delta_bytes"]), item["path"]), reverse=True)
    added = sorted(after_paths - before_paths)
    removed = sorted(before_paths - after_paths)
    return {
        "before": _public_summary(before),
        "after": _public_summary(after),
        "delta_file_count": after.get("file_count", 0) - before.get("file_count", 0),
        "delta_total_bytes": after.get("total_bytes", 0) - before.get("total_bytes", 0),
        "changed_file_count": len(changed),
        "added_file_count": len(added),
        "removed_file_count": len(removed),
        "changed_files": changed[:sample_size],
        "added_files": added[:sample_size],
        "removed_files": removed[:sample_size],
    }


def fetch_summary_with_bytedcli(
    hdfs_path: str,
    *,
    username: str,
    user_email: str,
    env: str,
    idc: str,
    zone: str,
    cluster: str,
    sample_size: int = 10,
) -> Dict[str, Any]:
    body = {
        "command_line": f"hdfs dfs -ls {hdfs_path}",
        "user_context": {
            "username": username,
            "user_role": "",
            "user_email": user_email,
        },
        "base": {
            "log_id": "",
            "caller": "",
            "addr": "",
            "client": "",
            "traffic_env": {"open": False, "env": ""},
            "extra": {},
        },
    }
    command = [
        "bytedcli",
        "--json",
        "bits",
        "rpc-call",
        "ad.ai.data_forge",
        "ExecuteHdfsCommand",
        "--idl-version",
        "codex/use-python-311",
        "--idl-source",
        "branch",
        "--zone",
        zone,
        "--idc",
        idc,
        "--env",
        env,
        "--cluster",
        cluster,
        "--body",
        json.dumps(body),
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    response = json.loads(result.stdout)
    body_json = _body_from_response(response)
    status_code = body_json.get("status_code", 0)
    if status_code not in (0, "0", None):
        message = body_json.get("status_message") or "unknown error"
        raise ValueError(f"ExecuteHdfsCommand failed with status_code={status_code}: {message}")
    output = body_json.get("output")
    if not isinstance(output, str):
        raise ValueError("ExecuteHdfsCommand response did not contain output")
    summary = summarize_ls_output(output, sample_size=sample_size)
    summary["command"] = body_json.get("command") or body["command_line"]
    return summary


def _public_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in summary.items() if not key.startswith("_")}


def _human_bytes(value: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    number = float(value)
    for unit in units:
        if abs(number) < 1024 or unit == units[-1]:
            return f"{number:.1f}{unit}" if unit != "B" else f"{int(number)}B"
        number /= 1024
    return f"{value}B"


def format_summary_markdown(summary: Dict[str, Any]) -> str:
    public = _public_summary(summary)
    lines = ["# HDFS LS Summary", ""]
    for key in [
        "command",
        "file_count",
        "dir_count",
        "total_bytes",
        "latest_mtime",
        "latest_path",
        "largest_file_bytes",
        "largest_file_path",
    ]:
        if key in public:
            value = public[key]
            if key.endswith("_bytes") or key == "total_bytes":
                value = f"{value} ({_human_bytes(int(value))})"
            lines.append(f"- {key}: `{value}`")
    if public.get("latest_files"):
        lines.extend(["", "## Latest Files"])
        for item in public["latest_files"]:
            lines.append(f"- `{item['mtime']}` `{item['size']}` `{item['path']}`")
    return "\n".join(lines)


def format_comparison_markdown(comparison: Dict[str, Any]) -> str:
    lines = ["# HDFS LS Comparison", ""]
    for key in [
        "delta_file_count",
        "delta_total_bytes",
        "changed_file_count",
        "added_file_count",
        "removed_file_count",
    ]:
        value = comparison[key]
        if key == "delta_total_bytes":
            value = f"{value} ({_human_bytes(int(value))})"
        lines.append(f"- {key}: `{value}`")

    before = comparison["before"]
    after = comparison["after"]
    lines.extend(
        [
            f"- before_total_bytes: `{before.get('total_bytes', 0)}`",
            f"- after_total_bytes: `{after.get('total_bytes', 0)}`",
            f"- before_latest: `{before.get('latest_mtime')}` `{before.get('latest_path')}`",
            f"- after_latest: `{after.get('latest_mtime')}` `{after.get('latest_path')}`",
        ]
    )
    if comparison["changed_files"]:
        lines.extend(["", "## Changed Files"])
        for item in comparison["changed_files"]:
            lines.append(
                "- "
                f"`{item['delta_bytes']}` "
                f"`{item['old_size']}->{item['new_size']}` "
                f"`{item['old_mtime']}->{item['new_mtime']}` "
                f"`{item['path']}`"
            )
    return "\n".join(lines)


def _read_summary(args: argparse.Namespace) -> Dict[str, Any]:
    if args.response:
        return load_summary_from_response(args.response, sample_size=args.sample_size)
    if args.path:
        username = args.username or os.environ.get("USER") or ""
        user_email = args.user_email or os.environ.get("BYTE_USER_EMAIL") or ""
        if not username or not user_email:
            raise ValueError("--path requires --username and --user-email unless env vars provide them")
        samples = [
            fetch_summary_with_bytedcli(
                args.path,
                username=username,
                user_email=user_email,
                env=args.env,
                idc=args.idc,
                zone=args.zone,
                cluster=args.cluster,
                sample_size=args.sample_size,
            )
        ]
        for _ in range(1, args.samples):
            time.sleep(args.interval)
            samples.append(
                fetch_summary_with_bytedcli(
                    args.path,
                    username=username,
                    user_email=user_email,
                    env=args.env,
                    idc=args.idc,
                    zone=args.zone,
                    cluster=args.cluster,
                    sample_size=args.sample_size,
                )
            )
        if len(samples) == 1:
            return samples[0]
        return {"comparison": compare_summaries(samples[0], samples[-1], sample_size=args.sample_size)}
    raise ValueError("Provide --response, --compare-response, or --path")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--response", help="ExecuteHdfsCommand JSON response containing hdfs dfs -ls output")
    source.add_argument(
        "--compare-response",
        nargs=2,
        metavar=("BEFORE", "AFTER"),
        help="Compare two ExecuteHdfsCommand JSON responses containing hdfs dfs -ls output",
    )
    source.add_argument("--path", help="HDFS directory path; reads hdfs dfs -ls via ExecuteHdfsCommand")
    parser.add_argument("--samples", type=int, default=1, help="Number of live --path samples")
    parser.add_argument("--interval", type=float, default=60.0, help="Seconds between live --path samples")
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--format", choices=["json", "markdown"], default="markdown")
    parser.add_argument("--username", default=None)
    parser.add_argument("--user-email", default=None)
    parser.add_argument("--env", default="ppe_terranova")
    parser.add_argument("--idc", default="hl")
    parser.add_argument("--zone", default="CN")
    parser.add_argument("--cluster", default="default")
    args = parser.parse_args(argv)

    if args.samples < 1:
        raise ValueError("--samples must be >= 1")

    if args.compare_response:
        before = load_summary_from_response(args.compare_response[0], sample_size=args.sample_size)
        after = load_summary_from_response(args.compare_response[1], sample_size=args.sample_size)
        result = {"comparison": compare_summaries(before, after, sample_size=args.sample_size)}
    else:
        result = _read_summary(args)

    if args.format == "json":
        print(json.dumps(_strip_internal(result), indent=2, ensure_ascii=False, sort_keys=True))
    elif "comparison" in result:
        print(format_comparison_markdown(result["comparison"]))
    else:
        print(format_summary_markdown(result))
    return 0


def _strip_internal(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _strip_internal(item) for key, item in value.items() if not key.startswith("_")}
    if isinstance(value, list):
        return [_strip_internal(item) for item in value]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
