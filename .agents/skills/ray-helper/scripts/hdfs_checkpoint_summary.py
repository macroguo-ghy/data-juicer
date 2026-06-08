#!/usr/bin/env python3
"""Summarize Data-Juicer Ray HDFS checkpoint metadata without dumping payloads."""

from __future__ import annotations

import argparse
import base64
import json
import os
import pickle
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


FILE_SUFFIXES = {
    ".arrow",
    ".csv",
    ".gz",
    ".json",
    ".jsonl",
    ".orc",
    ".parquet",
    ".txt",
}


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, set):
        return sorted(_jsonable(v) for v in value)
    return value


def _looks_file_like(path: str) -> bool:
    name = Path(path).name
    suffixes = Path(name).suffixes
    return (
        name.startswith("part-")
        or any(suffix.lower() in FILE_SUFFIXES for suffix in suffixes)
        or "." in name
    )


def _classify_completed_files(paths: Iterable[Any]) -> str:
    normalized = [str(path) for path in paths]
    if not normalized:
        return "empty"
    file_like = [_looks_file_like(path) for path in normalized]
    if all(file_like):
        return "file_like"
    if not any(file_like):
        return "directory_like"
    return "mixed"


def summarize_checkpoint_metadata(metadata: Dict[str, Any], sample_size: int = 20) -> Dict[str, Any]:
    completed_files = metadata.get("completed_files") or []
    completed_files_list = [str(path) for path in completed_files]
    completed_files_sample = sorted(set(completed_files_list))[:sample_size]
    progress = metadata.get("global_progress") or []

    summary = {
        "version": metadata.get("version"),
        "checkpoint_id": metadata.get("checkpoint_id"),
        "job_id": _jsonable(metadata.get("job_id")),
        "completed_files_count": len(completed_files_list),
        "completed_files_unique_count": len(set(completed_files_list)),
        "completed_files_kind": _classify_completed_files(completed_files_list),
        "completed_files_sample": completed_files_sample,
        "global_progress": _jsonable(progress),
    }
    return {key: value for key, value in summary.items() if value is not None}


def summarize_checkpoint_metadata_bytes(raw: bytes, sample_size: int = 20) -> Dict[str, Any]:
    metadata = pickle.loads(raw)
    if not isinstance(metadata, dict):
        raise TypeError(f"Checkpoint metadata must be a dict, got {type(metadata).__name__}")
    return summarize_checkpoint_metadata(metadata, sample_size=sample_size)


def _load_raw_from_response(path: str) -> bytes:
    with open(path, "r", encoding="utf-8") as handle:
        response = json.load(handle)
    body = response.get("data", {}).get("resp_body_json", response)
    if isinstance(body, str):
        body = json.loads(body)
    output = body.get("output")
    if not isinstance(output, str):
        raise ValueError(f"Cannot find string output in ExecuteHdfsCommand response: {path}")
    encoding = body.get("output_encoding") or body.get("encoding") or "text"
    if encoding == "base64":
        return base64.b64decode(output)
    return output.encode("latin1")


def _fetch_checkpoint_metadata_with_bytedcli(
    checkpoint_dir: str,
    *,
    username: str,
    user_email: str,
    env: str,
    idc: str,
    zone: str,
    cluster: str,
) -> bytes:
    body = {
        "command_line": f"hdfs dfs -cat {checkpoint_dir.rstrip('/')}/_metadata",
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
    body_json = response.get("data", {}).get("resp_body_json", response)
    if isinstance(body_json, str):
        body_json = json.loads(body_json)
    output = body_json.get("output")
    if not isinstance(output, str):
        raise ValueError("ExecuteHdfsCommand response did not contain output")
    if body_json.get("output_encoding") == "base64":
        return base64.b64decode(output)
    return output.encode("latin1")


def _read_raw(args: argparse.Namespace) -> bytes:
    if args.metadata_file:
        with open(args.metadata_file, "rb") as handle:
            return handle.read()
    if args.metadata_response:
        return _load_raw_from_response(args.metadata_response)
    if args.checkpoint_dir:
        username = args.username or os.environ.get("USER") or ""
        user_email = args.user_email or os.environ.get("BYTE_USER_EMAIL") or ""
        if not username or not user_email:
            raise ValueError("--checkpoint-dir requires --username and --user-email unless env vars provide them")
        return _fetch_checkpoint_metadata_with_bytedcli(
            args.checkpoint_dir,
            username=username,
            user_email=user_email,
            env=args.env,
            idc=args.idc,
            zone=args.zone,
            cluster=args.cluster,
        )
    raise ValueError("Provide one of --metadata-file, --metadata-response, or --checkpoint-dir")


def format_markdown(summary: Dict[str, Any]) -> str:
    lines: List[str] = ["# HDFS Checkpoint Summary", ""]
    for key in [
        "version",
        "checkpoint_id",
        "job_id",
        "completed_files_count",
        "completed_files_unique_count",
        "completed_files_kind",
        "completed_files_sample",
        "global_progress",
    ]:
        if key in summary:
            value = summary[key]
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False, sort_keys=True)
            lines.append(f"- {key}: `{value}`")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--metadata-file", help="Local pickle metadata file")
    source.add_argument("--metadata-response", help="ExecuteHdfsCommand JSON response containing metadata output")
    source.add_argument("--checkpoint-dir", help="HDFS checkpoint directory; reads <dir>/_metadata via ExecuteHdfsCommand")
    parser.add_argument("--sample-size", type=int, default=20)
    parser.add_argument("--format", choices=["json", "markdown"], default="markdown")
    parser.add_argument("--username", default=None)
    parser.add_argument("--user-email", default=None)
    parser.add_argument("--env", default="ppe_terranova")
    parser.add_argument("--idc", default="hl")
    parser.add_argument("--zone", default="CN")
    parser.add_argument("--cluster", default="default")
    args = parser.parse_args(argv)

    summary = summarize_checkpoint_metadata_bytes(_read_raw(args), sample_size=args.sample_size)
    if args.format == "json":
        print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True))
    else:
        print(format_markdown(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
