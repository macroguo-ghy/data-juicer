from __future__ import annotations

import csv
import hashlib
import importlib
import inspect
import json
import os
import posixpath
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple
from urllib.parse import parse_qs, quote, urlparse

from jsonargparse import Namespace, namespace_to_dict
from loguru import logger

_MAGNUS_RAY_DISABLE_REPARTITION = "magnus.ray.write.disable_repartition"
_MAGNUS_RAY_DISABLE_SORT = "magnus.ray.write.disable_sort"
MAGNUS_FAILURE_POLICY_ABORT = "abort"
MAGNUS_FAILURE_POLICY_COMMIT_COMPLETED_UNSAFE = "commit_completed_unsafe"
_MAGNUS_FAILURE_POLICY_SNAPSHOT_SUMMARY_KEY = "data_juicer.magnus.failure_policy"


def namespace_to_plain_dict(value: Any) -> Any:
    """Recursively convert jsonargparse namespaces to plain Python types."""
    if isinstance(value, Namespace):
        value = namespace_to_dict(value)
    if isinstance(value, dict):
        return {k: namespace_to_plain_dict(v) for k, v in value.items()}
    if isinstance(value, list):
        return [namespace_to_plain_dict(v) for v in value]
    return value


def _flatten_dotted_options(options: Any, prefix: str = "") -> Dict[str, Any]:
    options = namespace_to_plain_dict(options or {})
    if not isinstance(options, dict):
        return {}

    flattened = {}
    for key, value in options.items():
        dotted_key = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            flattened.update(_flatten_dotted_options(value, dotted_key))
        else:
            flattened[dotted_key] = value
    return flattened


def merge_dicts(base: Dict[str, Any] | None, override: Dict[str, Any] | None) -> Dict[str, Any]:
    merged = {}
    if base:
        merged.update(namespace_to_plain_dict(base))
    if override:
        merged.update(namespace_to_plain_dict(override))
    return merged


def make_staging_dir(work_dir: str, kind: str, name: str) -> str:
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
    path = os.path.join(work_dir, ".io_cache", kind, digest)
    os.makedirs(path, exist_ok=True)
    return path


def infer_local_name_from_uri(uri: str | None, default_name: str = "dataset") -> str:
    if not uri:
        return default_name
    if "://" in uri:
        parsed = urlparse(uri)
        basename = posixpath.basename(parsed.path.rstrip("/"))
    else:
        basename = os.path.basename(uri.rstrip("/"))
    return basename or default_name


def ensure_parent(path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)


def infer_storage_target_from_path(path: str | None) -> str:
    if not path:
        return "local"
    if path.startswith("s3://"):
        return "s3"
    if path.startswith("hdfs://"):
        return "hdfs"
    return "local"


def get_pyarrow_filesystem(uri: str):
    import pyarrow.fs as pa_fs

    fs, fs_path = pa_fs.FileSystem.from_uri(uri)
    return fs, fs_path


def copy_uri_to_local(uri: str, local_path: str) -> str:
    if not (uri.startswith("hdfs://") or uri.startswith("s3://") or uri.startswith("file://")):
        src = Path(uri)
        dst = Path(local_path)
        if src.is_dir():
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
        else:
            ensure_parent(str(dst))
            shutil.copy2(src, dst)
        return str(dst)

    import pyarrow.fs as pa_fs

    fs, fs_path = get_pyarrow_filesystem(uri)
    info = fs.get_file_info(fs_path)
    if info.type == pa_fs.FileType.NotFound:
        raise FileNotFoundError(f"Remote path not found: {uri}")

    local_root = Path(local_path)
    if info.type == pa_fs.FileType.File:
        ensure_parent(str(local_root))
        with fs.open_input_stream(fs_path) as rf, open(local_root, "wb") as wf:
            shutil.copyfileobj(rf, wf)
        return str(local_root)

    local_root.mkdir(parents=True, exist_ok=True)
    selector = pa_fs.FileSelector(fs_path, recursive=True)
    for file_info in fs.get_file_info(selector):
        if file_info.type != pa_fs.FileType.File:
            continue
        rel_path = os.path.relpath(file_info.path, fs_path)
        output_path = local_root / rel_path
        ensure_parent(str(output_path))
        with fs.open_input_stream(file_info.path) as rf, open(output_path, "wb") as wf:
            shutil.copyfileobj(rf, wf)
    return str(local_root)


def copy_local_to_uri(local_path: str, uri: str) -> None:
    if not (uri.startswith("hdfs://") or uri.startswith("s3://") or uri.startswith("file://")):
        src = Path(local_path)
        dst = Path(uri)
        if src.is_dir():
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
        else:
            ensure_parent(str(dst))
            shutil.copy2(src, dst)
        return

    import pyarrow.fs as pa_fs

    fs, fs_path = get_pyarrow_filesystem(uri)
    src = Path(local_path)
    if src.is_file():
        ensure_remote_parent(fs, fs_path)
        with open(src, "rb") as rf, fs.open_output_stream(fs_path) as wf:
            shutil.copyfileobj(rf, wf)
        return

    clear_remote_path(fs, fs_path)
    fs.create_dir(fs_path, recursive=True)
    for file_path in src.rglob("*"):
        if not file_path.is_file():
            continue
        rel_path = file_path.relative_to(src).as_posix()
        target_path = f"{fs_path.rstrip('/')}/{rel_path}"
        ensure_remote_parent(fs, target_path)
        with open(file_path, "rb") as rf, fs.open_output_stream(target_path) as wf:
            shutil.copyfileobj(rf, wf)


def clear_remote_path(fs, path: str) -> None:
    import pyarrow.fs as pa_fs

    info = fs.get_file_info(path)
    if info.type == pa_fs.FileType.Directory:
        fs.delete_dir(path)
    elif info.type == pa_fs.FileType.File:
        fs.delete_file(path)


def ensure_remote_parent(fs, path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        fs.create_dir(parent, recursive=True)


def import_optional_dependency(module_name: str, extra_name: str | None = None):
    try:
        return importlib.import_module(module_name)
    except ImportError as exc:
        extra_hint = f" Install the `{extra_name}` extra first." if extra_name else ""
        raise ImportError(f"Optional dependency `{module_name}` is required.{extra_hint}") from exc


def import_from_path(import_path: str):
    module_name, attr_name = import_path.split(":", 1)
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


def parse_lark_sheet_location(lark_path: str, sheet_id: str | None = None) -> Tuple[str, str]:
    if not isinstance(lark_path, str) or not lark_path.strip():
        raise ValueError("Lark loader requires a non-empty `lark_path`.")

    lark_path = lark_path.strip()
    parsed = urlparse(lark_path)
    if parsed.scheme or parsed.netloc:
        spreadsheet_token = posixpath.basename(parsed.path.rstrip("/"))
        query_sheet_ids = parse_qs(parsed.query).get("sheet", [])
        url_sheet_id = query_sheet_ids[0] if query_sheet_ids else None
    else:
        spreadsheet_token = lark_path.rstrip("/")
        url_sheet_id = None

    if not spreadsheet_token:
        raise ValueError("Unable to parse Lark spreadsheet token from `lark_path`.")

    if sheet_id is not None:
        if not isinstance(sheet_id, str) or not sheet_id.strip():
            raise ValueError("`sheet_id` must be a non-empty string when configured.")
        sheet_id = sheet_id.strip()

    if url_sheet_id is not None and sheet_id is not None and url_sheet_id != sheet_id:
        raise ValueError(
            f"Lark `sheet_id` conflict: URL query has `{url_sheet_id}`, "
            f"but config has `{sheet_id}`."
        )

    resolved_sheet_id = sheet_id or url_sheet_id
    if not resolved_sheet_id:
        raise ValueError("Lark loader requires a sheet id in `lark_path` query `sheet` or in `sheet_id`.")

    return spreadsheet_token, resolved_sheet_id


def _is_lark_export_permission_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "lark export task creation failed" in message and (
        "code=1069902" in message
        or "code=99991672" in message
        or "no permission" in message
        or "drive:export:readonly" in message
        or "docs:document:export" in message
    )


def _normalize_lark_csv_cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if value is None:
        return ""
    return value


def _write_lark_sheet_values_to_csv(values: list[list[Any]], output_path: str) -> str:
    ensure_parent(output_path)
    with open(output_path, "w", encoding="utf-8", newline="") as wf:
        writer = csv.writer(wf)
        for row in values:
            writer.writerow([_normalize_lark_csv_cell(cell) for cell in row])
    return output_path


def read_lark_sheet_to_csv(
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
    output_path: str,
    sheet_id: str | None = None,
    value_render_option: str = "ToString",
) -> str:
    lark = import_optional_dependency("lark_oapi")
    token, sheet_id = parse_lark_sheet_location(lark_path, sheet_id=sheet_id)

    client = (
        lark.Client.builder().app_id(lark_app_id).app_secret(lark_app_secret).log_level(lark.LogLevel.ERROR).build()
    )
    encoded_range = quote(sheet_id, safe="")
    request = (
        lark.BaseRequest.builder()
        .http_method(lark.HttpMethod.GET)
        .uri(
            f"/open-apis/sheets/v2/spreadsheets/{token}/values/{encoded_range}"
            f"?valueRenderOption={value_render_option}"
        )
        .token_types({lark.AccessTokenType.TENANT})
        .build()
    )
    response = client.request(request)
    if not response.success():
        raise RuntimeError(f"Lark sheet read failed: code={response.code}, msg={response.msg}")

    content = getattr(getattr(response, "raw", None), "content", None)
    if content is None:
        raise RuntimeError("Lark sheet read failed: empty response content")
    payload = json.loads(content.decode("utf-8") if isinstance(content, bytes) else content)
    values = payload.get("data", {}).get("valueRange", {}).get("values", [])
    if not values:
        raise RuntimeError("Lark sheet read returned no values")
    return _write_lark_sheet_values_to_csv(values, output_path)


def _export_lark_sheet_with_drive(
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
    output_path: str,
    file_extension: str = "csv",
    document_type: str = "sheet",
    sheet_id: str | None = None,
    wait_export_time_seconds: int = 60,
) -> str:
    lark = import_optional_dependency("lark_oapi")
    drive_v1 = import_optional_dependency("lark_oapi.api.drive.v1")
    token, sheet_id = parse_lark_sheet_location(lark_path, sheet_id=sheet_id)

    client = (
        lark.Client.builder().app_id(lark_app_id).app_secret(lark_app_secret).log_level(lark.LogLevel.ERROR).build()
    )

    request = (
        drive_v1.CreateExportTaskRequest.builder()
        .request_body(
            drive_v1.ExportTask.builder()
            .file_extension(file_extension)
            .token(token)
            .type(document_type)
            .sub_id(sheet_id)
            .build()
        )
        .build()
    )
    response = client.drive.v1.export_task.create(request)
    if not response.success():
        raise RuntimeError(f"Lark export task creation failed: code={response.code}, msg={response.msg}")

    ticket_request = drive_v1.GetExportTaskRequest.builder().ticket(response.data.ticket).token(token).build()
    start_time = time.time()
    ticket_response = None
    while time.time() - start_time < wait_export_time_seconds:
        ticket_response = client.drive.v1.export_task.get(ticket_request)
        if not ticket_response.success():
            raise RuntimeError(f"Lark export task polling failed: code={ticket_response.code}, msg={ticket_response.msg}")
        if ticket_response.data.result.job_status == 0:
            break
        time.sleep(1)
    if ticket_response is None or ticket_response.data.result.job_status != 0:
        job_status = None if ticket_response is None else ticket_response.data.result.job_status
        raise RuntimeError(f"Lark export task did not finish successfully, job_status={job_status}")

    download_request = (
        drive_v1.DownloadExportTaskRequest.builder().file_token(ticket_response.data.result.file_token).build()
    )
    download_response = client.drive.v1.export_task.download(download_request)
    if not download_response.success():
        raise RuntimeError(f"Lark file download failed: code={download_response.code}, msg={download_response.msg}")

    ensure_parent(output_path)
    with open(output_path, "wb") as wf:
        wf.write(download_response.file.read())
    return output_path


def export_lark_sheet_to_local(
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
    output_path: str,
    file_extension: str = "csv",
    document_type: str = "sheet",
    sheet_id: str | None = None,
    wait_export_time_seconds: int = 60,
) -> str:
    try:
        return _export_lark_sheet_with_drive(
            lark_path=lark_path,
            lark_app_id=lark_app_id,
            lark_app_secret=lark_app_secret,
            output_path=output_path,
            file_extension=file_extension,
            document_type=document_type,
            sheet_id=sheet_id,
            wait_export_time_seconds=wait_export_time_seconds,
        )
    except RuntimeError as exc:
        if not _is_lark_export_permission_error(exc):
            raise
        if file_extension != "csv" or document_type != "sheet":
            raise
        logger.warning("Lark export is not permitted; falling back to sheet values read.")
        return read_lark_sheet_to_csv(
            lark_path=lark_path,
            lark_app_id=lark_app_id,
            lark_app_secret=lark_app_secret,
            output_path=output_path,
            sheet_id=sheet_id,
        )


def upload_file_to_lark_sheet(
    local_path: str,
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
    cell_range: str,
) -> None:
    lark = import_optional_dependency("lark_oapi")
    drive_v1 = import_optional_dependency("lark_oapi.api.drive.v1")

    client = (
        lark.Client.builder().app_id(lark_app_id).app_secret(lark_app_secret).log_level(lark.LogLevel.ERROR).build()
    )
    token = lark_path.split("/")[-1].split("?")[0]
    sheet_id = None
    if "?" in lark_path:
        sheet_id = lark_path.split("?")[1].split("=")[1]
    if not sheet_id:
        raise ValueError("Lark sink requires a sheet id in lark_path query string")

    with open(local_path, "rb") as rf:
        upload_request = (
            drive_v1.UploadAllMediaRequest.builder()
            .request_body(
                drive_v1.UploadAllMediaRequestBody.builder()
                .file_name(os.path.basename(local_path))
                .parent_type("sheet_file")
                .size(str(os.path.getsize(local_path)))
                .parent_node(token)
                .file(rf)
                .build()
            )
            .build()
        )
        upload_response = client.drive.v1.media.upload_all(upload_request)
    if not upload_response.success():
        raise RuntimeError(f"Lark file upload failed: code={upload_response.code}, msg={upload_response.msg}")

    request = (
        lark.BaseRequest.builder()
        .http_method(lark.HttpMethod.POST)
        .uri(f"/open-apis/sheets/v3/spreadsheets/{token}/sheets/{sheet_id}/values/batch_update?user_id_type=open_id")
        .token_types({lark.AccessTokenType.TENANT})
        .body(
            {
                "value_ranges": [
                    {
                        "range": f"{sheet_id}!{cell_range}",
                        "values": [
                            [
                                [
                                    {
                                        "type": "file",
                                        "file": {"file_token": upload_response.data.file_token},
                                    }
                                ]
                            ]
                        ],
                    }
                ]
            }
        )
        .build()
    )
    response = client.request(request)
    if not response.success():
        raise RuntimeError(f"Lark sheet update failed: code={response.code}, msg={response.msg}")


_A1_CELL_PATTERN = re.compile(r"^([A-Za-z]+)([1-9][0-9]*)$")


def _lark_column_to_index(column_name: str) -> int:
    column_index = 0
    for char in column_name.upper():
        column_index = column_index * 26 + ord(char) - ord("A") + 1
    return column_index


def _lark_index_to_column(column_index: int) -> str:
    column_name = ""
    while column_index:
        column_index, remainder = divmod(column_index - 1, 26)
        column_name = chr(ord("A") + remainder) + column_name
    return column_name


def _expand_lark_single_cell_range(cell_range: str, values: list[list[Any]] | None) -> str:
    match = _A1_CELL_PATTERN.match(cell_range)
    if match is None:
        return f"{cell_range}:{cell_range}"
    start_column, start_row = match.groups()
    row_count = max(len(values or []), 1)
    column_count = max((len(row) for row in values or []), default=1)
    end_column = _lark_index_to_column(_lark_column_to_index(start_column) + column_count - 1)
    end_row = int(start_row) + row_count - 1
    return f"{cell_range}:{end_column}{end_row}"


def _format_lark_sheet_range(
    sheet_id: str,
    cell_range: str | None,
    values: list[list[Any]] | None = None,
) -> str:
    if cell_range is None:
        return sheet_id
    if not isinstance(cell_range, str):
        raise ValueError("Lark sheet append `range` must be a string when configured.")
    cell_range = cell_range.strip()
    if not cell_range:
        return sheet_id
    if "!" in cell_range:
        sheet_prefix, cell_range = cell_range.split("!", 1)
    else:
        sheet_prefix = sheet_id
    if cell_range == sheet_id:
        return cell_range
    if ":" not in cell_range:
        cell_range = _expand_lark_single_cell_range(cell_range, values)
    return f"{sheet_prefix}!{cell_range}"


def append_values_to_lark_sheet(
    values: list[list[Any]],
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
    cell_range: str | None = None,
    sheet_id: str | None = None,
) -> None:
    if not values:
        logger.warning("Skip Lark sheet append because staged CSV contains no data rows.")
        return

    lark = import_optional_dependency("lark_oapi")
    token, sheet_id = parse_lark_sheet_location(lark_path, sheet_id=sheet_id)
    target_range = _format_lark_sheet_range(sheet_id, cell_range, values=values)

    client = (
        lark.Client.builder().app_id(lark_app_id).app_secret(lark_app_secret).log_level(lark.LogLevel.ERROR).build()
    )
    request = (
        lark.BaseRequest.builder()
        .http_method(lark.HttpMethod.POST)
        .uri(f"/open-apis/sheets/v2/spreadsheets/{token}/values_append")
        .token_types({lark.AccessTokenType.TENANT})
        .body({"valueRange": {"range": target_range, "values": values}})
        .build()
    )
    response = client.request(request)
    if not response.success():
        raise RuntimeError(f"Lark sheet append failed: code={response.code}, msg={response.msg}")


def append_csv_to_lark_sheet(
    local_path: str,
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
    cell_range: str | None = None,
    skip_header: bool = True,
    sheet_id: str | None = None,
) -> None:
    with open(local_path, encoding="utf-8-sig", newline="") as rf:
        rows = list(csv.reader(rf))
    if skip_header and rows:
        rows = rows[1:]
    append_values_to_lark_sheet(
        values=rows,
        lark_path=lark_path,
        lark_app_id=lark_app_id,
        lark_app_secret=lark_app_secret,
        cell_range=cell_range,
        sheet_id=sheet_id,
    )


def _get_lark_sheet_metainfo(
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
) -> dict[str, Any]:
    lark = import_optional_dependency("lark_oapi")
    token = lark_path.rstrip("/").split("/")[-1].split("?")[0]
    client = (
        lark.Client.builder().app_id(lark_app_id).app_secret(lark_app_secret).log_level(lark.LogLevel.ERROR).build()
    )
    request = (
        lark.BaseRequest.builder()
        .http_method(lark.HttpMethod.GET)
        .uri(f"/open-apis/sheets/v2/spreadsheets/{token}/metainfo")
        .token_types({lark.AccessTokenType.TENANT})
        .build()
    )
    response = client.request(request)
    if not response.success():
        raise RuntimeError(f"Lark spreadsheet metainfo failed: code={response.code}, msg={response.msg}")
    content = getattr(getattr(response, "raw", None), "content", None)
    if content is None:
        raise RuntimeError("Lark spreadsheet metainfo failed: empty response content")
    return json.loads(content.decode("utf-8") if isinstance(content, bytes) else content)


def _first_lark_sheet_id(
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
) -> str:
    payload = _get_lark_sheet_metainfo(lark_path, lark_app_id, lark_app_secret)
    sheets = payload.get("data", {}).get("sheets", [])
    if not sheets or not sheets[0].get("sheetId"):
        raise RuntimeError("Lark spreadsheet metainfo returned no sheets")
    return sheets[0]["sheetId"]


def _lark_sheet_row_count(
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
    sheet_id: str,
) -> int:
    payload = _get_lark_sheet_metainfo(lark_path, lark_app_id, lark_app_secret)
    for sheet in payload.get("data", {}).get("sheets", []):
        if sheet.get("sheetId") == sheet_id:
            return int(sheet.get("rowCount") or 0)
    raise RuntimeError(f"Lark spreadsheet metainfo returned no sheet `{sheet_id}`")


def create_lark_spreadsheet(
    lark_app_id: str,
    lark_app_secret: str,
    title: str,
) -> str:
    lark = import_optional_dependency("lark_oapi")
    client = (
        lark.Client.builder().app_id(lark_app_id).app_secret(lark_app_secret).log_level(lark.LogLevel.ERROR).build()
    )
    request = (
        lark.BaseRequest.builder()
        .http_method(lark.HttpMethod.POST)
        .uri("/open-apis/sheets/v3/spreadsheets")
        .token_types({lark.AccessTokenType.TENANT})
        .body({"title": title})
        .build()
    )
    response = client.request(request)
    if not response.success():
        raise RuntimeError(f"Lark spreadsheet creation failed: code={response.code}, msg={response.msg}")
    content = getattr(getattr(response, "raw", None), "content", None)
    if content is None:
        raise RuntimeError("Lark spreadsheet creation failed: empty response content")
    payload = json.loads(content.decode("utf-8") if isinstance(content, bytes) else content)
    spreadsheet = payload.get("data", {}).get("spreadsheet", {})
    token = spreadsheet.get("spreadsheet_token") or spreadsheet.get("token")
    if not token:
        raise RuntimeError("Lark spreadsheet creation returned no spreadsheet token")
    sheet_id = _first_lark_sheet_id(token, lark_app_id, lark_app_secret)
    return f"https://bytedance.larkoffice.com/sheets/{token}?sheet={sheet_id}"


def _format_lark_overwrite_range(
    sheet_id: str,
    cell_range: str | None,
    values: list[list[Any]],
) -> str:
    if cell_range is None:
        cell_range = "A1"
    if not isinstance(cell_range, str):
        raise ValueError("Lark overwrite `range` must be a string when configured.")
    cell_range = cell_range.strip() or "A1"
    if "!" in cell_range:
        sheet_prefix, cell_range = cell_range.split("!", 1)
    else:
        sheet_prefix = sheet_id
    if ":" not in cell_range:
        cell_range = _expand_lark_single_cell_range(cell_range, values)
    return f"{sheet_prefix}!{cell_range}"


def delete_lark_sheet_rows_after(
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
    keep_rows: int,
    sheet_id: str | None = None,
) -> None:
    if keep_rows < 1:
        return
    lark = import_optional_dependency("lark_oapi")
    token, sheet_id = parse_lark_sheet_location(lark_path, sheet_id=sheet_id)
    row_count = _lark_sheet_row_count(lark_path, lark_app_id, lark_app_secret, sheet_id)
    if row_count <= keep_rows:
        return

    client = (
        lark.Client.builder().app_id(lark_app_id).app_secret(lark_app_secret).log_level(lark.LogLevel.ERROR).build()
    )
    request = (
        lark.BaseRequest.builder()
        .http_method(lark.HttpMethod.DELETE)
        .uri(f"/open-apis/sheets/v2/spreadsheets/{token}/dimension_range")
        .token_types({lark.AccessTokenType.TENANT})
        .body(
            {
                "dimension": {
                    "sheetId": sheet_id,
                    "majorDimension": "ROWS",
                    "startIndex": keep_rows + 1,
                    "endIndex": row_count,
                }
            }
        )
        .build()
    )
    response = client.request(request)
    if not response.success():
        raise RuntimeError(f"Lark sheet row cleanup failed: code={response.code}, msg={response.msg}")


def overwrite_values_to_lark_sheet(
    values: list[list[Any]],
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
    cell_range: str | None = None,
    sheet_id: str | None = None,
) -> None:
    if not values:
        logger.warning("Skip Lark sheet overwrite because staged CSV contains no data rows.")
        return

    lark = import_optional_dependency("lark_oapi")
    token, sheet_id = parse_lark_sheet_location(lark_path, sheet_id=sheet_id)
    target_range = _format_lark_overwrite_range(sheet_id, cell_range, values)

    client = (
        lark.Client.builder().app_id(lark_app_id).app_secret(lark_app_secret).log_level(lark.LogLevel.ERROR).build()
    )
    request = (
        lark.BaseRequest.builder()
        .http_method(lark.HttpMethod.PUT)
        .uri(f"/open-apis/sheets/v2/spreadsheets/{token}/values")
        .token_types({lark.AccessTokenType.TENANT})
        .body({"valueRange": {"range": target_range, "values": values}})
        .build()
    )
    response = client.request(request)
    if not response.success():
        raise RuntimeError(f"Lark sheet overwrite failed: code={response.code}, msg={response.msg}")


def overwrite_csv_to_lark_sheet(
    local_path: str,
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
    cell_range: str | None = None,
    skip_header: bool = False,
    clear_sheet: bool = True,
    sheet_id: str | None = None,
) -> None:
    with open(local_path, encoding="utf-8-sig", newline="") as rf:
        rows = list(csv.reader(rf))
    if skip_header and rows:
        rows = rows[1:]
    if clear_sheet and cell_range is None:
        delete_lark_sheet_rows_after(
            lark_path=lark_path,
            lark_app_id=lark_app_id,
            lark_app_secret=lark_app_secret,
            keep_rows=len(rows),
            sheet_id=sheet_id,
        )
    overwrite_values_to_lark_sheet(
        values=rows,
        lark_path=lark_path,
        lark_app_id=lark_app_id,
        lark_app_secret=lark_app_secret,
        cell_range=cell_range,
        sheet_id=sheet_id,
    )


def build_tqs_export_sql(
    query: str,
    output_uri: str,
    *,
    cluster: str = "",
    queue_name: str = "",
    priority: int = 5,
    memory: int = 0,
) -> str:
    set_lines = [f"set spark.yarn.priority={priority};"]
    if cluster:
        set_lines.append(f"set yarn.cluster.name={cluster};")
    if queue_name:
        set_lines.append(f"set mapreduce.job.queuename={queue_name};")
    if memory > 0:
        set_lines.append(f"set spark.executor.memory={memory}g;")
    return (
        "\n".join(set_lines)
        + f"\nINSERT OVERWRITE DIRECTORY '{output_uri}'\nSTORED AS PARQUET\n{query.strip()}\n"
    )


def execute_tqs_sql(
    sql: str,
    *,
    tqs_app_id: str,
    tqs_app_key: str,
    user_name: str,
) -> None:
    bytedtqs = import_optional_dependency("bytedtqs", extra_name="internal_io")
    client = bytedtqs.TQSClient(app_id=tqs_app_id, app_key=tqs_app_key, cluster=bytedtqs.Cluster.CN)
    analysis_result = client.analyze_query(user_name, sql)
    if analysis_result.is_failed():
        raise RuntimeError(analysis_result.error_message)

    job = client.execute_query(user_name, sql)
    if not job.is_success():
        raise RuntimeError(
            "TQS query execution failed:\n"
            f"query_error_url={job.query_error_url}\n"
            f"query_log_url={job.query_log_url}\n"
            f"tracking_urls={job.tracking_urls}"
        )


def _tqs_job_error(job) -> str:
    return (
        "TQS query execution failed:\n"
        f"query_error_url={getattr(job, 'query_error_url', '')}\n"
        f"query_log_url={getattr(job, 'query_log_url', '')}\n"
        f"tracking_urls={getattr(job, 'tracking_urls', '')}"
    )


def _rows_to_records(rows, columns=None) -> list[dict]:
    records = []
    for row in rows:
        if isinstance(row, dict):
            records.append(row)
        elif hasattr(row, "asDict"):
            records.append(row.asDict())
        elif columns:
            records.append(dict(zip(columns, row)))
        else:
            raise TypeError("TQS client_result rows must be dict-like or accompanied by column names")
    return records


def _ensure_csv_field_size_limit() -> None:
    target_limit = sys.maxsize
    current_limit = csv.field_size_limit()
    if current_limit >= target_limit:
        return

    while target_limit > current_limit:
        try:
            csv.field_size_limit(target_limit)
            return
        except OverflowError:
            target_limit //= 10


def _extract_tqs_result_records(job) -> list[dict]:
    _ensure_csv_field_size_limit()

    if hasattr(job, "get_typed_result"):
        try:
            rows = job.get_typed_result(return_header=False)
            columns = [item[0] for item in getattr(job, "result_schema", None) or []]
            if columns:
                return _rows_to_records(rows, columns)
        except NotImplementedError:
            pass

    result = None
    for attr in ("to_pandas", "fetch_pandas_all"):
        if hasattr(job, attr):
            result = getattr(job, attr)()
            break

    if result is None:
        for attr in ("get_result", "fetch_all", "fetchall", "result"):
            if hasattr(job, attr):
                candidate = getattr(job, attr)
                result = candidate() if callable(candidate) else candidate
                break

    if result is None and hasattr(job, "results"):
        result = job.results

    if result is None:
        raise RuntimeError(
            "TQS client_result mode requires the bytedtqs job object to expose one of "
            "`get_typed_result`, `to_pandas`, `fetch_pandas_all`, `get_result`, `fetch_all`, "
            "`fetchall`, `result`, or `results`."
        )

    if hasattr(result, "to_pandas"):
        result = result.to_pandas()

    if hasattr(result, "to_dict"):
        try:
            return result.to_dict(orient="records")
        except TypeError:
            pass

    if isinstance(result, dict):
        rows = result.get("rows") or result.get("data") or result.get("result")
        columns = result.get("columns") or result.get("schema")
        if rows is None:
            return [result]
        return _rows_to_records(rows, columns)

    if hasattr(result, "fetch_all_data"):
        rows = result.fetch_all_data()
        if getattr(result, "with_header", False) and rows:
            columns = rows[0]
            rows = rows[1:]
        else:
            columns = [item[0] for item in getattr(job, "result_schema", None) or []]
        return _rows_to_records(rows, columns)

    if hasattr(result, "sample_data"):
        rows = result.sample_data
        columns = [item[0] for item in getattr(job, "result_schema", None) or []]
        return _rows_to_records(rows, columns)

    columns = getattr(result, "columns", None) or getattr(job, "columns", None)
    if columns is not None and not isinstance(columns, list):
        columns = list(columns)
    return _rows_to_records(result, columns)


def _strip_sql_trailing_semicolon(query: str) -> str:
    stripped = query.strip()
    while stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()
    return stripped


def build_tqs_client_result_limited_query(query: str, max_result_rows: int) -> str:
    if max_result_rows <= 0:
        raise ValueError("`max_result_rows` must be positive for TQS client_result mode")
    return (
        "SELECT *\n"
        "FROM (\n"
        f"{_strip_sql_trailing_semicolon(query)}\n"
        ") __dj_tqs_client_result_limit\n"
        f"LIMIT {max_result_rows}"
    )


def run_tqs_query_to_records(
    query: str,
    *,
    tqs_app_id: str,
    tqs_app_key: str,
    user_name: str,
    tqs_cluster: str = "cn",
    tqs_enable_domain: bool | None = None,
    tqs_timeout: int = 120,
    max_result_rows: int = 10000,
) -> list[dict]:
    if max_result_rows <= 0:
        raise ValueError("`max_result_rows` must be positive for TQS client_result mode")

    limited_query = build_tqs_client_result_limited_query(query, max_result_rows)
    bytedtqs = import_optional_dependency("bytedtqs", extra_name="internal_io")
    client_kwargs = {"timeout": tqs_timeout}
    if tqs_enable_domain is not None:
        client_kwargs["enable_domain"] = tqs_enable_domain
    client = bytedtqs.TQSClient(app_id=tqs_app_id, app_key=tqs_app_key, cluster=tqs_cluster, **client_kwargs)
    analysis_result = client.analyze_query(user_name, limited_query)
    if analysis_result.is_failed():
        raise RuntimeError(analysis_result.error_message)

    job = client.execute_query(user_name, limited_query)
    if not job.is_success():
        raise RuntimeError(_tqs_job_error(job))

    records = _extract_tqs_result_records(job)
    if len(records) > max_result_rows:
        raise RuntimeError(
            f"TQS client_result returned {len(records)} rows, exceeding max_result_rows={max_result_rows}. "
            "Use materialized mode for larger results."
        )
    logger.info(f"TQS query loaded {len(records)} records through client_result mode")
    return records


def run_tqs_query(
    query: str,
    output_uri: str,
    *,
    tqs_app_id: str,
    tqs_app_key: str,
    user_name: str,
    cluster: str = "",
    queue_name: str = "",
    priority: int = 5,
    memory: int = 0,
) -> str:
    export_sql = build_tqs_export_sql(
        query,
        output_uri,
        cluster=cluster,
        queue_name=queue_name,
        priority=priority,
        memory=memory,
    )
    execute_tqs_sql(export_sql, tqs_app_id=tqs_app_id, tqs_app_key=tqs_app_key, user_name=user_name)
    logger.info(f"TQS query materialized data to [{output_uri}]")
    return output_uri


def materialize_duckdb_query(
    load_sql: str,
    output_path: str,
    *,
    path_mapping: Dict[str, str] | None = None,
) -> str:
    duckdb = import_optional_dependency("duckdb", extra_name="internal_io")

    def replace_placeholders(template: str, mapping: Dict[str, Any]) -> str:
        result = template
        for key, value in (mapping or {}).items():
            token = "${" + key + "}"
            if isinstance(value, list):
                value = json.dumps(value)
            result = result.replace(token, str(value))
        return result

    sql = replace_placeholders(load_sql, path_mapping or {})
    ensure_parent(output_path)
    conn = duckdb.connect()
    try:
        conn.execute(f"COPY ({sql}) TO '{output_path}' (FORMAT PARQUET)")
    finally:
        conn.close()
    return output_path


def upload_file_to_tos(
    local_path: str,
    *,
    bucket_name: str,
    object_key: str,
    endpoint: str = "https://tos-cn-beijing.volces.com",
    region: str = "cn-beijing",
    access_key: str | None = None,
    secret_key: str | None = None,
    session_token: str | None = None,
) -> None:
    tos = import_optional_dependency("tos", extra_name="internal_io")
    client = tos.TosClientV2(
        ak=access_key,
        sk=secret_key,
        endpoint=endpoint,
        region=region,
        security_token=session_token,
    )
    client.upload_file(bucket_name, object_key, local_path)


def read_magnus_to_pandas(table_name: str, **kwargs):
    magnus_module = import_optional_dependency("pyiceberg.magnus", extra_name="internal_io")
    magnus_reader_module = import_optional_dependency("pyiceberg.magnus.magnus_reader", extra_name="internal_io")

    partition_filter = kwargs.get("filter", None)
    magnus_conf = kwargs.get("magnus_conf", {})
    magnus_client = magnus_module.MagnusClient()
    table = magnus_client.load_table(table_name)
    magnus_reader = magnus_reader_module.MagnusReader(table, partition_filter, **magnus_conf)
    return magnus_reader.to_pandas()


def read_magnus_to_ray(table_name: str, **kwargs):
    pyiceberg_ray = import_optional_dependency("pyiceberg.ray", extra_name="internal_io")
    magnus_conf = kwargs.get("magnus_conf", {})
    partition_filter = kwargs.get("filter", None)
    return pyiceberg_ray.read_magnus(identifier=table_name, filter=partition_filter, **magnus_conf)


def _parse_arrow_data_type(type_config):
    import pyarrow as pa

    if isinstance(type_config, pa.DataType):
        return type_config

    if not isinstance(type_config, str):
        raise TypeError(f"Magnus schema field type must be a string or pyarrow type, got {type(type_config)!r}")

    type_spec = type_config.strip()
    type_name = type_spec.lower()
    if type_name.startswith("list<") and type_name.endswith(">"):
        return pa.list_(_parse_arrow_data_type(type_spec[5:-1]))
    if type_name.startswith("large_list<") and type_name.endswith(">"):
        return pa.large_list(_parse_arrow_data_type(type_spec[11:-1]))
    if type_name.startswith("struct<") and type_name.endswith(">"):
        fields = []
        for field_spec in _split_arrow_type_fields(type_spec[7:-1]):
            if ":" not in field_spec:
                raise ValueError(f"Unsupported struct field type: {field_spec!r}")
            field_name, field_type = field_spec.split(":", 1)
            field_name = field_name.strip()
            if not field_name:
                raise ValueError(f"Unsupported struct field type: {field_spec!r}")
            fields.append(pa.field(field_name, _parse_arrow_data_type(field_type)))
        return pa.struct(fields)

    type_aliases = {
        "string": pa.string(),
        "str": pa.string(),
        "bool": pa.bool_(),
        "boolean": pa.bool_(),
        "int": pa.int32(),
        "int32": pa.int32(),
        "integer": pa.int32(),
        "bigint": pa.int64(),
        "long": pa.int64(),
        "int64": pa.int64(),
        "float": pa.float32(),
        "float32": pa.float32(),
        "double": pa.float64(),
        "float64": pa.float64(),
        "binary": pa.binary(),
        "bytes": pa.binary(),
        "null": pa.null(),
    }
    if type_name not in type_aliases:
        raise ValueError(f"Unsupported Magnus schema field type: {type_config!r}")
    return type_aliases[type_name]


def _split_arrow_type_fields(fields_config: str) -> list[str]:
    fields = []
    start = 0
    depth = 0
    for index, char in enumerate(fields_config):
        if char == "<":
            depth += 1
        elif char == ">":
            depth -= 1
            if depth < 0:
                raise ValueError(f"Malformed Arrow type: {fields_config!r}")
        elif char == "," and depth == 0:
            fields.append(fields_config[start:index].strip())
            start = index + 1
    if depth != 0:
        raise ValueError(f"Malformed Arrow type: {fields_config!r}")
    tail = fields_config[start:].strip()
    if tail:
        fields.append(tail)
    return fields


def build_arrow_schema_from_config(schema_config):
    import pyarrow as pa

    schema_config = namespace_to_plain_dict(schema_config)
    if schema_config is None:
        return None
    if isinstance(schema_config, pa.Schema):
        return schema_config

    fields_config = schema_config.get("fields") if isinstance(schema_config, dict) else schema_config
    if not isinstance(fields_config, list):
        raise TypeError("Magnus export schema must be a list of fields or a dict with a `fields` list")

    fields = []
    for field_config in fields_config:
        field_config = namespace_to_plain_dict(field_config)
        if not isinstance(field_config, dict):
            raise TypeError("Each Magnus schema field must be a mapping with `name` and `type`")
        if "name" not in field_config or "type" not in field_config:
            raise ValueError("Each Magnus schema field must contain `name` and `type`")
        fields.append(
            pa.field(
                field_config["name"],
                _parse_arrow_data_type(field_config["type"]),
                nullable=field_config.get("nullable", True),
            )
        )
    return pa.schema(fields)


def _as_arrow_schema(schema, *, source):
    import pyarrow as pa

    base_schema = getattr(schema, "base_schema", schema)
    if isinstance(base_schema, pa.Schema):
        return base_schema
    raise ValueError(f"Magnus `infer_schema_on_create` could not infer a PyArrow schema from {source}")


def _infer_magnus_schema_from_arrow_batches(dataset):
    import pyarrow as pa

    schemas = []
    try:
        batches = dataset.iter_batches(batch_format="pyarrow", batch_size=8192)
        for batch in batches:
            schemas.append(_as_arrow_schema(getattr(batch, "schema", None), source="Ray Dataset pyarrow batch"))
    except Exception as exc:
        raise ValueError(
            "Magnus `infer_schema_on_create` could not infer a PyArrow schema from Ray Dataset pyarrow batches"
        ) from exc
    if not schemas:
        raise ValueError("Magnus `infer_schema_on_create` could not infer a PyArrow schema from empty Ray Dataset")
    try:
        return pa.unify_schemas(schemas)
    except Exception as exc:
        raise ValueError("Magnus `infer_schema_on_create` found incompatible Ray Dataset pyarrow batch schemas") from exc


def _infer_magnus_schema_and_dataset_from_ray_dataset(dataset):
    try:
        schema = dataset.schema(fetch_if_missing=True)
    except TypeError:
        schema = dataset.schema()
    except Exception as exc:
        raise ValueError("Magnus `infer_schema_on_create` failed to fetch Ray Dataset schema") from exc
    try:
        return _as_arrow_schema(schema, source="Ray Dataset"), dataset
    except ValueError:
        inferred_dataset = dataset
        if hasattr(dataset, "materialize"):
            try:
                inferred_dataset = dataset.materialize()
            except Exception as exc:
                raise ValueError(
                    "Magnus `infer_schema_on_create` could not materialize Ray Dataset for pyarrow batch schema inference"
                ) from exc
            try:
                schema = inferred_dataset.schema(fetch_if_missing=True)
            except TypeError:
                schema = inferred_dataset.schema()
            except Exception:
                schema = None
            try:
                return _as_arrow_schema(schema, source="materialized Ray Dataset"), inferred_dataset
            except ValueError:
                pass
        return _infer_magnus_schema_from_arrow_batches(inferred_dataset), inferred_dataset


def _infer_magnus_schema_from_ray_dataset(dataset):
    schema, _ = _infer_magnus_schema_and_dataset_from_ray_dataset(dataset)
    return schema


def _infer_magnus_schema_from_hf_dataset(dataset):
    features = getattr(dataset, "features", None)
    return _as_arrow_schema(getattr(features, "arrow_schema", None), source="HuggingFace Dataset features")


def _magnus_table_metadata_properties(table) -> dict[str, Any] | None:
    metadata = getattr(table, "metadata", None)
    properties = getattr(metadata, "properties", None)
    if isinstance(properties, dict):
        return properties
    return None


def _validate_existing_magnus_table(table, schema, partition_columns, table_properties=None):
    expected_columns = set(schema.names) if schema is not None else set()
    if expected_columns:
        table_schema = table.schema()
        table_columns = {field.name for field in getattr(table_schema, "fields", [])}
        if table_columns:
            missing_columns = expected_columns - table_columns
            if missing_columns:
                raise ValueError(f"Existing Magnus table is missing schema columns: {sorted(missing_columns)}")
        else:
            logger.warning("Could not inspect existing Magnus table schema fields; skipping schema validation")

    if partition_columns:
        table_spec = table.spec()
        partition_names = {
            name
            for field in getattr(table_spec, "fields", [])
            for name in (getattr(field, "name", None), getattr(field, "field_name", None))
            if name
        }
        if partition_names:
            missing_partitions = set(partition_columns) - partition_names
            if missing_partitions:
                raise ValueError(f"Existing Magnus table is missing partition columns: {sorted(missing_partitions)}")
        else:
            logger.warning("Could not inspect existing Magnus table partition fields; skipping partition validation")

    table_properties = dict(namespace_to_plain_dict(table_properties or {}))
    if table_properties:
        existing_properties = _magnus_table_metadata_properties(table)
        if existing_properties is None:
            logger.warning("Could not inspect existing Magnus table properties; skipping property validation")
            return
        mismatched_properties = {
            key: (existing_properties.get(key), value)
            for key, value in table_properties.items()
            if existing_properties.get(key) != value
        }
        if mismatched_properties:
            details = ", ".join(
                f"{key}: existing={existing!r}, expected={expected!r}"
                for key, (existing, expected) in sorted(mismatched_properties.items())
            )
            raise ValueError(f"Existing Magnus table properties do not match export config: {details}")


def create_magnus_table_if_not_exists(
    table_name: str,
    schema,
    partition_columns=None,
    table_properties=None,
    schema_provider=None,
):
    magnus_module = import_optional_dependency("pyiceberg.magnus", extra_name="internal_io")

    catalog, database, short_table_name = table_name.split(".")
    magnus_client = magnus_module.MagnusClient()
    table_properties = dict(namespace_to_plain_dict(table_properties or {}))
    if magnus_client.exist_table(catalog, database, short_table_name):
        if schema is not None or partition_columns or table_properties:
            _validate_existing_magnus_table(
                magnus_client.load_table(table_name),
                schema,
                partition_columns,
                table_properties=table_properties,
        )
        return magnus_client

    if schema is None and schema_provider is not None:
        schema = schema_provider()

    _validate_magnus_create_table_config(schema, partition_columns)
    try:
        magnus_client.create_table(
            catalog,
            database,
            short_table_name,
            schema,
            properties=table_properties,
            partition_columns=partition_columns,
            load_table=False,
        )
    except TypeError:
        magnus_client.create_table(
            catalog,
            database,
            short_table_name,
            schema,
            properties=table_properties,
            partition_columns=partition_columns,
        )
    return magnus_client


def load_magnus_table(table_name: str):
    magnus_module = import_optional_dependency("pyiceberg.magnus", extra_name="internal_io")
    return magnus_module.MagnusClient().load_table(table_name)


def _validate_magnus_create_table_config(schema, partition_columns):
    if schema is None:
        raise ValueError("Magnus `create_table_if_not_exists` requires explicit `export.schema`")
    if not partition_columns:
        return
    missing_partitions = set(partition_columns) - set(schema.names)
    if missing_partitions:
        raise ValueError(
            "Magnus `create_table_if_not_exists` requires partition columns in `export.schema`: "
            f"{sorted(missing_partitions)}"
        )


def write_hf_dataset_to_magnus(dataset, table_name: str, **kwargs):
    magnus_writer_module = import_optional_dependency("pyiceberg.magnus.magnus_writer", extra_name="internal_io")

    partition_columns = kwargs.get("partition_columns", None)
    magnus_conf = _normalize_magnus_conf(kwargs.get("magnus_conf", {}))
    batch_size = kwargs.get("batch_size", 2000)
    schema_config = kwargs.get("schema")
    create_table_if_not_exists = bool(kwargs.get("create_table_if_not_exists", False))
    infer_schema_on_create = bool(kwargs.get("infer_schema_on_create", False))
    failure_policy = _normalize_magnus_failure_policy(kwargs.get("magnus_failure_policy"))
    if failure_policy != MAGNUS_FAILURE_POLICY_ABORT:
        raise ValueError("Magnus `commit_completed_unsafe` failure policy is only supported for Ray Dataset exports")
    explicit_schema = build_arrow_schema_from_config(schema_config)

    if create_table_if_not_exists:
        create_table_kwargs = {}
        if infer_schema_on_create and explicit_schema is None:
            create_table_kwargs["schema_provider"] = lambda: _infer_magnus_schema_from_hf_dataset(dataset)
        magnus_client = create_magnus_table_if_not_exists(
            table_name,
            explicit_schema,
            partition_columns=partition_columns,
            table_properties=_magnus_table_properties_from_write_options(magnus_conf),
            **create_table_kwargs,
        )
        table = magnus_client.load_table(table_name)
    else:
        table = load_magnus_table(table_name)
    writer = magnus_writer_module.MagnusMultiFileWriter(table, **magnus_conf)

    df = dataset.to_pandas()
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i : i + batch_size]
        writer.write(batch_df.to_dict("records"))
    writer.finish()
    writer.commit()


def _grouped_data_map_groups_supports_localsort(grouped_data_cls) -> bool:
    map_groups = getattr(grouped_data_cls, "map_groups", None)
    if map_groups is None:
        return False

    try:
        parameters = inspect.signature(map_groups).parameters
    except (TypeError, ValueError):
        return False

    return "localsort" in parameters


def _patch_magnus_transformer_localsort_compat(transformers_module):
    if getattr(transformers_module, "_dj_localsort_compat_patched", False):
        return

    sort_transformer_cls = getattr(transformers_module, "SortBySortOrderTransformer", None)
    grouped_data_cls = getattr(transformers_module, "GroupedData", None)
    dataset_cls = getattr(transformers_module, "Dataset", None)
    get_bool_config = getattr(transformers_module, "get_bool_config", None)
    get_sort_asc_columns = getattr(transformers_module, "get_sort_asc_columns", None)
    ray_write_options = getattr(transformers_module, "RayWriteOptions", None)
    disable_sort_key = getattr(ray_write_options, "DISABLE_SORT", None)

    if sort_transformer_cls is None or grouped_data_cls is None or dataset_cls is None:
        return
    if get_bool_config is None or get_sort_asc_columns is None or disable_sort_key is None:
        return
    if _grouped_data_map_groups_supports_localsort(grouped_data_cls):
        transformers_module._dj_localsort_compat_patched = True
        return

    def transform_with_localsort_compat(self, ray_ds):
        disable_sort = get_bool_config(self.config, disable_sort_key, False)
        if disable_sort:
            return ray_ds

        sort_columns = get_sort_asc_columns(self.table)
        if sort_columns is None or len(sort_columns) == 0:
            return ray_ds

        if isinstance(ray_ds, grouped_data_cls):
            sort_keys = [(column, "ascending") if isinstance(column, str) else column for column in sort_columns]

            def sort_group(group):
                return group.sort_by(sort_keys)

            return ray_ds.map_groups(sort_group, batch_format="pyarrow")
        if isinstance(ray_ds, dataset_cls):
            return ray_ds.sort(sort_columns)
        raise Exception("Unexpected type for ray dataset")

    sort_transformer_cls.transform = transform_with_localsort_compat
    transformers_module._dj_localsort_compat_patched = True


def _patch_magnus_datasink_write_result_compat(magnus_datasink_module):
    if getattr(magnus_datasink_module, "_dj_write_result_compat_patched", False):
        return

    # Ray 2.54 passes a WriteResult object to Datasink callbacks, while older
    # Magnus sinks expect the raw list of writer returns.
    def unwrap_write_result(write_results):
        return getattr(write_results, "write_returns", write_results)

    magnus_data_sink = getattr(magnus_datasink_module, "MagnusDataSink", None)
    if magnus_data_sink is not None:
        original_commit = magnus_data_sink.commit

        def commit_with_write_result_compat(self, write_results):
            return original_commit(self, unwrap_write_result(write_results))

        magnus_data_sink.commit = commit_with_write_result_compat

    magnus_commit_data_sink = getattr(magnus_datasink_module, "MagnusCommitDataSink", None)
    if magnus_commit_data_sink is not None:
        original_on_write_complete = magnus_commit_data_sink.on_write_complete

        def on_write_complete_with_write_result_compat(self, write_results):
            return original_on_write_complete(self, unwrap_write_result(write_results))

        magnus_commit_data_sink.on_write_complete = on_write_complete_with_write_result_compat

    magnus_datasink_module._dj_write_result_compat_patched = True


def _patch_magnus_parquet_appender_hdfs_uri_compat(file_appender_module):
    if getattr(file_appender_module, "_dj_parquet_appender_hdfs_uri_compat_patched", False):
        return

    parquet_appender = getattr(file_appender_module, "ParquetAppender", None)
    if parquet_appender is None:
        return

    original_append = parquet_appender.append
    pq_module = getattr(file_appender_module, "pq", None)
    if pq_module is None or not hasattr(pq_module, "ParquetWriter"):
        return

    def append_with_hdfs_uri_compat(self, data_batch):
        parsed = urlparse(getattr(self, "_file_path", ""))
        if parsed.scheme not in {"hdfs", "viewfs"}:
            return original_append(self, data_batch)
        if data_batch.num_rows == 0:
            return

        import pyarrow as pa

        file_system = self._get_file_system()
        if self._writer is None:
            # HadoopFileSystem expects an internal path when a filesystem object
            # is passed to ParquetWriter. Keeping the full URI here raises
            # OSError: [Errno 22] Invalid argument on some HDFS deployments.
            writer_path = parsed.path or self._file_path
            self._writer = pq_module.ParquetWriter(
                writer_path,
                self._arrow_schema,
                filesystem=file_system,
                use_dictionary=self._use_dictionary,
                compression=self._compression,
                write_batch_size=self._write_batch_size,
                write_page_index=True,
                **self._writer_config,
            )

        if isinstance(data_batch, pa.Table):
            new_table = self._prune_and_arrange_cols(data_batch)
            total_bytes = new_table.nbytes
            bytes_per_row = total_bytes / new_table.num_rows
            rows_per_row_group = max(1, int(self._row_group_size_bytes / bytes_per_row))
            self._writer.write_table(new_table, row_group_size=rows_per_row_group)
            self._row_count += data_batch.num_rows
            return
        raise Exception("Unsupported type")

    parquet_appender.append = append_with_hdfs_uri_compat
    file_appender_module._dj_parquet_appender_hdfs_uri_compat_patched = True


def _ensure_magnus_parquet_appender_hdfs_uri_compat_in_worker():
    file_appender_module = import_optional_dependency("pyiceberg.magnus.file_appender", extra_name="internal_io")
    _patch_magnus_parquet_appender_hdfs_uri_compat(file_appender_module)


def _patch_magnus_datasink_worker_file_appender_compat(magnus_datasink_module):
    if getattr(magnus_datasink_module, "_dj_worker_file_appender_compat_patched", False):
        return

    magnus_data_sink = getattr(magnus_datasink_module, "MagnusDataSink", None)
    if magnus_data_sink is not None and hasattr(magnus_data_sink, "_do_write"):
        original_do_write = magnus_data_sink._do_write

        def do_write_with_file_appender_compat(self, blocks, ctx=None):
            _ensure_magnus_parquet_appender_hdfs_uri_compat_in_worker()
            return original_do_write(self, blocks, ctx)

        magnus_data_sink._do_write = do_write_with_file_appender_compat

    for class_name, method_name in [
        ("MagnusDatasinkWriter", "__call__"),
        ("MagnusDynamicBucketDatasinkWriter", "__call__"),
        ("MagnusBucketedDatasinkWriter", "_write"),
    ]:
        writer_cls = getattr(magnus_datasink_module, class_name, None)
        if writer_cls is None or not hasattr(writer_cls, method_name):
            continue
        original_method = getattr(writer_cls, method_name)

        def method_with_file_appender_compat(self, *args, _original_method=original_method, **kwargs):
            _ensure_magnus_parquet_appender_hdfs_uri_compat_in_worker()
            return _original_method(self, *args, **kwargs)

        setattr(writer_cls, method_name, method_with_file_appender_compat)

    magnus_datasink_module._dj_worker_file_appender_compat_patched = True


def _schema_columns_from_config(schema_config):
    explicit_schema = build_arrow_schema_from_config(schema_config)
    if explicit_schema is None:
        return None
    return list(explicit_schema.names)


def _ray_dataset_columns(dataset, *, schema_config=None, fetch_if_missing=True):
    configured_columns = _schema_columns_from_config(schema_config)
    if configured_columns is not None:
        return configured_columns

    try:
        columns = dataset.columns(fetch_if_missing=fetch_if_missing)
        if columns is not None:
            return columns
    except TypeError:
        if fetch_if_missing:
            columns = dataset.columns()
            if columns is not None:
                return columns
    except Exception:
        return None

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
    if names is not None:
        return list(names)
    return None


def _ensure_empty_ray_dataset_has_schema(dataset, schema, preserve_lazy=False):
    if preserve_lazy:
        return dataset

    import pyarrow as pa

    try:
        inferred_schema = dataset.schema()
    except Exception:
        return dataset
    base_schema = getattr(inferred_schema, "base_schema", inferred_schema)
    if base_schema is not None:
        return dataset
    if dataset.count() != 0:
        raise ValueError("Magnus export cannot replace a non-empty Ray dataset with unknown schema")

    import ray

    return ray.data.from_arrow(pa.Table.from_pylist([], schema=schema))


def _validate_magnus_partition_overwrite(dataset, partition_columns, partition_values):
    _validate_magnus_partition_overwrite_config(partition_columns, partition_values)
    partition_columns = list(partition_columns or [])
    partition_values = namespace_to_plain_dict(partition_values)

    row_count = dataset.count()
    if row_count == 0:
        raise ValueError("Magnus partition overwrite refuses to overwrite with an empty dataset")

    _validate_magnus_partition_overwrite_columns(dataset, partition_columns)

    selected_dataset = dataset.select_columns(partition_columns)
    for batch in selected_dataset.iter_batches(batch_format="pyarrow", batch_size=8192):
        _validate_magnus_partition_overwrite_batch(batch, partition_columns, partition_values)


def _validate_magnus_partition_overwrite_config(partition_columns, partition_values):
    partition_columns = list(partition_columns or [])
    partition_values = namespace_to_plain_dict(partition_values)
    if not partition_columns:
        raise ValueError("Magnus OVERWRITE with partition_values requires `partition_columns`")
    if not isinstance(partition_values, dict) or not partition_values:
        raise ValueError("Magnus partition overwrite requires non-empty `partition_values`")

    missing_values = [column for column in partition_columns if column not in partition_values]
    if missing_values:
        raise ValueError(f"Magnus partition overwrite is missing partition values for: {missing_values}")


def _validate_magnus_partition_overwrite_columns(dataset, partition_columns, *, schema_config=None, fetch_if_missing=True):
    columns = _ray_dataset_columns(dataset, schema_config=schema_config, fetch_if_missing=fetch_if_missing) or []
    missing_columns = [column for column in partition_columns if column not in columns]
    if missing_columns:
        raise ValueError(f"Magnus partition overwrite dataset is missing partition columns: {missing_columns}")


def _validate_magnus_partition_overwrite_batch(batch, partition_columns, partition_values):
    for partition_column in partition_columns:
        if batch.schema.get_field_index(partition_column) < 0:
            raise ValueError(f"Magnus partition overwrite dataset is missing partition columns: [{partition_column!r}]")
        expected = partition_values[partition_column]
        for actual in batch[partition_column].to_pylist():
            if actual != expected:
                raise ValueError(
                    "Magnus partition overwrite dataset contains unexpected partition value "
                    f"for {partition_column}: expected {expected!r}, got {actual!r}"
                )


def _validate_magnus_partition_overwrite_lazy(dataset, partition_columns, partition_values, *, schema_config=None):
    _validate_magnus_partition_overwrite_config(partition_columns, partition_values)
    partition_columns = list(partition_columns or [])
    partition_values = namespace_to_plain_dict(partition_values)
    if _ray_dataset_columns(dataset, schema_config=schema_config, fetch_if_missing=False) is not None:
        _validate_magnus_partition_overwrite_columns(
            dataset,
            partition_columns,
            schema_config=schema_config,
            fetch_if_missing=False,
        )

    def validate_partition_values(batch):
        _validate_magnus_partition_overwrite_batch(batch, partition_columns, partition_values)
        return batch

    return dataset.map_batches(validate_partition_values, batch_format="pyarrow")


def _ensure_magnus_partition_columns(dataset, partition_columns, partition_values, schema_config=None, preserve_lazy=False):
    partition_columns = list(partition_columns or [])
    partition_values = namespace_to_plain_dict(partition_values)
    columns = _ray_dataset_columns(
        dataset,
        schema_config=schema_config if preserve_lazy else None,
        fetch_if_missing=not preserve_lazy,
    )
    missing_columns = [column for column in partition_columns if columns is None or column not in columns]
    if not preserve_lazy and not missing_columns:
        return dataset

    import pyarrow as pa

    explicit_schema = build_arrow_schema_from_config(schema_config)
    explicit_types = {}
    if explicit_schema is not None:
        explicit_types = {
            field.name: field.type
            for field in explicit_schema
            if field.name in missing_columns
        }

    def add_partition_columns(batch):
        row_count = batch.num_rows
        batch_missing_columns = [
            column
            for column in (partition_columns if preserve_lazy else missing_columns)
            if batch.schema.get_field_index(column) < 0
        ]
        for column in batch_missing_columns:
            batch = batch.append_column(
                column,
                pa.array(
                    [partition_values[column]] * row_count,
                    type=explicit_types.get(column),
                ),
            )
        return batch

    return dataset.map_batches(add_partition_columns, batch_format="pyarrow")


def _apply_magnus_write_defaults(magnus_conf, *, is_partition_overwrite=False):
    magnus_conf = _normalize_magnus_conf(magnus_conf)
    if not is_partition_overwrite:
        return magnus_conf

    write_options = _flatten_dotted_options(magnus_conf.get("write_options") or {})
    write_options.setdefault(_MAGNUS_RAY_DISABLE_REPARTITION, "true")
    write_options.setdefault(_MAGNUS_RAY_DISABLE_SORT, "true")
    magnus_conf["write_options"] = write_options
    return magnus_conf


def _project_ray_dataset_to_schema(dataset, schema):
    if schema is None:
        return dataset
    select_columns = getattr(dataset.__class__, "select_columns", None)
    if not callable(select_columns):
        return dataset
    dataset = dataset.select_columns(list(schema.names))
    map_batches = getattr(dataset.__class__, "map_batches", None)
    if not callable(map_batches):
        return dataset

    def align_batch_to_schema(batch):
        return _align_arrow_table_to_schema(batch, schema)

    return dataset.map_batches(align_batch_to_schema, batch_format="pyarrow")


def _align_arrow_table_to_schema(table, schema):
    import pyarrow as pa

    arrays = []
    for field in schema:
        field_index = table.schema.get_field_index(field.name)
        if field_index < 0:
            raise ValueError(f"Magnus export dataset is missing schema column: {field.name!r}")
        arrays.append(_coerce_arrow_array_to_type(table.column(field_index), field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def _coerce_arrow_array_to_type(array, target_type):
    import pyarrow as pa

    if array.type.equals(target_type):
        return array
    try:
        return array.cast(target_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
        # Older PyArrow builds can fail to cast nested structs whose fields are
        # the same by name but ordered differently. Rebuilding from Python values
        # lets PyArrow bind struct members by field name to the explicit schema.
        return pa.array(array.to_pylist(), type=target_type)


def _normalize_magnus_conf(magnus_conf):
    magnus_conf = dict(namespace_to_plain_dict(magnus_conf or {}))
    if "write_options" in magnus_conf:
        magnus_conf["write_options"] = _flatten_dotted_options(magnus_conf["write_options"])
    return magnus_conf


def _normalize_magnus_failure_policy(policy):
    policy = MAGNUS_FAILURE_POLICY_ABORT if policy is None else str(policy)
    policy = policy.lower()
    if policy not in {MAGNUS_FAILURE_POLICY_ABORT, MAGNUS_FAILURE_POLICY_COMMIT_COMPLETED_UNSAFE}:
        raise ValueError(
            "Unsupported Magnus failure policy: "
            f"{policy!r}. Expected {MAGNUS_FAILURE_POLICY_ABORT!r} or "
            f"{MAGNUS_FAILURE_POLICY_COMMIT_COMPLETED_UNSAFE!r}."
        )
    return policy


def _normalize_magnus_write_operation(operation):
    operation = "APPEND" if operation is None else str(operation).upper()
    if operation == "OVERWRITE_PARTITION":
        return "OVERWRITE"
    if operation not in {"APPEND", "OVERWRITE"}:
        raise ValueError(f"Unsupported Magnus write operation: {operation!r}")
    return operation


def _apply_magnus_failure_policy_marker(magnus_conf, failure_policy):
    if failure_policy == MAGNUS_FAILURE_POLICY_ABORT:
        return magnus_conf
    magnus_conf = dict(magnus_conf or {})
    snapshot_summary = dict(namespace_to_plain_dict(magnus_conf.get("snapshot_summary") or {}))
    snapshot_summary[_MAGNUS_FAILURE_POLICY_SNAPSHOT_SUMMARY_KEY] = failure_policy
    magnus_conf["snapshot_summary"] = snapshot_summary
    return magnus_conf


def _patch_magnus_datasink_failure_policy(magnus_datasink_module):
    if getattr(magnus_datasink_module, "_dj_failure_policy_patched", False):
        return

    magnus_data_sink = getattr(magnus_datasink_module, "MagnusDataSink", None)
    if magnus_data_sink is None:
        return

    original_init = magnus_data_sink.__init__
    original_on_write_failed = getattr(magnus_data_sink, "on_write_failed", None)

    def init_with_failure_policy(self, table, operation, write_options, snapshot_summary, tag_name=None):
        snapshot_summary = dict(snapshot_summary or {})
        failure_policy = _normalize_magnus_failure_policy(
            snapshot_summary.pop(_MAGNUS_FAILURE_POLICY_SNAPSHOT_SUMMARY_KEY, MAGNUS_FAILURE_POLICY_ABORT)
        )
        original_init(self, table, operation, write_options, snapshot_summary, tag_name)
        self._dj_magnus_failure_policy = failure_policy

    def on_write_failed_with_failure_policy(self, error):
        failure_policy = getattr(self, "_dj_magnus_failure_policy", MAGNUS_FAILURE_POLICY_ABORT)
        if failure_policy == MAGNUS_FAILURE_POLICY_COMMIT_COMPLETED_UNSAFE:
            try:
                # Magnus commit() reads completed files from Ray Data checkpoint
                # metadata when checkpointing is enabled. The placeholder shape is
                # only used when the SDK falls back to its non-checkpoint path.
                self.commit([[]])
            except Exception:
                logger.exception("Failed to commit completed Magnus files after write failure")
        if original_on_write_failed is not None:
            return original_on_write_failed(self, error)
        return None

    magnus_data_sink.__init__ = init_with_failure_policy
    magnus_data_sink.on_write_failed = on_write_failed_with_failure_policy
    magnus_datasink_module._dj_failure_policy_patched = True


def _magnus_table_properties_from_write_options(magnus_conf):
    write_options = _flatten_dotted_options((magnus_conf or {}).get("write_options") or {})
    table_properties = {}
    if "write.format.default" in write_options:
        table_properties["write.format.default"] = write_options["write.format.default"]
    return table_properties


def _is_ray_data_checkpoint_enabled():
    try:
        import ray

        context = ray.data.DataContext.get_current()
    except Exception:
        return False
    return bool(getattr(context, "data_checkpoint_dir", ""))


def write_ray_dataset_to_magnus(dataset, table_name: str, **kwargs):
    pyiceberg_ray = import_optional_dependency("pyiceberg.ray", extra_name="internal_io")
    magnus_datasink_module = import_optional_dependency("pyiceberg.ray.magnus_datasink", extra_name="internal_io")
    magnus_transformers_module = import_optional_dependency("pyiceberg.ray.transformers", extra_name="internal_io")
    magnus_file_appender_module = import_optional_dependency("pyiceberg.magnus.file_appender", extra_name="internal_io")
    _patch_magnus_datasink_write_result_compat(magnus_datasink_module)
    _patch_magnus_datasink_failure_policy(magnus_datasink_module)
    _patch_magnus_transformer_localsort_compat(magnus_transformers_module)
    _patch_magnus_parquet_appender_hdfs_uri_compat(magnus_file_appender_module)
    _patch_magnus_datasink_worker_file_appender_compat(magnus_datasink_module)

    partition_columns = kwargs.get("partition_columns", None)
    partition_values = kwargs.get("partition_values", None)
    schema_config = kwargs.get("schema")
    create_table_if_not_exists = bool(kwargs.get("create_table_if_not_exists", False))
    infer_schema_on_create = bool(kwargs.get("infer_schema_on_create", False))
    failure_policy = _normalize_magnus_failure_policy(kwargs.get("magnus_failure_policy"))
    magnus_conf = _normalize_magnus_conf(kwargs.get("magnus_conf", {}) or {})
    operation = kwargs.get("operation", magnus_conf.pop("operation", "APPEND")) or "APPEND"
    operation = _normalize_magnus_write_operation(operation)
    is_partition_overwrite = operation == "OVERWRITE" and partition_values is not None
    magnus_conf = _apply_magnus_write_defaults(
        magnus_conf,
        is_partition_overwrite=is_partition_overwrite,
    )
    use_ray_data_checkpoint = _is_ray_data_checkpoint_enabled()
    if failure_policy == MAGNUS_FAILURE_POLICY_COMMIT_COMPLETED_UNSAFE and not use_ray_data_checkpoint:
        raise ValueError("Magnus `commit_completed_unsafe` failure policy requires Ray Data checkpointing")
    magnus_conf = _apply_magnus_failure_policy_marker(magnus_conf, failure_policy)
    validate_overwrite_partition_before_write = bool(
        kwargs.get("validate_overwrite_partition_before_write", False)
    )
    preserve_lazy = use_ray_data_checkpoint or (
        is_partition_overwrite and not validate_overwrite_partition_before_write
    )
    if is_partition_overwrite:
        if (
            validate_overwrite_partition_before_write
            and hasattr(dataset, "materialize")
            and not use_ray_data_checkpoint
        ):
            dataset = dataset.materialize()
        dataset = _ensure_magnus_partition_columns(
            dataset,
            partition_columns,
            partition_values,
            schema_config=kwargs.get("schema"),
            preserve_lazy=preserve_lazy,
        )
        if preserve_lazy:
            dataset = _validate_magnus_partition_overwrite_lazy(
                dataset,
                partition_columns,
                partition_values,
                schema_config=kwargs.get("schema"),
            )
        else:
            _validate_magnus_partition_overwrite(dataset, partition_columns, partition_values)
    explicit_schema = build_arrow_schema_from_config(schema_config)
    if explicit_schema is not None:
        dataset = _ensure_empty_ray_dataset_has_schema(dataset, explicit_schema, preserve_lazy=preserve_lazy)
    if create_table_if_not_exists:
        create_table_kwargs = {"partition_columns": partition_columns}
        if infer_schema_on_create and explicit_schema is None:
            def schema_provider():
                nonlocal dataset
                inferred_schema, dataset = _infer_magnus_schema_and_dataset_from_ray_dataset(dataset)
                return inferred_schema

            create_table_kwargs["schema_provider"] = schema_provider
        table_properties = _magnus_table_properties_from_write_options(magnus_conf)
        if table_properties:
            create_table_kwargs["table_properties"] = table_properties
        create_magnus_table_if_not_exists(table_name, explicit_schema, **create_table_kwargs)
    dataset = _project_ray_dataset_to_schema(dataset, explicit_schema)
    pyiceberg_ray.write_magnus(
        dataset,
        identifier=table_name,
        operation=operation,
        **magnus_conf,
    )
