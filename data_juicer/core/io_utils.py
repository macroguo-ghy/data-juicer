from __future__ import annotations

import hashlib
import importlib
import json
import os
import posixpath
import shutil
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple
from urllib.parse import urlparse

from jsonargparse import Namespace, namespace_to_dict
from loguru import logger


def namespace_to_plain_dict(value: Any) -> Any:
    """Recursively convert jsonargparse namespaces to plain Python types."""
    if isinstance(value, Namespace):
        value = namespace_to_dict(value)
    if isinstance(value, dict):
        return {k: namespace_to_plain_dict(v) for k, v in value.items()}
    if isinstance(value, list):
        return [namespace_to_plain_dict(v) for v in value]
    return value


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

    fs.create_dir(fs_path, recursive=True)
    for file_path in src.rglob("*"):
        if not file_path.is_file():
            continue
        rel_path = file_path.relative_to(src).as_posix()
        target_path = f"{fs_path.rstrip('/')}/{rel_path}"
        ensure_remote_parent(fs, target_path)
        with open(file_path, "rb") as rf, fs.open_output_stream(target_path) as wf:
            shutil.copyfileobj(rf, wf)


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
    lark = import_optional_dependency("lark_oapi", extra_name="internal_io")
    drive_v1 = import_optional_dependency("lark_oapi.api.drive.v1", extra_name="internal_io")

    client = (
        lark.Client.builder().app_id(lark_app_id).app_secret(lark_app_secret).log_level(lark.LogLevel.ERROR).build()
    )
    token = lark_path.split("/")[-1].split("?")[0]
    if sheet_id is None and "?" in lark_path:
        sheet_id = lark_path.split("?")[1].split("=")[1]

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
        raise RuntimeError("Lark export task did not finish successfully")

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


def upload_file_to_lark_sheet(
    local_path: str,
    lark_path: str,
    lark_app_id: str,
    lark_app_secret: str,
    cell_range: str,
) -> None:
    lark = import_optional_dependency("lark_oapi", extra_name="internal_io")
    drive_v1 = import_optional_dependency("lark_oapi.api.drive.v1", extra_name="internal_io")

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


def create_magnus_table_if_not_exists(table_name: str, schema, partition_columns=None):
    magnus_module = import_optional_dependency("pyiceberg.magnus", extra_name="internal_io")

    catalog, database, short_table_name = table_name.split(".")
    magnus_client = magnus_module.MagnusClient()
    if magnus_client.exist_table(catalog, database, short_table_name):
        return magnus_client

    magnus_client.create_table(catalog, database, short_table_name, schema, partition_columns=partition_columns)
    return magnus_client


def write_hf_dataset_to_magnus(dataset, table_name: str, **kwargs):
    import pyarrow as pa

    magnus_writer_module = import_optional_dependency("pyiceberg.magnus.magnus_writer", extra_name="internal_io")

    partition_columns = kwargs.get("partition_columns", None)
    magnus_conf = kwargs.get("magnus_conf", {})
    batch_size = kwargs.get("batch_size", 2000)

    arrow_schema = dataset.features.arrow_schema
    schema = pa.schema([pa.field(col_name, col_type) for col_name, col_type in zip(arrow_schema.names, arrow_schema.types)])
    magnus_client = create_magnus_table_if_not_exists(table_name, schema, partition_columns=partition_columns)
    table = magnus_client.load_table(table_name)
    writer = magnus_writer_module.MagnusMultiFileWriter(table, **magnus_conf)

    df = dataset.to_pandas()
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i : i + batch_size]
        writer.write(batch_df.to_dict("records"))
    writer.finish()
    writer.commit()


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


def write_ray_dataset_to_magnus(dataset, table_name: str, **kwargs):
    pyiceberg_ray = import_optional_dependency("pyiceberg.ray", extra_name="internal_io")
    magnus_datasink_module = import_optional_dependency("pyiceberg.ray.magnus_datasink", extra_name="internal_io")
    _patch_magnus_datasink_write_result_compat(magnus_datasink_module)

    partition_columns = kwargs.get("partition_columns", None)
    magnus_conf = dict(kwargs.get("magnus_conf", {}))
    operation = kwargs.get("operation", magnus_conf.pop("operation", "APPEND")) or "APPEND"
    operation = str(operation).upper()
    if operation not in {"APPEND", "OVERWRITE"}:
        raise ValueError(f"Unsupported Magnus write operation: {operation!r}")
    schema = dataset.schema().base_schema
    create_magnus_table_if_not_exists(table_name, schema, partition_columns=partition_columns)
    pyiceberg_ray.write_magnus(dataset, identifier=table_name, operation=operation, **magnus_conf)
