import base64
import inspect
import json
import math
import os
import posixpath
import uuid
from functools import partial

from loguru import logger

from data_juicer.core.fanout_compact import normalize_fanout_target_compacts
from data_juicer.core.io_utils import _is_ray_data_checkpoint_enabled, get_pyarrow_filesystem
from data_juicer.utils.constant import DATA_JUICER_INTERNAL_FIELDS, Fields, HashKeys
from data_juicer.utils.file_utils import Sizes, byte_size_to_size_str
from data_juicer.utils.model_utils import filter_arguments
from data_juicer.utils.ray_task_kv_store import incr_task_kv, snapshot_task_kv
from data_juicer.utils.webdataset_utils import reconstruct_custom_webdataset_format

EXPORT_WRITE_STATS_NAMESPACE = "export_write_stats"
TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in TRUTHY_ENV_VALUES


def _fanout_debug_logs_enabled() -> bool:
    return _truthy_env("DATA_JUICER_RAY_DEBUG_LOGS") or _truthy_env("DATA_JUICER_RAY_FANOUT_DEBUG")


def _dataset_columns_no_fetch(dataset):
    try:
        columns = dataset.columns(fetch_if_missing=False)
        if columns is not None:
            return columns
    except TypeError:
        pass
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


def _json_default(value):
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return tolist()
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    return str(value)


def _log_fanout_event(level: str, event: str, payload: dict, *, debug: bool = False) -> None:
    if debug and not _fanout_debug_logs_enabled():
        return
    body = {"event": event, **payload}
    try:
        message = json.dumps(body, sort_keys=True, default=_json_default)
    except Exception as exc:  # noqa: BLE001
        message = json.dumps(
            {
                "event": event,
                "log_error": repr(exc),
                "payload_repr": repr(payload),
            },
            sort_keys=True,
        )
    getattr(logger, level)(message)


def _target_log_context(target) -> dict:
    extra_args = target.get("extra_args") or {}
    columns = target.get("columns")
    if columns is None:
        columns = extra_args.get("columns")
    return {
        "target_index": target.get("index"),
        "target_type": target.get("type"),
        "path": target.get("original_uri", target.get("path")),
        "mode": target.get("mode"),
        "condition": target.get("condition", target.get("filter_condition", "")),
        "compact": target.get("compact") or False,
        "columns_count": len(columns or []),
    }


def _schema_log_context(schema) -> dict:
    if schema is None:
        return {"available": False}
    base_schema = getattr(schema, "base_schema", schema)
    names = getattr(base_schema, "names", None)
    if names is None:
        return {"available": True, "repr": str(schema)[:2048]}
    names = list(names)
    return {
        "available": True,
        "columns_count": len(names),
        "columns_sample": names[:50],
    }


def summarize_filesystem_path(filesystem, path: str) -> dict[str, int]:
    from pyarrow.fs import FileSelector, FileType

    info = filesystem.get_file_info(path)
    if info.type is FileType.NotFound:
        return {"output_files": 0, "output_bytes": 0}
    if info.type is FileType.File:
        return {"output_files": 1, "output_bytes": int(info.size or 0)}
    if info.type is not FileType.Directory:
        return {"output_files": 0, "output_bytes": 0}

    selector = FileSelector(path, recursive=True)
    file_infos = filesystem.get_file_info(selector)
    files = [file_info for file_info in file_infos if file_info.type is FileType.File]
    return {
        "output_files": len(files),
        "output_bytes": sum(int(file_info.size or 0) for file_info in files),
    }


try:
    from ray.data.datasource import Datasink
except ImportError:  # pragma: no cover - Ray is required for RayExporter at runtime.
    Datasink = object

try:
    from ray.data.datasource.file_datasink import BlockBasedFileDatasink
except ImportError:  # pragma: no cover - Ray is required for RayExporter at runtime.
    BlockBasedFileDatasink = object

try:
    from ray.data.datasource.filename_provider import FilenameProvider
except ImportError:  # pragma: no cover - Ray is required for RayExporter at runtime.
    FilenameProvider = object


class _JsonlDatasink(BlockBasedFileDatasink):
    def __init__(self, path, *, ensure_ascii=False, **file_datasink_kwargs):
        super().__init__(path, file_format="json", **file_datasink_kwargs)
        self.ensure_ascii = ensure_ascii

    def write_block_to_file(self, block, file):
        table = block.to_arrow()
        for row in table.to_pylist():
            line = json.dumps(row, ensure_ascii=self.ensure_ascii, default=_json_default)
            file.write((line + "\n").encode("utf-8"))


class _AppendFilenameProvider(FilenameProvider):
    def __init__(self, prefix: str, file_format: str):
        self.prefix = prefix
        self.file_format = file_format

    def _filename(self, write_uuid, task_index, block_index=None, row_index=None):
        parts = [self.prefix]
        if write_uuid:
            parts.append(str(write_uuid))
        parts.append(f"{task_index:06}")
        if block_index is not None:
            parts.append(f"{block_index:06}")
        if row_index is not None:
            parts.append(f"{row_index:06}")
        return "_".join(parts) + f".{self.file_format}"

    def get_filename_for_task(self, write_uuid: str, task_index: int) -> str:
        return self._filename(write_uuid, task_index)

    def get_filename_for_block(self, block, *args) -> str:
        if len(args) == 2:
            task_index, block_index = args
            return self._filename(None, task_index, block_index)
        if len(args) == 3:
            write_uuid, task_index, block_index = args
            return self._filename(write_uuid, task_index, block_index)
        raise TypeError("Unexpected FilenameProvider block callback signature.")

    def get_filename_for_row(self, row, *args) -> str:
        if len(args) == 3:
            task_index, block_index, row_index = args
            return self._filename(None, task_index, block_index, row_index)
        if len(args) == 4:
            write_uuid, task_index, block_index, row_index = args
            return self._filename(write_uuid, task_index, block_index, row_index)
        raise TypeError("Unexpected FilenameProvider row callback signature.")


class _FanoutCompactBuffer:
    def __init__(self, *, datasink, target, task_index: int):
        self.datasink = datasink
        self.target = target
        self.task_index = task_index
        self.compact = target["compact"]
        self.flush_index = 0
        self.tables = []
        self.jsonl_lines = []
        self.schema = None
        self.rows = 0
        self.bytes = 0
        self.stats = {
            "files": 0,
            "flushes": 0,
            "schema_mismatch_flushes": 0,
        }
        self.append_calls = 0
        self.warned_schema_mismatch = False
        self.warned_large_row = False
        self.logged_table_split = False
        _log_fanout_event(
            "info",
            "ray_fanout_compact_buffer_init",
            {
                "write_uuid": self.datasink.write_uuid,
                "task_index": self.task_index,
                **_target_log_context(self.target),
            },
            debug=True,
        )

    def append(self, table) -> dict[str, int]:
        if table.num_rows == 0:
            return self._empty_result()
        self.append_calls += 1
        if self.append_calls == 1:
            _log_fanout_event(
                "info",
                "ray_fanout_compact_first_append",
                {
                    "write_uuid": self.datasink.write_uuid,
                    "task_index": self.task_index,
                    "input_rows": table.num_rows,
                    "input_bytes": self._table_nbytes(table),
                    **_target_log_context(self.target),
                },
                debug=True,
            )
        if self.target["type"] == "jsonl":
            return self._append_jsonl(table)
        return self._append_parquet(table)

    def flush(self, *, reason: str = "manual") -> dict[str, int]:
        if self.rows == 0:
            return self._empty_result()

        rows = self.rows
        bytes_buffered = self.bytes
        table_count = len(self.tables)
        jsonl_line_count = len(self.jsonl_lines)
        output_path = posixpath.join(self.target["path"], self._filename())
        _log_fanout_event(
            "info",
            "ray_fanout_compact_flush_start",
            {
                "write_uuid": self.datasink.write_uuid,
                "task_index": self.task_index,
                "flush_index": self.flush_index,
                "reason": reason,
                "buffered_rows": rows,
                "buffered_bytes": bytes_buffered,
                "buffered_tables": table_count,
                "buffered_jsonl_lines": jsonl_line_count,
                "output_path": output_path,
                **_target_log_context(self.target),
            },
            debug=True,
        )
        try:
            if self.target["type"] == "jsonl":
                self.datasink._write_jsonl_lines(self.target["filesystem"], output_path, self.jsonl_lines)
            else:
                table = self.tables[0] if len(self.tables) == 1 else self._concat_tables(self.tables)
                self.datasink._write_table(self.target, table, output_path)
        except Exception as exc:  # noqa: BLE001
            _log_fanout_event(
                "warning",
                "ray_fanout_compact_flush_failed",
                {
                    "write_uuid": self.datasink.write_uuid,
                    "task_index": self.task_index,
                    "flush_index": self.flush_index,
                    "reason": reason,
                    "buffered_rows": rows,
                    "buffered_bytes": bytes_buffered,
                    "output_path": output_path,
                    "error": repr(exc),
                    **_target_log_context(self.target),
                },
            )
            raise
        metadata = self.datasink._summarize_written_file(self.target, output_path)
        self.stats["files"] += metadata["output_files"]
        self.stats["flushes"] += 1
        self.flush_index += 1
        self._reset()
        _log_fanout_event(
            "info",
            "ray_fanout_compact_flush_complete",
            {
                "write_uuid": self.datasink.write_uuid,
                "task_index": self.task_index,
                "flush_index": self.flush_index - 1,
                "reason": reason,
                "rows": rows,
                "output_files": metadata["output_files"],
                "output_bytes": metadata["output_bytes"],
                "output_path": output_path,
                **_target_log_context(self.target),
            },
            debug=True,
        )
        return {
            "rows": rows,
            "files": metadata["output_files"],
            "bytes": metadata["output_bytes"],
        }

    def compact_summary(self) -> dict[str, int]:
        return dict(self.stats)

    def _append_parquet(self, table) -> dict[str, int]:
        result = self._empty_result()
        for table_slice in self._iter_table_slices(table):
            result = self._merge_results(result, self._append_parquet_slice(table_slice))
        return result

    def _append_parquet_slice(self, table) -> dict[str, int]:
        result = self._empty_result()
        if self.schema is not None and not table.schema.equals(self.schema, check_metadata=False):
            if self.rows:
                result = self._merge_results(result, self.flush(reason="schema_mismatch"))
            self.stats["schema_mismatch_flushes"] += 1
            self._warn_schema_mismatch_once()

        table_bytes = self._table_nbytes(table)
        if table.num_rows == 1 and table_bytes > self.compact["max_buffer_bytes"]:
            self._warn_large_row_once(table_bytes)
        if self.rows and self.bytes + table_bytes > self.compact["max_buffer_bytes"]:
            result = self._merge_results(result, self.flush(reason="max_buffer_bytes"))

        self.tables.append(table)
        self.schema = table.schema
        self.rows += table.num_rows
        self.bytes += table_bytes
        flush_reason = self._flush_reason()
        if flush_reason:
            result = self._merge_results(result, self.flush(reason=flush_reason))
        return result

    def _append_jsonl(self, table) -> dict[str, int]:
        result = self._empty_result()
        ensure_ascii = self.target.get("extra_args", {}).get(
            "force_ascii",
            self.target.get("extra_args", {}).get("ensure_ascii", False),
        )
        for row in table.to_pylist():
            line = (json.dumps(row, ensure_ascii=ensure_ascii, default=_json_default) + "\n").encode("utf-8")
            line_bytes = len(line)
            if line_bytes > self.compact["max_buffer_bytes"]:
                self._warn_large_row_once(line_bytes)
            if self.rows and self.bytes + line_bytes > self.compact["max_buffer_bytes"]:
                result = self._merge_results(result, self.flush(reason="max_buffer_bytes"))
            self.jsonl_lines.append(line)
            self.rows += 1
            self.bytes += line_bytes
            flush_reason = self._flush_reason()
            if flush_reason:
                result = self._merge_results(result, self.flush(reason=flush_reason))
        return result

    def _iter_table_slices(self, table):
        max_buffer_bytes = self.compact["max_buffer_bytes"]
        table_bytes = self._table_nbytes(table)
        if table_bytes <= max_buffer_bytes or table.num_rows <= 1:
            yield table
            return

        if not self.logged_table_split:
            self.logged_table_split = True
            _log_fanout_event(
                "info",
                "ray_fanout_compact_split_large_table",
                {
                    "write_uuid": self.datasink.write_uuid,
                    "task_index": self.task_index,
                    "input_rows": table.num_rows,
                    "input_bytes": table_bytes,
                    "max_buffer_bytes": max_buffer_bytes,
                    **_target_log_context(self.target),
                },
                debug=True,
            )
        avg_row_bytes = max(1, math.ceil(table_bytes / table.num_rows))
        rows_per_slice = max(1, min(table.num_rows, max_buffer_bytes // avg_row_bytes))
        start = 0
        while start < table.num_rows:
            row_count = min(rows_per_slice, table.num_rows - start)
            table_slice = table.slice(start, row_count)
            while row_count > 1 and self._table_nbytes(table_slice) > max_buffer_bytes:
                row_count = max(1, row_count // 2)
                table_slice = table.slice(start, row_count)
            yield table_slice
            start += row_count

    def _filename(self) -> str:
        extension = "jsonl" if self.target["type"] == "jsonl" else self.target["type"]
        return (
            f"part-{self.target['index']}-{self.datasink.write_uuid}-"
            f"{self.task_index}-compact-{self.flush_index}.{extension}"
        )

    def _flush_reason(self):
        bytes_reached = self.bytes >= self.compact["target_bytes_per_file"]
        rows_reached = self.rows >= self.compact["target_rows_per_file"]
        if bytes_reached and rows_reached:
            return "target_bytes_and_rows"
        if bytes_reached:
            return "target_bytes_per_file"
        if rows_reached:
            return "target_rows_per_file"
        return None

    def _reset(self) -> None:
        self.tables = []
        self.jsonl_lines = []
        self.schema = None
        self.rows = 0
        self.bytes = 0

    def _warn_schema_mismatch_once(self) -> None:
        if self.warned_schema_mismatch:
            return
        self.warned_schema_mismatch = True
        _log_fanout_event(
            "warning",
            "ray_fanout_compact_schema_mismatch",
            {
                "write_uuid": self.datasink.write_uuid,
                "task_index": self.task_index,
                **_target_log_context(self.target),
            },
            debug=True,
        )

    def _warn_large_row_once(self, row_bytes: int) -> None:
        if self.warned_large_row:
            return
        self.warned_large_row = True
        _log_fanout_event(
            "warning",
            "ray_fanout_compact_large_row",
            {
                "write_uuid": self.datasink.write_uuid,
                "task_index": self.task_index,
                "row_bytes": row_bytes,
                "max_buffer_bytes": self.compact["max_buffer_bytes"],
                **_target_log_context(self.target),
            },
            debug=True,
        )

    @staticmethod
    def _table_nbytes(table) -> int:
        return int(getattr(table, "nbytes", 0) or 0)

    @staticmethod
    def _concat_tables(tables):
        import pyarrow as pa

        return pa.concat_tables(tables)

    @staticmethod
    def _empty_result() -> dict[str, int]:
        return {"rows": 0, "files": 0, "bytes": 0}

    @staticmethod
    def _merge_results(left: dict[str, int], right: dict[str, int]) -> dict[str, int]:
        return {
            "rows": left["rows"] + right["rows"],
            "files": left["files"] + right["files"],
            "bytes": left["bytes"] + right["bytes"],
        }


class RayHdfsFanoutDatasink(Datasink):
    """A Ray datasink that writes one input dataset to multiple file sinks."""

    def __init__(self, *, targets, columns=None):
        self.targets = []
        self.columns = columns
        self.write_uuid = uuid.uuid4().hex
        targets = [dict(target) for target in targets]
        normalize_fanout_target_compacts(targets)
        compact_configs = [target["compact"] for target in targets if target.get("compact")]
        self.compact_config = compact_configs[0] if compact_configs else None
        for index, target in enumerate(targets):
            condition = target.get("condition", target.get("filter_condition", ""))
            from data_juicer.ops.filter.general_field_filter import compile_filter_condition

            writer_target = {
                **target,
                "index": index,
                "condition": condition,
                "compiled_condition": compile_filter_condition(condition),
                "mode": target.get("mode") or "error_if_exists",
                "extra_args": dict(target.get("extra_args") or {}),
                "created_dir": False,
            }
            if "compact" in target:
                writer_target["compact"] = target["compact"]
            self.targets.append(writer_target)
        _log_fanout_event(
            "info",
            "ray_fanout_datasink_init",
            {
                "write_uuid": self.write_uuid,
                "target_count": len(self.targets),
                "compact_target_count": len([target for target in self.targets if target.get("compact")]),
                "direct_target_count": len([target for target in self.targets if not target.get("compact")]),
                "min_rows_per_write": self.min_rows_per_write,
                "targets": [_target_log_context(target) for target in self.targets],
            },
            debug=True,
        )

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    @property
    def min_rows_per_write(self):
        if not self.compact_config:
            return None
        return self.compact_config["target_rows_per_file"]

    def get_name(self) -> str:
        return "FileFanout"

    def on_write_start(self, schema=None) -> None:
        from pyarrow.fs import FileType

        _log_fanout_event(
            "info",
            "ray_fanout_on_write_start",
            {
                "write_uuid": self.write_uuid,
                "min_rows_per_write": self.min_rows_per_write,
                "schema": _schema_log_context(schema),
                "targets": [_target_log_context(target) for target in self.targets],
            },
            debug=True,
        )
        file_infos = []
        for target in self.targets:
            info = target["filesystem"].get_file_info(target["path"])
            file_infos.append(info)
            if target["mode"] == "error_if_exists" and info.type is not FileType.NotFound:
                raise FileExistsError(
                    f"Ray file fan-out export path already exists: {target['original_uri']}. "
                    "Set `mode: overwrite` to replace it or `mode: append` to append."
                )

        for target, info in zip(self.targets, file_infos):
            filesystem = target["filesystem"]
            if target["mode"] == "overwrite" and info.type is not FileType.NotFound:
                if info.type is FileType.Directory:
                    filesystem.delete_dir(target["path"])
                else:
                    filesystem.delete_file(target["path"])
                target["created_dir"] = True
            elif info.type is FileType.NotFound:
                target["created_dir"] = True
            self._create_dir(filesystem, target["path"])
        _log_fanout_event(
            "info",
            "ray_fanout_output_dirs_ready",
            {
                "write_uuid": self.write_uuid,
                "targets": [
                    {
                        **_target_log_context(target),
                        "created_dir": target["created_dir"],
                    }
                    for target in self.targets
                ],
            },
            debug=True,
        )

    @staticmethod
    def _create_dir(filesystem, path):
        try:
            filesystem.create_dir(path, recursive=True)
        except TypeError:
            filesystem.create_dir(path)

    def write(self, blocks, ctx):
        task_index = getattr(ctx, "task_idx", 0)
        results = {target["index"]: 0 for target in self.targets}
        partial_rows = {target["index"]: 0 for target in self.targets}
        partial_files = {target["index"]: 0 for target in self.targets}
        partial_bytes = {target["index"]: 0 for target in self.targets}
        compact_buffers = {
            target["index"]: _FanoutCompactBuffer(
                datasink=self,
                target=target,
                task_index=task_index,
            )
            for target in self.targets
            if target.get("compact")
        }
        direct_logged_targets = set()
        _log_fanout_event(
            "info",
            "ray_fanout_write_task_start",
            {
                "write_uuid": self.write_uuid,
                "task_index": task_index,
                "min_rows_per_write": self.min_rows_per_write,
                "compact_target_indices": sorted(compact_buffers),
                "direct_target_indices": sorted(target["index"] for target in self.targets if not target.get("compact")),
            },
            debug=True,
        )
        try:
            for block_index, block in enumerate(blocks):
                table = self._block_to_table(block)
                for target in self.targets:
                    filtered_table = self._filter_table_for_target(table, target)
                    if filtered_table.num_rows == 0:
                        continue
                    filtered_table = self._select_export_columns(filtered_table, target)
                    target_index = target["index"]
                    if target.get("compact"):
                        write_delta = compact_buffers[target_index].append(filtered_table)
                        self._apply_write_delta(
                            target_index,
                            write_delta,
                            results,
                            partial_rows,
                            partial_files,
                            partial_bytes,
                        )
                        continue
                    filename = self._filename(target, task_index, block_index)
                    output_path = posixpath.join(target["path"], filename)
                    if target_index not in direct_logged_targets:
                        direct_logged_targets.add(target_index)
                        _log_fanout_event(
                            "info",
                            "ray_fanout_direct_write_first_file",
                            {
                                "write_uuid": self.write_uuid,
                                "task_index": task_index,
                                "block_index": block_index,
                                "rows": filtered_table.num_rows,
                                "output_path": output_path,
                                **_target_log_context(target),
                            },
                            debug=True,
                        )
                    try:
                        self._write_table(target, filtered_table, output_path)
                    except Exception as exc:  # noqa: BLE001
                        _log_fanout_event(
                            "warning",
                            "ray_fanout_direct_write_failed",
                            {
                                "write_uuid": self.write_uuid,
                                "task_index": task_index,
                                "block_index": block_index,
                                "rows": filtered_table.num_rows,
                                "output_path": output_path,
                                "error": repr(exc),
                                **_target_log_context(target),
                            },
                        )
                        raise
                    metadata = self._summarize_written_file(target, output_path)
                    rows = filtered_table.num_rows
                    results[target_index] += rows
                    partial_rows[target_index] += rows
                    partial_files[target_index] += metadata["output_files"]
                    partial_bytes[target_index] += metadata["output_bytes"]
            for target_index, compact_buffer in compact_buffers.items():
                write_delta = compact_buffer.flush(reason="final")
                self._apply_write_delta(
                    target_index,
                    write_delta,
                    results,
                    partial_rows,
                    partial_files,
                    partial_bytes,
                )
            if compact_buffers:
                results["_compact"] = {
                    target_index: compact_buffer.compact_summary()
                    for target_index, compact_buffer in compact_buffers.items()
                }
            _log_fanout_event(
                "info",
                "ray_fanout_write_task_complete",
                {
                    "write_uuid": self.write_uuid,
                    "task_index": task_index,
                    "targets": self._task_targets_for_log(partial_rows, partial_files, partial_bytes),
                    "compact": {
                        target_index: compact_buffer.compact_summary()
                        for target_index, compact_buffer in compact_buffers.items()
                    },
                },
                debug=True,
            )
            return results
        except Exception as exc:  # noqa: BLE001
            _log_fanout_event(
                "warning",
                "ray_fanout_write_task_failed",
                {
                    "write_uuid": self.write_uuid,
                    "task_index": task_index,
                    "error": repr(exc),
                    "targets": self._task_targets_for_log(partial_rows, partial_files, partial_bytes),
                    "compact": {
                        target_index: compact_buffer.compact_summary()
                        for target_index, compact_buffer in compact_buffers.items()
                    },
                },
            )
            raise
        finally:
            self._record_partial_write_stats(
                partial_rows,
                partial_files,
                partial_bytes,
                {
                    target_index: compact_buffer.compact_summary()
                    for target_index, compact_buffer in compact_buffers.items()
                },
            )

    def _task_targets_for_log(self, partial_rows, partial_files, partial_bytes):
        return [
            {
                "target_index": target["index"],
                "rows": partial_rows.get(target["index"], 0),
                "files": partial_files.get(target["index"], 0),
                "bytes": partial_bytes.get(target["index"], 0),
                "compact_enabled": bool(target.get("compact")),
            }
            for target in self.targets
        ]

    @staticmethod
    def _apply_write_delta(target_index, write_delta, results, partial_rows, partial_files, partial_bytes):
        rows = int(write_delta.get("rows", 0) or 0)
        files = int(write_delta.get("files", 0) or 0)
        bytes_written = int(write_delta.get("bytes", 0) or 0)
        if rows:
            results[target_index] += rows
            partial_rows[target_index] += rows
        if files:
            partial_files[target_index] += files
        if bytes_written:
            partial_bytes[target_index] += bytes_written

    def on_write_complete(self, write_result):
        from pyarrow.fs import FileType

        row_counts = {target["index"]: 0 for target in self.targets}
        compact_counts = {
            target["index"]: self._new_compact_stats()
            for target in self.targets
            if target.get("compact")
        }
        for write_return in getattr(write_result, "write_returns", []) or []:
            if not isinstance(write_return, dict):
                continue
            self._merge_compact_write_return(compact_counts, write_return.get("_compact"))
            for target_index, count in write_return.items():
                if target_index == "_compact":
                    continue
                coerced_index = self._coerce_target_index(target_index)
                if coerced_index is None:
                    continue
                if isinstance(count, dict):
                    self._merge_compact_write_return(compact_counts, count.get("compact"))
                    count = count.get("rows", 0)
                row_counts[coerced_index] = row_counts.get(coerced_index, 0) + int(count or 0)

        for target in self.targets:
            if not target.get("created_dir") or row_counts.get(target["index"], 0) != 0:
                continue
            filesystem = target["filesystem"]
            if filesystem.get_file_info(target["path"]).type is not FileType.NotFound:
                filesystem.delete_dir(target["path"])

        target_summaries = []
        for target in self.targets:
            metadata = summarize_filesystem_path(target["filesystem"], target["path"])
            target_summary = {
                "path": target["original_uri"],
                "rows": row_counts.get(target["index"], 0),
                **metadata,
            }
            if target.get("compact"):
                target_summary["compact"] = self._compact_summary(compact_counts.get(target["index"]))
            target_summaries.append(target_summary)
        summary = {
            "output_rows": sum(target["rows"] for target in target_summaries),
            "output_files": sum(target["output_files"] for target in target_summaries),
            "output_bytes": sum(target["output_bytes"] for target in target_summaries),
            "targets": target_summaries,
        }
        self.last_write_summary = summary
        _log_fanout_event(
            "info",
            "ray_fanout_on_write_complete",
            {
                "write_uuid": self.write_uuid,
                "summary": summary,
            },
        )
        return summary

    @staticmethod
    def _new_compact_stats():
        return {
            "files": 0,
            "flushes": 0,
            "schema_mismatch_flushes": 0,
        }

    @classmethod
    def _compact_summary(cls, compact_stats):
        stats = cls._new_compact_stats()
        if isinstance(compact_stats, dict):
            for key in stats:
                stats[key] = int(compact_stats.get(key, 0) or 0)
        return {"enabled": True, **stats}

    @classmethod
    def _merge_compact_write_return(cls, compact_counts, compact_return):
        if not isinstance(compact_return, dict):
            return
        for target_index, stats in compact_return.items():
            coerced_index = cls._coerce_target_index(target_index)
            if coerced_index is None or not isinstance(stats, dict):
                continue
            target_stats = compact_counts.setdefault(coerced_index, cls._new_compact_stats())
            for key in target_stats:
                target_stats[key] += int(stats.get(key, 0) or 0)

    @staticmethod
    def _coerce_target_index(target_index):
        try:
            return int(target_index)
        except (TypeError, ValueError):
            return None

    def partial_write_summary(self):
        stats = snapshot_task_kv(namespace=EXPORT_WRITE_STATS_NAMESPACE) or {}
        target_summaries = []
        for target in self.targets:
            metadata = self._safe_summarize_target(target)
            target_index = target["index"]
            target_summary = {
                "path": target["original_uri"],
                "rows": self._optional_int(stats.get(self._stats_key("targets", target_index, "rows"))),
                **metadata,
            }
            if target.get("compact"):
                target_summary["compact"] = self._partial_compact_summary(stats, target_index)
            target_summaries.append(target_summary)

        output_rows = self._optional_int(stats.get(self._stats_key("output_rows")))
        output_files = self._sum_optional_values(target["output_files"] for target in target_summaries)
        output_bytes = self._sum_optional_values(target["output_bytes"] for target in target_summaries)
        summary = {
            "partial": True,
            "output_rows": output_rows,
            "output_files": output_files,
            "output_bytes": output_bytes,
            "targets": target_summaries,
        }
        _log_fanout_event(
            "info",
            "ray_fanout_partial_write_summary",
            {
                "write_uuid": self.write_uuid,
                "summary": summary,
            },
        )
        return summary

    def _partial_compact_summary(self, stats, target_index):
        compact_stats = self._new_compact_stats()
        for key in compact_stats:
            value = self._optional_int(stats.get(self._stats_key("targets", target_index, "compact", key)))
            if value is None:
                value = 0
            compact_stats[key] = value
        return {"enabled": True, **compact_stats}

    def _record_partial_write_stats(self, partial_rows, partial_files, partial_bytes, partial_compact_stats=None):
        deltas = {}
        partial_compact_stats = partial_compact_stats or {}
        for target in self.targets:
            target_index = target["index"]
            rows = partial_rows.get(target_index, 0)
            files = partial_files.get(target_index, 0)
            bytes_written = partial_bytes.get(target_index, 0)
            if rows:
                deltas[self._stats_key("output_rows")] = deltas.get(self._stats_key("output_rows"), 0) + rows
                deltas[self._stats_key("targets", target_index, "rows")] = rows
            if files:
                deltas[self._stats_key("output_files")] = deltas.get(self._stats_key("output_files"), 0) + files
                deltas[self._stats_key("targets", target_index, "output_files")] = files
            if bytes_written:
                deltas[self._stats_key("output_bytes")] = (
                    deltas.get(self._stats_key("output_bytes"), 0) + bytes_written
                )
                deltas[self._stats_key("targets", target_index, "output_bytes")] = bytes_written
            compact_stats = partial_compact_stats.get(target_index)
            if compact_stats:
                for key in ["files", "flushes", "schema_mismatch_flushes"]:
                    value = int(compact_stats.get(key, 0) or 0)
                    if value:
                        deltas[self._stats_key("targets", target_index, "compact", key)] = value
        if not deltas:
            return
        try:
            for key, delta in deltas.items():
                incr_task_kv(key, delta, namespace=EXPORT_WRITE_STATS_NAMESPACE, wait=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to record partial Ray fan-out export stats: {}", exc)

    def _stats_key(self, *parts) -> str:
        return ".".join(["fanout", self.write_uuid, *(str(part) for part in parts)])

    @staticmethod
    def _summarize_written_file(target, output_path: str) -> dict[str, int]:
        try:
            return summarize_filesystem_path(target["filesystem"], output_path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to collect written file metadata for {}: {}", output_path, exc)
            return {"output_files": 1, "output_bytes": 0}

    @staticmethod
    def _optional_int(value):
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _sum_optional_values(values):
        values = list(values)
        if any(value is None for value in values):
            return None
        return sum(values)

    @staticmethod
    def _safe_summarize_target(target):
        try:
            return summarize_filesystem_path(target["filesystem"], target["path"])
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to collect partial fan-out export metadata for {}: {}", target["original_uri"], exc)
            return {"output_files": None, "output_bytes": None}

    @staticmethod
    def _block_to_table(block):
        import pyarrow as pa
        from ray.data.block import BlockAccessor

        if isinstance(block, pa.Table):
            return block
        to_arrow = getattr(block, "to_arrow", None)
        if callable(to_arrow):
            return to_arrow()
        return BlockAccessor.for_block(block).to_arrow()

    def _filter_table_for_target(self, table, target):
        condition = target["compiled_condition"]
        if not condition.filter_condition:
            return table

        rows = table.to_pylist()
        mask = [condition.matches(row) for row in rows]
        if all(mask):
            return table
        if not any(mask):
            return table.slice(0, 0)

        import pyarrow as pa

        return table.filter(pa.array(mask))

    def _select_export_columns(self, table, target=None):
        columns = None if target is None else target.get("columns")
        if columns is None:
            columns = self.columns
        if columns is None:
            return table
        available = set(table.schema.names)
        columns = [column for column in columns if column in available]
        return table.select(columns)

    def _filename(self, target, task_index: int, block_index: int) -> str:
        extension = "jsonl" if target["type"] == "jsonl" else target["type"]
        return f"part-{target['index']:02d}-{self.write_uuid}-{task_index:06d}-{block_index:06d}.{extension}"

    def _write_table(self, target, table, output_path: str) -> None:
        export_type = target["type"]
        extra_args = dict(target.get("extra_args") or {})
        if export_type == "parquet":
            self._write_parquet(target["filesystem"], output_path, table, extra_args)
            return
        if export_type == "jsonl":
            self._write_jsonl(target["filesystem"], output_path, table, extra_args)
            return
        raise NotImplementedError(f"Ray file fan-out export does not support type [{export_type}]")

    @staticmethod
    def _write_parquet(filesystem, output_path: str, table, extra_args):
        import pyarrow.parquet as pq

        allowed = set(inspect.signature(pq.write_table).parameters)
        parquet_args = {key: value for key, value in extra_args.items() if key in allowed}
        with filesystem.open_output_stream(output_path) as file:
            pq.write_table(table, file, **parquet_args)

    @staticmethod
    def _write_jsonl(filesystem, output_path: str, table, extra_args):
        ensure_ascii = extra_args.get("force_ascii", extra_args.get("ensure_ascii", False))
        with filesystem.open_output_stream(output_path) as file:
            for row in table.to_pylist():
                line = json.dumps(row, ensure_ascii=ensure_ascii, default=_json_default)
                file.write((line + "\n").encode("utf-8"))

    @staticmethod
    def _write_jsonl_lines(filesystem, output_path: str, lines):
        with filesystem.open_output_stream(output_path) as file:
            for line in lines:
                file.write(line)


class RayExporter:
    """The Exporter class is used to export a ray dataset to files of specific
    format."""

    # TODO: support config for export, some export methods require additional args
    _SUPPORTED_FORMATS = {
        "json",
        "jsonl",
        "parquet",
        "csv",
        "tfrecords",
        "webdataset",
        "lance",
        # 'images',
        # 'numpy',
    }

    def __init__(
        self,
        export_path,
        export_type=None,
        export_shard_size=0,
        keep_stats_in_res_ds=True,
        keep_hashes_in_res_ds=False,
        **kwargs,
    ):
        """
        Initialization method.

        :param export_path: the path to export datasets.
        :param export_type: the format type of the exported datasets.
        :param export_shard_size: the approximate size of each shard of exported
            dataset. In default, it's 0, which means export the dataset in the default setting of ray.
        :param keep_stats_in_res_ds: whether to keep stats in the result
            dataset.
        :param keep_hashes_in_res_ds: whether to keep hashes in the result
            dataset.
        """
        self.export_path = export_path
        self.export_shard_size = export_shard_size
        self.keep_stats_in_res_ds = keep_stats_in_res_ds
        self.keep_hashes_in_res_ds = keep_hashes_in_res_ds
        self.export_format = self._get_export_format(export_path) if export_type is None else export_type
        if self.export_format not in self._SUPPORTED_FORMATS:
            raise NotImplementedError(
                f'export data format "{self.export_format}" is not supported '
                f"for now. Only support {self._SUPPORTED_FORMATS}. Please check export_type or export_path."
            )
        self.export_extra_args = kwargs if kwargs is not None else {}
        self.pyarrow_filesystem = None
        self.writer_export_path = export_path

        # Check if export_path is S3 and create filesystem if needed
        self.s3_filesystem = None
        if export_path.startswith("s3://"):
            # Extract AWS credentials from export_extra_args (if provided)
            s3_config = {}
            if "aws_access_key_id" in self.export_extra_args:
                s3_config["aws_access_key_id"] = self.export_extra_args.pop("aws_access_key_id")
            if "aws_secret_access_key" in self.export_extra_args:
                s3_config["aws_secret_access_key"] = self.export_extra_args.pop("aws_secret_access_key")
            if "aws_session_token" in self.export_extra_args:
                s3_config["aws_session_token"] = self.export_extra_args.pop("aws_session_token")
            if "aws_region" in self.export_extra_args:
                s3_config["aws_region"] = self.export_extra_args.pop("aws_region")
            if "endpoint_url" in self.export_extra_args:
                s3_config["endpoint_url"] = self.export_extra_args.pop("endpoint_url")

            # Create PyArrow S3FileSystem with credentials
            # This matches the pattern used in RayS3DataLoadStrategy
            from data_juicer.utils.s3_utils import create_pyarrow_s3_filesystem

            self.s3_filesystem = create_pyarrow_s3_filesystem(s3_config)
            logger.info(f"Detected S3 export path: {export_path}. S3 filesystem configured.")

        if export_path.startswith("hdfs://"):
            filesystem_type = self.export_extra_args.pop("filesystem", None)
            storage_options = self.export_extra_args.pop("webhdfs", None)
            mode = self.export_extra_args.pop("mode", None)
            mode = self._resolve_hdfs_output_mode(mode)
            self.pyarrow_filesystem, self.writer_export_path = get_pyarrow_filesystem(
                export_path,
                filesystem=filesystem_type,
                storage_options=storage_options,
            )
            self._apply_hdfs_output_mode(self.pyarrow_filesystem, self.writer_export_path, export_path, mode)
            if self.export_format in {"json", "jsonl"}:
                self.export_extra_args["_use_arrow_jsonl_datasink"] = True
            if mode == "append":
                self.export_extra_args.setdefault(
                    "filename_provider",
                    _AppendFilenameProvider(
                        f"dj_append_{uuid.uuid4().hex}",
                        self._filename_provider_format(self.export_format),
                    ),
                )
                logger.warning(
                    "Ray HDFS distributed export is running with `mode=append`. "
                    "Append is at-least-once: retry or rerun may produce duplicate part files."
                )
            logger.info(f"Detected HDFS export path: {export_path}. HDFS filesystem configured.")

        self.max_shard_size_str = ""

        # get the string format of shard size
        self.max_shard_size_str = byte_size_to_size_str(self.export_shard_size)

        # we recommend users to set a shard size between MiB and TiB.
        if 0 < self.export_shard_size < Sizes.MiB:
            logger.warning(
                f"The export_shard_size [{self.max_shard_size_str}]"
                f" is less than 1MiB. If the result dataset is too "
                f"large, there might be too many shard files to "
                f"generate."
            )
        if self.export_shard_size >= Sizes.TiB:
            logger.warning(
                f"The export_shard_size [{self.max_shard_size_str}]"
                f" is larger than 1TiB. It might generate large "
                f"single shard file and make loading and exporting "
                f"slower."
            )

    def _get_export_format(self, export_path):
        """
        Get the suffix of export path and check if it's supported.
        We only support ["jsonl", "json", "parquet"] for now.

        :param export_path: the path to export datasets.
        :return: the export data format.
        """
        suffix = os.path.splitext(export_path)[-1].strip(".")
        if not suffix:
            logger.warning(
                f'export_path "{export_path}" does not have a suffix. '
                f'We will use "jsonl" as the default export type.'
            )
            suffix = "jsonl"

        export_format = suffix
        return export_format

    def _export_impl(self, dataset, export_path, columns=None):
        """
        Export a dataset to specific path.

        :param dataset: the dataset to export.
        :param export_path: the path to export the dataset.
        :param columns: the columns to export.
        :return:
        """
        # Handle empty dataset case - Ray returns None for columns() on empty datasets.
        # In Ray Data checkpoint mode, avoid columns(fetch_if_missing=True) because
        # it can execute a Limit[1] action before the sink write.
        checkpoint_enabled = _is_ray_data_checkpoint_enabled()
        if checkpoint_enabled:
            cols = columns if columns is not None else _dataset_columns_no_fetch(dataset)
        else:
            cols = dataset.columns()
        if cols is None:
            if checkpoint_enabled:
                logger.warning(
                    "Dataset schema is unknown while Ray Data checkpointing is enabled; "
                    "exporting without eager column pruning."
                )
                cols = []
            else:
                # Empty dataset with unknown schema - create an empty file
                logger.warning(f"Dataset is empty, creating empty export file at {export_path}")
                os.makedirs(os.path.dirname(export_path) or ".", exist_ok=True)
                with open(export_path, "w"):
                    pass  # Create empty file
                return

        # Use provided columns or infer from dataset
        feature_fields = columns if columns is not None else cols
        removed_fields = []
        if not self.keep_stats_in_res_ds:
            feature_field_set = set(feature_fields)
            removed_fields.extend([field for field in DATA_JUICER_INTERNAL_FIELDS if field in feature_field_set])
        if not self.keep_hashes_in_res_ds:
            extra_fields = {
                HashKeys.hash,
                HashKeys.minhash,
                HashKeys.simhash,
                HashKeys.imagehash,
                HashKeys.videohash,
            }
            removed_fields.extend(list(extra_fields.intersection(feature_fields)))

        if len(removed_fields):
            dataset = dataset.drop_columns(removed_fields)

        export_method = RayExporter._router()[self.export_format]
        export_kwargs = {
            "export_extra_args": self.export_extra_args,
            "export_format": self.export_format,
        }
        # Add S3 filesystem if available
        if self.s3_filesystem is not None:
            export_kwargs["export_extra_args"]["filesystem"] = self.s3_filesystem
        if self.export_shard_size > 0:
            # compute the min_rows_per_file for export methods
            dataset_nbytes = dataset.size_bytes()
            dataset_num_rows = dataset.count()
            num_shards = int(dataset_nbytes / self.export_shard_size) + 1
            num_shards = min(num_shards, dataset_num_rows)
            rows_per_file = int(dataset_num_rows / num_shards)
            export_kwargs["export_extra_args"]["min_rows_per_file"] = rows_per_file

        if self.pyarrow_filesystem is not None:
            export_kwargs["export_extra_args"]["filesystem"] = self.pyarrow_filesystem

        # Ensure export directory exists (Ray's write_json treats export_path as a directory).
        if self.s3_filesystem is None and self.pyarrow_filesystem is None:
            os.makedirs(export_path, exist_ok=True)

        return export_method(dataset, export_path, **export_kwargs)

    def export(self, dataset, columns=None):
        """
        Export method for a dataset.

        :param dataset: the dataset to export.
        :param columns: the columns to export.
        :return:
        """
        self._export_impl(dataset, self.writer_export_path, columns)

    @staticmethod
    def _resolve_hdfs_output_mode(mode):
        mode = mode or "error_if_exists"
        if mode not in {"error_if_exists", "overwrite", "append"}:
            raise ValueError("`export.mode` for Ray HDFS export must be one of error_if_exists, overwrite, append.")
        return mode

    @staticmethod
    def _filename_provider_format(export_format):
        if export_format in {"json", "jsonl"}:
            return "json"
        return export_format

    @classmethod
    def _apply_hdfs_output_mode(cls, filesystem, path: str, original_uri: str, mode: str) -> None:
        from pyarrow.fs import FileType

        file_info = filesystem.get_file_info(path)
        if mode == "error_if_exists":
            if file_info.type is not FileType.NotFound:
                raise FileExistsError(
                    f"Ray HDFS export path already exists: {original_uri}. "
                    "Set `export.mode: overwrite` to replace it or `export.mode: append` to append."
                )
            return

        if mode == "overwrite" and file_info.type is not FileType.NotFound:
            if file_info.type is FileType.Directory:
                filesystem.delete_dir(path)
            else:
                filesystem.delete_file(path)

    @staticmethod
    def write_json(dataset, export_path, **kwargs):
        """
        Export method for json/jsonl target files.

        :param dataset: the dataset to export.
        :param export_path: the path to store the exported dataset.
        :param kwargs: extra arguments.
        :return:
        """
        export_extra_args = kwargs.get("export_extra_args", {})
        if export_extra_args.pop("_use_arrow_jsonl_datasink", False):
            return RayExporter.write_jsonl_datasink(dataset, export_path, export_extra_args)
        filtered_kwargs = filter_arguments(dataset.write_json, export_extra_args)
        # Add S3 filesystem if available
        if "filesystem" in export_extra_args:
            filtered_kwargs["filesystem"] = export_extra_args["filesystem"]
        return dataset.write_json(export_path, force_ascii=False, **filtered_kwargs)

    @staticmethod
    def write_jsonl_datasink(dataset, export_path, export_extra_args):
        ray_remote_args = export_extra_args.pop("ray_remote_args", None)
        concurrency = export_extra_args.pop("concurrency", None)
        open_stream_args = export_extra_args.pop("arrow_open_stream_args", None)
        min_rows_per_file = export_extra_args.pop("min_rows_per_file", None)
        num_rows_per_file = export_extra_args.pop("num_rows_per_file", None)
        if num_rows_per_file is not None:
            min_rows_per_file = num_rows_per_file
        if export_extra_args.pop("max_rows_per_file", None) is not None:
            logger.warning(
                "`max_rows_per_file` is not supported by the custom HDFS JSONL datasink; "
                "use `min_rows_per_file` or `num_rows_per_file` instead."
            )
        datasink = _JsonlDatasink(
            export_path,
            filesystem=export_extra_args.pop("filesystem", None),
            try_create_dir=export_extra_args.pop("try_create_dir", True),
            open_stream_args=open_stream_args,
            filename_provider=export_extra_args.pop("filename_provider", None),
            min_rows_per_file=min_rows_per_file,
            ensure_ascii=export_extra_args.pop("force_ascii", False),
        )
        export_extra_args.pop("mode", None)
        if export_extra_args:
            logger.warning(f"Ignoring unsupported HDFS JSONL export args: {sorted(export_extra_args)}")
        return dataset.write_datasink(
            datasink,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
        )

    @staticmethod
    def write_webdataset(dataset, export_path, **kwargs):
        """
        Export method for webdataset target files.

        :param dataset: the dataset to export.
        :param export_path: the path to store the exported dataset.
        :param kwargs: extra arguments.
        :return:
        """
        from data_juicer.utils.webdataset_utils import _custom_default_encoder

        # check if we need to reconstruct the customized WebDataset format
        export_extra_args = kwargs.get("export_extra_args", {})
        field_mapping = export_extra_args.get("field_mapping", {})
        if len(field_mapping) > 0:
            reconstruct_func = partial(reconstruct_custom_webdataset_format, field_mapping=field_mapping)
            dataset = dataset.map(reconstruct_func)
        filtered_kwargs = filter_arguments(dataset.write_webdataset, export_extra_args)
        # Add S3 filesystem if available
        if "filesystem" in export_extra_args:
            filtered_kwargs["filesystem"] = export_extra_args["filesystem"]

        return dataset.write_webdataset(export_path, encoder=_custom_default_encoder, **filtered_kwargs)

    @staticmethod
    def write_others(dataset, export_path, **kwargs):
        """
        Export method for other target files.

        :param dataset: the dataset to export.
        :param export_path: the path to store the exported dataset.
        :param kwargs: extra arguments.
        :return:
        """
        export_format = kwargs.get("export_format", "parquet")
        write_method = getattr(dataset, f"write_{export_format}")
        export_extra_args = dict(kwargs.get("export_extra_args", {}))
        if (
            "max_rows_per_file" in export_extra_args
            and "max_rows_per_file" not in inspect.signature(write_method).parameters
        ):
            export_extra_args.pop("max_rows_per_file")
        filtered_kwargs = filter_arguments(write_method, export_extra_args)
        # Add S3 filesystem if available
        if "filesystem" in export_extra_args:
            filtered_kwargs["filesystem"] = export_extra_args["filesystem"]
        return write_method(export_path, **filtered_kwargs)

    # suffix to export method
    @staticmethod
    def _router():
        """
        A router from different suffixes to corresponding export methods.

        :return: A dict router.
        """
        return {
            "jsonl": RayExporter.write_json,
            "json": RayExporter.write_json,
            "webdataset": RayExporter.write_webdataset,
            "parquet": RayExporter.write_others,
            "csv": RayExporter.write_others,
            "tfrecords": RayExporter.write_others,
            "lance": RayExporter.write_others,
        }
