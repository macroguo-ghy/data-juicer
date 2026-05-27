from __future__ import annotations

import os
from functools import partial
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import pyarrow
import ray
from jsonargparse import Namespace
from loguru import logger
from ray.data._internal.util import get_compute_strategy

from data_juicer.core.data import DJDataset
from data_juicer.core.data.schema import Schema
from data_juicer.core.tracer import should_trace_op
from data_juicer.ops import Deduplicator, Filter, Mapper, Pipeline
from data_juicer.ops.base_op import DEFAULT_BATCH_SIZE, OP, TAGGING_OPS
from data_juicer.utils.constant import Fields
from data_juicer.utils.file_utils import is_remote_path
from data_juicer.utils.webdataset_utils import _custom_default_decoder


def _cfg_get(config, key, default=None):
    if config is None:
        return default
    if isinstance(config, dict):
        return config.get(key, default)
    getter = getattr(config, "get", None)
    if callable(getter):
        try:
            return getter(key, default)
        except TypeError:
            try:
                return getter(key)
            except (KeyError, AttributeError):
                return default
    return getattr(config, key, default)


def _schema_field_names_from_config(schema_config):
    schema_config = _cfg_get(schema_config, "fields", schema_config)
    if not isinstance(schema_config, list):
        return None
    names = []
    for field_config in schema_config:
        name = _cfg_get(field_config, "name")
        if name:
            names.append(name)
    return names or None


def _column_names_from_configured_columns(configured_columns):
    if isinstance(configured_columns, dict):
        return list(configured_columns)
    if isinstance(configured_columns, list):
        return configured_columns
    return []


def get_configured_ray_columns(cfg):
    dataset_cfg = _cfg_get(cfg, "dataset")
    configs = _cfg_get(dataset_cfg, "configs", None)
    if configs is None and dataset_cfg:
        configs = [dataset_cfg]

    columns = []
    for ds_config in configs or []:
        configured_columns = _cfg_get(ds_config, "columns")
        for column in _column_names_from_configured_columns(configured_columns):
            if column not in columns:
                columns.append(column)
    if columns:
        return columns

    export_cfg = _cfg_get(cfg, "export")
    return _schema_field_names_from_config(_cfg_get(export_cfg, "schema"))


def _get_ray_data_context():
    try:
        return ray.data.DataContext.get_current()
    except Exception:
        return None


def _is_ray_data_checkpoint_enabled():
    context = _get_ray_data_context()
    return bool(getattr(context, "data_checkpoint_dir", ""))


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


def _dataset_arrow_schema(dataset, *, fetch_if_missing=True):
    try:
        schema = dataset.schema(fetch_if_missing=fetch_if_missing)
    except TypeError:
        try:
            schema = dataset.schema()
        except Exception:
            return None
    except Exception:
        return None
    return getattr(schema, "base_schema", schema)


def _is_path_like_arrow_type(field_type):
    if pyarrow.types.is_string(field_type) or pyarrow.types.is_large_string(field_type):
        return True
    if pyarrow.types.is_list(field_type) or pyarrow.types.is_large_list(field_type):
        value_type = field_type.value_type
        return pyarrow.types.is_string(value_type) or pyarrow.types.is_large_string(value_type)
    return False


def _configured_media_field_type(cfg, key):
    export_cfg = _cfg_get(cfg, "export")
    schema_config = _cfg_get(export_cfg, "schema")
    fields_config = _cfg_get(schema_config, "fields", schema_config)
    if not isinstance(fields_config, list):
        return None
    for field_config in fields_config:
        if _cfg_get(field_config, "name") == key:
            field_type = _cfg_get(field_config, "type")
            return str(field_type).strip().lower() if field_type is not None else None
    return None


def _is_configured_path_like_type(field_type):
    if field_type in {"string", "str", "large_string"}:
        return True
    if field_type in {"binary", "bytes", "large_binary"}:
        return False
    if field_type.startswith("list<") and field_type.endswith(">"):
        return _is_configured_path_like_type(field_type[len("list<") : -1].strip())
    return None


def _path_keys_from_media_columns(dataset, columns, cfg):
    schema = None
    schema_loaded = False
    path_keys = []
    for key in [
        cfg.get("video_key", "videos"),
        cfg.get("image_key", "images"),
        cfg.get("audio_key", "audios"),
    ]:
        if key not in columns:
            continue
        configured_type = _configured_media_field_type(cfg, key)
        configured_path_like = _is_configured_path_like_type(configured_type) if configured_type else None
        if configured_path_like is True:
            path_keys.append(key)
            continue
        if configured_path_like is False:
            logger.debug(
                "Skip absolute-path conversion for non-path media column {} from configured type {}",
                key,
                configured_type,
            )
            continue
        if not schema_loaded:
            schema = _dataset_arrow_schema(dataset, fetch_if_missing=not _is_ray_data_checkpoint_enabled())
            schema_loaded = True
        if schema is None:
            path_keys.append(key)
            continue
        field_index = schema.get_field_index(key)
        if field_index < 0:
            continue
        field_type = schema.field(field_index).type
        if _is_path_like_arrow_type(field_type):
            path_keys.append(key)
        else:
            logger.debug("Skip absolute-path conversion for non-path media column {} with type {}", key, field_type)
    return path_keys


def get_abs_path(path, dataset_dir):
    if not isinstance(path, str):
        return path
    if is_remote_path(path):
        return path
    path = os.path.join(dataset_dir, path)
    if is_remote_path(path):
        return path
    full_path = os.path.abspath(path)
    if os.path.exists(full_path):
        return full_path
    else:
        return path


def convert_to_absolute_paths(samples: pyarrow.Table, dataset_dir, path_keys):
    for key in path_keys:
        col_idx = samples.schema.get_field_index(key)
        cols = samples.column(col_idx)

        def _process_paths():
            for col in cols:
                path = col.as_py()
                if isinstance(path, str):
                    yield get_abs_path(path, dataset_dir)
                elif isinstance(path, list):
                    yield [get_abs_path(p, dataset_dir) for p in path]
                else:
                    yield path

        samples = samples.set_column(col_idx, key, pyarrow.array(_process_paths()))
    return samples


# TODO: check path for nestdataset
def set_dataset_to_absolute_path(dataset, dataset_path, cfg):
    """
    Set all the path in input data to absolute path.
    Checks dataset_dir and project_dir for valid paths.
    """
    if bool(_cfg_get(cfg, "ray_dry_run_plan", False)):
        columns = get_configured_ray_columns(cfg) or _dataset_columns_no_fetch(dataset)
    elif _is_ray_data_checkpoint_enabled():
        columns = get_configured_ray_columns(cfg) or _dataset_columns_no_fetch(dataset)
    else:
        columns = get_configured_ray_columns(cfg) or _dataset_columns_no_fetch(dataset)
        if columns is None:
            columns = dataset.columns()
    if columns is None:
        return dataset
    path_keys = _path_keys_from_media_columns(dataset, columns, cfg)
    if len(path_keys) > 0:
        dataset_dir = os.path.dirname(dataset_path)
        logger.info(f"dataset_dir: {dataset_dir}")
        dataset = dataset.map_batches(
            partial(convert_to_absolute_paths, dataset_dir=dataset_dir, path_keys=path_keys),
            batch_format="pyarrow",
            zero_copy_batch=True,
            batch_size=DEFAULT_BATCH_SIZE,
        )
    return dataset


def preprocess_dataset(dataset: ray.data.Dataset, dataset_path, cfg) -> ray.data.Dataset:
    if dataset_path:
        dataset = set_dataset_to_absolute_path(dataset, dataset_path, cfg)
    return dataset


def filter_batch(batch, filter_func):
    mask = pyarrow.array(filter_func(batch.to_pydict()))
    return batch.filter(mask)


def _dict_batch_to_arrow_table_preserving_schema(batch, input_schema):
    arrays = []
    fields = []
    for key, values in batch.items():
        field = None
        if input_schema is not None:
            field_index = input_schema.get_field_index(key)
            if field_index >= 0:
                field = input_schema.field(field_index)
        if field is not None and pyarrow.types.is_struct(field.type):
            struct_field_names = {child.name for child in field.type}
            if any(
                isinstance(value, dict) and any(child_key not in struct_field_names for child_key in value)
                for value in values
            ):
                field = None
        try:
            array = pyarrow.array(values, type=field.type if field is not None else None)
        except (pyarrow.ArrowInvalid, pyarrow.ArrowTypeError, TypeError, ValueError):
            array = pyarrow.array(values)
            field = None
        arrays.append(array)
        fields.append(field if field is not None else pyarrow.field(key, array.type))
    return pyarrow.Table.from_arrays(arrays, schema=pyarrow.schema(fields))


def process_mapper_batch_preserving_schema(batch, process_func):
    input_schema = batch.schema if isinstance(batch, pyarrow.Table) else None
    output = process_func(batch)
    if input_schema is None or isinstance(output, pyarrow.Table) or not isinstance(output, dict):
        return output
    return _dict_batch_to_arrow_table_preserving_schema(output, input_schema)


def make_named_mapper_batch_fn(op_name, process_func):
    def mapper_batch_fn(batch):
        return process_mapper_batch_preserving_schema(batch, process_func=process_func)

    mapper_batch_fn.__name__ = op_name
    mapper_batch_fn.__qualname__ = op_name
    return mapper_batch_fn


class RayDataset(DJDataset):
    def __init__(
        self,
        dataset: ray.data.Dataset,
        dataset_path: str = None,
        cfg: Optional[Namespace] = None,
        auto_op_parallelism=True,
        row_count: int | None = None,
        row_count_getter: Callable[[], int | None] | None = None,
    ) -> None:
        self.cfg = cfg
        self.data = preprocess_dataset(dataset, dataset_path, cfg)
        self._cached_row_count = row_count
        self._row_count_getter = row_count_getter

        # if auto_op_parallelism is set in both args and cfg, cfg takes precedence
        if cfg and cfg.get("auto_op_parallelism") is not None:
            self._auto_proc = cfg.get("auto_op_parallelism")
        else:
            self._auto_proc = auto_op_parallelism

    def schema(self) -> Schema:
        """Get dataset schema.

        Returns:
            Schema: Dataset schema containing column names and types
        """
        if self.data is None or self.data.columns() is None:
            raise ValueError("Dataset is empty or not initialized")

        return Schema.from_ray_schema(self.data.schema())

    def get(self, k: int) -> List[Dict[str, Any]]:
        """Get k rows from the dataset."""
        if k < 0:
            raise ValueError(f"k must be non-negative, got {k}")

        if k == 0:
            return []

        k = min(k, self.count())
        return list(self.data.limit(k).take())

    def get_column(self, column: str, k: Optional[int] = None) -> List[Any]:
        """Get column values from Ray dataset.

        Args:
            column: Name of the column to retrieve
            k: Optional number of rows to return. If None, returns all rows

        Returns:
            List of values from the specified column

        Raises:
            KeyError: If column doesn't exist
            ValueError: If k is negative
        """
        if self.data is None or self.data.columns() is None or column not in self.data.columns():
            raise KeyError(f"Column '{column}' not found in dataset")

        if k is not None:
            if k < 0:
                raise ValueError(f"k must be non-negative, got {k}")
            if k == 0:
                return []
            k = min(k, self.count())
            return [row[column] for row in self.data.limit(k).take()]

        return [row[column] for row in self.data.take()]

    def process(
        self,
        operators,
        *,
        exporter=None,
        checkpointer=None,
        tracer=None,
        plan_only: bool = False,
        materialize_after_each_op: bool | None = None,
    ) -> DJDataset:
        if operators is None:
            return self
        if not isinstance(operators, list):
            operators = [operators]
        if operators:
            self._cached_row_count = None
            self._row_count_getter = None

        from data_juicer.utils.process_utils import calculate_ray_np

        if self._auto_proc:
            calculate_ray_np(operators)

        if materialize_after_each_op is None:
            materialize_after_each_op = bool(_cfg_get(self.cfg, "ray_materialize_after_each_op", False))
        checkpoint_enabled = _is_ray_data_checkpoint_enabled()

        # Cache columns once at start to avoid breaking pipeline with repeated columns() calls.
        # Ray's count() and columns(fetch_if_missing=True) can force execution of lazy reads,
        # so prefer configured schema or no-fetch Ray schema before falling back.
        if plan_only:
            columns_result = get_configured_ray_columns(self.cfg) or _dataset_columns_no_fetch(self.data) or []
        elif checkpoint_enabled:
            columns_result = get_configured_ray_columns(self.cfg) or _dataset_columns_no_fetch(self.data)
        else:
            columns_result = get_configured_ray_columns(self.cfg) or _dataset_columns_no_fetch(self.data)
            if columns_result is None:
                columns_result = self.data.columns()
        # Handle empty dataset case where columns() returns None
        if columns_result is None:
            if checkpoint_enabled:
                logger.warning(
                    "Dataset schema is unknown while Ray Data checkpointing is enabled; "
                    "continuing without eager schema fetch."
                )
                columns_result = []
            else:
                logger.warning("Dataset has unknown schema (likely empty), skipping operator processing")
                return self

        cached_columns = set(columns_result)

        for op in operators:
            original_data = self.data
            if (
                not plan_only
                and type(op).before_operator_started is not OP.before_operator_started
            ):
                op.before_operator_started(
                    dataset=self,
                    context={
                        "executor_type": "ray",
                        "op_name": op._name,
                    },
                )
            try:
                cached_columns = self._run_single_op_with_optional_materialize(
                    op,
                    cached_columns,
                    tracer=tracer,
                    plan_only=plan_only,
                    materialize_after_each_op=materialize_after_each_op,
                )
            except Exception as e:
                logger.error(f"Error processing operator {op}: {e}.")
                if op.runtime_env is not None:
                    logger.error("Try to fallback to the base runtime environment.")
                    original_runtime_env = op.runtime_env
                    try:
                        self.data = original_data
                        op.runtime_env = None
                        cached_columns = self._run_single_op_with_optional_materialize(
                            op,
                            cached_columns,
                            tracer=tracer,
                            plan_only=plan_only,
                            materialize_after_each_op=materialize_after_each_op,
                        )
                    except Exception as fallback_e:
                        self._call_after_operator_finished(op, error=fallback_e)
                        raise
                    finally:
                        op.runtime_env = original_runtime_env
                else:
                    self._call_after_operator_finished(op, error=e)
                    raise e
            if (
                materialize_after_each_op
                and not plan_only
                and type(op).after_operator_finished is not OP.after_operator_finished
            ):
                self._call_after_operator_finished(op, error=None)
        return self

    def _call_after_operator_finished(self, op, error=None):
        if type(op).after_operator_finished is OP.after_operator_finished:
            return
        try:
            op.after_operator_finished(
                dataset=self,
                context={
                    "executor_type": "ray",
                    "op_name": op._name,
                },
                error=error,
            )
        except Exception as hook_exc:
            logger.warning(
                "Failed to run after_operator_finished hook for Op [{}]: {}",
                op._name,
                hook_exc,
            )

    def _run_single_op_with_optional_materialize(
        self,
        op,
        cached_columns=None,
        tracer=None,
        plan_only: bool = False,
        materialize_after_each_op: bool = False,
    ):
        cached_columns = self._run_single_op(op, cached_columns, tracer=tracer, plan_only=plan_only)
        if materialize_after_each_op and not plan_only:
            logger.info(f"Materializing Ray Dataset after Op [{op._name}]...")
            self.data = self.data.materialize()
        return cached_columns

    def _run_single_op(self, op, cached_columns=None, tracer=None, plan_only: bool = False):
        # Use cached columns to avoid calling self.data.columns() which breaks pipeline
        if cached_columns is None:
            cached_columns = set(self.data.columns())

        if op._name in TAGGING_OPS.modules and Fields.meta not in cached_columns:

            def process_batch_arrow(table: pyarrow.Table):
                new_column_data = [{} for _ in range(len(table))]
                new_table = table.append_column(Fields.meta, [new_column_data])
                return new_table

            self.data = self.data.map_batches(
                process_batch_arrow, batch_format="pyarrow", batch_size=DEFAULT_BATCH_SIZE
            )
            cached_columns.add(Fields.meta)

        try:
            batch_size = getattr(op, "batch_size", 1) if op.is_batched_op() else 1
            if isinstance(op, Mapper):
                prepare_ray_dataset = getattr(op, "prepare_ray_dataset", None)
                if callable(prepare_ray_dataset):
                    self.data = prepare_ray_dataset(self.data)

                # Wrap process method with tracer for sample-level collection
                original_process = None
                if tracer and should_trace_op(tracer, op._name):
                    from data_juicer.ops.base_op import wrap_mapper_with_tracer

                    original_process = op.process
                    op.process = wrap_mapper_with_tracer(original_process, op._name, op.text_key, tracer, True)

                try:
                    if op.use_ray_actor():
                        compute = get_compute_strategy(op.__class__, concurrency=op.num_proc)
                        self.data = self.data.map_batches(
                            op.__class__,
                            fn_args=None,
                            fn_kwargs=None,
                            fn_constructor_args=op._init_args,
                            fn_constructor_kwargs=op._init_kwargs,
                            batch_size=batch_size,
                            num_cpus=op.num_cpus,
                            num_gpus=op.num_gpus,
                            compute=compute,
                            batch_format="pyarrow",
                            runtime_env=op.runtime_env,
                        )
                    else:
                        process_func = make_named_mapper_batch_fn(op._name, op.process)
                        compute = get_compute_strategy(process_func, concurrency=op.num_proc)
                        self.data = self.data.map_batches(
                            process_func,
                            batch_size=batch_size,
                            batch_format="pyarrow",
                            num_cpus=op.num_cpus,
                            num_gpus=op.num_gpus,
                            compute=compute,
                            runtime_env=op.runtime_env,
                        )
                finally:
                    # Restore original process method
                    if tracer and should_trace_op(tracer, op._name) and original_process:
                        op.process = original_process
            elif isinstance(op, Filter):
                # Use cached_columns instead of self.data.columns() to avoid breaking pipeline
                if Fields.stats not in cached_columns:

                    def process_batch_arrow(table: pyarrow.Table):
                        new_column_data = [{} for _ in range(len(table))]
                        new_talbe = table.append_column(Fields.stats, [new_column_data])
                        return new_talbe

                    self.data = self.data.map_batches(
                        process_batch_arrow, batch_format="pyarrow", batch_size=DEFAULT_BATCH_SIZE
                    )
                    cached_columns.add(Fields.stats)
                if op.use_ray_actor():
                    compute = get_compute_strategy(op.__class__, concurrency=op.num_proc)
                    self.data = self.data.map_batches(
                        op.__class__,
                        fn_args=None,
                        fn_kwargs=None,
                        fn_constructor_args=op._init_args,
                        fn_constructor_kwargs=op._init_kwargs,
                        batch_size=batch_size,
                        num_cpus=op.num_cpus,
                        num_gpus=op.num_gpus,
                        compute=compute,
                        batch_format="pyarrow",
                        runtime_env=op.runtime_env,
                    )
                else:
                    prepare_for_ray_tasks = getattr(op, "prepare_backend_for_ray_tasks", None)
                    if callable(prepare_for_ray_tasks):
                        prepare_for_ray_tasks()
                    compute_stats_func = partial(process_mapper_batch_preserving_schema, process_func=op.compute_stats)
                    compute = get_compute_strategy(compute_stats_func, concurrency=op.num_proc)
                    self.data = self.data.map_batches(
                        compute_stats_func,
                        batch_size=batch_size,
                        batch_format="pyarrow",
                        num_cpus=op.num_cpus,
                        num_gpus=op.num_gpus,
                        compute=compute,
                        runtime_env=op.runtime_env,
                    )
                if op.stats_export_path is not None:
                    self.data.write_json(op.stats_export_path, force_ascii=False)
                # Wrap process method with tracer for sample-level collection
                original_process = None
                if tracer and should_trace_op(tracer, op._name):
                    from data_juicer.ops.base_op import wrap_filter_with_tracer

                    original_process = op.process
                    op.process = wrap_filter_with_tracer(original_process, op._name, tracer, op.is_batched_op())

                try:
                    if op.is_batched_op():
                        # The core computation have been done in compute_stats,
                        # and the filter process only performs simple filtering.
                        # cpu and parallelism are not set here
                        self.data = self.data.map_batches(
                            partial(filter_batch, filter_func=op.process),
                            batch_format="pyarrow",
                            zero_copy_batch=True,
                            batch_size=DEFAULT_BATCH_SIZE,
                            runtime_env=op.runtime_env,
                        )
                    else:
                        self.data = self.data.filter(
                            op.process,
                            runtime_env=op.runtime_env,
                        )
                finally:
                    # Restore original process method
                    if tracer and should_trace_op(tracer, op._name) and original_process:
                        op.process = original_process
            elif isinstance(op, (Deduplicator, Pipeline)):
                run_plan_only = getattr(op, "run_plan_only", None)
                if plan_only and callable(run_plan_only):
                    self.data = run_plan_only(self.data)
                else:
                    set_runtime_context = getattr(op, "set_runtime_context", None)
                    if callable(set_runtime_context):
                        set_runtime_context(cfg=self.cfg)
                    self.data = op.run(self.data)
            else:
                logger.error("Ray executor only support Filter, Mapper, Deduplicator and Pipeline OPs for now")
                raise NotImplementedError
        except:  # noqa: E722
            logger.error(f"An error occurred during Op [{op._name}].")
            import traceback

            traceback.print_exc()
            exit(1)

        return cached_columns

    def count(self) -> int:
        cached_row_count = getattr(self, "_cached_row_count", None)
        if cached_row_count is not None:
            return cached_row_count
        row_count_getter = getattr(self, "_row_count_getter", None)
        if row_count_getter is not None:
            self._row_count_getter = None
            row_count = row_count_getter()
            if row_count is not None:
                self._cached_row_count = row_count
                return row_count
        return self.data.count()

    @classmethod
    def read(cls, data_format: str, paths: Union[str, List[str]]) -> RayDataset:
        if data_format in {"json", "jsonl", "json.gz", "jsonl.gz", "json.zst", "jsonl.zst"}:
            return RayDataset.read_json(paths)
        elif data_format == "webdataset":
            return RayDataset.read_webdataset(paths)
        elif data_format in {
            "parquet",
            "images",
            "parquet_bulk",
            "csv",
            "text",
            "avro",
            "numpy",
            "tfrecords",
            "binary_files",
            "lance",
        }:
            return getattr(ray.data, f"read_{data_format}")(paths)

    @classmethod
    def read_json(cls, paths: Union[str, List[str]]) -> RayDataset:
        # Note: a temp solution for reading json stream
        # TODO: replace with ray.data.read_json_stream once it is available
        import pyarrow.json as js

        try:
            js.open_json
            return read_json_stream(paths)
        except AttributeError:
            return ray.data.read_json(paths)

    @classmethod
    def read_webdataset(cls, paths: Union[str, List[str]]) -> RayDataset:
        return ray.data.read_webdataset(paths, decoder=partial(_custom_default_decoder, format="PIL"))

    def to_list(self) -> list:
        return self.data.to_pandas().to_dict(orient="records")


_JSON_DATASOURCE_BASE = getattr(ray.data.read_api, "ArrowJSONDatasource", None)
if _JSON_DATASOURCE_BASE is None:
    _JSON_DATASOURCE_BASE = ray.data.read_api.JSONDatasource


class JSONStreamDatasource(_JSON_DATASOURCE_BASE):
    """
    A temp Datasource for reading json stream.

    Note:

        Depends on a customized `pyarrow` with `open_json` method.
    """

    def __init__(self, *args, on_bad_files: str = "error", **kwargs):
        if on_bad_files not in {"error", "skip"}:
            raise ValueError("Expected `error` or `skip` for on_bad_files")
        self.on_bad_files = on_bad_files
        super().__init__(*args, **kwargs)

    def _skip_bad_json_file(self, path: str, error: Exception) -> bool:
        if getattr(self, "on_bad_files", "error") != "skip":
            return False
        logger.warning(
            "Skipping bad JSON file {} due to on_bad_files=skip. Error: {}",
            path,
            error,
        )
        return True

    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        # Check if open_json is available (PyArrow 20.0.0+)
        try:
            from pyarrow.json import open_json
        except ImportError:
            # Fall back to read_json for older PyArrow versions
            # This will read the entire file into memory, but works with older PyArrow
            import pyarrow.json as js

            try:
                # Read the entire file as a table
                table = js.read_json(f, **self.arrow_json_args)
                if table.num_rows > 0:
                    yield table
            except Exception as e:
                if self._skip_bad_json_file(path, e):
                    return
                raise ValueError(f"Failed to read JSON file: {path}. Error: {e}") from e
            return

        if getattr(self, "on_bad_files", "error") == "skip":
            try:
                reader = open_json(
                    f,
                    read_options=self.read_options,
                    **self.arrow_json_args,
                )
                schema = None
                tables = []
                while True:
                    try:
                        batch = reader.read_next_batch()
                        table = pyarrow.Table.from_batches([batch], schema=schema)
                        if schema is None:
                            schema = table.schema
                        tables.append(table)
                    except StopIteration:
                        break
            except pyarrow.lib.ArrowInvalid as e:
                if self._skip_bad_json_file(path, e):
                    return
                raise ValueError(f"Failed to read JSON file: {path}.") from e

            for table in tables:
                yield table
            return

        try:
            reader = open_json(
                f,
                read_options=self.read_options,
                **self.arrow_json_args,
            )
            schema = None
            while True:
                try:
                    batch = reader.read_next_batch()
                    table = pyarrow.Table.from_batches([batch], schema=schema)
                    if schema is None:
                        schema = table.schema
                    yield table
                except StopIteration:
                    return
        except pyarrow.lib.ArrowInvalid as e:
            raise ValueError(f"Failed to read JSON file: {path}.") from e


def read_json_stream(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider=None,
    partition_filter=None,
    partitioning=ray.data.read_api.Partitioning("hive"),
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = ["json", "jsonl", "json.gz", "jsonl.gz", "json.zst", "jsonl.zst"],
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    on_bad_files: str = "error",
    **arrow_json_args,
) -> ray.data.Dataset:
    if on_bad_files not in {"error", "skip"}:
        raise ValueError("Expected `error` or `skip` for on_bad_files")

    # Check if open_json is available (PyArrow 20.0.0+)
    # If not, fall back to ray.data.read_json which works with older PyArrow
    try:
        import pyarrow.json as js

        js.open_json  # Check if attribute exists
    except (ImportError, AttributeError):
        # Fall back to standard ray.data.read_json for older PyArrow versions
        # This works with filesystem parameter for S3
        if on_bad_files == "error":
            return ray.data.read_json(paths, filesystem=filesystem)

    if meta_provider is None:
        meta_provider = ray.data.read_api.DefaultFileMetadataProvider()

    datasource = JSONStreamDatasource(
        paths,
        arrow_json_args=arrow_json_args,
        filesystem=filesystem,
        open_stream_args=arrow_open_stream_args,
        meta_provider=meta_provider,
        partition_filter=partition_filter,
        partitioning=partitioning,
        ignore_missing_paths=ignore_missing_paths,
        shuffle=shuffle,
        include_paths=include_paths,
        file_extensions=file_extensions,
        on_bad_files=on_bad_files,
    )
    return ray.data.read_datasource(
        datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )
