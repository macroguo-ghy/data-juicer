import fnmatch
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional, Type
from urllib.parse import urlparse

import datasets
import pyarrow as pa
from jsonargparse import Namespace
from loguru import logger

from data_juicer.core.data import DJDataset
from data_juicer.core.data.config_validator import ConfigValidationError, ConfigValidator
from data_juicer.core.io_utils import (
    copy_uri_to_local,
    export_lark_sheet_to_local,
    get_pyarrow_filesystem,
    get_webhdfs_pyarrow_filesystem,
    infer_local_name_from_uri,
    make_staging_dir,
    materialize_duckdb_query,
    parse_lark_sheet_location,
    read_magnus_to_pandas,
    read_magnus_to_ray,
    run_tqs_query,
    run_tqs_query_to_records,
)
from data_juicer.download.downloader import validate_snapshot_format
from data_juicer.format.formatter import unify_format
from data_juicer.format.load import load_formatter
from data_juicer.utils.s3_utils import create_pyarrow_s3_filesystem, validate_s3_path

# based on executor type and data source type, use different
# data load strategy to product corresponding datasets
# DJDataset, RayDataset, DaskDataset, etc

_RAY_PARQUET_READ_KWARGS = {
    "columns",
    "parallelism",
    "num_cpus",
    "num_gpus",
    "memory",
    "ray_remote_args",
    "tensor_column_schema",
    "partition_filter",
    "partitioning",
    "shuffle",
    "include_paths",
    "file_extensions",
    "concurrency",
    "override_num_blocks",
}

_RAY_READ_RESOURCE_KWARGS = ("num_cpus", "num_gpus", "memory")

_RAY_JSON_READ_KWARGS = {
    "parallelism",
    "ray_remote_args",
    "arrow_open_stream_args",
    "meta_provider",
    "partition_filter",
    "partitioning",
    "include_paths",
    "ignore_missing_paths",
    "shuffle",
    "file_extensions",
    "concurrency",
    "override_num_blocks",
    "read_options",
    "parse_options",
}

_RAY_HDFS_PARQUET_FORMATS = {"parquet", ".parquet"}
_RAY_HDFS_JSON_FORMATS = {
    "json",
    ".json",
    "jsonl",
    ".jsonl",
    "json.gz",
    ".json.gz",
    "jsonl.gz",
    ".jsonl.gz",
    "json.zst",
    ".json.zst",
    "jsonl.zst",
    ".jsonl.zst",
}


def _validate_ray_hdfs_filesystem(filesystem_type: str) -> None:
    if filesystem_type not in {"pyarrow", "webhdfs"}:
        raise ValueError(
            f"Unsupported Ray HDFS filesystem [{filesystem_type}]. "
            "Expected `pyarrow` or `webhdfs`."
        )


def _validate_hdfs_uri(uri: str) -> None:
    if not isinstance(uri, str) or not uri.startswith("hdfs://"):
        raise ValueError(f"Expected an HDFS URI starting with `hdfs://`, got [{uri}].")


def _validate_hdfs_path_config(path: str | list[str]) -> None:
    if isinstance(path, str):
        _validate_hdfs_uri(path)
        return
    if not isinstance(path, list) or not path:
        raise ValueError("Expected a non-empty HDFS URI string or list of HDFS URI strings.")
    for uri in path:
        _validate_hdfs_uri(uri)


def _validate_hdfs_paths_share_filesystem(paths: list[str]) -> None:
    if len(paths) <= 1:
        return
    first = urlparse(paths[0])
    for path in paths[1:]:
        parsed = urlparse(path)
        if (parsed.scheme, parsed.netloc) != (first.scheme, first.netloc):
            raise ValueError("All HDFS paths in one Ray HDFS loader must use the same filesystem.")


def _validate_positive_int(value: int) -> None:
    if isinstance(value, bool) or value <= 0:
        raise ValueError("Expected a positive integer")


def _with_ray_loader_limit_validation(base_rules: dict) -> dict:
    rules = {
        "required_fields": list(base_rules.get("required_fields", [])),
        "optional_fields": list(base_rules.get("optional_fields", [])),
        "field_types": dict(base_rules.get("field_types", {})),
        "custom_validators": dict(base_rules.get("custom_validators", {})),
    }
    for field in ["limit", "materialize_after_limit"]:
        if field not in rules["optional_fields"]:
            rules["optional_fields"].append(field)
    rules["field_types"].update(
        {
            "limit": int,
            "materialize_after_limit": bool,
        }
    )
    rules["custom_validators"]["limit"] = _validate_positive_int
    return rules


def _validate_on_bad_files(value: str) -> None:
    if value not in {"error", "skip"}:
        raise ValueError("Expected `error` or `skip`")


def _is_countable_parquet_metadata_file(path: str) -> bool:
    name = os.path.basename(path)
    if not name or name.startswith(("_", ".")):
        return False
    path_parts = path.replace("\\", "/").split("/")
    if any(part.startswith(("_", ".")) for part in path_parts[:-1]):
        return False
    return True


def _is_zero_byte_parquet_file(filesystem, path: str) -> bool:
    try:
        file_info = filesystem.get_file_info(path)
        return file_info.size == 0
    except Exception:
        return False


@dataclass(frozen=True)
class _ParquetReadPlan:
    paths: str | list[str]
    schema: pa.Schema | None = None
    row_count: int | None = None
    skipped_empty_file_count: int = 0


def _parquet_files_from_filesystem(filesystem, path: str | list[str]) -> list[str] | None:
    import pyarrow.fs as pa_fs

    if isinstance(path, list):
        parquet_paths = []
        for item_path in path:
            item_parquet_paths = _parquet_files_from_filesystem(filesystem, item_path)
            if item_parquet_paths is None:
                return None
            parquet_paths.extend(item_parquet_paths)
        return parquet_paths

    file_info = filesystem.get_file_info(path)
    if file_info.type == pa_fs.FileType.NotFound:
        return []
    if file_info.type == pa_fs.FileType.File:
        return [path] if _is_countable_parquet_metadata_file(path) else []
    if file_info.type == pa_fs.FileType.Directory:
        selector = pa_fs.FileSelector(path, recursive=True)
        return sorted(
            info.path
            for info in filesystem.get_file_info(selector)
            if info.type == pa_fs.FileType.File and _is_countable_parquet_metadata_file(info.path)
        )
    return None


def _ray_parquet_sample_indices(num_files: int) -> list[int]:
    if num_files <= 0:
        return []
    try:
        from ray.data._internal.datasource import parquet_datasource as ray_parquet

        sample_ratio = ray_parquet.PARQUET_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO
        min_samples = ray_parquet.PARQUET_ENCODING_RATIO_ESTIMATE_MIN_NUM_SAMPLES
        max_samples = ray_parquet.PARQUET_ENCODING_RATIO_ESTIMATE_MAX_NUM_SAMPLES
    except Exception:
        sample_ratio = 0.01
        min_samples = 2
        max_samples = 10

    num_samples = int(num_files * sample_ratio)
    min_num_samples = min(min_samples, num_files)
    max_num_samples = min(max_samples, num_files)
    num_samples = max(min(num_samples, max_num_samples), min_num_samples)
    if num_samples <= 1:
        return [0]
    return [
        int(idx * (num_files - 1) / (num_samples - 1))
        for idx in range(num_samples)
    ]


def _filter_ray_sampled_zero_row_group_files(
    filesystem,
    path: str | list[str],
    parquet_paths: list[str],
) -> _ParquetReadPlan:
    import pyarrow.parquet as pq

    readable_paths = list(parquet_paths)
    metadata_cache = {}
    schema = None
    skipped_empty_file_count = 0
    first_skipped_path = None

    while readable_paths:
        zero_row_group_paths = set()
        for sample_idx in _ray_parquet_sample_indices(len(readable_paths)):
            parquet_path = readable_paths[sample_idx]
            if _is_zero_byte_parquet_file(filesystem, parquet_path):
                zero_row_group_paths.add(parquet_path)
                continue
            metadata = metadata_cache.get(parquet_path)
            if metadata is None:
                metadata = pq.read_metadata(parquet_path, filesystem=filesystem)
                metadata_cache[parquet_path] = metadata
            if schema is None:
                schema = metadata.schema.to_arrow_schema()
            if metadata.num_row_groups == 0:
                zero_row_group_paths.add(parquet_path)

        if not zero_row_group_paths:
            break

        skipped_empty_file_count += len(zero_row_group_paths)
        if first_skipped_path is None:
            first_skipped_path = next(iter(zero_row_group_paths))
        readable_paths = [
            parquet_path
            for parquet_path in readable_paths
            if parquet_path not in zero_row_group_paths
        ]

    if skipped_empty_file_count:
        logger.warning(
            "Skipping {} Ray-sampled parquet file(s) with 0 row groups under {}. "
            "First skipped file: {}",
            skipped_empty_file_count,
            path,
            first_skipped_path,
        )
        return _ParquetReadPlan(
            paths=readable_paths,
            schema=schema,
            skipped_empty_file_count=skipped_empty_file_count,
        )
    return _ParquetReadPlan(paths=path, schema=schema)


def _build_parquet_read_plan_from_filesystem(
    filesystem,
    path: str | list[str],
    *,
    filter_for_ray_sampling_only: bool = False,
    skip_bad_files: bool = False,
    limit: int | None = None,
    allow_empty: bool = True,
) -> _ParquetReadPlan:
    try:
        import pyarrow.parquet as pq

        parquet_paths = _parquet_files_from_filesystem(filesystem, path)
        if parquet_paths is None:
            return _ParquetReadPlan(paths=path)
        if parquet_paths == [] and not allow_empty:
            return _ParquetReadPlan(paths=path)
        if limit is None and filter_for_ray_sampling_only and not skip_bad_files:
            return _filter_ray_sampled_zero_row_group_files(filesystem, path, parquet_paths)

        readable_paths = []
        row_count = 0
        schema = None
        skipped_empty_file_count = 0
        first_skipped_path = None
        first_skipped_error = None

        def record_skipped_file(parquet_path: str, error: str) -> None:
            nonlocal skipped_empty_file_count, first_skipped_path, first_skipped_error
            skipped_empty_file_count += 1
            if first_skipped_path is None:
                first_skipped_path = parquet_path
                first_skipped_error = error

        for parquet_path in parquet_paths:
            if _is_zero_byte_parquet_file(filesystem, parquet_path):
                record_skipped_file(parquet_path, "zero-byte file")
                continue
            try:
                metadata = pq.read_metadata(parquet_path, filesystem=filesystem)
            except Exception as exc:
                if not skip_bad_files:
                    raise
                record_skipped_file(parquet_path, f"{type(exc).__name__}: {exc}")
                continue
            if schema is None:
                schema = metadata.schema.to_arrow_schema()
            row_count += metadata.num_rows
            if metadata.num_row_groups == 0:
                record_skipped_file(parquet_path, "0 row groups")
                continue
            readable_paths.append(parquet_path)
            if limit is not None and row_count >= limit:
                break

        if skipped_empty_file_count:
            if skip_bad_files:
                logger.warning(
                    "Skipping {} bad parquet file(s) under {} due to on_bad_files=skip. "
                    "First skipped file: {}. First error: {}",
                    skipped_empty_file_count,
                    path,
                    first_skipped_path,
                    first_skipped_error,
                )
            else:
                logger.warning(
                    "Skipping {} parquet file(s) with 0 row groups under {}. First skipped file: {}",
                    skipped_empty_file_count,
                    path,
                    first_skipped_path,
                )
            return _ParquetReadPlan(
                paths=readable_paths,
                schema=schema,
                row_count=row_count,
                skipped_empty_file_count=skipped_empty_file_count,
            )
        if limit is not None:
            return _ParquetReadPlan(paths=readable_paths, schema=schema, row_count=row_count)
        return _ParquetReadPlan(paths=path, schema=schema, row_count=row_count)
    except Exception as exc:
        if skip_bad_files:
            raise
        logger.debug("Failed to build parquet read plan from metadata for {}: {}", path, exc)
        return _ParquetReadPlan(paths=path)


def _count_parquet_rows_from_filesystem(filesystem, path: str | list[str]) -> int | None:
    return _build_parquet_read_plan_from_filesystem(filesystem, path).row_count


def _limit_parquet_row_count(row_count: int | None, limit: int | None) -> int | None:
    if row_count is None or limit is None:
        return row_count
    return min(row_count, limit)


def _apply_ray_loader_limit(dataset, limit: int | None, materialize_after_limit: bool, cfg: Namespace):
    if limit is None:
        return dataset

    dataset = dataset.limit(limit)
    if not materialize_after_limit:
        return dataset

    if bool(getattr(cfg, "ray_dry_run_plan", False)):
        logger.info(
            "Skipping `materialize_after_limit` because `ray_dry_run_plan` is enabled."
        )
        return dataset

    logger.info("Materializing Ray Dataset after loader limit={}.", limit)
    return dataset.materialize()


def _apply_ray_dataset_loader_limit(
    ray_dataset,
    limit: int | None,
    materialize_after_limit: bool,
    cfg: Namespace,
):
    if limit is None:
        return ray_dataset

    ray_dataset.data = _apply_ray_loader_limit(
        ray_dataset.data,
        limit,
        materialize_after_limit,
        cfg,
    )
    ray_dataset._cached_row_count = None
    ray_dataset._row_count_getter = None
    return ray_dataset


@dataclass(frozen=True)
class StrategyKey:
    """
    Immutable key for strategy registration with wildcard support
    """

    executor_type: str
    data_type: str
    data_source: str

    def matches(self, other: "StrategyKey") -> bool:
        """
        Check if this key matches another key with wildcard support

        Supports Unix-style wildcards:
        - '*' matches any string
        - '?' matches any single character
        - '[seq]' matches any character in seq
        - '[!seq]' matches any character not in seq
        """
        return (
            fnmatch.fnmatch(other.executor_type, self.executor_type)
            and fnmatch.fnmatch(other.data_type, self.data_type)
            and fnmatch.fnmatch(other.data_source, self.data_source)
        )


@dataclass(frozen=True)
class RayDataCheckpointSupport:
    supported: bool
    reason: str = ""


class DataLoadStrategy(ABC, ConfigValidator):
    """
    abstract class for data load strategy
    """

    def __init__(self, ds_config: Dict, cfg: Namespace):
        self.validate_config(ds_config)
        self.ds_config = ds_config
        self.cfg = cfg
        self.weight = ds_config.get("weight", 1.0)  # default weight is 1.0

    def get_load_kwargs(self, **kwargs) -> Dict:
        merged = dict(kwargs)
        merged.update(self.ds_config.get("load_kwargs", {}))
        return merged

    def get_ray_parquet_read_kwargs(self, load_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        read_kwargs = {
            key: load_kwargs[key]
            for key in _RAY_PARQUET_READ_KWARGS
            if key in load_kwargs
        }
        read_kwargs.update(
            {
                key: self.ds_config[key]
                for key in _RAY_PARQUET_READ_KWARGS
                if key in self.ds_config
            }
        )
        self._move_ray_read_resource_kwargs(read_kwargs)
        return read_kwargs

    @staticmethod
    def _move_ray_read_resource_kwargs(read_kwargs: Dict[str, Any]) -> None:
        resource_kwargs = {}
        for key in _RAY_READ_RESOURCE_KWARGS:
            if key in read_kwargs:
                value = read_kwargs.pop(key)
                if value is not None:
                    resource_kwargs[key] = value
        if not resource_kwargs:
            return

        ray_remote_args = dict(read_kwargs.get("ray_remote_args") or {})
        for key, value in resource_kwargs.items():
            ray_remote_args.setdefault(key, value)
        read_kwargs["ray_remote_args"] = ray_remote_args

    def get_ray_json_read_kwargs(self, load_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        read_kwargs = {
            key: load_kwargs[key]
            for key in _RAY_JSON_READ_KWARGS
            if key in load_kwargs
        }
        read_kwargs.update(
            {
                key: self.ds_config[key]
                for key in _RAY_JSON_READ_KWARGS
                if key in self.ds_config
            }
        )
        return read_kwargs

    def get_ray_data_checkpoint_support(self) -> RayDataCheckpointSupport:
        return RayDataCheckpointSupport(
            False,
            "loader does not declare Ray Data checkpoint support",
        )

    @abstractmethod
    def load_data(self, **kwargs) -> DJDataset:
        """Need to be implemented in the"""


class DataLoadStrategyRegistry:
    """
    Flexible strategy registry with wildcard matching
    """

    _strategies: Dict[StrategyKey, Type[DataLoadStrategy]] = {}

    @classmethod
    def get_strategy_class(
        cls, executor_type: str, data_type: str, data_source: str
    ) -> Optional[Type[DataLoadStrategy]]:
        """
        Retrieve the most specific matching strategy

        Matching priority:
        1. Exact match
        2. Wildcard matches from most specific to most general
        """
        logger.info(
            f"Getting strategy class for "
            f"exec: {executor_type}, "
            f"data_type: {data_type}, "
            f"data_source: {data_source}"
        )

        # default to wildcard if not provided
        executor_type = executor_type or "*"
        data_type = data_type or "*"
        data_source = data_source or "*"

        # Create the lookup key
        lookup_key = StrategyKey(executor_type, data_type, data_source)

        # First, check for exact match
        exact_match = cls._strategies.get(lookup_key)
        if exact_match:
            return exact_match

        # Find all matching wildcard strategies
        matching_strategies = []
        for registered_key, strategy in cls._strategies.items():
            if registered_key.matches(lookup_key):
                matching_strategies.append((registered_key, strategy))

        # Sort matching strategies by specificity (fewer wildcards first)
        if matching_strategies:

            def specificity_score(key: StrategyKey) -> int:
                """
                Calculate specificity score (lower is more specific)
                Exact match: 0
                One wildcard: 1
                Two wildcards: 2
                All wildcards: 3
                """
                return sum(1 for part in [key.executor_type, key.data_type, key.data_source] if part == "*")

            matching_strategies.sort(key=lambda x: specificity_score(x[0]))
            found = matching_strategies[0][1]
            logger.info(f"Found matching strategies: {found}")
            return found

        # No matching strategy found
        logger.warning(
            f"No matching strategy found for combination "
            f"exec: {executor_type}, "
            f"data_type: {data_type}, "
            f"data_source: {data_source}"
        )
        return None

    @classmethod
    def register(cls, executor_type: str, data_type: str, data_source: str):
        """
        Decorator for registering data load strategies with wildcard support

        :param executor_type: Type of executor (e.g., 'default', 'ray')
        :param data_type: Type of data (e.g., 'local', 'remote')
        :param data_source: Specific data source (e.g., 'arxiv', 's3')
        :return: Decorator function
        """

        def decorator(strategy_class: Type[DataLoadStrategy]):
            """
            Register the strategy class for the given key

            :param strategy_class: Strategy class to register
            :return: Original strategy class
            """
            key = StrategyKey(executor_type, data_type, data_source)
            cls._strategies[key] = strategy_class
            return strategy_class

        return decorator


def _reject_hive_legacy_field(field: str):
    def validator(_value):
        raise ValueError(
            f"`{field}` is not supported by the Ray Hive loader. "
            "Use `table_name` with optional `columns`, `filter`, "
            "`concurrency`, `override_num_blocks`, `ray_remote_args`, "
            "and `arrow_parquet_args`. To cast Hive columns, configure "
            "`columns` as a mapping from column name to Hive type."
        )

    return validator


def _normalize_hive_columns_config(columns_config):
    if columns_config is None:
        return None, {}
    if isinstance(columns_config, list):
        if not columns_config:
            raise ValueError("`columns` must not be empty when configured.")
        for column in columns_config:
            if not isinstance(column, str):
                raise ValueError("`columns` list entries must be column-name strings.")
        return columns_config, {}
    if isinstance(columns_config, dict):
        if not columns_config:
            raise ValueError("`columns` must not be empty when configured.")
        columns = []
        cast_columns = {}
        for column, hive_type in columns_config.items():
            if not isinstance(column, str):
                raise ValueError("`columns` mapping keys must be column-name strings.")
            columns.append(column)
            if hive_type is None:
                continue
            if not isinstance(hive_type, str):
                raise ValueError("`columns` mapping values must be Hive type strings or null.")
            cast_columns[column] = hive_type
        return columns, cast_columns
    raise ValueError(
        "`columns` must be either a list of column names or a mapping of column names to Hive types."
    )


def _validate_hive_columns_config(columns_config):
    _normalize_hive_columns_config(columns_config)


def _get_ray_hive_catalog():
    hive_catalog_cls = _load_ray_hive_catalog_cls()
    return _start_ray_hive_catalog(hive_catalog_cls())


def _load_ray_hive_catalog_cls():
    try:
        from ray.data.datasource.hive import HiveCatalog
    except ImportError as exc:
        raise ImportError(
            "Hive loading requires bytedray Hive support. "
            "Install `bytedray[default,data,serve,bytedance,hive]>=2.10.0.47`."
        ) from exc
    return HiveCatalog


def _start_ray_hive_catalog(catalog):
    start = getattr(catalog, "start", None)
    if callable(start):
        start()
    return catalog


class RayDataLoadStrategy(DataLoadStrategy):
    """
    abstract class for data load strategy for RayExecutor
    """

    @abstractmethod
    def load_data(self, **kwargs) -> DJDataset:
        """Need to be implemented in the"""


class DefaultDataLoadStrategy(DataLoadStrategy):
    """
    abstract class for data load strategy for LocalExecutor
    """

    @abstractmethod
    def load_data(self, **kwargs) -> DJDataset:
        """Need to be implemented in the"""


class StagedLocalLoadMixin:
    def _make_stage_dir(self, identifier: str) -> str:
        return make_staging_dir(self.cfg.work_dir, "load", identifier)

    def _make_stage_path(self, identifier: str, remote_path: str | None = None, default_name: str = "dataset") -> str:
        stage_dir = self._make_stage_dir(identifier)
        local_name = infer_local_name_from_uri(remote_path, default_name=default_name)
        return os.path.join(stage_dir, local_name)


class DefaultStagedRemoteLoadStrategy(StagedLocalLoadMixin, DefaultDataLoadStrategy):
    def _load_staged_local_dataset(self, local_path: str, **kwargs):
        ds_config = dict(self.ds_config)
        ds_config["type"] = "local"
        ds_config["path"] = local_path
        return DefaultLocalDataLoadStrategy(ds_config, self.cfg).load_data(**kwargs)


class RayStagedRemoteLoadStrategy(StagedLocalLoadMixin, RayDataLoadStrategy):
    def get_ray_data_checkpoint_support(self) -> RayDataCheckpointSupport:
        return RayDataCheckpointSupport(
            True,
            "stages data to local files and loads through Ray file datasource",
        )

    def _load_staged_local_dataset(self, local_path: str, **kwargs):
        ds_config = dict(self.ds_config)
        ds_config["type"] = "local"
        ds_config["path"] = local_path
        return RayLocalJsonDataLoadStrategy(ds_config, self.cfg).load_data(**kwargs)


# TODO dask support
# class DaskDataLoadStrategy(DataLoadStrategy):
#     @abstractmethod
#     def load_data(self) -> Union[DaskDataset]:
#         pass

# TODO nemo support
# class NemoDataLoadStrategy(DataLoadStrategy):
#     @abstractmethod
#     def load_data(self) -> Union[NemoDataset]:
#         pass


@DataLoadStrategyRegistry.register("ray", "local", "*")
class RayLocalJsonDataLoadStrategy(RayDataLoadStrategy):
    # TODO ray defaults to json

    CONFIG_VALIDATION_RULES = {"required_fields": ["path"], "field_types": {"path": str}, "custom_validators": {}}

    def get_ray_data_checkpoint_support(self) -> RayDataCheckpointSupport:
        return RayDataCheckpointSupport(
            True,
            "loads local files through Ray file datasource",
        )

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        from data_juicer.core.data.ray_dataset import RayDataset

        path = self.ds_config["path"]

        # Convert to absolute path if relative
        if not os.path.isabs(path):
            # Try multiple base paths
            possible_paths = [
                # Current working directory
                os.path.abspath(path),
                # Original DJ root directory relative to script location
                os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", path)),
                # User's home directory
                os.path.expanduser(os.path.join("~", path)),
            ]

            # Ray work directory
            ray_work_dir = getattr(self.cfg, "work_dir", None) if self.cfg else None
            if ray_work_dir:
                possible_paths.append(os.path.abspath(os.path.join(ray_work_dir, path)))

            # Try each path
            for abs_path in possible_paths:
                if os.path.exists(abs_path):
                    path = abs_path
                    break
            else:
                # No valid path found
                raise FileNotFoundError(
                    f"Could not find file '{path}' in any location. "
                    f"Tried: {possible_paths}. "
                    f"Current working directory: {os.getcwd()}"
                )

        logger.info(f"Using resolved path for loading ray dataset: {path}")

        file_extension_map = {
            ".json": "json",
            ".jsonl": "json",
            ".txt": "text",
            ".csv": "csv",
            ".tsv": "csv",
            ".parquet": "parquet",
            ".npy": "numpy",
            ".tfrecords": "tfrecords",
            ".lance": "lance",
        }
        auto_detect = False
        data_source = self.ds_config.get("source", None)
        if data_source is None:
            auto_detect = True
        else:
            suffix = os.path.splitext(data_source)[1]
            if suffix in file_extension_map:
                data_format = file_extension_map[suffix]
            elif "." + data_source in file_extension_map:
                data_format = file_extension_map["." + data_source]
            else:
                auto_detect = True
        if auto_detect:
            item_path = path
            if os.path.isdir(item_path):
                # The first file encountered in the directory
                # determines which data reader to use.
                path_list = [path]
                not_found = True
                while not_found and len(path_list) > 0:
                    cur_path = path_list.pop()
                    for item in os.listdir(cur_path):
                        item_path = os.path.join(cur_path, item)
                        if os.path.isdir(item_path):
                            path_list.append(item_path)
                        elif os.path.isfile(item_path):
                            not_found = False
                            break
            file_extension = os.path.splitext(item_path)[1]
            # by default, we use json type to load data
            data_format = file_extension_map.get(file_extension, "json")
            logger.info(f"Try to load data as {data_format}.")
        else:
            logger.info(f"Loading {data_format} data.")
        try:
            dataset = RayDataset.read(data_format, path)
            return RayDataset(dataset, dataset_path=path, cfg=self.cfg)
        except Exception as e:
            if auto_detect:
                raise RuntimeError(
                    f"Failed to load data from {path}. "
                    f"Please check data format and set the correct `dataset.configs.source`. "
                    f"Current working directory: {os.getcwd()}. "
                    f"Error: {str(e)}"
                )
            else:
                raise RuntimeError(
                    f"Failed to load {data_format} data from {path}. "
                    f"Current working directory: {os.getcwd()}. "
                    f"Error: {str(e)}"
                )


@DataLoadStrategyRegistry.register("ray", "remote", "huggingface")
class RayHuggingfaceDataLoadStrategy(RayDataLoadStrategy):
    CONFIG_VALIDATION_RULES = {"required_fields": ["path"], "field_types": {"path": str}, "custom_validators": {}}

    def load_data(self, **kwargs):
        raise NotImplementedError("Huggingface data load strategy for Ray is not implemented")


@DataLoadStrategyRegistry.register("default", "local", "*")
class DefaultLocalDataLoadStrategy(DefaultDataLoadStrategy):
    """
    data load strategy for on disk data for LocalExecutor
    rely on AutoFormatter for actual data loading
    """

    CONFIG_VALIDATION_RULES = {"required_fields": ["path"], "field_types": {"path": str}, "custom_validators": {}}

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        # Get config values with defaults
        text_keys = getattr(self.cfg, "text_keys", ["text"])  # Default to ['text']
        suffixes = getattr(self.cfg, "suffixes", None)  # Default to None
        # if there is suffix_filter op, turn on the add_suffix flag
        add_suffix = False
        process_list = self.cfg.process if hasattr(self.cfg, "process") else []
        for op in process_list:
            op_name, _ = list(op.items())[0]
            if op_name == "suffix_filter":
                add_suffix = True
                break
        load_data_np = kwargs.get("num_proc", 1)

        # use proper formatter to load data
        formatter = load_formatter(
            dataset_path=self.ds_config["path"], text_keys=text_keys, suffixes=suffixes, add_suffix=add_suffix, **kwargs
        )
        # TODO more sophiscated localformatter routing
        return formatter.load_dataset(load_data_np, self.cfg)


@DataLoadStrategyRegistry.register("default", "remote", "huggingface")
class DefaultHuggingfaceDataLoadStrategy(DefaultDataLoadStrategy):
    """
    data load strategy for Huggingface dataset for LocalExecutor
    """

    CONFIG_VALIDATION_RULES = {
        "required_fields": ["path"],
        "optional_fields": ["split", "limit", "name", "data_files", "data_dir"],
        "field_types": {"path": str},
        "custom_validators": {},
    }

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        num_proc = kwargs.pop("num_proc", 1)
        ds = datasets.load_dataset(
            self.ds_config["path"],
            split=self.ds_config.get("split", None),
            data_files=self.ds_config.get("data_files", None),
            data_dir=self.ds_config.get("data_dir", None),
            name=self.ds_config.get("name", None),
            limit=self.ds_config.get("limit", None),
            num_proc=num_proc,
            **kwargs,
        )
        return unify_format(ds, text_keys=self.cfg.text_keys, num_proc=num_proc, global_cfg=self.cfg)


@DataLoadStrategyRegistry.register("default", "remote", "modelscope")
class DefaultModelScopeDataLoadStrategy(DefaultDataLoadStrategy):
    """
    data load strategy for ModelScope dataset for LocalExecutor
    """

    def load_data(self, **kwargs):
        raise NotImplementedError("ModelScope data load strategy is not implemented")


@DataLoadStrategyRegistry.register("default", "remote", "arxiv")
class DefaultArxivDataLoadStrategy(DefaultDataLoadStrategy):
    """
    data load strategy for arxiv dataset for LocalExecutor
    """

    CONFIG_VALIDATION_RULES = {
        "required_fields": ["path"],
        "field_types": {"path": (str)},  # has to be a string
        "custom_validators": {},
    }

    def load_data(self, **kwargs):
        raise NotImplementedError("Arxiv data load strategy is not implemented")


@DataLoadStrategyRegistry.register("default", "remote", "wiki")
class DefaultWikiDataLoadStrategy(DefaultDataLoadStrategy):
    """
    data load strategy for wiki dataset for LocalExecutor
    """

    CONFIG_VALIDATION_RULES = {"required_fields": ["path"], "field_types": {"path": str}, "custom_validators": {}}

    def load_data(self, **kwargs):
        raise NotImplementedError("Wiki data load strategy is not implemented")


@DataLoadStrategyRegistry.register("default", "remote", "commoncrawl")
class DefaultCommonCrawlDataLoadStrategy(DefaultDataLoadStrategy):
    """
    data load strategy for commoncrawl dataset for LocalExecutor
    """

    CONFIG_VALIDATION_RULES = {
        "required_fields": ["start_snapshot", "end_snapshot"],
        "optional_fields": ["aws", "url_limit"],
        "field_types": {"start_snapshot": str, "end_snapshot": str},
        "custom_validators": {
            "start_snashot": validate_snapshot_format,
            "end_snapshot": validate_snapshot_format,
            "url_limit": lambda x: x > 0,
        },
    }

    def load_data(self, **kwargs):
        raise NotImplementedError("CommonCrawl data load strategy is not implemented")


@DataLoadStrategyRegistry.register("default", "remote", "s3")
class DefaultS3DataLoadStrategy(DefaultDataLoadStrategy):
    """
    data load strategy for S3 datasets for LocalExecutor
    Uses fsspec/s3fs to access S3 files
    """

    CONFIG_VALIDATION_RULES = {
        "required_fields": ["path"],
        "optional_fields": [
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
            "aws_region",
            "endpoint_url",
        ],
        "field_types": {"path": str},
        "custom_validators": {
            "path": lambda x: x.startswith("s3://"),
        },
    }

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        import os

        import datasets

        from data_juicer.format.formatter import unify_format
        from data_juicer.utils.s3_utils import get_aws_credentials

        path = self.ds_config["path"]
        validate_s3_path(path)

        load_data_np = kwargs.get("num_proc", 1)

        # Get config values with defaults
        text_keys = getattr(self.cfg, "text_keys", ["text"])

        logger.info(f"Loading dataset from S3: {path}")

        # Determine file format from extension (reuse logic from RayLocalJsonDataLoadStrategy)
        file_extension = os.path.splitext(path)[1].lower()
        file_extension_map = {
            ".json": "json",
            ".jsonl": "json",
            ".txt": "text",
            ".csv": "csv",
            ".tsv": "csv",
            ".parquet": "parquet",
        }
        data_format = file_extension_map.get(file_extension, "json")  # Default to json
        logger.info(f"Detected format: {data_format} for S3 path: {path}")

        # Create S3FileSystem with credentials from config
        # Get credentials with priority order (env vars first, then config)
        aws_access_key_id, aws_secret_access_key, aws_session_token, _ = get_aws_credentials(self.ds_config)
        # Region is auto-detected from S3 path for HuggingFace datasets, don't need it from credentials

        # Build storage_options for S3FileSystem
        # Note: region should NOT be in storage_options for HuggingFace datasets
        # as it causes issues with AioSession. Region is auto-detected from S3 path.
        storage_options = {}
        if aws_access_key_id:
            storage_options["key"] = aws_access_key_id
        if aws_secret_access_key:
            storage_options["secret"] = aws_secret_access_key
        if aws_session_token:
            storage_options["token"] = aws_session_token
        # Region is auto-detected from S3 path, don't pass it in storage_options
        # If explicit region is needed, it should be set via AWS_REGION env var
        if "endpoint_url" in self.ds_config:
            storage_options["endpoint_url"] = self.ds_config["endpoint_url"]

        # HuggingFace datasets uses storage_options (not fs parameter) for filesystem configuration
        # storage_options are passed to fsspec/s3fs internally
        # For public buckets without credentials, use anonymous access
        # HuggingFace datasets uses storage_options for filesystem configuration.
        # If storage_options is empty, s3fs will use its default credential chain (e.g., IAM role, ~/.aws/credentials).
        if storage_options.get("key") or storage_options.get("secret"):
            logger.info("Using explicit AWS credentials for S3 access")
        else:
            logger.info("Using default AWS credential chain for S3 access")

        # Allow explicit anonymous access via config
        if self.ds_config.get("anon"):
            storage_options["anon"] = True
            logger.info("Anonymous access for public S3 bucket enabled via config.")

        try:
            # Pass storage_options to load_dataset (not fs parameter)
            # storage_options are used by fsspec/s3fs internally
            ds = datasets.load_dataset(
                data_format,
                data_files=path,  # Direct S3 path
                storage_options=storage_options,  # Pass storage_options for S3 filesystem configuration
                **kwargs,
            )
            # Handle DatasetDict (multiple splits) vs Dataset (single)
            if isinstance(ds, datasets.DatasetDict):
                from data_juicer.core.data import NestedDataset

                ds = NestedDataset(datasets.concatenate_datasets([d for d in ds.values()]))
            else:
                from data_juicer.core.data import NestedDataset

                ds = NestedDataset(ds)

            # Unify format
            ds = unify_format(ds, text_keys=text_keys, num_proc=load_data_np, global_cfg=self.cfg)
            return ds
        except Exception as e:
            raise RuntimeError(
                f"Failed to load dataset from S3 path {path}. "
                f"Ensure s3fs is installed and your AWS credentials are configured. "
                f"Error: {str(e)}"
            )


@DataLoadStrategyRegistry.register("ray", "remote", "s3")
class RayS3DataLoadStrategy(RayDataLoadStrategy):
    """
    data load strategy for S3 datasets for RayExecutor
    Uses PyArrow's filesystem to read from S3
    """

    CONFIG_VALIDATION_RULES = {
        "required_fields": ["path"],
        "optional_fields": [
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
            "aws_region",
            "endpoint_url",
            "format",
        ],
        "field_types": {"path": str},
        "custom_validators": {
            "path": lambda x: x.startswith("s3://"),
        },
    }

    def get_ray_data_checkpoint_support(self) -> RayDataCheckpointSupport:
        return RayDataCheckpointSupport(
            True,
            "loads S3 files through Ray file datasource",
        )

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        from data_juicer.core.data.ray_dataset import RayDataset

        path = self.ds_config["path"]
        validate_s3_path(path)

        # Create S3 filesystem using utility function
        s3_fs = create_pyarrow_s3_filesystem(self.ds_config)

        logger.info(f"Loading dataset from S3: {path}")

        # Determine file format from extension or config
        file_extension_map = {
            ".json": "json",
            ".jsonl": "json",
            ".txt": "text",
            ".csv": "csv",
            ".tsv": "csv",
            ".parquet": "parquet",
            ".npy": "numpy",
            ".tfrecords": "tfrecords",
            ".lance": "lance",
        }

        auto_detect = False
        data_format = self.ds_config.get("format", None)
        if data_format is None:
            auto_detect = True
        else:
            # First check if it's already a valid format name
            valid_formats = set(file_extension_map.values())
            if data_format in valid_formats:
                pass  # It's a valid format name, use it as is
            else:
                # Try to interpret as an extension or filename
                suffix = os.path.splitext(data_format)[1]
                if suffix in file_extension_map:
                    data_format = file_extension_map[suffix]
                elif "." + data_format in file_extension_map:
                    data_format = file_extension_map["." + data_format]
                else:
                    auto_detect = True

        if auto_detect:
            # Extract extension from path
            file_extension = os.path.splitext(path)[1]
            if file_extension in file_extension_map:
                data_format = file_extension_map[file_extension]
                logger.info(f"Auto-detected data format: {data_format} from extension: {file_extension}")
            else:
                data_format = "parquet"
                logger.warning(
                    f"Could not determine data format from path '{path}' "
                    f"(extension: '{file_extension or '(none)'}'), "
                    f"defaulting to 'parquet'. "
                    f"Consider explicitly specifying 'format' field in dataset config."
                )
        else:
            logger.info(f"Using specified data format: {data_format}")

        try:
            import ray.data

            # Use ray.data functions directly with PyArrow filesystem support
            # Ray's read functions support filesystem parameter via PyArrow
            if data_format in {"json", "jsonl", "json.gz", "jsonl.gz", "json.zst", "jsonl.zst"}:
                # For JSON, we need to use read_json_stream with filesystem
                from data_juicer.core.data.ray_dataset import read_json_stream

                dataset = read_json_stream(path, filesystem=s3_fs)
            elif data_format == "parquet":
                dataset = ray.data.read_parquet(path, filesystem=s3_fs)
            elif data_format == "csv":
                dataset = ray.data.read_csv(path, filesystem=s3_fs)
            elif data_format == "text":
                dataset = ray.data.read_text(path, filesystem=s3_fs)
            elif data_format == "numpy":
                dataset = ray.data.read_numpy(path, filesystem=s3_fs)
            elif data_format == "tfrecords":
                dataset = ray.data.read_tfrecords(path, filesystem=s3_fs)
            elif data_format == "lance":
                dataset = ray.data.read_lance(path, filesystem=s3_fs)
            else:
                raise ValueError(f"Unsupported data format for S3: {data_format}")

            return RayDataset(dataset, dataset_path=path, cfg=self.cfg)
        except Exception as e:
            raise RuntimeError(
                f"Failed to load {data_format} data from S3 path {path}. "
                f"Ensure your AWS credentials are configured. "
                f"Error: {str(e)}"
            )


@DataLoadStrategyRegistry.register("default", "remote", "hdfs")
class DefaultHDFSDataLoadStrategy(DefaultStagedRemoteLoadStrategy):
    CONFIG_VALIDATION_RULES = {
        "required_fields": ["path"],
        "field_types": {"path": str},
        "custom_validators": {"path": lambda x: x.startswith("hdfs://")},
    }

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        remote_path = self.ds_config["path"]
        local_path = self._make_stage_path(f"hdfs:{remote_path}", remote_path=remote_path)
        staged_path = copy_uri_to_local(remote_path, local_path)
        return self._load_staged_local_dataset(staged_path, **kwargs)


@DataLoadStrategyRegistry.register("ray", "remote", "hdfs")
class RayHDFSDataLoadStrategy(RayDataLoadStrategy):
    CONFIG_VALIDATION_RULES = {
        "required_fields": ["path"],
        "field_types": {
            "format": str,
            "filesystem": str,
            "webhdfs": dict,
            "load_kwargs": dict,
            "limit": int,
            "skip_zero_row_group_files": bool,
            "on_bad_files": str,
            "materialize_after_limit": bool,
        },
        "custom_validators": {
            "path": _validate_hdfs_path_config,
            "filesystem": _validate_ray_hdfs_filesystem,
            "limit": _validate_positive_int,
            "on_bad_files": _validate_on_bad_files,
        },
    }

    def get_ray_data_checkpoint_support(self) -> RayDataCheckpointSupport:
        return RayDataCheckpointSupport(
            True,
            "loads HDFS parquet files through Ray file datasource",
        )

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        from data_juicer.core.data.ray_dataset import RayDataset

        import ray.data

        hdfs_uri = self.ds_config["path"]
        data_format = self.ds_config.get("format", "parquet")
        normalized_format = str(data_format).lower()
        if normalized_format in _RAY_HDFS_PARQUET_FORMATS:
            normalized_format = "parquet"
        elif normalized_format in _RAY_HDFS_JSON_FORMATS:
            normalized_format = "json"
        else:
            raise ValueError(
                f"Unsupported HDFS data format for Ray direct loading: {data_format}. "
                "Ray HDFS loading currently supports parquet, json, and jsonl. "
                "Use the default executor or stage the data locally for other formats."
            )

        logger.info(f"Loading {normalized_format} dataset from HDFS with Ray: {hdfs_uri}")

        try:
            filesystem_type = self.ds_config.get("filesystem", "pyarrow")
            if isinstance(hdfs_uri, list):
                _validate_hdfs_paths_share_filesystem(hdfs_uri)
                if filesystem_type == "webhdfs":
                    filesystem, _ = get_webhdfs_pyarrow_filesystem(
                        hdfs_uri[0],
                        self.ds_config.get("webhdfs"),
                    )
                    fs_path = [urlparse(uri).path or "/" for uri in hdfs_uri]
                else:
                    filesystem, first_fs_path = get_pyarrow_filesystem(hdfs_uri[0])
                    fs_path = [first_fs_path]
                    for uri in hdfs_uri[1:]:
                        _, item_fs_path = get_pyarrow_filesystem(uri)
                        fs_path.append(item_fs_path)
            else:
                if filesystem_type == "webhdfs":
                    filesystem, fs_path = get_webhdfs_pyarrow_filesystem(
                        hdfs_uri,
                        self.ds_config.get("webhdfs"),
                    )
                else:
                    filesystem, fs_path = get_pyarrow_filesystem(hdfs_uri)
            on_bad_files = self.ds_config.get("on_bad_files", "error")
            if normalized_format == "json":
                from data_juicer.core.data.ray_dataset import read_json_stream

                read_kwargs = self.get_ray_json_read_kwargs(kwargs)
                dataset = read_json_stream(
                    fs_path,
                    filesystem=filesystem,
                    on_bad_files=on_bad_files,
                    **read_kwargs,
                )
                limit = self.ds_config.get("limit")
                dataset = _apply_ray_loader_limit(
                    dataset,
                    limit,
                    self.ds_config.get("materialize_after_limit", False),
                    self.cfg,
                )
                return RayDataset(
                    dataset,
                    dataset_path=hdfs_uri[0] if isinstance(hdfs_uri, list) else hdfs_uri,
                    cfg=self.cfg,
                )

            read_kwargs = self.get_ray_parquet_read_kwargs(kwargs)
            skip_zero_row_group_files = self.ds_config.get("skip_zero_row_group_files", True)
            limit = self.ds_config.get("limit")
            read_plan_limit = limit
            if (
                read_kwargs.get("shuffle") not in (None, False)
                or read_kwargs.get("partition_filter") is not None
                or (not skip_zero_row_group_files and on_bad_files != "skip")
            ):
                read_plan_limit = None
            if on_bad_files == "skip":
                read_plan_kwargs = {"skip_bad_files": True}
                if read_plan_limit is not None:
                    read_plan_kwargs["limit"] = read_plan_limit
                    read_plan_kwargs["allow_empty"] = True
                read_plan = _build_parquet_read_plan_from_filesystem(
                    filesystem,
                    fs_path,
                    **read_plan_kwargs,
                )
            elif skip_zero_row_group_files or read_plan_limit is not None:
                read_plan_kwargs = {"filter_for_ray_sampling_only": skip_zero_row_group_files}
                if read_plan_limit is not None:
                    read_plan_kwargs["limit"] = read_plan_limit
                    read_plan_kwargs["allow_empty"] = False
                read_plan = _build_parquet_read_plan_from_filesystem(
                    filesystem,
                    fs_path,
                    **read_plan_kwargs,
                )
            else:
                read_plan = _ParquetReadPlan(paths=fs_path)
            if read_plan.paths == []:
                schema = read_plan.schema or pa.schema([])
                columns = read_kwargs.get("columns")
                if columns:
                    schema = pa.schema([field for field in schema if field.name in columns])
                empty_table = pa.Table.from_batches([], schema=schema)
                from_arrow_kwargs = {}
                if read_kwargs.get("override_num_blocks") is not None:
                    from_arrow_kwargs["override_num_blocks"] = read_kwargs["override_num_blocks"]
                dataset = ray.data.from_arrow(empty_table, **from_arrow_kwargs)
            else:
                dataset = ray.data.read_parquet(read_plan.paths, filesystem=filesystem, **read_kwargs)
            if limit is not None:
                dataset = _apply_ray_loader_limit(
                    dataset,
                    limit,
                    self.ds_config.get("materialize_after_limit", False),
                    self.cfg,
                )
            return RayDataset(
                dataset,
                dataset_path=hdfs_uri[0] if isinstance(hdfs_uri, list) else hdfs_uri,
                cfg=self.cfg,
                row_count_getter=lambda: (
                    _limit_parquet_row_count(
                        read_plan.row_count
                        if read_plan.row_count is not None
                        else _count_parquet_rows_from_filesystem(filesystem, fs_path),
                        limit,
                    )
                ),
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to load {normalized_format} data from HDFS path {hdfs_uri}. "
                "Ensure Hadoop client libraries, libjvm, and HDFS credentials are available "
                "on the Ray driver and workers, or use `filesystem: webhdfs` with a reachable "
                "WebHDFS endpoint. "
                f"Error: {str(e)}"
            )


class TQSQueryLoadMixin(StagedLocalLoadMixin):
    query_field = "query"
    MATERIALIZED_READ_MODE = "materialized"
    MATERIALIZED_REMOTE_READ_MODE = "materialized_remote"
    CLIENT_RESULT_READ_MODE = "client_result"
    _MATERIALIZED_REMOTE_HDFS_FIELDS = {
        "filesystem",
        "webhdfs",
        "columns",
        "concurrency",
        "override_num_blocks",
        "ray_remote_args",
        "limit",
        "skip_zero_row_group_files",
        "on_bad_files",
        "materialize_after_limit",
        "load_kwargs",
    }

    def _get_query(self) -> str:
        query = self.ds_config.get(self.query_field)
        if not query:
            raise ValueError(f"`{self.query_field}` is required")
        return query

    def _get_read_mode(self) -> str:
        read_mode = self.ds_config.get("read_mode", self.MATERIALIZED_READ_MODE)
        supported_modes = {
            self.MATERIALIZED_READ_MODE,
            self.MATERIALIZED_REMOTE_READ_MODE,
            self.CLIENT_RESULT_READ_MODE,
        }
        if read_mode not in supported_modes:
            raise ValueError(
                f"Unsupported TQS/Hive read_mode [{read_mode}]. "
                "Supported modes: "
                f"{self.MATERIALIZED_READ_MODE}, "
                f"{self.MATERIALIZED_REMOTE_READ_MODE}, "
                f"{self.CLIENT_RESULT_READ_MODE}"
            )
        return read_mode

    def _get_output_uri(self) -> str:
        output_uri = self.ds_config.get("output_uri") or self.ds_config.get("tqs_output_uri")
        if not output_uri:
            raise ValueError("TQS/Hive loading requires `output_uri` or `tqs_output_uri`")
        return output_uri

    def _run_tqs_materialization(self, output_uri: str) -> str:
        run_tqs_query(
            query=self._get_query(),
            output_uri=output_uri,
            tqs_app_id=self.ds_config["tqs_app_id"],
            tqs_app_key=self.ds_config["tqs_app_key"],
            user_name=self.ds_config["user_name"],
            cluster=self.ds_config.get("cluster", ""),
            queue_name=self.ds_config.get("queue_name", ""),
            priority=self.ds_config.get("priority", 5),
            memory=self.ds_config.get("memory", 0),
        )
        return output_uri

    def _materialize_query_output(self) -> str:
        output_uri = self._run_tqs_materialization(self._get_output_uri())
        local_dir = self._make_stage_dir(f"tqs:{output_uri}")
        return copy_uri_to_local(output_uri, local_dir)

    def _load_remote_materialized_hdfs_output(self, output_uri: str, **kwargs):
        _validate_hdfs_uri(output_uri)
        self._run_tqs_materialization(output_uri)
        hdfs_config = {
            "type": "remote",
            "source": "hdfs",
            "path": output_uri,
            "format": self.ds_config.get("format", "parquet"),
        }
        for field in self._MATERIALIZED_REMOTE_HDFS_FIELDS:
            if field in self.ds_config:
                hdfs_config[field] = self.ds_config[field]
        return RayHDFSDataLoadStrategy(hdfs_config, self.cfg).load_data(**kwargs)

    def _get_client_result_max_rows(self) -> int:
        max_result_rows = self.ds_config.get("max_result_rows", 10000)
        limit = self.ds_config.get("limit")
        if limit is None:
            return max_result_rows
        return min(max_result_rows, limit)

    def _load_client_result_records(self, max_result_rows: int | None = None) -> list[dict]:
        if max_result_rows is None:
            max_result_rows = self.ds_config.get("max_result_rows", 10000)
        return run_tqs_query_to_records(
            query=self._get_query(),
            tqs_app_id=self.ds_config["tqs_app_id"],
            tqs_app_key=self.ds_config["tqs_app_key"],
            user_name=self.ds_config["user_name"],
            tqs_cluster=self.ds_config.get("tqs_cluster", "cn"),
            tqs_enable_domain=self.ds_config.get("tqs_enable_domain"),
            tqs_timeout=self.ds_config.get("tqs_timeout", 120),
            max_result_rows=max_result_rows,
        )


@DataLoadStrategyRegistry.register("default", "remote", "tqs")
class DefaultTQSDataLoadStrategy(TQSQueryLoadMixin, DefaultStagedRemoteLoadStrategy):
    CONFIG_VALIDATION_RULES = {
        "required_fields": ["query", "tqs_app_id", "tqs_app_key", "user_name"],
        "optional_fields": [
            "output_uri",
            "tqs_output_uri",
            "read_mode",
            "max_result_rows",
            "tqs_cluster",
            "tqs_enable_domain",
            "tqs_timeout",
        ],
        "field_types": {"query": str},
        "custom_validators": {},
    }

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        read_mode = self._get_read_mode()
        if read_mode == self.MATERIALIZED_REMOTE_READ_MODE:
            raise ValueError("TQS read_mode `materialized_remote` requires `executor_type: ray`.")
        if read_mode == self.CLIENT_RESULT_READ_MODE:
            from data_juicer.core.data import NestedDataset

            return NestedDataset.from_list(self._load_client_result_records())
        staged_path = self._materialize_query_output()
        return self._load_staged_local_dataset(staged_path, **kwargs)


@DataLoadStrategyRegistry.register("ray", "remote", "tqs")
class RayTQSDataLoadStrategy(TQSQueryLoadMixin, RayStagedRemoteLoadStrategy):
    CONFIG_VALIDATION_RULES = _with_ray_loader_limit_validation(DefaultTQSDataLoadStrategy.CONFIG_VALIDATION_RULES)

    def get_ray_data_checkpoint_support(self) -> RayDataCheckpointSupport:
        return RayDataCheckpointSupport(
            False,
            "TQS loader is intended for tests and does not provide a supported Ray Data checkpoint "
            "source boundary. For rerun or recovery, configure the already materialized HDFS path "
            "directly as `source: hdfs` / `path: hdfs://...`（直接读已物化 HDFS 路径）.",
        )

    def load_data(self, **kwargs):
        read_mode = self._get_read_mode()
        if read_mode == self.CLIENT_RESULT_READ_MODE:
            import ray

            from data_juicer.core.data.ray_dataset import RayDataset

            dataset = ray.data.from_items(
                self._load_client_result_records(max_result_rows=self._get_client_result_max_rows())
            )
            dataset = _apply_ray_loader_limit(
                dataset,
                self.ds_config.get("limit"),
                self.ds_config.get("materialize_after_limit", False),
                self.cfg,
            )
            return RayDataset(dataset, cfg=self.cfg)
        if read_mode == self.MATERIALIZED_REMOTE_READ_MODE:
            return self._load_remote_materialized_hdfs_output(self._get_output_uri(), **kwargs)
        kwargs = self.get_load_kwargs(**kwargs)
        staged_path = self._materialize_query_output()
        dataset = self._load_staged_local_dataset(staged_path, **kwargs)
        return _apply_ray_dataset_loader_limit(
            dataset,
            self.ds_config.get("limit"),
            self.ds_config.get("materialize_after_limit", False),
            self.cfg,
        )


@DataLoadStrategyRegistry.register("default", "remote", "hive")
class DefaultHiveDataLoadStrategy(DefaultDataLoadStrategy):
    CONFIG_VALIDATION_RULES = {
        "required_fields": [],
        "optional_fields": [],
        "field_types": {},
        "custom_validators": {},
    }

    def load_data(self, **kwargs):
        raise RuntimeError(
            "Hive loading requires `executor_type: ray` and internal bytedray Hive support. "
            "Install `bytedray[default,data,serve,bytedance,hive]>=2.10.0.47`."
        )


_HIVE_CAST_ARROW_TYPES = {
    "STRING": pa.string(),
    "BIGINT": pa.int64(),
}


def _cast_hive_batch_columns(batch, cast_columns):
    for column, hive_type in cast_columns.items():
        if column not in batch.column_names:
            continue
        arrow_type = _HIVE_CAST_ARROW_TYPES.get(hive_type.upper())
        if arrow_type is None:
            raise ValueError(f"Unsupported Hive cast type [{hive_type}] for column [{column}].")
        column_index = batch.schema.get_field_index(column)
        casted_column = batch.column(column).cast(arrow_type)
        batch = batch.set_column(column_index, column, casted_column)
    return batch


def _build_hive_cast_block_udf(cast_columns, existing_block_udf=None):
    def _block_udf(batch):
        if existing_block_udf is not None:
            batch = existing_block_udf(batch)
        return _cast_hive_batch_columns(batch, cast_columns)

    return _block_udf


@DataLoadStrategyRegistry.register("ray", "remote", "hive")
class RayHiveDataLoadStrategy(RayDataLoadStrategy):
    CONFIG_VALIDATION_RULES = {
        "required_fields": ["table_name"],
        "optional_fields": [
            "columns",
            "filter",
            "concurrency",
            "override_num_blocks",
            "ray_remote_args",
            "arrow_parquet_args",
        ],
        "field_types": {
            "table_name": str,
            "filter": str,
            "concurrency": int,
            "override_num_blocks": int,
            "ray_remote_args": dict,
            "arrow_parquet_args": dict,
            "limit": int,
            "materialize_after_limit": bool,
        },
        "custom_validators": {
            "columns": _validate_hive_columns_config,
            "limit": _validate_positive_int,
            **{
                field: _reject_hive_legacy_field(field)
                for field in [
                    "sql",
                    "table",
                    "output_uri",
                    "tqs_output_uri",
                    "read_mode",
                    "max_result_rows",
                    "tqs_app_id",
                    "tqs_app_key",
                    "user_name",
                    "tqs_cluster",
                    "tqs_enable_domain",
                    "tqs_timeout",
                    "catalog",
                    "cast_columns",
                ]
            },
        },
    }

    def get_ray_data_checkpoint_support(self) -> RayDataCheckpointSupport:
        return RayDataCheckpointSupport(
            True,
            "loads Hive table data through bytedray read datasource",
        )

    def load_data(self, **kwargs):
        import ray

        from data_juicer.core.data.ray_dataset import RayDataset

        read_hive_table = getattr(ray.data, "read_hive_table", None)
        if read_hive_table is None:
            raise ImportError(
                "Ray Hive loading requires internal bytedray Hive support. "
                "Install `bytedray[default,data,serve,bytedance,hive]>=2.10.0.47`; "
                "open-source Ray does not provide `ray.data.read_hive_table`."
            )

        # Do not forward executor-level load kwargs such as `num_proc` into
        # read_hive_table. byted-ray may pass unknown kwargs down to DataFusion /
        # PyArrow readers, where they can fail as unsupported parquet arguments.
        read_kwargs = {}
        read_kwargs.update(self.ds_config.get("load_kwargs", {}))
        columns, cast_columns = _normalize_hive_columns_config(self.ds_config.get("columns"))
        if columns is not None:
            read_kwargs["columns"] = columns

        for field in ["filter", "concurrency", "override_num_blocks", "ray_remote_args"]:
            if field in self.ds_config:
                read_kwargs[field] = self.ds_config[field]
        read_kwargs.update(self.ds_config.get("arrow_parquet_args", {}))
        if cast_columns:
            read_kwargs["_block_udf"] = _build_hive_cast_block_udf(
                cast_columns,
                read_kwargs.get("_block_udf"),
            )

        catalog = _get_ray_hive_catalog()
        if catalog is not None:
            read_kwargs["catalog"] = catalog

        dataset = read_hive_table(table_name=self.ds_config["table_name"], **read_kwargs)
        dataset = _apply_ray_loader_limit(
            dataset,
            self.ds_config.get("limit"),
            self.ds_config.get("materialize_after_limit", False),
            self.cfg,
        )
        return RayDataset(dataset, cfg=self.cfg)


@DataLoadStrategyRegistry.register("default", "remote", "duckdb")
class DefaultDuckDBDataLoadStrategy(DefaultStagedRemoteLoadStrategy):
    CONFIG_VALIDATION_RULES = {
        "required_fields": ["sql"],
        "optional_fields": ["path_mapping"],
        "field_types": {"sql": str},
        "custom_validators": {},
    }

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        output_path = self._make_stage_path(f"duckdb:{self.ds_config['sql']}", default_name="dataset.parquet")
        staged_path = materialize_duckdb_query(
            self.ds_config["sql"],
            output_path,
            path_mapping=self.ds_config.get("path_mapping"),
        )
        return self._load_staged_local_dataset(staged_path, **kwargs)


@DataLoadStrategyRegistry.register("ray", "remote", "duckdb")
class RayDuckDBDataLoadStrategy(RayStagedRemoteLoadStrategy):
    CONFIG_VALIDATION_RULES = DefaultDuckDBDataLoadStrategy.CONFIG_VALIDATION_RULES

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        output_path = self._make_stage_path(f"duckdb:{self.ds_config['sql']}", default_name="dataset.parquet")
        staged_path = materialize_duckdb_query(
            self.ds_config["sql"],
            output_path,
            path_mapping=self.ds_config.get("path_mapping"),
        )
        return self._load_staged_local_dataset(staged_path, **kwargs)


def _validate_lark_csv_extension(file_extension: str | None):
    if file_extension is None:
        return
    if file_extension != "csv":
        raise ValueError("Lark loader currently supports only csv export because Excel loading is not supported.")


def _validate_lark_document_type(document_type: str | None):
    if document_type is None:
        return
    if document_type != "sheet":
        raise ValueError("Lark loader currently supports only `document_type: sheet`.")


class LarkDataLoadMixin:
    CONFIG_VALIDATION_RULES = {
        "required_fields": ["lark_path", "lark_app_id", "lark_app_secret"],
        "field_types": {
            "lark_path": str,
            "lark_app_id": str,
            "lark_app_secret": str,
            "sheet_id": str,
            "file_extension": str,
            "document_type": str,
        },
        "custom_validators": {
            "file_extension": _validate_lark_csv_extension,
            "document_type": _validate_lark_document_type,
        },
    }

    def validate_config(self, ds_config: Dict) -> None:
        super().validate_config(ds_config)
        try:
            parse_lark_sheet_location(ds_config["lark_path"], sheet_id=ds_config.get("sheet_id"))
        except ValueError as exc:
            raise ConfigValidationError(str(exc)) from exc

    def _get_lark_csv_stage_path(self) -> str:
        file_extension = self.ds_config.get("file_extension", "csv")
        token, sheet_id = parse_lark_sheet_location(self.ds_config["lark_path"], sheet_id=self.ds_config.get("sheet_id"))
        return self._make_stage_path(
            f"lark:{token}:{sheet_id}:{file_extension}",
            default_name=f"dataset.{file_extension}",
        )

    def _export_lark_csv(self) -> str:
        file_extension = self.ds_config.get("file_extension", "csv")
        _, sheet_id = parse_lark_sheet_location(self.ds_config["lark_path"], sheet_id=self.ds_config.get("sheet_id"))
        return export_lark_sheet_to_local(
            lark_path=self.ds_config["lark_path"],
            lark_app_id=self.ds_config["lark_app_id"],
            lark_app_secret=self.ds_config["lark_app_secret"],
            output_path=self._get_lark_csv_stage_path(),
            file_extension=file_extension,
            document_type=self.ds_config.get("document_type", "sheet"),
            sheet_id=sheet_id,
            wait_export_time_seconds=self.ds_config.get("wait_export_time_seconds", 60),
        )


@DataLoadStrategyRegistry.register("default", "remote", "lark")
class DefaultLarkDataLoadStrategy(LarkDataLoadMixin, DefaultStagedRemoteLoadStrategy):
    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        staged_path = self._export_lark_csv()
        return self._load_staged_local_dataset(staged_path, **kwargs)


@DataLoadStrategyRegistry.register("ray", "remote", "lark")
class RayLarkDataLoadStrategy(LarkDataLoadMixin, StagedLocalLoadMixin, RayDataLoadStrategy):
    CONFIG_VALIDATION_RULES = _with_ray_loader_limit_validation(LarkDataLoadMixin.CONFIG_VALIDATION_RULES)

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        staged_path = self._export_lark_csv()
        local_ds_config = dict(self.ds_config)
        local_ds_config["type"] = "local"
        local_ds_config["path"] = staged_path
        local_dataset = DefaultLocalDataLoadStrategy(local_ds_config, self.cfg).load_data(**kwargs)

        import ray
        from data_juicer.core.data.ray_dataset import RayDataset

        dataset = ray.data.from_arrow(local_dataset.data.table)
        dataset = _apply_ray_loader_limit(
            dataset,
            self.ds_config.get("limit"),
            self.ds_config.get("materialize_after_limit", False),
            self.cfg,
        )
        return RayDataset(dataset, dataset_path=staged_path, cfg=self.cfg)


@DataLoadStrategyRegistry.register("default", "remote", "magnus")
class DefaultMagnusDataLoadStrategy(DefaultDataLoadStrategy):
    CONFIG_VALIDATION_RULES = {
        "required_fields": ["table_name"],
        "field_types": {"table_name": str},
        "custom_validators": {},
    }

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        from data_juicer.core.data import NestedDataset
        from datasets import Dataset

        dataset = read_magnus_to_pandas(
            self.ds_config["table_name"],
            filter=self.ds_config.get("filter"),
            magnus_conf=self.ds_config.get("magnus_conf", {}),
        )
        return NestedDataset(Dataset.from_pandas(dataset, preserve_index=False))


@DataLoadStrategyRegistry.register("ray", "remote", "magnus")
class RayMagnusDataLoadStrategy(RayDataLoadStrategy):
    CONFIG_VALIDATION_RULES = _with_ray_loader_limit_validation(
        {
            "required_fields": ["table_name"],
            "field_types": {
                "table_name": str,
                "filter": str,
                "magnus_conf": dict,
            },
            "custom_validators": {},
        }
    )

    def get_ray_data_checkpoint_support(self) -> RayDataCheckpointSupport:
        return RayDataCheckpointSupport(
            True,
            "loads Magnus table data through Ray datasource with read task identifiers",
        )

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        from data_juicer.core.data.ray_dataset import RayDataset

        dataset = read_magnus_to_ray(
            self.ds_config["table_name"],
            filter=self.ds_config.get("filter"),
            magnus_conf=self.ds_config.get("magnus_conf", {}),
        )
        dataset = _apply_ray_loader_limit(
            dataset,
            self.ds_config.get("limit"),
            self.ds_config.get("materialize_after_limit", False),
            self.cfg,
        )
        return RayDataset(dataset, cfg=self.cfg)
