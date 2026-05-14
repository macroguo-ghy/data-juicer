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
from data_juicer.core.data.config_validator import ConfigValidator
from data_juicer.core.io_utils import (
    copy_uri_to_local,
    export_lark_sheet_to_local,
    get_pyarrow_filesystem,
    infer_local_name_from_uri,
    make_staging_dir,
    materialize_duckdb_query,
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


def _get_webhdfs_pyarrow_filesystem(uri: str, webhdfs_config: Dict[str, Any] | None = None):
    import fsspec
    import pyarrow.fs as pa_fs

    webhdfs_config = webhdfs_config or {}
    parsed = urlparse(uri)
    fs_path = parsed.path or "/"

    filesystem_kwargs = {
        "host": webhdfs_config.get("host") or parsed.hostname or "localhost",
        "port": webhdfs_config.get("port", 9870),
    }
    if webhdfs_config.get("user") is not None:
        filesystem_kwargs["user"] = webhdfs_config["user"]

    for key, value in webhdfs_config.items():
        if key not in {"host", "port", "user"}:
            filesystem_kwargs[key] = value

    webhdfs_fs = fsspec.filesystem("webhdfs", **filesystem_kwargs)
    return pa_fs.PyFileSystem(pa_fs.FSSpecHandler(webhdfs_fs)), fs_path


def _validate_ray_hdfs_filesystem(filesystem_type: str) -> None:
    if filesystem_type not in {"pyarrow", "webhdfs"}:
        raise ValueError(
            f"Unsupported Ray HDFS filesystem [{filesystem_type}]. "
            "Expected `pyarrow` or `webhdfs`."
        )


def _validate_hdfs_uri(uri: str) -> None:
    if not uri.startswith("hdfs://"):
        raise ValueError(f"Expected an HDFS URI starting with `hdfs://`, got [{uri}].")


def _is_countable_parquet_metadata_file(path: str) -> bool:
    name = os.path.basename(path)
    if not name or name.startswith(("_", ".")):
        return False
    return True


def _count_parquet_rows_from_filesystem(filesystem, path: str) -> int | None:
    try:
        import pyarrow.fs as pa_fs
        import pyarrow.parquet as pq

        file_info = filesystem.get_file_info(path)
        if file_info.type == pa_fs.FileType.NotFound:
            return None
        if file_info.type == pa_fs.FileType.File:
            paths = [path] if _is_countable_parquet_metadata_file(path) else []
        elif file_info.type == pa_fs.FileType.Directory:
            selector = pa_fs.FileSelector(path, recursive=True)
            paths = [
                info.path
                for info in filesystem.get_file_info(selector)
                if info.type == pa_fs.FileType.File and _is_countable_parquet_metadata_file(info.path)
            ]
        else:
            return None

        row_count = 0
        for parquet_path in paths:
            row_count += pq.read_metadata(parquet_path, filesystem=filesystem).num_rows
        return row_count
    except Exception as exc:
        logger.debug("Failed to count parquet rows from metadata for {}: {}", path, exc)
        return None


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
        return read_kwargs

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
            "path": str,
            "format": str,
            "filesystem": str,
            "webhdfs": dict,
            "load_kwargs": dict,
        },
        "custom_validators": {
            "path": _validate_hdfs_uri,
            "filesystem": _validate_ray_hdfs_filesystem,
        },
    }

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        from data_juicer.core.data.ray_dataset import RayDataset

        import ray.data

        hdfs_uri = self.ds_config["path"]
        data_format = self.ds_config.get("format", "parquet")
        if data_format not in {"parquet", ".parquet"}:
            raise ValueError(
                f"Unsupported HDFS data format for Ray direct loading: {data_format}. "
                "Ray HDFS loading currently supports parquet only. "
                "Use the default executor or stage the data locally for other formats."
            )

        read_kwargs = self.get_ray_parquet_read_kwargs(kwargs)
        logger.info(f"Loading parquet dataset from HDFS with Ray: {hdfs_uri}")

        try:
            filesystem_type = self.ds_config.get("filesystem", "pyarrow")
            if filesystem_type == "webhdfs":
                filesystem, fs_path = _get_webhdfs_pyarrow_filesystem(
                    hdfs_uri,
                    self.ds_config.get("webhdfs"),
                )
            else:
                filesystem, fs_path = get_pyarrow_filesystem(hdfs_uri)
            dataset = ray.data.read_parquet(fs_path, filesystem=filesystem, **read_kwargs)
            return RayDataset(
                dataset,
                dataset_path=hdfs_uri,
                cfg=self.cfg,
                row_count_getter=lambda: _count_parquet_rows_from_filesystem(filesystem, fs_path),
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to load parquet data from HDFS path {hdfs_uri}. "
                "Ensure Hadoop client libraries, libjvm, and HDFS credentials are available "
                "on the Ray driver and workers, or use `filesystem: webhdfs` with a reachable "
                "WebHDFS endpoint. "
                f"Error: {str(e)}"
            )


class TQSQueryLoadMixin(StagedLocalLoadMixin):
    query_field = "query"
    MATERIALIZED_READ_MODE = "materialized"
    CLIENT_RESULT_READ_MODE = "client_result"

    def _get_query(self) -> str:
        query = self.ds_config.get(self.query_field)
        if not query:
            raise ValueError(f"`{self.query_field}` is required")
        return query

    def _get_read_mode(self) -> str:
        read_mode = self.ds_config.get("read_mode", self.MATERIALIZED_READ_MODE)
        if read_mode not in {self.MATERIALIZED_READ_MODE, self.CLIENT_RESULT_READ_MODE}:
            raise ValueError(
                f"Unsupported TQS/Hive read_mode [{read_mode}]. "
                f"Supported modes: {self.MATERIALIZED_READ_MODE}, {self.CLIENT_RESULT_READ_MODE}"
            )
        return read_mode

    def _materialize_query_output(self) -> str:
        query = self._get_query()
        output_uri = self.ds_config.get("output_uri") or self.ds_config.get("tqs_output_uri")
        if not output_uri:
            raise ValueError("TQS/Hive loading requires `output_uri` or `tqs_output_uri`")
        run_tqs_query(
            query=query,
            output_uri=output_uri,
            tqs_app_id=self.ds_config["tqs_app_id"],
            tqs_app_key=self.ds_config["tqs_app_key"],
            user_name=self.ds_config["user_name"],
            cluster=self.ds_config.get("cluster", ""),
            queue_name=self.ds_config.get("queue_name", ""),
            priority=self.ds_config.get("priority", 5),
            memory=self.ds_config.get("memory", 0),
        )
        local_dir = self._make_stage_dir(f"tqs:{output_uri}")
        return copy_uri_to_local(output_uri, local_dir)

    def _load_client_result_records(self) -> list[dict]:
        return run_tqs_query_to_records(
            query=self._get_query(),
            tqs_app_id=self.ds_config["tqs_app_id"],
            tqs_app_key=self.ds_config["tqs_app_key"],
            user_name=self.ds_config["user_name"],
            tqs_cluster=self.ds_config.get("tqs_cluster", "cn"),
            tqs_enable_domain=self.ds_config.get("tqs_enable_domain"),
            tqs_timeout=self.ds_config.get("tqs_timeout", 120),
            max_result_rows=self.ds_config.get("max_result_rows", 10000),
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
        if self._get_read_mode() == self.CLIENT_RESULT_READ_MODE:
            from data_juicer.core.data import NestedDataset

            return NestedDataset.from_list(self._load_client_result_records())
        staged_path = self._materialize_query_output()
        return self._load_staged_local_dataset(staged_path, **kwargs)


@DataLoadStrategyRegistry.register("ray", "remote", "tqs")
class RayTQSDataLoadStrategy(TQSQueryLoadMixin, RayStagedRemoteLoadStrategy):
    CONFIG_VALIDATION_RULES = DefaultTQSDataLoadStrategy.CONFIG_VALIDATION_RULES

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        if self._get_read_mode() == self.CLIENT_RESULT_READ_MODE:
            import ray

            from data_juicer.core.data.ray_dataset import RayDataset

            return RayDataset(ray.data.from_items(self._load_client_result_records()), cfg=self.cfg)
        staged_path = self._materialize_query_output()
        return self._load_staged_local_dataset(staged_path, **kwargs)


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
        },
        "custom_validators": {
            "columns": _validate_hive_columns_config,
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


@DataLoadStrategyRegistry.register("default", "remote", "lark")
class DefaultLarkDataLoadStrategy(DefaultStagedRemoteLoadStrategy):
    CONFIG_VALIDATION_RULES = {
        "required_fields": ["lark_path", "lark_app_id", "lark_app_secret"],
        "field_types": {"lark_path": str},
        "custom_validators": {},
    }

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        output_path = self._make_stage_path(f"lark:{self.ds_config['lark_path']}", default_name="dataset.csv")
        staged_path = export_lark_sheet_to_local(
            lark_path=self.ds_config["lark_path"],
            lark_app_id=self.ds_config["lark_app_id"],
            lark_app_secret=self.ds_config["lark_app_secret"],
            output_path=output_path,
            file_extension=self.ds_config.get("file_extension", "csv"),
            document_type=self.ds_config.get("document_type", "sheet"),
            sheet_id=self.ds_config.get("sheet_id"),
            wait_export_time_seconds=self.ds_config.get("wait_export_time_seconds", 60),
        )
        return self._load_staged_local_dataset(staged_path, **kwargs)


@DataLoadStrategyRegistry.register("ray", "remote", "lark")
class RayLarkDataLoadStrategy(RayStagedRemoteLoadStrategy):
    CONFIG_VALIDATION_RULES = DefaultLarkDataLoadStrategy.CONFIG_VALIDATION_RULES

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        output_path = self._make_stage_path(f"lark:{self.ds_config['lark_path']}", default_name="dataset.csv")
        staged_path = export_lark_sheet_to_local(
            lark_path=self.ds_config["lark_path"],
            lark_app_id=self.ds_config["lark_app_id"],
            lark_app_secret=self.ds_config["lark_app_secret"],
            output_path=output_path,
            file_extension=self.ds_config.get("file_extension", "csv"),
            document_type=self.ds_config.get("document_type", "sheet"),
            sheet_id=self.ds_config.get("sheet_id"),
            wait_export_time_seconds=self.ds_config.get("wait_export_time_seconds", 60),
        )
        return self._load_staged_local_dataset(staged_path, **kwargs)


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
    CONFIG_VALIDATION_RULES = DefaultMagnusDataLoadStrategy.CONFIG_VALIDATION_RULES

    def load_data(self, **kwargs):
        kwargs = self.get_load_kwargs(**kwargs)
        from data_juicer.core.data.ray_dataset import RayDataset

        dataset = read_magnus_to_ray(
            self.ds_config["table_name"],
            filter=self.ds_config.get("filter"),
            magnus_conf=self.ds_config.get("magnus_conf", {}),
        )
        return RayDataset(dataset, cfg=self.cfg)
