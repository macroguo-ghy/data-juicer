import base64
import inspect
import json
import os
import uuid
from functools import partial

from loguru import logger

from data_juicer.core.io_utils import _is_ray_data_checkpoint_enabled, get_pyarrow_filesystem
from data_juicer.utils.constant import Fields, HashKeys
from data_juicer.utils.file_utils import Sizes, byte_size_to_size_str
from data_juicer.utils.model_utils import filter_arguments
from data_juicer.utils.webdataset_utils import reconstruct_custom_webdataset_format


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
            extra_fields = {Fields.stats, Fields.meta}
            removed_fields.extend(list(extra_fields.intersection(feature_fields)))
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
