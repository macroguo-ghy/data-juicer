from __future__ import annotations

import os
from typing import Any, Dict

from loguru import logger

from data_juicer.core.exporter import Exporter
from data_juicer.core.io_utils import (
    copy_local_to_uri,
    ensure_parent,
    execute_tqs_sql,
    infer_storage_target_from_path,
    make_staging_dir,
    merge_dicts,
    namespace_to_plain_dict,
    upload_file_to_lark_sheet,
    upload_file_to_tos,
    write_hf_dataset_to_magnus,
    write_ray_dataset_to_magnus,
)
from data_juicer.core.ray_exporter import RayExporter
from data_juicer.utils.constant import Fields, HashKeys


class ExportManager:
    """Single-target export facade that keeps the old exporter interface."""

    def __init__(self, cfg, *, executor_type: str):
        self.cfg = cfg
        self.executor_type = executor_type
        self.export_cfg = self._normalize_export_cfg(cfg)
        self.target = self.export_cfg["target"]
        self.path = self.export_cfg.get("path") or getattr(cfg, "export_path", "")

        self.file_exporter = None
        if self.target in {"local", "s3"}:
            self.file_exporter = self._build_file_exporter(self.path)

    def export(self, dataset, columns=None):
        if self.target in {"local", "s3"}:
            if self.executor_type == "ray":
                return self.file_exporter.export(dataset, columns=columns)
            return self.file_exporter.export(dataset)

        if self.target == "hdfs":
            return self._export_to_hdfs(dataset, columns=columns)
        if self.target == "hive":
            return self._export_to_hive(dataset, columns=columns)
        if self.target == "lark":
            return self._export_to_lark(dataset, columns=columns)
        if self.target == "tos":
            return self._export_to_tos(dataset, columns=columns)
        if self.target == "magnus":
            return self._export_to_magnus(dataset)
        raise NotImplementedError(f"Unsupported export target [{self.target}]")

    def export_compute_stats(self, dataset, export_path):
        exporter = Exporter(
            export_path=export_path,
            export_type=None,
            export_shard_size=0,
            export_in_parallel=getattr(self.cfg, "export_in_parallel", False),
            num_proc=getattr(self.cfg, "np", 1),
            keep_stats_in_res_ds=True,
            keep_hashes_in_res_ds=True,
            export_stats=False,
        )
        exporter.export_compute_stats(dataset, export_path)

    def _build_file_exporter(self, export_path: str):
        extra_args = merge_dicts(getattr(self.cfg, "export_extra_args", {}), self.export_cfg.get("extra_args"))
        if self.target == "s3":
            extra_args.update(self.export_cfg.get("aws_credentials", {}))

        if self.executor_type == "ray":
            return RayExporter(
                export_path,
                self.export_cfg.get("type"),
                self.export_cfg.get("shard_size", 0),
                keep_stats_in_res_ds=getattr(self.cfg, "keep_stats_in_res_ds", True),
                keep_hashes_in_res_ds=getattr(self.cfg, "keep_hashes_in_res_ds", False),
                **extra_args,
            )

        return Exporter(
            export_path=export_path,
            export_type=self.export_cfg.get("type"),
            export_shard_size=self.export_cfg.get("shard_size", 0),
            export_in_parallel=self.export_cfg.get("in_parallel", getattr(self.cfg, "export_in_parallel", False)),
            num_proc=getattr(self.cfg, "np", 1),
            export_ds=True,
            keep_stats_in_res_ds=getattr(self.cfg, "keep_stats_in_res_ds", False),
            keep_hashes_in_res_ds=getattr(self.cfg, "keep_hashes_in_res_ds", False),
            export_stats=True,
            **extra_args,
        )

    def _export_via_staging(self, dataset, columns=None, *, filename: str | None = None) -> str:
        stage_dir = make_staging_dir(self.cfg.work_dir, "export", f"{self.target}:{self.path}")
        export_type = self.export_cfg.get("type") or "jsonl"
        filename = filename or f"dataset.{export_type}"
        stage_path = os.path.join(stage_dir, filename)
        dataset, columns = self._prepare_dataset_for_export(dataset, columns=columns)

        if self.executor_type == "ray" and self.target in {"lark", "tos"}:
            self._export_ray_single_file(dataset, stage_path, columns=columns, export_type=export_type)
            return stage_path

        stage_exporter = self._build_file_exporter(stage_path)
        if self.executor_type == "ray":
            stage_exporter.export(dataset, columns=columns)
        else:
            stage_exporter.export(dataset)
        return stage_path

    def _export_ray_single_file(self, dataset, stage_path: str, *, columns=None, export_type: str):
        if columns:
            dataset = dataset.select_columns(columns)
        df = dataset.to_pandas()
        ensure_parent(stage_path)
        if export_type == "csv":
            df.to_csv(stage_path, index=False)
            return
        if export_type == "json":
            df.to_json(stage_path, orient="records", force_ascii=False)
            return
        if export_type == "jsonl":
            df.to_json(stage_path, orient="records", lines=True, force_ascii=False)
            return
        if export_type == "parquet":
            df.to_parquet(stage_path, index=False)
            return
        raise NotImplementedError(
            f"Ray export to target [{self.target}] only supports single-file csv/json/jsonl/parquet staging for now"
        )

    def _export_to_hdfs(self, dataset, columns=None):
        stage_path = self._export_via_staging(dataset, columns=columns)
        copy_local_to_uri(stage_path, self.path)

    def _export_to_hive(self, dataset, columns=None):
        staging_path = self.export_cfg.get("path")
        if not staging_path or not staging_path.startswith("hdfs://"):
            raise ValueError("Hive export requires an HDFS `path` staging location")

        self._export_to_hdfs(dataset, columns=columns)
        partition = self.export_cfg.get("partition")
        if not self.export_cfg.get("hive_table"):
            raise ValueError("Hive export requires `hive_table`")

        query = f"alter table {self.export_cfg['hive_table']} add if not exists"
        if partition:
            query += f" partition ({self._format_partition_spec(partition)})"
        query += f" location '{staging_path}'"

        execute_tqs_sql(
            query,
            tqs_app_id=self.export_cfg["tqs_app_id"],
            tqs_app_key=self.export_cfg["tqs_app_key"],
            user_name=self.export_cfg["user_name"],
        )

    def _export_to_lark(self, dataset, columns=None):
        stage_path = self._export_via_staging(dataset, columns=columns, filename="dataset.csv")
        upload_file_to_lark_sheet(
            local_path=stage_path,
            lark_path=self.export_cfg["lark_path"],
            lark_app_id=self.export_cfg["lark_app_id"],
            lark_app_secret=self.export_cfg["lark_app_secret"],
            cell_range=self.export_cfg["range"],
        )

    def _export_to_tos(self, dataset, columns=None):
        stage_path = self._export_via_staging(dataset, columns=columns)
        if os.path.isdir(stage_path):
            raise ValueError("TOS export currently requires a single-file export format")
        upload_file_to_tos(
            stage_path,
            bucket_name=self.export_cfg["bucket_name"],
            object_key=self.export_cfg["object_key"],
            endpoint=self.export_cfg.get("endpoint", "https://tos-cn-beijing.volces.com"),
            region=self.export_cfg.get("region", "cn-beijing"),
            access_key=self.export_cfg.get("access_key"),
            secret_key=self.export_cfg.get("secret_key"),
            session_token=self.export_cfg.get("session_token"),
        )

    def _export_to_magnus(self, dataset):
        dataset, _ = self._prepare_dataset_for_export(dataset)
        if hasattr(dataset, "column_names"):
            return write_hf_dataset_to_magnus(
                dataset,
                self.export_cfg["table_name"],
                partition_columns=self.export_cfg.get("partition_columns"),
                magnus_conf=self.export_cfg.get("magnus_conf", {}),
                batch_size=self.export_cfg.get("batch_size", 2000),
            )
        if hasattr(dataset, "columns"):
            return write_ray_dataset_to_magnus(
                dataset,
                self.export_cfg["table_name"],
                partition_columns=self.export_cfg.get("partition_columns"),
                magnus_conf=self.export_cfg.get("magnus_conf", {}),
                operation=self.export_cfg.get("operation", "APPEND"),
            )
        if hasattr(dataset, "data") and hasattr(dataset.data, "columns"):
            return write_ray_dataset_to_magnus(
                dataset.data,
                self.export_cfg["table_name"],
                partition_columns=self.export_cfg.get("partition_columns"),
                magnus_conf=self.export_cfg.get("magnus_conf", {}),
                operation=self.export_cfg.get("operation", "APPEND"),
            )
        raise ImportError("Magnus export requires `pyiceberg` support for the current dataset type")

    @staticmethod
    def _normalize_export_cfg(cfg) -> Dict[str, Any]:
        export_cfg = namespace_to_plain_dict(getattr(cfg, "export", None) or {})
        if export_cfg:
            export_cfg.setdefault("target", ExportManager._infer_export_target(export_cfg))
            export_cfg.setdefault("path", getattr(cfg, "export_path", ""))
            export_cfg.setdefault("type", export_cfg.get("export_type"))
            export_cfg.setdefault("shard_size", export_cfg.get("export_shard_size", getattr(cfg, "export_shard_size", 0)))
            export_cfg.setdefault(
                "in_parallel",
                export_cfg.get("export_in_parallel", getattr(cfg, "export_in_parallel", False)),
            )
            export_cfg.setdefault("extra_args", {})
            export_cfg.setdefault("aws_credentials", {})
            return export_cfg

        return {
            "target": ExportManager._infer_export_target({"path": cfg.export_path}),
            "path": cfg.export_path,
            "type": getattr(cfg, "export_type", None),
            "shard_size": getattr(cfg, "export_shard_size", 0),
            "in_parallel": getattr(cfg, "export_in_parallel", False),
            "extra_args": namespace_to_plain_dict(getattr(cfg, "export_extra_args", {})),
            "aws_credentials": namespace_to_plain_dict(getattr(cfg, "export_aws_credentials", {})),
        }

    @staticmethod
    def _infer_export_target(export_cfg: Dict[str, Any]) -> str:
        target = export_cfg.get("target")
        if target:
            return target
        if export_cfg.get("hive_table"):
            return "hive"
        if export_cfg.get("table_name") and export_cfg.get("magnus_conf") is not None:
            return "magnus"
        if export_cfg.get("lark_path"):
            return "lark"
        if export_cfg.get("bucket_name") and export_cfg.get("object_key"):
            return "tos"
        return infer_storage_target_from_path(export_cfg.get("path"))

    @staticmethod
    def _format_partition_spec(partition: Any) -> str:
        if isinstance(partition, dict):
            return ", ".join(f"{key}='{value}'" for key, value in partition.items())
        if isinstance(partition, str):
            return partition
        raise TypeError("Hive partition must be a string or dictionary")

    def _prepare_dataset_for_export(self, dataset, columns=None):
        removed_fields = []
        if not getattr(self.cfg, "keep_stats_in_res_ds", False):
            removed_fields.extend([Fields.stats, Fields.meta])
        if not getattr(self.cfg, "keep_hashes_in_res_ds", False):
            removed_fields.extend(
                [
                    HashKeys.hash,
                    HashKeys.minhash,
                    HashKeys.simhash,
                    HashKeys.imagehash,
                    HashKeys.videohash,
                ]
            )

        if hasattr(dataset, "column_names"):
            existing = set(dataset.column_names)
            removable = [field for field in removed_fields if field in existing]
            if removable:
                dataset = dataset.remove_columns(removable)
            if columns is not None:
                columns = [column for column in columns if column not in removable]
            return dataset, columns

        if hasattr(dataset, "columns"):
            existing_columns = dataset.columns() or []
            removable = [field for field in removed_fields if field in existing_columns]
            if removable:
                dataset = dataset.drop_columns(removable)
            if columns is not None:
                columns = [column for column in columns if column not in removable]
            return dataset, columns

        return dataset, columns
