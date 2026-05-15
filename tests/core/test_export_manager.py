import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pyarrow as pa
import yaml
from jsonargparse import Namespace

_register_extension_type = pa.register_extension_type


def _register_extension_type_once(extension_type):
    try:
        _register_extension_type(extension_type)
    except pa.ArrowKeyError:
        if not extension_type.extension_name.startswith("datasets.features.features."):
            raise


pa.register_extension_type = _register_extension_type_once

from data_juicer.core.export_manager import ExportManager
from data_juicer.utils.constant import Fields, HashKeys


class RayLikeDataset:
    def __init__(self, columns):
        self._columns = columns
        self.drop_columns = MagicMock(return_value=self)
        self.select_columns = MagicMock(return_value=self)
        self.write_hive_table = MagicMock()

    def columns(self, *args, **kwargs):
        return self._columns


class HFDatasetLike:
    def __init__(self, rows):
        self.rows = rows
        self.selected_range = None

    def __len__(self):
        return len(self.rows)

    def select(self, row_range):
        selected_range = list(row_range)
        selected = HFDatasetLike([self.rows[index] for index in selected_range])
        selected.selected_range = selected_range
        return selected


class RayLimitDataset(RayLikeDataset):
    def __init__(self, columns, label="original"):
        super().__init__(columns)
        self.label = label
        self.limited_dataset = None
        self.limit = MagicMock(side_effect=self._limit)

    def _limit(self, max_rows):
        self.limited_dataset = RayLimitDataset(self._columns, label=f"limit-{max_rows}")
        return self.limited_dataset


class ExportManagerTest(unittest.TestCase):
    def _make_cfg(self, export=None):
        return Namespace(
            work_dir="tmp/test_export_manager",
            export=export,
            export_path="./outputs/legacy.jsonl",
            export_type="jsonl",
            export_shard_size=0,
            export_in_parallel=False,
            export_extra_args={},
            export_aws_credentials=None,
            keep_stats_in_res_ds=False,
            keep_hashes_in_res_ds=False,
            np=1,
        )

    def test_structured_export_takes_precedence(self):
        cfg = self._make_cfg(
            {
                "target": "hdfs",
                "path": "hdfs://cluster/path/result.parquet",
                "type": "parquet",
                "extra_args": {"compression": "snappy"},
            }
        )

        manager = ExportManager(cfg, executor_type="default")

        self.assertEqual(manager.target, "hdfs")
        self.assertEqual(manager.path, "hdfs://cluster/path/result.parquet")
        self.assertEqual(manager.export_cfg["type"], "parquet")
        self.assertEqual(manager.export_cfg["extra_args"], {"compression": "snappy"})

    def test_export_max_rows_limits_hf_like_dataset_before_hdfs_export(self):
        cfg = self._make_cfg(
            {
                "target": "hdfs",
                "path": "hdfs://cluster/path/result.jsonl",
                "max_rows": 2,
            }
        )
        manager = ExportManager(cfg, executor_type="default")
        dataset = HFDatasetLike([{"id": 1}, {"id": 2}, {"id": 3}])

        with patch.object(manager, "_export_to_hdfs") as mock_export_to_hdfs:
            manager.export(dataset)

        exported_dataset = mock_export_to_hdfs.call_args.args[0]
        self.assertEqual(exported_dataset.selected_range, [0, 1])
        self.assertEqual(len(exported_dataset), 2)

    def test_export_max_rows_limits_ray_dataset_before_hive_export(self):
        cfg = self._make_cfg(
            {
                "target": "hive",
                "table_name": "db.table_name",
                "max_rows": 2,
            }
        )
        manager = ExportManager(cfg, executor_type="ray")
        dataset = RayLimitDataset(["id"])

        manager.export(dataset)

        dataset.limit.assert_called_once_with(2)
        self.assertIsNotNone(dataset.limited_dataset)
        dataset.write_hive_table.assert_not_called()
        dataset.limited_dataset.write_hive_table.assert_called_once_with(table_name="db.table_name")

    @patch("data_juicer.core.export_manager.write_ray_dataset_to_magnus")
    def test_export_max_rows_limits_ray_dataset_before_magnus_export(self, mock_write_ray_dataset_to_magnus):
        cfg = self._make_cfg(
            {
                "target": "magnus",
                "table_name": "catalog.db.table",
                "magnus_conf": {},
                "max_rows": 2,
            }
        )
        manager = ExportManager(cfg, executor_type="ray")
        dataset = RayLimitDataset(["id"])

        manager.export(dataset)

        dataset.limit.assert_called_once_with(2)
        self.assertIs(mock_write_ray_dataset_to_magnus.call_args.args[0], dataset.limited_dataset)

    def test_bytedance_magnus_demo_configs_write_lance(self):
        demo_root = os.path.join(os.getcwd(), "demos", "bytedance")
        if not os.path.isdir(demo_root):
            self.skipTest("ByteDance demo configs are not included")

        magnus_configs = []
        missing_lance_configs = []

        for root, _, filenames in os.walk(demo_root):
            for filename in filenames:
                if not filename.endswith((".yaml", ".yml")):
                    continue
                path = os.path.join(root, filename)
                with open(path, encoding="utf-8") as f:
                    config = yaml.safe_load(f) or {}
                export_config = config.get("export") or {}
                if export_config.get("target") != "magnus":
                    continue
                magnus_configs.append(os.path.relpath(path, os.getcwd()))
                write_options = ((export_config.get("magnus_conf") or {}).get("write_options") or {})
                if write_options.get("write.format.default") != "lance":
                    missing_lance_configs.append(os.path.relpath(path, os.getcwd()))

        if not magnus_configs:
            self.skipTest("ByteDance Magnus demo configs are not included")
        self.assertEqual(missing_lance_configs, [])

    def test_staging_export_infers_type_from_target_path(self):
        cfg = self._make_cfg(
            {
                "target": "hdfs",
                "path": "hdfs://cluster/path/result.parquet",
            }
        )
        manager = ExportManager(cfg, executor_type="default")

        class ExporterStub:
            def export(self, dataset):
                return None

        with (
            tempfile.TemporaryDirectory() as tmp_dir,
            patch("data_juicer.core.export_manager.make_staging_dir", return_value=tmp_dir),
            patch.object(manager, "_build_file_exporter", return_value=ExporterStub()) as mock_build_exporter,
        ):
            stage_path = manager._export_via_staging(object())

        self.assertEqual(os.path.basename(stage_path), "result.parquet")
        mock_build_exporter.assert_called_once_with(os.path.join(tmp_dir, "result.parquet"))

    def test_staging_export_infers_type_from_tos_object_key(self):
        cfg = self._make_cfg(
            {
                "target": "tos",
                "bucket_name": "bucket",
                "object_key": "exports/result.parquet",
            }
        )
        manager = ExportManager(cfg, executor_type="default")

        class ExporterStub:
            def export(self, dataset):
                return None

        with (
            tempfile.TemporaryDirectory() as tmp_dir,
            patch("data_juicer.core.export_manager.make_staging_dir", return_value=tmp_dir),
            patch.object(manager, "_build_file_exporter", return_value=ExporterStub()) as mock_build_exporter,
        ):
            stage_path = manager._export_via_staging(object())

        self.assertEqual(os.path.basename(stage_path), "result.parquet")
        mock_build_exporter.assert_called_once_with(os.path.join(tmp_dir, "result.parquet"))

    @patch("data_juicer.core.export_manager.upload_file_to_lark_sheet")
    def test_lark_export_defaults_to_csv_staging_content(self, mock_upload_file_to_lark_sheet):
        cfg = self._make_cfg(
            {
                "target": "lark",
                "lark_path": "https://example.feishu.cn/sheets/shtcn123?sheet=abc",
                "lark_app_id": "app_id",
                "lark_app_secret": "app_secret",
                "range": "A1",
            }
        )
        manager = ExportManager(cfg, executor_type="default")

        class CsvDataset:
            features = {"text": object()}

            def remove_columns(self, columns):
                return self

            def to_csv(self, export_path, num_proc=1, storage_options=None):
                with open(export_path, "w") as fout:
                    fout.write("text\nhello\n")

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("data_juicer.core.export_manager.make_staging_dir", return_value=tmp_dir):
                manager._export_to_lark(CsvDataset())
            uploaded_path = mock_upload_file_to_lark_sheet.call_args.kwargs["local_path"]
            self.assertEqual(os.path.basename(uploaded_path), "dataset.csv")
            with open(uploaded_path) as fin:
                self.assertEqual(fin.read(), "text\nhello\n")

    @patch("data_juicer.core.export_manager.append_csv_to_lark_sheet")
    @patch("data_juicer.core.export_manager.upload_file_to_lark_sheet")
    def test_lark_export_append_mode_appends_staged_csv_rows(
        self,
        mock_upload_file_to_lark_sheet,
        mock_append_csv_to_lark_sheet,
    ):
        cfg = self._make_cfg(
            {
                "target": "lark",
                "lark_path": "https://example.feishu.cn/sheets/shtcn123?sheet=abc",
                "lark_app_id": "app_id",
                "lark_app_secret": "app_secret",
                "type": "csv",
                "mode": "append",
                "skip_header": True,
            }
        )
        manager = ExportManager(cfg, executor_type="default")

        class CsvDataset:
            features = {"text": object()}

            def remove_columns(self, columns):
                return self

            def to_csv(self, export_path, num_proc=1, storage_options=None):
                with open(export_path, "w") as fout:
                    fout.write("text\nhello_process_by_dj\n")

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("data_juicer.core.export_manager.make_staging_dir", return_value=tmp_dir):
                manager._export_to_lark(CsvDataset())
            staged_path = mock_append_csv_to_lark_sheet.call_args.kwargs["local_path"]
            self.assertEqual(os.path.basename(staged_path), "dataset.csv")
            with open(staged_path) as fin:
                self.assertEqual(fin.read(), "text\nhello_process_by_dj\n")

        mock_append_csv_to_lark_sheet.assert_called_once_with(
            local_path=staged_path,
            lark_path="https://example.feishu.cn/sheets/shtcn123?sheet=abc",
            lark_app_id="app_id",
            lark_app_secret="app_secret",
            cell_range=None,
            skip_header=True,
        )
        mock_upload_file_to_lark_sheet.assert_not_called()

    def test_lark_file_export_requires_range(self):
        cfg = self._make_cfg(
            {
                "target": "lark",
                "lark_path": "https://example.feishu.cn/sheets/shtcn123?sheet=abc",
                "lark_app_id": "app_id",
                "lark_app_secret": "app_secret",
            }
        )
        manager = ExportManager(cfg, executor_type="default")

        class CsvDataset:
            features = {"text": object()}

            def remove_columns(self, columns):
                return self

            def to_csv(self, export_path, num_proc=1, storage_options=None):
                with open(export_path, "w") as fout:
                    fout.write("text\nhello\n")

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("data_juicer.core.export_manager.make_staging_dir", return_value=tmp_dir):
                with self.assertRaisesRegex(ValueError, "requires `range`"):
                    manager._export_to_lark(CsvDataset())

    @patch("data_juicer.core.export_manager.overwrite_csv_to_lark_sheet")
    def test_lark_export_overwrite_mode_writes_staged_csv_rows(self, mock_overwrite_csv_to_lark_sheet):
        cfg = self._make_cfg(
            {
                "target": "lark",
                "lark_path": "https://example.feishu.cn/sheets/shtcn123?sheet=abc",
                "lark_app_id": "app_id",
                "lark_app_secret": "app_secret",
                "type": "csv",
                "mode": "overwrite",
                "range": "B2",
                "skip_header": False,
            }
        )
        manager = ExportManager(cfg, executor_type="default")

        class CsvDataset:
            features = {"text": object()}

            def remove_columns(self, columns):
                return self

            def to_csv(self, export_path, num_proc=1, storage_options=None):
                with open(export_path, "w") as fout:
                    fout.write("text\nhello_process_by_dj\n")

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("data_juicer.core.export_manager.make_staging_dir", return_value=tmp_dir):
                result = manager._export_to_lark(CsvDataset())
            staged_path = mock_overwrite_csv_to_lark_sheet.call_args.kwargs["local_path"]

        self.assertEqual(result, "https://example.feishu.cn/sheets/shtcn123?sheet=abc")
        mock_overwrite_csv_to_lark_sheet.assert_called_once_with(
            local_path=staged_path,
            lark_path="https://example.feishu.cn/sheets/shtcn123?sheet=abc",
            lark_app_id="app_id",
            lark_app_secret="app_secret",
            cell_range="B2",
            skip_header=False,
            clear_sheet=True,
        )

    @patch("data_juicer.core.export_manager.overwrite_csv_to_lark_sheet")
    @patch("data_juicer.core.export_manager.create_lark_spreadsheet")
    def test_lark_export_can_create_spreadsheet_then_overwrite(
        self,
        mock_create_lark_spreadsheet,
        mock_overwrite_csv_to_lark_sheet,
    ):
        mock_create_lark_spreadsheet.return_value = "https://example.feishu.cn/sheets/newtoken?sheet=abc"
        cfg = self._make_cfg(
            {
                "target": "lark",
                "create_spreadsheet": True,
                "spreadsheet_title": "dj-e2e",
                "lark_app_id": "app_id",
                "lark_app_secret": "app_secret",
                "type": "csv",
                "mode": "overwrite",
            }
        )
        manager = ExportManager(cfg, executor_type="default")

        class CsvDataset:
            features = {"text": object()}

            def remove_columns(self, columns):
                return self

            def to_csv(self, export_path, num_proc=1, storage_options=None):
                with open(export_path, "w") as fout:
                    fout.write("text\nhello_process_by_dj\n")

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("data_juicer.core.export_manager.make_staging_dir", return_value=tmp_dir):
                result = manager._export_to_lark(CsvDataset())

        self.assertEqual(result, "https://example.feishu.cn/sheets/newtoken?sheet=abc")
        mock_create_lark_spreadsheet.assert_called_once_with(
            lark_app_id="app_id",
            lark_app_secret="app_secret",
            title="dj-e2e",
        )
        mock_overwrite_csv_to_lark_sheet.assert_called_once()

    def test_format_partition_spec(self):
        self.assertEqual(
            ExportManager._format_partition_spec({"dt": "20260101", "region": "cn"}),
            "dt='20260101', region='cn'",
        )
        self.assertEqual(ExportManager._format_partition_spec("dt='20260101'"), "dt='20260101'")

    def test_ray_hive_export_uses_write_hive_table(self):
        ray_dataset = RayLikeDataset(["id", "name"])
        cfg = self._make_cfg(
            {
                "target": "hive",
                "table_name": "db.table_name",
                "partition": {"date": "20260426"},
                "mode": "overwrite",
                "auto_cast_schema": True,
                "concurrency": 100,
                "ray_remote_args": {"num_cpus": 2},
                "arrow_parquet_args": {"compression": "snappy"},
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        manager._export_to_hive(dataset=ray_dataset, columns=None)

        ray_dataset.write_hive_table.assert_called_once_with(
            table_name="db.table_name",
            partition={"date": "20260426"},
            mode="overwrite",
            auto_cast_schema=True,
            concurrency=100,
            ray_remote_args={"num_cpus": 2},
            arrow_parquet_args={"compression": "snappy"},
        )

    def test_ray_hive_export_removes_stats_and_hash_columns(self):
        cleaned_dataset = RayLikeDataset(["id"])
        ray_dataset = RayLikeDataset(["id", Fields.stats, Fields.meta, HashKeys.hash])
        ray_dataset.drop_columns.return_value = cleaned_dataset
        cleaned_dataset.select_columns.return_value = cleaned_dataset
        cfg = self._make_cfg({"target": "hive", "table_name": "db.table_name"})
        manager = ExportManager(cfg, executor_type="ray")

        manager._export_to_hive(dataset=ray_dataset, columns=["id", Fields.stats, HashKeys.hash])

        ray_dataset.drop_columns.assert_called_once_with([Fields.stats, Fields.meta, HashKeys.hash])
        cleaned_dataset.select_columns.assert_called_once_with(["id"])
        cleaned_dataset.write_hive_table.assert_called_once_with(table_name="db.table_name")

    def test_prepare_export_with_ray_checkpoint_uses_schema_without_columns_fetch(self):
        class StrictRayDataset:
            def __init__(self):
                self.drop_columns = MagicMock(return_value=self)

            def columns(self):
                raise AssertionError("checkpoint mode must not fetch columns before export")

        cfg = self._make_cfg(
            {
                "target": "magnus",
                "table_name": "catalog.db.table",
                "schema": {
                    "fields": [
                        {"name": "id", "type": "string"},
                        {"name": Fields.stats, "type": "struct<>"},
                    ]
                },
                "magnus_conf": {},
            }
        )
        manager = ExportManager(cfg, executor_type="ray")
        dataset = StrictRayDataset()

        prepared, columns = manager._prepare_dataset_for_export(dataset, columns=None)

        self.assertIs(prepared, dataset)
        self.assertIsNone(columns)
        dataset.drop_columns.assert_called_once_with([Fields.stats])

    def test_prepare_ray_export_without_known_columns_does_not_fetch_eagerly(self):
        class LazyRayDataset:
            def __init__(self):
                self.drop_columns = MagicMock(return_value=self)

            def columns(self, *args, **kwargs):
                if kwargs.get("fetch_if_missing") is False:
                    return None
                raise AssertionError("Ray export preparation must not eagerly fetch columns")

            def schema(self, *args, **kwargs):
                if kwargs.get("fetch_if_missing") is False:
                    return None
                raise AssertionError("Ray export preparation must not eagerly fetch schema")

        cfg = self._make_cfg(
            {
                "target": "magnus",
                "table_name": "catalog.db.table",
                "magnus_conf": {},
            }
        )
        manager = ExportManager(cfg, executor_type="ray")
        dataset = LazyRayDataset()

        prepared, columns = manager._prepare_dataset_for_export(dataset, columns=None)

        self.assertIs(prepared, dataset)
        self.assertIsNone(columns)
        dataset.drop_columns.assert_not_called()

    @patch("data_juicer.core.export_manager.write_ray_dataset_to_magnus")
    def test_magnus_export_with_ray_checkpoint_uses_passed_columns_without_fetch(
        self, mock_write_ray_dataset_to_magnus
    ):
        class StrictRayDataset:
            def __init__(self):
                self.drop_columns = MagicMock(return_value=self)

            def columns(self):
                raise AssertionError("checkpoint mode must not fetch columns before Magnus export")

        cfg = self._make_cfg(
            {
                "target": "magnus",
                "table_name": "catalog.db.table",
                "schema": {"fields": [{"name": "id", "type": "string"}]},
                "magnus_conf": {},
            }
        )
        manager = ExportManager(cfg, executor_type="ray")
        dataset = StrictRayDataset()

        manager.export(dataset, columns=["id", Fields.stats])

        dataset.drop_columns.assert_called_once_with([Fields.stats])
        mock_write_ray_dataset_to_magnus.assert_called_once_with(
            dataset,
            "catalog.db.table",
            partition_columns=None,
            partition_values=None,
            schema={"fields": [{"name": "id", "type": "string"}]},
            magnus_conf={},
            create_table_if_not_exists=False,
            magnus_failure_policy="abort",
            operation="APPEND",
            validate_overwrite_partition_before_write=False,
            infer_schema_on_create=False,
        )

    def test_default_hive_export_requires_ray_executor(self):
        cfg = self._make_cfg({"target": "hive", "table_name": "db.table_name"})
        manager = ExportManager(cfg, executor_type="default")

        with self.assertRaisesRegex(RuntimeError, "executor_type: ray"):
            manager._export_to_hive(dataset=object(), columns=None)

    def test_hive_export_requires_write_hive_table_api(self):
        class RayDatasetWithoutHive:
            def columns(self):
                return ["id"]

        cfg = self._make_cfg({"target": "hive", "table_name": "db.table_name"})
        manager = ExportManager(cfg, executor_type="ray")

        with self.assertRaisesRegex(ImportError, "bytedray"):
            manager._export_to_hive(dataset=RayDatasetWithoutHive(), columns=None)

    def test_hive_export_no_longer_registers_partition_with_tqs(self):
        ray_dataset = RayLikeDataset(["id"])
        cfg = self._make_cfg(
            {
                "target": "hive",
                "table_name": "db.table_name",
                "partition": {"date": "20260426"},
                "path": "hdfs://cluster/legacy/staging",
                "tqs_app_id": "app_id",
                "tqs_app_key": "app_key",
                "user_name": "tester",
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        with patch.object(manager, "_export_to_hdfs") as mock_export_to_hdfs:
            manager._export_to_hive(dataset=ray_dataset, columns=None)

        mock_export_to_hdfs.assert_not_called()
        ray_dataset.write_hive_table.assert_called_once_with(
            table_name="db.table_name",
            partition={"date": "20260426"},
        )

    @patch("data_juicer.core.export_manager.write_ray_dataset_to_magnus")
    def test_magnus_export_passes_operation_to_ray_writer(self, mock_write_ray_dataset_to_magnus):
        class RayLikeDataset:
            def columns(self):
                return ["id", "name"]

        cfg = self._make_cfg(
            {
                "target": "magnus",
                "table_name": "catalog.db.table",
                "operation": "OVERWRITE",
                "partition_columns": ["p_date"],
                "partition_values": {"p_date": "20260421"},
                "schema": {"fields": [{"name": "id", "type": "string"}, {"name": "p_date", "type": "string"}]},
                "magnus_conf": {"write_options": {"write.format.default": "lance"}},
                "validate_overwrite_partition_before_write": True,
            }
        )
        manager = ExportManager(cfg, executor_type="ray")
        dataset = RayLikeDataset()

        manager._export_to_magnus(dataset)

        mock_write_ray_dataset_to_magnus.assert_called_once_with(
            dataset,
            "catalog.db.table",
            partition_columns=["p_date"],
            partition_values={"p_date": "20260421"},
            schema={"fields": [{"name": "id", "type": "string"}, {"name": "p_date", "type": "string"}]},
            magnus_conf={"write_options": {"write.format.default": "lance"}},
            create_table_if_not_exists=False,
            magnus_failure_policy="abort",
            operation="OVERWRITE",
            validate_overwrite_partition_before_write=True,
            infer_schema_on_create=False,
        )

    @patch("data_juicer.core.export_manager.write_ray_dataset_to_magnus")
    def test_magnus_export_passes_create_table_flag_to_ray_writer(self, mock_write_ray_dataset_to_magnus):
        class RayLikeDataset:
            def columns(self):
                return ["id", "p_date"]

        cfg = self._make_cfg(
            {
                "target": "magnus",
                "table_name": "catalog.db.table",
                "partition_columns": ["p_date"],
                "schema": {"fields": [{"name": "id", "type": "string"}, {"name": "p_date", "type": "string"}]},
                "create_table_if_not_exists": True,
                "infer_schema_on_create": True,
                "magnus_conf": {},
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        manager._export_to_magnus(RayLikeDataset())

        self.assertTrue(mock_write_ray_dataset_to_magnus.call_args.kwargs["create_table_if_not_exists"])
        self.assertTrue(mock_write_ray_dataset_to_magnus.call_args.kwargs["infer_schema_on_create"])

    @patch("data_juicer.core.export_manager.write_hf_dataset_to_magnus")
    def test_magnus_export_passes_infer_schema_flag_to_hf_writer(self, mock_write_hf_dataset_to_magnus):
        class HFDatasetLike:
            column_names = ["id", "p_date"]

        cfg = self._make_cfg(
            {
                "target": "magnus",
                "table_name": "catalog.db.table",
                "partition_columns": ["p_date"],
                "create_table_if_not_exists": True,
                "infer_schema_on_create": True,
                "magnus_conf": {},
            }
        )
        manager = ExportManager(cfg, executor_type="default")

        manager._export_to_magnus(HFDatasetLike())

        self.assertTrue(mock_write_hf_dataset_to_magnus.call_args.kwargs["create_table_if_not_exists"])
        self.assertTrue(mock_write_hf_dataset_to_magnus.call_args.kwargs["infer_schema_on_create"])

    @patch("data_juicer.core.export_manager.write_ray_dataset_to_magnus")
    def test_magnus_export_passes_failure_policy_to_ray_writer(self, mock_write_ray_dataset_to_magnus):
        class RayLikeDataset:
            def columns(self):
                return ["id"]

        cfg = self._make_cfg(
            {
                "target": "magnus",
                "table_name": "catalog.db.table",
                "magnus_failure_policy": "commit_completed_unsafe",
                "magnus_conf": {},
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        manager._export_to_magnus(RayLikeDataset())

        self.assertEqual(
            mock_write_ray_dataset_to_magnus.call_args.kwargs["magnus_failure_policy"],
            "commit_completed_unsafe",
        )
