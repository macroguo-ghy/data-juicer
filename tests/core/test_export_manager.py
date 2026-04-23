import unittest
from unittest.mock import patch

from jsonargparse import Namespace

from data_juicer.core.export_manager import ExportManager


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

    def test_format_partition_spec(self):
        self.assertEqual(
            ExportManager._format_partition_spec({"dt": "20260101", "region": "cn"}),
            "dt='20260101', region='cn'",
        )
        self.assertEqual(ExportManager._format_partition_spec("dt='20260101'"), "dt='20260101'")

    @patch("data_juicer.core.export_manager.execute_tqs_sql")
    def test_hive_export_registers_partition(self, mock_execute_tqs_sql):
        cfg = self._make_cfg(
            {
                "target": "hive",
                "path": "hdfs://cluster/path/result.parquet",
                "hive_table": "db.table_name",
                "partition": {"dt": "20260101"},
                "tqs_app_id": "app_id",
                "tqs_app_key": "app_key",
                "user_name": "tester",
            }
        )
        manager = ExportManager(cfg, executor_type="default")

        with patch.object(manager, "_export_to_hdfs") as mock_export_to_hdfs:
            manager._export_to_hive(dataset=object(), columns=None)

        mock_export_to_hdfs.assert_called_once()
        mock_execute_tqs_sql.assert_called_once()
        sql = mock_execute_tqs_sql.call_args.args[0]
        self.assertIn("alter table db.table_name add if not exists", sql.lower())
        self.assertIn("partition (dt='20260101')", sql.lower())
        self.assertIn("location 'hdfs://cluster/path/result.parquet'", sql.lower())

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
                "magnus_conf": {"write_options": {"write.format.default": "lance"}},
            }
        )
        manager = ExportManager(cfg, executor_type="ray")
        dataset = RayLikeDataset()

        manager._export_to_magnus(dataset)

        mock_write_ray_dataset_to_magnus.assert_called_once_with(
            dataset,
            "catalog.db.table",
            partition_columns=None,
            magnus_conf={"write_options": {"write.format.default": "lance"}},
            operation="OVERWRITE",
        )
