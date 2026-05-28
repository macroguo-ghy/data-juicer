import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.fs as pa_fs
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

from data_juicer.core.export_manager import ExportManager, _quota_reserve_batch
from data_juicer.core.io_utils import _flatten_dotted_options
from data_juicer.utils.constant import DATA_JUICER_INTERNAL_FIELDS, Fields, HashKeys


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


class RayQuotaDataset(RayLikeDataset):
    def __init__(self, columns):
        super().__init__(columns)
        self.map_batches = MagicMock(return_value=self)


class RayQuotaUnknownColumnsDataset(RayQuotaDataset):
    def __init__(self):
        super().__init__(columns=None)

    def columns(self, *args, **kwargs):
        if kwargs.get("fetch_if_missing") is False:
            return None
        raise AssertionError("quota reservation must not fetch columns after quota mapping")


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

    @patch("data_juicer.core.export_manager.copy_local_to_uri")
    def test_non_ray_hdfs_export_passes_webhdfs_filesystem_options(self, mock_copy_local_to_uri):
        cfg = self._make_cfg(
            {
                "target": "hdfs",
                "path": "hdfs://localhost:9000/tmp/result.parquet",
                "type": "parquet",
                "filesystem": "webhdfs",
                "webhdfs": {"host": "localhost", "port": 9870, "user": "root"},
            }
        )
        manager = ExportManager(cfg, executor_type="default")

        with patch.object(manager, "_export_via_staging", return_value="/tmp/result.parquet"):
            manager._export_to_hdfs(dataset=object(), columns=None)

        mock_copy_local_to_uri.assert_called_once_with(
            "/tmp/result.parquet",
            "hdfs://localhost:9000/tmp/result.parquet",
            filesystem="webhdfs",
            storage_options={"host": "localhost", "port": 9870, "user": "root"},
        )

    @patch("data_juicer.core.export_manager.RayExporter")
    @patch("data_juicer.core.export_manager.copy_local_to_uri")
    def test_ray_hdfs_parquet_export_uses_distributed_writer_not_staging(
        self,
        mock_copy_local_to_uri,
        mock_ray_exporter,
    ):
        cfg = self._make_cfg(
            {
                "target": "hdfs",
                "path": "hdfs://cluster/path/output_dir",
                "type": "parquet",
                "filesystem": "pyarrow",
                "mode": "overwrite",
                "extra_args": {"max_rows_per_file": 100, "concurrency": 8},
            }
        )
        manager = ExportManager(cfg, executor_type="ray")
        dataset = RayLikeDataset(["text"])
        exporter = MagicMock()
        mock_ray_exporter.return_value = exporter

        with patch.object(manager, "_export_via_staging") as mock_staging:
            manager._export_to_hdfs(dataset=dataset, columns=["text"])

        mock_staging.assert_not_called()
        mock_copy_local_to_uri.assert_not_called()
        mock_ray_exporter.assert_called_once_with(
            "hdfs://cluster/path/output_dir",
            "parquet",
            0,
            keep_stats_in_res_ds=False,
            keep_hashes_in_res_ds=False,
            filesystem="pyarrow",
            webhdfs=None,
            mode="overwrite",
            max_rows_per_file=100,
            concurrency=8,
        )
        exporter.export.assert_called_once_with(dataset, columns=["text"])

    @patch("data_juicer.core.export_manager.RayExporter")
    def test_ray_hdfs_jsonl_export_uses_distributed_writer(self, mock_ray_exporter):
        cfg = self._make_cfg(
            {
                "target": "hdfs",
                "path": "hdfs://cluster/path/output_jsonl_dir",
                "type": "jsonl",
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        manager._export_to_hdfs(dataset=RayLikeDataset(["text"]), columns=None)

        mock_ray_exporter.assert_called_once()
        self.assertEqual(mock_ray_exporter.call_args.args[:3], ("hdfs://cluster/path/output_jsonl_dir", "jsonl", 0))

    @patch("data_juicer.core.export_manager.RayHdfsFanoutDatasink")
    @patch("data_juicer.core.export_manager.get_pyarrow_filesystem")
    def test_ray_hdfs_multi_target_export_uses_single_fanout_datasink(
        self,
        mock_get_pyarrow_filesystem,
        mock_fanout_datasink,
    ):
        cfg = self._make_cfg(
            {
                "targets": [
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_a",
                        "type": "parquet",
                        "mode": "overwrite",
                        "filter_condition": "score >= 0.8",
                        "extra_args": {"compression": "snappy"},
                    },
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_b",
                        "type": "parquet",
                        "mode": "overwrite",
                        "filter_condition": "lang == 'zh'",
                    },
                ],
            }
        )
        fake_filesystem = MagicMock()
        mock_get_pyarrow_filesystem.side_effect = [
            (fake_filesystem, "/path/output_a"),
            (fake_filesystem, "/path/output_b"),
        ]
        datasink = object()
        mock_fanout_datasink.return_value = datasink
        dataset = RayLikeDataset(["text", "score", "lang", Fields.stats, HashKeys.hash])
        dataset.write_datasink = MagicMock()

        manager = ExportManager(cfg, executor_type="ray")
        manager.export(dataset, columns=["text", "score", "lang", Fields.stats, HashKeys.hash])

        dataset.write_datasink.assert_called_once_with(datasink, ray_remote_args=None, concurrency=None)
        mock_fanout_datasink.assert_called_once()
        _, kwargs = mock_fanout_datasink.call_args
        self.assertEqual(kwargs["columns"], ["text", "score", "lang"])
        self.assertEqual(kwargs["targets"][0]["path"], "/path/output_a")
        self.assertEqual(kwargs["targets"][0]["original_uri"], "hdfs://cluster/path/output_a")
        self.assertEqual(kwargs["targets"][0]["extra_args"], {"compression": "snappy"})
        self.assertEqual(kwargs["targets"][1]["condition"], "lang == 'zh'")

    @patch("data_juicer.core.export_manager.RayHdfsFanoutDatasink")
    @patch("data_juicer.core.export_manager.get_pyarrow_filesystem")
    def test_ray_hdfs_multi_target_export_uses_target_columns_from_extra_args(
        self,
        mock_get_pyarrow_filesystem,
        mock_fanout_datasink,
    ):
        cfg = self._make_cfg(
            {
                "targets": [
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_a",
                        "type": "parquet",
                        "extra_args": {
                            "columns": ["id", "videos", "md5"],
                            "compression": "snappy",
                        },
                    },
                ],
            }
        )
        fake_filesystem = MagicMock()
        mock_get_pyarrow_filesystem.return_value = (fake_filesystem, "/path/output_a")
        datasink = object()
        mock_fanout_datasink.return_value = datasink
        dataset = RayLikeDataset(["item_id", "vid"])
        dataset.write_datasink = MagicMock()

        manager = ExportManager(cfg, executor_type="ray")
        manager.export(dataset, columns=["item_id", "vid"])

        _, kwargs = mock_fanout_datasink.call_args
        self.assertEqual(kwargs["columns"], ["item_id", "vid"])
        self.assertEqual(kwargs["targets"][0]["columns"], ["id", "videos", "md5"])
        self.assertEqual(kwargs["targets"][0]["extra_args"], {"compression": "snappy"})

    @patch("data_juicer.core.export_manager.RayHdfsFanoutDatasink")
    @patch("data_juicer.core.export_manager.get_pyarrow_filesystem")
    def test_ray_hdfs_multi_target_export_passes_top_level_compact_to_datasink(
        self,
        mock_get_pyarrow_filesystem,
        mock_fanout_datasink,
    ):
        cfg = self._make_cfg(
            {
                "targets": [
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_a",
                        "type": "parquet",
                        "compact": {
                            "target_bytes_per_file": 1024,
                            "target_rows_per_file": 100,
                        },
                        "extra_args": {
                            "columns": ["id", "videos"],
                            "compression": "zstd",
                        },
                    },
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_b",
                        "type": "parquet",
                        "extra_args": {"columns": ["id", "md5"]},
                    },
                ],
                "extra_args": {"concurrency": 3},
            }
        )
        fake_filesystem = MagicMock()
        mock_get_pyarrow_filesystem.side_effect = [
            (fake_filesystem, "/path/output_a"),
            (fake_filesystem, "/path/output_b"),
        ]
        datasink = object()
        mock_fanout_datasink.return_value = datasink
        dataset = RayLikeDataset(["id", "videos", "md5"])
        dataset.write_datasink = MagicMock()

        manager = ExportManager(cfg, executor_type="ray")
        manager.export(dataset, columns=["id", "videos", "md5"])

        dataset.write_datasink.assert_called_once_with(datasink, ray_remote_args=None, concurrency=3)
        _, kwargs = mock_fanout_datasink.call_args
        compact = kwargs["targets"][0]["compact"]
        self.assertEqual(compact["target_bytes_per_file"], 1024)
        self.assertEqual(compact["target_rows_per_file"], 100)
        self.assertEqual(compact["max_buffer_bytes"], 2048)
        self.assertEqual(kwargs["targets"][0]["extra_args"], {"compression": "zstd"})
        self.assertNotIn("compact", kwargs["targets"][0]["extra_args"])
        self.assertNotIn("compact", kwargs["targets"][1])

    @patch("data_juicer.core.export_manager.RayHdfsFanoutDatasink")
    @patch("data_juicer.core.export_manager.get_pyarrow_filesystem")
    def test_ray_hdfs_multi_target_export_propagates_action_args_and_unknown_columns(
        self,
        mock_get_pyarrow_filesystem,
        mock_fanout_datasink,
    ):
        cfg = self._make_cfg(
            {
                "targets": [
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_a",
                        "type": "jsonl",
                    },
                ],
                "extra_args": {
                    "ray_remote_args": {"num_cpus": 0.5},
                    "concurrency": 4,
                },
            }
        )
        fake_filesystem = MagicMock()
        mock_get_pyarrow_filesystem.return_value = (fake_filesystem, "/path/output_a")
        datasink = object()
        mock_fanout_datasink.return_value = datasink
        dataset = RayLikeDataset(None)
        dataset.write_datasink = MagicMock()

        manager = ExportManager(cfg, executor_type="ray")
        manager.export(dataset, columns=None)

        dataset.write_datasink.assert_called_once_with(
            datasink,
            ray_remote_args={"num_cpus": 0.5},
            concurrency=4,
        )
        _, kwargs = mock_fanout_datasink.call_args
        self.assertIsNone(kwargs["columns"])

    @patch("data_juicer.core.export_manager.RayHdfsFanoutDatasink")
    @patch("data_juicer.core.export_manager.get_pyarrow_filesystem")
    def test_ray_hdfs_multi_target_export_accepts_target_concurrency_alias(
        self,
        mock_get_pyarrow_filesystem,
        mock_fanout_datasink,
    ):
        cfg = self._make_cfg(
            {
                "targets": [
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_a",
                        "type": "parquet",
                        "extra_args": {
                            "columns": ["id", "videos"],
                            "concurrency": 2,
                        },
                    },
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_b",
                        "type": "parquet",
                        "extra_args": {
                            "columns": ["id", "extra"],
                            "concurrency": 2,
                        },
                    },
                ],
            }
        )
        fake_filesystem = MagicMock()
        mock_get_pyarrow_filesystem.side_effect = [
            (fake_filesystem, "/path/output_a"),
            (fake_filesystem, "/path/output_b"),
        ]
        datasink = object()
        mock_fanout_datasink.return_value = datasink
        dataset = RayLikeDataset(["id", "videos", "extra"])
        dataset.write_datasink = MagicMock()

        manager = ExportManager(cfg, executor_type="ray")
        manager.export(dataset, columns=["id", "videos", "extra"])

        dataset.write_datasink.assert_called_once_with(datasink, ray_remote_args=None, concurrency=2)
        _, kwargs = mock_fanout_datasink.call_args
        self.assertEqual(kwargs["targets"][0]["columns"], ["id", "videos"])
        self.assertEqual(kwargs["targets"][0]["extra_args"], {})
        self.assertEqual(kwargs["targets"][1]["columns"], ["id", "extra"])
        self.assertEqual(kwargs["targets"][1]["extra_args"], {})

    @patch("data_juicer.core.export_manager.get_pyarrow_filesystem")
    def test_ray_hdfs_multi_target_export_rejects_conflicting_target_concurrency(
        self,
        mock_get_pyarrow_filesystem,
    ):
        cfg = self._make_cfg(
            {
                "targets": [
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_a",
                        "type": "parquet",
                        "extra_args": {"concurrency": 1},
                    },
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_b",
                        "type": "parquet",
                        "extra_args": {"concurrency": 2},
                    },
                ],
            }
        )
        fake_filesystem = MagicMock()
        mock_get_pyarrow_filesystem.side_effect = [
            (fake_filesystem, "/path/output_a"),
            (fake_filesystem, "/path/output_b"),
        ]
        dataset = RayLikeDataset(["id"])
        dataset.write_datasink = MagicMock()

        manager = ExportManager(cfg, executor_type="ray")

        with self.assertRaisesRegex(ValueError, "export.targets\\[\\]\\.extra_args\\.concurrency"):
            manager.export(dataset, columns=["id"])

    @patch("data_juicer.core.export_manager.get_pyarrow_filesystem")
    def test_ray_hdfs_multi_target_export_rejects_top_level_and_target_concurrency_conflict(
        self,
        mock_get_pyarrow_filesystem,
    ):
        cfg = self._make_cfg(
            {
                "extra_args": {"concurrency": 1},
                "targets": [
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_a",
                        "type": "parquet",
                        "extra_args": {"concurrency": 2},
                    },
                ],
            }
        )
        fake_filesystem = MagicMock()
        mock_get_pyarrow_filesystem.return_value = (fake_filesystem, "/path/output_a")
        dataset = RayLikeDataset(["id"])
        dataset.write_datasink = MagicMock()

        manager = ExportManager(cfg, executor_type="ray")

        with self.assertRaisesRegex(ValueError, "export.extra_args.concurrency"):
            manager.export(dataset, columns=["id"])

    @patch("data_juicer.core.export_manager.RayHdfsFanoutDatasink")
    @patch("data_juicer.core.export_manager.get_pyarrow_filesystem")
    def test_ray_hdfs_multi_target_export_stores_fanout_summary(
        self,
        mock_get_pyarrow_filesystem,
        mock_fanout_datasink,
    ):
        cfg = self._make_cfg(
            {
                "targets": [
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_a",
                        "type": "jsonl",
                    },
                ],
            }
        )
        fake_filesystem = MagicMock()
        mock_get_pyarrow_filesystem.return_value = (fake_filesystem, "/path/output_a")
        datasink = object()
        mock_fanout_datasink.return_value = datasink
        dataset = RayLikeDataset(["text"])
        dataset.write_datasink = MagicMock(
            return_value={
                "output_rows": 3,
                "output_files": 1,
                "output_bytes": 42,
            }
        )

        manager = ExportManager(cfg, executor_type="ray")
        manager.export(dataset, columns=["text"])

        self.assertEqual(manager.last_export_summary["output_rows"], 3)
        self.assertEqual(manager.last_export_summary["output_files"], 1)
        self.assertEqual(manager.last_export_summary["output_bytes"], 42)

    @patch("data_juicer.core.export_manager.RayHdfsFanoutDatasink")
    @patch("data_juicer.core.export_manager.get_pyarrow_filesystem")
    def test_ray_hdfs_multi_target_export_stores_partial_summary_on_failure(
        self,
        mock_get_pyarrow_filesystem,
        mock_fanout_datasink,
    ):
        cfg = self._make_cfg(
            {
                "targets": [
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_a",
                        "type": "jsonl",
                    },
                ],
            }
        )
        fake_filesystem = MagicMock()
        mock_get_pyarrow_filesystem.return_value = (fake_filesystem, "/path/output_a")
        datasink = MagicMock()
        datasink.partial_write_summary.return_value = {
            "partial": True,
            "output_rows": 2,
            "output_files": 1,
            "output_bytes": 42,
            "targets": [
                {
                    "path": "hdfs://cluster/path/output_a",
                    "rows": 2,
                    "output_files": 1,
                    "output_bytes": 42,
                }
            ],
        }
        mock_fanout_datasink.return_value = datasink
        dataset = RayLikeDataset(["text"])
        dataset.write_datasink = MagicMock(side_effect=RuntimeError("write failed"))

        manager = ExportManager(cfg, executor_type="ray")
        with self.assertRaisesRegex(RuntimeError, "write failed"):
            manager.export(dataset, columns=["text"])

        self.assertEqual(manager.last_export_summary["output_rows"], 2)
        self.assertEqual(manager.last_export_summary["output_files"], 1)
        self.assertEqual(manager.last_export_summary["output_bytes"], 42)
        self.assertTrue(manager.last_export_summary["partial"])

    @patch("data_juicer.core.export_manager.RayHdfsFanoutDatasink")
    @patch("data_juicer.core.export_manager.get_pyarrow_filesystem")
    def test_ray_hdfs_multi_target_current_export_summary_reads_active_fanout(
        self,
        mock_get_pyarrow_filesystem,
        mock_fanout_datasink,
    ):
        cfg = self._make_cfg(
            {
                "targets": [
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_a",
                        "type": "jsonl",
                    },
                ],
            }
        )
        fake_filesystem = MagicMock()
        mock_get_pyarrow_filesystem.return_value = (fake_filesystem, "/path/output_a")
        datasink = MagicMock()
        datasink.partial_write_summary.return_value = {
            "partial": True,
            "output_rows": 5,
            "output_files": 1,
            "output_bytes": 64,
        }
        mock_fanout_datasink.return_value = datasink
        dataset = RayLikeDataset(["text"])
        observed = []

        manager = ExportManager(cfg, executor_type="ray")

        def write_datasink(*args, **kwargs):
            observed.append(manager.current_export_summary())
            return {"output_rows": 6, "output_files": 2, "output_bytes": 80}

        dataset.write_datasink = MagicMock(side_effect=write_datasink)
        manager.export(dataset, columns=["text"])

        self.assertEqual(observed[0]["output_rows"], 5)
        self.assertTrue(observed[0]["partial"])
        self.assertEqual(manager.current_export_summary()["output_rows"], 6)

    @patch("data_juicer.core.export_manager.RayHdfsFanoutDatasink")
    @patch("data_juicer.core.export_manager.get_pyarrow_filesystem")
    def test_ray_local_multi_target_export_uses_local_filesystem(
        self,
        mock_get_pyarrow_filesystem,
        mock_fanout_datasink,
    ):
        cfg = self._make_cfg(
            {
                "targets": [
                    {
                        "target": "local",
                        "path": "./outputs/fanout_local/a",
                        "type": "jsonl",
                        "mode": "overwrite",
                        "filter_condition": "score >= 0.8",
                    },
                    {
                        "target": "local",
                        "path": "file:///tmp/fanout_local/b",
                        "type": "jsonl",
                        "mode": "append",
                        "filter_condition": "lang == 'zh'",
                    },
                ],
            }
        )
        datasink = object()
        mock_fanout_datasink.return_value = datasink
        dataset = RayLikeDataset(["text", "score", "lang"])
        dataset.write_datasink = MagicMock()

        manager = ExportManager(cfg, executor_type="ray")
        manager.export(dataset, columns=["text", "score", "lang"])

        mock_get_pyarrow_filesystem.assert_not_called()
        dataset.write_datasink.assert_called_once_with(datasink, ray_remote_args=None, concurrency=None)
        _, kwargs = mock_fanout_datasink.call_args
        targets = kwargs["targets"]
        self.assertIsInstance(targets[0]["filesystem"], pa_fs.LocalFileSystem)
        self.assertTrue(os.path.isabs(targets[0]["path"]))
        self.assertEqual(targets[0]["original_uri"], "./outputs/fanout_local/a")
        self.assertIsInstance(targets[1]["filesystem"], pa_fs.LocalFileSystem)
        self.assertEqual(targets[1]["path"], "/tmp/fanout_local/b")
        self.assertEqual(targets[1]["condition"], "lang == 'zh'")

    def test_ray_hdfs_multi_target_export_revalidates_config_bypass(self):
        cases = [
            ({
                "targets": [
                    {"target": "hdfs", "path": "hdfs://cluster/path/result.parquet", "type": "parquet"},
                ],
            }, "directory paths"),
            ({
                "targets": [
                    {"target": "hdfs", "path": "hdfs://cluster/path/output_a", "type": "parquet"},
                    {"target": "hdfs", "path": "hdfs://cluster/path/output_a", "type": "parquet"},
                ],
            }, "paths must be unique"),
            ({
                "targets": [
                    {"target": "hdfs", "path": "hdfs://cluster/path/output_a", "type": "parquet"},
                    {"target": "hdfs", "path": "hdfs://cluster/path/output_b", "type": "jsonl"},
                ],
            }, "same `type`"),
            ({
                "targets": [
                    {"target": "local", "path": "./outputs/a", "type": "parquet"},
                    {"target": "hdfs", "path": "hdfs://cluster/path/output_b", "type": "parquet"},
                ],
            }, "same `target`"),
            ({
                "targets": [
                    {
                        "target": "local",
                        "path": "./outputs/a",
                        "type": "jsonl",
                        "compact": {"target_bytes_per_file": 1024, "target_rows_per_file": 100},
                    },
                    {
                        "target": "local",
                        "path": "./outputs/b",
                        "type": "jsonl",
                        "compact": {"target_bytes_per_file": 2048, "target_rows_per_file": 100},
                    },
                ],
            }, "compact"),
            ({
                "targets": [
                    {
                        "target": "local",
                        "path": "./outputs/a",
                        "type": "jsonl",
                        "extra_args": {"compact": {"target_bytes_per_file": 1024}},
                    },
                ],
            }, "top level"),
        ]

        for export_cfg, expected_error in cases:
            manager = ExportManager(self._make_cfg(export_cfg), executor_type="ray")
            with self.assertRaisesRegex(ValueError, expected_error):
                manager.export(RayLikeDataset(["text"]))

    def test_ray_data_checkpoint_accepts_append_multi_target_export(self):
        for targets in [
            [
                {
                    "target": "hdfs",
                    "path": "hdfs://cluster/path/output_a",
                    "type": "parquet",
                    "mode": "append",
                },
                {
                    "target": "hdfs",
                    "path": "hdfs://cluster/path/output_b",
                    "type": "parquet",
                    "mode": "append",
                },
            ],
            [
                {
                    "target": "local",
                    "path": "./outputs/checkpoint_local/output_a",
                    "type": "parquet",
                    "mode": "append",
                },
                {
                    "target": "local",
                    "path": "file:///tmp/checkpoint_local/output_b",
                    "type": "parquet",
                    "mode": "append",
                },
            ],
        ]:
            cfg = self._make_cfg({"targets": targets})
            manager = ExportManager(cfg, executor_type="ray")

            with patch("data_juicer.core.export_manager.logger.warning") as mock_warning:
                manager.validate_ray_data_checkpoint_sink()

            mock_warning.assert_not_called()

    def test_ray_data_checkpoint_rejects_non_append_multi_target_export(self):
        cases = [
            (None, "error_if_exists"),
            ("error_if_exists", "error_if_exists"),
            ("overwrite", "overwrite"),
        ]

        for mode, expected_mode in cases:
            target = {
                "target": "hdfs",
                "path": "hdfs://cluster/path/output_a",
                "type": "parquet",
            }
            if mode is not None:
                target["mode"] = mode
            cfg = self._make_cfg({"targets": [target]})
            manager = ExportManager(cfg, executor_type="ray")

            with self.assertRaisesRegex(ValueError, expected_mode):
                manager.validate_ray_data_checkpoint_sink()

    def test_ray_data_checkpoint_warns_when_delete_no_checkpoint_files_with_multi_target_export(self):
        cfg = self._make_cfg(
            {
                "targets": [
                    {
                        "target": "hdfs",
                        "path": "hdfs://cluster/path/output_a",
                        "type": "jsonl",
                        "mode": "append",
                    },
                ]
            }
        )
        cfg.ray_data_checkpoint = Namespace(enabled=True, delete_no_checkpoint_files=True)
        manager = ExportManager(cfg, executor_type="ray")

        with patch("data_juicer.core.export_manager.logger.warning") as mock_warning:
            manager.validate_ray_data_checkpoint_sink()

        mock_warning.assert_called_once()
        self.assertIn("fan-out", mock_warning.call_args.args[0])
        self.assertIn("at-least-once", mock_warning.call_args.args[0])
        self.assertIn("delete_no_checkpoint_files", mock_warning.call_args.args[0])

    def test_ray_hdfs_distributed_export_rejects_file_like_path(self):
        cfg = self._make_cfg(
            {
                "target": "hdfs",
                "path": "hdfs://cluster/path/result.parquet",
                "type": "parquet",
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        with self.assertRaisesRegex(ValueError, "directory path"):
            manager._export_to_hdfs(dataset=RayLikeDataset(["text"]))

    def test_ray_hdfs_distributed_export_rejects_shard_size(self):
        cfg = self._make_cfg(
            {
                "target": "hdfs",
                "path": "hdfs://cluster/path/output_dir",
                "type": "parquet",
                "shard_size": 1024,
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        with self.assertRaisesRegex(ValueError, "export.shard_size"):
            manager._export_to_hdfs(dataset=RayLikeDataset(["text"]))

    @patch("data_juicer.core.export_manager.copy_local_to_uri")
    def test_ray_hdfs_unsupported_format_keeps_staging_export(self, mock_copy_local_to_uri):
        cfg = self._make_cfg(
            {
                "target": "hdfs",
                "path": "hdfs://cluster/path/result.csv",
                "type": "csv",
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        with patch.object(manager, "_export_via_staging", return_value="/tmp/result.csv") as mock_staging:
            manager._export_to_hdfs(dataset=RayLikeDataset(["text"]))

        mock_staging.assert_called_once()
        mock_copy_local_to_uri.assert_called_once_with(
            "/tmp/result.csv",
            "hdfs://cluster/path/result.csv",
            filesystem=None,
            storage_options=None,
        )

    def test_ray_data_checkpoint_requires_distributed_hdfs_sink_for_hdfs_export(self):
        cfg = self._make_cfg(
            {
                "target": "hdfs",
                "path": "hdfs://cluster/path/result.csv",
                "type": "csv",
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        with self.assertRaisesRegex(ValueError, "parquet/jsonl"):
            manager.validate_ray_data_checkpoint_sink()

    def test_ray_data_checkpoint_warns_when_hdfs_mode_has_limited_cross_job_recovery(self):
        cfg = self._make_cfg(
            {
                "target": "hdfs",
                "path": "hdfs://cluster/path/output_dir",
                "type": "parquet",
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        with patch("data_juicer.core.export_manager.logger.warning") as mock_warning:
            manager.validate_ray_data_checkpoint_sink()

        mock_warning.assert_called_once()
        self.assertIn("export.mode=error_if_exists", mock_warning.call_args.args[0])
        self.assertIn("cross-job recovery", mock_warning.call_args.args[0])

    def test_ray_data_checkpoint_does_not_warn_for_hdfs_append_mode(self):
        cfg = self._make_cfg(
            {
                "target": "hdfs",
                "path": "hdfs://cluster/path/output_dir",
                "type": "jsonl",
                "mode": "append",
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        with patch("data_juicer.core.export_manager.logger.warning") as mock_warning:
            manager.validate_ray_data_checkpoint_sink()

        mock_warning.assert_not_called()

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

    @patch("data_juicer.core.export_manager.write_ray_dataset_to_magnus")
    def test_export_quota_reservation_maps_ray_batches_before_magnus_export(self, mock_write_ray_dataset_to_magnus):
        cfg = self._make_cfg(
            {
                "target": "magnus",
                "table_name": "catalog.db.table",
                "magnus_conf": {},
                "max_rows": 2,
                "max_rows_mode": "quota_reservation",
                "max_rows_quota_batch_size": 4,
            }
        )
        manager = ExportManager(cfg, executor_type="ray")
        dataset = RayQuotaDataset(["id"])

        with patch("ray.remote") as mock_ray_remote:
            actor_cls = MagicMock()
            actor_handle = object()
            actor_cls.remote.return_value = actor_handle

            def remote_decorator(**kwargs):
                self.assertEqual(kwargs, {"num_cpus": 0})
                return lambda cls: actor_cls

            mock_ray_remote.side_effect = remote_decorator

            manager.export(dataset)

        mock_ray_remote.assert_called_once_with(num_cpus=0)
        actor_cls.remote.assert_called_once_with(2)
        dataset.map_batches.assert_called_once()
        _, kwargs = dataset.map_batches.call_args
        self.assertIs(kwargs["fn_kwargs"]["quota_actor"], actor_handle)
        self.assertEqual(kwargs["batch_format"], "pyarrow")
        self.assertEqual(kwargs["batch_size"], 4)
        self.assertIs(mock_write_ray_dataset_to_magnus.call_args.args[0], dataset)

    def test_export_quota_reservation_local_export_avoids_post_quota_column_fetch(self):
        cfg = self._make_cfg(
            {
                "target": "local",
                "path": "./outputs/result.parquet",
                "type": "parquet",
                "max_rows": 2,
                "max_rows_mode": "quota_reservation",
            }
        )
        manager = ExportManager(cfg, executor_type="ray")
        dataset = RayQuotaUnknownColumnsDataset()

        with patch("ray.remote") as mock_ray_remote:
            actor_cls = MagicMock()
            actor_handle = object()
            actor_cls.remote.return_value = actor_handle
            mock_ray_remote.side_effect = lambda **kwargs: (lambda cls: actor_cls)
            manager.file_exporter.export = MagicMock()

            manager.export(dataset)

        dataset.map_batches.assert_called_once()
        manager.file_exporter.export.assert_called_once_with(dataset, columns=[])

    def test_export_quota_reservation_requires_ray_map_batches(self):
        cfg = self._make_cfg(
            {
                "target": "local",
                "path": "./outputs/result.jsonl",
                "max_rows": 2,
                "max_rows_mode": "quota_reservation",
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        with self.assertRaisesRegex(RuntimeError, "map_batches"):
            manager._reserve_quota_for_export(object())

    def test_export_quota_reservation_materializes_reserved_dataset(self):
        cfg = self._make_cfg(
            {
                "target": "local",
                "path": "./outputs/result.jsonl",
                "max_rows": 2,
                "max_rows_mode": "quota_reservation",
            }
        )
        manager = ExportManager(cfg, executor_type="ray")
        dataset = RayQuotaDataset(["id"])
        materialized_dataset = object()
        dataset.materialize = MagicMock(return_value=materialized_dataset)

        with patch("ray.remote") as mock_ray_remote:
            actor_cls = MagicMock()
            mock_ray_remote.side_effect = lambda **kwargs: (lambda cls: actor_cls)

            reserved_dataset = manager._reserve_quota_for_export(dataset)

        self.assertIs(reserved_dataset, materialized_dataset)
        dataset.map_batches.assert_called_once()
        dataset.materialize.assert_called_once_with()

    def test_quota_reserve_batch_accepts_whole_batch_until_target_is_reached(self):
        table = pa.Table.from_pylist([{"id": 1}, {"id": 2}])
        quota_actor = MagicMock()
        quota_actor.reserve.remote.side_effect = ["accepted", "rejected"]

        with patch("ray.get", side_effect=[True, False]):
            accepted = _quota_reserve_batch(table, quota_actor=quota_actor)
            rejected = _quota_reserve_batch(table, quota_actor=quota_actor)

        quota_actor.reserve.remote.assert_any_call(2)
        self.assertEqual(accepted.num_rows, 2)
        self.assertEqual(rejected.num_rows, 0)
        self.assertEqual(rejected.schema, table.schema)

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
                write_options = _flatten_dotted_options(
                    ((export_config.get("magnus_conf") or {}).get("write_options") or {})
                )
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
        ray_dataset = RayLikeDataset(["id", *DATA_JUICER_INTERNAL_FIELDS, HashKeys.hash])
        ray_dataset.drop_columns.return_value = cleaned_dataset
        cleaned_dataset.select_columns.return_value = cleaned_dataset
        cfg = self._make_cfg({"target": "hive", "table_name": "db.table_name"})
        manager = ExportManager(cfg, executor_type="ray")

        manager._export_to_hive(dataset=ray_dataset, columns=["id", Fields.stats, Fields.source_file, HashKeys.hash])

        ray_dataset.drop_columns.assert_called_once_with([*DATA_JUICER_INTERNAL_FIELDS, HashKeys.hash])
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
                        {"name": Fields.source_file, "type": "string"},
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
        dataset.drop_columns.assert_called_once_with([Fields.stats, Fields.source_file])

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

        manager.export(dataset, columns=["id", Fields.stats, Fields.source_file])

        dataset.drop_columns.assert_called_once_with([Fields.stats, Fields.source_file])
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
            serialize_complex_fields=False,
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
            serialize_complex_fields=False,
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
    def test_magnus_export_passes_serialize_complex_fields_flag_to_ray_writer(
        self, mock_write_ray_dataset_to_magnus
    ):
        class RayLikeDataset:
            def columns(self):
                return ["id", "state"]

        cfg = self._make_cfg(
            {
                "target": "magnus",
                "table_name": "catalog.db.table",
                "create_table_if_not_exists": True,
                "infer_schema_on_create": True,
                "serialize_complex_fields": True,
                "magnus_conf": {},
            }
        )
        manager = ExportManager(cfg, executor_type="ray")

        manager._export_to_magnus(RayLikeDataset())

        self.assertTrue(mock_write_ray_dataset_to_magnus.call_args.kwargs["serialize_complex_fields"])

    @patch("data_juicer.core.export_manager.write_hf_dataset_to_magnus")
    def test_magnus_export_does_not_pass_serialize_complex_fields_flag_to_hf_writer(
        self, mock_write_hf_dataset_to_magnus
    ):
        class HFDatasetLike:
            column_names = ["id", "state"]

        cfg = self._make_cfg(
            {
                "target": "magnus",
                "table_name": "catalog.db.table",
                "create_table_if_not_exists": True,
                "infer_schema_on_create": True,
                "magnus_conf": {},
            }
        )
        manager = ExportManager(cfg, executor_type="default")

        manager._export_to_magnus(HFDatasetLike())

        self.assertNotIn("serialize_complex_fields", mock_write_hf_dataset_to_magnus.call_args.kwargs)

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
