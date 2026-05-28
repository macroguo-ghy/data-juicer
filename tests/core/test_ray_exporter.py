import copy
import builtins
import base64
import json
import os
import os.path as osp
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import FileInfo, FileType, LocalFileSystem

from data_juicer.utils.unittest_utils import TEST_TAG, DataJuicerTestCaseBase
from data_juicer.core.ray_exporter import (
    EXPORT_WRITE_STATS_NAMESPACE,
    RayExporter,
    RayHdfsFanoutDatasink,
)
from data_juicer.utils.constant import Fields, HashKeys
from data_juicer.utils.mm_utils import load_images_byte


class TestRayExporterCheckpoint(unittest.TestCase):
    def test_checkpoint_export_uses_supplied_columns_without_fetch(self):
        class StrictRayDataset:
            def __init__(self):
                self.drop_columns = MagicMock(return_value=self)

            def columns(self):
                raise AssertionError("checkpoint mode must not fetch columns before export")

        dataset = StrictRayDataset()
        exporter = RayExporter(
            "/tmp/checkpoint_export.json",
            keep_stats_in_res_ds=False,
            keep_hashes_in_res_ds=False,
        )
        export_method = MagicMock()

        with patch("data_juicer.core.ray_exporter._is_ray_data_checkpoint_enabled", return_value=True):
            with patch.object(RayExporter, "_router", return_value={"json": export_method}):
                exporter.export(dataset, columns=["id", Fields.stats, HashKeys.hash])

        dataset.drop_columns.assert_called_once_with([Fields.stats, HashKeys.hash])
        export_method.assert_called_once()


class TestRayExporterHDFS(unittest.TestCase):
    def test_hdfs_export_resolves_filesystem_path_and_defaults_to_error_if_exists(self):
        class FakeDataset:
            def __init__(self):
                self.drop_columns = MagicMock(return_value=self)
                self.write_parquet = MagicMock()

            def columns(self):
                return ["text"]

        dataset = FakeDataset()
        fake_filesystem = MagicMock()
        fake_filesystem.get_file_info.return_value = FileInfo("/path/output_dir", FileType.NotFound)

        with patch(
            "data_juicer.core.ray_exporter.get_pyarrow_filesystem",
            return_value=(fake_filesystem, "/path/output_dir"),
        ) as mock_get_filesystem:
            exporter = RayExporter(
                "hdfs://cluster/path/output_dir",
                export_type="parquet",
                filesystem="pyarrow",
            )
            exporter.export(dataset)

        mock_get_filesystem.assert_called_once_with(
            "hdfs://cluster/path/output_dir",
            filesystem="pyarrow",
            storage_options=None,
        )
        dataset.write_parquet.assert_called_once()
        args, kwargs = dataset.write_parquet.call_args
        self.assertEqual(args[0], "/path/output_dir")
        self.assertIs(kwargs["filesystem"], fake_filesystem)
        self.assertNotIn("mode", kwargs)
        fake_filesystem.get_file_info.assert_called_once_with("/path/output_dir")

    def test_hdfs_mode_resolution_does_not_depend_on_ray_savemode_module(self):
        real_import = builtins.__import__

        def fail_savemode_import(name, *args, **kwargs):
            if name == "ray.data._internal.savemode":
                raise ModuleNotFoundError("No module named 'ray.data._internal.savemode'")
            return real_import(name, *args, **kwargs)

        fake_filesystem = MagicMock()
        fake_filesystem.get_file_info.return_value = FileInfo("/path/output_dir", FileType.NotFound)

        with (
            patch("builtins.__import__", side_effect=fail_savemode_import),
            patch(
                "data_juicer.core.ray_exporter.get_pyarrow_filesystem",
                return_value=(fake_filesystem, "/path/output_dir"),
            ),
        ):
            RayExporter(
                "hdfs://cluster/path/output_dir",
                export_type="parquet",
                filesystem="pyarrow",
                mode="error_if_exists",
            )

    def test_hdfs_jsonl_append_maps_mode_and_warns(self):
        class FakeDataset:
            def __init__(self):
                self.write_datasink = MagicMock()

            def columns(self):
                return ["text"]

        dataset = FakeDataset()
        fake_filesystem = LocalFileSystem()

        with (
            patch(
                "data_juicer.core.ray_exporter.get_pyarrow_filesystem",
                return_value=(fake_filesystem, "/path/output_jsonl_dir"),
            ),
            patch("data_juicer.core.ray_exporter.logger.warning") as mock_warning,
        ):
            exporter = RayExporter(
                "hdfs://cluster/path/output_jsonl_dir",
                export_type="jsonl",
                filesystem="pyarrow",
                mode="append",
            )
            exporter.export(dataset)

        dataset.write_datasink.assert_called_once()
        datasink = dataset.write_datasink.call_args.args[0]
        self.assertEqual(datasink.path, "/path/output_jsonl_dir")
        if getattr(datasink, "mode", None) is not None:
            self.assertEqual(datasink.mode.value, "append")
        mock_warning.assert_called_once()

    def test_hdfs_append_generates_unique_default_filenames_across_submissions(self):
        class FakeDataset:
            def __init__(self):
                self.filenames = []

            def columns(self):
                return ["text"]

            def write_parquet(self, path, *, filename_provider=None, filesystem=None):
                self.filenames.append(filename_provider.get_filename_for_task("0", 0))

        fake_filesystem = MagicMock()
        fake_filesystem.get_file_info.return_value = FileInfo("/path/output_dir", FileType.Directory)
        datasets = [FakeDataset(), FakeDataset()]

        with patch(
            "data_juicer.core.ray_exporter.get_pyarrow_filesystem",
            return_value=(fake_filesystem, "/path/output_dir"),
        ):
            for dataset in datasets:
                RayExporter(
                    "hdfs://cluster/path/output_dir",
                    export_type="parquet",
                    filesystem="pyarrow",
                    mode="append",
                ).export(dataset)

        self.assertEqual(len(datasets[0].filenames), 1)
        self.assertEqual(len(datasets[1].filenames), 1)
        self.assertNotEqual(datasets[0].filenames[0], datasets[1].filenames[0])
        self.assertTrue(datasets[0].filenames[0].endswith(".parquet"))

    def test_hdfs_overwrite_deletes_existing_directory_before_write(self):
        fake_filesystem = MagicMock()
        fake_filesystem.get_file_info.return_value = FileInfo("/path/output_dir", FileType.Directory)

        with patch(
            "data_juicer.core.ray_exporter.get_pyarrow_filesystem",
            return_value=(fake_filesystem, "/path/output_dir"),
        ):
            RayExporter(
                "hdfs://cluster/path/output_dir",
                export_type="parquet",
                mode="overwrite",
            )

        fake_filesystem.delete_dir.assert_called_once_with("/path/output_dir")

    def test_hdfs_jsonl_datasink_drops_mode_for_older_ray_file_datasink(self):
        class FakeDataset:
            def write_datasink(self, *args, **kwargs):
                pass

        with patch("data_juicer.core.ray_exporter._JsonlDatasink") as mock_datasink:
            RayExporter.write_jsonl_datasink(
                FakeDataset(),
                "/path/output_jsonl_dir",
                {
                    "filesystem": LocalFileSystem(),
                    "mode": "append",
                    "num_rows_per_file": 100,
                },
            )

        _, kwargs = mock_datasink.call_args
        self.assertNotIn("mode", kwargs)

    def test_write_others_drops_max_rows_per_file_for_older_ray_parquet_signature(self):
        class FakeDataset:
            def __init__(self):
                self.received_kwargs = None

            def write_parquet(self, path, *, min_rows_per_file=None, concurrency=None, **arrow_parquet_args):
                self.received_kwargs = {
                    "path": path,
                    "min_rows_per_file": min_rows_per_file,
                    "concurrency": concurrency,
                    "arrow_parquet_args": arrow_parquet_args,
                }

        dataset = FakeDataset()
        RayExporter.write_others(
            dataset,
            "/path/output_dir",
            export_format="parquet",
            export_extra_args={"max_rows_per_file": 1000, "concurrency": 8},
        )

        self.assertIsNone(dataset.received_kwargs["min_rows_per_file"])
        self.assertEqual(dataset.received_kwargs["concurrency"], 8)
        self.assertNotIn("max_rows_per_file", dataset.received_kwargs["arrow_parquet_args"])

    def test_hdfs_export_rejects_unknown_mode(self):
        with self.assertRaisesRegex(ValueError, "export.mode"):
            RayExporter(
                "hdfs://cluster/path/output_dir",
                export_type="parquet",
                mode="bad-mode",
            )

    def test_hdfs_error_if_exists_rejects_existing_path_before_write(self):
        fake_filesystem = MagicMock()
        fake_filesystem.get_file_info.return_value = FileInfo("/path/output_dir", FileType.Directory)

        with (
            patch(
                "data_juicer.core.ray_exporter.get_pyarrow_filesystem",
                return_value=(fake_filesystem, "/path/output_dir"),
            ),
            self.assertRaisesRegex(FileExistsError, "already exists"),
        ):
            RayExporter(
                "hdfs://cluster/path/output_dir",
                export_type="parquet",
                mode="error_if_exists",
            )


class TestRayHdfsFanoutDatasink(unittest.TestCase):
    def _ctx(self, task_idx=0):
        return type("Ctx", (), {"task_idx": task_idx})()

    def _target(
        self,
        path,
        *,
        filesystem=None,
        export_type="jsonl",
        mode="error_if_exists",
        condition="",
        extra_args=None,
        compact=False,
    ):
        target = {
            "path": path,
            "original_uri": path,
            "filesystem": filesystem or LocalFileSystem(),
            "type": export_type,
            "mode": mode,
            "condition": condition,
            "extra_args": extra_args or {},
        }
        if compact is not None:
            target["compact"] = compact
        return target

    def _data_files(self, output_dir, suffix):
        return sorted(
            os.path.join(output_dir, name)
            for name in os.listdir(output_dir)
            if name.endswith(suffix)
        )

    def _compact_config(self, *, bytes_per_file=1024 * 1024, rows_per_file=100, max_buffer_bytes=None):
        compact = {
            "target_bytes_per_file": bytes_per_file,
            "target_rows_per_file": rows_per_file,
        }
        if max_buffer_bytes is not None:
            compact["max_buffer_bytes"] = max_buffer_bytes
        return compact

    def test_fanout_parquet_writes_matching_rows_to_each_target(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_a = os.path.join(tmp_dir, "a")
            output_b = os.path.join(tmp_dir, "b")
            datasink = RayHdfsFanoutDatasink(
                targets=[
                    {
                        "path": output_a,
                        "original_uri": output_a,
                        "filesystem": LocalFileSystem(),
                        "type": "parquet",
                        "mode": "error_if_exists",
                        "condition": "score >= 0.8",
                        "extra_args": {},
                    },
                    {
                        "path": output_b,
                        "original_uri": output_b,
                        "filesystem": LocalFileSystem(),
                        "type": "parquet",
                        "mode": "error_if_exists",
                        "condition": "lang == 'zh'",
                        "extra_args": {},
                    },
                ],
                columns=["id", "score", "lang"],
            )
            table = pa.table(
                {
                    "id": [1, 2, 3],
                    "score": [0.9, 0.1, 0.95],
                    "lang": ["en", "zh", "zh"],
                    Fields.stats: [{"x": 1}, {"x": 2}, {"x": 3}],
                }
            )

            datasink.on_write_start(table.schema)
            datasink.write([table], self._ctx())

            rows_a = pq.read_table(output_a).to_pylist()
            rows_b = pq.read_table(output_b).to_pylist()
            self.assertEqual([row["id"] for row in rows_a], [1, 3])
            self.assertEqual([row["id"] for row in rows_b], [2, 3])
            self.assertNotIn(Fields.stats, rows_a[0])

    def test_fanout_jsonl_write_failure_is_propagated(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_a = os.path.join(tmp_dir, "a")
            output_b = os.path.join(tmp_dir, "b")
            failing_filesystem = MagicMock()
            failing_filesystem.get_file_info.return_value = FileInfo(output_b, FileType.NotFound)
            failing_filesystem.create_dir.side_effect = RuntimeError("cannot create target")
            datasink = RayHdfsFanoutDatasink(
                targets=[
                    {
                        "path": output_a,
                        "original_uri": output_a,
                        "filesystem": LocalFileSystem(),
                        "type": "jsonl",
                        "mode": "error_if_exists",
                        "condition": "",
                        "extra_args": {},
                    },
                    {
                        "path": output_b,
                        "original_uri": output_b,
                        "filesystem": failing_filesystem,
                        "type": "jsonl",
                        "mode": "error_if_exists",
                        "condition": "",
                        "extra_args": {},
                    },
                ],
                columns=["id"],
            )

            with self.assertRaisesRegex(RuntimeError, "cannot create target"):
                datasink.on_write_start(pa.schema([pa.field("id", pa.int64())]))

    def test_fanout_jsonl_writes_matching_rows(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = os.path.join(tmp_dir, "jsonl")
            datasink = RayHdfsFanoutDatasink(
                targets=[
                    {
                        "path": output_dir,
                        "original_uri": output_dir,
                        "filesystem": LocalFileSystem(),
                        "type": "jsonl",
                        "mode": "error_if_exists",
                        "condition": "score >= 0.8",
                        "extra_args": {},
                    },
                ],
                columns=["id", "score"],
            )
            table = pa.table({"id": [1, 2], "score": [0.9, 0.1], Fields.stats: [{"x": 1}, {"x": 2}]})

            datasink.on_write_start(table.schema)
            datasink.write([table], self._ctx())

            files = [os.path.join(output_dir, name) for name in os.listdir(output_dir)]
            self.assertEqual(len(files), 1)
            with open(files[0], "r", encoding="utf-8") as file:
                rows = [json.loads(line) for line in file]
            self.assertEqual(rows, [{"id": 1, "score": 0.9}])

    def test_fanout_respects_empty_export_columns(self):
        datasink = RayHdfsFanoutDatasink(
            targets=[
                {
                    "path": "/unused",
                    "original_uri": "/unused",
                    "filesystem": LocalFileSystem(),
                    "type": "jsonl",
                    "mode": "error_if_exists",
                    "condition": "",
                    "extra_args": {},
                },
            ],
            columns=[],
        )
        table = pa.table({"id": [1], Fields.stats: [{"x": 1}]})

        selected = datasink._select_export_columns(table)

        self.assertEqual(selected.schema.names, [])
        self.assertEqual(selected.num_rows, 1)

    def test_fanout_target_columns_override_global_export_columns(self):
        datasink = RayHdfsFanoutDatasink(
            targets=[
                {
                    "path": "/unused",
                    "original_uri": "/unused",
                    "filesystem": LocalFileSystem(),
                    "type": "jsonl",
                    "mode": "error_if_exists",
                    "condition": "",
                    "columns": ["id", "videos", "md5"],
                    "extra_args": {},
                },
            ],
            columns=["item_id", "vid"],
        )
        table = pa.table(
            {
                "item_id": [1],
                "vid": ["v1"],
                "id": ["item_id-1"],
                "videos": [[b"video"]],
                "md5": ["abc"],
            }
        )

        selected = datasink._select_export_columns(table, datasink.targets[0])

        self.assertEqual(selected.schema.names, ["id", "videos", "md5"])

    def test_fanout_error_if_exists_preflights_all_targets_before_mutating(self):
        fs_overwrite = MagicMock()
        fs_existing = MagicMock()
        fs_overwrite.get_file_info.return_value = FileInfo("/path/a", FileType.Directory)
        fs_existing.get_file_info.return_value = FileInfo("/path/b", FileType.Directory)
        datasink = RayHdfsFanoutDatasink(
            targets=[
                self._target("/path/a", filesystem=fs_overwrite, mode="overwrite"),
                self._target("/path/b", filesystem=fs_existing, mode="error_if_exists"),
            ],
            columns=["id"],
        )

        with self.assertRaisesRegex(FileExistsError, "already exists"):
            datasink.on_write_start(pa.schema([pa.field("id", pa.int64())]))

        fs_overwrite.delete_dir.assert_not_called()
        fs_overwrite.delete_file.assert_not_called()
        fs_overwrite.create_dir.assert_not_called()
        fs_existing.create_dir.assert_not_called()

    def test_fanout_on_write_start_handles_modes_and_existing_path_types(self):
        cases = [
            ("overwrite", FileType.Directory, "delete_dir", True),
            ("overwrite", FileType.File, "delete_file", True),
            ("append", FileType.Directory, None, False),
            ("error_if_exists", FileType.NotFound, None, True),
        ]
        for mode, file_type, delete_method, created_dir in cases:
            filesystem = MagicMock()
            filesystem.get_file_info.return_value = FileInfo("/path/output", file_type)
            datasink = RayHdfsFanoutDatasink(
                targets=[self._target("/path/output", filesystem=filesystem, mode=mode)],
                columns=["id"],
            )

            datasink.on_write_start(pa.schema([pa.field("id", pa.int64())]))

            self.assertEqual(datasink.targets[0]["created_dir"], created_dir)
            if delete_method is None:
                filesystem.delete_dir.assert_not_called()
                filesystem.delete_file.assert_not_called()
            else:
                getattr(filesystem, delete_method).assert_called_once_with("/path/output")
            filesystem.create_dir.assert_called_once_with("/path/output", recursive=True)

    def test_fanout_error_if_exists_rejects_existing_file(self):
        filesystem = MagicMock()
        filesystem.get_file_info.return_value = FileInfo("/path/output", FileType.File)
        datasink = RayHdfsFanoutDatasink(
            targets=[self._target("/path/output", filesystem=filesystem, mode="error_if_exists")],
            columns=["id"],
        )

        with self.assertRaisesRegex(FileExistsError, "already exists"):
            datasink.on_write_start(pa.schema([pa.field("id", pa.int64())]))

        filesystem.create_dir.assert_not_called()

    def test_fanout_on_write_complete_removes_only_new_empty_targets(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            empty_dir = os.path.join(tmp_dir, "empty")
            non_empty_dir = os.path.join(tmp_dir, "non_empty")
            append_dir = os.path.join(tmp_dir, "append")
            for path in (empty_dir, non_empty_dir, append_dir):
                os.makedirs(path, exist_ok=True)
            datasink = RayHdfsFanoutDatasink(
                targets=[
                    self._target(empty_dir, mode="error_if_exists"),
                    self._target(non_empty_dir, mode="error_if_exists"),
                    self._target(append_dir, mode="append"),
                ],
                columns=["id"],
            )
            datasink.targets[0]["created_dir"] = True
            datasink.targets[1]["created_dir"] = True
            datasink.targets[2]["created_dir"] = False
            write_result = type("WriteResult", (), {"write_returns": [{0: 0, 1: 1, 2: 0}]})()

            datasink.on_write_complete(write_result)

            self.assertFalse(os.path.exists(empty_dir))
            self.assertTrue(os.path.exists(non_empty_dir))
            self.assertTrue(os.path.exists(append_dir))

    def test_fanout_on_write_complete_returns_rows_files_and_bytes_summary(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = os.path.join(tmp_dir, "jsonl")
            datasink = RayHdfsFanoutDatasink(
                targets=[self._target(output_dir, export_type="jsonl")],
                columns=["id"],
            )
            table = pa.table({"id": [1, 2]})
            datasink.on_write_start(table.schema)
            write_return = datasink.write([table], self._ctx())
            write_result = type("WriteResult", (), {"write_returns": [write_return]})()

            summary = datasink.on_write_complete(write_result)

            self.assertEqual(summary["output_rows"], 2)
            self.assertEqual(summary["output_files"], 1)
            self.assertGreater(summary["output_bytes"], 0)
            self.assertEqual(summary["targets"][0]["rows"], 2)
            self.assertEqual(summary["targets"][0]["output_files"], 1)

    def test_fanout_missing_compact_preserves_direct_per_block_writes(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = os.path.join(tmp_dir, "missing_compact")
            datasink = RayHdfsFanoutDatasink(
                targets=[
                    self._target(
                        output_dir,
                        export_type="jsonl",
                        compact=None,
                    )
                ],
                columns=["id"],
            )
            blocks = [
                pa.table({"id": [1]}),
                pa.table({"id": [2]}),
                pa.table({"id": [3]}),
            ]

            datasink.on_write_start(blocks[0].schema)
            self.assertIsNone(datasink.min_rows_per_write)
            write_return = datasink.write(blocks, self._ctx(task_idx=3))
            summary = datasink.on_write_complete(type("WriteResult", (), {"write_returns": [write_return]})())

            files = self._data_files(output_dir, ".jsonl")
            self.assertEqual(len(files), 3)
            self.assertEqual(summary["targets"][0]["rows"], 3)
            self.assertNotIn("compact", summary["targets"][0])

    def test_fanout_compact_false_preserves_direct_per_block_writes(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = os.path.join(tmp_dir, "direct_jsonl")
            datasink = RayHdfsFanoutDatasink(
                targets=[
                    self._target(
                        output_dir,
                        export_type="jsonl",
                        compact=False,
                    )
                ],
                columns=["id"],
            )
            blocks = [
                pa.table({"id": [1]}),
                pa.table({"id": [2]}),
                pa.table({"id": [3]}),
            ]

            datasink.on_write_start(blocks[0].schema)
            self.assertIsNone(datasink.min_rows_per_write)
            write_return = datasink.write(blocks, self._ctx())
            summary = datasink.on_write_complete(type("WriteResult", (), {"write_returns": [write_return]})())

            files = self._data_files(output_dir, ".jsonl")
            self.assertEqual(len(files), 3)
            self.assertNotIn("compact", summary["targets"][0])

    def test_fanout_write_failure_after_partial_target_write_is_propagated(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_a = os.path.join(tmp_dir, "a")
            output_b = os.path.join(tmp_dir, "b")
            failing_filesystem = MagicMock()
            failing_filesystem.get_file_info.return_value = FileInfo(output_b, FileType.NotFound)
            failing_filesystem.open_output_stream.side_effect = RuntimeError("cannot write target")
            datasink = RayHdfsFanoutDatasink(
                targets=[
                    self._target(output_a, condition="score >= 0.8"),
                    self._target(output_b, filesystem=failing_filesystem, condition="score >= 0.8"),
                ],
                columns=["id", "score"],
            )
            table = pa.table({"id": [1], "score": [0.9]})
            datasink.on_write_start(table.schema)

            with self.assertRaisesRegex(RuntimeError, "cannot write target"):
                datasink.write([table], self._ctx())

            self.assertEqual(len(os.listdir(output_a)), 1)

    @patch("data_juicer.core.ray_exporter.incr_task_kv")
    def test_fanout_write_failure_records_partial_write_stats(self, incr_mock):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_a = os.path.join(tmp_dir, "a")
            output_b = os.path.join(tmp_dir, "b")
            failing_filesystem = MagicMock()
            failing_filesystem.get_file_info.return_value = FileInfo(output_b, FileType.NotFound)
            failing_filesystem.open_output_stream.side_effect = RuntimeError("cannot write target")
            datasink = RayHdfsFanoutDatasink(
                targets=[
                    self._target(output_a, condition="score >= 0.8"),
                    self._target(output_b, filesystem=failing_filesystem, condition="score >= 0.8"),
                ],
                columns=["id", "score"],
            )
            table = pa.table({"id": [1, 2], "score": [0.9, 0.7]})
            datasink.on_write_start(table.schema)

            with self.assertRaisesRegex(RuntimeError, "cannot write target"):
                datasink.write([table], self._ctx())

            deltas = {call.args[0]: call.args[1] for call in incr_mock.call_args_list}
            prefix = f"fanout.{datasink.write_uuid}"
            self.assertEqual(deltas[f"{prefix}.output_rows"], 1)
            self.assertEqual(deltas[f"{prefix}.output_files"], 1)
            self.assertGreater(deltas[f"{prefix}.output_bytes"], 0)
            self.assertEqual(deltas[f"{prefix}.targets.0.rows"], 1)
            self.assertEqual(deltas[f"{prefix}.targets.0.output_files"], 1)
            self.assertNotIn(f"{prefix}.targets.1.rows", deltas)
            for call in incr_mock.call_args_list:
                self.assertEqual(call.kwargs["namespace"], EXPORT_WRITE_STATS_NAMESPACE)
                self.assertTrue(call.kwargs["wait"])

    @patch("data_juicer.core.ray_exporter.snapshot_task_kv")
    def test_fanout_partial_write_summary_uses_recorded_rows_and_filesystem_metadata(self, snapshot_mock):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = os.path.join(tmp_dir, "jsonl")
            datasink = RayHdfsFanoutDatasink(
                targets=[self._target(output_dir, export_type="jsonl")],
                columns=["id"],
            )
            table = pa.table({"id": [1, 2]})
            datasink.on_write_start(table.schema)
            datasink.write([table], self._ctx())
            prefix = f"fanout.{datasink.write_uuid}"
            snapshot_mock.return_value = {
                f"{prefix}.output_rows": 2,
                f"{prefix}.targets.0.rows": 2,
            }

            summary = datasink.partial_write_summary()

            self.assertTrue(summary["partial"])
            self.assertEqual(summary["output_rows"], 2)
            self.assertEqual(summary["output_files"], 1)
            self.assertGreater(summary["output_bytes"], 0)
            self.assertEqual(summary["targets"][0]["rows"], 2)

    def test_fanout_parquet_compact_merges_blocks_and_reports_summary(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = os.path.join(tmp_dir, "compact_parquet")
            datasink = RayHdfsFanoutDatasink(
                targets=[
                    self._target(
                        output_dir,
                        export_type="parquet",
                        compact=self._compact_config(rows_per_file=10),
                    )
                ],
                columns=["id", "text"],
            )
            blocks = [
                pa.table({"id": [1], "text": ["one"]}),
                pa.table({"id": [2], "text": ["two"]}),
                pa.table({"id": [3], "text": ["three"]}),
            ]

            datasink.on_write_start(blocks[0].schema)
            self.assertEqual(datasink.min_rows_per_write, 10)
            write_return = datasink.write(blocks, self._ctx(task_idx=7))
            summary = datasink.on_write_complete(type("WriteResult", (), {"write_returns": [write_return]})())

            files = self._data_files(output_dir, ".parquet")
            self.assertEqual(len(files), 1)
            self.assertIn("-7-compact-0.parquet", os.path.basename(files[0]))
            rows = pq.read_table(output_dir).to_pylist()
            self.assertEqual([row["id"] for row in rows], [1, 2, 3])
            self.assertEqual(summary["targets"][0]["rows"], 3)
            self.assertEqual(summary["targets"][0]["compact"]["enabled"], True)
            self.assertEqual(summary["targets"][0]["compact"]["files"], 1)
            self.assertEqual(summary["targets"][0]["compact"]["flushes"], 1)
            self.assertEqual(summary["targets"][0]["compact"]["schema_mismatch_flushes"], 0)

    def test_fanout_compact_probe_logs_are_disabled_by_default(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = os.path.join(tmp_dir, "compact_default_quiet")
            with (
                patch.dict(
                    os.environ,
                    {
                        "DATA_JUICER_RAY_DEBUG_LOGS": "",
                        "DATA_JUICER_RAY_FANOUT_DEBUG": "",
                    },
                    clear=False,
                ),
                patch("data_juicer.core.ray_exporter.logger.info") as info_mock,
            ):
                datasink = RayHdfsFanoutDatasink(
                    targets=[
                        self._target(
                            output_dir,
                            export_type="parquet",
                            compact=self._compact_config(rows_per_file=10),
                        )
                    ],
                    columns=["id"],
                )
                table = pa.table({"id": [1, 2]})

                datasink.on_write_start(table.schema)
                datasink.write([table], self._ctx())

            info_mock.assert_not_called()

    def test_fanout_compact_probe_logs_can_be_enabled_for_debug_runs(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = os.path.join(tmp_dir, "compact_debug_logs")
            with (
                patch.dict(
                    os.environ,
                    {"DATA_JUICER_RAY_FANOUT_DEBUG": "1"},
                    clear=False,
                ),
                patch("data_juicer.core.ray_exporter.logger.info") as info_mock,
            ):
                datasink = RayHdfsFanoutDatasink(
                    targets=[
                        self._target(
                            output_dir,
                            export_type="parquet",
                            compact=self._compact_config(rows_per_file=10),
                        )
                    ],
                    columns=["id"],
                )
                table = pa.table({"id": [1, 2]})

                datasink.on_write_start(table.schema)
                datasink.write([table], self._ctx())

            messages = [call.args[0] for call in info_mock.call_args_list]
            self.assertTrue(any("ray_fanout_compact_flush_complete" in message for message in messages))

    def test_fanout_jsonl_compact_merges_blocks(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = os.path.join(tmp_dir, "compact_jsonl")
            datasink = RayHdfsFanoutDatasink(
                targets=[
                    self._target(
                        output_dir,
                        export_type="jsonl",
                        compact=self._compact_config(rows_per_file=10),
                    )
                ],
                columns=["id", "text"],
            )
            blocks = [
                pa.table({"id": [1], "text": ["one"]}),
                pa.table({"id": [2], "text": ["two"]}),
                pa.table({"id": [3], "text": ["three"]}),
            ]

            datasink.on_write_start(blocks[0].schema)
            write_return = datasink.write(blocks, self._ctx(task_idx=2))
            datasink.on_write_complete(type("WriteResult", (), {"write_returns": [write_return]})())

            files = self._data_files(output_dir, ".jsonl")
            self.assertEqual(len(files), 1)
            self.assertIn("-2-compact-0.jsonl", os.path.basename(files[0]))
            with open(files[0], encoding="utf-8") as reader:
                rows = [json.loads(line) for line in reader]
            self.assertEqual([row["id"] for row in rows], [1, 2, 3])

    def test_fanout_compact_target_can_mix_with_direct_target(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            compact_dir = os.path.join(tmp_dir, "compact")
            direct_dir = os.path.join(tmp_dir, "direct")
            datasink = RayHdfsFanoutDatasink(
                targets=[
                    self._target(
                        compact_dir,
                        export_type="jsonl",
                        compact=self._compact_config(rows_per_file=10),
                    ),
                    self._target(direct_dir, export_type="jsonl"),
                ],
                columns=["id"],
            )
            blocks = [
                pa.table({"id": [1]}),
                pa.table({"id": [2]}),
            ]

            datasink.on_write_start(blocks[0].schema)
            datasink.write(blocks, self._ctx())

            self.assertEqual(len(self._data_files(compact_dir, ".jsonl")), 1)
            self.assertEqual(len(self._data_files(direct_dir, ".jsonl")), 2)

    def test_fanout_compact_flushes_when_rows_or_bytes_threshold_reached(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            rows_dir = os.path.join(tmp_dir, "rows")
            rows_datasink = RayHdfsFanoutDatasink(
                targets=[
                    self._target(
                        rows_dir,
                        export_type="jsonl",
                        compact=self._compact_config(bytes_per_file=1024 * 1024, rows_per_file=2),
                    )
                ],
                columns=["id"],
            )
            row_blocks = [
                pa.table({"id": [1]}),
                pa.table({"id": [2]}),
                pa.table({"id": [3]}),
            ]
            rows_datasink.on_write_start(row_blocks[0].schema)
            rows_datasink.write(row_blocks, self._ctx())

            bytes_dir = os.path.join(tmp_dir, "bytes")
            bytes_datasink = RayHdfsFanoutDatasink(
                targets=[
                    self._target(
                        bytes_dir,
                        export_type="jsonl",
                        compact=self._compact_config(bytes_per_file=20, rows_per_file=100, max_buffer_bytes=40),
                    )
                ],
                columns=["id", "text"],
            )
            byte_blocks = [
                pa.table({"id": [1], "text": ["x" * 32]}),
                pa.table({"id": [2], "text": ["y" * 32]}),
            ]
            bytes_datasink.on_write_start(byte_blocks[0].schema)
            bytes_datasink.write(byte_blocks, self._ctx())

            self.assertEqual(len(self._data_files(rows_dir, ".jsonl")), 2)
            self.assertEqual(len(self._data_files(bytes_dir, ".jsonl")), 2)

    def test_fanout_parquet_compact_schema_mismatch_flushes_and_counts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = os.path.join(tmp_dir, "schema_mismatch")
            datasink = RayHdfsFanoutDatasink(
                targets=[
                    self._target(
                        output_dir,
                        export_type="parquet",
                        compact=self._compact_config(rows_per_file=100),
                    )
                ],
                columns=["id"],
            )
            blocks = [
                pa.table({"id": pa.array([1], type=pa.int64())}),
                pa.table({"id": pa.array(["2"], type=pa.string())}),
            ]

            datasink.on_write_start(blocks[0].schema)
            write_return = datasink.write(blocks, self._ctx())
            summary = datasink.on_write_complete(type("WriteResult", (), {"write_returns": [write_return]})())

            self.assertEqual(len(self._data_files(output_dir, ".parquet")), 2)
            self.assertEqual(summary["targets"][0]["compact"]["files"], 2)
            self.assertEqual(summary["targets"][0]["compact"]["schema_mismatch_flushes"], 1)

    def test_fanout_unknown_export_type_fails_fast(self):
        datasink = RayHdfsFanoutDatasink(
            targets=[self._target("/unused", export_type="csv")],
            columns=["id"],
        )

        with self.assertRaisesRegex(NotImplementedError, "does not support"):
            datasink._write_table(datasink.targets[0], pa.table({"id": [1]}), "/unused/file.csv")


class TestRayExporterHDFSRoundTrip(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import ray

        cls._av_stub_dir = tempfile.TemporaryDirectory()
        with open(osp.join(cls._av_stub_dir.name, "av.py"), "w", encoding="utf-8") as file:
            file.write(
                "class _Logging:\n"
                "    PANIC = 0\n"
                "    def set_level(self, *args, **kwargs):\n"
                "        pass\n"
                "logging = _Logging()\n"
                "class _Container:\n"
                "    class InputContainer:\n"
                "        pass\n"
                "container = _Container()\n"
            )
        cls._ray_started_by_test = not ray.is_initialized()
        if cls._ray_started_by_test:
            pythonpath = cls._av_stub_dir.name
            if os.environ.get("PYTHONPATH"):
                pythonpath = pythonpath + os.pathsep + os.environ["PYTHONPATH"]
            ray.init(
                address="local",
                num_cpus=2,
                include_dashboard=False,
                log_to_driver=False,
                runtime_env={"env_vars": {"PYTHONPATH": pythonpath}},
            )

    @classmethod
    def tearDownClass(cls):
        if cls._ray_started_by_test:
            import ray

            ray.shutdown()
        cls._av_stub_dir.cleanup()

    def setUp(self):
        self.tmp_dir = osp.join(
            osp.dirname(osp.abspath(__file__)),
            "tmp",
            self.__class__.__name__,
            self._testMethodName,
        )
        shutil.rmtree(self.tmp_dir, ignore_errors=True)
        os.makedirs(self.tmp_dir, exist_ok=True)
        self.filesystem = LocalFileSystem()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _dataset(self, start=0, count=5):
        import ray

        ids = list(range(start, start + count))
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "text": pa.array(
                    [
                        f"row-{row_id} line\nquote \" slash \\"
                        for row_id in ids
                    ],
                    type=pa.string(),
                ),
                "optional_text": pa.array(
                    [None if index % 2 == 0 else f"value-{row_id}" for index, row_id in enumerate(ids)],
                    type=pa.string(),
                ),
                "all_null_text": pa.array([None] * count, type=pa.string()),
                "payload": pa.array([bytes([row_id % 256, (row_id + 1) % 256]) for row_id in ids], type=pa.binary()),
                "tags": pa.array(
                    [
                        [f"tag-{row_id}", "common"] if row_id % 3 == 0 else ([] if row_id % 3 == 1 else None)
                        for row_id in ids
                    ],
                    type=pa.list_(pa.string()),
                ),
            }
        )
        return ray.data.from_arrow(table).repartition(2)

    def _writer_args(self, export_type):
        if export_type == "parquet":
            return {"min_rows_per_file": 2, "concurrency": 2}
        return {"num_rows_per_file": 2, "concurrency": 2}

    def _export(self, export_type, output_dir, dataset, mode="error_if_exists", **kwargs):
        export_args = self._writer_args(export_type)
        export_args.update(kwargs)
        with patch(
            "data_juicer.core.ray_exporter.get_pyarrow_filesystem",
            return_value=(self.filesystem, output_dir),
        ):
            RayExporter(
                f"hdfs://cluster/test/{osp.basename(output_dir)}",
                export_type=export_type,
                filesystem="pyarrow",
                mode=mode,
                **export_args,
            ).export(dataset)

    def test_fanout_datasink_ray_write_datasink_jsonl_roundtrip(self):
        import ray

        output_a = osp.join(self.tmp_dir, "fanout_a")
        output_b = osp.join(self.tmp_dir, "fanout_b")
        dataset = ray.data.from_items(
            [
                {"id": 1, "score": 0.9, "lang": "en"},
                {"id": 2, "score": 0.1, "lang": "zh"},
                {"id": 3, "score": 0.95, "lang": "zh"},
            ]
        ).repartition(2)
        datasink = RayHdfsFanoutDatasink(
            targets=[
                {
                    "path": output_a,
                    "original_uri": output_a,
                    "filesystem": self.filesystem,
                    "type": "jsonl",
                    "mode": "error_if_exists",
                    "condition": "score >= 0.8",
                    "extra_args": {},
                },
                {
                    "path": output_b,
                    "original_uri": output_b,
                    "filesystem": self.filesystem,
                    "type": "jsonl",
                    "mode": "error_if_exists",
                    "condition": "lang == 'zh'",
                    "extra_args": {},
                },
            ],
            columns=["id", "score", "lang"],
        )

        dataset.write_datasink(datasink)

        def read_jsonl_dir(path):
            rows = []
            for filename in os.listdir(path):
                if not filename.endswith(".jsonl"):
                    continue
                with open(osp.join(path, filename), "r", encoding="utf-8") as file:
                    rows.extend(json.loads(line) for line in file)
            return sorted(rows, key=lambda row: row["id"])

        self.assertEqual([row["id"] for row in read_jsonl_dir(output_a)], [1, 3])
        self.assertEqual([row["id"] for row in read_jsonl_dir(output_b)], [2, 3])

    def _data_files(self, output_dir, export_type):
        suffix = ".parquet" if export_type == "parquet" else ".json"
        return sorted(
            osp.join(output_dir, filename)
            for filename in os.listdir(output_dir)
            if filename.endswith(suffix)
        )

    def _read_rows(self, output_dir, export_type):
        files = self._data_files(output_dir, export_type)
        if export_type == "parquet":
            table = pa.concat_tables([pq.read_table(path) for path in files])
            rows = table.to_pylist()
        else:
            rows = []
            for path in files:
                with open(path, encoding="utf-8") as reader:
                    rows.extend(json.loads(line) for line in reader if line.strip())
        return sorted(rows, key=lambda row: row["id"])

    def _assert_data_shape_round_trip(self, rows, expected_ids, export_type):
        self.assertEqual([row["id"] for row in rows], expected_ids)
        for row in rows:
            row_id = row["id"]
            self.assertEqual(row["text"], f"row-{row_id} line\nquote \" slash \\")
            self.assertIsNone(row["all_null_text"])
            if row_id % 2 == 0:
                self.assertIsNone(row["optional_text"])
            else:
                self.assertEqual(row["optional_text"], f"value-{row_id}")
            if export_type == "parquet":
                self.assertEqual(row["payload"], bytes([row_id % 256, (row_id + 1) % 256]))
            else:
                self.assertEqual(
                    row["payload"],
                    base64.b64encode(bytes([row_id % 256, (row_id + 1) % 256])).decode("ascii"),
                )
            if row_id % 3 == 0:
                self.assertEqual(row["tags"], [f"tag-{row_id}", "common"])
            elif row_id % 3 == 1:
                self.assertEqual(row["tags"], [])
            else:
                self.assertIsNone(row["tags"])

    @TEST_TAG("ray")
    def test_hdfs_parquet_and_jsonl_write_multiple_parts_and_round_trip_data_shapes(self):
        for export_type in ("parquet", "jsonl"):
            with self.subTest(export_type=export_type):
                output_dir = osp.join(self.tmp_dir, export_type)

                self._export(export_type, output_dir, self._dataset())

                files = self._data_files(output_dir, export_type)
                self.assertGreaterEqual(len(files), 2)
                rows = self._read_rows(output_dir, export_type)
                self._assert_data_shape_round_trip(rows, [0, 1, 2, 3, 4], export_type)

    @TEST_TAG("ray")
    def test_hdfs_append_second_submission_adds_parts_and_preserves_rows(self):
        for export_type in ("parquet", "jsonl"):
            with self.subTest(export_type=export_type):
                output_dir = osp.join(self.tmp_dir, export_type)
                self._export(export_type, output_dir, self._dataset(start=0, count=2))
                first_files = self._data_files(output_dir, export_type)

                self._export(export_type, output_dir, self._dataset(start=100, count=3), mode="append")

                files_after_append = self._data_files(output_dir, export_type)
                self.assertGreater(len(files_after_append), len(first_files))
                rows = self._read_rows(output_dir, export_type)
                self._assert_data_shape_round_trip(rows, [0, 1, 100, 101, 102], export_type)

    @TEST_TAG("ray")
    def test_hdfs_error_if_exists_rejects_existing_output_directory(self):
        for export_type in ("parquet", "jsonl"):
            with self.subTest(export_type=export_type):
                output_dir = osp.join(self.tmp_dir, export_type)
                self._export(export_type, output_dir, self._dataset(start=0, count=2))

                with self.assertRaisesRegex(FileExistsError, "already exists"):
                    self._export(export_type, output_dir, self._dataset(start=100, count=1))

    @TEST_TAG("ray")
    def test_hdfs_overwrite_existing_directory_removes_stale_parts(self):
        for export_type in ("parquet", "jsonl"):
            with self.subTest(export_type=export_type):
                output_dir = osp.join(self.tmp_dir, export_type)
                self._export(export_type, output_dir, self._dataset(start=0, count=4))
                stale_file = osp.join(output_dir, "stale-part")
                with open(stale_file, "w", encoding="utf-8") as writer:
                    writer.write("stale")

                self._export(export_type, output_dir, self._dataset(start=100, count=2), mode="overwrite")

                self.assertFalse(osp.exists(stale_file))
                rows = self._read_rows(output_dir, export_type)
                self._assert_data_shape_round_trip(rows, [100, 101], export_type)


class TestRayExporter(DataJuicerTestCaseBase):

    def setUp(self):
        """Set up test data"""
        super().setUp()

        import ray
        from data_juicer.core.data.ray_dataset import RayDataset

        cur_dir = osp.dirname(osp.abspath(__file__))
        self.tmp_dir = f'{cur_dir}/tmp/{self.__class__.__name__}/{self._testMethodName}'
        os.makedirs(self.tmp_dir, exist_ok=True)

        self.data = [
            {'text': 'hello', Fields.stats: {'score': 1}, HashKeys.hash: 'a1'},
            {'text': 'world', Fields.stats: {'score': 2}, HashKeys.hash: 'b2'},
            {'text': 'test', Fields.stats: {'score': 3}, HashKeys.hash: 'c3'}
        ]
        self.dataset = RayDataset(ray.data.from_items(self.data))

    def tearDown(self):
        """Clean up temporary outputs"""

        self.dataset = None
        if osp.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

        super().tearDown()

    def _pop_raw_data_keys(self, keys):
        res = copy.deepcopy(self.data)
        for d_i in res:
            for k in keys:
                d_i.pop(k, None)

        return res

    @TEST_TAG('ray')
    def test_json_not_keep_stats_and_hashes(self):
        import ray

        out_path = osp.join(self.tmp_dir, 'outdata.json')
        ray_exporter = RayExporter(
            out_path,
            keep_stats_in_res_ds=False,
            keep_hashes_in_res_ds=False)
        ray_exporter.export(self.dataset.data)

        ds = ray.data.read_json(out_path)
        data_list = ds.take_all()

        self.assertListOfDictEqual(data_list, self._pop_raw_data_keys([Fields.stats, HashKeys.hash]))

    @TEST_TAG('ray')
    def test_jsonl_keep_stats_and_hashes(self):
        import ray

        out_path = osp.join(self.tmp_dir, 'outdata.jsonl')
        ray_exporter = RayExporter(
            out_path,
            keep_stats_in_res_ds=True,
            keep_hashes_in_res_ds=True)
        ray_exporter.export(self.dataset.data)

        ds = ray.data.read_json(out_path)
        data_list = ds.take_all()

        self.assertListOfDictEqual(data_list, self.data)

    @TEST_TAG('ray')
    def test_parquet_keep_stats(self):
        import ray

        out_path = osp.join(self.tmp_dir, 'outdata.parquet')
        ray_exporter = RayExporter(
            out_path,
            keep_stats_in_res_ds=True,
            keep_hashes_in_res_ds=False)
        ray_exporter.export(self.dataset.data)

        ds = ray.data.read_parquet(out_path)
        data_list = ds.take_all()

        self.assertListEqual(data_list, self._pop_raw_data_keys([HashKeys.hash]))

    @TEST_TAG('ray')
    def test_lance_keep_hashes(self):
        import ray

        out_path = osp.join(self.tmp_dir, 'outdata.lance')
        ray_exporter = RayExporter(
            out_path,
            keep_stats_in_res_ds=False,
            keep_hashes_in_res_ds=True)
        ray_exporter.export(self.dataset.data)

        ds = ray.data.read_lance(out_path)
        data_list = ds.take_all()

        self.assertListOfDictEqual(data_list, self._pop_raw_data_keys([Fields.stats]))

    @TEST_TAG('ray')
    def test_webdataset_multi_images(self):
        import io
        from PIL import Image
        import ray
        from data_juicer.core.data.ray_dataset import RayDataset

        data_dir = osp.abspath(osp.join(osp.dirname(osp.realpath(__file__)), '..', 'ops', 'data'))
        img1_path = osp.join(data_dir, 'img1.png')
        img2_path = osp.join(data_dir, 'img2.jpg')
        img3_path = osp.join(data_dir, 'img3.jpg')

        data = [
            {
                'json': {
                    'text': 'hello',
                    'images': [img1_path, img2_path]
                    },
                'jpgs': load_images_byte([img1_path, img2_path])},
            {
                'json': {
                    'text': 'world',
                    'images': [img2_path, img3_path]
                    },
                'jpgs': load_images_byte([img2_path, img3_path])},
            {
                'json': {
                    'text': 'test',
                    'images': [img1_path, img2_path, img3_path]
                    },
                'jpgs': load_images_byte([img1_path, img2_path, img3_path])}
        ]
        dataset = RayDataset(ray.data.from_items(data))
        out_path = osp.join(self.tmp_dir, 'outdata.webdataset')
        ray_exporter = RayExporter(out_path)
        ray_exporter.export(dataset.data)

        ds = RayDataset.read_webdataset(out_path)
        res_list = ds.take_all()
        
        self.assertEqual(len(res_list), len(data))
        res_list.sort(key=lambda x: x['json']['text'])
        data.sort(key=lambda x: x['json']['text'])

        for i in range(len(data)):
            self.assertDictEqual(res_list[i]['json'], data[i]['json'])
            self.assertEqual(
                res_list[i]['jpgs'],
                [Image.open(io.BytesIO(v)) for v in data[i]['jpgs']]
            )

    @TEST_TAG('ray')
    def test_webdataset_multi_videos_frames_bytes(self):
        import io
        from PIL import Image
        import ray
        from data_juicer.core.data.ray_dataset import RayDataset

        data_dir = osp.abspath(osp.join(osp.dirname(osp.realpath(__file__)), '..', 'ops', 'data'))
        img1_path = osp.join(data_dir, 'img1.png')
        img2_path = osp.join(data_dir, 'img2.jpg')
        img3_path = osp.join(data_dir, 'img3.jpg')

        data = [
            {
                'json': {
                    'text': 'hello',
                    'videos': ['video1.mp4', 'video2.mp4']
                    },
                'mp4s': [
                    load_images_byte([img1_path]),  # as video1 frames bytes
                    load_images_byte([img1_path, img2_path])   # as video2 frames path
                    ]
            },
            {
                'json': {
                    'text': 'world',
                    'videos': ['video1.mp4']
                    },
                'mp4s': [
                    load_images_byte([img2_path, img3_path])  # as video1 frames
                    ]
            }
        ]
        dataset = RayDataset(ray.data.from_items(data))
        out_path = osp.join(self.tmp_dir, 'outdata.webdataset')
        ray_exporter = RayExporter(out_path, export_type='webdataset')
        ray_exporter.export(dataset.data)

        ds = RayDataset.read_webdataset(out_path)
        res_list = ds.take_all()
        
        self.assertEqual(len(res_list), len(data))
        res_list.sort(key=lambda x: x['json']['text'])
        data.sort(key=lambda x: x['json']['text'])
        
        for i in range(len(data)):
            if len(data[i]['mp4s']) > 1:
                tgt_mp4s = [[Image.open(io.BytesIO(f_i)) for f_i in v_i] for v_i in data[i]['mp4s']]
            else:
                tgt_mp4s = [Image.open(io.BytesIO(f_i)) for f_i in data[i]['mp4s'][0]]
            self.assertDictEqual(res_list[i]['json'], data[i]['json'])
            self.assertEqual(res_list[i]['mp4s'], tgt_mp4s)

    @TEST_TAG('ray')
    def test_webdataset_multi_videos_frames_path(self):
        import io
        from PIL import Image
        import ray
        from data_juicer.core.data.ray_dataset import RayDataset

        data_dir = osp.abspath(osp.join(osp.dirname(osp.realpath(__file__)), '..', 'ops', 'data'))
        img1_path = osp.join(data_dir, 'img8.jpg')
        img2_path = osp.join(data_dir, 'img9.jpg')
        img3_path = osp.join(data_dir, 'img10.jpg')

        data = [
            {
                'json': {
                    'text': 'hello',
                    'videos': ['video1.mp4', 'video2.mp4']
                    },
                'mp4s': [
                    [img1_path],  # as video1 frames path
                    [img1_path, img2_path]   # as video2 frames path
                    ]
            },
            {
                'json': {
                    'text': 'world',
                    'videos': ['video1.mp4']
                    },
                'mp4s': [
                    [img2_path, img3_path]  # as video1 frames path
                    ]
            }
        ]
        dataset = RayDataset(ray.data.from_items(data))
        out_path = osp.join(self.tmp_dir, 'outdata.webdataset')
        ray_exporter = RayExporter(out_path, export_type='webdataset')
        ray_exporter.export(dataset.data)

        ds = RayDataset.read_webdataset(out_path)
        res_list = ds.take_all()
        
        self.assertEqual(len(res_list), len(data))
        res_list.sort(key=lambda x: x['json']['text'])
        data.sort(key=lambda x: x['json']['text'])
        
        for i in range(len(data)):
            if len(data[i]['mp4s']) > 1:
                tgt_mp4s = [[Image.open(f_i, formats=['jpeg']) for f_i in v_i] for v_i in data[i]['mp4s']]
            else:
                tgt_mp4s = [Image.open(f_i, formats=['jpeg']) for f_i in data[i]['mp4s'][0]]
            self.assertDictEqual(res_list[i]['json'], data[i]['json'])
            self.assertEqual(res_list[i]['mp4s'], tgt_mp4s)

    @TEST_TAG('ray')
    def test_webdataset_multi_audios_path(self):
        import ray
        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.utils.mm_utils import load_audio

        data_dir = osp.abspath(osp.join(osp.dirname(osp.realpath(__file__)), '..', 'ops', 'data'))
        audio1_path = osp.join(data_dir, 'audio1.wav')
        audio2_path = osp.join(data_dir, 'audio2.wav')
        audio3_path = osp.join(data_dir, 'audio3.ogg')

        data = [
            {
                'json': {
                    'text': 'hello',
                    },
                'mp3s': [audio1_path]
            },
            {
                'json': {
                    'text': 'world',
                    },
                'mp3s': [audio2_path, audio3_path]
            }
        ]
        dataset = RayDataset(ray.data.from_items(data))
        out_path = osp.join(self.tmp_dir, 'outdata.webdataset')
        ray_exporter = RayExporter(out_path, export_type='webdataset')
        ray_exporter.export(dataset.data)

        ds = RayDataset.read_webdataset(out_path)
        res_list = ds.take_all()
        
        self.assertEqual(len(res_list), len(data))

        res_list.sort(key=lambda x: x['json']['text'])
        data.sort(key=lambda x: x['json']['text'])
        
        for i in range(len(data)):
            if len(data[i]['mp3s']) <= 1:
                mp3s_list = [res_list[i]['mp3s']]
            else:
                mp3s_list = res_list[i]['mp3s']

            tgt_mp3s = [load_audio(f_i) for f_i in data[i]['mp3s']]
            
            self.assertDictEqual(res_list[i]['json'], data[i]['json'])

            for j in range(len(mp3s_list)):
                arr, sampling_rate = mp3s_list[j]
                tgt_arr, tgt_sampling_rate = tgt_mp3s[j]
                import numpy as np
                np.testing.assert_array_equal(arr, tgt_arr)
                self.assertEqual(sampling_rate, tgt_sampling_rate)


if __name__ == '__main__':
    unittest.main()
