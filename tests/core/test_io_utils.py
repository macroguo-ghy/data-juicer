import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from data_juicer.core.io_utils import _patch_magnus_datasink_write_result_compat, write_ray_dataset_to_magnus


class WriteRayDatasetToMagnusTest(unittest.TestCase):
    def test_patch_magnus_datasink_unwraps_ray_write_result(self):
        calls = []

        class MagnusDataSink:
            def commit(self, write_results):
                calls.append(("commit", write_results))
                return "committed"

        class MagnusCommitDataSink:
            def on_write_complete(self, write_results):
                calls.append(("on_write_complete", write_results))
                return "completed"

        module = SimpleNamespace(
            MagnusDataSink=MagnusDataSink,
            MagnusCommitDataSink=MagnusCommitDataSink,
        )
        write_result = SimpleNamespace(write_returns=[["file1"], ["file2"]])

        _patch_magnus_datasink_write_result_compat(module)

        self.assertEqual(MagnusDataSink().commit(write_result), "committed")
        self.assertEqual(MagnusCommitDataSink().on_write_complete(write_result), "completed")
        self.assertEqual(
            calls,
            [
                ("commit", [["file1"], ["file2"]]),
                ("on_write_complete", [["file1"], ["file2"]]),
            ],
        )

    def test_write_ray_dataset_to_magnus_passes_default_operation(self):
        dataset = MagicMock()
        dataset.schema.return_value = SimpleNamespace(base_schema="schema")
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists") as mock_create_table,
        ):
            write_ray_dataset_to_magnus(dataset, "catalog.db.table")

        mock_create_table.assert_called_once_with("catalog.db.table", "schema", partition_columns=None)
        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset,
            identifier="catalog.db.table",
            operation="APPEND",
        )

    def test_write_ray_dataset_to_magnus_passes_configured_operation(self):
        dataset = MagicMock()
        dataset.schema.return_value = SimpleNamespace(base_schema="schema")
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists"),
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="overwrite",
                magnus_conf={"write_options": {"write.format.default": "lance"}},
            )

        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset,
            identifier="catalog.db.table",
            operation="OVERWRITE",
            write_options={"write.format.default": "lance"},
        )

    def test_write_ray_dataset_to_magnus_accepts_operation_from_magnus_conf(self):
        dataset = MagicMock()
        dataset.schema.return_value = SimpleNamespace(base_schema="schema")
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists"),
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                magnus_conf={"operation": "overwrite", "write_options": {"write.format.default": "lance"}},
            )

        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset,
            identifier="catalog.db.table",
            operation="OVERWRITE",
            write_options={"write.format.default": "lance"},
        )

    def test_write_ray_dataset_to_magnus_rejects_unknown_operation(self):
        dataset = MagicMock()
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray):
            with self.assertRaisesRegex(ValueError, "Unsupported Magnus write operation"):
                write_ray_dataset_to_magnus(dataset, "catalog.db.table", operation="DELETE")
