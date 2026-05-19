import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pyarrow as pa

_register_extension_type = pa.register_extension_type


def _register_extension_type_once(extension_type):
    try:
        _register_extension_type(extension_type)
    except pa.ArrowKeyError:
        if not extension_type.extension_name.startswith("datasets.features.features."):
            raise


pa.register_extension_type = _register_extension_type_once

from data_juicer.core.io_utils import (
    MAGNUS_FAILURE_POLICY_COMMIT_COMPLETED_UNSAFE,
    _MAGNUS_FAILURE_POLICY_SNAPSHOT_SUMMARY_KEY,
    _infer_magnus_schema_from_ray_dataset,
    _magnus_table_properties_from_write_options,
    _patch_magnus_datasink_failure_policy,
    _patch_magnus_datasink_worker_file_appender_compat,
    _patch_magnus_parquet_appender_hdfs_uri_compat,
    _patch_magnus_transformer_localsort_compat,
    _patch_magnus_datasink_write_result_compat,
    build_arrow_schema_from_config,
    copy_local_to_uri,
    create_magnus_table_if_not_exists,
    write_hf_dataset_to_magnus,
    write_ray_dataset_to_magnus,
)

pa.register_extension_type = _register_extension_type


class FakeRayDataset:
    def __init__(self, rows, schema=None):
        self.rows = rows
        self._schema = schema or pa.schema(
            [
                pa.field("id", pa.string()),
                pa.field("p_date", pa.string()),
            ]
        )

    def schema(self):
        return SimpleNamespace(base_schema=self._schema)

    def columns(self):
        return self._schema.names

    def count(self):
        return len(self.rows)

    def materialize(self):
        return self

    def map_batches(self, fn, *, batch_format="pyarrow", **kwargs):
        if batch_format != "pyarrow":
            raise ValueError(batch_format)
        table = pa.Table.from_pylist(self.rows, schema=self._schema)
        output = fn(table)
        return FakeRayDataset(output.to_pylist(), output.schema)

    def select_columns(self, columns):
        fields = [self._schema.field(column) for column in columns]
        return FakeRayDataset([{column: row.get(column) for column in columns} for row in self.rows], pa.schema(fields))

    def iter_batches(self, batch_format="pyarrow", batch_size=8192):
        if batch_format != "pyarrow":
            raise ValueError(batch_format)
        for idx in range(0, len(self.rows), batch_size):
            yield pa.Table.from_pylist(self.rows[idx : idx + batch_size], schema=self._schema)


class EmptyUnknownSchemaRayDataset:
    def schema(self):
        return None

    def materialize(self):
        return self

    def columns(self):
        return None

    def count(self):
        return 0

    def map_batches(self, fn, *, batch_format="pyarrow", **kwargs):
        return self


class LazyOnlyRayDataset(FakeRayDataset):
    def materialize(self):
        raise AssertionError("checkpoint mode must not eagerly materialize the dataset")

    def count(self):
        raise AssertionError("checkpoint mode must not eagerly count the dataset")


class StrictCheckpointRayDataset(LazyOnlyRayDataset):
    def schema(self, *args, **kwargs):
        raise AssertionError("checkpoint mode must not eagerly fetch schema")

    def columns(self, *args, **kwargs):
        raise AssertionError("checkpoint mode must not eagerly fetch columns")


class UnknownSchemaRayDatasetWithArrowBatches:
    def schema(self, *args, **kwargs):
        return SimpleNamespace(base_schema=None)

    def iter_batches(self, batch_format="pyarrow", batch_size=8192):
        if batch_format != "pyarrow":
            raise ValueError(batch_format)
        yield pa.Table.from_pylist(
            [
                {
                    "id": "1",
                    "state": {
                        "world_state": {"bench_material_ctr": "2.9%"},
                        "adv_state": [{"adv_id": "9283746510928374", "adv_roi": [0.28, 0.33]}],
                    },
                }
            ]
        )


class UnknownSchemaRayDatasetWithMultipleArrowBatches:
    def schema(self, *args, **kwargs):
        return SimpleNamespace(base_schema=None)

    def iter_batches(self, batch_format="pyarrow", batch_size=8192):
        if batch_format != "pyarrow":
            raise ValueError(batch_format)
        yield pa.Table.from_pylist([{"id": "1", "state": {"world_state": {"bench_material_ctr": "2.9%"}}}])
        yield pa.Table.from_pylist(
            [
                {
                    "id": "2",
                    "state": {
                        "world_state": {
                            "bench_material_ctr": "3.1%",
                            "extra_metric": "ok",
                        },
                        "adv_state": [{"adv_id": "9283746510928374", "adv_roi": [0.28, 0.33]}],
                    },
                }
            ]
        )


class WriteRayDatasetToMagnusTest(unittest.TestCase):
    def test_copy_local_to_remote_directory_removes_stale_files(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            src_dir = os.path.join(tmp_dir, "src")
            dst_dir = os.path.join(tmp_dir, "dst")
            os.makedirs(src_dir)
            os.makedirs(dst_dir)
            with open(os.path.join(src_dir, "part-000.jsonl"), "w") as fout:
                fout.write('{"id": 1}\n')
            with open(os.path.join(dst_dir, "stale.jsonl"), "w") as fout:
                fout.write('{"id": "old"}\n')

            copy_local_to_uri(src_dir, f"file://{dst_dir}")

            self.assertTrue(os.path.exists(os.path.join(dst_dir, "part-000.jsonl")))
            self.assertFalse(os.path.exists(os.path.join(dst_dir, "stale.jsonl")))

    def test_build_arrow_schema_from_config_supports_nested_list_types(self):
        schema = build_arrow_schema_from_config(
            {
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "images", "type": "list<binary>"},
                    {"name": "texts", "type": "list<string>"},
                    {"name": "messages", "type": "list<struct<role:string,content:string>>"},
                    {"name": "site_id", "type": "int64"},
                    {"name": "has_audio_in_video", "type": "bool"},
                ]
            }
        )

        self.assertEqual(schema.field("id").type, pa.string())
        self.assertEqual(schema.field("images").type, pa.list_(pa.binary()))
        self.assertEqual(schema.field("texts").type, pa.list_(pa.string()))
        self.assertEqual(
            schema.field("messages").type,
            pa.list_(pa.struct([pa.field("role", pa.string()), pa.field("content", pa.string())])),
        )
        self.assertEqual(schema.field("site_id").type, pa.int64())
        self.assertEqual(schema.field("has_audio_in_video").type, pa.bool_())

    def test_infer_magnus_schema_from_ray_dataset_falls_back_to_arrow_batches(self):
        schema = _infer_magnus_schema_from_ray_dataset(UnknownSchemaRayDatasetWithArrowBatches())

        self.assertEqual(schema.field("id").type, pa.string())
        self.assertEqual(
            schema.field("state").type.field("world_state").type.field("bench_material_ctr").type,
            pa.string(),
        )
        self.assertEqual(
            schema.field("state").type.field("adv_state").type.value_type.field("adv_roi").type,
            pa.list_(pa.float64()),
        )

    def test_infer_magnus_schema_from_ray_dataset_merges_multiple_arrow_batches(self):
        schema = _infer_magnus_schema_from_ray_dataset(UnknownSchemaRayDatasetWithMultipleArrowBatches())

        world_state_type = schema.field("state").type.field("world_state").type
        self.assertEqual(world_state_type.field("bench_material_ctr").type, pa.string())
        self.assertEqual(world_state_type.field("extra_metric").type, pa.string())
        self.assertEqual(
            schema.field("state").type.field("adv_state").type.value_type.field("adv_roi").type,
            pa.list_(pa.float64()),
        )

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

    def test_patch_magnus_transformer_falls_back_to_pyarrow_group_sort(self):
        class FakeArrowGroup:
            def __init__(self, rows):
                self.rows = rows

            def sort_by(self, sort_keys):
                ordered = sorted(self.rows, key=lambda row: tuple(row[column] for column, _ in sort_keys))
                return {"rows": ordered, "sort_keys": sort_keys}

        class FakeGroupedData:
            def __init__(self):
                self.calls = []

            def map_groups(self, fn, *, batch_format="default", **kwargs):
                self.calls.append({"batch_format": batch_format, "kwargs": kwargs})
                return fn(
                    FakeArrowGroup(
                        [
                            {"p_date": "20260421", "id": "2"},
                            {"p_date": "20260421", "id": "1"},
                        ]
                    )
                )

        class FakeDataset:
            pass

        class SortBySortOrderTransformer:
            def __init__(self, table, config, operation):
                self.table = table
                self.config = config
                self.operation = operation

            def transform(self, ray_ds):
                raise AssertionError("compat patch should replace this implementation")

        module = SimpleNamespace(
            SortBySortOrderTransformer=SortBySortOrderTransformer,
            GroupedData=FakeGroupedData,
            Dataset=FakeDataset,
            get_bool_config=lambda config, key, default: config.get(key, default),
            get_sort_asc_columns=lambda table: ["p_date", "id"],
            RayWriteOptions=SimpleNamespace(DISABLE_SORT="disable_sort"),
        )

        _patch_magnus_transformer_localsort_compat(module)

        grouped = FakeGroupedData()
        result = module.SortBySortOrderTransformer(table=object(), config={}, operation="APPEND").transform(grouped)

        self.assertEqual(grouped.calls, [{"batch_format": "pyarrow", "kwargs": {}}])
        self.assertEqual(
            result,
            {
                "rows": [
                    {"p_date": "20260421", "id": "1"},
                    {"p_date": "20260421", "id": "2"},
                ],
                "sort_keys": [("p_date", "ascending"), ("id", "ascending")],
            },
        )

    def test_patch_magnus_parquet_appender_uses_hdfs_internal_path_for_writer(self):
        writer_calls = []

        class FakeParquetWriter:
            def __init__(self, path, schema, **kwargs):
                writer_calls.append((path, schema, kwargs))
                self.writes = []

            def write_table(self, table, *, row_group_size):
                self.writes.append((table, row_group_size))

        class ParquetAppender:
            def __init__(self):
                self._file_path = "hdfs://haruna/tmp/table/data/p_date=20260423/file.parquet"
                self._arrow_schema = pa.schema([pa.field("id", pa.string())])
                self._writer = None
                self._writer_config = {"coerce_timestamps": "us"}
                self._use_dictionary = False
                self._compression = "NONE"
                self._write_batch_size = 1
                self._row_group_size_bytes = 1024
                self._row_count = 0

            def _get_file_system(self):
                return "hdfs-fs"

            def _prune_and_arrange_cols(self, table):
                return table

            def append(self, data_batch):
                raise AssertionError("hdfs path should use compat implementation")

        module = SimpleNamespace(
            ParquetAppender=ParquetAppender,
            pq=SimpleNamespace(ParquetWriter=FakeParquetWriter),
        )
        table = pa.table({"id": ["1", "2"]})

        _patch_magnus_parquet_appender_hdfs_uri_compat(module)
        appender = ParquetAppender()
        appender.append(table)

        self.assertEqual(writer_calls[0][0], "/tmp/table/data/p_date=20260423/file.parquet")
        self.assertEqual(writer_calls[0][1], appender._arrow_schema)
        self.assertEqual(writer_calls[0][2]["filesystem"], "hdfs-fs")
        self.assertEqual(writer_calls[0][2]["coerce_timestamps"], "us")
        self.assertEqual(appender._row_count, 2)

    def test_patch_magnus_datasink_applies_file_appender_patch_inside_worker_entrypoints(self):
        calls = []

        class MagnusDataSink:
            def _do_write(self, blocks, ctx=None):
                calls.append(("do_write", list(blocks), ctx))
                return "written"

        class MagnusDatasinkWriter:
            def __call__(self, blocks, ctx):
                calls.append(("writer", list(blocks), ctx))
                return "writer-result"

        class MagnusDynamicBucketDatasinkWriter:
            def __call__(self, blocks, ctx):
                calls.append(("dynamic", list(blocks), ctx))
                return "dynamic-result"

        class MagnusBucketedDatasinkWriter:
            def _write(self, blocks):
                calls.append(("bucketed", list(blocks)))
                return "bucketed-result"

        module = SimpleNamespace(
            MagnusDataSink=MagnusDataSink,
            MagnusDatasinkWriter=MagnusDatasinkWriter,
            MagnusDynamicBucketDatasinkWriter=MagnusDynamicBucketDatasinkWriter,
            MagnusBucketedDatasinkWriter=MagnusBucketedDatasinkWriter,
        )
        file_appender_module = SimpleNamespace()

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=file_appender_module),
            patch("data_juicer.core.io_utils._patch_magnus_parquet_appender_hdfs_uri_compat") as mock_patch_appender,
        ):
            _patch_magnus_datasink_worker_file_appender_compat(module)
            self.assertEqual(MagnusDataSink()._do_write(iter(["a"]), ctx="ctx"), "written")
            self.assertEqual(MagnusDatasinkWriter()(iter(["b"]), "ctx"), "writer-result")
            self.assertEqual(MagnusDynamicBucketDatasinkWriter()(iter(["c"]), "ctx"), "dynamic-result")
            self.assertEqual(MagnusBucketedDatasinkWriter()._write(iter(["d"])), "bucketed-result")

        self.assertEqual(
            calls,
            [
                ("do_write", ["a"], "ctx"),
                ("writer", ["b"], "ctx"),
                ("dynamic", ["c"], "ctx"),
                ("bucketed", ["d"]),
            ],
        )
        self.assertEqual(mock_patch_appender.call_count, 4)
        mock_patch_appender.assert_called_with(file_appender_module)

    def test_write_ray_dataset_to_magnus_patches_transformer_module(self):
        dataset = MagicMock()
        dataset.schema.return_value = SimpleNamespace(base_schema="schema")
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())
        magnus_datasink_module = SimpleNamespace()
        magnus_transformers_module = SimpleNamespace()
        magnus_file_appender_module = SimpleNamespace()

        with (
            patch(
                "data_juicer.core.io_utils.import_optional_dependency",
                side_effect=[
                    pyiceberg_ray,
                    magnus_datasink_module,
                    magnus_transformers_module,
                    magnus_file_appender_module,
                ],
            ),
            patch("data_juicer.core.io_utils._patch_magnus_transformer_localsort_compat") as mock_patch_transformers,
            patch("data_juicer.core.io_utils._patch_magnus_parquet_appender_hdfs_uri_compat") as mock_patch_appender,
            patch("data_juicer.core.io_utils._patch_magnus_datasink_worker_file_appender_compat") as mock_patch_worker,
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists"),
        ):
            write_ray_dataset_to_magnus(dataset, "catalog.db.table")

        mock_patch_transformers.assert_called_once_with(magnus_transformers_module)
        mock_patch_appender.assert_called_once_with(magnus_file_appender_module)
        mock_patch_worker.assert_called_once_with(magnus_datasink_module)

    def test_magnus_failure_policy_patch_commits_completed_files_on_failure(self):
        class FakeMagnusDataSink:
            def __init__(self, table, operation, write_options, snapshot_summary, tag_name=None):
                self.write_options = write_options
                self.snapshot_summary = snapshot_summary
                self.commit = MagicMock()

            def on_write_failed(self, error):
                self.original_error = error

        module = SimpleNamespace(MagnusDataSink=FakeMagnusDataSink)

        _patch_magnus_datasink_failure_policy(module)

        sink = module.MagnusDataSink(
            "table",
            "APPEND",
            {},
            {_MAGNUS_FAILURE_POLICY_SNAPSHOT_SUMMARY_KEY: MAGNUS_FAILURE_POLICY_COMMIT_COMPLETED_UNSAFE, "keep": "yes"},
        )
        error = RuntimeError("write failed")
        sink.on_write_failed(error)

        self.assertEqual(sink.write_options, {})
        self.assertEqual(sink.snapshot_summary, {"keep": "yes"})
        sink.commit.assert_called_once_with([[]])
        self.assertIs(sink.original_error, error)

    def test_write_ray_dataset_to_magnus_passes_default_operation(self):
        dataset = MagicMock()
        dataset.schema.return_value = SimpleNamespace(base_schema="schema")
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists") as mock_create_table,
        ):
            write_ray_dataset_to_magnus(dataset, "catalog.db.table")

        mock_create_table.assert_not_called()
        dataset.schema.assert_not_called()
        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset,
            identifier="catalog.db.table",
            operation="APPEND",
        )

    def test_write_ray_dataset_to_magnus_rejects_unknown_failure_policy(self):
        dataset = MagicMock()
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray):
            with self.assertRaisesRegex(ValueError, "Unsupported Magnus failure policy"):
                write_ray_dataset_to_magnus(
                    dataset,
                    "catalog.db.table",
                    magnus_failure_policy="delete_everything",
                )

        pyiceberg_ray.write_magnus.assert_not_called()

    def test_write_ray_dataset_to_magnus_commit_completed_requires_checkpoint(self):
        dataset = MagicMock()
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils._is_ray_data_checkpoint_enabled", return_value=False),
        ):
            with self.assertRaisesRegex(ValueError, "requires Ray Data checkpointing"):
                write_ray_dataset_to_magnus(
                    dataset,
                    "catalog.db.table",
                    magnus_failure_policy=MAGNUS_FAILURE_POLICY_COMMIT_COMPLETED_UNSAFE,
                )

        pyiceberg_ray.write_magnus.assert_not_called()

    def test_write_ray_dataset_to_magnus_commit_completed_sets_private_snapshot_summary(self):
        dataset = MagicMock()
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils._is_ray_data_checkpoint_enabled", return_value=True),
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                magnus_failure_policy=MAGNUS_FAILURE_POLICY_COMMIT_COMPLETED_UNSAFE,
                magnus_conf={
                    "snapshot_summary": {"existing": "summary"},
                    "write_options": {"custom": "yes"},
                },
            )

        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset,
            identifier="catalog.db.table",
            operation="APPEND",
            snapshot_summary={
                "existing": "summary",
                _MAGNUS_FAILURE_POLICY_SNAPSHOT_SUMMARY_KEY: MAGNUS_FAILURE_POLICY_COMMIT_COMPLETED_UNSAFE,
            },
            write_options={
                "custom": "yes",
            },
        )

    def test_write_ray_dataset_to_magnus_passes_configured_operation(self):
        dataset = MagicMock()
        dataset.schema.return_value = SimpleNamespace(base_schema="schema")
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists") as mock_create_table,
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="overwrite",
                magnus_conf={"write_options": {"write.format.default": "lance"}},
            )

        mock_create_table.assert_not_called()
        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset,
            identifier="catalog.db.table",
            operation="OVERWRITE",
            write_options={"write.format.default": "lance"},
        )

    def test_write_ray_dataset_to_magnus_flattens_nested_write_options(self):
        dataset = MagicMock()
        dataset.schema.return_value = SimpleNamespace(base_schema="schema")
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists") as mock_create_table,
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="overwrite",
                magnus_conf={"write_options": {"write": {"format": {"default": "lance"}}}},
            )

        mock_create_table.assert_not_called()
        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset,
            identifier="catalog.db.table",
            operation="OVERWRITE",
            write_options={"write.format.default": "lance"},
        )

    def test_magnus_table_properties_accept_nested_write_options(self):
        self.assertEqual(
            _magnus_table_properties_from_write_options(
                {"write_options": {"write": {"format": {"default": "lance"}}}}
            ),
            {"write.format.default": "lance"},
        )

    def test_write_hf_dataset_to_magnus_passes_format_table_property(self):
        class FakeFrame:
            def __init__(self, rows):
                self.rows = rows
                self.iloc = self

            def __len__(self):
                return len(self.rows)

            def __getitem__(self, item):
                return FakeFrame(self.rows[item])

            def to_dict(self, orient):
                if orient != "records":
                    raise AssertionError(f"Unexpected orient: {orient}")
                return self.rows

        class FakeHFDataset:
            features = SimpleNamespace(arrow_schema=pa.schema([pa.field("id", pa.int64())]))

            def to_pandas(self):
                return FakeFrame([{"id": 1}])

        writer = MagicMock()
        writer_module = SimpleNamespace(MagnusMultiFileWriter=MagicMock(return_value=writer))
        client = MagicMock()
        client.load_table.return_value = "table"

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=writer_module),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists", return_value=client) as mock_create,
        ):
            write_hf_dataset_to_magnus(
                FakeHFDataset(),
                "catalog.db.table",
                partition_columns=["p_date"],
                schema={
                    "fields": [
                        {"name": "id", "type": "int64"},
                        {"name": "p_date", "type": "string"},
                    ]
                },
                create_table_if_not_exists=True,
                magnus_conf={"write_options": {"write": {"format": {"default": "lance"}}}},
            )

        mock_create.assert_called_once_with(
            "catalog.db.table",
            pa.schema([pa.field("id", pa.int64()), pa.field("p_date", pa.string())]),
            partition_columns=["p_date"],
            table_properties={"write.format.default": "lance"},
        )
        writer_module.MagnusMultiFileWriter.assert_called_once_with(
            "table",
            write_options={"write.format.default": "lance"},
        )
        writer.write.assert_called_once_with([{"id": 1}])
        writer.finish.assert_called_once_with()
        writer.commit.assert_called_once_with()

    def test_write_hf_dataset_to_magnus_infers_schema_on_create(self):
        class FakeFrame:
            def __init__(self, rows):
                self.rows = rows
                self.iloc = self

            def __len__(self):
                return len(self.rows)

            def __getitem__(self, item):
                return FakeFrame(self.rows[item])

            def to_dict(self, orient):
                if orient != "records":
                    raise AssertionError(f"Unexpected orient: {orient}")
                return self.rows

        class FakeHFDataset:
            features = SimpleNamespace(
                arrow_schema=pa.schema([pa.field("id", pa.int64()), pa.field("p_date", pa.string())])
            )

            def to_pandas(self):
                return FakeFrame([{"id": 1, "p_date": "20260421"}])

        writer = MagicMock()
        writer_module = SimpleNamespace(MagnusMultiFileWriter=MagicMock(return_value=writer))
        client = MagicMock()
        client.exist_table.return_value = False
        client.load_table.return_value = "table"
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))

        with patch(
            "data_juicer.core.io_utils.import_optional_dependency",
            side_effect=[writer_module, magnus_module],
        ):
            write_hf_dataset_to_magnus(
                FakeHFDataset(),
                "catalog.db.table",
                partition_columns=["p_date"],
                create_table_if_not_exists=True,
                infer_schema_on_create=True,
            )

        inferred_schema = pa.schema([pa.field("id", pa.int64()), pa.field("p_date", pa.string())])
        client.create_table.assert_called_once_with(
            "catalog",
            "db",
            "table",
            inferred_schema,
            properties={},
            partition_columns=["p_date"],
            load_table=False,
        )
        writer_module.MagnusMultiFileWriter.assert_called_once_with("table")

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

    def test_write_ray_dataset_to_magnus_overwrite_with_partition_columns_only_is_table_overwrite(self):
        dataset = FakeRayDataset([{"id": "1", "p_date": "20260421"}])
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists"),
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="OVERWRITE",
                partition_columns=["p_date"],
            )

        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset,
            identifier="catalog.db.table",
            operation="OVERWRITE",
        )

    def test_create_magnus_table_sets_requested_file_format_property(self):
        client = MagicMock()
        client.exist_table.return_value = False
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))
        schema = pa.schema([pa.field("id", pa.string()), pa.field("p_date", pa.string())])

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=magnus_module):
            create_magnus_table_if_not_exists(
                "catalog.db.table",
                schema,
                partition_columns=["p_date"],
                table_properties={"write.format.default": "lance"},
            )

        client.create_table.assert_called_once_with(
            "catalog",
            "db",
            "table",
            schema,
            properties={"write.format.default": "lance"},
            partition_columns=["p_date"],
            load_table=False,
        )

    def test_create_magnus_table_strips_arrow_metadata_before_sdk_create(self):
        client = MagicMock()
        client.exist_table.return_value = False
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))
        schema = pa.schema(
            [
                pa.field(
                    "state",
                    pa.struct(
                        [
                            pa.field("query_time", pa.string(), metadata={b"child_key": b"child_value"}),
                        ]
                    ),
                    metadata={b"field_key": b"field_value"},
                )
            ],
            metadata={b"schema_key": b"schema_value"},
        )

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=magnus_module):
            create_magnus_table_if_not_exists("catalog.db.table", schema)

        created_schema = client.create_table.call_args.args[3]
        self.assertIsNone(created_schema.metadata)
        self.assertIsNone(created_schema.field("state").metadata)
        self.assertIsNone(created_schema.field("state").type.field("query_time").metadata)

    def test_create_magnus_table_strips_nested_arrow_metadata_when_top_level_is_clean(self):
        client = MagicMock()
        client.exist_table.return_value = False
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))
        schema = pa.schema(
            [
                pa.field(
                    "events",
                    pa.list_(pa.field("event", pa.struct([pa.field("id", pa.string(), metadata={b"k": b"v"})]))),
                )
            ]
        )

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=magnus_module):
            create_magnus_table_if_not_exists("catalog.db.table", schema)

        created_schema = client.create_table.call_args.args[3]
        id_field = created_schema.field("events").type.value_field.type.field("id")
        self.assertIsNone(id_field.metadata)

    def test_create_magnus_table_allows_existing_table_without_schema(self):
        client = MagicMock()
        client.exist_table.return_value = True
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=magnus_module):
            result = create_magnus_table_if_not_exists("catalog.db.table", None)

        self.assertIs(result, client)
        client.load_table.assert_not_called()
        client.create_table.assert_not_called()

    def test_create_magnus_table_rejects_missing_schema_when_table_missing(self):
        client = MagicMock()
        client.exist_table.return_value = False
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=magnus_module):
            with self.assertRaisesRegex(ValueError, "export.schema"):
                create_magnus_table_if_not_exists("catalog.db.table", None)

        client.create_table.assert_not_called()

    def test_existing_magnus_table_rejects_mismatched_file_format_property(self):
        schema = pa.schema([pa.field("id", pa.string())])
        table = SimpleNamespace(
            metadata=SimpleNamespace(properties={"write.format.default": "parquet"}),
            schema=lambda: schema,
            spec=lambda: SimpleNamespace(fields=[]),
        )
        client = MagicMock()
        client.exist_table.return_value = True
        client.load_table.return_value = table
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=magnus_module):
            with self.assertRaisesRegex(ValueError, "write.format.default"):
                create_magnus_table_if_not_exists(
                    "catalog.db.table",
                    schema,
                    table_properties={"write.format.default": "lance"},
                )

        client.create_table.assert_not_called()

    def test_write_ray_dataset_to_magnus_rejects_unknown_operation(self):
        dataset = MagicMock()
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray):
            with self.assertRaisesRegex(ValueError, "Unsupported Magnus write operation"):
                write_ray_dataset_to_magnus(dataset, "catalog.db.table", operation="DELETE")

    def test_write_ray_dataset_to_magnus_uses_explicit_schema(self):
        dataset = MagicMock()
        dataset.schema.side_effect = AssertionError("inferred schema should not be used")
        explicit_schema = {
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "p_date", "type": "string"},
            ]
        }
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists") as mock_create_table,
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                schema=explicit_schema,
                partition_columns=["p_date"],
                create_table_if_not_exists=True,
            )

        created_schema = mock_create_table.call_args.args[1]
        self.assertEqual(created_schema, pa.schema([pa.field("id", pa.string()), pa.field("p_date", pa.string())]))

    def test_write_ray_dataset_to_magnus_projects_dataset_to_explicit_schema(self):
        dataset = FakeRayDataset(
            [{"id": "1", "p_date": "20260428", "__dj__stats__": {"score": 1.0}}],
            pa.schema(
                [
                    pa.field("id", pa.string()),
                    pa.field("p_date", pa.string()),
                    pa.field("__dj__stats__", pa.struct([pa.field("score", pa.float64())])),
                ]
            ),
        )
        explicit_schema = {
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "p_date", "type": "string"},
            ]
        }
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists"),
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                schema=explicit_schema,
            )

        written_dataset = pyiceberg_ray.write_magnus.call_args.args[0]
        self.assertEqual(written_dataset.columns(), ["id", "p_date"])
        self.assertEqual(written_dataset.rows, [{"id": "1", "p_date": "20260428"}])

    def test_write_ray_dataset_to_magnus_aligns_nested_struct_fields_to_explicit_schema(self):
        inferred_messages_type = pa.list_(
            pa.struct([pa.field("content", pa.string()), pa.field("role", pa.string())])
        )
        expected_messages_type = pa.list_(
            pa.struct([pa.field("role", pa.string()), pa.field("content", pa.string())])
        )
        dataset = FakeRayDataset(
            [
                {
                    "id": "1",
                    "messages": [
                        {"role": "user", "content": "prompt"},
                        {"role": "assistant", "content": "answer"},
                    ],
                }
            ],
            pa.schema(
                [
                    pa.field("id", pa.string()),
                    pa.field("messages", inferred_messages_type),
                ]
            ),
        )
        explicit_schema = {
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "messages", "type": "list<struct<role:string,content:string>>"},
            ]
        }
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists"),
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                schema=explicit_schema,
            )

        written_dataset = pyiceberg_ray.write_magnus.call_args.args[0]
        self.assertEqual(written_dataset._schema.field("messages").type, expected_messages_type)
        self.assertEqual(written_dataset.rows[0]["messages"][0]["role"], "user")

    def test_write_ray_dataset_to_magnus_rebuilds_empty_unknown_schema_dataset_from_explicit_schema(self):
        dataset = EmptyUnknownSchemaRayDataset()
        replacement_dataset = object()
        fake_ray = SimpleNamespace(data=SimpleNamespace(from_arrow=MagicMock(return_value=replacement_dataset)))
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())
        explicit_schema = {
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "p_date", "type": "string"},
                {"name": "messages", "type": "list<struct<role:string,content:string>>"},
            ]
        }

        with (
            patch.dict(sys.modules, {"ray": fake_ray}),
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists") as mock_create_table,
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                schema=explicit_schema,
                partition_columns=["p_date"],
                create_table_if_not_exists=True,
            )

        empty_table = fake_ray.data.from_arrow.call_args.args[0]
        expected_schema = pa.schema(
            [
                pa.field("id", pa.string()),
                pa.field("p_date", pa.string()),
                pa.field("messages", pa.list_(pa.struct([pa.field("role", pa.string()), pa.field("content", pa.string())]))),
            ]
        )
        self.assertEqual(empty_table.schema, expected_schema)
        mock_create_table.assert_called_once_with(
            "catalog.db.table",
            expected_schema,
            partition_columns=["p_date"],
        )
        pyiceberg_ray.write_magnus.assert_called_once_with(
            replacement_dataset,
            identifier="catalog.db.table",
            operation="APPEND",
        )

    def test_write_ray_dataset_to_magnus_create_table_allows_missing_schema_until_create(self):
        dataset = EmptyUnknownSchemaRayDataset()
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists") as mock_create_table,
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                create_table_if_not_exists=True,
            )

        mock_create_table.assert_called_once_with(
            "catalog.db.table",
            None,
            partition_columns=None,
        )
        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset,
            identifier="catalog.db.table",
            operation="APPEND",
        )

    def test_create_magnus_table_does_not_infer_schema_when_table_exists(self):
        client = MagicMock()
        client.exist_table.return_value = True
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))
        schema_provider = MagicMock(side_effect=AssertionError("schema inference should not run for existing tables"))

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=magnus_module):
            create_magnus_table_if_not_exists(
                "catalog.db.table",
                None,
                schema_provider=schema_provider,
            )

        schema_provider.assert_not_called()
        client.load_table.assert_not_called()
        client.create_table.assert_not_called()

    def test_write_ray_dataset_to_magnus_infers_schema_on_create(self):
        class SchemaFetchingRayDataset(FakeRayDataset):
            def __init__(self):
                super().__init__([{"id": "1", "p_date": "20260421"}])
                self.schema_calls = []

            def schema(self, *args, **kwargs):
                self.schema_calls.append((args, kwargs))
                return SimpleNamespace(base_schema=self._schema)

        dataset = SchemaFetchingRayDataset()
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())
        client = MagicMock()
        client.exist_table.return_value = False
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))

        with patch(
            "data_juicer.core.io_utils.import_optional_dependency",
            side_effect=[pyiceberg_ray, pyiceberg_ray, pyiceberg_ray, pyiceberg_ray, magnus_module],
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                partition_columns=["p_date"],
                create_table_if_not_exists=True,
                infer_schema_on_create=True,
            )

        self.assertEqual(dataset.schema_calls, [((), {"fetch_if_missing": True})])
        client.create_table.assert_called_once_with(
            "catalog",
            "db",
            "table",
            dataset._schema,
            properties={},
            partition_columns=["p_date"],
            load_table=False,
        )
        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset,
            identifier="catalog.db.table",
            operation="APPEND",
        )

    def test_write_ray_dataset_to_magnus_reuses_materialized_dataset_after_batch_schema_inference(self):
        class MaterializedRayDataset:
            def __init__(self):
                self._schema = pa.schema(
                    [
                        pa.field("id", pa.string()),
                        pa.field("state", pa.struct([pa.field("ok", pa.bool_())])),
                    ]
                )

            def schema(self, *args, **kwargs):
                return SimpleNamespace(base_schema=None)

            def iter_batches(self, batch_format="pyarrow", batch_size=8192):
                if batch_format != "pyarrow":
                    raise ValueError(batch_format)
                yield pa.Table.from_pylist([{"id": "1", "state": {"ok": True}}], schema=self._schema)

        class UnknownLazyRayDataset:
            def __init__(self):
                self.materialized = MaterializedRayDataset()
                self.materialize_calls = 0

            def schema(self, *args, **kwargs):
                return SimpleNamespace(base_schema=None)

            def materialize(self):
                self.materialize_calls += 1
                return self.materialized

            def iter_batches(self, *args, **kwargs):
                raise AssertionError("schema inference should use the materialized dataset")

        dataset = UnknownLazyRayDataset()
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())
        client = MagicMock()
        client.exist_table.return_value = False
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))

        with patch(
            "data_juicer.core.io_utils.import_optional_dependency",
            side_effect=[pyiceberg_ray, pyiceberg_ray, pyiceberg_ray, pyiceberg_ray, magnus_module],
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                create_table_if_not_exists=True,
                infer_schema_on_create=True,
            )

        self.assertEqual(dataset.materialize_calls, 1)
        client.create_table.assert_called_once_with(
            "catalog",
            "db",
            "table",
            dataset.materialized._schema,
            properties={},
            partition_columns=None,
            load_table=False,
        )
        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset.materialized,
            identifier="catalog.db.table",
            operation="APPEND",
        )

    def test_write_ray_dataset_to_magnus_infer_schema_rejects_unknown_schema(self):
        dataset = EmptyUnknownSchemaRayDataset()
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())
        client = MagicMock()
        client.exist_table.return_value = False
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))

        with patch(
            "data_juicer.core.io_utils.import_optional_dependency",
            side_effect=[pyiceberg_ray, pyiceberg_ray, pyiceberg_ray, pyiceberg_ray, magnus_module],
        ):
            with self.assertRaisesRegex(ValueError, "infer_schema_on_create"):
                write_ray_dataset_to_magnus(
                    dataset,
                    "catalog.db.table",
                    create_table_if_not_exists=True,
                    infer_schema_on_create=True,
                )

        client.create_table.assert_not_called()
        pyiceberg_ray.write_magnus.assert_not_called()

    def test_write_ray_dataset_to_magnus_infer_schema_wraps_schema_fetch_failure(self):
        class BrokenSchemaRayDataset:
            def columns(self):
                return ["id"]

            def schema(self, *args, **kwargs):
                raise RuntimeError("schema unavailable")

        dataset = BrokenSchemaRayDataset()
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())
        client = MagicMock()
        client.exist_table.return_value = False
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))

        with patch(
            "data_juicer.core.io_utils.import_optional_dependency",
            side_effect=[pyiceberg_ray, pyiceberg_ray, pyiceberg_ray, pyiceberg_ray, magnus_module],
        ):
            with self.assertRaisesRegex(ValueError, "failed to fetch Ray Dataset schema"):
                write_ray_dataset_to_magnus(
                    dataset,
                    "catalog.db.table",
                    create_table_if_not_exists=True,
                    infer_schema_on_create=True,
                )

        client.create_table.assert_not_called()
        pyiceberg_ray.write_magnus.assert_not_called()

    def test_write_ray_dataset_to_magnus_inferred_schema_rejects_missing_partition_column(self):
        dataset = FakeRayDataset(
            [{"id": "1"}],
            pa.schema([pa.field("id", pa.string())]),
        )
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())
        client = MagicMock()
        client.exist_table.return_value = False
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))

        with patch(
            "data_juicer.core.io_utils.import_optional_dependency",
            side_effect=[pyiceberg_ray, pyiceberg_ray, pyiceberg_ray, pyiceberg_ray, magnus_module],
        ):
            with self.assertRaisesRegex(ValueError, "partition columns in `export.schema`"):
                write_ray_dataset_to_magnus(
                    dataset,
                    "catalog.db.table",
                    partition_columns=["p_date"],
                    create_table_if_not_exists=True,
                    infer_schema_on_create=True,
                )

        client.create_table.assert_not_called()
        pyiceberg_ray.write_magnus.assert_not_called()

    def test_write_ray_dataset_to_magnus_create_table_allows_unpartitioned_schema(self):
        dataset = MagicMock()
        explicit_schema = {"fields": [{"name": "id", "type": "string"}]}
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists") as mock_create_table,
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                schema=explicit_schema,
                create_table_if_not_exists=True,
            )

        mock_create_table.assert_called_once_with(
            "catalog.db.table",
            pa.schema([pa.field("id", pa.string())]),
            partition_columns=None,
        )
        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset,
            identifier="catalog.db.table",
            operation="APPEND",
        )

    def test_write_ray_dataset_to_magnus_create_table_rejects_partition_not_in_schema(self):
        dataset = MagicMock()
        explicit_schema = {"fields": [{"name": "id", "type": "string"}]}
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())
        client = MagicMock()
        client.exist_table.return_value = False
        magnus_module = SimpleNamespace(MagnusClient=MagicMock(return_value=client))

        with patch(
            "data_juicer.core.io_utils.import_optional_dependency",
            side_effect=[pyiceberg_ray, pyiceberg_ray, pyiceberg_ray, pyiceberg_ray, magnus_module],
        ):
            with self.assertRaisesRegex(ValueError, "partition columns in `export.schema`"):
                write_ray_dataset_to_magnus(
                    dataset,
                    "catalog.db.table",
                    schema=explicit_schema,
                    partition_columns=["p_date"],
                    create_table_if_not_exists=True,
                )

        pyiceberg_ray.write_magnus.assert_not_called()

    def test_overwrite_partition_uses_lazy_validation_by_default(self):
        dataset = FakeRayDataset(
            [
                {"id": "1", "p_date": "20260421"},
                {"id": "2", "p_date": "20260421"},
            ]
        )
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists") as mock_create_table,
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="OVERWRITE",
                partition_columns=["p_date"],
                partition_values={"p_date": "20260421"},
            )

        written_dataset = pyiceberg_ray.write_magnus.call_args.args[0]
        self.assertIsNot(written_dataset, dataset)
        self.assertEqual(written_dataset.rows, dataset.rows)
        mock_create_table.assert_not_called()
        pyiceberg_ray.write_magnus.assert_called_once_with(
            written_dataset,
            identifier="catalog.db.table",
            operation="OVERWRITE",
            write_options={
                "magnus.ray.write.disable_repartition": "true",
                "magnus.ray.write.disable_sort": "true",
            },
        )

    def test_overwrite_partition_operation_alias_writes_as_overwrite(self):
        dataset = FakeRayDataset(
            [
                {"id": "1", "p_date": "20260421"},
                {"id": "2", "p_date": "20260421"},
            ]
        )
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists") as mock_create_table,
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="OVERWRITE_PARTITION",
                partition_columns=["p_date"],
                partition_values={"p_date": "20260421"},
            )

        written_dataset = pyiceberg_ray.write_magnus.call_args.args[0]
        self.assertIsNot(written_dataset, dataset)
        self.assertEqual(written_dataset.rows, dataset.rows)
        mock_create_table.assert_not_called()
        pyiceberg_ray.write_magnus.assert_called_once_with(
            written_dataset,
            identifier="catalog.db.table",
            operation="OVERWRITE",
            write_options={
                "magnus.ray.write.disable_repartition": "true",
                "magnus.ray.write.disable_sort": "true",
            },
        )

    def test_overwrite_partition_keeps_lazy_dataset_without_prewrite_validation_by_default(self):
        dataset = LazyOnlyRayDataset([{"id": "1", "p_date": "20260421"}])
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists"),
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="OVERWRITE",
                partition_columns=["p_date"],
                partition_values={"p_date": "20260421"},
            )

        written_dataset = pyiceberg_ray.write_magnus.call_args.args[0]
        self.assertEqual(written_dataset.rows, [{"id": "1", "p_date": "20260421"}])

    def test_overwrite_partition_does_not_infer_schema_after_lazy_maps_when_create_disabled(self):
        class SchemaLostRayDataset(FakeRayDataset):
            def columns(self, *args, **kwargs):
                return None

            def schema(self, *args, **kwargs):
                raise AssertionError("schema should not be fetched after lazy validation maps")

            def map_batches(self, fn, *, batch_format="pyarrow", **kwargs):
                if batch_format != "pyarrow":
                    raise ValueError(batch_format)
                table = pa.Table.from_pylist(self.rows, schema=self._schema)
                output = fn(table)
                return SchemaLostRayDataset(output.to_pylist(), output.schema)

        class SchemaLosingRayDataset(FakeRayDataset):
            def map_batches(self, fn, *, batch_format="pyarrow", **kwargs):
                if batch_format != "pyarrow":
                    raise ValueError(batch_format)
                table = pa.Table.from_pylist(self.rows, schema=self._schema)
                output = fn(table)
                return SchemaLostRayDataset(output.to_pylist(), output.schema)

        dataset = SchemaLosingRayDataset([{"id": "1", "p_date": "20260421"}])
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists") as mock_create_table,
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="OVERWRITE",
                partition_columns=["p_date"],
                partition_values={"p_date": "20260421"},
            )

        mock_create_table.assert_not_called()
        pyiceberg_ray.write_magnus.assert_called_once()

    def test_overwrite_partition_eager_validation_materializes_and_counts_when_enabled(self):
        class TrackingRayDataset(FakeRayDataset):
            def __init__(self, rows):
                super().__init__(rows)
                self.materialize_calls = 0
                self.count_calls = 0

            def materialize(self):
                self.materialize_calls += 1
                return self

            def count(self):
                self.count_calls += 1
                return super().count()

        dataset = TrackingRayDataset([{"id": "1", "p_date": "20260421"}])
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists"),
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="OVERWRITE",
                partition_columns=["p_date"],
                partition_values={"p_date": "20260421"},
                validate_overwrite_partition_before_write=True,
            )

        self.assertEqual(dataset.materialize_calls, 1)
        self.assertEqual(dataset.count_calls, 1)
        pyiceberg_ray.write_magnus.assert_called_once_with(
            dataset,
            identifier="catalog.db.table",
            operation="OVERWRITE",
            write_options={
                "magnus.ray.write.disable_repartition": "true",
                "magnus.ray.write.disable_sort": "true",
            },
        )

    def test_overwrite_partition_adds_missing_partition_column_from_partition_value(self):
        dataset = FakeRayDataset(
            [{"id": "1"}, {"id": "2"}],
            pa.schema([pa.field("id", pa.string())]),
        )
        explicit_schema = {
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "p_date", "type": "string"},
            ]
        }
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists") as mock_create_table,
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="OVERWRITE",
                partition_columns=["p_date"],
                partition_values={"p_date": "20260421"},
                schema=explicit_schema,
                create_table_if_not_exists=True,
            )

        written_dataset = pyiceberg_ray.write_magnus.call_args.args[0]
        self.assertEqual(written_dataset.columns(), ["id", "p_date"])
        self.assertEqual([row["p_date"] for row in written_dataset.rows], ["20260421", "20260421"])
        mock_create_table.assert_called_once_with(
            "catalog.db.table",
            pa.schema([pa.field("id", pa.string()), pa.field("p_date", pa.string())]),
            partition_columns=["p_date"],
        )

    def test_overwrite_partition_preserves_configured_ray_write_options(self):
        dataset = FakeRayDataset([{"id": "1", "p_date": "20260421"}])
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists"),
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="OVERWRITE",
                partition_columns=["p_date"],
                partition_values={"p_date": "20260421"},
                magnus_conf={
                    "write_options": {
                        "magnus.ray.write.disable_repartition": "false",
                        "custom.option": "keep",
                    }
                },
            )

        written_dataset = pyiceberg_ray.write_magnus.call_args.args[0]
        pyiceberg_ray.write_magnus.assert_called_once_with(
            written_dataset,
            identifier="catalog.db.table",
            operation="OVERWRITE",
            write_options={
                "magnus.ray.write.disable_repartition": "false",
                "magnus.ray.write.disable_sort": "true",
                "custom.option": "keep",
            },
        )

    def test_overwrite_partition_with_ray_checkpoint_keeps_lazy_dataset(self):
        dataset = LazyOnlyRayDataset([{"id": "1", "p_date": "20260421"}])
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists"),
            patch("data_juicer.core.io_utils._is_ray_data_checkpoint_enabled", return_value=True),
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="OVERWRITE",
                partition_columns=["p_date"],
                partition_values={"p_date": "20260421"},
            )

        written_dataset = pyiceberg_ray.write_magnus.call_args.args[0]
        self.assertIsInstance(written_dataset, FakeRayDataset)
        self.assertEqual(written_dataset.rows, [{"id": "1", "p_date": "20260421"}])
        pyiceberg_ray.write_magnus.assert_called_once_with(
            written_dataset,
            identifier="catalog.db.table",
            operation="OVERWRITE",
            write_options={
                "magnus.ray.write.disable_repartition": "true",
                "magnus.ray.write.disable_sort": "true",
            },
        )

    def test_overwrite_partition_with_ray_checkpoint_uses_explicit_schema_without_columns_fetch(self):
        explicit_schema = {
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "p_date", "type": "string"},
            ]
        }
        dataset = StrictCheckpointRayDataset([{"id": "1", "p_date": "20260421"}])
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with (
            patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray),
            patch("data_juicer.core.io_utils.create_magnus_table_if_not_exists"),
            patch("data_juicer.core.io_utils._is_ray_data_checkpoint_enabled", return_value=True),
        ):
            write_ray_dataset_to_magnus(
                dataset,
                "catalog.db.table",
                operation="OVERWRITE",
                partition_columns=["p_date"],
                partition_values={"p_date": "20260421"},
                schema=explicit_schema,
            )

        written_dataset = pyiceberg_ray.write_magnus.call_args.args[0]
        self.assertEqual(written_dataset.rows, [{"id": "1", "p_date": "20260421"}])

    def test_overwrite_partition_rejects_empty_dataset_with_eager_validation(self):
        dataset = FakeRayDataset([])
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray):
            with self.assertRaisesRegex(ValueError, "empty dataset"):
                write_ray_dataset_to_magnus(
                    dataset,
                    "catalog.db.table",
                    operation="OVERWRITE",
                    partition_columns=["p_date"],
                    partition_values={"p_date": "20260421"},
                    validate_overwrite_partition_before_write=True,
                )

        pyiceberg_ray.write_magnus.assert_not_called()

    def test_overwrite_partition_rejects_empty_unknown_schema_with_eager_validation(self):
        dataset = EmptyUnknownSchemaRayDataset()
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray):
            with self.assertRaisesRegex(ValueError, "empty dataset"):
                write_ray_dataset_to_magnus(
                    dataset,
                    "catalog.db.table",
                    operation="OVERWRITE",
                    partition_columns=["p_date"],
                    partition_values={"p_date": "20260421"},
                    schema={"fields": [{"name": "p_date", "type": "string"}]},
                    validate_overwrite_partition_before_write=True,
                )

        pyiceberg_ray.write_magnus.assert_not_called()

    def test_overwrite_partition_rejects_unexpected_partition_value(self):
        dataset = FakeRayDataset([{"id": "1", "p_date": "20260420"}])
        pyiceberg_ray = SimpleNamespace(write_magnus=MagicMock())

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=pyiceberg_ray):
            with self.assertRaisesRegex(ValueError, "unexpected partition value"):
                write_ray_dataset_to_magnus(
                    dataset,
                    "catalog.db.table",
                    operation="OVERWRITE",
                    partition_columns=["p_date"],
                    partition_values={"p_date": "20260421"},
                )

        pyiceberg_ray.write_magnus.assert_not_called()
