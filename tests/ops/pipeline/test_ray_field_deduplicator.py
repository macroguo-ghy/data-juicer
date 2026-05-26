import unittest

import pyarrow as pa

_register_extension_type = pa.register_extension_type


def _register_extension_type_once(extension_type):
    try:
        _register_extension_type(extension_type)
    except pa.ArrowKeyError:
        if not extension_type.extension_name.startswith("datasets.features.features."):
            raise


pa.register_extension_type = _register_extension_type_once

from data_juicer.core.data import NestedDataset
from data_juicer.ops.pipeline.ray_field_deduplicator import RayFieldDeduplicator

pa.register_extension_type = _register_extension_type


class RayFieldDeduplicatorTest(unittest.TestCase):
    def test_ray_dataset_builds_hash_groupby_plan(self):
        class GroupedData:
            def __init__(self, dataset, key):
                self.dataset = dataset
                self.key = key

            def map_groups(self, fn, *, batch_format, fn_kwargs):
                self.dataset.group_batch_format = batch_format
                self.dataset.group_fn_kwargs = fn_kwargs
                return self.dataset

        class Dataset:
            def __init__(self):
                self.map_batches_kwargs = None
                self.group_key = None

            def map_batches(self, fn, *, batch_format, batch_size, fn_kwargs):
                self.map_batches_kwargs = {
                    "batch_format": batch_format,
                    "batch_size": batch_size,
                    "fn_kwargs": fn_kwargs,
                }
                return self

            def groupby(self, key):
                self.group_key = key
                return GroupedData(self, key)

        dataset = Dataset()

        output = RayFieldDeduplicator(field_key="images", batch_size=7).run(dataset)

        self.assertIs(output, dataset)
        self.assertEqual(dataset.map_batches_kwargs["batch_format"], "pyarrow")
        self.assertEqual(dataset.map_batches_kwargs["batch_size"], 7)
        self.assertEqual(dataset.map_batches_kwargs["fn_kwargs"]["field_key"], "images")
        self.assertEqual(dataset.group_key, RayFieldDeduplicator._HASH_KEY)
        self.assertEqual(dataset.group_batch_format, "pyarrow")

    def test_nested_dataset_deduplicates_by_field_value(self):
        dataset = NestedDataset.from_list(
            [
                {"id": "a", "images": [b"1"]},
                {"id": "b", "images": [b"1"]},
                {"id": "c", "images": [b"2"]},
            ]
        )

        rows = RayFieldDeduplicator(field_key="images").run(dataset).to_list()

        self.assertEqual(rows, [{"id": "a", "images": [b"1"]}, {"id": "c", "images": [b"2"]}])

    def test_append_hash_batch_and_take_first_group_preserve_schema(self):
        table = pa.Table.from_pylist(
            [{"id": "a", "images": [b"1"]}, {"id": "b", "images": [b"1"]}],
            schema=pa.schema([pa.field("id", pa.string()), pa.field("images", pa.list_(pa.binary()))]),
        )

        with_hash = RayFieldDeduplicator._append_hash_batch(
            table,
            field_key="images",
            hash_key=RayFieldDeduplicator._HASH_KEY,
        )
        output = RayFieldDeduplicator._take_first_group(
            with_hash,
            hash_key=RayFieldDeduplicator._HASH_KEY,
        )

        self.assertEqual(with_hash.num_rows, 2)
        self.assertIn(RayFieldDeduplicator._HASH_KEY, with_hash.column_names)
        self.assertEqual(output.to_pylist(), [{"id": "a", "images": [b"1"]}])
        self.assertEqual(output.schema, table.schema)

    def test_constructor_validates_field_key(self):
        with self.assertRaisesRegex(ValueError, "field_key"):
            RayFieldDeduplicator(field_key="")

    def test_hash_value_handles_nested_values(self):
        value = {"b": [b"x", None], "a": ("1", 2)}

        self.assertEqual(RayFieldDeduplicator._hash_value(value), RayFieldDeduplicator._hash_value(value))
        self.assertNotEqual(RayFieldDeduplicator._hash_value(value), RayFieldDeduplicator._hash_value({"a": "1"}))

    def test_group_representative_writes_duplicate_ids_when_existing_list_is_null_typed(self):
        table = pa.Table.from_arrays(
            [
                pa.array(["b", "a"], type=pa.string()),
                pa.array(["same", "same"], type=pa.string()),
                pa.array([[], []], type=pa.list_(pa.null())),
            ],
            names=["id", "md5", "duplicate_id_list"],
        )
        with_hash = RayFieldDeduplicator._append_hash_batch(
            table,
            field_key="md5",
            hash_key=RayFieldDeduplicator._HASH_KEY,
        )

        output = RayFieldDeduplicator._take_group_representative(
            with_hash,
            hash_key=RayFieldDeduplicator._HASH_KEY,
            id_key="id",
            duplicate_ids_key="duplicate_id_list",
            duplicate_ids_mode="removed",
            representative_policy="min_id",
        )

        self.assertEqual(output.to_pylist(), [{"id": "a", "md5": "same", "duplicate_id_list": ["b"]}])
        self.assertEqual(output.schema.field("duplicate_id_list").type, pa.list_(pa.string()))


if __name__ == "__main__":
    unittest.main()
