import unittest
from unittest.mock import patch

import pyarrow as pa

_register_extension_type = pa.register_extension_type


def _register_extension_type_once(extension_type):
    try:
        _register_extension_type(extension_type)
    except pa.ArrowKeyError:
        if not extension_type.extension_name.startswith("datasets.features.features."):
            raise


pa.register_extension_type = _register_extension_type_once
from data_juicer.ops.deduplicator.ray_field_dedup_pipeline import RayFieldDedupPipeline

pa.register_extension_type = _register_extension_type


class _LocalBackend:
    def __init__(self):
        self.seen = set()

    def is_unique(self, value):
        if value in self.seen:
            return False
        self.seen.add(value)
        return True


class _PreparedLocalBackend(_LocalBackend):
    def __init__(self):
        super().__init__()
        self.prepared = False

    def prepare_for_ray_tasks(self):
        self.prepared = True


class _FakeRayDataset:
    def __init__(self):
        self.kwargs = None

    def map_batches(self, *args, **kwargs):
        self.kwargs = kwargs
        return "mapped"


class _FakeNestedDataset:
    def __init__(self, samples):
        self.samples = samples
        self.kwargs = None

    def filter(self, fn, **kwargs):
        self.kwargs = kwargs
        return [sample for sample in self.samples if fn(sample)]


class _Scalar:
    def __init__(self, value):
        self.value = value

    def as_py(self):
        return self.value


class RayFieldDedupPipelineTest(unittest.TestCase):
    def test_constructor_requires_field_key(self):
        with self.assertRaisesRegex(ValueError, "field_key"):
            RayFieldDedupPipeline(
                field_key="",
                auto_op_parallelism=False,
                num_proc=1,
            )

    def test_run_prepares_backend_and_uses_arrow_map_batches(self):
        op = RayFieldDedupPipeline(
            field_key="comment_id",
            batch_size=17,
            auto_op_parallelism=False,
            num_proc=1,
        )
        backend = _PreparedLocalBackend()
        op.backend = backend
        dataset = _FakeRayDataset()

        self.assertEqual(op.run(dataset), "mapped")
        self.assertTrue(backend.prepared)
        self.assertEqual(dataset.kwargs["batch_format"], "pyarrow")
        self.assertEqual(dataset.kwargs["batch_size"], 17)

    def test_run_supports_nested_dataset_with_local_seen_set(self):
        op = RayFieldDedupPipeline(
            field_key="comment_id",
            batch_size=19,
            auto_op_parallelism=False,
            num_proc=1,
        )
        dataset = _FakeNestedDataset(
            [
                {"comment_id": 1},
                {"comment_id": 1},
                {"comment_id": 2},
            ]
        )

        with patch(
            "data_juicer.ops.deduplicator.ray_field_dedup_pipeline.NestedDataset",
            _FakeNestedDataset,
        ):
            output = op.run(dataset)

        self.assertEqual(output, [{"comment_id": 1}, {"comment_id": 2}])
        self.assertEqual(dataset.kwargs["batch_size"], 19)
        self.assertEqual(dataset.kwargs["num_proc"], 1)

    def test_process_batched_deduplicates_field_without_adding_stats_column(self):
        op = RayFieldDedupPipeline(
            field_key="comment_id",
            auto_op_parallelism=False,
            num_proc=1,
        )
        op.backend = _LocalBackend()
        table = pa.table(
            {
                "comment_id": pa.array([1, 1, 2, None, None], type=pa.int64()),
                "content": pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
            }
        )

        output = op.process_batched(table)

        self.assertEqual(output.column("comment_id").to_pylist(), [1, 2, None])
        self.assertEqual(output.column("content").to_pylist(), ["a", "c", "d"])
        self.assertNotIn("__dj__stats__", output.column_names)

    def test_calculate_hash_distinguishes_string_and_integer_values(self):
        op = RayFieldDedupPipeline(
            field_key="payload.value",
            auto_op_parallelism=False,
            num_proc=1,
        )

        self.assertNotEqual(
            op.calculate_hash({"payload": {"value": 1}}),
            op.calculate_hash({"payload": {"value": "1"}}),
        )
        self.assertEqual(op.calculate_hash({"payload": {}}), "EMPTY")

    def test_process_batched_supports_dict_batches_and_empty_batches(self):
        op = RayFieldDedupPipeline(
            field_key="comment_id",
            auto_op_parallelism=False,
            num_proc=1,
        )
        op.backend = _LocalBackend()

        self.assertEqual(op.process_batched({}), {})
        output = op.process_batched(
            {
                "comment_id": [1, 1, 2],
                "content": ["a", "b", "c"],
            }
        )

        self.assertEqual(output, {"comment_id": [1, 2], "content": ["a", "c"]})

    def test_normalize_value_handles_arrow_scalar_arrays_and_bytes(self):
        self.assertEqual(
            RayFieldDedupPipeline._normalize_value(_Scalar("value")),
            '"value"',
        )
        self.assertEqual(
            RayFieldDedupPipeline._normalize_value(pa.array([1, 2])),
            "[1, 2]",
        )
        self.assertEqual(
            RayFieldDedupPipeline._normalize_value(b"value"),
            '"value"',
        )
        op = RayFieldDedupPipeline(
            field_key="payload.value",
            auto_op_parallelism=False,
            num_proc=1,
        )
        self.assertEqual(op._get_field_value(_Scalar({"payload": {"value": "x"}})), "x")
        self.assertEqual(op._get_field_value({"payload": {"value": _Scalar("x")}}), "x")


if __name__ == "__main__":
    unittest.main()
