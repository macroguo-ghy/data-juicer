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
from data_juicer.ops.deduplicator.ray_basic_deduplicator import DedupSet
from data_juicer.ops.deduplicator.ray_field_dedup_pipeline import RayFieldDedupPipeline

pa.register_extension_type = _register_extension_type


class _LocalBackend:
    def __init__(self):
        self.seen = set()
        self.calls = []

    def is_unique(self, value):
        if value in self.seen:
            return False
        self.seen.add(value)
        return True

    def is_unique_many(self, values, row_ids=None):
        self.calls.append((list(values), list(row_ids) if row_ids is not None else None))
        return [self.is_unique(value) for value in values]


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
    def test_dedup_set_batch_api_reuses_row_decisions_for_retries(self):
        dedup_set = DedupSet()

        self.assertEqual(dedup_set.is_unique_many(["same", "same"], ["a", "b"]), [True, False])
        self.assertEqual(dedup_set.is_unique_many(["same", "same"], ["a", "b"]), [True, False])
        self.assertFalse(dedup_set.is_unique("same", "c"))

    def test_constructor_requires_field_key(self):
        with self.assertRaisesRegex(ValueError, "field_key"):
            RayFieldDedupPipeline(
                field_key="",
                auto_op_parallelism=False,
                num_proc=1,
            )

    def test_constructor_passes_actor_timeout_options_to_backend(self):
        op = RayFieldDedupPipeline(
            field_key="comment_id",
            dedup_set_num=7,
            actor_get_timeout=123,
            actor_get_retry_times=4,
            auto_op_parallelism=False,
            num_proc=1,
        )

        self.assertEqual(op.backend._dedup_set_num_config, 7)
        self.assertEqual(op.backend.actor_get_timeout, 123.0)
        self.assertEqual(op.backend.actor_get_retry_times, 4)

    def test_run_prepares_backend_and_uses_arrow_map_batches(self):
        op = RayFieldDedupPipeline(
            field_key="comment_id",
            batch_size=17,
            auto_op_parallelism=False,
            num_proc=7,
            num_cpus=0.5,
            runtime_env={"env_vars": {"DJ_TEST": "1"}},
        )
        backend = _PreparedLocalBackend()
        op.backend = backend
        dataset = _FakeRayDataset()

        self.assertEqual(op.run(dataset), "mapped")
        self.assertTrue(backend.prepared)
        self.assertEqual(dataset.kwargs["batch_format"], "pyarrow")
        self.assertEqual(dataset.kwargs["batch_size"], 17)
        self.assertIsNotNone(dataset.kwargs["compute"])
        self.assertEqual(getattr(dataset.kwargs["compute"], "size", None), 7)
        self.assertEqual(dataset.kwargs["num_cpus"], 0.5)
        self.assertIsNone(dataset.kwargs["num_gpus"])
        self.assertEqual(dataset.kwargs["runtime_env"], {"env_vars": {"DJ_TEST": "1"}})

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

    def test_process_batched_applies_condition_and_passes_nonmatching_rows_through(self):
        op = RayFieldDedupPipeline(
            field_key="md5",
            condition="item_duration <= 60 and valid_video_count > 0",
            id_key="id",
            auto_op_parallelism=False,
            num_proc=1,
        )
        backend = _LocalBackend()
        op.backend = backend
        table = pa.table(
            {
                "id": pa.array(["a", "b", "long", "invalid", "c"], type=pa.string()),
                "md5": pa.array(["same", "same", "same", "same", "other"], type=pa.string()),
                "item_duration": pa.array([10, 10, 90, 10, 10], type=pa.int64()),
                "valid_video_count": pa.array([1, 1, 1, 0, 1], type=pa.int64()),
            }
        )

        with patch(
            "data_juicer.ops.deduplicator.ray_field_dedup_pipeline.emit_dedup_rows"
        ) as emit_mock:
            output = op.process_batched(table)

        self.assertEqual(output.column("id").to_pylist(), ["a", "long", "invalid", "c"])
        self.assertEqual(len(backend.calls), 1)
        self.assertEqual(backend.calls[0][1], ["a", "b", "c"])
        self.assertEqual(
            [call.kwargs for call in emit_mock.call_args_list],
            [
                {
                    "op_name": "ray_field_dedup_pipeline",
                    "field_key": "md5",
                    "event": "eligible",
                    "count": 3,
                    "extra_tags": {"backend": "ray_actor"},
                },
                {
                    "op_name": "ray_field_dedup_pipeline",
                    "field_key": "md5",
                    "event": "unique",
                    "count": 2,
                    "extra_tags": {"backend": "ray_actor"},
                },
                {
                    "op_name": "ray_field_dedup_pipeline",
                    "field_key": "md5",
                    "event": "duplicate",
                    "count": 1,
                    "extra_tags": {"backend": "ray_actor"},
                },
            ],
        )

    def test_process_batched_records_runtime_dedup_stats(self):
        op = RayFieldDedupPipeline(
            field_key="md5",
            auto_op_parallelism=False,
            num_proc=1,
        )
        op.backend = _LocalBackend()
        table = pa.table(
            {
                "md5": pa.array(["same", "same", "other"], type=pa.string()),
            }
        )

        with patch(
            "data_juicer.ops.deduplicator.ray_field_dedup_pipeline.RuntimeStatsCollector"
        ) as collector_cls:
            collector = collector_cls.return_value
            op.process_batched(table)

        self.assertEqual(
            [call.args for call in collector.increment.call_args_list],
            [
                ("dedup.eligible_rows", 3),
                ("dedup.unique_rows", 2),
                ("dedup.duplicate_rows", 1),
            ],
        )

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
