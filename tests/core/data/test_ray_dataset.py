import os
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from data_juicer.utils.unittest_utils import TEST_TAG, DataJuicerTestCaseBase


@unittest.skipUnless(importlib.util.find_spec("ray"), "ray is not installed")
class RayDatasetImportTest(unittest.TestCase):
    def test_json_stream_datasource_imports_with_current_ray(self):
        from data_juicer.core.data.ray_dataset import JSONStreamDatasource

        self.assertIsNotNone(JSONStreamDatasource)

    def _make_json_stream_datasource(self, on_bad_files="error"):
        from data_juicer.core.data.ray_dataset import JSONStreamDatasource

        datasource = object.__new__(JSONStreamDatasource)
        datasource.arrow_json_args = {}
        datasource.read_options = None
        datasource.on_bad_files = on_bad_files
        return datasource

    def test_json_stream_datasource_skips_empty_file_when_configured(self):
        import pyarrow as pa

        datasource = self._make_json_stream_datasource(on_bad_files="skip")

        tables = list(datasource._read_stream(pa.BufferReader(b""), "empty.jsonl"))

        self.assertEqual(tables, [])

    def test_json_stream_datasource_skips_invalid_file_when_configured(self):
        import pyarrow as pa

        datasource = self._make_json_stream_datasource(on_bad_files="skip")

        tables = list(datasource._read_stream(pa.BufferReader(b"{bad json}\n"), "bad.jsonl"))

        self.assertEqual(tables, [])

    def test_json_stream_datasource_skips_partially_invalid_file_as_whole_file(self):
        import pyarrow as pa

        datasource = self._make_json_stream_datasource(on_bad_files="skip")

        tables = list(
            datasource._read_stream(
                pa.BufferReader(b'{"id": 1, "text": "ok"}\n{"id": bad}\n'),
                "mixed_bad.jsonl",
            )
        )

        self.assertEqual(tables, [])

    def test_json_stream_datasource_errors_on_invalid_file_by_default(self):
        import pyarrow as pa

        datasource = self._make_json_stream_datasource()

        with self.assertRaisesRegex(ValueError, "Failed to read JSON file"):
            list(datasource._read_stream(pa.BufferReader(b"{bad json}\n"), "bad.jsonl"))

    def test_json_stream_datasource_reads_valid_jsonl(self):
        import pyarrow as pa

        datasource = self._make_json_stream_datasource(on_bad_files="skip")

        tables = list(
            datasource._read_stream(
                pa.BufferReader(b'{"id": 1, "text": "a"}\n{"id": 2, "text": "b"}\n'),
                "good.jsonl",
            )
        )

        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0].to_pylist(), [{"id": 1, "text": "a"}, {"id": 2, "text": "b"}])

    def test_configured_ray_columns_accepts_hive_cast_mapping(self):
        from types import SimpleNamespace

        from data_juicer.core.data.ray_dataset import get_configured_ray_columns

        cfg = SimpleNamespace(
            dataset={
                "configs": [
                    {"columns": {"id": "BIGINT", "text": "STRING"}},
                    {"columns": ["image"]},
                ]
            }
        )

        self.assertEqual(get_configured_ray_columns(cfg), ["id", "text", "image"])

    def test_state_metric_summary_output_stays_string_across_blocks(self):
        import pyarrow as pa
        import ray

        from data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper import (
            StateMetricCalculatorMapper,
        )
        from data_juicer.utils.operator_execution_callback_utils import RECORD_KEY_FIELD

        class DummyCallback:
            def report_record_success(self, **kwargs):
                return None

            def report_record_failure(self, **kwargs):
                return None

        detail = {
            "id": 901,
            "operatorNameEn": "echo_value",
            "operatorNameCn": "值回显",
            "inputParameter": (
                '{"params": ['
                '{"data_type": "placeholder", "key_name_en": "ids", '
                '"default_or_placeholder_value": "ids"},'
                '{"data_type": "placeholder", "key_name_en": "value", '
                '"default_or_placeholder_value": "value"}'
                ']}'
            ),
            "operatorCode": (
                "def calculate(value):\n"
                "    if value == 'bad':\n"
                "        raise ValueError('bad value')\n"
                "    return value\n"
            ),
        }
        op = StateMetricCalculatorMapper(
            operators=[{
                "operator_id": 901,
                "parameter_mapping": {
                    "ids": "metric_id",
                    "value": "value",
                },
            }],
            ctx={
                "apiBase": "https://ai-data-center.bytedance.net/api",
                "userAccount": "wangjianda.667",
            },
            auto_op_parallelism=False,
            num_proc=1,
        )
        op._operator_details = {901: detail}
        op._operator_execution_callback_client = DummyCallback()

        def apply_state_metric(batch):
            rows = []
            for row in batch.to_pylist():
                if row.get("empty_summary"):
                    row["query_metric_data_outputs"] = ""
                else:
                    row = op.process_single(row)
                rows.append(row)
            return pa.Table.from_pylist(rows)

        started_ray = False
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, include_dashboard=False)
            started_ray = True
        try:
            result = (
                ray.data.from_items([
                    {
                        RECORD_KEY_FIELD: "record-1",
                        "metric_id": "metric-1",
                        "value": "ok",
                    },
                    {
                        RECORD_KEY_FIELD: "record-2",
                        "metric_id": "metric-2",
                        "value": "bad",
                    },
                    {
                        RECORD_KEY_FIELD: "record-3",
                        "metric_id": "metric-3",
                        "value": "ignored",
                        "empty_summary": True,
                    },
                ])
                .repartition(2)
                .map_batches(apply_state_metric, batch_format="pyarrow")
            )

            values = []
            for batch in result.iter_batches(batch_format="pyarrow"):
                self.assertEqual(
                    batch.schema.field("query_metric_data_outputs").type,
                    pa.string(),
                )
                values.extend(batch.column("query_metric_data_outputs").to_pylist())

            self.assertIn("", values)
            self.assertTrue(all(isinstance(value, str) for value in values))
            self.assertTrue(any('"error": "bad value"' in value for value in values))
        finally:
            if started_ray:
                ray.shutdown()

    def test_process_materializes_and_calls_after_hook_after_each_op_when_enabled(self):
        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops import Mapper

        events = []

        class FakeRayData:
            def __init__(self):
                self.materialize_calls = 0

            def schema(self, *args, **kwargs):
                return SimpleNamespace(base_schema=SimpleNamespace(names=["text"]))

            def map_batches(self, *args, **kwargs):
                events.append(("map_batches", kwargs.get("batch_format")))
                return self

            def materialize(self):
                self.materialize_calls += 1
                events.append(("materialize", self.materialize_calls))
                return self

        class HookMapper(Mapper):
            _name = "hook_mapper"

            def process_single(self, sample):
                return sample

            def after_operator_finished(self, dataset=None, context=None, error=None):
                events.append((
                    "after",
                    self._name,
                    dataset.data.materialize_calls,
                    context["executor_type"],
                    error,
                ))

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["text"]}]})
        ray_dataset.data = FakeRayData()
        ray_dataset._auto_proc = False
        ray_dataset._cached_row_count = None
        ray_dataset._row_count_getter = None

        ray_dataset.process(
            [
                HookMapper(auto_op_parallelism=False),
                HookMapper(auto_op_parallelism=False),
            ],
            materialize_after_each_op=True,
        )

        self.assertEqual(ray_dataset.data.materialize_calls, 2)
        self.assertEqual(
            events,
            [
                ("map_batches", "pyarrow"),
                ("materialize", 1),
                ("after", "hook_mapper", 1, "ray", None),
                ("map_batches", "pyarrow"),
                ("materialize", 2),
                ("after", "hook_mapper", 2, "ray", None),
            ],
        )

    def test_process_does_not_materialize_or_call_after_hook_by_default(self):
        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops import Mapper

        events = []

        class FakeRayData:
            def schema(self, *args, **kwargs):
                return SimpleNamespace(base_schema=SimpleNamespace(names=["text"]))

            def map_batches(self, *args, **kwargs):
                events.append("map_batches")
                return self

            def materialize(self):
                events.append("materialize")
                return self

        class HookMapper(Mapper):
            _name = "hook_mapper"

            def process_single(self, sample):
                return sample

            def after_operator_finished(self, dataset=None, context=None, error=None):
                events.append("after")

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["text"]}]})
        ray_dataset.data = FakeRayData()
        ray_dataset._auto_proc = False
        ray_dataset._cached_row_count = None
        ray_dataset._row_count_getter = None

        ray_dataset.process([HookMapper(auto_op_parallelism=False)])

        self.assertEqual(events, ["map_batches"])

    def test_process_stateless_non_stats_filter_skips_stats_phase(self):
        from unittest.mock import patch

        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops.filter.stateless_field_filter import StatelessFieldFilter
        from data_juicer.utils.constant import Fields

        events = []
        compute_calls = []

        class FakeRayData:
            def __init__(self):
                self.rows = [
                    {"id": "keep", "valid_video_count": 1},
                    {"id": "drop", "valid_video_count": 0},
                ]

            def schema(self, *args, **kwargs):
                return SimpleNamespace(base_schema=SimpleNamespace(names=["id", "valid_video_count"]))

            def map_batches(self, *args, **kwargs):
                events.append(("map_batches", kwargs.get("batch_format")))
                return self

            def filter(self, filter_func, **kwargs):
                events.append(("filter", getattr(filter_func, "__name__", repr(filter_func)), kwargs))
                self.rows = [row for row in self.rows if filter_func(row)]
                return self

        def fake_get_compute_strategy(fn, concurrency):
            compute_calls.append((getattr(fn, "__name__", repr(fn)), concurrency))
            return "direct-filter-compute"

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["id", "valid_video_count"]}]})
        ray_dataset.data = FakeRayData()
        ray_dataset._auto_proc = False
        ray_dataset._cached_row_count = None
        ray_dataset._row_count_getter = None

        with patch(
            "data_juicer.core.data.ray_dataset.get_compute_strategy",
            side_effect=fake_get_compute_strategy,
        ):
            ray_dataset.process(
                StatelessFieldFilter(
                    filter_condition="valid_video_count > 0",
                    num_proc=8,
                    num_cpus=0.25,
                    num_gpus=0.5,
                    auto_op_parallelism=False,
                )
            )

        self.assertEqual(compute_calls, [("process_single", 8)])
        self.assertEqual(
            events,
            [
                (
                    "filter",
                    "process_single",
                    {
                        "compute": "direct-filter-compute",
                        "num_cpus": 0.25,
                        "num_gpus": 0.5,
                        "runtime_env": None,
                    },
                )
            ],
        )
        self.assertEqual(ray_dataset.data.rows, [{"id": "keep", "valid_video_count": 1}])
        self.assertNotIn(Fields.stats, ray_dataset.data.rows[0])

    def test_process_stateless_non_stats_filter_handles_nullable_arrow_rows(self):
        import pyarrow as pa
        import ray

        with tempfile.TemporaryDirectory() as av_stub_dir:
            Path(av_stub_dir, "av.py").write_text(
                "class _Logging:\n"
                "    PANIC = 0\n"
                "    def set_level(self, *args, **kwargs):\n"
                "        pass\n"
                "logging = _Logging()\n"
                "class _Container:\n"
                "    class InputContainer:\n"
                "        pass\n"
                "container = _Container()\n",
                encoding="utf-8",
            )
            pythonpath_parts = [av_stub_dir, os.getcwd()]
            if os.environ.get("PYTHONPATH"):
                pythonpath_parts.append(os.environ["PYTHONPATH"])
            pythonpath = os.pathsep.join(pythonpath_parts)
            sys.path.insert(0, av_stub_dir)

            from data_juicer.core.data.ray_dataset import RayDataset
            from data_juicer.ops.filter.stateless_field_filter import StatelessFieldFilter
            from data_juicer.utils.constant import Fields

            started_ray = False
            if not ray.is_initialized():
                ray.init(
                    num_cpus=2,
                    include_dashboard=False,
                    ignore_reinit_error=True,
                    runtime_env={"env_vars": {"PYTHONPATH": pythonpath}},
                )
                started_ray = True
            try:
                table = pa.table(
                    {
                        "id": pa.array(["null", "drop", "keep"], type=pa.string()),
                        "valid_video_count": pa.array([None, 0, 1], type=pa.int64()),
                    }
                )
                ray_dataset = RayDataset.__new__(RayDataset)
                ray_dataset.cfg = SimpleNamespace(
                    dataset={"configs": [{"columns": ["id", "valid_video_count"]}]}
                )
                ray_dataset.data = ray.data.from_arrow(table)
                ray_dataset._auto_proc = False
                ray_dataset._cached_row_count = None
                ray_dataset._row_count_getter = None

                ray_dataset.process(
                    StatelessFieldFilter(
                        filter_condition="valid_video_count > 0",
                        auto_op_parallelism=False,
                    )
                )

                rows = ray_dataset.data.take_all()

                self.assertEqual(rows, [{"id": "keep", "valid_video_count": 1}])
                self.assertNotIn(Fields.stats, rows[0])
            finally:
                if started_ray:
                    ray.shutdown()
                if av_stub_dir in sys.path:
                    sys.path.remove(av_stub_dir)

    def test_process_batched_non_stats_filter_uses_single_filter_batch(self):
        import pyarrow as pa
        from unittest.mock import patch

        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops.filter.specified_field_non_empty_filter import SpecifiedFieldNonEmptyFilter
        from data_juicer.utils.constant import Fields

        events = []
        compute_calls = []

        class FakeRayData:
            def __init__(self):
                self.rows = [
                    {"id": "drop", "ocr_result": []},
                    {"id": "keep", "ocr_result": ["ocr-json"]},
                ]

            def schema(self, *args, **kwargs):
                return SimpleNamespace(base_schema=SimpleNamespace(names=["id", "ocr_result"]))

            def map_batches(self, batch_func, *args, **kwargs):
                events.append(("map_batches", kwargs))
                output = batch_func(pa.Table.from_pylist(self.rows))
                self.rows = output.to_pylist()
                return self

            def filter(self, *args, **kwargs):
                events.append(("filter", None))
                return self

        def fake_get_compute_strategy(fn, concurrency):
            compute_calls.append((callable(fn), concurrency))
            return "direct-batch-filter-compute"

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["id", "ocr_result"]}]})
        ray_dataset.data = FakeRayData()
        ray_dataset._auto_proc = False
        ray_dataset._cached_row_count = None
        ray_dataset._row_count_getter = None

        with patch(
            "data_juicer.core.data.ray_dataset.get_compute_strategy",
            side_effect=fake_get_compute_strategy,
        ):
            ray_dataset.process(
                SpecifiedFieldNonEmptyFilter(
                    field_key="ocr_result",
                    batch_size=2,
                    num_proc=4,
                    num_cpus=0.25,
                    num_gpus=0.5,
                    auto_op_parallelism=False,
                )
            )

        self.assertEqual(compute_calls, [(True, 4)])
        self.assertEqual(len(events), 1)
        event_name, event_kwargs = events[0]
        self.assertEqual(event_name, "map_batches")
        self.assertEqual(
            {
                "batch_format": event_kwargs["batch_format"],
                "zero_copy_batch": event_kwargs["zero_copy_batch"],
                "batch_size": event_kwargs["batch_size"],
                "compute": event_kwargs["compute"],
                "num_cpus": event_kwargs["num_cpus"],
                "num_gpus": event_kwargs["num_gpus"],
                "runtime_env": event_kwargs["runtime_env"],
            },
            {
                "batch_format": "pyarrow",
                "zero_copy_batch": True,
                "batch_size": 2,
                "compute": "direct-batch-filter-compute",
                "num_cpus": 0.25,
                "num_gpus": 0.5,
                "runtime_env": None,
            },
        )

        self.assertEqual(ray_dataset.data.rows, [{"id": "keep", "ocr_result": ["ocr-json"]}])
        self.assertNotIn(Fields.stats, ray_dataset.data.rows[0])

    def test_process_does_not_call_base_lifecycle_hooks_for_plain_operator(self):
        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops import Mapper

        events = []

        class FakeRayData:
            def schema(self, *args, **kwargs):
                return SimpleNamespace(base_schema=SimpleNamespace(names=["text"]))

            def map_batches(self, *args, **kwargs):
                events.append("map_batches")
                return self

            def materialize(self):
                events.append("materialize")
                return self

        class PlainMapper(Mapper):
            _name = "plain_mapper"

            def process_single(self, sample):
                return sample

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["text"]}]})
        ray_dataset.data = FakeRayData()
        ray_dataset._auto_proc = False
        ray_dataset._cached_row_count = None
        ray_dataset._row_count_getter = None

        ray_dataset.process([PlainMapper(auto_op_parallelism=False)], materialize_after_each_op=True)

        self.assertEqual(events, ["map_batches", "materialize"])

    @unittest.skipUnless(importlib.util.find_spec("ray"), "ray is not installed")
    def test_process_calls_before_hook_once_before_each_op(self):
        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops import Mapper

        events = []

        class FakeRayData:
            def schema(self, *args, **kwargs):
                return SimpleNamespace(base_schema=SimpleNamespace(names=["text"]))

            def map_batches(self, *args, **kwargs):
                events.append(("map_batches", kwargs.get("batch_format")))
                return self

        class HookMapper(Mapper):
            _name = "hook_mapper"

            def before_operator_started(self, dataset=None, context=None):
                events.append(("before", self._name, context["executor_type"]))

            def process_single(self, sample):
                return sample

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["text"]}]})
        ray_dataset.data = FakeRayData()
        ray_dataset._auto_proc = False
        ray_dataset._cached_row_count = None
        ray_dataset._row_count_getter = None

        ray_dataset.process([
            HookMapper(auto_op_parallelism=False),
            HookMapper(auto_op_parallelism=False),
        ])

        self.assertEqual(
            events,
            [
                ("before", "hook_mapper", "ray"),
                ("map_batches", "pyarrow"),
                ("before", "hook_mapper", "ray"),
                ("map_batches", "pyarrow"),
            ],
        )

    @unittest.skipUnless(importlib.util.find_spec("ray"), "ray is not installed")
    def test_process_does_not_call_before_hook_for_plan_only(self):
        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops import Mapper

        events = []

        class FakeRayData:
            def schema(self, *args, **kwargs):
                return SimpleNamespace(base_schema=SimpleNamespace(names=["text"]))

            def map_batches(self, *args, **kwargs):
                events.append("map_batches")
                return self

        class HookMapper(Mapper):
            _name = "hook_mapper"

            def before_operator_started(self, dataset=None, context=None):
                events.append("before")

            def process_single(self, sample):
                return sample

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["text"]}]})
        ray_dataset.data = FakeRayData()
        ray_dataset._auto_proc = False
        ray_dataset._cached_row_count = None
        ray_dataset._row_count_getter = None

        ray_dataset.process([HookMapper(auto_op_parallelism=False)], plan_only=True)

        self.assertEqual(events, ["map_batches"])

    def test_process_calls_finished_hook_with_error_when_materialize_fails(self):
        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops import Mapper

        events = []

        class FakeRayData:
            def schema(self, *args, **kwargs):
                return SimpleNamespace(base_schema=SimpleNamespace(names=["text"]))

            def map_batches(self, *args, **kwargs):
                events.append("map_batches")
                return self

            def materialize(self):
                events.append("materialize")
                raise RuntimeError("materialize failed")

        class HookMapper(Mapper):
            _name = "hook_mapper"

            def process_single(self, sample):
                return sample

            def after_operator_finished(self, dataset=None, context=None, error=None):
                events.append(("after", context["executor_type"], str(error)))

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["text"]}]})
        ray_dataset.data = FakeRayData()
        ray_dataset._auto_proc = False
        ray_dataset._cached_row_count = None
        ray_dataset._row_count_getter = None

        with self.assertRaisesRegex(RuntimeError, "materialize failed"):
            ray_dataset.process(
                [HookMapper(auto_op_parallelism=False)],
                materialize_after_each_op=True,
            )

        self.assertEqual(
            events,
            ["map_batches", "materialize", ("after", "ray", "materialize failed")],
        )

    def test_process_preserves_materialize_failure_when_after_hook_fails(self):
        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops import Mapper

        events = []

        class FakeRayData:
            def schema(self, *args, **kwargs):
                return SimpleNamespace(base_schema=SimpleNamespace(names=["text"]))

            def map_batches(self, *args, **kwargs):
                events.append("map_batches")
                return self

            def materialize(self):
                events.append("materialize")
                raise RuntimeError("materialize failed")

        class HookMapper(Mapper):
            _name = "hook_mapper"

            def process_single(self, sample):
                return sample

            def after_operator_finished(self, dataset=None, context=None, error=None):
                events.append("after")
                raise RuntimeError("hook failed")

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["text"]}]})
        ray_dataset.data = FakeRayData()
        ray_dataset._auto_proc = False
        ray_dataset._cached_row_count = None
        ray_dataset._row_count_getter = None

        with self.assertRaisesRegex(RuntimeError, "materialize failed"):
            ray_dataset.process(
                [HookMapper(auto_op_parallelism=False)],
                materialize_after_each_op=True,
            )

        self.assertEqual(events, ["map_batches", "materialize", "after"])

    def test_process_retries_with_base_runtime_env_when_materialize_fails(self):
        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops import Mapper

        events = []

        class FakeRayData:
            def __init__(self):
                self.last_runtime_env = None

            def schema(self, *args, **kwargs):
                return SimpleNamespace(base_schema=SimpleNamespace(names=["text"]))

            def map_batches(self, *args, **kwargs):
                self.last_runtime_env = kwargs.get("runtime_env")
                events.append(("map_batches", self.last_runtime_env))
                return self

            def materialize(self):
                events.append(("materialize", self.last_runtime_env))
                if self.last_runtime_env:
                    raise RuntimeError("runtime env failed")
                return self

        class HookMapper(Mapper):
            _name = "hook_mapper"

            def process_single(self, sample):
                return sample

            def after_operator_finished(self, dataset=None, context=None, error=None):
                events.append(("after", self.runtime_env))

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["text"]}]})
        ray_dataset.data = FakeRayData()
        ray_dataset._auto_proc = False
        ray_dataset._cached_row_count = None
        ray_dataset._row_count_getter = None
        op = HookMapper(
            auto_op_parallelism=False,
            runtime_env={"pip": ["bad-package"]},
        )

        ray_dataset.process([op], materialize_after_each_op=True)

        self.assertEqual(op.runtime_env, {"pip": ["bad-package"]})
        self.assertEqual(
            events,
            [
                ("map_batches", {"pip": ["bad-package"]}),
                ("materialize", {"pip": ["bad-package"]}),
                ("map_batches", None),
                ("materialize", None),
                ("after", {"pip": ["bad-package"]}),
            ],
        )


class RayDatasetFuncsTest(DataJuicerTestCaseBase):

    def setUp(self):
        """Set up test data"""
        super().setUp()
        
        import ray
        from data_juicer.core.data.ray_dataset import (
            get_abs_path,
            convert_to_absolute_paths,
            set_dataset_to_absolute_path,
            preprocess_dataset
        )
        
        self.get_abs_path = get_abs_path
        self.convert_to_absolute_paths = convert_to_absolute_paths
        self.set_dataset_to_absolute_path = set_dataset_to_absolute_path
        self.preprocess_dataset = preprocess_dataset
        
        self.test_data = [
            {
                'text': 'Hello',
                'images': ['image1.jpg', 'subdir/image2.png'],
                'videos': ['video1.mp4'],
                'audios': ['audio1.wav', 'audio2.mp3']
            },
            {
                'text': 'World',
                'images': ['image3.jpg'],
                'videos': ['subdir/video2.mp4'],
                'audios': ['audio3.wav']
            }
        ]

        self.tmp_dir = 'tmp/test_ray_executor/'
        os.makedirs(self.tmp_dir, exist_ok=True)

    def tearDown(self) -> None:
        super().tearDown()
        if os.path.exists(self.tmp_dir):
            os.system(f'rm -rf {self.tmp_dir}')

    def _touch_a_file(self, path):
        """Create a file at the given path"""
        with open(path, 'w') as f:
            f.write('test')

    @TEST_TAG('ray')
    def test_get_abs_path_local(self):
        """Test get_abs_path function for local paths"""
        import os
        
        # Test relative path
        dataset_dir = self.tmp_dir
        rel_path = "image.jpg"
        full_path = os.path.join(dataset_dir, rel_path)
        self._touch_a_file(full_path)
        expected = os.path.abspath(os.path.join(dataset_dir, rel_path))
        result = self.get_abs_path(rel_path, dataset_dir)
        self.assertEqual(result, expected)
        
        # Test absolute path (should remain unchanged)
        abs_path = os.path.abspath(full_path)
        result = self.get_abs_path(abs_path, dataset_dir)
        self.assertEqual(result, abs_path)
        
        # Test remote path (should remain unchanged)
        remote_path = "http://bucket/file.jpg"
        result = self.get_abs_path(remote_path, dataset_dir)
        self.assertEqual(result, remote_path)

    @TEST_TAG('ray')
    def test_convert_to_absolute_paths(self):
        """Test convert_to_absolute_paths function"""
        import pyarrow as pa
        
        # Create a PyArrow table similar to what would be passed to the function

        sample_data = {
            'images': [['image1.jpg', 'subdir/image2.png'], ['image3.jpg']],
            'videos': [['video1.mp4'], ['subdir/video2.mp4']]
        }

        for key, value_list in sample_data.items():
            for sub_list in value_list:
                for path in sub_list:
                    full_path = os.path.join(self.tmp_dir, path)
                    os.makedirs(os.path.dirname(full_path), exist_ok=True)
                    self._touch_a_file(full_path)

        table = pa.Table.from_pydict(sample_data)
        
        dataset_dir = self.tmp_dir
        path_keys = ['images', 'videos']
        
        result_table = self.convert_to_absolute_paths(table, dataset_dir, path_keys)
        
        result_dict = result_table.to_pydict()

        # Check that images were converted to absolute paths
        self.assertTrue(result_dict['images'][0][0].startswith('/'))
        self.assertTrue(result_dict['images'][0][1].startswith('/'))
        self.assertTrue(result_dict['images'][1][0].startswith('/'))
        
        # Check that videos were converted to absolute paths
        self.assertTrue(result_dict['videos'][0][0].startswith('/'))
        self.assertTrue(result_dict['videos'][1][0].startswith('/'))

    @TEST_TAG('ray')
    def test_convert_to_absolute_paths_preserves_image_bytes(self):
        """Test path conversion keeps bytes/list bytes media fields unchanged."""
        import pyarrow as pa

        table = pa.Table.from_pydict({
            'images': [[b'image-a', b'image-b'], [b'image-c']],
            'videos': [['video.mp4'], [None]],
        })
        video_path = os.path.join(self.tmp_dir, 'video.mp4')
        os.makedirs(os.path.dirname(video_path), exist_ok=True)
        self._touch_a_file(video_path)

        result_table = self.convert_to_absolute_paths(
            table,
            self.tmp_dir,
            ['images', 'videos'],
        )

        result_dict = result_table.to_pydict()
        self.assertEqual(result_dict['images'], [[b'image-a', b'image-b'], [b'image-c']])
        self.assertTrue(result_dict['videos'][0][0].startswith('/'))
        self.assertEqual(result_dict['videos'][1], [None])

    @TEST_TAG('ray')
    def test_convert_to_absolute_paths_skips_missing_path_keys(self):
        """Configured export media columns may not exist in the raw input table."""
        import pyarrow as pa

        table = pa.Table.from_pydict({
            'item_id': [1, 2],
            'item_duration': [30.0, 90.0],
        })

        result_table = self.convert_to_absolute_paths(
            table,
            self.tmp_dir,
            ['images', 'audios'],
        )

        self.assertEqual(result_table.schema.names, ['item_id', 'item_duration'])
        self.assertEqual(result_table.to_pydict(), table.to_pydict())

    @TEST_TAG('ray')
    def test_set_dataset_to_absolute_path_skips_binary_media_columns(self):
        """Binary media columns are already materialized data, not paths."""
        import pyarrow as pa

        class FakeDataset:
            def __init__(self, schema):
                self._schema = schema
                self.map_batches_called = False

            def columns(self):
                return self._schema.names

            def schema(self, *args, **kwargs):
                return self._schema

            def map_batches(self, *args, **kwargs):
                self.map_batches_called = True
                return self

        dataset = FakeDataset(pa.schema([pa.field('images', pa.list_(pa.binary()))]))

        result = self.set_dataset_to_absolute_path(dataset, '/tmp/data.parquet', {'image_key': 'images'})

        self.assertIs(result, dataset)
        self.assertFalse(dataset.map_batches_called)

    @TEST_TAG('ray')
    def test_set_dataset_to_absolute_path_keeps_string_media_columns(self):
        """String media columns still get relative path normalization."""
        import pyarrow as pa

        class FakeDataset:
            def __init__(self, schema):
                self._schema = schema
                self.map_batches_kwargs = None

            def columns(self):
                return self._schema.names

            def schema(self, *args, **kwargs):
                return self._schema

            def map_batches(self, *args, **kwargs):
                self.map_batches_kwargs = kwargs
                return self

        dataset = FakeDataset(pa.schema([pa.field('images', pa.list_(pa.string()))]))

        result = self.set_dataset_to_absolute_path(dataset, '/tmp/data.parquet', {'image_key': 'images'})

        self.assertIs(result, dataset)
        self.assertEqual(dataset.map_batches_kwargs['batch_format'], 'pyarrow')
        self.assertEqual(dataset.map_batches_kwargs['batch_size'], 1000)

    @TEST_TAG('ray')
    def test_set_dataset_to_absolute_path_uses_export_schema_when_ray_schema_unknown(self):
        """Configured binary media schema should skip path conversion even if Ray schema is unavailable."""
        class FakeDataset:
            def __init__(self):
                self.map_batches_called = False

            def columns(self):
                return ['images']

            def schema(self, *args, **kwargs):
                return None

            def map_batches(self, *args, **kwargs):
                self.map_batches_called = True
                return self

        cfg = {
            'image_key': 'images',
            'export': {'schema': {'fields': [{'name': 'images', 'type': 'list<binary>'}]}},
        }
        dataset = FakeDataset()

        result = self.set_dataset_to_absolute_path(dataset, '/tmp/data.parquet', cfg)

        self.assertIs(result, dataset)
        self.assertFalse(dataset.map_batches_called)

    @TEST_TAG('ray')
    def test_set_dataset_to_absolute_path_uses_configured_binary_media_without_fetch(self):
        """Configured binary media schema should avoid eager Ray column/schema fetches."""
        class StrictLazyRayDataset:
            def columns(self, *args, **kwargs):
                raise AssertionError("configured columns should avoid eager column fetch")

            def schema(self, *args, **kwargs):
                raise AssertionError("configured media schema should avoid eager schema fetch")

            def map_batches(self, *args, **kwargs):
                raise AssertionError("binary media column should not be path-normalized")

        cfg = {
            'image_key': 'images',
            'dataset': {'configs': [{'columns': ['images']}]},
            'export': {'schema': {'fields': [{'name': 'images', 'type': 'list<binary>'}]}},
        }
        dataset = StrictLazyRayDataset()

        self.assertIs(self.set_dataset_to_absolute_path(dataset, '/tmp/data.parquet', cfg), dataset)

    @TEST_TAG('ray')
    def test_set_dataset_to_absolute_path_keeps_configured_string_media_when_ray_schema_unknown(self):
        """Configured string media schema should still trigger path conversion."""
        class FakeDataset:
            def __init__(self):
                self.map_batches_kwargs = None

            def columns(self):
                return ['images']

            def schema(self, *args, **kwargs):
                return None

            def map_batches(self, *args, **kwargs):
                self.map_batches_kwargs = kwargs
                return self

        cfg = {
            'image_key': 'images',
            'export': {'schema': {'fields': [{'name': 'images', 'type': 'list<string>'}]}},
        }
        dataset = FakeDataset()

        result = self.set_dataset_to_absolute_path(dataset, '/tmp/data.parquet', cfg)

        self.assertIs(result, dataset)
        self.assertEqual(dataset.map_batches_kwargs['batch_format'], 'pyarrow')
    
    @TEST_TAG('ray')
    def test_get_abs_path_with_nonexistent_local_path(self):
        """Test get_abs_path when local path doesn't exist"""
        # When the joined path doesn't exist, it should return the current path
        dataset_dir = "./nonexistent_dataset"
        path = "existing_file.txt"
        tgt_path = os.path.join(dataset_dir, path)
        non_tgt_path = os.path.abspath(tgt_path)
        result = self.get_abs_path(path, dataset_dir)
        self.assertEqual(result, tgt_path)
        self.assertNotEqual(result, non_tgt_path)


class RayDatasetMapperHookTest(unittest.TestCase):
    def test_mapper_prepare_backend_for_ray_tasks_hook_runs_before_map_batches(self):
        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops.base_op import Mapper

        events = []

        class FakeRayDataset:
            def columns(self):
                return ["text"]

            def map_batches(self, fn, **kwargs):
                events.append(("map_batches", kwargs.get("batch_format")))
                return self

        class BackendPreparingMapper(Mapper):
            _name = "backend_preparing_mapper"

            def prepare_backend_for_ray_tasks(self):
                events.append(("prepare_backend_for_ray_tasks", None))

            def process_single(self, sample):
                return sample

        fake_dataset = FakeRayDataset()
        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.data = fake_dataset

        op = BackendPreparingMapper(auto_op_parallelism=False, num_proc=1)
        ray_dataset._run_single_op(op, cached_columns=set(fake_dataset.columns()))

        self.assertEqual(
            events,
            [
                ("prepare_backend_for_ray_tasks", None),
                ("map_batches", "pyarrow"),
            ],
        )


class TestRayDataset(DataJuicerTestCaseBase):
    def setUp(self):
        """Set up test data"""
        super().setUp()

        import ray
        from data_juicer.core.data.ray_dataset import RayDataset

        self.data = [
            {
                'text': 'Hello',
                'score': 1,
                'metadata': {'lang': 'en'},
                'labels': [1, 2, 3]
            },
            {
                'text': 'World',
                'score': 2,
                'metadata': {'lang': 'es'},
                'labels': [4, 5, 6]
            },
            {
                'text': 'Test',
                'score': 3,
                'metadata': {'lang': 'fr'},
                'labels': [7, 8, 9]
            }
        ]

        # Create fresh dataset for each test
        self.dataset = RayDataset(ray.data.from_items(self.data))

    def tearDown(self):
        """Clean up test data"""
        self.dataset = None
        super().tearDown()

    @TEST_TAG('ray')
    def test_get_column_basic(self):
        """Test basic column retrieval"""
        # Test string column
        texts = self.dataset.get_column('text')
        self.assertEqual(texts, ['Hello', 'World', 'Test'])

        # Test numeric column
        scores = self.dataset.get_column('score')
        self.assertEqual(scores, [1, 2, 3])

        # Test dict column
        metadata = self.dataset.get_column('metadata')
        self.assertEqual(metadata, [
            {'lang': 'en'},
            {'lang': 'es'},
            {'lang': 'fr'}
        ])

        # Test list column
        labels = self.dataset.get_column('labels')
        self.assertEqual(labels, [
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9]
        ])

    @TEST_TAG('ray')
    def test_get_column_with_k(self):
        """Test column retrieval with k limit"""
        # Test k=2
        texts = self.dataset.get_column('text', k=2)
        self.assertEqual(texts, ['Hello', 'World'])

        # Test k larger than dataset
        texts = self.dataset.get_column('text', k=5)
        self.assertEqual(texts, ['Hello', 'World', 'Test'])

        # Test k=0
        texts = self.dataset.get_column('text', k=0)
        self.assertEqual(texts, [])

        # Test k=1
        texts = self.dataset.get_column('text', k=1)
        self.assertEqual(texts, ['Hello'])

    @TEST_TAG('ray')
    def test_get_column_errors(self):
        """Test error handling"""
        # Test non-existent column
        with self.assertRaises(KeyError) as context:
            self.dataset.get_column('nonexistent')
        self.assertIn("not found in dataset", str(context.exception))

        # Test negative k
        with self.assertRaises(ValueError) as context:
            self.dataset.get_column('text', k=-1)
        self.assertIn("must be non-negative", str(context.exception))

    @TEST_TAG('ray')
    def test_get_column_empty_dataset(self):
        """Test with empty dataset"""
        import ray
        from data_juicer.core.data.ray_dataset import RayDataset

        empty_dataset = RayDataset(ray.data.from_items([]))

        # Should raise ValuError for empty dataset/columns
        with self.assertRaises(KeyError):
            empty_dataset.get_column('text')

    @TEST_TAG('ray')
    def test_get_column_types(self):
        """Test return type consistency"""
        # All elements should be strings
        texts = self.dataset.get_column('text')
        self.assertTrue(all(isinstance(x, str) for x in texts))

        # All elements should be ints
        scores = self.dataset.get_column('score')
        self.assertTrue(all(isinstance(x, int) for x in scores))

        # All elements should be dicts
        metadata = self.dataset.get_column('metadata')
        self.assertTrue(all(isinstance(x, dict) for x in metadata))

        # All elements should be lists
        labels = self.dataset.get_column('labels')
        self.assertTrue(all(isinstance(x, list) for x in labels))

    @TEST_TAG('ray')
    def test_get_column_preserve_order(self):
        """Test that column order is preserved"""
        texts = self.dataset.get_column('text')
        self.assertEqual(texts[0], 'Hello')
        self.assertEqual(texts[1], 'World')
        self.assertEqual(texts[2], 'Test')

        # Test with k
        texts = self.dataset.get_column('text', k=2)
        self.assertEqual(texts[0], 'Hello')
        self.assertEqual(texts[1], 'World')

    @TEST_TAG('ray')
    def test_get(self):
        """Test get method for RayDataset"""
        import ray
        from data_juicer.core.data.ray_dataset import RayDataset

        # Test with simple data
        simple_data = [
            {'text': 'hello', 'score': 1},
            {'text': 'world', 'score': 2},
            {'text': 'test', 'score': 3}
        ]
        dataset = RayDataset(ray.data.from_items(simple_data))

        # Basic get
        rows = dataset.get(2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], {'text': 'hello', 'score': 1})
        self.assertEqual(rows[1], {'text': 'world', 'score': 2})

        # Test with nested structures
        nested_data = [
            {
                'text': 'hello',
                'metadata': {'lang': 'en', 'source': 'web'},
                'tags': [1, 2, 3]
            },
            {
                'text': 'world',
                'metadata': {'lang': 'es', 'source': 'book'},
                'tags': [4, 5, 6]
            }
        ]
        nested_dataset = RayDataset(ray.data.from_items(nested_data))

        # Test nested structure preservation
        rows = nested_dataset.get(1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['metadata']['lang'], 'en')
        self.assertEqual(rows[0]['tags'], [1, 2, 3])

        # Test edge cases
        self.assertEqual(dataset.get(0), [])
        self.assertEqual(len(dataset.get(10)), 3)  # More than dataset size
        with self.assertRaises(ValueError):
            dataset.get(-1)

        # Test type preservation
        row = dataset.get(1)[0]
        self.assertIsInstance(row, dict)
        self.assertIsInstance(row['text'], str)
        self.assertIsInstance(row['score'], int)

    def test_mapper_batch_output_preserves_existing_arrow_schema(self):
        import pyarrow as pa

        from data_juicer.core.data.ray_dataset import process_mapper_batch_preserving_schema

        input_batch = pa.table(
            {
                "cost": pa.array([None], type=pa.int64()),
                "site_id": pa.array([123], type=pa.int64()),
            }
        )

        def process_func(batch):
            values = batch.to_pydict()
            values["new_field"] = [None]
            return values

        output = process_mapper_batch_preserving_schema(input_batch, process_func)

        self.assertEqual(output.schema.field("cost").type, pa.int64())
        self.assertEqual(output.schema.field("site_id").type, pa.int64())
        self.assertEqual(output.schema.field("new_field").type, pa.null())

    def test_mapper_batch_output_expands_struct_schema_for_new_stats_keys(self):
        import pyarrow as pa

        from data_juicer.core.data.ray_dataset import process_mapper_batch_preserving_schema
        from data_juicer.ops.filter.character_repetition_filter import CharacterRepetitionFilter
        from data_juicer.utils.constant import Fields, StatsKeys

        input_batch = pa.table(
            {
                Fields.stats: pa.array(
                    [{"ocr_type_en": "simple_extract"}],
                    type=pa.struct([pa.field("ocr_type_en", pa.string())]),
                ),
                "qwen_response_text": pa.array(["hello hello"]),
            }
        )

        op = CharacterRepetitionFilter(text_key="qwen_response_text", rep_len=2)
        output = process_mapper_batch_preserving_schema(input_batch, op.compute_stats)

        self.assertEqual(
            output.schema.field(Fields.stats).type,
            pa.struct(
                [
                    pa.field(StatsKeys.char_rep_ratio, pa.float64()),
                    pa.field("ocr_type_en", pa.string()),
                ]
            ),
        )
        self.assertEqual(
            output.column(Fields.stats).to_pylist(),
            [{StatsKeys.char_rep_ratio: 0.4, "ocr_type_en": "simple_extract"}],
        )

    def test_mapper_batch_output_all_null_struct_key_can_later_infer_concrete_type(self):
        import pyarrow as pa

        from data_juicer.core.data.ray_dataset import process_mapper_batch_preserving_schema
        from data_juicer.utils.constant import Fields

        input_batch = pa.table(
            {
                Fields.stats: pa.array(
                    [{"ocr_type_en": "simple_extract"}],
                    type=pa.struct([pa.field("ocr_type_en", pa.string())]),
                ),
                "text": pa.array(["sample"]),
            }
        )

        def add_null_stat(batch):
            values = batch.to_pydict()
            values[Fields.stats][0]["new_score"] = None
            return values

        def fill_stat(batch):
            values = batch.to_pydict()
            values[Fields.stats][0]["new_score"] = 0.7
            return values

        first = process_mapper_batch_preserving_schema(input_batch, add_null_stat)
        second = process_mapper_batch_preserving_schema(first, fill_stat)
        first_stats_types = {field.name: field.type for field in first.schema.field(Fields.stats).type}
        second_stats_types = {field.name: field.type for field in second.schema.field(Fields.stats).type}

        self.assertEqual(first_stats_types["new_score"], pa.null())
        self.assertEqual(first.column(Fields.stats).to_pylist(), [{"new_score": None, "ocr_type_en": "simple_extract"}])
        self.assertEqual(second_stats_types["new_score"], pa.float64())
        self.assertEqual(second.column(Fields.stats).to_pylist(), [{"new_score": 0.7, "ocr_type_en": "simple_extract"}])

    def test_mapper_batch_output_preserves_multiple_filter_stats_extensions(self):
        import pyarrow as pa

        from data_juicer.core.data.ray_dataset import process_mapper_batch_preserving_schema
        from data_juicer.ops.filter.character_repetition_filter import CharacterRepetitionFilter
        from data_juicer.ops.filter.specified_numeric_field_filter import SpecifiedNumericFieldFilter
        from data_juicer.utils.constant import Fields, StatsKeys

        input_batch = pa.table(
            {
                Fields.stats: pa.array(
                    [{"ocr_type_en": "simple_extract"}],
                    type=pa.struct([pa.field("ocr_type_en", pa.string())]),
                ),
                "qwen_response_text": pa.array(["hello hello"]),
                "valid_image_count": pa.array([3], type=pa.int64()),
            }
        )

        char_filter = CharacterRepetitionFilter(text_key="qwen_response_text", rep_len=2)
        numeric_filter = SpecifiedNumericFieldFilter(field_key="valid_image_count", min_value=1)

        after_char = process_mapper_batch_preserving_schema(input_batch, char_filter.compute_stats)
        after_numeric = process_mapper_batch_preserving_schema(after_char, numeric_filter.compute_stats)
        stats_types = {field.name: field.type for field in after_numeric.schema.field(Fields.stats).type}

        self.assertEqual(stats_types[StatsKeys.char_rep_ratio], pa.float64())
        self.assertEqual(stats_types["valid_image_count"], pa.int64())
        self.assertEqual(
            after_numeric.column(Fields.stats).to_pylist(),
            [{StatsKeys.char_rep_ratio: 0.4, "ocr_type_en": "simple_extract", "valid_image_count": 3}],
        )

    def test_cpu_mapper_uses_operator_name_for_ray_map_batches(self):
        import pyarrow as pa

        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops.base_op import Mapper

        class FakeRayDataset:
            def __init__(self):
                self.fn = None
                self.kwargs = None

            def columns(self):
                return ["text"]

            def map_batches(self, fn, **kwargs):
                self.fn = fn
                self.kwargs = kwargs
                return self

        class NamedTestMapper(Mapper):
            _name = "named_test_mapper"
            _batched_op = True

            def process_batched(self, samples):
                samples = samples.to_pydict() if isinstance(samples, pa.Table) else dict(samples)
                samples["text"] = [text.upper() for text in samples["text"]]
                return samples

        fake_dataset = FakeRayDataset()
        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.data = fake_dataset

        op = NamedTestMapper(auto_op_parallelism=False, num_proc=1)
        ray_dataset._run_single_op(op, cached_columns=set(fake_dataset.columns()))

        self.assertEqual(fake_dataset.fn.__name__, "named_test_mapper")
        self.assertEqual(fake_dataset.fn.__qualname__, "named_test_mapper")
        self.assertNotIn("partial", fake_dataset.fn.__qualname__)
        output = fake_dataset.fn(pa.table({"text": ["hello"]}))
        self.assertEqual(output.column("text").to_pylist(), ["HELLO"])

    def test_mapper_prepare_ray_dataset_hook_runs_before_map_batches(self):
        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops.base_op import Mapper

        events = []

        class FakeRayDataset:
            def columns(self):
                return ["text"]

            def repartition(self, **kwargs):
                events.append(("repartition", kwargs))
                return self

            def map_batches(self, fn, **kwargs):
                events.append(("map_batches", kwargs.get("batch_format")))
                return self

        class RepartitioningMapper(Mapper):
            _name = "repartitioning_mapper"

            def prepare_ray_dataset(self, dataset):
                return dataset.repartition(num_blocks=8, shuffle=False)

            def process_single(self, sample):
                return sample

        fake_dataset = FakeRayDataset()
        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.data = fake_dataset

        op = RepartitioningMapper(auto_op_parallelism=False, num_proc=1)
        ray_dataset._run_single_op(op, cached_columns=set(fake_dataset.columns()))

        self.assertEqual(
            events,
            [
                ("repartition", {"num_blocks": 8, "shuffle": False}),
                ("map_batches", "pyarrow"),
            ],
        )

    def test_filter_compute_stats_preserves_existing_arrow_schema(self):
        import pyarrow as pa

        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops.filter.specified_numeric_field_filter import SpecifiedNumericFieldFilter
        from data_juicer.utils.constant import Fields

        class FakeRayDataset:
            def __init__(self, table):
                self.table = table

            def columns(self):
                return self.table.column_names

            def map_batches(self, fn, **kwargs):
                self.table = fn(self.table)
                return self

            def filter(self, fn, **kwargs):
                return self

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.data = FakeRayDataset(
            pa.table(
                {
                    "is_highlight": pa.array([1], type=pa.int64()),
                    "valid_image_count": pa.array([1], type=pa.int64()),
                }
            )
        )
        op = SpecifiedNumericFieldFilter(
            field_key="valid_image_count",
            min_value=1,
            auto_op_parallelism=False,
            num_proc=1,
        )

        ray_dataset._run_single_op(op, cached_columns=set(ray_dataset.data.columns()))

        self.assertEqual(ray_dataset.data.table.schema.field("is_highlight").type, pa.int64())
        self.assertEqual(ray_dataset.data.table.schema.field("valid_image_count").type, pa.int64())
        self.assertEqual(ray_dataset.data.table.column(Fields.stats).to_pylist(), [{"valid_image_count": 1}])

    def test_process_with_ray_data_checkpoint_avoids_eager_count_and_columns(self):
        from types import SimpleNamespace
        from unittest.mock import patch

        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops.base_op import Mapper

        class StrictLazyRayDataset:
            def __init__(self):
                self.map_batches_calls = 0

            def count(self):
                raise AssertionError("checkpoint mode must not count before export")

            def columns(self, *args, **kwargs):
                raise AssertionError("checkpoint mode must not fetch columns before export")

            def schema(self, *args, **kwargs):
                return None

            def map_batches(self, *args, **kwargs):
                self.map_batches_calls += 1
                return self

        class CheckpointNoEagerTestMapper(Mapper):
            def process_single(self, sample):
                sample["text"] = sample["text"].upper()
                return sample

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.data = StrictLazyRayDataset()
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["text"]}]})
        ray_dataset._auto_proc = False

        op = CheckpointNoEagerTestMapper(auto_op_parallelism=False, num_proc=1)
        with patch("data_juicer.core.data.ray_dataset._is_ray_data_checkpoint_enabled", return_value=True):
            ray_dataset.process([op])

        self.assertEqual(ray_dataset.data.map_batches_calls, 1)

    def test_process_uses_configured_columns_without_eager_count(self):
        from types import SimpleNamespace

        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops.base_op import Mapper

        class StrictLazyRayDataset:
            def __init__(self):
                self.map_batches_calls = 0

            def count(self):
                raise AssertionError("process must not count lazy Ray datasets before operators")

            def columns(self, *args, **kwargs):
                raise AssertionError("configured columns should avoid eager column fetch")

            def schema(self, *args, **kwargs):
                raise AssertionError("configured columns should avoid eager schema fetch")

            def map_batches(self, *args, **kwargs):
                self.map_batches_calls += 1
                return self

        class ConfiguredColumnsTestMapper(Mapper):
            def process_single(self, sample):
                sample["text"] = sample["text"].upper()
                return sample

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.data = StrictLazyRayDataset()
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["text"]}]})
        ray_dataset._auto_proc = False

        op = ConfiguredColumnsTestMapper(auto_op_parallelism=False, num_proc=1)
        ray_dataset.process([op])

        self.assertEqual(ray_dataset.data.map_batches_calls, 1)

    def test_count_uses_cached_row_count_until_processing(self):
        from types import SimpleNamespace

        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops.base_op import Mapper

        class FakeRayDataset:
            def __init__(self):
                self.map_batches_calls = 0
                self.count_calls = 0

            def count(self):
                self.count_calls += 1
                return 3

            def map_batches(self, *args, **kwargs):
                self.map_batches_calls += 1
                return self

        class CountInvalidatingMapper(Mapper):
            def process_single(self, sample):
                sample["text"] = sample["text"].upper()
                return sample

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.data = FakeRayDataset()
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": [{"columns": ["text"]}]})
        ray_dataset._auto_proc = False
        ray_dataset._cached_row_count = 7
        getter_calls = []
        ray_dataset._row_count_getter = lambda: getter_calls.append("called") or 11

        self.assertEqual(ray_dataset.count(), 7)
        self.assertEqual(ray_dataset.data.count_calls, 0)
        self.assertEqual(getter_calls, [])

        ray_dataset._cached_row_count = None
        self.assertEqual(ray_dataset.count(), 11)
        self.assertEqual(ray_dataset.count(), 11)
        self.assertEqual(getter_calls, ["called"])
        self.assertEqual(ray_dataset.data.count_calls, 0)

        op = CountInvalidatingMapper(auto_op_parallelism=False, num_proc=1)
        ray_dataset.process([op])

        self.assertIsNone(ray_dataset._cached_row_count)
        self.assertIsNone(ray_dataset._row_count_getter)
        self.assertEqual(ray_dataset.count(), 3)
        self.assertEqual(ray_dataset.data.count_calls, 1)

    def test_process_plan_only_avoids_eager_count_and_columns(self):
        from types import SimpleNamespace

        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops.base_op import Mapper

        class StrictLazyRayDataset:
            def __init__(self):
                self.map_batches_calls = 0

            def count(self):
                raise AssertionError("plan-only mode must not count")

            def columns(self, *args, **kwargs):
                if kwargs.get("fetch_if_missing") is False:
                    return None
                raise AssertionError("plan-only mode must not eagerly fetch columns")

            def schema(self, *args, **kwargs):
                if kwargs.get("fetch_if_missing") is False:
                    return None
                raise AssertionError("plan-only mode must not eagerly fetch schema")

            def map_batches(self, *args, **kwargs):
                self.map_batches_calls += 1
                return self

        class PlanOnlyTestMapper(Mapper):
            def process_single(self, sample):
                sample["text"] = sample["text"].upper()
                return sample

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.data = StrictLazyRayDataset()
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": []})
        ray_dataset._auto_proc = False

        op = PlanOnlyTestMapper(auto_op_parallelism=False, num_proc=1)
        ray_dataset.process([op], plan_only=True)

        self.assertEqual(ray_dataset.data.map_batches_calls, 1)

    def test_process_plan_only_uses_pipeline_plan_only_hook(self):
        from types import SimpleNamespace

        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.ops.base_op import Pipeline

        class StrictLazyRayDataset:
            pass

        class PlanOnlyPipeline(Pipeline):
            def __init__(self):
                super().__init__()
                self.plan_only_calls = 0

            def run(self, dataset, *, exporter=None, tracer=None):
                raise AssertionError("plan-only mode must not call normal pipeline run")

            def run_plan_only(self, dataset):
                self.plan_only_calls += 1
                return dataset

        ray_dataset = RayDataset.__new__(RayDataset)
        ray_dataset.data = StrictLazyRayDataset()
        ray_dataset.cfg = SimpleNamespace(dataset={"configs": []})
        ray_dataset._auto_proc = False

        op = PlanOnlyPipeline()
        ray_dataset.process([op], plan_only=True)

        self.assertEqual(op.plan_only_calls, 1)

    def test_set_absolute_path_dry_run_avoids_eager_columns(self):
        from types import SimpleNamespace

        from data_juicer.core.data.ray_dataset import set_dataset_to_absolute_path

        class StrictLazyRayDataset:
            def columns(self, *args, **kwargs):
                if kwargs.get("fetch_if_missing") is False:
                    return None
                raise AssertionError("dry-run path preprocessing must not eagerly fetch columns")

            def schema(self, *args, **kwargs):
                if kwargs.get("fetch_if_missing") is False:
                    return None
                raise AssertionError("dry-run path preprocessing must not eagerly fetch schema")

        dataset = StrictLazyRayDataset()
        cfg = SimpleNamespace(ray_dry_run_plan=True, dataset={"configs": []})

        self.assertIs(set_dataset_to_absolute_path(dataset, "/tmp/input/data.jsonl", cfg), dataset)


if __name__ == '__main__':
    unittest.main()
