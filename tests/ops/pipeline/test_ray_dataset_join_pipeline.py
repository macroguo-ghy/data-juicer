import re
import tempfile
import unittest
from types import SimpleNamespace
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

from data_juicer.core.data import NestedDataset
from data_juicer.core.data.config_validator import ConfigValidationError
from data_juicer.core.data import ray_join
from data_juicer.ops.pipeline.ray_dataset_join_pipeline import RayDatasetJoinPipeline
from data_juicer.utils.unittest_utils import TEST_TAG

pa.register_extension_type = _register_extension_type


class FakeRayDataset:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.join_call = None

    def join(self, other, **kwargs):
        self.join_call = (other, kwargs)
        return FakeRayDataset([{"joined": True}])


class RayDatasetJoinPipelineTest(unittest.TestCase):
    def test_constructor_rejects_dataset_block_as_right_config(self):
        with self.assertRaisesRegex(ValueError, "single inline"):
            RayDatasetJoinPipeline(right={"configs": []}, on="id")

    def test_nested_dataset_rejected_for_default_executor_path(self):
        op = RayDatasetJoinPipeline(right={"type": "local", "path": "right.jsonl"}, on="id")

        with self.assertRaisesRegex(ValueError, "only supported by the Ray executor"):
            op.run(NestedDataset.from_list([{"id": 1}]))

    def test_run_requires_ray_runtime_context(self):
        op = RayDatasetJoinPipeline(right={"type": "local", "path": "right.jsonl"}, on="id")

        with self.assertRaisesRegex(ValueError, "runtime context"):
            op.run(FakeRayDataset())

    def test_join_with_same_key_uses_inline_right_loader(self):
        left = FakeRayDataset([{"id": 1, "text": "a"}])
        right = FakeRayDataset([{"id": 1, "label": "x"}])
        op = RayDatasetJoinPipeline(
            right={"type": "local", "path": "right.jsonl"},
            join_type="left_outer",
            on="id",
            num_partitions=8,
            right_suffix="_dim",
        )
        op.set_runtime_context(cfg=SimpleNamespace(work_dir="/tmp"))

        with patch(
            "data_juicer.ops.pipeline.ray_dataset_join_pipeline.load_single_ray_dataset_from_config",
            return_value=SimpleNamespace(data=right),
        ) as load_right:
            output = op.run(left)

        load_right.assert_called_once_with({"type": "local", "path": "right.jsonl"}, op.cfg)
        self.assertEqual(output.rows, [{"joined": True}])
        self.assertIs(left.join_call[0], right)
        self.assertEqual(
            left.join_call[1],
            {
                "join_type": "left_outer",
                "num_partitions": 8,
                "on": ("id",),
                "left_suffix": None,
                "right_suffix": "_dim",
            },
        )

    def test_join_with_different_keys_passes_left_and_right_keys(self):
        left = FakeRayDataset()
        right = FakeRayDataset()
        op = RayDatasetJoinPipeline(
            right={"type": "remote", "source": "magnus", "table_name": "db.table"},
            left_on="item_id",
            right_on="id",
        )
        op.set_runtime_context(cfg=SimpleNamespace(work_dir="/tmp"))

        with patch(
            "data_juicer.ops.pipeline.ray_dataset_join_pipeline.load_single_ray_dataset_from_config",
            return_value=SimpleNamespace(data=right),
        ):
            op.run(left)

        self.assertEqual(left.join_call[1]["on"], ("item_id",))
        self.assertEqual(left.join_call[1]["right_on"], ("id",))

    def test_run_plan_only_does_not_load_right_dataset(self):
        op = RayDatasetJoinPipeline(right={"type": "local", "path": "right.jsonl"}, on="id")
        dataset = FakeRayDataset()

        with patch(
            "data_juicer.ops.pipeline.ray_dataset_join_pipeline.load_single_ray_dataset_from_config"
        ) as load_right:
            output = op.run_plan_only(dataset)

        self.assertIs(output, dataset)
        load_right.assert_not_called()

    def test_join_config_validation_rejects_invalid_shapes(self):
        invalid_configs = [
            {"join_type": "semi", "on": "id"},
            {"join_type": "inner", "num_partitions": 0, "on": "id"},
            {"join_type": "inner", "on": ""},
            {"join_type": "inner", "on": []},
            {"join_type": "inner", "on": ["id", 1]},
            {"join_type": "inner", "on": "id", "left_on": "a", "right_on": "b"},
            {"join_type": "inner", "left_on": "a"},
            {"join_type": "inner", "left_on": ["a"], "right_on": ["b", "c"]},
            {"join_type": "inner", "on": "id", "right_suffix": 1},
        ]

        for config in invalid_configs:
            with self.subTest(config=config):
                with self.assertRaises(ConfigValidationError):
                    ray_join.normalize_ray_join_config(config)

    def test_join_config_accepts_yaml_bool_on_key(self):
        config = ray_join.normalize_ray_join_config({"join_type": "inner", True: "id"})

        self.assertEqual(config.left_on, ("id",))

    def test_ray_join_helpers_detect_configs_and_checkpoint_conflict(self):
        cfg = SimpleNamespace(
            dataset={"join": {"left": "a", "right": "b", "on": "id"}},
            process=[{"noop": {}}, {"ray_dataset_join_pipeline": {"right": {}, "on": "id"}}],
            ray_data_checkpoint=SimpleNamespace(enabled=True),
        )

        self.assertTrue(ray_join.has_dataset_join(cfg))
        self.assertTrue(ray_join.has_pipeline_join(cfg))
        self.assertTrue(ray_join.has_ray_join(cfg))
        self.assertEqual(ray_join.normalize_data_type_and_source({"type": "hdfs"}), ("remote", "hdfs"))
        with self.assertRaisesRegex(ValueError, "ray_data_checkpoint is not supported"):
            ray_join.validate_no_checkpoint_with_ray_join(cfg)

    def test_load_single_ray_dataset_from_config_uses_registry(self):
        class FakeStrategy:
            def __init__(self, ds_config, cfg):
                self.ds_config = ds_config
                self.cfg = cfg

            def load_data(self, **kwargs):
                self.kwargs = kwargs
                return SimpleNamespace(data=FakeRayDataset())

        with patch(
            "data_juicer.core.data.ray_join.DataLoadStrategyRegistry.get_strategy_class",
            return_value=FakeStrategy,
        ):
            with patch("data_juicer.core.data.ray_join.RayDataset", SimpleNamespace):
                dataset = ray_join.load_single_ray_dataset_from_config(
                    {"type": "local", "path": "right.jsonl"},
                    SimpleNamespace(work_dir="/tmp"),
                    num_proc=2,
                )

        self.assertIsInstance(dataset.data, FakeRayDataset)

    def test_load_single_ray_dataset_from_config_rejects_missing_strategy_and_wrong_return_type(self):
        with patch(
            "data_juicer.core.data.ray_join.DataLoadStrategyRegistry.get_strategy_class",
            return_value=None,
        ):
            with self.assertRaisesRegex(ValueError, "No data load strategy"):
                ray_join.load_single_ray_dataset_from_config({"type": "unknown"}, SimpleNamespace())

        class WrongStrategy:
            def __init__(self, ds_config, cfg):
                pass

            def load_data(self, **kwargs):
                return object()

        with patch(
            "data_juicer.core.data.ray_join.DataLoadStrategyRegistry.get_strategy_class",
            return_value=WrongStrategy,
        ):
            with self.assertRaisesRegex(TypeError, "returned object"):
                ray_join.load_single_ray_dataset_from_config({"type": "local"}, SimpleNamespace())

    def test_config_parser_preserves_inline_right_loader_dict(self):
        from data_juicer.config import init_configs

        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = f"{tmp_dir}/ray_join_parser.yaml"
            with open(config_path, "w") as config_file:
                config_file.write(
                """
project_name: ray-join-parser-test
executor_type: ray
dataset:
  configs:
    - type: local
      path: left.jsonl
process:
  - ray_dataset_join_pipeline:
      right:
        type: remote
        source: hdfs
        path: hdfs://localhost:9000/tmp/right
        format: parquet
      join_type: inner
      on: id
export_path: /tmp/ray-join-parser-test.parquet
"""
                )

            cfg = init_configs(args=["--config", config_path])

        op_cfg = cfg.process[0]["ray_dataset_join_pipeline"]
        self.assertEqual(op_cfg["right"]["source"], "hdfs")
        self.assertEqual(op_cfg["on"], "id")

    @TEST_TAG("ray")
    def test_local_ray_inner_and_left_outer_join_semantics(self):
        import ray

        ray.shutdown()
        ray.init(address="local", num_cpus=2, include_dashboard=False, log_to_driver=False)
        try:
            left = ray.data.from_items([
                {"id": 1, "left_value": "a"},
                {"id": 2, "left_value": "b"},
                {"id": 2, "left_value": "b2"},
            ])
            right = ray.data.from_items([
                {"id": 2, "right_value": "x"},
                {"id": 2, "right_value": "y"},
                {"id": 3, "right_value": "z"},
            ])

            inner = left.join(
                right,
                join_type="inner",
                num_partitions=2,
                on=("id",),
                right_suffix="_right",
            )
            left_outer = left.join(
                right,
                join_type="left_outer",
                num_partitions=2,
                on=("id",),
                right_suffix="_right",
            )

            inner_rows = sorted(inner.take_all(), key=lambda row: (row["left_value"], row["right_value"]))
            left_outer_rows = sorted(
                left_outer.take_all(),
                key=lambda row: (row["id"], row["left_value"], str(row.get("right_value"))),
            )

            self.assertEqual(len(inner_rows), 4)
            self.assertEqual(sum(row["id"] == 2 for row in inner_rows), 4)
            self.assertEqual(len(left_outer_rows), 5)
            self.assertIn({"id": 1, "left_value": "a", "right_value": None}, left_outer_rows)
        finally:
            ray.shutdown()

    @TEST_TAG("ray")
    def test_local_ray_key_type_mismatch_failure_is_readable(self):
        import ray

        ray.shutdown()
        ray.init(address="local", num_cpus=1, include_dashboard=False, log_to_driver=False)
        try:
            left = ray.data.from_items([{"id": 1, "left_value": "a"}])
            right = ray.data.from_items([{"id": "1", "right_value": "x"}])
            joined = left.join(right, join_type="inner", num_partitions=1, on=("id",))

            with self.assertRaisesRegex(Exception, re.compile("id|schema|type|join", re.IGNORECASE)):
                joined.take_all()
        finally:
            ray.shutdown()


if __name__ == "__main__":
    unittest.main()
