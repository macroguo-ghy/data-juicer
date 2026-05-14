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
from data_juicer.ops.pipeline.ray_random_sample_pipeline import RayRandomSamplePipeline

pa.register_extension_type = _register_extension_type


class FakeRayDataset:
    def __init__(self, rows):
        self.rows = list(rows)
        self.shuffle_seed = None
        self.limit_value = None

    def random_shuffle(self, *, seed=None):
        self.shuffle_seed = seed
        return self

    def limit(self, limit):
        self.limit_value = limit
        return FakeRayDataset(self.rows[:limit])


class RayRandomSamplePipelineTest(unittest.TestCase):
    def test_ray_dataset_uses_lazy_shuffle_and_limit(self):
        dataset = FakeRayDataset([{"id": i} for i in range(10)])

        output = RayRandomSamplePipeline(select_num=3, seed=123).run(dataset)

        self.assertEqual(dataset.shuffle_seed, 123)
        self.assertEqual(dataset.limit_value, 3)
        self.assertEqual(output.rows, [{"id": 0}, {"id": 1}, {"id": 2}])

    def test_nested_dataset_samples_reproducibly(self):
        dataset = NestedDataset.from_list([{"id": i} for i in range(10)])
        op = RayRandomSamplePipeline(select_num=4, seed=123)

        first = op.run(dataset).to_list()
        second = op.run(dataset).to_list()

        self.assertEqual(first, second)
        self.assertEqual(len(first), 4)

    def test_nested_dataset_keeps_small_dataset(self):
        rows = [{"id": 1}, {"id": 2}]
        output = RayRandomSamplePipeline(select_num=5, seed=123).run(NestedDataset.from_list(rows)).to_list()

        self.assertEqual(output, rows)


if __name__ == "__main__":
    unittest.main()
