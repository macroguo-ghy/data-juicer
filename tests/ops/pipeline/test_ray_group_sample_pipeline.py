import unittest

import pyarrow as pa

from data_juicer.core.data import NestedDataset
from data_juicer.ops.pipeline.ray_group_sample_pipeline import RayGroupSamplePipeline


class FakeGroupedData:
    def __init__(self, dataset, key):
        self.dataset = dataset
        self.key = key

    def map_groups(self, fn, *, batch_format, fn_kwargs):
        self.dataset.batch_format = batch_format
        groups = {}
        for row in self.dataset.rows:
            groups.setdefault(row[self.key], []).append(row)
        sampled_rows = []
        for rows in groups.values():
            table = pa.Table.from_pylist(rows)
            sampled_rows.extend(fn(table, **fn_kwargs).to_pylist())
        return FakeRayDataset(sampled_rows)


class FakeRayDataset:
    def __init__(self, rows):
        self.rows = list(rows)
        self.batch_format = None

    def groupby(self, key):
        self.group_key = key
        return FakeGroupedData(self, key)


class RayGroupSamplePipelineTest(unittest.TestCase):
    def test_constructor_validates_group_field(self):
        with self.assertRaisesRegex(ValueError, "group_field_key"):
            RayGroupSamplePipeline(group_field_key="")

    def test_ray_dataset_samples_each_group(self):
        dataset = FakeRayDataset(
            [{"ocr_type_en": "a", "id": i} for i in range(5)]
            + [{"ocr_type_en": "b", "id": i} for i in range(10, 15)]
        )
        op = RayGroupSamplePipeline(select_num_per_group=2, seed=123)

        output = op.run(dataset)

        self.assertEqual(dataset.group_key, "ocr_type_en")
        self.assertEqual(dataset.batch_format, "pyarrow")
        self.assertEqual(len(output.rows), 4)
        self.assertEqual(
            {row["ocr_type_en"]: sum(1 for item in output.rows if item["ocr_type_en"] == row["ocr_type_en"]) for row in output.rows},
            {"a": 2, "b": 2},
        )

    def test_nested_dataset_samples_each_group_and_keeps_small_groups(self):
        dataset = NestedDataset.from_list(
            [{"ocr_type_en": "a", "id": i} for i in range(5)]
            + [{"ocr_type_en": "b", "id": i} for i in range(2)]
        )

        rows = RayGroupSamplePipeline(select_num_per_group=3, seed=123).run(dataset).to_list()

        self.assertEqual(sum(row["ocr_type_en"] == "a" for row in rows), 3)
        self.assertEqual(sum(row["ocr_type_en"] == "b" for row in rows), 2)

    def test_arrow_group_sampling_is_reproducible_and_schema_stable(self):
        table = pa.Table.from_pylist(
            [{"ocr_type_en": "a", "id": str(i), "images": [b"x"]} for i in range(5)],
            schema=pa.schema(
                [
                    pa.field("ocr_type_en", pa.string()),
                    pa.field("id", pa.string()),
                    pa.field("images", pa.list_(pa.binary())),
                ]
            ),
        )

        first = RayGroupSamplePipeline._sample_arrow_group(
            table,
            group_field_key="ocr_type_en",
            select_num_per_group=2,
            seed=123,
        )
        second = RayGroupSamplePipeline._sample_arrow_group(
            table,
            group_field_key="ocr_type_en",
            select_num_per_group=2,
            seed=123,
        )
        kept = RayGroupSamplePipeline._sample_arrow_group(
            table,
            group_field_key="ocr_type_en",
            select_num_per_group=10,
            seed=123,
        )

        self.assertEqual(first.to_pylist(), second.to_pylist())
        self.assertEqual(first.num_rows, 2)
        self.assertEqual(first.schema.field("images").type, pa.list_(pa.binary()))
        self.assertIs(kept, table)


if __name__ == "__main__":
    unittest.main()
