import unittest

import pyarrow as pa

from data_juicer.ops.filter.specified_field_non_empty_filter import SpecifiedFieldNonEmptyFilter
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase


class SpecifiedFieldNonEmptyFilterTest(DataJuicerTestCaseBase):
    def test_top_level_empty_semantics(self):
        op = SpecifiedFieldNonEmptyFilter(field_key="ocr_result")
        samples = {
            "id": [
                "none",
                "empty_str",
                "spaces",
                "empty_bytes",
                "empty_list",
                "only_empty_list",
                "empty_dict",
                "nonempty_list",
                "zero",
                "false",
                "nonempty_dict",
            ],
            "ocr_result": [
                None,
                "",
                "   ",
                b"",
                [],
                [None, ""],
                {"text": "", "confidence": None},
                ["ocr-json"],
                0,
                False,
                {"text": "ocr-json"},
            ],
        }

        self.assertEqual(
            op.process_batched(samples),
            [False, False, False, False, False, False, False, True, True, True, True],
        )

    def test_nested_field_path(self):
        op = SpecifiedFieldNonEmptyFilter(field_key="meta.ocr.text")
        samples = {
            "id": ["empty", "nonempty"],
            "meta": [
                {"ocr": {"text": [None, ""]}},
                {"ocr": {"text": ["ocr-json"]}},
            ],
        }

        self.assertEqual(op.process_batched(samples), [False, True])

    def test_reversed_range_keeps_empty_values(self):
        op = SpecifiedFieldNonEmptyFilter(field_key="ocr_result", reversed_range=True)
        samples = {
            "id": ["empty", "nonempty"],
            "ocr_result": [[], ["ocr-json"]],
        }

        self.assertEqual(op.process_batched(samples), [True, False])

    def test_missing_field_raises(self):
        op = SpecifiedFieldNonEmptyFilter(field_key="meta.ocr_result")

        with self.assertRaisesRegex(KeyError, "meta"):
            op.process_batched({"id": ["1"]})
        with self.assertRaisesRegex(KeyError, "meta.ocr_result"):
            op.process_batched({"id": ["1"], "meta": [{}]})

    def test_arrow_batch_path(self):
        op = SpecifiedFieldNonEmptyFilter(field_key="ocr_result")
        table = pa.Table.from_pylist(
            [
                {"id": "empty", "ocr_result": []},
                {"id": "nonempty", "ocr_result": ["ocr-json"]},
            ],
            schema=pa.schema(
                [
                    pa.field("id", pa.string()),
                    pa.field("ocr_result", pa.list_(pa.string())),
                ]
            ),
        )

        self.assertEqual(op.process(table), [False, True])

    def test_run_filters_dataset_without_stats_dependency(self):
        dataset = self.generate_dataset(
            [
                {"id": "empty", "ocr_result": []},
                {"id": "nonempty", "ocr_result": ["ocr-json"]},
                {"id": "none", "ocr_result": None},
            ]
        )
        op = SpecifiedFieldNonEmptyFilter(field_key="ocr_result", batch_size=2, num_proc=1)

        result = self.run_single_op(dataset, op, ["id", "ocr_result"])

        self.assertEqual(result, [{"id": "nonempty", "ocr_result": ["ocr-json"]}])

    def test_empty_field_key_is_invalid(self):
        with self.assertRaisesRegex(ValueError, "field_key must be provided"):
            SpecifiedFieldNonEmptyFilter()


if __name__ == "__main__":
    unittest.main()
