import time
import unittest

from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops.mapper.ad_test_processing_timestamp_mapper import (
    AdTestProcessingTimestampMapper,
)
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase


class AdTestProcessingTimestampMapperTest(DataJuicerTestCaseBase):

    def test_adds_default_timestamp_field(self):
        dataset = Dataset.from_list([
            {"text": "first"},
            {"text": "second"},
        ])
        op = AdTestProcessingTimestampMapper()

        before = time.time()
        result = dataset.process([op], open_monitor=False)
        after = time.time()

        result_list = result.to_list()
        self.assertEqual([sample["text"] for sample in result_list], ["first", "second"])
        for sample in result_list:
            self.assertIn("processing_timestamp", sample)
            self.assertGreaterEqual(sample["processing_timestamp"], before)
            self.assertLessEqual(sample["processing_timestamp"], after)

    def test_supports_custom_field_name(self):
        dataset = Dataset.from_list([{"text": "sample"}])
        op = AdTestProcessingTimestampMapper(field_name="processed_at")

        result = dataset.process([op], open_monitor=False).to_list()

        self.assertIn("processed_at", result[0])
        self.assertNotIn("processing_timestamp", result[0])

    def test_rejects_empty_field_name(self):
        with self.assertRaises(ValueError):
            AdTestProcessingTimestampMapper(field_name="")


if __name__ == "__main__":
    unittest.main()
