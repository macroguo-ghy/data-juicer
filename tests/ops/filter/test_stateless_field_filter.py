import unittest

from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops.filter.general_field_filter import GeneralFieldFilter
from data_juicer.ops.filter.stateless_field_filter import StatelessFieldFilter
from data_juicer.utils.constant import Fields, StatsKeys
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase


class StatelessFieldFilterTest(DataJuicerTestCaseBase):
    def test_process_single_evaluates_condition_without_stats(self):
        op = StatelessFieldFilter(filter_condition="valid_video_count > 0")
        keep = {"id": "keep", "valid_video_count": 1}
        drop = {"id": "drop", "valid_video_count": 0}

        self.assertTrue(op.process_single(keep))
        self.assertFalse(op.process_single(drop))
        self.assertEqual(op.compute_stats_single(drop), drop)
        self.assertNotIn(Fields.stats, drop)

    def test_empty_condition_keeps_all(self):
        op = StatelessFieldFilter(filter_condition="")

        self.assertTrue(op.process_single({"id": "keep"}))

    def test_ignores_prior_general_field_filter_stats_key(self):
        dataset = Dataset.from_list(
            [
                {"id": "keep", "item_duration": 10, "valid_video_count": 1},
                {"id": "drop_zero_count", "item_duration": 10, "valid_video_count": 0},
                {"id": "drop_long", "item_duration": 90, "valid_video_count": 1},
            ]
        )

        dataset = GeneralFieldFilter(filter_condition="item_duration <= 60").run(dataset)
        result = StatelessFieldFilter(filter_condition="valid_video_count > 0").run(dataset)

        self.assertEqual(result.select_columns(column_names=["id"]).to_list(), [{"id": "keep"}])

    def test_precomputed_general_field_filter_true_does_not_force_keep(self):
        op = StatelessFieldFilter(filter_condition="valid_video_count > 0")
        sample = {
            "id": "drop",
            "valid_video_count": 0,
            Fields.stats: {StatsKeys.general_field_filter_condition: True},
        }

        self.assertFalse(op.process_single(sample))

    def test_skip_op_error_drops_condition_errors(self):
        sample = {"id": "bad", "valid_video_count": "1"}

        with self.assertRaises(TypeError):
            StatelessFieldFilter(filter_condition="valid_video_count > 0").process(sample)

        op = StatelessFieldFilter(
            filter_condition="valid_video_count > 0",
            skip_op_error=True,
        )

        self.assertFalse(op.process(sample))


if __name__ == "__main__":
    unittest.main()
