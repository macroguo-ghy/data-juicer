import unittest

from data_juicer.ops.filter.numeric_probability_sampling_filter import (
    NumericProbabilitySamplingFilter,
)
from data_juicer.utils.constant import Fields


class NumericProbabilitySamplingFilterTest(unittest.TestCase):
    def test_thresholds_match_ocr_sampling_policy(self):
        op = NumericProbabilitySamplingFilter(
            field_key="text_richness_score",
            base_sample_prob=1.0,
            auto_op_parallelism=False,
            num_proc=1,
        )

        low = op.compute_stats_single({"id": "low", "text_richness_score": 0.2, Fields.stats: {}})
        mid = op.compute_stats_single({"id": "mid", "text_richness_score": 0.3, Fields.stats: {}})
        high = op.compute_stats_single({"id": "high", "text_richness_score": 0.6, Fields.stats: {}})

        self.assertFalse(op.process_single({Fields.stats: low[Fields.stats]}))
        self.assertTrue(op.process_single({Fields.stats: mid[Fields.stats]}))
        self.assertTrue(op.process_single({Fields.stats: high[Fields.stats]}))
        self.assertFalse(op.process_single({Fields.stats: {"text_richness_score": "bad"}}))

    def test_stable_hash_is_reproducible_and_seeded(self):
        op_a = NumericProbabilitySamplingFilter(
            field_key="text_richness_score",
            seed="a",
            auto_op_parallelism=False,
            num_proc=1,
        )
        op_b = NumericProbabilitySamplingFilter(
            field_key="text_richness_score",
            seed="b",
            auto_op_parallelism=False,
            num_proc=1,
        )
        sample = {"id": "same", "text_richness_score": 0.3, Fields.stats: {}}

        first = op_a.compute_stats_single(dict(sample, **{Fields.stats: {}}))[Fields.stats]
        second = op_a.compute_stats_single(dict(sample, **{Fields.stats: {}}))[Fields.stats]
        third = op_b.compute_stats_single(dict(sample, **{Fields.stats: {}}))[Fields.stats]

        self.assertEqual(first["text_richness_score__stable_sample"], second["text_richness_score__stable_sample"])
        self.assertNotEqual(first["text_richness_score__stable_sample"], third["text_richness_score__stable_sample"])

    def test_missing_hash_key_and_missing_score_fall_back_deterministically(self):
        op = NumericProbabilitySamplingFilter(
            field_key="meta.score",
            hash_key=None,
            auto_op_parallelism=False,
            num_proc=1,
        )
        stats = op.compute_stats_single({"id": "x", "meta": {}, Fields.stats: {}})[Fields.stats]

        self.assertIsNone(stats["meta.score"])
        self.assertIn("meta.score__stable_sample", stats)
        self.assertFalse(op.process_single({Fields.stats: stats}))

    def test_constructor_validates_probability_settings(self):
        with self.assertRaisesRegex(ValueError, "base_sample_prob"):
            NumericProbabilitySamplingFilter(base_sample_prob=2.0)
        with self.assertRaisesRegex(ValueError, "low_threshold"):
            NumericProbabilitySamplingFilter(low_threshold=0.6, high_threshold=0.5)


if __name__ == "__main__":
    unittest.main()
