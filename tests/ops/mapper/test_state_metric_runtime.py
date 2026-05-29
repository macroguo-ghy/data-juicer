import unittest
from datetime import date

from data_juicer.ops.mapper.ad_ai_data_center.state_metric_runtime import (
    MetricHelpers,
    detect_id_key,
    extract_metric_ids,
)


class StateMetricRuntimeTest(unittest.TestCase):

    def test_extract_metric_ids_deduplicates_numeric_fragments(self):
        self.assertEqual(
            extract_metric_ids("ad:123, adv:456, again:123"),
            ["123", "456"],
        )

    def test_extract_metric_ids_falls_back_to_stripped_original(self):
        self.assertEqual(extract_metric_ids("abc_def"), ["abc_def"])

    def test_detect_id_key_prefers_ad_when_id_matches_both_ad_and_adv(self):
        state = {
            "ad_state": [{"ad_id": "123"}],
            "adv_state": [{"adv_id": "123"}],
        }

        self.assertEqual(detect_id_key(state, "123"), "ad_id")

    def test_detect_id_key_supports_adv_meta_data_fallback(self):
        state = {"adv_state": [{"meta_data": {"adv_id": "456"}}]}

        self.assertEqual(detect_id_key(state, "456"), "adv_id")

    def test_helpers_get_id_key_exposes_id_detection(self):
        helpers = MetricHelpers()
        state = {
            "ad_state": [{"ad_id": "123"}],
            "adv_state": [{"adv_id": "456"}],
        }

        self.assertEqual(helpers.get_id_key(state, "123"), "ad_id")
        self.assertEqual(helpers.get_id_key(state, "456"), "adv_id")
        self.assertIsNone(helpers.get_id_key(state, "789"))

    def test_helpers_safe_divide_and_parse_percent(self):
        helpers = MetricHelpers()

        self.assertEqual(helpers.safe_divide(1, 0), 0.0)
        self.assertEqual(helpers.parse_percent_to_ratio("75%"), 0.75)

    def test_helpers_calc_sequential_stats_integer(self):
        helpers = MetricHelpers()
        series = {
            "2024-01-01": 10,
            "2024-01-02": 20,
            "2024-01-03": 30,
            "2024-01-04": 60,
        }

        self.assertEqual(
            helpers.calc_sequential_stats_integer(
                series,
                date(2024, 1, 3),
                date(2024, 1, 4),
            ),
            (45, 15, 2.0),
        )


if __name__ == "__main__":
    unittest.main()
