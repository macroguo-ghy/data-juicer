import unittest

from data_juicer.utils.adc_record_context import (
    ADC_LOG_ID_FIELD,
    TT_LOG_ID_HEADER,
    add_record_log_id_header,
)


class ADCRecordContextTest(unittest.TestCase):

    def test_adds_record_log_id_header(self):
        headers = {"Content-Type": "application/json"}

        result = add_record_log_id_header(headers, {ADC_LOG_ID_FIELD: 12345})

        self.assertIs(result, headers)
        self.assertEqual(result[TT_LOG_ID_HEADER], "12345")

    def test_ignores_missing_empty_or_non_dict_sample(self):
        self.assertEqual(add_record_log_id_header({}, None), {})
        self.assertEqual(add_record_log_id_header({}, {ADC_LOG_ID_FIELD: ""}), {})
        self.assertEqual(add_record_log_id_header({}, {"text": "hello"}), {})


if __name__ == "__main__":
    unittest.main()
