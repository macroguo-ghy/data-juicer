import time
import unittest
from unittest.mock import ANY, call, patch

from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops.mapper.ad_ai_data_center.ad_test_processing_timestamp_mapper import (
    AdTestProcessingTimestampMapper,
    NEED_CTX,
    RECORD_KEY_FIELD,
)
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase


class AdTestProcessingTimestampMapperTest(DataJuicerTestCaseBase):

    def setUp(self):
        self.notification_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.ad_test_processing_timestamp_mapper.send_test_card_notification"
        )
        self.mock_send_test_card_notification = self.notification_patcher.start()
        self.callback_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.ad_test_processing_timestamp_mapper.OperatorExecutionCallbackClient"
        )
        self.mock_callback_cls = self.callback_patcher.start()
        self.mock_callback = self.mock_callback_cls.return_value
        self.mock_callback.start.return_value = 10001

    def tearDown(self):
        self.callback_patcher.stop()
        self.notification_patcher.stop()

    @staticmethod
    def _ctx():
        return {
            "userAccount": "tester@example.com",
            "x-tt-env": "ppe_sirius2",
            "x-use-ppe": "1",
            "synthesisInstanceId": 10001,
            "flowInstanceId": 20001,
            "flowNodeId": "node_timestamp",
            "taskId": 30001,
            "taskVersion": 1,
            "operatorIndex": 0,
            "operatorName": "ad_test_processing_timestamp_mapper",
            "operatorType": "business",
            "apiBase": "https://ai-data-center.bytedance.net/api",
        }

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

    def test_declares_need_ctx(self):
        self.assertEqual(NEED_CTX, True)

    def test_reports_callback_and_sends_notifications_for_each_sample(self):
        dataset = Dataset.from_list([
            {"text": "first", RECORD_KEY_FIELD: "record-1"},
            {"text": "second", RECORD_KEY_FIELD: "record-2"},
        ])
        op = AdTestProcessingTimestampMapper(ctx=self._ctx(), auto_op_parallelism=False)

        result = dataset.process([op], open_monitor=False).to_list()

        self.mock_callback.start.assert_called_once_with(
            operator_config={"field_name": "processing_timestamp"}
        )
        self.assertEqual(self.mock_callback.report_record_success.call_count, 2)
        self.mock_callback.report_record_success.assert_any_call(
            record_key="record-1",
            input_data={"text": "first", RECORD_KEY_FIELD: "record-1"},
            output_data=result[0],
            started_at=ANY,
        )
        self.mock_callback.report_record_success.assert_any_call(
            record_key="record-2",
            input_data={"text": "second", RECORD_KEY_FIELD: "record-2"},
            output_data=result[1],
            started_at=ANY,
        )
        self.assertEqual(self.mock_send_test_card_notification.call_count, 4)
        self.mock_send_test_card_notification.assert_has_calls([
            call(
                template_id="AAqt1lQ72dVxK",
                template_variable={
                    "operator": "ad_test_processing_timestamp_mapper",
                    "stage": "开始",
                    "content": '{"text": "first", "__adc_record_key": "record-1"}',
                    "errMsg": "",
                },
                ctx=self._ctx(),
            ),
            call(
                template_id="AAqt1lQ72dVxK",
                template_variable={
                    "operator": "ad_test_processing_timestamp_mapper",
                    "stage": "开始",
                    "content": '{"text": "second", "__adc_record_key": "record-2"}',
                    "errMsg": "",
                },
                ctx=self._ctx(),
            ),
        ], any_order=True)

    def test_observation_failure_does_not_block_timestamp(self):
        self.mock_send_test_card_notification.side_effect = RuntimeError("notify down")
        self.mock_callback.report_record_success.side_effect = RuntimeError("callback down")
        dataset = Dataset.from_list([{"text": "sample", RECORD_KEY_FIELD: "record-1"}])
        op = AdTestProcessingTimestampMapper(ctx=self._ctx(), auto_op_parallelism=False)

        result = dataset.process([op], open_monitor=False).to_list()

        self.assertIn("processing_timestamp", result[0])

    def test_before_operator_started_starts_running_once(self):
        op = AdTestProcessingTimestampMapper(ctx=self._ctx(), auto_op_parallelism=False)

        op.before_operator_started()
        op.before_operator_started()

        self.mock_callback.start.assert_called_once_with(
            operator_config={"field_name": "processing_timestamp"}
        )

    def test_after_operator_finished_finalizes_success_or_failure(self):
        op = AdTestProcessingTimestampMapper(ctx=self._ctx(), auto_op_parallelism=False)

        op.after_operator_finished(error=None)
        op.after_operator_finished(error=RuntimeError("consume failed"))

        self.mock_callback.finalize.assert_called_once_with()
        self.mock_callback.failed.assert_called_once_with(
            error_message="consume failed"
        )

    def test_start_failure_does_not_cache_uninitialized_callback_client(self):
        self.mock_callback.start.side_effect = [RuntimeError("start down"), 10001]
        op = AdTestProcessingTimestampMapper(ctx=self._ctx(), auto_op_parallelism=False)

        first_result = op.process_single({
            "text": "first",
            RECORD_KEY_FIELD: "record-1",
        })
        second_result = op.process_single({
            "text": "second",
            RECORD_KEY_FIELD: "record-2",
        })

        self.assertIn("processing_timestamp", first_result)
        self.assertIn("processing_timestamp", second_result)
        self.assertEqual(self.mock_callback.start.call_count, 2)
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key="record-2",
            input_data={"text": "second", RECORD_KEY_FIELD: "record-2"},
            output_data={
                "text": "second",
                RECORD_KEY_FIELD: "record-2",
                "processing_timestamp": second_result["processing_timestamp"],
            },
            started_at=ANY,
        )


if __name__ == "__main__":
    unittest.main()
