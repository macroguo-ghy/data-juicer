import unittest
from pathlib import Path
from unittest.mock import ANY, call, patch

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.code_review_mapper import (
    CONFIG_PAGE_KEY,
    NEED_CTX,
    OP_NAME,
    CodeReviewMapper,
)
from data_juicer.utils.operator_execution_callback_utils import RECORD_KEY_FIELD


class CodeReviewMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    def setUp(self):
        self.notification_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.code_review_mapper.send_test_card_notification"
        )
        self.mock_send_test_card_notification = self.notification_patcher.start()
        self.callback_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.code_review_mapper.OperatorExecutionCallbackClient"
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
            "userAccount": "wangjianda.667",
            "x-tt-env": "ppe_sirius2",
            "x-use-ppe": "1",
            "synthesisInstanceId": 10001,
            "flowInstanceId": 20001,
            "flowNodeId": "node_code_review",
            "taskId": 30001,
            "taskVersion": 1,
            "operatorIndex": 1,
            "operatorName": "code_review_mapper",
            "operatorType": "business",
            "apiBase": "https://ai-data-center.bytedance.net/api",
        }

    def test_declares_operator_metadata(self):
        self.assertEqual(OP_NAME, "code_review_mapper")
        self.assertEqual(CONFIG_PAGE_KEY, "code_review_builder")
        self.assertEqual(NEED_CTX, True)

    def test_config_loads_operator_name_without_ad_ai_data_center_prefix(self):
        config_path = Path("/private/tmp/code_review_mapper_config_test.yaml")
        config_path.write_text(
            """
project_name: test_code_review_mapper
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - code_review_mapper:
      ctx:
        userAccount: "wangjianda.667"
        synthesisInstanceId: 10001
        taskId: 30001
        taskVersion: 1
        operatorIndex: 1
        operatorName: "code_review_mapper"
        operatorType: "business"
        apiBase: "https://ai-data-center.bytedance.net/api"
      input_field: "state"
      status_field: "state_review_status"
      reason_field: "state_review_reason"
      python_code: "def review_row(value, row, context):\\n    return True, ''"
""",
            encoding="utf-8",
        )

        cfg = init_configs(
            args=[
                "--config",
                str(config_path),
            ],
            load_configs_only=True,
        )
        ops = load_ops(cfg.process)

        self.assertIsInstance(ops[0], CodeReviewMapper)
        self.assertEqual(ops[0].ctx["userAccount"], "wangjianda.667")
        self.assertEqual(ops[0].input_field, "state")

    def test_tuple_result_writes_status_and_reason_and_reports_success(self):
        op = CodeReviewMapper(
            input_field="state",
            status_field="state_review_status",
            reason_field="state_review_reason",
            python_code=(
                "def review_row(value, row, context):\n"
                "    return value['scene'] == row['state']['scene'], ''\n"
            ),
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )
        dataset = Dataset.from_list([{
            RECORD_KEY_FIELD: "record-1",
            "state": {
                "scene": "feed",
            },
        }])

        result = op.run(dataset).to_list()

        self.assertEqual(result[0]["state_review_status"], True)
        self.assertEqual(result[0]["state_review_reason"], "")
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key="record-1",
            input_data={
                RECORD_KEY_FIELD: "record-1",
                "state": {
                    "scene": "feed",
                },
            },
            output_data=result[0],
            started_at=ANY,
        )

    def test_dict_result_writes_business_failure_without_raising(self):
        op = CodeReviewMapper(
            input_field="state",
            status_field="state_review_status",
            reason_field="state_review_reason",
            python_code=(
                "def review_row(value, row, context):\n"
                "    return {'passed': False, 'reason': '缺少 scene 字段'}\n"
            ),
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "state": {},
        })

        self.assertEqual(result["state_review_status"], False)
        self.assertEqual(result["state_review_reason"], "缺少 scene 字段")
        self.mock_callback.report_record_failure.assert_not_called()

    def test_missing_input_field_reports_record_failure_and_raises(self):
        op = CodeReviewMapper(
            input_field="state",
            python_code="def review_row(value, row, context):\n    return True, ''",
            ctx=self._ctx(),
        )
        sample = {
            RECORD_KEY_FIELD: "record-1",
        }

        with self.assertRaisesRegex(ValueError, "sample.state must be provided"):
            op.process_single(sample)

        self.mock_callback.report_record_failure.assert_called_once_with(
            record_key="record-1",
            input_data={
                RECORD_KEY_FIELD: "record-1",
            },
            output_data=sample,
            error_message="sample.state must be provided",
            started_at=ANY,
        )

    def test_invalid_review_result_reports_record_failure_and_raises(self):
        op = CodeReviewMapper(
            input_field="state",
            python_code="def review_row(value, row, context):\n    return 'bad'",
            ctx=self._ctx(),
        )

        with self.assertRaisesRegex(ValueError, "review result"):
            op.process_single({
                RECORD_KEY_FIELD: "record-1",
                "state": {},
            })

        self.mock_callback.report_record_failure.assert_called_once()

    def test_non_string_reason_reports_record_failure_and_raises(self):
        op = CodeReviewMapper(
            input_field="state",
            python_code="def review_row(value, row, context):\n    return False, 123",
            ctx=self._ctx(),
        )

        with self.assertRaisesRegex(ValueError, "review reason must be a string"):
            op.process_single({
                RECORD_KEY_FIELD: "record-1",
                "state": {},
            })

        self.mock_callback.report_record_failure.assert_called_once()

    def test_script_runtime_exception_reports_record_failure_and_raises(self):
        op = CodeReviewMapper(
            input_field="state",
            python_code="def review_row(value, row, context):\n    raise ValueError('script failed')",
            ctx=self._ctx(),
        )

        with self.assertRaisesRegex(ValueError, "script failed"):
            op.process_single({
                RECORD_KEY_FIELD: "record-1",
                "state": {},
            })

        self.mock_callback.report_record_failure.assert_called_once()

    def test_callback_failure_does_not_block_successful_review(self):
        self.mock_callback.report_record_success.side_effect = RuntimeError("callback down")
        op = CodeReviewMapper(
            input_field="state",
            python_code="def review_row(value, row, context):\n    return True, ''",
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "state": {},
        })

        self.assertEqual(result["review_status"], True)
        self.assertEqual(result["review_reason"], "")

    def test_before_operator_started_starts_running_once(self):
        op = CodeReviewMapper(
            input_field="state",
            python_code="def review_row(value, row, context):\n    return True, ''",
            ctx=self._ctx(),
        )

        op.before_operator_started()
        op.before_operator_started()

        self.mock_callback.start.assert_called_once_with(
            operator_config={
                "input_field": "state",
                "status_field": "review_status",
                "reason_field": "review_reason",
                "entrypoint": "review_row",
            }
        )

    def test_after_operator_finished_finalizes_success_or_failure(self):
        op = CodeReviewMapper(
            input_field="state",
            python_code="def review_row(value, row, context):\n    return True, ''",
            ctx=self._ctx(),
        )

        op.after_operator_finished(error=None)
        op.after_operator_finished(error=RuntimeError("consume failed"))

        self.mock_callback.finalize.assert_called_once_with()
        self.mock_callback.failed.assert_called_once_with(
            error_message="consume failed"
        )

    def test_lifecycle_does_not_send_test_card_notification(self):
        op = CodeReviewMapper(
            input_field="state",
            python_code="def review_row(value, row, context):\n    return True, ''",
            ctx=self._ctx(),
        )

        op.before_operator_started()
        op.after_operator_finished(error=None)

        self.mock_callback.start.assert_called_once()
        self.mock_callback.finalize.assert_called_once_with()
        self.mock_send_test_card_notification.assert_not_called()


if __name__ == "__main__":
    unittest.main()
