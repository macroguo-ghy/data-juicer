import unittest
from pathlib import Path
from unittest.mock import ANY, call, patch

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper import (
    CONFIG_PAGE_KEY,
    NEED_CTX,
    OP_NAME,
    LLMInferenceMapper,
)
from data_juicer.utils.operator_execution_callback_utils import RECORD_KEY_FIELD


class FakeHttpClient:

    def __init__(self, result):
        self.result = result
        self.requests = []

    def request(self, **kwargs):
        self.requests.append(kwargs)
        return self.result


def success_envelope(data):
    return {
        "ok": True,
        "status_code": 200,
        "data": {
            "code": 0,
            "message": "success",
            "data": data,
        },
        "text": None,
        "error": None,
    }


class LLMInferenceMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    def setUp(self):
        self.notification_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.send_test_card_notification"
        )
        self.mock_send_test_card_notification = self.notification_patcher.start()
        self.callback_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.OperatorExecutionCallbackClient"
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
            "flowNodeId": "node_llm",
            "taskId": 30001,
            "taskVersion": 1,
            "operatorIndex": 1,
            "operatorName": "llm_inference_mapper",
            "operatorType": "business",
            "apiBase": "https://ai-data-center.bytedance.net/api",
        }

    @staticmethod
    def _submit_data():
        return {
            "taskId": "task-001",
            "conversationId": "conv-001",
            "requestId": "req-001",
        }

    @staticmethod
    def _success_result_data():
        return {
            "finished": True,
            "success": True,
            "resultStatus": "SUCCESS",
            "status": "success",
            "output": {"summary": "hello summary"},
            "message": None,
            "taskId": "task-001",
            "conversationId": "conv-001",
            "requestId": "req-001",
        }

    def test_declares_operator_metadata(self):
        self.assertEqual(OP_NAME, "llm_inference_mapper")
        self.assertEqual(CONFIG_PAGE_KEY, "llm_state_generator")
        self.assertEqual(NEED_CTX, True)

    def test_config_loads_operator_name_without_ad_ai_data_center_prefix(self):
        config_path = Path("/private/tmp/llm_inference_mapper_config_test.yaml")
        config_path.write_text(
            """
project_name: test_llm_inference_mapper
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - llm_inference_mapper:
      ctx:
        userAccount: "wangjianda.667"
        synthesisInstanceId: 10001
        taskId: 30001
        taskVersion: 1
        operatorIndex: 1
        operatorName: "llm_inference_mapper"
        operatorType: "business"
        apiBase: "https://ai-data-center.bytedance.net/api"
      prompt_template: "请总结：{text}"
      model: "doubao"
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

        self.assertIsInstance(ops[0], LLMInferenceMapper)
        self.assertEqual(ops[0].ctx["userAccount"], "wangjianda.667")
        self.assertIsNone(ops[0].metadata_field)

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_submits_prompt_from_template_polls_result_and_writes_output(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        running_client = FakeHttpClient(success_envelope({
            "finished": False,
            "success": None,
            "resultStatus": "RUNNING",
            "status": "executing",
            "output": None,
            "message": None,
            "taskId": "task-001",
            "conversationId": "conv-001",
            "requestId": "req-001",
        }))
        success_client = FakeHttpClient(success_envelope(self._success_result_data()))
        mock_client_cls.side_effect = [submit_client, running_client, success_client]
        dataset = Dataset.from_list([{
            "text": "long text",
            RECORD_KEY_FIELD: "record-1",
        }])
        op = LLMInferenceMapper(
            prompt_template="请总结：{text}",
            model="doubao",
            ctx=self._ctx(),
            poll_interval_seconds=0,
            auto_op_parallelism=False,
        )

        result = op.run(dataset).to_list()

        expected_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "user-account": "wangjianda.667",
            "x-tt-env": "ppe_sirius2",
            "x-use-ppe": "1",
        }
        self.assertEqual(
            mock_client_cls.call_args_list,
            [
                call(
                    endpoint=(
                        "https://ai-data-center.bytedance.net/api/"
                        "openapi/synthesis/llm-inference/submit"
                    ),
                    method="POST",
                    headers=expected_headers,
                    timeout=30.0,
                ),
                call(
                    endpoint=(
                        "https://ai-data-center.bytedance.net/api/"
                        "openapi/synthesis/llm-inference/result"
                    ),
                    method="POST",
                    headers=expected_headers,
                    timeout=30.0,
                ),
                call(
                    endpoint=(
                        "https://ai-data-center.bytedance.net/api/"
                        "openapi/synthesis/llm-inference/result"
                    ),
                    method="POST",
                    headers=expected_headers,
                    timeout=30.0,
                ),
            ],
        )
        self.assertEqual(submit_client.requests, [{
            "json_body": {
                "prompt": "请总结：long text",
                "model": "doubao",
            }
        }])
        self.assertEqual(running_client.requests, [{"json_body": {"taskId": "task-001"}}])
        self.assertEqual(success_client.requests, [{"json_body": {"taskId": "task-001"}}])
        self.assertEqual(result[0]["llm_output"], '{"summary": "hello summary"}')
        self.assertNotIn("llm_metadata", result[0])
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key="record-1",
            input_data={
                "text": "long text",
                RECORD_KEY_FIELD: "record-1",
            },
            output_data=result[0],
            started_at=ANY,
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_prompt_field_takes_precedence_and_sends_empty_model_by_default(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        success_client = FakeHttpClient(success_envelope(self._success_result_data()))
        mock_client_cls.side_effect = [submit_client, success_client]
        op = LLMInferenceMapper(
            prompt_field="prompt",
            prompt_template="ignored {text}",
            ctx=self._ctx(),
            poll_interval_seconds=0,
        )

        op.process_single({
            "prompt": "direct prompt",
            "text": "text",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(submit_client.requests[0]["json_body"], {
            "prompt": "direct prompt",
            "model": "",
        })

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_string_output_is_preserved_and_metadata_is_stringified(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        success_data = self._success_result_data()
        success_data["output"] = "plain text summary"
        success_client = FakeHttpClient(success_envelope(success_data))
        mock_client_cls.side_effect = [submit_client, success_client]
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            poll_interval_seconds=0,
        )

        result = op.process_single({RECORD_KEY_FIELD: "record-1"})

        self.assertEqual(result["llm_output"], "plain text summary")
        self.assertNotIn("llm_metadata", result)

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_writes_metadata_only_when_metadata_field_is_configured(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        success_data = self._success_result_data()
        success_data["output"] = "plain text summary"
        success_client = FakeHttpClient(success_envelope(success_data))
        mock_client_cls.side_effect = [submit_client, success_client]
        op = LLMInferenceMapper(
            prompt="static prompt",
            metadata_field="llm_metadata",
            ctx=self._ctx(),
            poll_interval_seconds=0,
        )

        result = op.process_single({RECORD_KEY_FIELD: "record-1"})

        self.assertEqual(result["llm_output"], "plain text summary")
        self.assertEqual(
            result["llm_metadata"],
            (
                '{"taskId": "task-001", "conversationId": "conv-001", '
                '"requestId": "req-001", "resultStatus": "SUCCESS", "status": "success"}'
            ),
        )

    def test_rejects_missing_prompt_template_field(self):
        op = LLMInferenceMapper(
            prompt_template="请总结：{missing}",
            ctx=self._ctx(),
        )

        with self.assertRaisesRegex(ValueError, "missing"):
            op.process_single({RECORD_KEY_FIELD: "record-1"})

    def test_prompt_template_renders_dict_and_list_fields_as_json(self):
        op = LLMInferenceMapper(
            prompt_template="请总结对象：{content}；标签：{tags}",
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "content": {
                "city": "北京",
                "weather": "晴朗",
            },
            "tags": ["天气", "户外"],
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(
            prompt,
            '请总结对象：{"city": "北京", "weather": "晴朗"}；标签：["天气", "户外"]',
        )

    def test_prompt_template_resolves_nested_object_and_array_paths(self):
        op = LLMInferenceMapper(
            prompt_template="城市：{a.b.d}；指标：{items[*].metric}；名称：{items[].name}",
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "a": {
                "b": {
                    "d": "北京",
                },
            },
            "items": [
                {
                    "metric": 1,
                    "name": "曝光",
                },
                {
                    "metric": 2,
                    "name": "点击",
                },
            ],
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(prompt, '城市：北京；指标：[1, 2]；名称：["曝光", "点击"]')

    def test_prompt_template_only_serializes_referenced_fields(self):
        op = LLMInferenceMapper(
            prompt_template="请总结：{text}",
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "text": "hello",
            "unused": {
                "raw": b"abc",
            },
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(prompt, "请总结：hello")

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_failed_result_reports_record_failure_and_raises(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        failed_client = FakeHttpClient(success_envelope({
            "finished": True,
            "success": False,
            "resultStatus": "FAILED",
            "status": "fail",
            "output": None,
            "message": "workflow task fail, request_id=req-001",
            "taskId": "task-001",
            "conversationId": "conv-001",
            "requestId": "req-001",
        }))
        mock_client_cls.side_effect = [submit_client, failed_client]
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            poll_interval_seconds=0,
        )
        sample = {
            RECORD_KEY_FIELD: "record-1",
        }

        with self.assertRaisesRegex(ValueError, "workflow task fail"):
            op.process_single(sample)

        self.mock_callback.report_record_failure.assert_called_once_with(
            record_key="record-1",
            input_data={
                RECORD_KEY_FIELD: "record-1",
            },
            output_data=sample,
            error_message="workflow task fail, request_id=req-001",
            started_at=ANY,
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_success_false_takes_precedence_over_result_status_success(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        inconsistent_client = FakeHttpClient(success_envelope({
            "finished": True,
            "success": False,
            "resultStatus": "SUCCESS",
            "status": "fail",
            "output": {"summary": "should not use"},
            "message": "workflow marked failed",
            "taskId": "task-001",
        }))
        mock_client_cls.side_effect = [submit_client, inconsistent_client]
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            poll_interval_seconds=0,
        )

        with self.assertRaisesRegex(ValueError, "workflow marked failed"):
            op.process_single({RECORD_KEY_FIELD: "record-1"})

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_rejects_submit_response_without_task_id(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope({
            "conversationId": "conv-001",
            "requestId": "req-001",
        }))
        mock_client_cls.return_value = submit_client
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            poll_interval_seconds=0,
        )

        with self.assertRaisesRegex(ValueError, "taskId"):
            op.process_single({RECORD_KEY_FIELD: "record-1"})

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_times_out_when_result_never_finishes(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        running_client = FakeHttpClient(success_envelope({
            "finished": False,
            "success": None,
            "resultStatus": "RUNNING",
            "status": "executing",
            "output": None,
            "message": None,
            "taskId": "task-001",
        }))
        mock_client_cls.side_effect = [submit_client, running_client, running_client]
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            poll_interval_seconds=0,
            max_poll_attempts=2,
        )

        with self.assertRaisesRegex(TimeoutError, "task-001"):
            op.process_single({RECORD_KEY_FIELD: "record-1"})

    def test_before_operator_started_sends_start_callback_and_notification(self):
        op = LLMInferenceMapper(
            prompt_template="请总结：{text}",
            model="doubao",
            output_field="out",
            metadata_field="meta",
            poll_interval_seconds=3,
            max_poll_attempts=10,
            ctx=self._ctx(),
        )

        op.before_operator_started()

        self.mock_callback.start.assert_called_once_with(
            operator_config={
                "prompt_source": "prompt_template",
                "model": "doubao",
                "output_field": "out",
                "metadata_field": "meta",
                "poll_interval_seconds": 3,
                "max_poll_attempts": 10,
            }
        )
        self.mock_send_test_card_notification.assert_called_once_with(
            template_id="AAqt1lQ72dVxK",
            template_variable={
                "operator": "llm_inference_mapper",
                "stage": "开始",
                "content": (
                    '{"prompt_source": "prompt_template", "model": "doubao", '
                    '"output_field": "out", "metadata_field": "meta", '
                    '"poll_interval_seconds": 3, "max_poll_attempts": 10}'
                ),
                "errMsg": "",
            },
            ctx=self._ctx(),
        )

    def test_after_operator_finished_finalizes_and_sends_finish_notification(self):
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
        )

        op.after_operator_finished(error=RuntimeError("consume failed"))

        self.mock_callback.failed.assert_called_once_with(error_message="consume failed")
        self.mock_send_test_card_notification.assert_called_once_with(
            template_id="AAqt1lQ72dVxK",
            template_variable={
                "operator": "llm_inference_mapper",
                "stage": "结束",
                "content": '{"status": "FAILED"}',
                "errMsg": "consume failed",
            },
            ctx=self._ctx(),
        )

    def test_notification_failure_does_not_block_lifecycle_callback(self):
        self.mock_send_test_card_notification.side_effect = RuntimeError("notify down")
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
        )

        op.before_operator_started()
        op.after_operator_finished(error=None)

        self.mock_callback.start.assert_called_once()
        self.mock_callback.finalize.assert_called_once_with()

    def test_rejects_invalid_constructor_arguments(self):
        with self.assertRaisesRegex(ValueError, "prompt"):
            LLMInferenceMapper(ctx=self._ctx())
        with self.assertRaisesRegex(ValueError, "output_field"):
            LLMInferenceMapper(prompt="x", output_field="", ctx=self._ctx())
        with self.assertRaisesRegex(ValueError, "metadata_field"):
            LLMInferenceMapper(prompt="x", metadata_field="", ctx=self._ctx())
        with self.assertRaisesRegex(ValueError, "max_poll_attempts"):
            LLMInferenceMapper(prompt="x", max_poll_attempts=0, ctx=self._ctx())


if __name__ == "__main__":
    unittest.main()
