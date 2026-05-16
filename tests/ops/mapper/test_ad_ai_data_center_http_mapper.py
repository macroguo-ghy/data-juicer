import json
import os
import unittest
from pathlib import Path
from unittest.mock import call, patch

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.ad_ai_data_center_http_mapper import (
    CONFIG_PAGE_KEY,
    AdAiDataCenterHttpMapper,
    NEED_CTX,
    RECORD_KEY_FIELD,
)


class FakeHttpClient:

    def __init__(self, result):
        self.result = result
        self.requests = []

    def request(self, **kwargs):
        self.requests.append(kwargs)
        return self.result


class AdAiDataCenterHttpMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    def setUp(self):
        self.notification_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.ad_ai_data_center_http_mapper.send_test_card_notification"
        )
        self.mock_send_test_card_notification = self.notification_patcher.start()
        self.mock_send_test_card_notification.return_value = {
            "ok": True,
            "status_code": 200,
            "data": {"code": 0},
            "text": None,
            "error": None,
        }
        self.callback_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.ad_ai_data_center_http_mapper.OperatorExecutionCallbackClient"
        )
        self.mock_callback_cls = self.callback_patcher.start()
        self.mock_callback = self.mock_callback_cls.return_value
        self.mock_callback.upsert.return_value = 10001

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
            "flowNodeId": "node_load_data",
            "taskId": 30001,
            "taskVersion": 1,
            "operatorIndex": 0,
            "operatorName": "ad_ai_data_center_http_mapper",
            "operatorType": "business",
            "openapiBaseUrl": "https://ai-data-center.bytedance.net/api",
        }

    def test_declares_custom_config_page_key(self):
        self.assertEqual(CONFIG_PAGE_KEY, "adAiDataCenterHttp")
        self.assertEqual(NEED_CTX, True)

    @unittest.skipUnless(
        os.getenv("RUN_REAL_AD_AI_DATA_CENTER_HTTP_TEST") == "1",
        "Set RUN_REAL_AD_AI_DATA_CENTER_HTTP_TEST=1 to call the real bpboost API.",
    )
    def test_dimension_and_metric_curl_sends_real_http_request_without_cookie(self):
        op = AdAiDataCenterHttpMapper(
            endpoint=(
                "https://bpboost.bytedance.net/api/query-site/openapi/"
                "dimension-and-metric?datasourceGroupId=11308"
            ),
            method="GET",
            headers={
                "Project-Identifier": "ai_data_center",
            },
            input_fields=["datasourceGroupId"],
            output_field="dimension_and_metric",
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )
        dataset = Dataset.from_list([{
            "datasourceGroupId": "11308",
            RECORD_KEY_FIELD: "record-1",
        }])

        result = op.run(dataset).to_list()

        self.assertIsInstance(result[0]["dimension_and_metric"], str)
        self.assertIsInstance(json.loads(result[0]["dimension_and_metric"]), dict)

    def test_config_process_accepts_endpoint_and_input_fields(self):
        config_path = Path("/private/tmp/ad_ai_data_center_http_mapper_config_test.yaml")
        config_path.write_text(
            """
project_name: test_http_mapper
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - ad_test_processing_timestamp_mapper: {}
  - ad_ai_data_center_http_mapper:
      endpoint: "https://bpboost.bytedance.net/api/query-site/openapi/dimension-and-metric?datasourceGroupId=11308"
      output_field: "http_output"
      error_field: "http_err"
      method: "GET"
      headers:
        Project-Identifier: "ai_data_center"
      ctx:
        userAccount: "tester@example.com"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        synthesisInstanceId: 10001
        flowInstanceId: 20001
        flowNodeId: "node_load_data"
        taskId: 30001
        taskVersion: 1
        operatorIndex: 0
        operatorName: "ad_ai_data_center_http_mapper"
        operatorType: "business"
        openapiBaseUrl: "https://ai-data-center.bytedance.net/api"
      input_fields:
        - "1"
""",
            encoding="utf-8",
        )

        cfg = init_configs(
            args=["--config", str(config_path)],
            load_configs_only=True,
        )
        ops = load_ops(cfg.process)

        self.assertIsInstance(ops[1], AdAiDataCenterHttpMapper)
        self.assertEqual(ops[1].output_field, "http_output")
        self.assertEqual(ops[1].error_field, "http_err")
        self.assertEqual(ops[1].ctx["userAccount"], "tester@example.com")

    @patch("data_juicer.ops.mapper.ad_ai_data_center.ad_ai_data_center_http_mapper.HttpClient")
    def test_writes_http_response_to_output_field(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"answer": "hello"},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        dataset = Dataset.from_list([{
            "prompt": "hi",
            "extra": "keep",
            RECORD_KEY_FIELD: "record-1",
        }])
        op = AdAiDataCenterHttpMapper(
            endpoint="http://example.test/invoke",
            headers={"X-Test": "1"},
            input_fields=["prompt"],
            output_field="http_result",
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )

        result = op.run(dataset).to_list()

        mock_client_cls.assert_called_once_with(
            endpoint="http://example.test/invoke",
            method="POST",
            headers={
                "X-Test": "1",
                "user-account": "tester@example.com",
            },
            timeout=30.0,
        )
        self.assertEqual(
            fake_client.requests,
            [{"json_body": {"inputs": {"prompt": "hi"}}}],
        )
        self.assertEqual(result[0]["prompt"], "hi")
        self.assertEqual(result[0]["extra"], "keep")
        self.assertEqual(json.loads(result[0]["http_result"]), {"answer": "hello"})
        self.assertNotIn("http_error", result[0])
        self.mock_callback.upsert.assert_called_once()
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key="record-1",
            input_data={
                "prompt": "hi",
                "extra": "keep",
                RECORD_KEY_FIELD: "record-1",
            },
            output_data=result[0],
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.ad_ai_data_center_http_mapper.HttpClient")
    def test_callback_failure_does_not_block_http_output(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"answer": "hello"},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        self.mock_callback.report_record_success.side_effect = RuntimeError("callback down")
        op = AdAiDataCenterHttpMapper(
            endpoint="http://example.test/invoke",
            input_fields=["prompt"],
            output_field="http_result",
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )

        result = op.process_single({
            "prompt": "hi",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(json.loads(result["http_result"]), {"answer": "hello"})
        self.assertNotIn("http_error", result)

    @patch("data_juicer.ops.mapper.ad_ai_data_center.ad_ai_data_center_http_mapper.HttpClient")
    def test_notification_failure_does_not_block_http_output_or_report_record_failure(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"answer": "hello"},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        self.mock_send_test_card_notification.side_effect = RuntimeError("notify down")
        op = AdAiDataCenterHttpMapper(
            endpoint="http://example.test/invoke",
            input_fields=["prompt"],
            output_field="http_result",
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )

        result = op.process_single({
            "prompt": "hi",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(json.loads(result["http_result"]), {"answer": "hello"})
        self.assertNotIn("http_error", result)
        self.mock_callback.report_record_success.assert_called_once()
        self.mock_callback.report_record_failure.assert_not_called()

    @patch("data_juicer.ops.mapper.ad_ai_data_center.ad_ai_data_center_http_mapper.HttpClient")
    def test_upsert_failure_does_not_cache_uninitialized_callback_client(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"answer": "hello"},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        self.mock_callback.upsert.side_effect = [RuntimeError("upsert down"), 10001]
        op = AdAiDataCenterHttpMapper(
            endpoint="http://example.test/invoke",
            input_fields=["prompt"],
            output_field="http_result",
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )

        first_result = op.process_single({
            "prompt": "first",
            RECORD_KEY_FIELD: "record-1",
        })
        second_result = op.process_single({
            "prompt": "second",
            RECORD_KEY_FIELD: "record-2",
        })

        self.assertEqual(json.loads(first_result["http_result"]), {"answer": "hello"})
        self.assertEqual(json.loads(second_result["http_result"]), {"answer": "hello"})
        self.assertEqual(self.mock_callback.upsert.call_count, 2)
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key="record-2",
            input_data={
                "prompt": "second",
                RECORD_KEY_FIELD: "record-2",
            },
            output_data=second_result,
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.ad_ai_data_center_http_mapper.HttpClient")
    def test_sends_test_card_notification_before_and_after_processing(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"answer": "hello"},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        op = AdAiDataCenterHttpMapper(
            endpoint="http://example.test/invoke",
            input_fields=["prompt"],
            output_field="http_result",
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )

        result = op.process_single({
            "prompt": "hi",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(
            self.mock_send_test_card_notification.call_args_list,
            [
                call(
                    template_id="AAqt1lQ72dVxK",
                    template_variable={
                        "operator": "ad_ai_data_center_http_mapper",
                        "stage": "开始",
                        "content": '{"prompt": "hi", "__adc_record_key": "record-1"}',
                        "errMsg": "",
                    },
                    ctx=self._ctx(),
                ),
                call(
                    template_id="AAqt1lQ72dVxK",
                    template_variable={
                        "operator": "ad_ai_data_center_http_mapper",
                        "stage": "结束",
                        "content": (
                            '{"prompt": "hi", "__adc_record_key": "record-1", '
                            '"http_result": "{\\"answer\\": \\"hello\\"}"}'
                        ),
                        "errMsg": "",
                    },
                    ctx=self._ctx(),
                ),
            ],
        )
        self.assertEqual(json.loads(result["http_result"]), {"answer": "hello"})

    @patch("data_juicer.ops.mapper.ad_ai_data_center.ad_ai_data_center_http_mapper.HttpClient")
    def test_sends_failed_http_error_message_in_end_notification(self, mock_client_cls):
        error_result = {
            "ok": False,
            "status_code": 500,
            "data": None,
            "text": "server error",
            "error": {"type": "HTTPStatusError", "message": "bad"},
        }
        fake_client = FakeHttpClient(error_result)
        mock_client_cls.return_value = fake_client
        op = AdAiDataCenterHttpMapper(
            endpoint="http://example.test/error",
            input_fields=["prompt"],
            output_field="http_result",
            error_field="http_error",
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )

        op.process_single({
            "prompt": "hi",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(
            self.mock_send_test_card_notification.call_args_list[-1].kwargs["template_variable"]["errMsg"],
            "bad",
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.ad_ai_data_center_http_mapper.HttpClient")
    def test_writes_text_response_when_response_is_not_json(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": None,
            "text": "plain response",
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        op = AdAiDataCenterHttpMapper(
            endpoint="http://example.test/plain",
            input_fields=["query"],
            output_field="http_result",
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )

        result = op.run(Dataset.from_list([{
            "query": "ping",
            RECORD_KEY_FIELD: "record-1",
        }])).to_list()

        self.assertEqual(result[0]["http_result"], "plain response")

    @patch("data_juicer.ops.mapper.ad_ai_data_center.ad_ai_data_center_http_mapper.HttpClient")
    def test_writes_error_field_for_failed_http_request(self, mock_client_cls):
        error_result = {
            "ok": False,
            "status_code": 500,
            "data": None,
            "text": "server error",
            "error": {"type": "HTTPStatusError", "message": "bad"},
        }
        fake_client = FakeHttpClient(error_result)
        mock_client_cls.return_value = fake_client
        op = AdAiDataCenterHttpMapper(
            endpoint="http://example.test/error",
            input_fields=["prompt"],
            output_field="http_result",
            error_field="http_error",
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )

        result = op.run(Dataset.from_list([{
            "prompt": "hi",
            RECORD_KEY_FIELD: "record-1",
        }])).to_list()

        self.assertNotIn("http_result", result[0])
        self.assertEqual(json.loads(result[0]["http_error"]), error_result)
        self.mock_callback.report_record_failure.assert_called_once_with(
            record_key="record-1",
            input_data={
                "prompt": "hi",
                RECORD_KEY_FIELD: "record-1",
            },
            error_message="bad",
            output_data=result[0],
        )

    def test_rejects_empty_input_fields(self):
        with self.assertRaises(ValueError):
            AdAiDataCenterHttpMapper(
                endpoint="http://example.test/invoke",
                input_fields=[],
            )

    def test_rejects_empty_output_field(self):
        with self.assertRaises(ValueError):
            AdAiDataCenterHttpMapper(
                endpoint="http://example.test/invoke",
                input_fields=["prompt"],
                output_field="",
            )


if __name__ == "__main__":
    unittest.main()
