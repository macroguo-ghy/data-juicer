import json
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

    def tearDown(self):
        self.notification_patcher.stop()

    def test_declares_custom_config_page_key(self):
        self.assertEqual(CONFIG_PAGE_KEY, "adAiDataCenterHttp")

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
            auto_op_parallelism=False,
        )
        dataset = Dataset.from_list([{"datasourceGroupId": "11308"}])

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
        dataset = Dataset.from_list([{"prompt": "hi", "extra": "keep"}])
        op = AdAiDataCenterHttpMapper(
            endpoint="http://example.test/invoke",
            headers={"X-Test": "1"},
            input_fields=["prompt"],
            output_field="http_result",
            auto_op_parallelism=False,
        )

        result = op.run(dataset).to_list()

        mock_client_cls.assert_called_once_with(
            endpoint="http://example.test/invoke",
            method="POST",
            headers={"X-Test": "1"},
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
            auto_op_parallelism=False,
        )

        result = op.process_single({"prompt": "hi"})

        self.assertEqual(
            self.mock_send_test_card_notification.call_args_list,
            [
                call(
                    template_id="AAqt1lQ72dVxK",
                    template_variable={"input": {"prompt": "hi"}},
                    user_email_or_account="wangjianda.667@bytedance.com",
                ),
                call(
                    template_id="AAqt1lQ72dVxK",
                    template_variable={
                        "input": {"prompt": "hi", "http_result": '{"answer": "hello"}'}
                    },
                    user_email_or_account="wangjianda.667@bytedance.com",
                ),
            ],
        )
        self.assertEqual(json.loads(result["http_result"]), {"answer": "hello"})

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
            auto_op_parallelism=False,
        )

        result = op.run(Dataset.from_list([{"query": "ping"}])).to_list()

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
            auto_op_parallelism=False,
        )

        result = op.run(Dataset.from_list([{"prompt": "hi"}])).to_list()

        self.assertNotIn("http_result", result[0])
        self.assertEqual(json.loads(result[0]["http_error"]), error_result)

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
