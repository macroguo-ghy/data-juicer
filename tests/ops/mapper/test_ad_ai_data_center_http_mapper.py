import unittest
from unittest.mock import patch

from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.mapper import AdAiDataCenterHttpMapper


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
        self.assertEqual(result[0]["http_result"], {"answer": "hello"})
        self.assertNotIn("http_error", result[0])

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
        self.assertEqual(result[0]["http_error"], error_result)

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
