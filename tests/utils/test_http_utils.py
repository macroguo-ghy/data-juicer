import os
import unittest
from unittest.mock import Mock, patch

import httpx

from data_juicer.utils.http_utils import HttpClient


class HttpClientTest(unittest.TestCase):

    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_get_with_params_returns_json_data(self, mock_request):
        response = Mock(spec=httpx.Response)
        response.status_code = 200
        response.text = '{"ok": true}'
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None
        mock_request.return_value = response

        client = HttpClient(
            endpoint="http://example.test/items",
            method="GET",
            headers={"X-Test": "1"},
            timeout=3.0,
        )
        result = client.request(params={"id": 1})

        mock_request.assert_called_once_with(
            method="GET",
            url="http://example.test/items",
            headers={"X-Test": "1"},
            timeout=3.0,
            params={"id": 1},
            json=None,
        )
        self.assertEqual(result, {
            "ok": True,
            "status_code": 200,
            "data": {"ok": True},
            "text": None,
            "error": None,
        })

    def test_get_dimension_and_metric_sends_real_http_request(self):
        client = HttpClient(
            endpoint="https://bpboost.bytedance.net/api/query-site/openapi/dimension-and-metric",
            method="GET",
            headers={
                "Project-Identifier": "ai_data_center",
            },
        )
        result = client.request(params={"datasourceGroupId": "11308"})

        self.assertEqual(result["ok"], True, result)
        self.assertEqual(result["status_code"], 200)
        self.assertIsInstance(result["data"], dict)

    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_post_with_json_body_returns_json_data(self, mock_request):
        response = Mock(spec=httpx.Response)
        response.status_code = 201
        response.text = '{"answer": "hello"}'
        response.json.return_value = {"answer": "hello"}
        response.raise_for_status.return_value = None
        mock_request.return_value = response

        client = HttpClient(endpoint="http://example.test/invoke", method="POST")
        result = client.request(json_body={"inputs": {"prompt": "hi"}})

        mock_request.assert_called_once_with(
            method="POST",
            url="http://example.test/invoke",
            headers={},
            timeout=30.0,
            params=None,
            json={"inputs": {"prompt": "hi"}},
        )
        self.assertEqual(result["ok"], True)
        self.assertEqual(result["status_code"], 201)
        self.assertEqual(result["data"], {"answer": "hello"})
        self.assertIsNone(result["text"])
        self.assertIsNone(result["error"])

    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_non_json_success_returns_text(self, mock_request):
        response = Mock(spec=httpx.Response)
        response.status_code = 200
        response.text = "plain text"
        response.json.side_effect = ValueError("not json")
        response.raise_for_status.return_value = None
        mock_request.return_value = response

        client = HttpClient(endpoint="http://example.test/plain")
        result = client.request()

        self.assertEqual(result, {
            "ok": True,
            "status_code": 200,
            "data": None,
            "text": "plain text",
            "error": None,
        })

    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_http_status_error_returns_error_result(self, mock_request):
        request = httpx.Request("GET", "http://example.test/error")
        response = httpx.Response(500, request=request, text="server error")
        mock_request.return_value = response

        client = HttpClient(endpoint="http://example.test/error", method="GET")
        result = client.request()

        self.assertEqual(result["ok"], False)
        self.assertEqual(result["status_code"], 500)
        self.assertIsNone(result["data"])
        self.assertEqual(result["text"], "server error")
        self.assertEqual(result["error"]["type"], "HTTPStatusError")

    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_request_error_returns_error_result(self, mock_request):
        mock_request.side_effect = httpx.TimeoutException("timed out")

        client = HttpClient(endpoint="http://example.test/timeout")
        result = client.request()

        self.assertEqual(result["ok"], False)
        self.assertIsNone(result["status_code"])
        self.assertIsNone(result["data"])
        self.assertIsNone(result["text"])
        self.assertEqual(result["error"]["type"], "TimeoutException")

    def test_rejects_unsupported_method(self):
        with self.assertRaises(ValueError):
            HttpClient(endpoint="http://example.test", method="TRACE")

    def test_rejects_empty_endpoint(self):
        with self.assertRaises(ValueError):
            HttpClient(endpoint="")


if __name__ == "__main__":
    unittest.main()
