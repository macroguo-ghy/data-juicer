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

    def test_dimension_and_metric_curl_sends_real_http_request_without_cookie(self):
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

    @patch("data_juicer.utils.http_utils.time.sleep")
    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_retries_retryable_http_status_until_success(self, mock_request, mock_sleep):
        request = httpx.Request("GET", "http://example.test/retry")
        failed_response = httpx.Response(503, request=request, text="temporarily unavailable")
        success_response = Mock(spec=httpx.Response)
        success_response.status_code = 200
        success_response.text = '{"ok": true}'
        success_response.json.return_value = {"ok": True}
        success_response.raise_for_status.return_value = None
        mock_request.side_effect = [failed_response, success_response]

        client = HttpClient(
            endpoint="http://example.test/retry",
            method="GET",
            retry_attempts=1,
            retry_backoff_seconds=0.25,
        )
        result = client.request()

        self.assertEqual(result["ok"], True)
        self.assertEqual(result["data"], {"ok": True})
        self.assertEqual(mock_request.call_count, 2)
        mock_sleep.assert_called_once_with(0.25)

    @patch("data_juicer.utils.http_utils.time.sleep")
    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_does_not_retry_non_retryable_http_status(self, mock_request, mock_sleep):
        request = httpx.Request("GET", "http://example.test/bad-request")
        response = httpx.Response(400, request=request, text="bad request")
        mock_request.return_value = response

        client = HttpClient(
            endpoint="http://example.test/bad-request",
            method="GET",
            retry_attempts=3,
        )
        result = client.request()

        self.assertEqual(result["ok"], False)
        self.assertEqual(result["status_code"], 400)
        self.assertEqual(mock_request.call_count, 1)
        mock_sleep.assert_not_called()

    @patch("data_juicer.utils.http_utils.time.sleep")
    @patch("data_juicer.utils.http_utils.httpx.request")
    def test_retries_timeout_until_success(self, mock_request, mock_sleep):
        success_response = Mock(spec=httpx.Response)
        success_response.status_code = 200
        success_response.text = '{"ok": true}'
        success_response.json.return_value = {"ok": True}
        success_response.raise_for_status.return_value = None
        mock_request.side_effect = [
            httpx.ConnectTimeout("connect timed out"),
            success_response,
        ]

        client = HttpClient(
            endpoint="http://example.test/timeout",
            retry_attempts=1,
            retry_backoff_seconds=0.25,
        )
        result = client.request()

        self.assertEqual(result["ok"], True)
        self.assertEqual(mock_request.call_count, 2)
        mock_sleep.assert_called_once_with(0.25)

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

    def test_rejects_negative_retry_attempts(self):
        with self.assertRaises(ValueError):
            HttpClient(endpoint="http://example.test", retry_attempts=-1)


if __name__ == "__main__":
    unittest.main()
