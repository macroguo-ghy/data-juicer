import json
import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from data_juicer.utils.operator_execution_callback_utils import (
    OperatorExecutionCallbackClient,
    OperatorExecutionStatus,
)
from data_juicer.utils.adc_record_context import ADC_LOG_ID_FIELD


class FakeHttpClient:

    def __init__(self, result):
        self.result = result
        self.requests = []

    def request(self, **kwargs):
        self.requests.append(kwargs)
        return self.result


class OperatorExecutionCallbackClientTest(unittest.TestCase):

    @staticmethod
    def _ctx():
        return {
            "userAccount": "wangjianda.667",
            "x-tt-env": "ppe_sirius2",
            "x-use-ppe": "1",
            "apiBase": "https://ai-data-center.bytedance.net/api",
            "synthesisInstanceId": 123,
            "flowInstanceId": 20001,
            "flowNodeId": "node_load_data",
            "taskId": 456,
            "taskVersion": 7,
            "operatorIndex": 0,
            "operatorName": "load_external_dataset",
            "operatorType": "Mapper",
            "spaceId": 1,
        }

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_start_builds_operator_payload_and_stores_execution_id(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {
                "code": 0,
                "data": {
                    "success": True,
                    "operatorExecutionId": 10001,
                },
            },
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx())

        operator_execution_id = client.start(
            operator_config={"sheet_url": "https://xxx"},
            started_at=1710000000000,
            properties={"source": "unit-test"},
        )

        self.assertEqual(operator_execution_id, 10001)
        self.assertEqual(client.operator_execution_id, 10001)
        mock_client_cls.assert_called_once_with(
            endpoint=(
                "https://ai-data-center.bytedance.net/api"
                "/openapi/synthesis/operator-execution/start"
            ),
            method="POST",
            headers={
                "Content-Type": "application/json",
                "user-account": "wangjianda.667",
                "space-id": "1",
                "x-tt-env": "ppe_sirius2",
                "x-use-ppe": "1",
            },
            timeout=30.0,
            retry_attempts=5,
        )
        self.assertEqual(
            fake_client.requests,
            [{
                "json_body": {
                    "synthesisInstanceId": 123,
                    "flowInstanceId": 20001,
                    "flowNodeId": "node_load_data",
                    "taskId": 456,
                    "taskVersion": 7,
                    "operatorIndex": 0,
                    "operatorName": "load_external_dataset",
                    "operatorType": "Mapper",
                    "operatorConfig": {"sheet_url": "https://xxx"},
                    "startedAt": 1710000000000,
                    "properties": {"source": "unit-test"},
                }
            }],
        )

    @patch("data_juicer.utils.operator_execution_callback_utils.time.time")
    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_start_defaults_started_at_to_current_millis(self, mock_client_cls, mock_time):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {
                "code": 0,
                "data": {
                    "success": True,
                    "operatorExecutionId": 10001,
                },
            },
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        mock_time.return_value = 1710000000.123
        client = OperatorExecutionCallbackClient(self._ctx())

        client.start(operator_config={"sheet_url": "https://xxx"})

        self.assertEqual(
            fake_client.requests[0]["json_body"]["startedAt"],
            1710000000123,
        )

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_start_allows_missing_task_id_and_task_version(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True, "operatorExecutionId": 10001}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        ctx = self._ctx()
        ctx.pop("taskId")
        ctx.pop("taskVersion")
        client = OperatorExecutionCallbackClient(ctx)

        client.start(started_at=1710000000000)

        payload = fake_client.requests[0]["json_body"]
        self.assertNotIn("taskId", payload)
        self.assertNotIn("taskVersion", payload)
        self.assertEqual(payload["synthesisInstanceId"], 123)
        self.assertEqual(payload["operatorIndex"], 0)
        self.assertEqual(payload["operatorName"], "load_external_dataset")

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_start_converts_complex_operator_config_to_json_safe_payload(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True, "operatorExecutionId": 10001}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx())

        client.start(
            operator_config={
                "query_date": date(2026, 5, 19),
                "amount": Decimal("12.30"),
            }
        )

        payload = fake_client.requests[0]["json_body"]
        json.dumps(payload, ensure_ascii=False)
        self.assertEqual(payload["operatorConfig"]["query_date"], "2026-05-19")
        self.assertEqual(payload["operatorConfig"]["amount"], "12.30")

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_report_record_success_uses_saved_operator_execution_id(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx())
        client.operator_execution_id = 10001

        client.report_record_success(
            record_key="adc-record-001",
            input_data={"text": "input"},
            output_data={"text": "output"},
            properties={"costMs": 120},
        )

        mock_client_cls.assert_called_once_with(
            endpoint=(
                "https://ai-data-center.bytedance.net/api"
                "/openapi/synthesis/operator-execution/record"
            ),
            method="POST",
            headers={
                "Content-Type": "application/json",
                "user-account": "wangjianda.667",
                "space-id": "1",
                "x-tt-env": "ppe_sirius2",
                "x-use-ppe": "1",
            },
            timeout=30.0,
            retry_attempts=5,
        )
        self.assertEqual(
            fake_client.requests,
            [{
                "json_body": {
                    "operatorExecutionId": 10001,
                    "recordKey": "adc-record-001",
                    "status": 2,
                    "inputData": {"text": "input"},
                    "outputData": {"text": "output"},
                    "properties": {"costMs": 120},
                }
            }],
        )

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_report_record_success_passes_optional_record_log_id_header(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        client.report_record_success(
            record_key="adc-record-001",
            input_data={
                "text": "input",
                ADC_LOG_ID_FIELD: "log-001",
            },
            output_data={"text": "output"},
        )

        mock_client_cls.assert_called_once_with(
            endpoint=(
                "https://ai-data-center.bytedance.net/api"
                "/openapi/synthesis/operator-execution/record"
            ),
            method="POST",
            headers={
                "Content-Type": "application/json",
                "user-account": "wangjianda.667",
                "space-id": "1",
                "x-tt-env": "ppe_sirius2",
                "x-use-ppe": "1",
                "x-tt-logid": "log-001",
            },
            timeout=30.0,
            retry_attempts=5,
        )

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_report_record_success_omits_empty_record_log_id_header(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        client.report_record_success(
            record_key="adc-record-001",
            input_data={
                "text": "input",
                ADC_LOG_ID_FIELD: "",
            },
            output_data={"text": "output"},
        )

        headers = mock_client_cls.call_args.kwargs["headers"]
        self.assertNotIn("x-tt-logid", headers)

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_report_record_success_converts_complex_values_to_json_safe_payload(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        client.report_record_success(
            record_key="adc-record-001",
            input_data={
                "query_date": date(2026, 5, 19),
                "created_at": datetime(2026, 5, 19, 12, 6, 32),
                "amount": Decimal("12.30"),
                "payload": b"hello",
                "tags": {"b", "a"},
                "nested": {
                    (1, 2): "tuple-key",
                },
                "opaque": object(),
            },
            output_data={
                "values": (1, 2),
            },
        )

        payload = fake_client.requests[0]["json_body"]
        json.dumps(payload, ensure_ascii=False)
        self.assertEqual(payload["inputData"]["query_date"], "2026-05-19")
        self.assertEqual(payload["inputData"]["created_at"], "2026-05-19T12:06:32")
        self.assertEqual(payload["inputData"]["amount"], "12.30")
        self.assertEqual(payload["inputData"]["payload"], {"__type__": "bytes", "base64": "aGVsbG8="})
        self.assertEqual(payload["inputData"]["tags"], ["a", "b"])
        self.assertEqual(payload["inputData"]["nested"], {"(1, 2)": "tuple-key"})
        self.assertEqual(payload["inputData"]["opaque"]["__type__"], "object")
        self.assertEqual(payload["outputData"]["values"], [1, 2])

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_report_record_success_summarizes_risky_complex_values(self, mock_client_cls):
        class CustomObject:
            def __init__(self):
                self.secret = "should-not-expand"

        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)
        circular = {}
        circular["self"] = circular

        client.report_record_success(
            record_key="adc-record-001",
            input_data={
                "nan": float("nan"),
                "inf": float("inf"),
                "large_list": list(range(40)),
                "custom": CustomObject(),
                "circular": circular,
            },
        )

        payload = fake_client.requests[0]["json_body"]
        json.dumps(payload, ensure_ascii=False, allow_nan=False)
        self.assertEqual(payload["inputData"]["nan"], {"__type__": "float", "value": "nan"})
        self.assertEqual(payload["inputData"]["inf"], {"__type__": "float", "value": "inf"})
        self.assertEqual(
            payload["inputData"]["large_list"],
            {
                "__type__": "list",
                "length": 40,
                "preview": list(range(10)),
                "truncated": True,
            },
        )
        self.assertNotIn("attrs", payload["inputData"]["custom"])
        self.assertEqual(payload["inputData"]["custom"]["__type__"], "object")
        self.assertEqual(payload["inputData"]["circular"]["self"]["__type__"], "circular_reference")

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_report_record_success_does_not_mark_shared_object_as_circular(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)
        shared = {"value": 1}

        client.report_record_success(
            record_key="adc-record-001",
            input_data={
                "left": shared,
                "right": shared,
            },
        )

        payload = fake_client.requests[0]["json_body"]
        self.assertEqual(payload["inputData"]["left"], {"value": 1})
        self.assertEqual(payload["inputData"]["right"], {"value": 1})

    @patch("data_juicer.utils.operator_execution_callback_utils.time.time")
    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_report_record_success_defaults_finished_at_when_started_at_is_set(self, mock_client_cls, mock_time):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        mock_time.return_value = 1710000001.456
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        client.report_record_success(
            record_key="adc-record-001",
            started_at=1710000000123,
        )

        self.assertEqual(
            fake_client.requests[0]["json_body"]["startedAt"],
            1710000000123,
        )
        self.assertEqual(
            fake_client.requests[0]["json_body"]["finishedAt"],
            1710000001456,
        )

    @patch("data_juicer.utils.operator_execution_callback_utils.time.time")
    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_report_record_success_auto_finished_at_is_after_started_at(self, mock_client_cls, mock_time):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        mock_time.return_value = 1710000000.123
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        client.report_record_success(
            record_key="adc-record-001",
            started_at=1710000000123,
        )

        self.assertEqual(
            fake_client.requests[0]["json_body"]["finishedAt"],
            1710000000124,
        )

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_report_record_failure_does_not_report_operator_failed_status(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        client.report_record_failure(
            record_key="adc-record-001",
            input_data={"text": "input"},
            output_data={"http_error": "process failed"},
            error_message="process failed",
        )

        mock_client_cls.assert_called_once()
        self.assertIn(
            "/openapi/synthesis/operator-execution/record",
            mock_client_cls.call_args.kwargs["endpoint"],
        )
        self.assertEqual(
            fake_client.requests[0]["json_body"],
            {
                "operatorExecutionId": 10001,
                "recordKey": "adc-record-001",
                "status": 3,
                "inputData": {"text": "input"},
                "outputData": {"http_error": "process failed"},
                "errorMessage": "process failed",
            },
        )

    @patch("data_juicer.utils.operator_execution_callback_utils.time.time")
    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_report_record_failure_defaults_finished_at_when_started_at_is_set(self, mock_client_cls, mock_time):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        mock_time.return_value = 1710000002.789
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        client.report_record_failure(
            record_key="adc-record-001",
            error_message="process failed",
            started_at=1710000000123,
        )

        self.assertEqual(
            fake_client.requests[0]["json_body"]["startedAt"],
            1710000000123,
        )
        self.assertEqual(
            fake_client.requests[0]["json_body"]["finishedAt"],
            1710000002789,
        )

    @patch("data_juicer.utils.operator_execution_callback_utils.time.time")
    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_report_record_failure_auto_finished_at_is_after_started_at(self, mock_client_cls, mock_time):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        mock_time.return_value = 1710000000.123
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        client.report_record_failure(
            record_key="adc-record-001",
            error_message="process failed",
            started_at=1710000000123,
        )

        self.assertEqual(
            fake_client.requests[0]["json_body"]["finishedAt"],
            1710000000124,
        )

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_failed_uses_failed_endpoint_for_execution_engine(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        client.failed(
            finished_at=1710000010000,
            error_message="operator failed",
        )

        self.assertIn(
            "/openapi/synthesis/operator-execution/failed",
            mock_client_cls.call_args.kwargs["endpoint"],
        )
        self.assertEqual(
            fake_client.requests[0]["json_body"],
            {
                "operatorExecutionId": 10001,
                "finishedAt": 1710000010000,
                "errorMessage": "operator failed",
            },
        )

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_finalize_uses_finalize_endpoint_for_execution_engine(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        client.finalize(finished_at=1710000020000)

        self.assertIn(
            "/openapi/synthesis/operator-execution/finalize",
            mock_client_cls.call_args.kwargs["endpoint"],
        )
        self.assertEqual(
            fake_client.requests[0]["json_body"],
            {
                "operatorExecutionId": 10001,
                "finishedAt": 1710000020000,
            },
        )

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_missing_ctx_disables_callback_requests(self, mock_client_cls):
        client = OperatorExecutionCallbackClient(None)

        self.assertIsNone(client.start(operator_config={"sheet_url": "https://xxx"}))
        self.assertEqual(
            client.report_record_success(record_key="adc-record-001"),
            {},
        )
        self.assertEqual(
            client.report_record_failure(
                record_key="adc-record-001",
                error_message="failed",
            ),
            {},
        )
        self.assertEqual(client.finalize(), {})
        self.assertEqual(client.failed(error_message="failed"), {})
        mock_client_cls.assert_not_called()

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_incomplete_ctx_disables_callback_requests(self, mock_client_cls):
        client = OperatorExecutionCallbackClient({
            "userAccount": "wangjianda.667",
        })

        self.assertIsNone(client.start(operator_config={"sheet_url": "https://xxx"}))
        self.assertEqual(client.report_record_success(record_key="adc-record-001"), {})
        mock_client_cls.assert_not_called()

    def test_rejects_record_report_before_start(self):
        client = OperatorExecutionCallbackClient(self._ctx())

        with self.assertRaisesRegex(ValueError, "operatorExecutionId must be provided"):
            client.report_record_success(record_key="adc-record-001")

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_rejects_openapi_business_code_failure(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {
                "code": 123,
                "message": "business failed",
                "data": {
                    "success": False,
                },
            },
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        with self.assertRaisesRegex(
            ValueError,
            "Operator execution callback business failed: code=123, message=business failed",
        ):
            client.finalize()

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_rejects_openapi_success_false(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {
                "code": 0,
                "data": {
                    "success": False,
                },
            },
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        with self.assertRaisesRegex(
            ValueError,
            "Operator execution callback business failed: success=false",
        ):
            client.finalize()


if __name__ == "__main__":
    unittest.main()
