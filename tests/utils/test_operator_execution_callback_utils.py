import unittest
from unittest.mock import patch

from data_juicer.utils.operator_execution_callback_utils import (
    OperatorExecutionCallbackClient,
    OperatorExecutionStatus,
)


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
        }

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_upsert_builds_operator_payload_and_stores_execution_id(self, mock_client_cls):
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

        operator_execution_id = client.upsert(
            operator_config={"sheet_url": "https://xxx"},
            started_at=1710000000000,
            properties={"source": "unit-test"},
        )

        self.assertEqual(operator_execution_id, 10001)
        self.assertEqual(client.operator_execution_id, 10001)
        mock_client_cls.assert_called_once_with(
            endpoint=(
                "https://ai-data-center.bytedance.net/api"
                "/openapi/synthesis/operator-execution/upsert"
            ),
            method="POST",
            headers={
                "Content-Type": "application/json",
                "user-account": "wangjianda.667",
                "x-tt-env": "ppe_sirius2",
                "x-use-ppe": "1",
            },
            timeout=30.0,
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
                    "status": 1,
                    "startedAt": 1710000000000,
                    "properties": {"source": "unit-test"},
                }
            }],
        )

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
                "x-tt-env": "ppe_sirius2",
                "x-use-ppe": "1",
            },
            timeout=30.0,
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
                "errorMessage": "process failed",
            },
        )

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_report_status_failed_uses_status_endpoint(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        client = OperatorExecutionCallbackClient(self._ctx(), operator_execution_id=10001)

        client.report_status(
            OperatorExecutionStatus.FAILED,
            finished_at=1710000010000,
            error_message="operator failed",
        )

        self.assertIn(
            "/openapi/synthesis/operator-execution/status",
            mock_client_cls.call_args.kwargs["endpoint"],
        )
        self.assertEqual(
            fake_client.requests[0]["json_body"],
            {
                "operatorExecutionId": 10001,
                "status": 3,
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

    def test_rejects_missing_ctx_required_by_need_ctx_operator(self):
        with self.assertRaisesRegex(ValueError, "ctx.apiBase must be provided"):
            OperatorExecutionCallbackClient({
                "userAccount": "wangjianda.667",
            })

    @patch("data_juicer.utils.operator_execution_callback_utils.HttpClient")
    def test_accepts_openapi_base_url_alias_for_existing_ctx_contract(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {"success": True}},
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        ctx = self._ctx()
        ctx["openapiBaseUrl"] = ctx.pop("apiBase")
        client = OperatorExecutionCallbackClient(ctx, operator_execution_id=10001)

        client.finalize()

        self.assertEqual(
            mock_client_cls.call_args.kwargs["endpoint"],
            (
                "https://ai-data-center.bytedance.net/api"
                "/openapi/synthesis/operator-execution/finalize"
            ),
        )

    def test_rejects_record_report_before_upsert(self):
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
