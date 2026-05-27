import hashlib
import unittest
from unittest.mock import patch

from data_juicer.core.export_hooks import run_after_export_hook


class FakeHttpClient:
    requests = []

    def __init__(
        self,
        endpoint,
        method="POST",
        headers=None,
        timeout=30.0,
        retry_attempts=0,
        retry_status_codes=None,
        retry_on_timeout=True,
    ):
        self.endpoint = endpoint
        self.method = method
        self.headers = headers or {}
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_status_codes = retry_status_codes
        self.retry_on_timeout = retry_on_timeout

    def request(self, *, params=None, json_body=None):
        self.__class__.requests.append({
            "endpoint": self.endpoint,
            "method": self.method,
            "headers": self.headers,
            "timeout": self.timeout,
            "retry_attempts": self.retry_attempts,
            "retry_status_codes": self.retry_status_codes,
            "retry_on_timeout": self.retry_on_timeout,
            "params": params,
            "json_body": json_body,
        })
        data = {
            "syncBatchId": "batch-1",
            "status": "ACCEPTED",
            "tasks": [
                {"syncTaskId": 10001, "targetType": "sheet", "status": "PENDING"},
                {"syncTaskId": 10002, "targetType": "eval_set", "status": "PENDING"},
            ],
        }
        return {
            "ok": True,
            "status_code": 200,
            "data": {
                "code": 0,
                "data": data,
            },
            "text": None,
            "error": None,
        }


class FailingHttpClient(FakeHttpClient):
    def request(self, *, params=None, json_body=None):
        super().request(params=params, json_body=json_body)
        return {
            "ok": False,
            "status_code": 500,
            "data": None,
            "text": "failed",
            "error": {
                "type": "HTTPStatusError",
                "message": "server failed",
            },
        }


class AfterExportHookTest(unittest.TestCase):
    def setUp(self):
        FakeHttpClient.requests = []
        FailingHttpClient.requests = []

    @staticmethod
    def _export_cfg():
        return {
            "target": "magnus",
            "table_name": "zsy_test.default.wjd_test_output_3",
            "operation": "OVERWRITE",
            "after_export_hook": {
                "enabled": True,
                "type": "adc_result_sync",
                "fail_on_error": False,
                "ctx": {
                    "userAccount": "wangjianda.667",
                    "apiBase": "https://ai-data-center.bytedance.net/api",
                    "spaceId": 1,
                    "synthesisInstanceId": 115,
                    "flowInstanceId": 75,
                    "flowNodeId": "task_1",
                    "x-tt-env": "ppe_sirius3",
                    "x-use-ppe": "1",
                },
                "sync": {
                    "targets": [
                        {
                            "type": "sheet",
                            "enabled": True,
                            "sheetTitle": "数据合成任务结果",
                        },
                        {
                            "type": "eval_set",
                            "enabled": True,
                            "target": {
                                "mode": "CREATE_EVAL_SET",
                                "evalSetName": "数据合成任务结果",
                                "importMode": "OVERWRITE",
                            },
                            "selectedFields": ["query", "state"],
                            "fieldMapping": {
                                "state": "state_json",
                            },
                        },
                    ],
                },
            },
        }

    @patch("data_juicer.core.export_hooks.adc_result_sync_hook.HttpClient", FakeHttpClient)
    def test_adc_result_sync_submits_unified_result_sync_request(self):
        run_after_export_hook(self._export_cfg())

        self.assertEqual(len(FakeHttpClient.requests), 1)
        request = FakeHttpClient.requests[0]

        self.assertEqual(
            request["endpoint"],
            "https://ai-data-center.bytedance.net/api/openapi/synthesis/result-sync/submit",
        )
        self.assertEqual(request["method"], "POST")
        self.assertEqual(request["headers"], {
            "Content-Type": "application/json",
            "User-Account": "wangjianda.667",
            "space-id": "1",
            "x-tt-env": "ppe_sirius3",
            "x-use-ppe": "1",
        })
        source_hash = hashlib.sha256(
            "zsy_test.default.wjd_test_output_3".encode("utf-8")
        ).hexdigest()[:16]
        self.assertEqual(
            request["json_body"]["idempotencyKey"],
            f"synthesis:75:node:task_1:result_sync:lance:{source_hash}",
        )
        self.assertEqual(request["json_body"], {
            "idempotencyKey": f"synthesis:75:node:task_1:result_sync:lance:{source_hash}",
            "source": {
                "sourceType": "lance",
                "datasourceName": "zsy_test.default.wjd_test_output_3",
            },
            "targets": [
                {
                    "targetType": "sheet",
                    "sheetTitle": "数据合成任务结果",
                },
                {
                    "targetType": "eval_set",
                    "selectedFields": ["query", "state"],
                    "target": {
                        "spaceId": 1,
                        "mode": "CREATE_EVAL_SET",
                        "evalSetName": "数据合成任务结果",
                        "importMode": "OVERWRITE",
                    },
                    "fieldMapping": {
                        "state": "state_json",
                    },
                },
            ],
        })

    @patch("data_juicer.core.export_hooks.adc_result_sync_hook.HttpClient", FakeHttpClient)
    def test_disabled_hook_is_skipped(self):
        export_cfg = self._export_cfg()
        export_cfg["after_export_hook"]["enabled"] = False

        run_after_export_hook(export_cfg)

        self.assertEqual(FakeHttpClient.requests, [])

    @patch("data_juicer.core.export_hooks.adc_result_sync_hook.HttpClient", FailingHttpClient)
    def test_fail_on_error_false_does_not_raise(self):
        run_after_export_hook(self._export_cfg())

        self.assertEqual(len(FailingHttpClient.requests), 1)

    @patch("data_juicer.core.export_hooks.adc_result_sync_hook.HttpClient", FailingHttpClient)
    def test_fail_on_error_true_raises(self):
        export_cfg = self._export_cfg()
        export_cfg["after_export_hook"]["fail_on_error"] = True

        with self.assertRaisesRegex(ValueError, "ADC result sync request failed"):
            run_after_export_hook(export_cfg)

    @patch("data_juicer.core.export_hooks.adc_result_sync_hook.HttpClient", FakeHttpClient)
    def test_idempotency_key_falls_back_to_synthesis_instance_id(self):
        export_cfg = self._export_cfg()
        del export_cfg["after_export_hook"]["ctx"]["flowInstanceId"]

        run_after_export_hook(export_cfg)

        source_hash = hashlib.sha256(
            "zsy_test.default.wjd_test_output_3".encode("utf-8")
        ).hexdigest()[:16]
        self.assertEqual(
            FakeHttpClient.requests[0]["json_body"]["idempotencyKey"],
            f"synthesis:115:node:task_1:result_sync:lance:{source_hash}",
        )

    @patch("data_juicer.core.export_hooks.adc_result_sync_hook.HttpClient", FakeHttpClient)
    def test_eval_set_target_space_id_is_preserved_in_target_payload(self):
        export_cfg = self._export_cfg()
        export_cfg["after_export_hook"]["sync"]["targets"] = [{
            "type": "eval_set",
            "enabled": True,
            "target": {
                "spaceId": 9,
                "mode": "CREATE_VERSION",
                "evalSetId": 123,
            },
        }]

        run_after_export_hook(export_cfg)

        request = FakeHttpClient.requests[0]
        self.assertEqual(request["headers"]["space-id"], "1")
        self.assertEqual(request["json_body"]["targets"][0], {
            "targetType": "eval_set",
            "target": {
                "spaceId": 9,
                "mode": "CREATE_VERSION",
                "evalSetId": 123,
            },
        })

    @patch("data_juicer.core.export_hooks.adc_result_sync_hook.HttpClient", FakeHttpClient)
    def test_disabled_sync_targets_are_not_submitted(self):
        export_cfg = self._export_cfg()
        export_cfg["after_export_hook"]["sync"]["targets"][0]["enabled"] = False

        run_after_export_hook(export_cfg)

        self.assertEqual(FakeHttpClient.requests[0]["json_body"]["targets"], [{
            "targetType": "eval_set",
            "selectedFields": ["query", "state"],
            "target": {
                "spaceId": 1,
                "mode": "CREATE_EVAL_SET",
                "evalSetName": "数据合成任务结果",
                "importMode": "OVERWRITE",
            },
            "fieldMapping": {
                "state": "state_json",
            },
        }])

    @patch("data_juicer.core.export_hooks.adc_result_sync_hook.HttpClient", FakeHttpClient)
    def test_adc_result_sync_retries_420_by_default(self):
        export_cfg = self._export_cfg()
        export_cfg["after_export_hook"]["sync"]["targets"] = [{
            "type": "eval_set",
            "enabled": True,
            "target": {
                "mode": "CREATE_VERSION",
                "evalSetId": 123,
            },
        }]

        run_after_export_hook(export_cfg)

        request = FakeHttpClient.requests[0]
        self.assertEqual(request["timeout"], 300.0)
        self.assertEqual(request["retry_attempts"], 5)
        self.assertEqual(request["retry_on_timeout"], False)
        self.assertIn(420, request["retry_status_codes"])


if __name__ == "__main__":
    unittest.main()
