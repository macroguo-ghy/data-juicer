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
        if self.endpoint.endswith("/openapi/dataset/export-to-sheet"):
            data = {
                "sheetUrl": "https://bytedance.feishu.cn/sheets/sht-test",
            }
        elif self.endpoint.endswith("/openapi/eval/evalsets/sync-from-lance"):
            data = {
                "evalSetId": 123,
            }
        else:
            data = {}
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
    def test_adc_result_sync_calls_sheet_and_eval_set_openapis(self):
        run_after_export_hook(self._export_cfg())

        self.assertEqual(len(FakeHttpClient.requests), 3)
        sheet_request = FakeHttpClient.requests[0]
        eval_request = FakeHttpClient.requests[1]
        notification_request = FakeHttpClient.requests[2]

        self.assertEqual(
            sheet_request["endpoint"],
            "https://ai-data-center.bytedance.net/api/openapi/dataset/export-to-sheet",
        )
        self.assertEqual(sheet_request["method"], "POST")
        self.assertEqual(sheet_request["headers"], {
            "Content-Type": "application/json",
            "User-Account": "wangjianda.667",
            "x-tt-env": "ppe_sirius3",
            "x-use-ppe": "1",
        })
        self.assertEqual(sheet_request["json_body"], {
            "datasourceType": "lance",
            "datasourceName": "zsy_test.default.wjd_test_output_3",
            "sheetTitle": "数据合成任务结果",
        })

        self.assertEqual(
            eval_request["endpoint"],
            "https://ai-data-center.bytedance.net/api/openapi/eval/evalsets/sync-from-lance",
        )
        self.assertEqual(eval_request["headers"], {
            "Content-Type": "application/json",
            "User-Account": "wangjianda.667",
            "space-id": "1",
            "x-tt-env": "ppe_sirius3",
            "x-use-ppe": "1",
        })
        self.assertEqual(eval_request["json_body"], {
            "source": {
                "catalog": "zsy_test",
                "namespaceName": "default",
                "tableName": "wjd_test_output_3",
                "selectedFields": ["query", "state"],
            },
            "target": {
                "mode": "CREATE_EVAL_SET",
                "evalSetName": "数据合成任务结果",
                "importMode": "OVERWRITE",
            },
            "fieldMapping": {
                "state": "state_json",
            },
        })
        self.assertEqual(
            notification_request["endpoint"],
            "https://ai-data-center.bytedance.net/api/openapi/lark/message/template-card/send-to-user",
        )
        self.assertEqual(notification_request["headers"], {
            "Content-Type": "application/json",
            "User-Account": "wangjianda.667",
            "x-tt-env": "ppe_sirius3",
            "x-use-ppe": "1",
        })
        self.assertEqual(notification_request["json_body"]["userEmailOrAccount"], "wangjianda.667")
        self.assertEqual(notification_request["json_body"]["templateId"], "AAqtBYKVfi75b")
        self.assertEqual(notification_request["json_body"]["templateVariable"], {
            "title": "数据合成结果同步完成",
            "sheetStatus": "SUCCESS",
            "sheetUrl": "https://bytedance.feishu.cn/sheets/sht-test",
            "evalSetStatus": "SUCCESS",
            "evalSetId": 123,
            "spaceId": 1,
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

        self.assertEqual(len(FailingHttpClient.requests), 3)

    @patch("data_juicer.core.export_hooks.adc_result_sync_hook.HttpClient", FailingHttpClient)
    def test_fail_on_error_true_raises(self):
        export_cfg = self._export_cfg()
        export_cfg["after_export_hook"]["fail_on_error"] = True

        with self.assertRaisesRegex(ValueError, "After export hook target failed"):
            run_after_export_hook(export_cfg)

    @patch("data_juicer.core.export_hooks.adc_result_sync_hook.HttpClient", FakeHttpClient)
    def test_eval_set_target_space_id_overrides_ctx_space_id_header(self):
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

        self.assertEqual(FakeHttpClient.requests[0]["headers"]["space-id"], "9")
        notification_request = FakeHttpClient.requests[1]
        self.assertEqual(notification_request["json_body"]["templateVariable"]["spaceId"], 9)

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

        eval_request = FakeHttpClient.requests[0]
        self.assertEqual(eval_request["timeout"], 300.0)
        self.assertEqual(eval_request["retry_attempts"], 5)
        self.assertEqual(eval_request["retry_on_timeout"], False)
        self.assertIn(420, eval_request["retry_status_codes"])

    @patch("data_juicer.core.export_hooks.adc_result_sync_hook.HttpClient", FailingHttpClient)
    def test_notification_summarizes_target_failures_without_raising_when_fail_on_error_false(self):
        run_after_export_hook(self._export_cfg())

        notification_request = FailingHttpClient.requests[2]
        self.assertEqual(notification_request["json_body"]["templateVariable"], {
            "title": "数据合成结果同步失败",
            "sheetStatus": "FAILED",
            "sheetUrl": "",
            "evalSetStatus": "FAILED",
            "evalSetId": "",
            "spaceId": 1,
        })


if __name__ == "__main__":
    unittest.main()
