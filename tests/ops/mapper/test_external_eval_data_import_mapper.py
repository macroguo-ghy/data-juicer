import os
import unittest
from pathlib import Path
from unittest.mock import patch

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.external_eval_data_import_mapper import (
    CLOUD_DOC_ALL_PLAIN_VALUES_PATH,
    NEED_CTX,
    OP_NAME,
    ExternalEvalDataImportMapper,
)


class FakeHttpClient:

    def __init__(self, result):
        self.result = result
        self.requests = []

    def request(self, **kwargs):
        self.requests.append(kwargs)
        return self.result


class ExternalEvalDataImportMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    def setUp(self):
        self.callback_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.external_eval_data_import_mapper.OperatorExecutionCallbackClient"
        )
        self.mock_callback_cls = self.callback_patcher.start()
        self.mock_callback = self.mock_callback_cls.return_value
        self.mock_callback.upsert.return_value = 10001

    def tearDown(self):
        self.callback_patcher.stop()

    @staticmethod
    def _ctx():
        return {
            "userAccount": "wangjianda.667",
            "x-tt-env": "ppe_sirius2",
            "x-use-ppe": "1",
            "synthesisInstanceId": 10001,
            "flowInstanceId": 20001,
            "flowNodeId": "node_load_data",
            "taskId": 30001,
            "taskVersion": 1,
            "operatorIndex": 0,
            "operatorName": "external_eval_data_import_mapper",
            "operatorType": "business",
            "apiBase": "https://ai-data-center.bytedance.net/api",
        }

    def test_before_operator_started_upserts_running_once(self):
        op = ExternalEvalDataImportMapper(
            sheet_url="https://bytedance.feishu.cn/sheets/xxxx",
            data_type="eval_data",
            ctx=self._ctx(),
            python_code="def process(data, context):\n    return data",
            auto_op_parallelism=False,
        )

        op.before_operator_started()
        op.before_operator_started()

        self.mock_callback.upsert.assert_called_once_with(
            operator_config={
                "sheet_url": "https://bytedance.feishu.cn/sheets/xxxx",
                "data_type": "eval_data",
                "output_field": "externalDataSet",
            }
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.external_eval_data_import_mapper.HttpClient")
    def test_imports_eval_data_sheet_and_runs_python_process(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {
                "code": 0,
                "data": {
                    "sheets": [
                        {
                            "sheetId": "sheet_1",
                            "title": "Sheet1",
                            "values": [
                                ["query", "answer"],
                                ["example question", "example answer"],
                            ],
                        }
                    ]
                },
            },
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        op = ExternalEvalDataImportMapper(
            sheet_url="https://bytedance.feishu.cn/sheets/xxxx",
            data_type="eval_data",
            ctx=self._ctx(),
            python_code=(
                "def process(data, context):\n"
                "    return {\"items\": data, \"data_type\": context[\"data_type\"], "
                "\"sheet_count\": len(context[\"raw_sheets\"])}"
            ),
            auto_op_parallelism=False,
        )
        dataset = Dataset.from_list([{
            "id": "row-1",
        }])

        result = op.run(dataset).to_list()

        mock_client_cls.assert_called_once_with(
            endpoint=(
                "https://ai-data-center.bytedance.net/api"
                f"{CLOUD_DOC_ALL_PLAIN_VALUES_PATH}"
            ),
            method="POST",
            timeout=30.0,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "user-account": "wangjianda.667",
            },
        )
        self.assertEqual(
            fake_client.requests,
            [{"json_body": {"docUrl": "https://bytedance.feishu.cn/sheets/xxxx"}}],
        )
        self.assertEqual(result[0]["id"], "row-1")
        self.assertEqual(
            result[0]["externalDataSet"],
            {
                "items": [
                    {
                        "query": "example question",
                        "answer": "example answer",
                    }
                ],
                "data_type": "eval_data",
                "sheet_count": 1,
            },
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.external_eval_data_import_mapper.HttpClient")
    def test_rejects_missing_user_account_in_ctx_before_loading_sheet(self, mock_client_cls):
        op = ExternalEvalDataImportMapper(
            sheet_url="https://bytedance.feishu.cn/sheets/xxxx",
            data_type="eval_data",
            ctx={},
            python_code="def process(data, context):\n    return {\"items\": data}",
            auto_op_parallelism=False,
        )

        with self.assertRaisesRegex(ValueError, "ctx.userAccount must be provided"):
            op.process_single({})

        mock_client_cls.assert_not_called()

    @patch("data_juicer.ops.mapper.ad_ai_data_center.external_eval_data_import_mapper.HttpClient")
    def test_rejects_missing_api_base_in_ctx_before_loading_sheet(self, mock_client_cls):
        ctx = self._ctx()
        ctx.pop("apiBase")
        op = ExternalEvalDataImportMapper(
            sheet_url="https://bytedance.feishu.cn/sheets/xxxx",
            data_type="eval_data",
            ctx=ctx,
            python_code="def process(data, context):\n    return {\"items\": data}",
            auto_op_parallelism=False,
        )

        with self.assertRaisesRegex(ValueError, "ctx.apiBase must be provided"):
            op.process_single({})

        mock_client_cls.assert_not_called()

    def test_config_uses_operator_name_without_ad_ai_data_center_prefix(self):
        config_path = Path("/private/tmp/external_eval_data_import_mapper_config_test.yaml")
        config_path.write_text(
            """
project_name: test_external_eval_data_import_mapper
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - external_eval_data_import_mapper:
      sheet_url: "https://bytedance.feishu.cn/sheets/xxxx"
      data_type: "eval_data"
      ctx:
        userAccount: "wangjianda.667"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        synthesisInstanceId: 10001
        flowInstanceId: 20001
        flowNodeId: "node_load_data"
        taskId: 30001
        taskVersion: 1
        operatorIndex: 0
        operatorName: "external_eval_data_import_mapper"
        operatorType: "business"
        apiBase: "https://ai-data-center.bytedance.net/api"
      python_code: "def process(data, context):\\n    return {\\"items\\": data}"
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

        self.assertEqual(OP_NAME, "external_eval_data_import_mapper")
        self.assertEqual(NEED_CTX, True)
        self.assertIsInstance(ops[0], ExternalEvalDataImportMapper)
        self.assertEqual(ops[0].ctx["userAccount"], "wangjianda.667")

    @unittest.skipUnless(
        os.getenv("RUN_REAL_EXTERNAL_EVAL_DATA_IMPORT_TEST") == "1",
        "Set RUN_REAL_EXTERNAL_EVAL_DATA_IMPORT_TEST=1 to call the real cloud-doc API.",
    )
    def test_lark_wiki_doc_url_sends_real_request_and_processes_result(self):
        op = ExternalEvalDataImportMapper(
            sheet_url="https://bytedance.larkoffice.com/wiki/AnACwPRtOiRxJRk30s8ckk8Yneg",
            data_type="eval_data",
            ctx=self._ctx(),
            python_code=(
                "def process(data, context):\n"
                "    return {\n"
                "        \"data_type\": context[\"data_type\"],\n"
                "        \"sheet_url\": context[\"sheet_url\"],\n"
                "        \"sheet_count\": len(context[\"raw_sheets\"]),\n"
                "        \"item_count\": len(data),\n"
                "        \"first_item\": data[0] if data else {},\n"
                "    }"
            ),
            auto_op_parallelism=False,
        )

        result = op.process_single({
            "id": "real-doc-url-example",
        })

        external_data_set = result["externalDataSet"]
        self.assertEqual(external_data_set["data_type"], "eval_data")
        self.assertEqual(
            external_data_set["sheet_url"],
            "https://bytedance.larkoffice.com/wiki/AnACwPRtOiRxJRk30s8ckk8Yneg",
        )
        self.assertGreater(external_data_set["sheet_count"], 0)
        self.assertIsInstance(external_data_set["item_count"], int)
        self.assertIsInstance(external_data_set["first_item"], dict)

    def test_rejects_empty_sheet_url(self):
        with self.assertRaisesRegex(ValueError, "sheet_url must be provided"):
            ExternalEvalDataImportMapper(
                sheet_url="",
                data_type="eval_data",
                python_code="def process(data, context):\n    return data",
            )

    def test_rejects_unsupported_data_type(self):
        with self.assertRaisesRegex(ValueError, "Unsupported data_type"):
            ExternalEvalDataImportMapper(
                sheet_url="https://bytedance.feishu.cn/sheets/xxxx",
                data_type="other",
                python_code="def process(data, context):\n    return data",
            )

    def test_rejects_python_code_without_process_function(self):
        with self.assertRaisesRegex(ValueError, "define a callable process"):
            ExternalEvalDataImportMapper(
                sheet_url="https://bytedance.feishu.cn/sheets/xxxx",
                data_type="eval_data",
                python_code="x = 1",
            )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.external_eval_data_import_mapper.HttpClient")
    def test_rejects_non_serializable_python_result(self, mock_client_cls):
        mock_client_cls.return_value = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {
                "sheets": [
                    {
                        "sheetId": "sheet_1",
                        "title": "Sheet1",
                        "values": [["query"], ["hello"]],
                    }
                ]
            },
            "text": None,
            "error": None,
        })
        op = ExternalEvalDataImportMapper(
            sheet_url="https://bytedance.feishu.cn/sheets/xxxx",
            data_type="eval_data",
            ctx=self._ctx(),
            python_code="def process(data, context):\n    return {\"bad\": {1, 2}}",
            auto_op_parallelism=False,
        )

        with self.assertRaisesRegex(ValueError, "must be JSON serializable"):
            op.process_single({})


if __name__ == "__main__":
    unittest.main()
