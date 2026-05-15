import unittest
from pathlib import Path
from unittest.mock import patch

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.external_eval_data_import_mapper import (
    DEFAULT_ENDPOINT,
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

    @patch("data_juicer.ops.mapper.ad_ai_data_center.external_eval_data_import_mapper.HttpClient")
    def test_imports_eval_data_sheet_and_runs_python_process(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
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
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        op = ExternalEvalDataImportMapper(
            sheet_url="https://bytedance.feishu.cn/sheets/xxxx",
            data_type="eval_data",
            python_code=(
                "def process(data, context):\n"
                "    return {\"items\": data, \"data_type\": context[\"data_type\"], "
                "\"sheet_count\": len(context[\"raw_sheets\"])}"
            ),
            auto_op_parallelism=False,
        )
        dataset = Dataset.from_list([{"id": "row-1"}])

        result = op.run(dataset).to_list()

        mock_client_cls.assert_called_once_with(
            endpoint=DEFAULT_ENDPOINT,
            method="POST",
            timeout=30.0,
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
        self.assertIsInstance(ops[0], ExternalEvalDataImportMapper)

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
            python_code="def process(data, context):\n    return {\"bad\": {1, 2}}",
            auto_op_parallelism=False,
        )

        with self.assertRaisesRegex(ValueError, "must be JSON serializable"):
            op.process_single({})


if __name__ == "__main__":
    unittest.main()
