import unittest
from pathlib import Path
from unittest.mock import ANY, patch

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.derived_input_parameter_context_mapper import (
    CONFIG_PAGE_KEY,
    INPUT_KEYS_BATCH_GET_PATH,
    NEED_CTX,
    OP_NAME,
    OP_DISPLAY_NAME,
    DerivedInputParameterContextMapper,
)
from data_juicer.utils.adc_record_context import ADC_LOG_ID_FIELD
from data_juicer.utils.operator_execution_callback_utils import RECORD_KEY_FIELD


class FakeHttpClient:

    def __init__(self, result):
        self.result = result
        self.requests = []

    def request(self, **kwargs):
        self.requests.append(kwargs)
        return self.result


def success_envelope(data):
    return {
        "ok": True,
        "status_code": 200,
        "data": {
            "code": 0,
            "message": "success",
            "data": data,
        },
        "text": None,
        "error": None,
    }


class DerivedInputParameterContextMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    def setUp(self):
        self.callback_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center."
            "derived_input_parameter_context_mapper.OperatorExecutionCallbackClient"
        )
        self.mock_callback_cls = self.callback_patcher.start()
        self.mock_callback = self.mock_callback_cls.return_value
        self.mock_callback.start.return_value = 10001

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
            "flowNodeId": "node_derived_input_parameter_context",
            "taskId": 30001,
            "taskVersion": 1,
            "operatorIndex": 1,
            "operatorName": "derived_input_parameter_context_mapper",
            "operatorType": "business",
            "apiBase": "https://ai-data-center.bytedance.net/api",
            "spaceId": 1,
        }

    def test_declares_operator_metadata(self):
        self.assertEqual(OP_NAME, "derived_input_parameter_context_mapper")
        self.assertEqual(OP_DISPLAY_NAME, "生成派生字段入参元信息")
        self.assertEqual(CONFIG_PAGE_KEY, "derived_input_parameter_context_builder")
        self.assertEqual(NEED_CTX, True)
        self.assertEqual(
            INPUT_KEYS_BATCH_GET_PATH,
            "/openapi/state-meta/input-keys/batch-get",
        )

    def test_accepts_input_key_ids_config(self):
        op = DerivedInputParameterContextMapper(
            input_key_ids=[8, "12", 13],
            ctx=self._ctx(),
        )

        self.assertEqual(op.input_key_ids, [8, 12, 13])

    def test_rejects_empty_input_key_ids(self):
        with self.assertRaisesRegex(ValueError, "input_key_ids"):
            DerivedInputParameterContextMapper(input_key_ids=[], ctx=self._ctx())

    def test_config_loads_operator_name_without_ad_ai_data_center_prefix(self):
        config_path = Path("/private/tmp/derived_input_parameter_context_mapper_config_test.yaml")
        config_path.write_text(
            """
project_name: test_derived_input_parameter_context_mapper
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - derived_input_parameter_context_mapper:
      input_key_ids:
        - 8
        - 12
      ctx:
        userAccount: "wangjianda.667"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        synthesisInstanceId: 10001
        flowInstanceId: 20001
        flowNodeId: "node_derived_input_parameter_context"
        taskId: 30001
        taskVersion: 1
        operatorIndex: 1
        operatorName: "derived_input_parameter_context_mapper"
        operatorType: "business"
        apiBase: "https://ai-data-center.bytedance.net/api"
        spaceId: 1
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

        self.assertIsInstance(ops[0], DerivedInputParameterContextMapper)
        self.assertEqual(ops[0].input_key_ids, [8, 12])
        self.assertEqual(ops[0].ctx["userAccount"], "wangjianda.667")

    @patch(
        "data_juicer.ops.mapper.ad_ai_data_center."
        "derived_input_parameter_context_mapper.HttpClient"
    )
    def test_fetches_selected_input_key_metadata_and_reports_success(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "inputParameterDetails": [
                {
                    "keyId": 8,
                    "keyNameEn": "unknown_id",
                    "keyNameCn": "未知ID",
                    "description": "可能表示广告、广告主或素材 ID",
                    "demoValue": "1837647382987362",
                    "multiValue": True,
                }
            ]
        }))
        mock_client_cls.return_value = fake_client
        op = DerivedInputParameterContextMapper(
            input_key_ids=[8],
            ctx=self._ctx(),
        )
        sample = {
            RECORD_KEY_FIELD: "record-1",
            ADC_LOG_ID_FIELD: "log-001",
            "text": "row",
        }
        input_sample = dict(sample)

        result = op.process_single(sample)

        mock_client_cls.assert_called_once_with(
            endpoint=(
                "https://ai-data-center.bytedance.net/api/"
                "openapi/state-meta/input-keys/batch-get"
            ),
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "user-account": "wangjianda.667",
                "space-id": "1",
                "x-tt-env": "ppe_sirius2",
                "x-use-ppe": "1",
                "x-tt-logid": "log-001",
            },
            timeout=30.0,
            retry_attempts=3,
        )
        self.assertEqual(fake_client.requests, [{
            "json_body": {
                "keyIds": [8],
            }
        }])
        self.assertEqual(
            result["unknown_id"],
            "未知ID：可能表示广告、广告主或素材 ID。示例：1837647382987362。支持多值。",
        )
        self.assertEqual(result["text"], "row")
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key="record-1",
            input_data=input_sample,
            output_data=result,
            started_at=ANY,
        )

    @patch(
        "data_juicer.ops.mapper.ad_ai_data_center."
        "derived_input_parameter_context_mapper.HttpClient"
    )
    def test_caches_parameter_columns_in_operator_instance(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "inputParameterDetails": [
                {
                    "keyId": 8,
                    "keyNameEn": "unknown_id",
                    "keyNameCn": "未知ID",
                    "description": "可能表示广告、广告主或素材 ID",
                }
            ]
        }))
        mock_client_cls.return_value = fake_client
        op = DerivedInputParameterContextMapper(
            input_key_ids=[8],
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )
        dataset = Dataset.from_list([
            {
                RECORD_KEY_FIELD: "record-1",
                "text": "row-1",
            },
            {
                RECORD_KEY_FIELD: "record-2",
                "text": "row-2",
            },
        ])

        result = op.run(dataset).to_list()

        mock_client_cls.assert_called_once()
        self.assertEqual(len(fake_client.requests), 1)
        self.assertEqual(result[0]["unknown_id"], "未知ID：可能表示广告、广告主或素材 ID。")
        self.assertEqual(result[1]["unknown_id"], result[0]["unknown_id"])

    def test_formats_parameter_description_with_optional_parts(self):
        self.assertEqual(
            DerivedInputParameterContextMapper._format_parameter_description({
                "keyId": 8,
                "keyNameEn": "unknown_id",
                "keyNameCn": "未知ID",
                "description": "可能表示广告、广告主或素材 ID",
                "demoValue": "1837647382987362",
                "multiValue": True,
            }),
            "未知ID：可能表示广告、广告主或素材 ID。示例：1837647382987362。支持多值。",
        )

    def test_build_parameter_columns_rejects_duplicate_key_name_en(self):
        with self.assertRaisesRegex(ValueError, "duplicate keyNameEn"):
            DerivedInputParameterContextMapper._build_parameter_columns([
                {
                    "keyId": 8,
                    "keyNameEn": "unknown_id",
                    "keyNameCn": "未知ID",
                    "description": "描述A",
                },
                {
                    "keyId": 9,
                    "keyNameEn": "unknown_id",
                    "keyNameCn": "未知ID",
                    "description": "描述B",
                },
            ])

    @patch(
        "data_juicer.ops.mapper.ad_ai_data_center."
        "derived_input_parameter_context_mapper.HttpClient"
    )
    def test_rejects_response_missing_requested_key_id(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "inputParameterDetails": [
                {
                    "keyId": 8,
                    "keyNameEn": "unknown_id",
                    "keyNameCn": "未知ID",
                }
            ]
        }))
        mock_client_cls.return_value = fake_client
        op = DerivedInputParameterContextMapper(
            input_key_ids=[8, 12],
            ctx=self._ctx(),
        )

        with self.assertRaisesRegex(ValueError, "missing keyIds: 12"):
            op.process_single({RECORD_KEY_FIELD: "record-1"})

    def test_build_parameter_columns_rejects_non_object_items(self):
        with self.assertRaisesRegex(
            ValueError,
            "inputParameterDetails item must be an object",
        ):
            DerivedInputParameterContextMapper._build_parameter_columns([
                "not-object",
            ])

    @patch(
        "data_juicer.ops.mapper.ad_ai_data_center."
        "derived_input_parameter_context_mapper.logger"
    )
    def test_build_parameter_columns_keeps_first_duplicate_key_id(self, mock_logger):
        columns = DerivedInputParameterContextMapper._build_parameter_columns([
            {
                "keyId": 8,
                "keyNameEn": "unknown_id",
                "keyNameCn": "未知ID",
                "description": "描述A",
            },
            {
                "keyId": 8,
                "keyNameEn": "unknown_id",
                "keyNameCn": "未知ID",
                "description": "描述B",
            },
        ])

        self.assertEqual(columns, {"unknown_id": "未知ID：描述A。"})
        mock_logger.warning.assert_called_once()

    @patch(
        "data_juicer.ops.mapper.ad_ai_data_center."
        "derived_input_parameter_context_mapper.HttpClient"
    )
    def test_http_failure_reports_record_failure_and_raises(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": False,
            "status_code": 500,
            "data": None,
            "text": None,
            "error": {
                "type": "HTTPStatusError",
                "message": "server error",
            },
        })
        mock_client_cls.return_value = fake_client
        op = DerivedInputParameterContextMapper(
            input_key_ids=[8],
            ctx=self._ctx(),
        )
        sample = {
            RECORD_KEY_FIELD: "record-1",
            "text": "row",
        }

        with self.assertRaisesRegex(ValueError, "Failed to fetch input key metadata"):
            op.process_single(sample)

        self.mock_callback.report_record_failure.assert_called_once_with(
            record_key="record-1",
            input_data=sample,
            output_data=None,
            error_message=ANY,
            started_at=ANY,
        )

    @patch(
        "data_juicer.ops.mapper.ad_ai_data_center."
        "derived_input_parameter_context_mapper.HttpClient"
    )
    def test_business_failure_raises_clear_error(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {
                "code": 1001,
                "message": "input key not found",
                "data": None,
            },
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        op = DerivedInputParameterContextMapper(
            input_key_ids=[8],
            ctx=self._ctx(),
        )

        with self.assertRaisesRegex(ValueError, "input key not found"):
            op.process_single({RECORD_KEY_FIELD: "record-1"})

    @patch(
        "data_juicer.ops.mapper.ad_ai_data_center."
        "derived_input_parameter_context_mapper.logger"
    )
    @patch(
        "data_juicer.ops.mapper.ad_ai_data_center."
        "derived_input_parameter_context_mapper.HttpClient"
    )
    def test_missing_record_key_keeps_result_and_logs_callback_failure(
        self,
        mock_client_cls,
        mock_logger,
    ):
        fake_client = FakeHttpClient(success_envelope({
            "inputParameterDetails": [
                {
                    "keyId": 8,
                    "keyNameEn": "unknown_id",
                    "keyNameCn": "未知ID",
                    "description": "可能表示广告、广告主或素材 ID",
                }
            ]
        }))
        mock_client_cls.return_value = fake_client
        op = DerivedInputParameterContextMapper(
            input_key_ids=[8],
            ctx=self._ctx(),
        )

        result = op.process_single({"text": "row"})

        self.assertEqual(result["unknown_id"], "未知ID：可能表示广告、广告主或素材 ID。")
        self.mock_callback.report_record_success.assert_not_called()
        self.assertEqual(mock_logger.warning.call_count, 1)
        message, exc = mock_logger.warning.call_args.args
        self.assertEqual(message, "Failed to report record success callback: {}")
        self.assertIsInstance(exc, ValueError)
        self.assertEqual(str(exc), f"sample.{RECORD_KEY_FIELD} must be provided")


if __name__ == "__main__":
    unittest.main()
