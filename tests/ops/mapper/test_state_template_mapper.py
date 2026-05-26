import unittest
from pathlib import Path
from unittest.mock import ANY, patch

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.state_template_mapper import (
    CONFIG_PAGE_KEY,
    GENERATE_JSON_PATH,
    NEED_CTX,
    OP_NAME,
    StateTemplateMapper,
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


class StateTemplateMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    def setUp(self):
        self.callback_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.state_template_mapper.OperatorExecutionCallbackClient"
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
            "flowNodeId": "node_state_template",
            "taskId": 30001,
            "taskVersion": 1,
            "operatorIndex": 1,
            "operatorName": "state_template_mapper",
            "operatorType": "business",
            "apiBase": "https://ai-data-center.bytedance.net/api",
            "spaceId": 1,
        }

    @staticmethod
    def _state_meta_group_items():
        return {
            "ad_state": [101, 102],
            "world_state": [201],
        }

    @staticmethod
    def _state_template():
        return {
            "ad_state": {
                "ad_material_clicks": {
                    "cn_name": "素材点击数",
                    "description": "素材每日点击数 14日序列",
                    "format_requirement": "14个元素的整数数组，非负",
                }
            }
        }

    def test_declares_operator_metadata(self):
        self.assertEqual(OP_NAME, "state_template_mapper")
        self.assertEqual(CONFIG_PAGE_KEY, "state_template_builder")
        self.assertEqual(NEED_CTX, True)
        self.assertEqual(GENERATE_JSON_PATH, "/openapi/state-meta/generate-json")

    def test_config_loads_operator_name_without_ad_ai_data_center_prefix(self):
        config_path = Path("/private/tmp/state_template_mapper_config_test.yaml")
        config_path.write_text(
            """
project_name: test_state_template_mapper
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - state_template_mapper:
      state_meta_group_items:
        ad_state:
          - 101
          - 102
        world_state:
          - 201
      ctx:
        userAccount: "wangjianda.667"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        synthesisInstanceId: 10001
        flowInstanceId: 20001
        flowNodeId: "node_state_template"
        taskId: 30001
        taskVersion: 1
        operatorIndex: 1
        operatorName: "state_template_mapper"
        operatorType: "business"
        apiBase: "https://ai-data-center.bytedance.net/api"
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

        self.assertIsInstance(ops[0], StateTemplateMapper)
        self.assertEqual(ops[0].state_meta_group_items, self._state_meta_group_items())

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_template_mapper.HttpClient")
    def test_generates_state_template_from_json_string_response(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope(
            '{"ad_state": {"ad_material_clicks": {"cn_name": "素材点击数"}}}'
        ))
        mock_client_cls.return_value = fake_client
        op = StateTemplateMapper(
            state_meta_group_items=self._state_meta_group_items(),
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )
        dataset = Dataset.from_list([{
            "text": "row",
            RECORD_KEY_FIELD: "record-1",
            ADC_LOG_ID_FIELD: "log-001",
        }])

        result = op.run(dataset).to_list()

        mock_client_cls.assert_called_once_with(
            endpoint=(
                "https://ai-data-center.bytedance.net/api/"
                "openapi/state-meta/generate-json"
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
                "groupItems": self._state_meta_group_items(),
            }
        }])
        self.assertEqual(
            result[0]["state_template"],
            '{"ad_state": {"ad_material_clicks": {"cn_name": "素材点击数"}}}',
        )
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key="record-1",
            input_data={
                "text": "row",
                RECORD_KEY_FIELD: "record-1",
                ADC_LOG_ID_FIELD: "log-001",
            },
            output_data=result[0],
            started_at=ANY,
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_template_mapper.HttpClient")
    def test_generates_state_template_from_object_response(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope(self._state_template()))
        mock_client_cls.return_value = fake_client
        op = StateTemplateMapper(
            state_meta_group_items=self._state_meta_group_items(),
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(
            result["state_template"],
            (
                '{"ad_state": {"ad_material_clicks": {"cn_name": "素材点击数", '
                '"description": "素材每日点击数 14日序列", '
                '"format_requirement": "14个元素的整数数组，非负"}}}'
            ),
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_template_mapper.HttpClient")
    def test_caches_generated_state_template_in_operator_instance(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope(
            '{"ad_state": {"ad_material_clicks": {"cn_name": "素材点击数"}}}'
        ))
        mock_client_cls.return_value = fake_client
        op = StateTemplateMapper(
            state_meta_group_items=self._state_meta_group_items(),
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )
        dataset = Dataset.from_list([
            {
                "text": "row-1",
                RECORD_KEY_FIELD: "record-1",
            },
            {
                "text": "row-2",
                RECORD_KEY_FIELD: "record-2",
            },
        ])

        result = op.run(dataset).to_list()

        mock_client_cls.assert_called_once()
        self.assertEqual(len(fake_client.requests), 1)
        self.assertEqual(
            result[0]["state_template"],
            '{"ad_state": {"ad_material_clicks": {"cn_name": "素材点击数"}}}',
        )
        self.assertEqual(result[1]["state_template"], result[0]["state_template"])

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_template_mapper.HttpClient")
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
        op = StateTemplateMapper(
            state_meta_group_items=self._state_meta_group_items(),
            ctx=self._ctx(),
        )
        sample = {
            RECORD_KEY_FIELD: "record-1",
        }

        with self.assertRaisesRegex(ValueError, "Failed to generate state template"):
            op.process_single(sample)

        self.mock_callback.report_record_failure.assert_called_once_with(
            record_key="record-1",
            input_data={
                RECORD_KEY_FIELD: "record-1",
            },
            output_data=None,
            error_message=ANY,
            started_at=ANY,
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_template_mapper.HttpClient")
    def test_business_failure_raises_clear_error(self, mock_client_cls):
        fake_client = FakeHttpClient({
            "ok": True,
            "status_code": 200,
            "data": {
                "code": 1001,
                "message": "groupName not found",
                "data": None,
            },
            "text": None,
            "error": None,
        })
        mock_client_cls.return_value = fake_client
        op = StateTemplateMapper(
            state_meta_group_items=self._state_meta_group_items(),
            ctx=self._ctx(),
        )

        with self.assertRaisesRegex(ValueError, "groupName not found"):
            op.process_single({RECORD_KEY_FIELD: "record-1"})

    def test_rejects_invalid_constructor_arguments(self):
        with self.assertRaisesRegex(ValueError, "state_meta_group_items"):
            StateTemplateMapper(state_meta_group_items={}, ctx=self._ctx())
        with self.assertRaisesRegex(ValueError, "state_meta_group_items"):
            StateTemplateMapper(state_meta_group_items=[], ctx=self._ctx())
        with self.assertRaisesRegex(ValueError, "output_field"):
            StateTemplateMapper(
                state_meta_group_items=self._state_meta_group_items(),
                output_field="",
                ctx=self._ctx(),
            )

    def test_before_operator_started_starts_running_once(self):
        op = StateTemplateMapper(
            state_meta_group_items=self._state_meta_group_items(),
            output_field="custom_state_template",
            ctx=self._ctx(),
        )

        op.before_operator_started()
        op.before_operator_started()

        self.mock_callback.start.assert_called_once_with(
            operator_config={
                "state_meta_group_items": self._state_meta_group_items(),
                "output_field": "custom_state_template",
            }
        )

    def test_after_operator_finished_finalizes_success_or_failure(self):
        op = StateTemplateMapper(
            state_meta_group_items=self._state_meta_group_items(),
            ctx=self._ctx(),
        )

        op.after_operator_finished(error=None)
        op.after_operator_finished(error=RuntimeError("consume failed"))

        self.mock_callback.finalize.assert_called_once_with()
        self.mock_callback.failed.assert_called_once_with(
            error_message="consume failed"
        )


if __name__ == "__main__":
    unittest.main()
