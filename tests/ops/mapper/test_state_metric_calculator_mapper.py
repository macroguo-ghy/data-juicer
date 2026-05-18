import unittest
from pathlib import Path
from unittest.mock import ANY, patch

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper import (
    BATCH_GET_OPERATORS_PATH,
    CONFIG_PAGE_KEY,
    NEED_CTX,
    OP_NAME,
    StateMetricCalculatorMapper,
)
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


class StateMetricCalculatorMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    def setUp(self):
        self.callback_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.OperatorExecutionCallbackClient"
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
            "flowNodeId": "node_state_metric",
            "taskId": 30001,
            "taskVersion": 1,
            "operatorIndex": 2,
            "operatorName": "state_metric_calculator",
            "operatorType": "business",
            "apiBase": "https://ai-data-center.bytedance.net/api",
        }

    @staticmethod
    def _operators():
        return [
            {
                "operator_id": 201,
                "parameter_mapping": {
                    "bench_roi": "bench_roi",
                },
            },
            {
                "operator_id": 202,
                "parameter_mapping": {},
            },
        ]

    @staticmethod
    def _operator_details():
        return {
            "operators": [
                {
                    "id": 201,
                    "operatorNameEn": "bench_roi_score",
                    "operatorNameCn": "行业基准 ROI 得分",
                    "inputParameter": (
                        '{"parameters": ['
                        '{"name": "state", "type": "object", "required": true, "source": "state"},'
                        '{"name": "ad_material_click_rate", "type": "float", "required": true, '
                        '"source": "attribute", "attributeId": 101},'
                        '{"name": "bench_roi", "type": "float", "required": true, "source": "field"}'
                        ']}'
                    ),
                    "relatedAttributes": '{"attributes": [{"id": 101}]}',
                    "operatorCode": (
                        "def calculate(state, ad_material_click_rate, bench_roi):\n"
                        "    return round(ad_material_click_rate / bench_roi, 2)\n"
                    ),
                },
                {
                    "id": 202,
                    "operatorNameEn": "quality_score",
                    "operatorNameCn": "质量得分",
                    "inputParameter": (
                        '{"parameters": ['
                        '{"name": "state", "required": true, "source": "state"},'
                        '{"name": "quality", "required": true, "source": "attribute", "attributeId": 102}'
                        ']}'
                    ),
                    "operatorCode": (
                        "def calculate(state, quality):\n"
                        "    return quality + 1\n"
                    ),
                },
            ]
        }

    def test_declares_operator_metadata(self):
        self.assertEqual(OP_NAME, "state_metric_calculator")
        self.assertEqual(CONFIG_PAGE_KEY, "state_metric_calculator")
        self.assertEqual(NEED_CTX, True)
        self.assertEqual(
            BATCH_GET_OPERATORS_PATH,
            "/openapi/state-meta/operators/batch-get",
        )

    def test_config_loads_operator_name_without_ad_ai_data_center_prefix(self):
        config_path = Path("/private/tmp/state_metric_calculator_config_test.yaml")
        config_path.write_text(
            """
project_name: test_state_metric_calculator
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - state_metric_calculator:
      state_key: "state"
      output_key: "query_metric_data_outputs"
      operators:
        - operator_id: 201
          parameter_mapping:
            bench_roi: "bench_roi"
      ctx:
        userAccount: "wangjianda.667"
        synthesisInstanceId: 10001
        taskId: 30001
        taskVersion: 1
        operatorIndex: 2
        operatorName: "state_metric_calculator"
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

        self.assertIsInstance(ops[0], StateMetricCalculatorMapper)
        self.assertEqual(ops[0].operators[0]["operator_id"], 201)

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_calculates_metrics_and_caches_operator_details(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope(self._operator_details()))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=self._operators(),
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )
        dataset = Dataset.from_list([
            {
                RECORD_KEY_FIELD: "record-1",
                "state": {
                    "101": 0.41,
                    "quality": 8,
                },
                "bench_roi": 0.5,
            },
            {
                RECORD_KEY_FIELD: "record-2",
                "state": {
                    "ad_material_click_rate": 0.8,
                    "102": 5,
                },
                "bench_roi": 0.4,
            },
        ])

        result = op.run(dataset).to_list()

        mock_client_cls.assert_called_once_with(
            endpoint=(
                "https://ai-data-center.bytedance.net/api/"
                "openapi/state-meta/operators/batch-get"
            ),
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "user-account": "wangjianda.667",
                "x-tt-env": "ppe_sirius2",
                "x-use-ppe": "1",
            },
            timeout=30.0,
        )
        self.assertEqual(fake_client.requests, [{
            "json_body": {
                "operatorIds": [201, 202],
            }
        }])
        self.assertEqual(
            result[0]["query_metric_data_outputs"]["bench_roi_score"],
            {
                "success": True,
                "value": 0.82,
                "error": "",
                "operator_id": 201,
                "operator_name_cn": "行业基准 ROI 得分",
            },
        )
        self.assertEqual(
            result[0]["query_metric_data_outputs"]["quality_score"]["value"],
            9,
        )
        self.assertEqual(
            result[1]["query_metric_data_outputs"]["bench_roi_score"]["value"],
            2.0,
        )
        self.mock_callback.report_record_success.assert_any_call(
            record_key="record-1",
            input_data=ANY,
            output_data=result[0],
            started_at=ANY,
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_metric_failure_is_written_to_output_when_fail_policy_continue(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope(self._operator_details()))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[self._operators()[0]],
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "state": {
                "101": 0.41,
            },
        })

        self.assertEqual(
            result["query_metric_data_outputs"]["bench_roi_score"],
            {
                "success": False,
                "value": None,
                "error": "missing required parameter: bench_roi",
                "operator_id": 201,
                "operator_name_cn": "行业基准 ROI 得分",
            },
        )
        self.mock_callback.report_record_failure.assert_not_called()
        self.mock_callback.report_record_success.assert_called_once()

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_http_failure_records_each_metric_failure_without_raising(self, mock_client_cls):
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
        op = StateMetricCalculatorMapper(
            operators=self._operators(),
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "state": {},
        })

        self.assertEqual(
            sorted(result["query_metric_data_outputs"].keys()),
            ["operator_201", "operator_202"],
        )
        self.assertFalse(result["query_metric_data_outputs"]["operator_201"]["success"])
        self.assertIsNone(result["query_metric_data_outputs"]["operator_201"]["value"])
        self.assertIn(
            "Failed to fetch state metric operators",
            result["query_metric_data_outputs"]["operator_201"]["error"],
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_missing_state_key_when_all_metrics_depend_on_state_fails_record(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope(self._operator_details()))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=self._operators(),
            ctx=self._ctx(),
        )
        sample = {
            RECORD_KEY_FIELD: "record-1",
            "bench_roi": 0.5,
        }

        with self.assertRaisesRegex(ValueError, "sample.state must be provided"):
            op.process_single(sample)

        self.mock_callback.report_record_failure.assert_called_once_with(
            record_key="record-1",
            input_data=sample,
            output_data=sample,
            error_message="sample.state must be provided",
            started_at=ANY,
        )

    def test_missing_user_account_fails_record(self):
        ctx = self._ctx()
        ctx.pop("userAccount")
        op = StateMetricCalculatorMapper(
            operators=self._operators(),
            ctx=ctx,
        )
        sample = {
            RECORD_KEY_FIELD: "record-1",
            "state": {},
        }

        with self.assertRaisesRegex(ValueError, "ctx.userAccount must be provided"):
            op.process_single(sample)

        self.mock_callback.report_record_failure.assert_called_once_with(
            record_key="record-1",
            input_data=sample,
            output_data=sample,
            error_message="ctx.userAccount must be provided",
            started_at=ANY,
        )

    def test_rejects_invalid_constructor_arguments(self):
        with self.assertRaisesRegex(ValueError, "operators"):
            StateMetricCalculatorMapper(operators=[], ctx=self._ctx())
        with self.assertRaisesRegex(ValueError, "operator_id"):
            StateMetricCalculatorMapper(operators=[{}], ctx=self._ctx())
        with self.assertRaisesRegex(ValueError, "result_mode"):
            StateMetricCalculatorMapper(
                operators=self._operators(),
                result_mode="list",
                ctx=self._ctx(),
            )
        with self.assertRaisesRegex(ValueError, "fail_policy"):
            StateMetricCalculatorMapper(
                operators=self._operators(),
                fail_policy="stop",
                ctx=self._ctx(),
            )

    def test_before_operator_started_starts_running_once(self):
        op = StateMetricCalculatorMapper(
            operators=self._operators(),
            output_key="metric_outputs",
            ctx=self._ctx(),
        )

        op.before_operator_started()
        op.before_operator_started()

        self.mock_callback.start.assert_called_once_with(
            operator_config={
                "state_key": "state",
                "output_key": "metric_outputs",
                "result_mode": "object",
                "fail_policy": "continue",
                "operators": self._operators(),
            }
        )

    def test_after_operator_finished_finalizes_success_or_failure(self):
        op = StateMetricCalculatorMapper(
            operators=self._operators(),
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
