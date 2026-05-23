import json
import unittest
from pathlib import Path
from unittest.mock import ANY, patch

import pyarrow as pa

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
from data_juicer.utils.adc_record_context import ADC_LOG_ID_FIELD
from data_juicer.utils.operator_execution_callback_utils import RECORD_KEY_FIELD


class FakeHttpClient:

    def __init__(self, result):
        self.result = result
        self.requests = []

    def request(self, **kwargs):
        self.requests.append(kwargs)
        return self.result


class FakeRayDataset:

    def __init__(self):
        self.repartition_calls = []

    def repartition(self, **kwargs):
        self.repartition_calls.append(kwargs)
        return self


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
                    "ids": "material_id",
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
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_cn": "素材 ID", '
                        '"key_name_en": "ids", "default_or_placeholder_value": "ids"},'
                        '{"data_type": "placeholder", "key_name_cn": "行业基准 ROI", '
                        '"key_name_en": "bench_roi", "default_or_placeholder_value": "bench_roi"}'
                        ']}'
                    ),
                    "relatedAttributes": '{"attributes": [{"id": 101}]}',
                    "operatorCode": (
                        "def calculate(state, bench_roi):\n"
                        "    ad_material_click_rate = state.get('ad_material_click_rate') or state.get('101')\n"
                        "    return round(ad_material_click_rate / bench_roi, 2)\n"
                    ),
                },
                {
                    "id": 202,
                    "operatorNameEn": "quality_score",
                    "operatorNameCn": "质量得分",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "defaultValue", "key_name_cn": "偏移量", '
                        '"key_name_en": "offset", "default_or_placeholder_value": 1}'
                        ']}'
                    ),
                    "operatorCode": (
                        "def calculate(state, offset):\n"
                        "    quality = state.get('quality') or state.get('102')\n"
                        "    return quality + offset\n"
                    ),
                },
            ]
        }

    def _summary(self, result, output_key="query_metric_data_outputs"):
        value = result[output_key]
        self.assertIsInstance(value, str)
        return json.loads(value) if value else {}

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
        self.assertIsNone(ops[0].repartition_num_blocks)

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
                ADC_LOG_ID_FIELD: "log-001",
                "material_id": "1854168911595796",
                "state": {
                    "101": 0.41,
                    "quality": 8,
                },
                "bench_roi": 0.5,
            },
            {
                RECORD_KEY_FIELD: "record-2",
                "material_id": "2854168911595796",
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
                "x-tt-logid": "log-001",
            },
            timeout=30.0,
            retry_attempts=3,
        )
        self.assertEqual(fake_client.requests, [{
            "json_body": {
                "operatorIds": [201, 202],
            }
        }])
        self.assertEqual(self._summary(result[0]), {
            "1854168911595796": {
                "metrics": [
                    {
                        "metricCode": "bench_roi_score",
                        "metricName": "行业基准 ROI 得分",
                        "output": "0.82",
                        "error": "",
                    },
                    {
                        "metricCode": "quality_score",
                        "metricName": "质量得分",
                        "output": "9",
                        "error": "",
                    },
                ],
            },
        })
        self.assertEqual(
            self._summary(result[1])["2854168911595796"]["metrics"][0]["output"],
            "2.0",
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
            "material_id": "1854168911595796",
            "state": {
                "101": 0.41,
            },
        })

        self.assertEqual(self._summary(result), {
            "1854168911595796": {
                "metrics": [
                    {
                        "metricCode": "bench_roi_score",
                        "metricName": "行业基准 ROI 得分",
                        "output": "null",
                        "error": "missing required parameter: bench_roi",
                    },
                ],
            },
        })
        self.mock_callback.report_record_failure.assert_not_called()
        self.mock_callback.report_record_success.assert_called_once()

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_json_string_state_is_parsed_before_calculate(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 203,
                    "operatorNameEn": "ad_roi_trend",
                    "operatorNameCn": "ROI趋势",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_cn": "开始时间", '
                        '"key_name_en": "start_time", "default_or_placeholder_value": "start_time"}'
                        ']}'
                    ),
                    "operatorCode": (
                        "def calculate(state):\n"
                        "    values = state['adv_state'][0]['adv_roi']\n"
                        "    return round(float(values[-1]) - float(values[0]), 4)\n"
                    ),
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[{
                "operator_id": 203,
                "parameter_mapping": {
                    "start_time": "source_table_start_time",
                },
            }],
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "state": (
                '{"world_state": {"query_time": "2026-06-10 15:22:34"}, '
                '"adv_state": [{"adv_id": "9876543210987654", '
                '"adv_roi": [0.28, 0.35, 0.42, 0.78, 0.59, 0.72, 0.31, '
                '0.6, 0.83, 0.24, 0.55, 0.69, 0.46, 0.91]}]}'
            ),
        })

        self.assertEqual(self._summary(result), {
            "unknown": {
                "metrics": [
                    {
                        "metricCode": "ad_roi_trend",
                        "metricName": "ROI趋势",
                        "output": "0.63",
                        "error": "",
                    },
                ],
            },
        })

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_complex_metric_output_is_preserved(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 204,
                    "operatorNameEn": "ad_roi_trend",
                    "operatorNameCn": "ROI趋势",
                    "inputParameter": '{"params": []}',
                    "operatorCode": (
                        "def calculate(state):\n"
                        "    return {'first': 0.28, 'latest': 0.91, 'trend': 'up'}\n"
                    ),
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[{
                "operator_id": 204,
                "parameter_mapping": {},
            }],
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "state": "{}",
        })

        self.assertEqual(
            self._summary(result)["unknown"]["metrics"][0]["output"],
            '{"first": 0.28, "latest": 0.91, "trend": "up"}',
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_default_value_param_is_passed_to_calculate(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope(self._operator_details()))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[self._operators()[1]],
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "state": '{"quality": 8}',
        })

        self.assertEqual(self._summary(result), {
            "unknown": {
                "metrics": [
                    {
                        "metricCode": "quality_score",
                        "metricName": "质量得分",
                        "output": "9",
                        "error": "",
                    },
                ],
            },
        })

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_calculate_can_receive_metric_context(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 220,
                    "operatorNameEn": "context_metric",
                    "operatorNameCn": "上下文指标",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "ids", '
                        '"default_or_placeholder_value": "ids"}'
                        ']}'
                    ),
                    "operatorCode": (
                        "def calculate(state, ids, id_key, id_value, "
                        "start_date=None, end_date=None, helpers=None):\n"
                        "    return {\n"
                        "        'ids': ids,\n"
                        "        'id_key': id_key,\n"
                        "        'id_value': id_value,\n"
                        "        'start': str(start_date),\n"
                        "        'end': str(end_date),\n"
                        "        'fmt': helpers.fmt4(1.2300),\n"
                        "    }\n"
                    ),
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[{
                "operator_id": 220,
                "parameter_mapping": {
                    "ids": "issue_id",
                },
            }],
            ctx=self._ctx(),
            start_date_key="start",
            end_date_key="end",
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "issue_id": "123",
            "start": "2024-01-01",
            "end": "2024-01-07",
            "state": {"ad_state": [{"ad_id": "123"}]},
        })

        output = json.loads(self._summary(result)["123"]["metrics"][0]["output"])
        self.assertEqual(output["ids"], "123")
        self.assertEqual(output["id_key"], "ad_id")
        self.assertEqual(output["id_value"], "123")
        self.assertEqual(output["start"], "2024-01-01")
        self.assertEqual(output["end"], "2024-01-07")
        self.assertEqual(output["fmt"], "1.23")

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_declared_params_take_precedence_over_reserved_context_names(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 223,
                    "operatorNameEn": "declared_start_date",
                    "operatorNameCn": "显式开始日期",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "ids", '
                        '"default_or_placeholder_value": "ids"},'
                        '{"data_type": "placeholder", "key_name_en": "start_date", '
                        '"default_or_placeholder_value": "start_date"}'
                        ']}'
                    ),
                    "operatorCode": (
                        "def calculate(state, ids, start_date):\n"
                        "    return {'ids': ids, 'start_date': start_date}\n"
                    ),
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[{
                "operator_id": 223,
                "parameter_mapping": {
                    "ids": "issue_id",
                    "start_date": "mapped_start",
                },
            }],
            ctx=self._ctx(),
            start_date_key="context_start",
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "issue_id": "123",
            "mapped_start": "mapped-value",
            "context_start": "2024-01-01",
            "state": {"ad_state": [{"ad_id": "123"}]},
        })

        output = json.loads(self._summary(result)["123"]["metrics"][0]["output"])
        self.assertEqual(output["start_date"], "mapped-value")

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_invalid_context_date_is_reported_as_metric_failure(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 224,
                    "operatorNameEn": "date_metric",
                    "operatorNameCn": "日期指标",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "ids", '
                        '"default_or_placeholder_value": "ids"}'
                        ']}'
                    ),
                    "operatorCode": "def calculate(state, ids, start_date=None):\n    return str(start_date)\n",
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[{
                "operator_id": 224,
                "parameter_mapping": {
                    "ids": "issue_id",
                },
            }],
            ctx=self._ctx(),
            start_date_key="bad_start",
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "issue_id": "123",
            "bad_start": "not-a-date",
            "state": {"ad_state": [{"ad_id": "123"}]},
        })

        metric = self._summary(result)["123"]["metrics"][0]
        self.assertEqual(metric["metricCode"], "date_metric")
        self.assertEqual(metric["output"], "null")
        self.assertIn("sample.bad_start must be a YYYY-MM-DD date", metric["error"])

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_unknown_id_is_reported_as_metric_failure(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 225,
                    "operatorNameEn": "id_key_metric",
                    "operatorNameCn": "ID 类型指标",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "ids", '
                        '"default_or_placeholder_value": "ids"}'
                        ']}'
                    ),
                    "operatorCode": "def calculate(state, ids, id_key):\n    return id_key\n",
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[{
                "operator_id": 225,
                "parameter_mapping": {
                    "ids": "issue_id",
                },
            }],
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "issue_id": "999",
            "state": {"ad_state": [{"ad_id": "123"}]},
        })

        metric = self._summary(result)["999"]["metrics"][0]
        self.assertEqual(metric["metricCode"], "id_key_metric")
        self.assertEqual(metric["output"], "null")
        self.assertEqual(metric["error"], "Unknown id: 999")

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_output_id_prefers_ids_then_id_then_other_id_params(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 205,
                    "operatorNameEn": "metric_with_other_id",
                    "operatorNameCn": "其他 ID 指标",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "task_id", '
                        '"default_or_placeholder_value": "task_id"}'
                        ']}'
                    ),
                    "operatorCode": "def calculate(state):\n    return 1\n",
                },
                {
                    "id": 206,
                    "operatorNameEn": "metric_with_id",
                    "operatorNameCn": "ID 指标",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "id", '
                        '"default_or_placeholder_value": "id"}'
                        ']}'
                    ),
                    "operatorCode": "def calculate(state):\n    return 2\n",
                },
                {
                    "id": 207,
                    "operatorNameEn": "metric_with_ids",
                    "operatorNameCn": "IDS 指标",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "ids", '
                        '"default_or_placeholder_value": "ids"}'
                        ']}'
                    ),
                    "operatorCode": "def calculate(state):\n    return 3\n",
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[
                {
                    "operator_id": 205,
                    "parameter_mapping": {
                        "task_id": "task_id_field",
                    },
                },
                {
                    "operator_id": 206,
                    "parameter_mapping": {
                        "id": "id_field",
                    },
                },
                {
                    "operator_id": 207,
                    "parameter_mapping": {
                        "ids": "ids_field",
                    },
                },
            ],
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "state": "{}",
            "task_id_field": "task-1",
            "id_field": "id-1",
            "ids_field": "idsabc",
        })

        self.assertEqual(list(self._summary(result).keys()), ["idsabc"])

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_comma_separated_ids_are_calculated_as_separate_items(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 208,
                    "operatorNameEn": "ad_roi_latest",
                    "operatorNameCn": "广告最新 ROI",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "ids", '
                        '"default_or_placeholder_value": "ids"}'
                        ']}'
                    ),
                    "operatorCode": (
                        "def calculate(state, ids):\n"
                        "    adv_by_id = {item['adv_id']: item for item in state['adv_state']}\n"
                        "    return adv_by_id[ids]['adv_roi'][-1]\n"
                    ),
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[{
                "operator_id": 208,
                "parameter_mapping": {
                    "ids": "material_ids",
                },
            }],
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "material_ids": "1854751525764108, 1853671159428096",
            "state": {
                "adv_state": [
                    {
                        "adv_id": "1854751525764108",
                        "adv_roi": [0.28, 0.33],
                    },
                    {
                        "adv_id": "1853671159428096",
                        "adv_roi": [0.41, 0.52],
                    },
                ],
            },
        })

        self.assertEqual(self._summary(result), {
            "1854751525764108": {
                "metrics": [
                    {
                        "metricCode": "ad_roi_latest",
                        "metricName": "广告最新 ROI",
                        "output": "0.33",
                        "error": "",
                    },
                ],
            },
            "1853671159428096": {
                "metrics": [
                    {
                        "metricCode": "ad_roi_latest",
                        "metricName": "广告最新 ROI",
                        "output": "0.52",
                        "error": "",
                    },
                ],
            },
        })

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_id_source_key_provides_common_ids_when_metric_has_no_id_mapping(
        self,
        mock_client_cls,
    ):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 208,
                    "operatorNameEn": "id_context",
                    "operatorNameCn": "ID 上下文",
                    "inputParameter": '{"params": []}',
                    "operatorCode": (
                        "def calculate(state, id_value, id_key):\n"
                        "    return f'{id_key}:{id_value}'\n"
                    ),
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            id_source_key="issue_id",
            operators=[{
                "operator_id": 208,
                "parameter_mapping": {},
            }],
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "issue_id": "1854751525764108, 1853671159428096",
            "state": {
                "adv_state": [
                    {"adv_id": "1854751525764108"},
                    {"adv_id": "1853671159428096"},
                ],
            },
        })

        self.assertEqual(self._summary(result), {
            "1854751525764108": {
                "metrics": [
                    {
                        "metricCode": "id_context",
                        "metricName": "ID 上下文",
                        "output": "adv_id:1854751525764108",
                        "error": "",
                    },
                ],
            },
            "1853671159428096": {
                "metrics": [
                    {
                        "metricCode": "id_context",
                        "metricName": "ID 上下文",
                        "output": "adv_id:1853671159428096",
                        "error": "",
                    },
                ],
            },
        })

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_metric_id_mapping_overrides_common_id_source_key(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 208,
                    "operatorNameEn": "echo_id",
                    "operatorNameCn": "ID 回显",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "ids", '
                        '"default_or_placeholder_value": "ids"}'
                        ']}'
                    ),
                    "operatorCode": "def calculate(state, ids):\n    return ids\n",
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            id_source_key="common_ids",
            operators=[{
                "operator_id": 208,
                "parameter_mapping": {
                    "ids": "metric_ids",
                },
            }],
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "common_ids": "111",
            "metric_ids": "222,333",
            "state": {},
        })

        self.assertEqual(list(self._summary(result).keys()), ["222", "333"])

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_intermediate_items_extract_numeric_ids_from_mixed_issue_id(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 208,
                    "operatorNameEn": "echo_id",
                    "operatorNameCn": "ID 回显",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "ids", '
                        '"default_or_placeholder_value": "ids"}'
                        ']}'
                    ),
                    "operatorCode": "def calculate(state, ids):\n    return ids\n",
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[{
                "operator_id": 208,
                "parameter_mapping": {
                    "ids": "material_ids",
                },
            }],
            ctx=self._ctx(),
        )

        output = op._calculate_metric_outputs({
            RECORD_KEY_FIELD: "record-1",
            "material_ids": (
                "ad:1854751525764108, adv:1853671159428096, "
                "again:1854751525764108"
            ),
            "state": {},
        })

        self.assertEqual(
            [item["id"] for item in output["items"]],
            ["1854751525764108", "1853671159428096"],
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_intermediate_items_fallback_to_original_when_no_numeric_id(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 208,
                    "operatorNameEn": "echo_id",
                    "operatorNameCn": "ID 回显",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "ids", '
                        '"default_or_placeholder_value": "ids"}'
                        ']}'
                    ),
                    "operatorCode": "def calculate(state, ids):\n    return ids\n",
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[{
                "operator_id": 208,
                "parameter_mapping": {
                    "ids": "material_ids",
                },
            }],
            ctx=self._ctx(),
        )

        output = op._calculate_metric_outputs({
            RECORD_KEY_FIELD: "record-1",
            "material_ids": "abc_def",
            "state": {},
        })

        self.assertEqual(output["items"][0]["id"], "abc_def")

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_intermediate_items_preserve_list_id_inputs(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 208,
                    "operatorNameEn": "echo_id",
                    "operatorNameCn": "ID 回显",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "ids", '
                        '"default_or_placeholder_value": "ids"}'
                        ']}'
                    ),
                    "operatorCode": "def calculate(state, ids):\n    return ids\n",
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[{
                "operator_id": 208,
                "parameter_mapping": {
                    "ids": "material_ids",
                },
            }],
            ctx=self._ctx(),
        )

        output = op._calculate_metric_outputs({
            RECORD_KEY_FIELD: "record-1",
            "material_ids": ["123", "456"],
            "state": {},
        })

        self.assertEqual(
            [item["id"] for item in output["items"]],
            ["123", "456"],
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_id_like_parameters_from_same_field_receive_current_item_id(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 209,
                    "operatorNameEn": "metric_with_ids",
                    "operatorNameCn": "IDS 指标",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "ids", '
                        '"default_or_placeholder_value": "ids"}'
                        ']}'
                    ),
                    "operatorCode": "def calculate(state, ids):\n    return ids\n",
                },
                {
                    "id": 210,
                    "operatorNameEn": "metric_with_adv_id",
                    "operatorNameCn": "广告 ID 指标",
                    "inputParameter": (
                        '{"params": ['
                        '{"data_type": "placeholder", "key_name_en": "adv_id", '
                        '"default_or_placeholder_value": "adv_id"}'
                        ']}'
                    ),
                    "operatorCode": "def calculate(state, adv_id):\n    return adv_id\n",
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[
                {
                    "operator_id": 209,
                    "parameter_mapping": {
                        "ids": "material_ids",
                    },
                },
                {
                    "operator_id": 210,
                    "parameter_mapping": {
                        "adv_id": "material_ids",
                    },
                },
            ],
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "material_ids": "1854751525764108,1853671159428096",
            "state": {},
        })

        self.assertEqual(self._summary(result), {
            "1854751525764108": {
                "metrics": [
                    {
                        "metricCode": "metric_with_ids",
                        "metricName": "IDS 指标",
                        "output": "1854751525764108",
                        "error": "",
                    },
                    {
                        "metricCode": "metric_with_adv_id",
                        "metricName": "广告 ID 指标",
                        "output": "1854751525764108",
                        "error": "",
                    },
                ],
            },
            "1853671159428096": {
                "metrics": [
                    {
                        "metricCode": "metric_with_ids",
                        "metricName": "IDS 指标",
                        "output": "1853671159428096",
                        "error": "",
                    },
                    {
                        "metricCode": "metric_with_adv_id",
                        "metricName": "广告 ID 指标",
                        "output": "1853671159428096",
                        "error": "",
                    },
                ],
            },
        })

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

        summary = self._summary(result)
        self.assertEqual(list(summary.keys()), ["unknown"])
        self.assertEqual(
            [
                metric["metricCode"]
                for metric in summary["unknown"]["metrics"]
            ],
            ["operator_201", "operator_202"],
        )
        self.assertEqual(
            summary["unknown"]["metrics"][0]["output"],
            "null",
        )
        self.assertIn(
            "Failed to fetch state metric operators",
            summary["unknown"]["metrics"][0]["error"],
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_missing_state_only_fails_metrics_that_require_state(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 221,
                    "operatorNameEn": "needs_state",
                    "operatorNameCn": "依赖 State",
                    "inputParameter": '{"params": []}',
                    "operatorCode": "def calculate(state):\n    return state.get('value')\n",
                },
                {
                    "id": 222,
                    "operatorNameEn": "no_state",
                    "operatorNameCn": "不依赖 State",
                    "inputParameter": '{"params": []}',
                    "operatorCode": "def calculate():\n    return 'ok'\n",
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[
                {"operator_id": 221, "parameter_mapping": {}},
                {"operator_id": 222, "parameter_mapping": {}},
            ],
            ctx=self._ctx(),
        )

        result = op.process_single({RECORD_KEY_FIELD: "record-1"})

        metrics = self._summary(result)["unknown"]["metrics"]
        self.assertEqual(metrics[0]["metricCode"], "needs_state")
        self.assertEqual(metrics[0]["output"], "null")
        self.assertIn("sample.state must be provided", metrics[0]["error"])
        self.assertEqual(metrics[1]["metricCode"], "no_state")
        self.assertEqual(metrics[1]["output"], "ok")
        self.assertEqual(metrics[1]["error"], "")

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_string_metric_output_is_not_json_quoted(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope({
            "operators": [
                {
                    "id": 223,
                    "operatorNameEn": "ad_online_materials_count",
                    "operatorNameCn": "在投素材数环比",
                    "inputParameter": '{"params": []}',
                    "operatorCode": (
                        "def calculate(id_value):\n"
                        "    return f'指标名称:在投素材数（环比）, "
                        "指标值：计划ID:{id_value}：12.0000 "
                        "环比下降25.00%（上周期16.0000）'\n"
                    ),
                },
            ],
        }))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            id_source_key="issue_id",
            operators=[{"operator_id": 223, "parameter_mapping": {}}],
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "issue_id": "1234567890123456",
        })

        metric_output = (
            self._summary(result)["1234567890123456"]["metrics"][0]["output"]
        )
        self.assertEqual(
            metric_output,
            "指标名称:在投素材数（环比）, "
            "指标值：计划ID:1234567890123456：12.0000 "
            "环比下降25.00%（上周期16.0000）",
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.state_metric_calculator_mapper.HttpClient")
    def test_dataset_factory_summary_serializes_outputs_as_strings(self, mock_client_cls):
        fake_client = FakeHttpClient(success_envelope(self._operator_details()))
        mock_client_cls.return_value = fake_client
        op = StateMetricCalculatorMapper(
            operators=[self._operators()[0]],
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "material_id": "1854168911595796",
            "state": {
                "101": 0.41,
            },
        })

        metric_output = (
            self._summary(result)["1854168911595796"]["metrics"][0]["output"]
        )
        self.assertEqual(metric_output, "null")
        table = pa.Table.from_pylist([{
            "query_metric_data_outputs": result["query_metric_data_outputs"],
        }])
        self.assertEqual(table.schema.field("query_metric_data_outputs").type, pa.string())

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
                result_mode="object",
                ctx=self._ctx(),
            )
        with self.assertRaisesRegex(ValueError, "fail_policy"):
            StateMetricCalculatorMapper(
                operators=self._operators(),
                fail_policy="stop",
                ctx=self._ctx(),
            )
        with self.assertRaisesRegex(ValueError, "id_source_key"):
            StateMetricCalculatorMapper(
                operators=self._operators(),
                id_source_key="",
                ctx=self._ctx(),
            )
        with self.assertRaisesRegex(ValueError, "repartition_num_blocks"):
            StateMetricCalculatorMapper(
                operators=self._operators(),
                repartition_num_blocks=0,
                ctx=self._ctx(),
            )
        with self.assertRaisesRegex(ValueError, "repartition_num_blocks"):
            StateMetricCalculatorMapper(
                operators=self._operators(),
                repartition_num_blocks=True,
                ctx=self._ctx(),
            )

    def test_defaults_empty_optional_output_config_values(self):
        for value in (None, ""):
            op = StateMetricCalculatorMapper(
                operators=self._operators(),
                output_key=value,
                result_mode=value,
                fail_policy=value,
                ctx=self._ctx(),
            )
            self.assertEqual(op.output_key, "query_metric_data_outputs")
            self.assertEqual(op.result_mode, "summary")
            self.assertEqual(op.fail_policy, "continue")

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
                "id_source_key": None,
                "output_key": "metric_outputs",
                "result_mode": "summary",
                "fail_policy": "continue",
                "start_date_key": None,
                "end_date_key": None,
                "output_format": "dataset_factory_summary",
                "preserve_error": True,
                "runtime": "adc_operator_code",
                "operators": self._operators(),
                "repartition_num_blocks": None,
            }
        )

    @patch("data_juicer.ops.base_op.Mapper.run", autospec=True)
    def test_run_repartitions_with_explicit_num_blocks(self, mock_mapper_run):
        dataset = FakeRayDataset()
        mock_mapper_run.side_effect = lambda _, ds, **kwargs: ds
        op = StateMetricCalculatorMapper(
            operators=self._operators(),
            ctx=self._ctx(),
            repartition_num_blocks=16,
        )

        result = op.run(dataset)

        self.assertIs(result, dataset)
        self.assertEqual(dataset.repartition_calls, [{
            "num_blocks": 16,
            "shuffle": False,
        }])

    @patch("data_juicer.ops.base_op.Mapper.run", autospec=True)
    def test_run_repartitions_to_four_times_positive_num_proc_by_default(self, mock_mapper_run):
        dataset = FakeRayDataset()
        mock_mapper_run.side_effect = lambda _, ds, **kwargs: ds
        op = StateMetricCalculatorMapper(
            operators=self._operators(),
            ctx=self._ctx(),
            num_proc=5,
        )

        result = op.run(dataset)

        self.assertIs(result, dataset)
        self.assertEqual(dataset.repartition_calls, [{
            "num_blocks": 20,
            "shuffle": False,
        }])

    @patch("data_juicer.ops.base_op.Mapper.run", autospec=True)
    def test_run_does_not_repartition_for_auto_num_proc_without_explicit_blocks(self, mock_mapper_run):
        dataset = FakeRayDataset()
        mock_mapper_run.side_effect = lambda _, ds, **kwargs: ds
        op = StateMetricCalculatorMapper(
            operators=self._operators(),
            ctx=self._ctx(),
            num_proc=-1,
        )

        result = op.run(dataset)

        self.assertIs(result, dataset)
        self.assertEqual(dataset.repartition_calls, [])

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
