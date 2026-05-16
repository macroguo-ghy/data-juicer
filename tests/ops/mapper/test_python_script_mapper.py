import unittest
from pathlib import Path
from unittest.mock import ANY, patch

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.python_script_mapper import (
    NEED_CTX,
    OP_NAME,
    PythonScriptMapper,
)
from data_juicer.utils.operator_execution_callback_utils import RECORD_KEY_FIELD


class PythonScriptMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    def setUp(self):
        self.callback_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.python_script_mapper.OperatorExecutionCallbackClient"
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
            "flowNodeId": "node_python_script",
            "taskId": 30001,
            "taskVersion": 1,
            "operatorIndex": 1,
            "operatorName": "python_script_mapper",
            "operatorType": "business",
            "apiBase": "https://ai-data-center.bytedance.net/api",
        }

    def test_processes_sample_with_ctx_and_reports_record_success(self):
        op = PythonScriptMapper(
            python_code=(
                "def process(sample, context):\n"
                "    sample['user'] = context['ctx']['userAccount']\n"
                "    sample['value'] = sample['value'] + 1\n"
                "    return sample\n"
            ),
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )
        dataset = Dataset.from_list([{
            RECORD_KEY_FIELD: "record-1",
            "value": 1,
        }])

        result = op.run(dataset).to_list()

        self.assertEqual(
            result[0],
            {
                RECORD_KEY_FIELD: "record-1",
                "value": 2,
                "user": "wangjianda.667",
            },
        )
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key="record-1",
            input_data={
                RECORD_KEY_FIELD: "record-1",
                "value": 1,
            },
            output_data=result[0],
            started_at=ANY,
        )

    def test_script_failure_reports_record_failure_and_reraises(self):
        op = PythonScriptMapper(
            python_code=(
                "def process(sample, context):\n"
                "    raise ValueError('script failed')\n"
            ),
            ctx=self._ctx(),
        )
        sample = {
            RECORD_KEY_FIELD: "record-1",
            "value": 1,
        }

        with self.assertRaisesRegex(ValueError, "script failed"):
            op.process_single(sample)

        self.mock_callback.report_record_failure.assert_called_once_with(
            record_key="record-1",
            input_data={
                RECORD_KEY_FIELD: "record-1",
                "value": 1,
            },
            output_data=sample,
            error_message="script failed",
            started_at=ANY,
        )

    def test_callback_failure_does_not_block_script_result(self):
        self.mock_callback.report_record_success.side_effect = RuntimeError("callback down")
        op = PythonScriptMapper(
            python_code=(
                "def process(sample, context):\n"
                "    sample['value'] = sample['value'] + 1\n"
                "    return sample\n"
            ),
            ctx=self._ctx(),
        )

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "value": 1,
        })

        self.assertEqual(result["value"], 2)

    def test_before_operator_started_starts_running_once(self):
        op = PythonScriptMapper(
            python_code="def process(sample, context):\n    return sample",
            ctx=self._ctx(),
        )

        op.before_operator_started()
        op.before_operator_started()

        self.mock_callback.start.assert_called_once_with(
            operator_config={
                "entrypoint": "process",
            }
        )

    def test_after_operator_finished_finalizes_success_or_failure(self):
        op = PythonScriptMapper(
            python_code="def process(sample, context):\n    return sample",
            ctx=self._ctx(),
        )

        op.after_operator_finished(error=None)
        op.after_operator_finished(error=RuntimeError("consume failed"))

        self.mock_callback.finalize.assert_called_once_with()
        self.mock_callback.failed.assert_called_once_with(
            error_message="consume failed"
        )

    def test_config_loads_operator_name_without_ad_ai_data_center_prefix(self):
        config_path = Path("/private/tmp/python_script_mapper_config_test.yaml")
        config_path.write_text(
            """
project_name: test_python_script_mapper
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - python_script_mapper:
      ctx:
        userAccount: "wangjianda.667"
        synthesisInstanceId: 10001
        taskId: 30001
        taskVersion: 1
        operatorIndex: 1
        operatorName: "python_script_mapper"
        operatorType: "business"
        apiBase: "https://ai-data-center.bytedance.net/api"
      python_code: "def process(sample, context):\\n    return sample"
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

        self.assertEqual(OP_NAME, "python_script_mapper")
        self.assertEqual(NEED_CTX, True)
        self.assertIsInstance(ops[0], PythonScriptMapper)
        self.assertEqual(ops[0].ctx["userAccount"], "wangjianda.667")


if __name__ == "__main__":
    unittest.main()
