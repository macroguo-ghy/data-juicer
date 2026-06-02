import json
import unittest
from pathlib import Path
from unittest.mock import ANY, patch

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.standard_dataset_assembler_mapper import (
    CONFIG_PAGE_KEY,
    NEED_CTX,
    OP_NAME,
    OP_DISPLAY_NAME,
    StandardDatasetAssemblerMapper,
)
from data_juicer.utils.operator_execution_callback_utils import RECORD_KEY_FIELD


class StandardDatasetAssemblerMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    def setUp(self):
        self.callback_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.standard_dataset_assembler_mapper."
            "OperatorExecutionCallbackClient"
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
            "synthesisInstanceId": 10001,
            "flowInstanceId": 20001,
            "flowNodeId": "node_standard_dataset_assembler",
            "taskId": 30001,
            "taskVersion": 1,
            "operatorIndex": 1,
            "operatorName": "standard_dataset_assembler_mapper",
            "operatorType": "business",
            "apiBase": "https://ai-data-center.bytedance.net/api",
            "spaceId": 1,
        }

    def test_assembles_standard_dataset_sample_and_reports_record_success(self):
        op = StandardDatasetAssemblerMapper(
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )
        source_sample = {
            RECORD_KEY_FIELD: "record-1",
            "meta_info/scenario/scenario_id": "scene-1",
            "input/user_query": "用户问题",
            "context/memory/user_profile": "高意向用户",
            "reference/content": "参考答案",
            "unused": "removed",
        }
        dataset = Dataset.from_list([source_sample])

        result = op.run(dataset).to_list()

        output_sample = result[0]
        self.assertEqual(set(output_sample.keys()), {
            "meta_info",
            "input",
            "context",
            "reference",
        })
        self.assertEqual(output_sample["meta_info"]["scenario"]["scenario_id"], "scene-1")
        self.assertIn("fingerprint", output_sample["meta_info"])
        self.assertIn("unique_id", output_sample["meta_info"])
        self.assertEqual(output_sample["input"], {"user_query": "用户问题"})
        self.assertEqual(
            json.loads(output_sample["context"]["memory"]["user_profile"]),
            {"profiles": {"user_profile": "高意向用户"}},
        )
        self.assertEqual(output_sample["reference"], {"content": "参考答案"})

        self.mock_callback.report_record_success.assert_called_once_with(
            record_key=None,
            fallback_record_key="record-1",
            input_data=source_sample,
            output_data=output_sample,
            started_at=ANY,
        )

    def test_missing_ctx_does_not_block_processing_or_report_callbacks(self):
        op = StandardDatasetAssemblerMapper(ctx=None)

        result = op.process_single({
            RECORD_KEY_FIELD: "record-1",
            "input/user_query": "用户问题",
        })

        self.assertEqual(result["input"], {"user_query": "用户问题"})
        self.mock_callback_cls.assert_not_called()

    def test_assembler_failure_reports_record_failure_and_reraises(self):
        op = StandardDatasetAssemblerMapper(ctx=self._ctx())
        sample = {
            RECORD_KEY_FIELD: "record-1",
            "rubrics/rubric1/dimensions": "维度-1分-1-定义是为1",
        }

        with self.assertRaisesRegex(ValueError, "meta 缺少 en_name"):
            op.process_single(sample)

        self.mock_callback.report_record_failure.assert_called_once_with(
            record_key="record-1",
            input_data=sample,
            output_data=None,
            error_message="rubric1 的 meta 缺少 en_name，无法生成 dimensions.en_name",
            started_at=ANY,
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.standard_dataset_assembler_mapper.logger")
    def test_missing_record_key_keeps_result_and_logs_callback_failure(self, mock_logger):
        op = StandardDatasetAssemblerMapper(ctx=self._ctx())

        result = op.process_single({
            "input/user_query": "用户问题",
        })

        self.assertEqual(result["input"], {"user_query": "用户问题"})
        self.mock_callback.report_record_success.assert_not_called()
        self.assertEqual(mock_logger.warning.call_count, 1)
        message, exc = mock_logger.warning.call_args.args
        self.assertEqual(message, "Failed to report record success callback: {}")
        self.assertIsInstance(exc, ValueError)
        self.assertEqual(str(exc), f"sample.{RECORD_KEY_FIELD} must be provided")

    def test_config_loads_operator_name_without_ad_ai_data_center_prefix(self):
        config_path = Path("/private/tmp/standard_dataset_assembler_mapper_config_test.yaml")
        config_path.write_text(
            """
project_name: test_standard_dataset_assembler_mapper
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - standard_dataset_assembler_mapper:
      ctx:
        userAccount: "wangjianda.667"
        synthesisInstanceId: 10001
        taskId: 30001
        taskVersion: 1
        operatorIndex: 1
        operatorName: "standard_dataset_assembler_mapper"
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

        self.assertEqual(OP_NAME, "standard_dataset_assembler_mapper")
        self.assertEqual(OP_DISPLAY_NAME, "标准数据集组装")
        self.assertEqual(CONFIG_PAGE_KEY, "standard_dataset_assembler_builder")
        self.assertEqual(NEED_CTX, True)
        self.assertIsInstance(ops[0], StandardDatasetAssemblerMapper)
        self.assertEqual(ops[0].ctx["userAccount"], "wangjianda.667")


if __name__ == "__main__":
    unittest.main()
