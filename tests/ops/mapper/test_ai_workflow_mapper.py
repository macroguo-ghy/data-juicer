import json
import unittest
from unittest.mock import patch

from data_juicer.ops import base_op
from data_juicer.ops.mapper.ad_ai_data_center.ai_workflow_mapper import (
    ZhishangWorkflowMapper,
)


class _Response:

    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class ZhishangWorkflowMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    @staticmethod
    def _build_mapper():
        return ZhishangWorkflowMapper(
            workflow_id="workflow-1",
            token="token-1",
            input_schema={
                "input_schema": [
                    {"key": "prompt", "value": "workflow_prompt", "type": "string"},
                ],
            },
            output_schema={
                "output_schema": [
                    {"key": "answer", "value": "workflow_answer"},
                ],
            },
            auto_op_parallelism=False,
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.ai_workflow_mapper.logger")
    @patch("data_juicer.ops.mapper.ad_ai_data_center.ai_workflow_mapper.time.monotonic")
    def test_process_single_logs_elapsed_time_after_success(
        self,
        mock_monotonic,
        mock_logger,
    ):
        mock_monotonic.side_effect = [10.0, 12.3456]
        op = self._build_mapper()

        with patch.object(
            op,
            "_run_workflow",
            return_value={"workflow_answer": "done"},
        ):
            result = op.process_single({"prompt": "hello"})

        self.assertEqual(
            result,
            {
                "workflow_answer": "done",
            },
        )
        mock_logger.info.assert_called_once()
        log_args = mock_logger.info.call_args.args
        self.assertEqual(
            log_args[0],
            "Zhishang workflow mapper process_single finished in {:.3f}s. success={}",
        )
        self.assertAlmostEqual(log_args[1], 2.3456)
        self.assertTrue(log_args[2])

    @patch("data_juicer.ops.mapper.ad_ai_data_center.ai_workflow_mapper.logger")
    @patch("data_juicer.ops.mapper.ad_ai_data_center.ai_workflow_mapper.time.monotonic")
    def test_process_single_logs_elapsed_time_after_workflow_failure(
        self,
        mock_monotonic,
        mock_logger,
    ):
        mock_monotonic.side_effect = [20.0, 25.0]
        op = self._build_mapper()

        with patch.object(
            op,
            "_run_workflow",
            side_effect=ValueError("workflow failed"),
        ):
            result = op.process_single({"prompt": "hello"})

        self.assertEqual(
            result,
            {
                "workflow_answer": None,
            },
        )
        mock_logger.info.assert_called_once()
        log_args = mock_logger.info.call_args.args
        self.assertEqual(
            log_args[0],
            "Zhishang workflow mapper process_single finished in {:.3f}s. success={}",
        )
        self.assertAlmostEqual(log_args[1], 5.0)
        self.assertFalse(log_args[2])

    def test_process_single_converts_output_values_by_schema_type(self):
        op = ZhishangWorkflowMapper(
            workflow_id="workflow-1",
            token="token-1",
            input_schema={
                "input_schema": [
                    {"key": "prompt", "value": "workflow_prompt", "type": "string"},
                ],
            },
            output_schema={
                "output_schema": [
                    {"key": "identifier", "value": "doc_id", "type": "string"},
                    {"key": "count", "value": "item_count", "type": "long"},
                ],
            },
            auto_op_parallelism=False,
        )
        workflow_content = json.dumps(
            {
                "identifier": 10001,
                "count": "42",
            }
        )

        with patch(
            "data_juicer.ops.mapper.ad_ai_data_center.ai_workflow_mapper.requests.post",
            side_effect=[
                _Response({"data": {"task_id": "task-1"}}),
                _Response(
                    {
                        "data": {
                            "task_status": "success",
                            "outputs": [{"content": workflow_content}],
                        }
                    }
                ),
            ],
        ):
            result = op.process_single({"prompt": "hello"})

        self.assertEqual(
            result,
            {
                "doc_id": "10001",
                "item_count": 42,
            },
        )

    def test_process_single_returns_only_output_schema_fields_after_conversion_failure(self):
        op = ZhishangWorkflowMapper(
            workflow_id="workflow-1",
            token="token-1",
            input_schema={
                "input_schema": [
                    {"key": "prompt", "value": "workflow_prompt", "type": "string"},
                ],
            },
            output_schema={
                "output_schema": [
                    {"key": "count", "value": "item_count", "type": "long"},
                ],
            },
            auto_op_parallelism=False,
        )
        workflow_content = json.dumps({"count": "not-a-number"})

        with patch(
            "data_juicer.ops.mapper.ad_ai_data_center.ai_workflow_mapper.requests.post",
            side_effect=[
                _Response({"data": {"task_id": "task-1"}}),
                _Response(
                    {
                        "data": {
                            "task_status": "success",
                            "outputs": [{"content": workflow_content}],
                        }
                    }
                ),
            ],
        ):
            result = op.process_single({"prompt": "hello"})

        self.assertEqual(result, {"item_count": None})

    def test_process_single_treats_process_msg_as_regular_schema_field(self):
        op = ZhishangWorkflowMapper(
            workflow_id="workflow-1",
            token="token-1",
            input_schema={
                "input_schema": [
                    {"key": "prompt", "value": "workflow_prompt", "type": "string"},
                ],
            },
            output_schema={
                "output_schema": [
                    {"key": "answer", "value": "process_msg"},
                    {"key": "count", "value": "item_count", "type": "long"},
                ],
            },
            auto_op_parallelism=False,
        )
        workflow_content = json.dumps({"answer": "done", "count": "3"})

        with patch(
            "data_juicer.ops.mapper.ad_ai_data_center.ai_workflow_mapper.requests.post",
            side_effect=[
                _Response({"data": {"task_id": "task-1"}}),
                _Response(
                    {
                        "data": {
                            "task_status": "success",
                            "outputs": [{"content": workflow_content}],
                        }
                    }
                ),
            ],
        ):
            result = op.process_single({"prompt": "hello"})

        self.assertEqual(
            result,
            {
                "process_msg": "done",
                "item_count": 3,
            },
        )

    def test_unsupported_output_schema_type_fails_fast(self):
        with self.assertRaisesRegex(ValueError, "output_schema type"):
            ZhishangWorkflowMapper(
                workflow_id="workflow-1",
                token="token-1",
                input_schema={
                    "input_schema": [
                        {
                            "key": "prompt",
                            "value": "workflow_prompt",
                            "type": "string",
                        },
                    ],
                },
                output_schema={
                    "output_schema": [
                        {"key": "score", "value": "score", "type": "double"},
                    ],
                },
                auto_op_parallelism=False,
            )


if __name__ == "__main__":
    unittest.main()
