import hashlib
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.prepare_record_key_mapper import (
    NEED_CTX,
    OP_NAME,
    RECORD_KEY_FIELD,
    PrepareRecordKeyMapper,
)


def stable_hash(value):
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class PrepareRecordKeyMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    def setUp(self):
        self.callback_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.prepare_record_key_mapper.OperatorExecutionCallbackClient"
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
            "flowNodeId": "node_prepare_record_key",
            "taskId": 30001,
            "taskVersion": 1,
            "operatorIndex": 0,
            "operatorName": "prepare_record_key_mapper",
            "operatorType": "system",
            "openapiBaseUrl": "https://ai-data-center.bytedance.net/api",
        }

    def test_generates_record_key_from_source_fields(self):
        op = PrepareRecordKeyMapper(
            source_fields=["query", "answer"],
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )
        dataset = Dataset.from_list([{
            "query": "hello",
            "answer": "world",
            "extra": "ignored",
        }])

        result = op.run(dataset).to_list()

        self.assertEqual(
            result[0][RECORD_KEY_FIELD],
            stable_hash({
                "answer": "world",
                "query": "hello",
            }),
        )
        self.mock_callback.upsert.assert_called_once()
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key=result[0][RECORD_KEY_FIELD],
            input_data={
                "query": "hello",
                "answer": "world",
                "extra": "ignored",
            },
            output_data=result[0],
        )

    def test_generates_record_key_from_sample_without_internal_fields(self):
        op = PrepareRecordKeyMapper(ctx=self._ctx(), auto_op_parallelism=False)
        sample = {
            "query": "hello",
            "ctx": {"userAccount": "wangjianda.667"},
            RECORD_KEY_FIELD: "old-key",
        }

        result = op.process_single(sample)

        self.assertEqual(
            result[RECORD_KEY_FIELD],
            "old-key",
        )

        overwrite_result = PrepareRecordKeyMapper(
            overwrite=True,
            ctx=self._ctx(),
        ).process_single(sample)
        self.assertEqual(
            overwrite_result[RECORD_KEY_FIELD],
            stable_hash({"query": "hello"}),
        )

    def test_preserves_existing_record_key_by_default(self):
        op = PrepareRecordKeyMapper(source_fields=["query"], ctx=self._ctx())
        sample = {
            "query": "hello",
            RECORD_KEY_FIELD: "existing-key",
        }

        result = op.process_single(sample)

        self.assertEqual(result[RECORD_KEY_FIELD], "existing-key")
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key="existing-key",
            input_data={
                "query": "hello",
                RECORD_KEY_FIELD: "existing-key",
            },
            output_data=result,
        )

    def test_missing_source_field_uses_none_value(self):
        op = PrepareRecordKeyMapper(source_fields=["query", "missing"], ctx=self._ctx())

        result = op.process_single({"query": "hello"})

        self.assertEqual(
            result[RECORD_KEY_FIELD],
            stable_hash({
                "missing": None,
                "query": "hello",
            }),
        )

    def test_callback_failure_does_not_block_record_key_generation(self):
        self.mock_callback.report_record_success.side_effect = RuntimeError("callback down")
        op = PrepareRecordKeyMapper(source_fields=["query"], ctx=self._ctx())

        result = op.process_single({"query": "hello"})

        self.assertEqual(
            result[RECORD_KEY_FIELD],
            stable_hash({"query": "hello"}),
        )

    def test_config_loads_operator_name_without_record_key_field(self):
        config_path = Path("/private/tmp/prepare_record_key_mapper_config_test.yaml")
        config_path.write_text(
            """
project_name: test_prepare_record_key_mapper
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - prepare_record_key_mapper:
      source_fields:
        - query
        - answer
      ctx:
        userAccount: "wangjianda.667"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        synthesisInstanceId: 10001
        flowInstanceId: 20001
        flowNodeId: "node_prepare_record_key"
        taskId: 30001
        taskVersion: 1
        operatorIndex: 0
        operatorName: "prepare_record_key_mapper"
        operatorType: "system"
        openapiBaseUrl: "https://ai-data-center.bytedance.net/api"
""",
            encoding="utf-8",
        )

        cfg = init_configs(
            args=["--config", str(config_path)],
            load_configs_only=True,
        )
        ops = load_ops(cfg.process)

        self.assertEqual(OP_NAME, "prepare_record_key_mapper")
        self.assertEqual(NEED_CTX, True)
        self.assertIsInstance(ops[0], PrepareRecordKeyMapper)
        self.assertEqual(ops[0].ctx["operatorName"], "prepare_record_key_mapper")


if __name__ == "__main__":
    unittest.main()
