import hashlib
import json
import unittest
from pathlib import Path

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.prepare_record_key_mapper import (
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

    def test_generates_record_key_from_source_fields(self):
        op = PrepareRecordKeyMapper(
            source_fields=["query", "answer"],
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

    def test_generates_record_key_from_sample_without_internal_fields(self):
        op = PrepareRecordKeyMapper(auto_op_parallelism=False)
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

        overwrite_result = PrepareRecordKeyMapper(overwrite=True).process_single(sample)
        self.assertEqual(
            overwrite_result[RECORD_KEY_FIELD],
            stable_hash({"query": "hello"}),
        )

    def test_preserves_existing_record_key_by_default(self):
        op = PrepareRecordKeyMapper(source_fields=["query"])
        sample = {
            "query": "hello",
            RECORD_KEY_FIELD: "existing-key",
        }

        result = op.process_single(sample)

        self.assertEqual(result[RECORD_KEY_FIELD], "existing-key")

    def test_missing_source_field_uses_none_value(self):
        op = PrepareRecordKeyMapper(source_fields=["query", "missing"])

        result = op.process_single({"query": "hello"})

        self.assertEqual(
            result[RECORD_KEY_FIELD],
            stable_hash({
                "missing": None,
                "query": "hello",
            }),
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
""",
            encoding="utf-8",
        )

        cfg = init_configs(
            args=["--config", str(config_path)],
            load_configs_only=True,
        )
        ops = load_ops(cfg.process)

        self.assertEqual(OP_NAME, "prepare_record_key_mapper")
        self.assertIsInstance(ops[0], PrepareRecordKeyMapper)


if __name__ == "__main__":
    unittest.main()
