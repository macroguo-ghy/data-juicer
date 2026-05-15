import unittest
from pathlib import Path

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.prepare_operator_context_mapper import (
    OP_NAME,
    PrepareOperatorContextMapper,
)


class PrepareOperatorContextMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    def test_adds_ctx_to_sample(self):
        op = PrepareOperatorContextMapper(
            user_account="wangjianda.667",
            tt_env="ppe_sirius2",
            use_ppe="1",
            auto_op_parallelism=False,
        )
        dataset = Dataset.from_list([{"externalDataSet": {}}])

        result = op.run(dataset).to_list()

        self.assertEqual(
            result[0],
            {
                "externalDataSet": {},
                "ctx": {
                    "userAccount": "wangjianda.667",
                    "x-tt-env": "ppe_sirius2",
                    "x-use-ppe": "1",
                },
            },
        )

    def test_preserves_existing_ctx_values_by_default(self):
        op = PrepareOperatorContextMapper(
            user_account="new-user",
            tt_env="ppe_sirius2",
            use_ppe="1",
            auto_op_parallelism=False,
        )
        sample = {
            "ctx": {
                "userAccount": "existing-user",
                "traceId": "trace-1",
            }
        }

        result = op.process_single(sample)

        self.assertEqual(result["ctx"]["userAccount"], "existing-user")
        self.assertEqual(result["ctx"]["traceId"], "trace-1")
        self.assertEqual(result["ctx"]["x-tt-env"], "ppe_sirius2")
        self.assertEqual(result["ctx"]["x-use-ppe"], "1")

    def test_overwrites_existing_ctx_when_enabled(self):
        op = PrepareOperatorContextMapper(
            user_account="new-user",
            tt_env="ppe_sirius2",
            use_ppe="1",
            overwrite=True,
            auto_op_parallelism=False,
        )
        sample = {
            "ctx": {
                "userAccount": "existing-user",
                "x-tt-env": "prod",
            }
        }

        result = op.process_single(sample)

        self.assertEqual(
            result["ctx"],
            {
                "userAccount": "new-user",
                "x-tt-env": "ppe_sirius2",
                "x-use-ppe": "1",
            },
        )

    def test_does_not_write_optional_empty_ppe_headers(self):
        op = PrepareOperatorContextMapper(
            user_account="wangjianda.667",
            auto_op_parallelism=False,
        )

        result = op.process_single({})

        self.assertEqual(result["ctx"], {"userAccount": "wangjianda.667"})

    def test_rejects_missing_user_account(self):
        with self.assertRaisesRegex(ValueError, "user_account must be provided"):
            PrepareOperatorContextMapper()

    def test_config_loads_operator_name(self):
        config_path = Path("/private/tmp/prepare_operator_context_mapper_config_test.yaml")
        config_path.write_text(
            """
project_name: test_prepare_operator_context_mapper
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - prepare_operator_context_mapper:
      user_account: "wangjianda.667"
      tt_env: "ppe_sirius2"
      use_ppe: "1"
""",
            encoding="utf-8",
        )

        cfg = init_configs(
            args=["--config", str(config_path)],
            load_configs_only=True,
        )
        ops = load_ops(cfg.process)

        self.assertEqual(OP_NAME, "prepare_operator_context_mapper")
        self.assertIsInstance(ops[0], PrepareOperatorContextMapper)


if __name__ == "__main__":
    unittest.main()
