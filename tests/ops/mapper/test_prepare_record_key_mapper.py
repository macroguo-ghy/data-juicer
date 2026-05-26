import hashlib
import json
import sys
import types
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import ANY, patch

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
from data_juicer.utils.adc_record_context import ADC_LOG_ID_FIELD


def stable_hash(value):
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class CustomValue:
    def __str__(self):
        return "custom-value"


class FailingDeepCopyDict(dict):
    def __deepcopy__(self, memo):
        raise ValueError("deepcopy failed")


class FakeRayDataset:

    def __init__(self):
        self.repartition_calls = []

    def repartition(self, **kwargs):
        self.repartition_calls.append(kwargs)
        return self


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
        self.mock_callback.start.return_value = 10001
        self.log_id_patcher = patch.object(
            PrepareRecordKeyMapper,
            "_generate_log_id",
            return_value="test-log-id",
        )
        self.log_id_patcher.start()

    def tearDown(self):
        self.log_id_patcher.stop()
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
            "apiBase": "https://ai-data-center.bytedance.net/api",
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
        self.mock_callback.start.assert_called_once()
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key=result[0][RECORD_KEY_FIELD],
            input_data={
                "query": "hello",
                "answer": "world",
                "extra": "ignored",
            },
            output_data=result[0],
            started_at=ANY,
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

    def test_places_record_key_as_first_output_field(self):
        op = PrepareRecordKeyMapper(ctx=self._ctx(), auto_op_parallelism=False)
        sample = {
            "query": "hello",
            "answer": "world",
        }

        result = op.process_single(sample)

        self.assertEqual(list(result.keys())[0], RECORD_KEY_FIELD)
        self.assertEqual(list(result.keys())[1], ADC_LOG_ID_FIELD)
        self.assertEqual(list(result.keys())[2:], ["query", "answer"])
        self.assertEqual(result[ADC_LOG_ID_FIELD], "test-log-id")

    def test_places_existing_record_key_as_first_output_field(self):
        op = PrepareRecordKeyMapper(ctx=self._ctx(), auto_op_parallelism=False)
        sample = {
            "query": "hello",
            RECORD_KEY_FIELD: "existing-key",
            "answer": "world",
        }

        result = op.process_single(sample)

        self.assertEqual(list(result.keys()), [RECORD_KEY_FIELD, ADC_LOG_ID_FIELD, "query", "answer"])
        self.assertEqual(result[RECORD_KEY_FIELD], "existing-key")
        self.assertEqual(result[ADC_LOG_ID_FIELD], "test-log-id")

    def test_generates_log_id_from_record_key_and_excludes_it_from_hash_source(self):
        op = PrepareRecordKeyMapper(ctx=self._ctx(), auto_op_parallelism=False)
        sample = {
            "query": "hello",
            ADC_LOG_ID_FIELD: "stale-log-id",
        }

        result = op.process_single(sample)

        expected_key = stable_hash({"query": "hello"})
        self.assertEqual(result[RECORD_KEY_FIELD], expected_key)
        self.assertEqual(result[ADC_LOG_ID_FIELD], "test-log-id")
        self.assertNotEqual(result[ADC_LOG_ID_FIELD], "stale-log-id")

    def test_generate_log_id_uses_bytedlogid_generate_v2(self):
        self.log_id_patcher.stop()
        fake_logid = types.SimpleNamespace(generate_v2=lambda: "company-log-id")

        with patch.dict(sys.modules, {"logid": fake_logid}):
            result = PrepareRecordKeyMapper._generate_log_id()

        self.assertEqual(result, "company-log-id")
        self.log_id_patcher.start()

    def test_log_id_generation_failure_does_not_block_record_key_generation(self):
        self.log_id_patcher.stop()
        op = PrepareRecordKeyMapper(ctx=self._ctx(), auto_op_parallelism=False)

        with patch.object(PrepareRecordKeyMapper, "_generate_log_id", side_effect=ImportError("logid missing")):
            result = op.process_single({"query": "hello"})

        self.assertEqual(result[RECORD_KEY_FIELD], stable_hash({"query": "hello"}))
        self.assertEqual(result[ADC_LOG_ID_FIELD], "")
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key=result[RECORD_KEY_FIELD],
            input_data={"query": "hello"},
            output_data=result,
            started_at=ANY,
        )
        self.log_id_patcher.start()

    def test_does_not_request_bytedlogid_runtime_env_dependency(self):
        op = PrepareRecordKeyMapper(ctx=self._ctx())

        self.assertEqual(op.get_env_spec().pip_pkgs, [])

    def test_reuses_source_field_as_record_key(self):
        op = PrepareRecordKeyMapper(
            source_field="questions_id",
            ctx=self._ctx(),
            auto_op_parallelism=False,
        )
        sample = {
            "questions_id": 1854168911595796,
            "query": "hello",
        }

        result = op.process_single(sample)

        self.assertEqual(list(result.keys())[0], RECORD_KEY_FIELD)
        self.assertEqual(result[RECORD_KEY_FIELD], "1854168911595796")
        self.assertEqual(result["questions_id"], 1854168911595796)

    def test_reuse_source_field_rejects_missing_or_empty_value(self):
        op = PrepareRecordKeyMapper(source_field="questions_id", ctx=self._ctx())

        with self.assertRaisesRegex(ValueError, "sample.questions_id must be provided"):
            op.process_single({"query": "hello"})

        with self.assertRaisesRegex(ValueError, "sample.questions_id must be provided"):
            op.process_single({"questions_id": ""})

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
            started_at=ANY,
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

    def test_generates_record_key_from_date_like_values(self):
        op = PrepareRecordKeyMapper(
            source_fields=["query_date", "created_at"],
            ctx=self._ctx(),
        )
        sample = {
            "query_date": date(2026, 5, 19),
            "created_at": datetime(2026, 5, 19, 10, 30, 1),
        }

        result = op.process_single(sample)

        self.assertEqual(
            result[RECORD_KEY_FIELD],
            stable_hash({
                "query_date": "2026-05-19",
                "created_at": "2026-05-19T10:30:01",
            }),
        )

    def test_generates_record_key_from_non_json_values_with_stable_normalization(self):
        op = PrepareRecordKeyMapper(
            source_fields=["amount", "payload", "tags", "mapping", "custom"],
            ctx=self._ctx(),
        )
        sample = {
            "amount": Decimal("12.30"),
            "payload": b"hello",
            "tags": {"b", "a"},
            "mapping": {
                1: "one",
                (2, 3): "tuple-key",
            },
            "custom": CustomValue(),
        }

        result = op.process_single(sample)

        self.assertEqual(
            result[RECORD_KEY_FIELD],
            stable_hash({
                "amount": "12.30",
                "payload": "aGVsbG8=",
                "tags": ["a", "b"],
                "mapping": [
                    [
                        1,
                        "one",
                    ],
                    [
                        [
                            2,
                            3,
                        ],
                        "tuple-key",
                    ],
                ],
                "custom": "custom-value",
            }),
        )

    def test_dict_key_types_do_not_collide_when_generating_record_key(self):
        int_key_result = PrepareRecordKeyMapper._stable_hash({
            "mapping": {
                1: "value",
            },
        })
        string_key_result = PrepareRecordKeyMapper._stable_hash({
            "mapping": {
                "1": "value",
            },
        })

        self.assertNotEqual(int_key_result, string_key_result)

    def test_generates_record_key_from_opaque_objects_without_memory_address(self):
        op = PrepareRecordKeyMapper(source_fields=["opaque"], ctx=self._ctx())

        first_result = op.process_single({"opaque": object()})
        second_result = op.process_single({"opaque": object()})

        self.assertEqual(
            first_result[RECORD_KEY_FIELD],
            stable_hash({
                "opaque": "<builtins.object>",
            }),
        )
        self.assertEqual(first_result[RECORD_KEY_FIELD], second_result[RECORD_KEY_FIELD])

    def test_reports_record_failure_with_fallback_key_when_record_key_generation_fails(self):
        op = PrepareRecordKeyMapper(source_fields=["query"], ctx=self._ctx())
        sample = {"query": "hello"}

        with patch.object(
            PrepareRecordKeyMapper,
            "_stable_hash",
            side_effect=ValueError("hash failed"),
        ):
            with self.assertRaisesRegex(ValueError, "hash failed"):
                op.process_single(sample)

        self.mock_callback.report_record_failure.assert_called_once()
        failure_call = self.mock_callback.report_record_failure.call_args.kwargs
        self.assertTrue(failure_call["record_key"].startswith("prepare_record_key_failed:"))
        self.assertEqual(failure_call["input_data"], {"query": "hello"})
        self.assertIsNone(failure_call["output_data"])
        self.assertEqual(failure_call["error_message"], "hash failed")
        self.assertNotIn(RECORD_KEY_FIELD, sample)

    def test_reports_record_failure_when_initial_deepcopy_fails(self):
        op = PrepareRecordKeyMapper(source_fields=["query"], ctx=self._ctx())
        sample = FailingDeepCopyDict(query="hello")

        with self.assertRaisesRegex(ValueError, "deepcopy failed"):
            op.process_single(sample)

        self.mock_callback.report_record_failure.assert_called_once()
        failure_call = self.mock_callback.report_record_failure.call_args.kwargs
        self.assertTrue(failure_call["record_key"].startswith("prepare_record_key_failed:"))
        self.assertEqual(failure_call["input_data"], sample)
        self.assertIsNone(failure_call["output_data"])
        self.assertEqual(failure_call["error_message"], "deepcopy failed")

    def test_callback_failure_does_not_block_record_key_generation(self):
        self.mock_callback.report_record_success.side_effect = RuntimeError("callback down")
        op = PrepareRecordKeyMapper(source_fields=["query"], ctx=self._ctx())

        result = op.process_single({"query": "hello"})

        self.assertEqual(
            result[RECORD_KEY_FIELD],
            stable_hash({"query": "hello"}),
        )

    def test_before_operator_started_starts_running_once(self):
        op = PrepareRecordKeyMapper(source_fields=["query"], ctx=self._ctx())

        op.before_operator_started()
        op.before_operator_started()

        self.mock_callback.start.assert_called_once_with(
            operator_config={
                "source_field": None,
                "source_fields": ["query"],
                "overwrite": False,
                "repartition_num_blocks": None,
            }
        )

    def test_after_operator_finished_finalizes_success_or_failure(self):
        op = PrepareRecordKeyMapper(source_fields=["query"], ctx=self._ctx())

        op.after_operator_finished(error=None)
        op.after_operator_finished(error=RuntimeError("consume failed"))

        self.mock_callback.finalize.assert_called_once_with()
        self.mock_callback.failed.assert_called_once_with(
            error_message="consume failed"
        )

    def test_start_failure_does_not_cache_uninitialized_callback_client(self):
        self.mock_callback.start.side_effect = [RuntimeError("start down"), 10001]
        op = PrepareRecordKeyMapper(source_fields=["query"], ctx=self._ctx())

        first_result = op.process_single({"query": "first"})
        second_result = op.process_single({"query": "second"})

        self.assertEqual(first_result[RECORD_KEY_FIELD], stable_hash({"query": "first"}))
        self.assertEqual(second_result[RECORD_KEY_FIELD], stable_hash({"query": "second"}))
        self.assertEqual(self.mock_callback.start.call_count, 2)
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key=second_result[RECORD_KEY_FIELD],
            input_data={"query": "second"},
            output_data=second_result,
            started_at=ANY,
        )

    @patch("data_juicer.ops.base_op.Mapper.run", autospec=True)
    def test_run_repartitions_with_explicit_num_blocks(self, mock_mapper_run):
        dataset = FakeRayDataset()
        mock_mapper_run.side_effect = lambda _, ds, **kwargs: ds
        op = PrepareRecordKeyMapper(
            source_fields=["query"],
            ctx=self._ctx(),
            repartition_num_blocks=8,
        )

        result = op.run(dataset)

        self.assertIs(result, dataset)
        self.assertEqual(dataset.repartition_calls, [{
            "num_blocks": 8,
            "shuffle": False,
        }])

    @patch("data_juicer.ops.base_op.Mapper.run", autospec=True)
    def test_run_repartitions_to_four_times_positive_num_proc_by_default(self, mock_mapper_run):
        dataset = FakeRayDataset()
        mock_mapper_run.side_effect = lambda _, ds, **kwargs: ds
        op = PrepareRecordKeyMapper(
            source_fields=["query"],
            ctx=self._ctx(),
            num_proc=3,
        )

        result = op.run(dataset)

        self.assertIs(result, dataset)
        self.assertEqual(dataset.repartition_calls, [{
            "num_blocks": 12,
            "shuffle": False,
        }])

    @patch("data_juicer.ops.base_op.Mapper.run", autospec=True)
    def test_run_does_not_repartition_for_auto_num_proc_without_explicit_blocks(self, mock_mapper_run):
        dataset = FakeRayDataset()
        mock_mapper_run.side_effect = lambda _, ds, **kwargs: ds
        op = PrepareRecordKeyMapper(
            source_fields=["query"],
            ctx=self._ctx(),
            num_proc=-1,
        )

        result = op.run(dataset)

        self.assertIs(result, dataset)
        self.assertEqual(dataset.repartition_calls, [])

    def test_config_loads_operator_name_without_record_key_field(self):
        config_path = Path("/private/tmp/prepare_record_key_mapper_config_test.yaml")
        config_path.write_text(
            """
project_name: test_prepare_record_key_mapper
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - prepare_record_key_mapper:
      source_field: questions_id
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
        apiBase: "https://ai-data-center.bytedance.net/api"
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
        self.assertEqual(ops[0].source_field, "questions_id")
        self.assertIsNone(ops[0].repartition_num_blocks)
        self.assertEqual(ops[0].ctx["operatorName"], "prepare_record_key_mapper")

        with self.assertRaisesRegex(ValueError, "repartition_num_blocks"):
            PrepareRecordKeyMapper(
                source_fields=["query"],
                ctx=self._ctx(),
                repartition_num_blocks=0,
            )
        with self.assertRaisesRegex(ValueError, "repartition_num_blocks"):
            PrepareRecordKeyMapper(
                source_fields=["query"],
                ctx=self._ctx(),
                repartition_num_blocks=True,
            )


if __name__ == "__main__":
    unittest.main()
