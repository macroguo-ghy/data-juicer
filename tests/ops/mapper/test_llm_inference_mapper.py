import unittest
from pathlib import Path
from unittest.mock import ANY, call, patch

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops import base_op
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper import (
    CONFIG_PAGE_KEY,
    NEED_CTX,
    OP_NAME,
    LLMInferenceMapper,
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


class LLMInferenceMapperTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._original_free_models = base_op.free_models
        base_op.free_models = lambda: None

    @classmethod
    def tearDownClass(cls):
        base_op.free_models = cls._original_free_models

    def setUp(self):
        self.notification_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.send_test_card_notification"
        )
        self.mock_send_test_card_notification = self.notification_patcher.start()
        self.callback_patcher = patch(
            "data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.OperatorExecutionCallbackClient"
        )
        self.mock_callback_cls = self.callback_patcher.start()
        self.mock_callback = self.mock_callback_cls.return_value
        self.mock_callback.start.return_value = 10001

    def tearDown(self):
        self.callback_patcher.stop()
        self.notification_patcher.stop()

    @staticmethod
    def _ctx():
        return {
            "userAccount": "wangjianda.667",
            "x-tt-env": "ppe_sirius2",
            "x-use-ppe": "1",
            "synthesisInstanceId": 10001,
            "flowInstanceId": 20001,
            "flowNodeId": "node_llm",
            "taskId": 30001,
            "taskVersion": 1,
            "operatorIndex": 1,
            "operatorName": "llm_inference_mapper",
            "operatorType": "business",
            "apiBase": "https://ai-data-center.bytedance.net/api",
            "spaceId": 1,
        }

    @staticmethod
    def _submit_data():
        return {
            "taskId": "task-001",
            "conversationId": "conv-001",
            "requestId": "req-001",
        }

    @staticmethod
    def _success_result_data():
        return {
            "finished": True,
            "success": True,
            "resultStatus": "SUCCESS",
            "status": "success",
            "output": {"summary": "hello summary"},
            "message": None,
            "taskId": "task-001",
            "conversationId": "conv-001",
            "requestId": "req-001",
        }

    def test_declares_operator_metadata(self):
        self.assertEqual(OP_NAME, "llm_inference_mapper")
        self.assertEqual(CONFIG_PAGE_KEY, "llm_state_generator")
        self.assertEqual(NEED_CTX, True)

    def test_config_loads_operator_name_without_ad_ai_data_center_prefix(self):
        config_path = Path("/private/tmp/llm_inference_mapper_config_test.yaml")
        config_path.write_text(
            """
project_name: test_llm_inference_mapper
dataset_path: /private/tmp/not-used.jsonl
export_path: /private/tmp/out.jsonl
process:
  - llm_inference_mapper:
      ctx:
        userAccount: "wangjianda.667"
        synthesisInstanceId: 10001
        taskId: 30001
        taskVersion: 1
        operatorIndex: 1
        operatorName: "llm_inference_mapper"
        operatorType: "business"
        apiBase: "https://ai-data-center.bytedance.net/api"
      prompt_template: "请总结：{{ text }}"
      model: "doubao"
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

        self.assertIsInstance(ops[0], LLMInferenceMapper)
        self.assertEqual(ops[0].ctx["userAccount"], "wangjianda.667")
        self.assertIsNone(ops[0].metadata_field)
        self.assertEqual(ops[0].poll_interval_seconds, 2.0)
        self.assertEqual(ops[0].max_poll_attempts, 300)
        self.assertEqual(ops[0].retry_attempts, 3)
        self.assertIsNone(ops[0].repartition_num_blocks)

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_submits_prompt_from_template_polls_result_and_writes_output(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        running_client = FakeHttpClient(success_envelope({
            "finished": False,
            "success": None,
            "resultStatus": "RUNNING",
            "status": "executing",
            "output": None,
            "message": None,
            "taskId": "task-001",
            "conversationId": "conv-001",
            "requestId": "req-001",
        }))
        success_client = FakeHttpClient(success_envelope(self._success_result_data()))
        mock_client_cls.side_effect = [submit_client, running_client, success_client]
        dataset = Dataset.from_list([{
            "text": "long text",
            RECORD_KEY_FIELD: "record-1",
            ADC_LOG_ID_FIELD: "log-001",
        }])
        op = LLMInferenceMapper(
            prompt_template="请总结：{{ text }}",
            model="doubao",
            ctx=self._ctx(),
            poll_interval_seconds=0,
            auto_op_parallelism=False,
        )

        result = op.run(dataset).to_list()

        expected_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "user-account": "wangjianda.667",
            "space-id": "1",
            "x-tt-env": "ppe_sirius2",
            "x-use-ppe": "1",
            "x-tt-logid": "log-001",
        }
        self.assertEqual(
            mock_client_cls.call_args_list,
            [
                call(
                    endpoint=(
                        "https://ai-data-center.bytedance.net/api/"
                        "openapi/synthesis/llm-inference/submit"
                    ),
                    method="POST",
                    headers=expected_headers,
                    timeout=30.0,
                    retry_attempts=3,
                ),
                call(
                    endpoint=(
                        "https://ai-data-center.bytedance.net/api/"
                        "openapi/synthesis/llm-inference/result"
                    ),
                    method="POST",
                    headers=expected_headers,
                    timeout=30.0,
                    retry_attempts=3,
                ),
                call(
                    endpoint=(
                        "https://ai-data-center.bytedance.net/api/"
                        "openapi/synthesis/llm-inference/result"
                    ),
                    method="POST",
                    headers=expected_headers,
                    timeout=30.0,
                    retry_attempts=3,
                ),
            ],
        )
        self.assertEqual(submit_client.requests, [{
            "json_body": {
                "systemPrompt": "你是一个数据合成助手。",
                "userPrompt": "请总结：long text",
                "model": "doubao",
            }
        }])
        self.assertEqual(running_client.requests, [{"json_body": {"taskId": "task-001"}}])
        self.assertEqual(success_client.requests, [{"json_body": {"taskId": "task-001"}}])
        self.assertEqual(result[0]["llm_output"], '{"summary": "hello summary"}')
        self.assertNotIn("llm_metadata", result[0])
        self.mock_callback.report_record_success.assert_called_once_with(
            record_key="record-1",
            input_data={
                "text": "long text",
                RECORD_KEY_FIELD: "record-1",
                ADC_LOG_ID_FIELD: "log-001",
            },
            output_data=result[0],
            started_at=ANY,
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_submits_system_and_user_prompt_payload(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        success_client = FakeHttpClient(success_envelope(self._success_result_data()))
        mock_client_cls.side_effect = [submit_client, success_client]
        dataset = Dataset.from_list([{
            "state_template": {"ad_state": [{"ad_roi": "ROI"}]},
            "input": {"user_query": "怎么提升 ROI"},
            RECORD_KEY_FIELD: "record-1",
        }])
        op = LLMInferenceMapper(
            system_prompt="你是广告诊断专家：{{ input.user_query }}",
            user_prompt="模板：{{ state_template }}",
            model="doubao",
            ctx=self._ctx(),
            poll_interval_seconds=0,
            auto_op_parallelism=False,
        )

        op.run(dataset).to_list()

        self.assertEqual(submit_client.requests, [{
            "json_body": {
                "systemPrompt": "你是广告诊断专家：怎么提升 ROI",
                "userPrompt": '模板：{"ad_state": [{"ad_roi": "ROI"}]}',
                "model": "doubao",
            }
        }])

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_prompt_field_takes_precedence_and_sends_empty_model_by_default(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        success_client = FakeHttpClient(success_envelope(self._success_result_data()))
        mock_client_cls.side_effect = [submit_client, success_client]
        op = LLMInferenceMapper(
            prompt_field="prompt",
            prompt_template="ignored {{ text }}",
            ctx=self._ctx(),
            poll_interval_seconds=0,
        )

        op.process_single({
            "prompt": "direct prompt",
            "text": "text",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(submit_client.requests[0]["json_body"], {
            "systemPrompt": "你是一个数据合成助手。",
            "userPrompt": "direct prompt",
            "model": "",
        })

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_rejects_missing_space_id_before_llm_request(self, mock_client_cls):
        ctx = self._ctx()
        ctx.pop("spaceId")
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=ctx,
            poll_interval_seconds=0,
        )

        with self.assertRaisesRegex(ValueError, "ctx.spaceId"):
            op.process_single({RECORD_KEY_FIELD: "record-1"})

        mock_client_cls.assert_not_called()

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_string_output_is_preserved_and_metadata_is_stringified(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        success_data = self._success_result_data()
        success_data["output"] = "plain text summary"
        success_client = FakeHttpClient(success_envelope(success_data))
        mock_client_cls.side_effect = [submit_client, success_client]
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            poll_interval_seconds=0,
        )

        result = op.process_single({RECORD_KEY_FIELD: "record-1"})

        self.assertEqual(result["llm_output"], "plain text summary")
        self.assertNotIn("llm_metadata", result)

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_writes_metadata_only_when_metadata_field_is_configured(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        success_data = self._success_result_data()
        success_data["output"] = "plain text summary"
        success_client = FakeHttpClient(success_envelope(success_data))
        mock_client_cls.side_effect = [submit_client, success_client]
        op = LLMInferenceMapper(
            prompt="static prompt",
            metadata_field="llm_metadata",
            ctx=self._ctx(),
            poll_interval_seconds=0,
        )

        result = op.process_single({RECORD_KEY_FIELD: "record-1"})

        self.assertEqual(result["llm_output"], "plain text summary")
        self.assertEqual(
            result["llm_metadata"],
            (
                '{"taskId": "task-001", "conversationId": "conv-001", '
                '"requestId": "req-001", "resultStatus": "SUCCESS", "status": "success"}'
            ),
        )

    def test_rejects_missing_prompt_template_field(self):
        op = LLMInferenceMapper(
            prompt_template="请总结：{{ missing }}",
            ctx=self._ctx(),
        )

        with self.assertRaisesRegex(ValueError, "missing"):
            op.process_single({RECORD_KEY_FIELD: "record-1"})

    def test_prompt_template_renders_dict_and_list_fields_as_json(self):
        op = LLMInferenceMapper(
            prompt_template="请总结对象：{{ content }}；标签：{{ tags }}",
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "content": {
                "city": "北京",
                "weather": "晴朗",
            },
            "tags": ["天气", "户外"],
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(
            prompt,
            '请总结对象：{"city": "北京", "weather": "晴朗"}；标签：["天气", "户外"]',
        )

    def test_prompt_template_resolves_nested_object_and_array_paths(self):
        op = LLMInferenceMapper(
            prompt_template="城市：{{ a.b.d }}；指标：{{ items[*].metric }}；名称：{{ items[].name }}",
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "a": {
                "b": {
                    "d": "北京",
                },
            },
            "items": [
                {
                    "metric": 1,
                    "name": "曝光",
                },
                {
                    "metric": 2,
                    "name": "点击",
                },
            ],
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(prompt, '城市：北京；指标：[1, 2]；名称：["曝光", "点击"]')

    def test_prompt_template_supports_jinja_style_placeholders(self):
        op = LLMInferenceMapper(
            prompt_template=(
                "模板：{{ state_template }}；"
                "城市：{{a.b.d}}；"
                "指标：{{ items[*].metric }}"
            ),
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "state_template": {
                "ad_state": {
                    "click_rate": "点击率",
                },
            },
            "a": {
                "b": {
                    "d": "北京",
                },
            },
            "items": [
                {
                    "metric": 1,
                },
                {
                    "metric": 2,
                },
            ],
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(
            prompt,
            '模板：{"ad_state": {"click_rate": "点击率"}}；城市：北京；指标：[1, 2]',
        )

    def test_prompt_template_supports_jinja2_if_and_for_blocks(self):
        op = LLMInferenceMapper(
            prompt_template=(
                "用户：{{ input.user_query }}\n"
                "{% if review_reason %}失败原因：{{ review_reason }}{% endif %}\n"
                "{% for item in adv_state %}计划：{{ item.adv_id }}={{ item.adv_roi }};{% endfor %}"
            ),
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "input": {"user_query": "怎么提升 ROI"},
            "review_reason": "ROI 为空",
            "adv_state": [
                {"adv_id": "1", "adv_roi": 1.2},
                {"adv_id": "2", "adv_roi": 1.5},
            ],
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertIn("用户：怎么提升 ROI", prompt)
        self.assertIn("失败原因：ROI 为空", prompt)
        self.assertIn("计划：1=1.2;计划：2=1.5;", prompt)

    def test_prompt_template_supports_tojson_cn_filter(self):
        op = LLMInferenceMapper(
            prompt_template="State：{{ state | tojson_cn }}",
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "state": {"广告": [{"roi": 1.2}]},
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(prompt, 'State：{"广告": [{"roi": 1.2}]}')

    def test_prompt_template_supports_sample_root_for_special_field_names(self):
        op = LLMInferenceMapper(
            prompt_template='历史对话：{{ sample["context/memory/chat_history"] }}',
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "context/memory/chat_history": "用户：ROI 为什么下降？",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(prompt, "历史对话：用户：ROI 为什么下降？")

    def test_user_prompt_supports_sample_root_for_special_field_names(self):
        op = LLMInferenceMapper(
            user_prompt='历史对话：{{ sample["context/memory/chat_history"] }}',
            ctx=self._ctx(),
        )

        payload = op._build_prompt_payload({
            "context/memory/chat_history": "用户：ROI 为什么下降？",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(payload["userPrompt"], "历史对话：用户：ROI 为什么下降？")

    def test_user_prompt_supports_variable_mapping(self):
        op = LLMInferenceMapper(
            user_prompt="用户问题：{{ user_query }}",
            variable_mapping={
                "user_query": "客户问题",
            },
            ctx=self._ctx(),
        )

        payload = op._build_prompt_payload({
            "客户问题": "ROI 为什么下降？",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(payload["userPrompt"], "用户问题：ROI 为什么下降？")

    def test_system_prompt_supports_variable_mapping(self):
        op = LLMInferenceMapper(
            system_prompt="历史：{{ chat_history }}",
            user_prompt="用户问题：{{ user_query }}",
            variable_mapping={
                "chat_history": "context/memory/chat_history",
                "user_query": "客户问题",
            },
            ctx=self._ctx(),
        )

        payload = op._build_prompt_payload({
            "context/memory/chat_history": "用户：ROI 为什么下降？",
            "客户问题": "怎么优化？",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(payload["systemPrompt"], "历史：用户：ROI 为什么下降？")
        self.assertEqual(payload["userPrompt"], "用户问题：怎么优化？")

    def test_prompt_template_supports_variable_mapping(self):
        op = LLMInferenceMapper(
            prompt_template="用户问题：{{ user_query }}",
            variable_mapping={
                "user_query": "客户问题",
            },
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "客户问题": "ROI 为什么下降？",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(prompt, "用户问题：ROI 为什么下降？")

    def test_variable_mapping_alias_takes_precedence_over_sample_field(self):
        op = LLMInferenceMapper(
            user_prompt="用户问题：{{ user_query }}",
            variable_mapping={
                "user_query": "客户问题",
            },
            ctx=self._ctx(),
        )

        payload = op._build_prompt_payload({
            "user_query": "普通字段",
            "客户问题": "映射字段",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(payload["userPrompt"], "用户问题：映射字段")

    def test_variable_mapping_missing_source_field_raises_clear_error(self):
        op = LLMInferenceMapper(
            user_prompt="用户问题：{{ user_query }}",
            variable_mapping={
                "user_query": "客户问题",
            },
            ctx=self._ctx(),
        )

        with self.assertRaisesRegex(
            ValueError,
            r"user_prompt missing field: variable_mapping\.user_query -> 客户问题",
        ):
            op._build_prompt_payload({
                RECORD_KEY_FIELD: "record-1",
            })

    def test_variable_mapping_alias_must_be_jinja_variable_name(self):
        invalid_aliases = [
            "user-query",
            "1query",
            "user query",
            "sample.foo",
            'sample["foo"]',
        ]

        for alias in invalid_aliases:
            with self.subTest(alias=alias):
                with self.assertRaisesRegex(
                    ValueError,
                    "variable_mapping keys must be valid Jinja variable names",
                ):
                    LLMInferenceMapper(
                        user_prompt="用户问题：{{ user_query }}",
                        variable_mapping={
                            alias: "客户问题",
                        },
                        ctx=self._ctx(),
                    )

    def test_variable_mapping_alias_supports_jinja_unicode_name(self):
        op = LLMInferenceMapper(
            user_prompt="用户问题：{{ 用户问题 }}",
            variable_mapping={
                "用户问题": "客户问题",
            },
            ctx=self._ctx(),
        )

        payload = op._build_prompt_payload({
            "客户问题": "ROI 为什么下降？",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(payload["userPrompt"], "用户问题：ROI 为什么下降？")

    def test_variable_mapping_alias_strips_surrounding_whitespace(self):
        op = LLMInferenceMapper(
            user_prompt="用户问题：{{ user_query }}",
            variable_mapping={
                " user_query ": "客户问题",
            },
            ctx=self._ctx(),
        )

        payload = op._build_prompt_payload({
            "客户问题": "ROI 为什么下降？",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(payload["userPrompt"], "用户问题：ROI 为什么下降？")

    def test_variable_mapping_rejects_duplicate_alias_after_normalization(self):
        with self.assertRaisesRegex(
            ValueError,
            "variable_mapping contains duplicate alias after normalization: user_query",
        ):
            LLMInferenceMapper(
                user_prompt="用户问题：{{ user_query }}",
                variable_mapping={
                    "user_query": "客户问题A",
                    " user_query ": "客户问题B",
                },
                ctx=self._ctx(),
            )

    def test_sample_root_takes_precedence_over_sample_field(self):
        op = LLMInferenceMapper(
            user_prompt='历史对话：{{ sample["context/memory/chat_history"] }}',
            ctx=self._ctx(),
        )

        payload = op._build_prompt_payload({
            "sample": "普通字段 sample 不作为 Jinja 根变量",
            "context/memory/chat_history": "用户：ROI 为什么下降？",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(payload["userPrompt"], "历史对话：用户：ROI 为什么下降？")

    def test_prompt_template_missing_nested_field_raises_clear_error(self):
        op = LLMInferenceMapper(
            prompt_template="用户：{{ input.user_query }}",
            ctx=self._ctx(),
        )

        with self.assertRaisesRegex(ValueError, "prompt_template missing field: input.user_query"):
            op._build_prompt({
                "input": {},
                RECORD_KEY_FIELD: "record-1",
            })

    def test_prompt_template_keeps_single_brace_text_literal(self):
        op = LLMInferenceMapper(
            prompt_template="请保留：{state_template}；替换：{{ state_template }}",
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "state_template": "STATE",
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(prompt, "请保留：{state_template}；替换：STATE")

    def test_prompt_template_stringifies_scalar_values(self):
        op = LLMInferenceMapper(
            prompt_template="数量：{{ count }}；是否有效：{{ enabled }}；空值：{{ empty }}",
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "count": 123,
            "enabled": True,
            "empty": None,
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(prompt, "数量：123；是否有效：True；空值：")

    def test_prompt_template_only_serializes_referenced_fields(self):
        op = LLMInferenceMapper(
            prompt_template="请总结：{{ text }}",
            ctx=self._ctx(),
        )

        prompt = op._build_prompt({
            "text": "hello",
            "unused": {
                "raw": b"abc",
            },
            RECORD_KEY_FIELD: "record-1",
        })

        self.assertEqual(prompt, "请总结：hello")

    def test_rejects_mixed_new_and_legacy_prompt_configs(self):
        with self.assertRaisesRegex(ValueError, "cannot be configured together"):
            LLMInferenceMapper(
                user_prompt="hello",
                prompt_template="{{ text }}",
                ctx=self._ctx(),
            )
        with self.assertRaisesRegex(ValueError, "cannot be configured together"):
            LLMInferenceMapper(
                user_prompt="hello",
                prompt_template="",
                ctx=self._ctx(),
            )

    def test_rejects_system_prompt_without_user_prompt(self):
        with self.assertRaisesRegex(ValueError, "user_prompt must be provided"):
            LLMInferenceMapper(
                system_prompt="system only",
                ctx=self._ctx(),
            )

    def test_rejects_empty_rendered_user_prompt(self):
        with self.assertRaisesRegex(ValueError, "user_prompt must be a non-empty string"):
            LLMInferenceMapper(
                user_prompt="   ",
                ctx=self._ctx(),
            )

        op = LLMInferenceMapper(
            user_prompt="{{ missing_or_empty }}",
            ctx=self._ctx(),
        )

        with self.assertRaisesRegex(ValueError, "user_prompt must be a non-empty string"):
            op._build_prompt_payload({
                "missing_or_empty": "",
                RECORD_KEY_FIELD: "record-1",
            })

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_failed_result_reports_record_failure_and_raises(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        failed_client = FakeHttpClient(success_envelope({
            "finished": True,
            "success": False,
            "resultStatus": "FAILED",
            "status": "fail",
            "output": None,
            "message": "workflow task fail, request_id=req-001",
            "taskId": "task-001",
            "conversationId": "conv-001",
            "requestId": "req-001",
        }))
        mock_client_cls.side_effect = [submit_client, failed_client]
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            poll_interval_seconds=0,
        )
        sample = {
            RECORD_KEY_FIELD: "record-1",
        }

        with self.assertRaisesRegex(ValueError, "workflow task fail"):
            op.process_single(sample)

        self.mock_callback.report_record_failure.assert_called_once_with(
            record_key="record-1",
            input_data={
                RECORD_KEY_FIELD: "record-1",
            },
            output_data=None,
            error_message="workflow task fail, request_id=req-001",
            started_at=ANY,
        )

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_success_false_takes_precedence_over_result_status_success(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        inconsistent_client = FakeHttpClient(success_envelope({
            "finished": True,
            "success": False,
            "resultStatus": "SUCCESS",
            "status": "fail",
            "output": {"summary": "should not use"},
            "message": "workflow marked failed",
            "taskId": "task-001",
        }))
        mock_client_cls.side_effect = [submit_client, inconsistent_client]
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            poll_interval_seconds=0,
        )

        with self.assertRaisesRegex(ValueError, "workflow marked failed"):
            op.process_single({RECORD_KEY_FIELD: "record-1"})

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_rejects_submit_response_without_task_id(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope({
            "conversationId": "conv-001",
            "requestId": "req-001",
        }))
        mock_client_cls.return_value = submit_client
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            poll_interval_seconds=0,
        )

        with self.assertRaisesRegex(ValueError, "taskId"):
            op.process_single({RECORD_KEY_FIELD: "record-1"})

    @patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
    def test_times_out_when_result_never_finishes(self, mock_client_cls):
        submit_client = FakeHttpClient(success_envelope(self._submit_data()))
        running_client = FakeHttpClient(success_envelope({
            "finished": False,
            "success": None,
            "resultStatus": "RUNNING",
            "status": "executing",
            "output": None,
            "message": None,
            "taskId": "task-001",
        }))
        mock_client_cls.side_effect = [submit_client, running_client, running_client]
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            poll_interval_seconds=0,
            max_poll_attempts=2,
        )

        with self.assertRaisesRegex(TimeoutError, "task-001"):
            op.process_single({RECORD_KEY_FIELD: "record-1"})

    def test_before_operator_started_sends_start_callback_without_notification(self):
        op = LLMInferenceMapper(
            prompt_template="请总结：{{ text }}",
            model="doubao",
            output_field="out",
            metadata_field="meta",
            poll_interval_seconds=3,
            max_poll_attempts=10,
            ctx=self._ctx(),
        )

        op.before_operator_started()

        self.mock_callback.start.assert_called_once_with(
            operator_config={
                "prompt_source": "prompt_template",
                "has_system_prompt": False,
                "model": "doubao",
                "output_field": "out",
                "metadata_field": "meta",
                "poll_interval_seconds": 3,
                "max_poll_attempts": 10,
                "retry_attempts": 3,
                "repartition_num_blocks": None,
            }
        )
        self.mock_send_test_card_notification.assert_not_called()

    def test_before_operator_started_reports_system_user_prompt_source(self):
        op = LLMInferenceMapper(
            system_prompt="system",
            user_prompt="user",
            model="doubao",
            ctx=self._ctx(),
        )

        op.before_operator_started()

        self.mock_callback.start.assert_called_once_with(
            operator_config={
                "prompt_source": "system_user_prompt",
                "has_system_prompt": True,
                "model": "doubao",
                "output_field": "llm_output",
                "metadata_field": None,
                "poll_interval_seconds": 2.0,
                "max_poll_attempts": 300,
                "retry_attempts": 3,
                "repartition_num_blocks": None,
            }
        )

    def test_empty_system_prompt_is_treated_as_user_prompt_source(self):
        op = LLMInferenceMapper(
            system_prompt="",
            user_prompt="user",
            ctx=self._ctx(),
        )

        op.before_operator_started()

        self.mock_callback.start.assert_called_once_with(
            operator_config={
                "prompt_source": "user_prompt",
                "has_system_prompt": False,
                "model": "",
                "output_field": "llm_output",
                "metadata_field": None,
                "poll_interval_seconds": 2.0,
                "max_poll_attempts": 300,
                "retry_attempts": 3,
                "repartition_num_blocks": None,
            }
        )

    def test_after_operator_finished_finalizes_without_finish_notification(self):
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
        )

        op.after_operator_finished(error=RuntimeError("consume failed"))

        self.mock_callback.failed.assert_called_once_with(error_message="consume failed")
        self.mock_send_test_card_notification.assert_not_called()

    def test_notification_failure_does_not_block_lifecycle_callback(self):
        self.mock_send_test_card_notification.side_effect = RuntimeError("notify down")
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
        )

        op.before_operator_started()
        op.after_operator_finished(error=None)

        self.mock_callback.start.assert_called_once()
        self.mock_callback.finalize.assert_called_once_with()

    @patch("data_juicer.ops.base_op.Mapper.run", autospec=True)
    def test_run_repartitions_with_explicit_num_blocks(self, mock_mapper_run):
        dataset = FakeRayDataset()
        mock_mapper_run.side_effect = lambda _, ds, **kwargs: ds
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            repartition_num_blocks=80,
        )

        result = op.run(dataset)

        self.assertIs(result, dataset)
        self.assertEqual(dataset.repartition_calls, [{
            "num_blocks": 80,
            "shuffle": False,
        }])

    @patch("data_juicer.ops.base_op.Mapper.run", autospec=True)
    def test_run_repartitions_to_four_times_positive_num_proc_by_default(self, mock_mapper_run):
        dataset = FakeRayDataset()
        mock_mapper_run.side_effect = lambda _, ds, **kwargs: ds
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            num_proc=10,
        )

        result = op.run(dataset)

        self.assertIs(result, dataset)
        self.assertEqual(dataset.repartition_calls, [{
            "num_blocks": 40,
            "shuffle": False,
        }])

    @patch("data_juicer.ops.base_op.Mapper.run", autospec=True)
    def test_run_does_not_repartition_for_auto_num_proc_without_explicit_blocks(self, mock_mapper_run):
        dataset = FakeRayDataset()
        mock_mapper_run.side_effect = lambda _, ds, **kwargs: ds
        op = LLMInferenceMapper(
            prompt="static prompt",
            ctx=self._ctx(),
            num_proc=-1,
        )

        result = op.run(dataset)

        self.assertIs(result, dataset)
        self.assertEqual(dataset.repartition_calls, [])

    def test_retry_attempts_positional_argument_keeps_legacy_order(self):
        op = LLMInferenceMapper(
            "static prompt",
            None,
            None,
            "",
            "llm_output",
            None,
            2.0,
            300,
            self._ctx(),
            30.0,
            5,
        )

        self.assertEqual(op.retry_attempts, 5)
        self.assertIsNone(op.repartition_num_blocks)

    def test_rejects_invalid_constructor_arguments(self):
        with self.assertRaisesRegex(ValueError, "prompt"):
            LLMInferenceMapper(ctx=self._ctx())
        with self.assertRaisesRegex(ValueError, "output_field"):
            LLMInferenceMapper(prompt="x", output_field="", ctx=self._ctx())
        with self.assertRaisesRegex(ValueError, "metadata_field"):
            LLMInferenceMapper(prompt="x", metadata_field="", ctx=self._ctx())
        with self.assertRaisesRegex(ValueError, "max_poll_attempts"):
            LLMInferenceMapper(prompt="x", max_poll_attempts=0, ctx=self._ctx())
        with self.assertRaisesRegex(ValueError, "repartition_num_blocks"):
            LLMInferenceMapper(prompt="x", repartition_num_blocks=0, ctx=self._ctx())
        with self.assertRaisesRegex(ValueError, "repartition_num_blocks"):
            LLMInferenceMapper(prompt="x", repartition_num_blocks=True, ctx=self._ctx())


if __name__ == "__main__":
    unittest.main()
