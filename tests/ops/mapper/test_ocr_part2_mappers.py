import unittest
import os
import sys
import types
from unittest.mock import patch

import pyarrow as pa

_register_extension_type = pa.register_extension_type


def _register_extension_type_once(extension_type):
    try:
        _register_extension_type(extension_type)
    except pa.ArrowKeyError:
        if not extension_type.extension_name.startswith("datasets.features.features."):
            raise


pa.register_extension_type = _register_extension_type_once

from data_juicer.config.config import init_configs
from data_juicer.core.io_utils import build_arrow_schema_from_config
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.qa.qwen_ocr_response_to_swift_mapper import QwenOcrResponseToSwiftMapper
from data_juicer.ops.mapper.qa.weighted_prompt_mapper import WeightedPromptMapper
from data_juicer.ops.op_env import OPEnvManager
from data_juicer.ops.pipeline.vlm_inference_with_ray_vllm_pipeline import VLMRayVLLMEnginePipeline

pa.register_extension_type = _register_extension_type


class WeightedPromptMapperTest(unittest.TestCase):
    def test_assigns_prompts_deterministically(self):
        op = WeightedPromptMapper(prompt_weights={"a": 1, "b": 2}, seed=123)
        samples = {"id": ["1", "2", "3"]}

        first = op.process_batched({"id": list(samples["id"])})
        second = op.process_batched({"id": list(samples["id"])})

        self.assertEqual(first["qwen_prompt"], second["qwen_prompt"])
        self.assertTrue(set(first["qwen_prompt"]).issubset({"a", "b"}))

    def test_rejects_empty_or_non_positive_weights(self):
        with self.assertRaisesRegex(ValueError, "prompt_weights"):
            WeightedPromptMapper(prompt_weights={})
        with self.assertRaisesRegex(ValueError, "positive"):
            WeightedPromptMapper(prompt_weights={"a": 0})
        with self.assertRaisesRegex(ValueError, "prompt and weight"):
            WeightedPromptMapper(prompt_weights=[{"prompt": "a"}])

    def test_accepts_list_prompt_weight_config(self):
        op = WeightedPromptMapper(prompt_weights=[{"prompt": "a", "weight": "1.0"}], seed=123)

        output = op.process_batched({"id": ["1"]})

        self.assertEqual(output["qwen_prompt"], ["a"])


class QwenOcrResponseToSwiftMapperTest(unittest.TestCase):
    def test_converts_valid_json_response_to_swift_messages(self):
        op = QwenOcrResponseToSwiftMapper()
        samples = {
            "qwen_prompt": ["output qwenvl json directly, with all info"],
            "qwen_response": ['```json\n{"name": "value"}\n```'],
        }

        output = op.process_batched(samples)

        self.assertEqual(output["qwen_postprocess_error"], [""])
        self.assertEqual(output["qwen_response_format"], ["json"])
        self.assertEqual(output["qwen_response_text"], ['{"name": "value"}'])
        self.assertEqual(output["messages"][0][0]["role"], "user")
        self.assertEqual(output["messages"][0][1]["content"], '```json\n{"name": "value"}\n```')

    def test_rejects_invalid_json_response(self):
        op = QwenOcrResponseToSwiftMapper()
        samples = {
            "qwen_prompt": ["output qwenvl json directly, with all info"],
            "qwen_response": ["not json"],
        }

        output = op.process_batched(samples)

        self.assertEqual(output["messages"], [[]])
        self.assertEqual(output["qwen_response_text"], [""])
        self.assertEqual(output["qwen_postprocess_error"], ["invalid_json"])

    def test_rejects_unknown_prompt(self):
        op = QwenOcrResponseToSwiftMapper()
        output = op.process_batched({"qwen_prompt": ["unknown"], "qwen_response": ["anything"]})

        self.assertEqual(output["qwen_postprocess_error"], ["unknown_prompt"])

    def test_validates_legacy_formats(self):
        op = QwenOcrResponseToSwiftMapper()
        samples = {
            "qwen_prompt": [
                "output qwenvl html directly, with style",
                "output qwenvl yaml directly",
                "output qwenvl xml directly",
                "output qwenvl markdown directly",
                "output qwenvl html directly, with style",
            ],
            "qwen_response": [
                "<div><span>x</span></div>",
                "name: value",
                "<root>value</root>",
                "# title",
                "<div><span>x</div>",
            ],
        }

        output = op.process_batched(samples)

        self.assertEqual(output["qwen_postprocess_error"][:4], ["", "", "", ""])
        self.assertEqual(output["qwen_response_format"][:4], ["html", "yaml", "xml", "markdown"])
        self.assertEqual(output["qwen_postprocess_error"][4], "invalid_html")

    def test_rejects_empty_response(self):
        op = QwenOcrResponseToSwiftMapper()
        output = op.process_batched(
            {
                "qwen_prompt": ["output qwenvl markdown directly"],
                "qwen_response": [None],
            }
        )

        self.assertEqual(output["qwen_postprocess_error"], ["empty_response"])


class VLMRayVLLMEnginePipelineTest(unittest.TestCase):
    def test_resolve_keep_columns_uses_explicit_columns_without_fetch(self):
        op = object.__new__(VLMRayVLLMEnginePipeline)
        op.keep_columns = ["id", "images"]

        class Dataset:
            def columns(self, *args, **kwargs):  # pragma: no cover - should not be called
                raise AssertionError("columns should not be called")

        self.assertEqual(op._resolve_keep_columns(Dataset()), ["id", "images"])

    def test_resolve_keep_columns_uses_no_fetch_columns_before_fallback(self):
        op = object.__new__(VLMRayVLLMEnginePipeline)
        op.keep_columns = None

        class Dataset:
            def __init__(self):
                self.calls = []

            def columns(self, *args, **kwargs):
                self.calls.append((args, kwargs))
                if kwargs.get("fetch_if_missing") is False:
                    return ["id"]
                return ["fallback"]

        dataset = Dataset()

        self.assertEqual(op._resolve_keep_columns(dataset), ["id"])
        self.assertEqual(dataset.calls, [((), {"fetch_if_missing": False})])

    def test_run_builds_processor_lazily(self):
        captured = {}

        class Config:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        def build_llm_processor(config, *, preprocess, postprocess):
            captured["config"] = config
            captured["preprocess"] = preprocess
            captured["postprocess"] = postprocess
            return lambda dataset: {"dataset": dataset, "config": config}

        fake_llm = types.ModuleType("ray.data.llm")
        fake_llm.vLLMEngineProcessorConfig = Config
        fake_llm.build_llm_processor = build_llm_processor

        with patch("data_juicer.ops.pipeline.ray_vllm_pipeline.is_ray_mode", return_value=True):
            with patch.dict(sys.modules, {"ray.data.llm": fake_llm}):
                op = VLMRayVLLMEnginePipeline(
                    api_or_hf_model="model",
                    query_key="qwen_prompt",
                    image_key="images",
                    response_key="qwen_response",
                    keep_columns=["id", "qwen_prompt"],
                    sampling_params={"temperature": 0.1},
                    engine_kwargs={"tensor_parallel_size": 2},
                    batch_size=1,
                    num_proc=1,
                )
                output = op.run("dataset")

        self.assertEqual(output["dataset"], "dataset")
        self.assertEqual(captured["config"].model_source, "model")
        self.assertEqual(captured["config"].engine_kwargs["tensor_parallel_size"], 2)
        self.assertFalse(captured["config"].tokenize)
        self.assertFalse(captured["config"].detokenize)
        preprocessed = captured["preprocess"]({"qwen_prompt": "prompt", "images": []})
        self.assertEqual(preprocessed["messages"][0]["content"][0]["text"], "prompt\n\n")
        postprocessed = captured["postprocess"](
            {"id": "1", "qwen_prompt": "prompt", "generated_text": "answer", "extra": "drop"}
        )
        self.assertEqual(postprocessed, {"id": "1", "qwen_prompt": "prompt", "qwen_response": "answer"})

    def test_tokenize_and_detokenize_can_be_overridden(self):
        captured = {}

        class Config:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        def build_llm_processor(config, **_):
            captured["config"] = config
            return lambda dataset: dataset

        fake_llm = types.ModuleType("ray.data.llm")
        fake_llm.vLLMEngineProcessorConfig = Config
        fake_llm.build_llm_processor = build_llm_processor

        with patch("data_juicer.ops.pipeline.ray_vllm_pipeline.is_ray_mode", return_value=True):
            with patch.dict(sys.modules, {"ray.data.llm": fake_llm}):
                op = VLMRayVLLMEnginePipeline(
                    api_or_hf_model="model",
                    keep_columns=["id"],
                    tokenize=True,
                    detokenize=True,
                )
                op.run("dataset")

        self.assertTrue(captured["config"].tokenize)
        self.assertTrue(captured["config"].detokenize)

    def test_run_plan_only_adds_response_column_without_ray_llm_import(self):
        op = object.__new__(VLMRayVLLMEnginePipeline)
        op.response_key = "qwen_response"
        op.batch_size = 1

        class Dataset:
            def map_batches(self, fn, **kwargs):
                table = pa.table({"id": ["1"]})
                return fn(table, **kwargs["fn_kwargs"])

        output = op.run_plan_only(Dataset())

        self.assertIn("qwen_response", output.column_names)
        self.assertEqual(output.column("qwen_response").to_pylist(), [None])

    def test_run_plan_only_preserves_existing_response_column(self):
        table = pa.table({"id": ["1"], "qwen_response": ["answer"]})

        output = VLMRayVLLMEnginePipeline._dry_run_batch(table, response_key="qwen_response")

        self.assertIs(output, table)


class OcrPart2ConfigTest(unittest.TestCase):
    def test_main_config_loads_expected_ops_and_schema(self):
        config_path = (
            "demos/bytedance/ocr_part2/"
            "third_site_ocr_seed_main_demo1_qwen3vl30b_simple_extract_hard.yaml"
        )
        if not os.path.exists(config_path):
            self.skipTest("ByteDance OCR part2 demo configs are not included")

        cfg = init_configs(args=["--config", config_path, "--ray_address", "local"], load_configs_only=True)
        with patch("data_juicer.ops.pipeline.ray_vllm_pipeline.is_ray_mode", return_value=True):
            ops = load_ops(cfg.process, OPEnvManager(min_common_dep_num_to_combine=0))

        self.assertEqual(
            [op.__class__.__name__ for op in ops],
            [
                "SpecifiedFieldFilter",
                "RayRandomSamplePipeline",
                "WeightedPromptMapper",
                "VlmApiResponseMapper",
                "QwenOcrResponseToSwiftMapper",
                "SpecifiedFieldFilter",
                "RayFieldDeduplicator",
                "CharacterRepetitionFilter",
            ],
        )
        self.assertEqual(cfg.export["operation"], "OVERWRITE")
        self.assertEqual(cfg.export["partition_values"]["p_date"], "20260509")
        schema = build_arrow_schema_from_config(cfg.export["schema"])
        for field_name in ["qwen_prompt", "qwen_response_text", "messages", "p_date"]:
            self.assertIn(field_name, schema.names)
        self.assertEqual(ops[0].field_key, "ocr_type_en")
        self.assertEqual(ops[0].target_value, ["simple_extract"])
        self.assertEqual(ops[1].select_num, 20000)
        self.assertEqual(ops[2].output_key, "qwen_prompt")
        self.assertEqual(ops[3].model, "qwen3vl")
        self.assertEqual(ops[3].base_url, "http://[2605:340:cd51:603:7dd5:e521:38d0:fe5c]:8001/v1")
        self.assertEqual(ops[3].prompt_template, "${qwen_prompt}")
        self.assertEqual(ops[3].output_key, "qwen_response")
        self.assertEqual(ops[3].error_key, "vlm_error")
        self.assertEqual(ops[3].max_tokens, 2048)
        self.assertEqual(ops[3].extra_body["top_p"], 0.4)
        self.assertEqual(ops[3].repartition_num_blocks, 160)
        self.assertEqual(ops[4].output_messages_key, "messages")
        self.assertEqual(ops[6].field_key, "images")

    def test_sample_label_config_targets_part2_output(self):
        config_path = (
            "demos/bytedance/ocr_part2/"
            "third_site_ocr_seed_main_demo1_qwen3vl30b_simple_extract_hard_sample_label.yaml"
        )
        if not os.path.exists(config_path):
            self.skipTest("ByteDance OCR part2 demo configs are not included")

        cfg = init_configs(args=["--config", config_path, "--ray_address", "local"], load_configs_only=True)
        ops = load_ops(cfg.process, OPEnvManager(min_common_dep_num_to_combine=0))

        self.assertEqual(cfg.dataset["configs"][0]["table_name"], cfg.export["table_name"].removesuffix("_sample_labels"))
        self.assertEqual(cfg.export["operation"], "OVERWRITE")
        self.assertEqual([op.__class__.__name__ for op in ops], ["RayGroupSamplePipeline"] + ["VlmApiResponseMapper"] * 3)
        schema = build_arrow_schema_from_config(cfg.export["schema"])
        self.assertIn("qwen_response_text", schema.names)
        self.assertIn("${qwen_response_text}", cfg.process[1]["vlm_api_response_mapper"]["prompt_template"])


if __name__ == "__main__":
    unittest.main()
