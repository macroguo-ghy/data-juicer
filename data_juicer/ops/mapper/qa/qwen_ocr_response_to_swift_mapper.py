from __future__ import annotations

import csv
import json
import re
from typing import Any, Mapping

from data_juicer.ops.base_op import OPERATORS, Mapper

try:
    import yaml
except ImportError:  # pragma: no cover - optional dependency
    yaml = None

OP_NAME = "qwen_ocr_response_to_swift_mapper"


DEFAULT_PROMPT_TO_INSTRUCTION = {
    "output qwenvl html directly, with style": "解析图像内容。要求按html格式输出，包含样式信息",
    "output qwenvl html directly, ignore style, attribute with data-bbox": "解析图像内容。要求按html格式输出，忽略样式信息",
    "output qwenvl html directly, ignore style, attribute without data-bbox": "解析图像内容。要求按html格式输出，忽略样式信息",
    "output qwenvl csv directly": "解析图像内容。要求按csv格式输出",
    "output qwenvl yaml directly": "解析图像内容。要求按yaml格式输出",
    "output qwenvl xml directly": "解析图像内容。要求按xml格式输出",
    "output qwenvl json directly, with all info": "解析图像内容。要求按json格式输出，包含所有信息",
    "output qwenvl json directly, with only text info": "解析图像内容。要求输出按json格式，只包含文本信息",
    "output qwenvl markdown directly": "解析图像内容。要求按markdown格式输出",
}


@OPERATORS.register_module(OP_NAME)
class QwenOcrResponseToSwiftMapper(Mapper):
    """Validate Qwen OCR responses and convert them to Swift message format."""

    _batched_op = True

    def __init__(
        self,
        prompt_key: str = "qwen_prompt",
        response_key: str = "qwen_response",
        output_messages_key: str = "messages",
        output_response_text_key: str = "qwen_response_text",
        output_format_key: str = "qwen_response_format",
        error_key: str = "qwen_postprocess_error",
        prompt_to_instruction: Mapping[str, str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.prompt_key = prompt_key
        self.response_key = response_key
        self.output_messages_key = output_messages_key
        self.output_response_text_key = output_response_text_key
        self.output_format_key = output_format_key
        self.error_key = error_key
        self.prompt_to_instruction = dict(prompt_to_instruction or DEFAULT_PROMPT_TO_INSTRUCTION)

    def process_batched(self, samples):
        row_count = self._row_count(samples)
        messages = []
        response_texts = []
        formats = []
        errors = []
        for idx in range(row_count):
            result = self._convert_single(
                prompt=samples.get(self.prompt_key, [None] * row_count)[idx],
                response=samples.get(self.response_key, [None] * row_count)[idx],
            )
            messages.append(result["messages"])
            response_texts.append(result["response_text"])
            formats.append(result["format"])
            errors.append(result["error"])

        samples[self.output_messages_key] = messages
        samples[self.output_response_text_key] = response_texts
        samples[self.output_format_key] = formats
        samples[self.error_key] = errors
        return samples

    @staticmethod
    def _row_count(samples) -> int:
        if not samples:
            return 0
        return len(samples[next(iter(samples))])

    def _convert_single(self, *, prompt: Any, response: Any) -> dict[str, Any]:
        prompt = "" if prompt is None else str(prompt)
        if prompt not in self.prompt_to_instruction:
            return self._error("unknown_prompt")
        if response is None:
            return self._error("empty_response")

        response = str(response)
        lang = self._prompt_lang(prompt)
        content = self._strip_code_block(response)
        if not self._is_valid(content, lang):
            return self._error(f"invalid_{lang or 'response'}")

        final_response = f"```{lang}\n{content}\n```" if lang else response
        return {
            "messages": [
                {"role": "user", "content": self.prompt_to_instruction[prompt]},
                {"role": "assistant", "content": final_response},
            ],
            "response_text": content,
            "format": lang or "",
            "error": "",
        }

    @staticmethod
    def _error(error: str) -> dict[str, Any]:
        return {"messages": [], "response_text": "", "format": "", "error": error}

    @staticmethod
    def _prompt_lang(prompt: str) -> str | None:
        for lang in ["html", "csv", "yaml", "xml", "json", "markdown"]:
            if lang in prompt:
                return lang
        return None

    @staticmethod
    def _strip_code_block(response: str) -> str:
        match = re.match(r"^\s*```(\w*)\s*\n(.*?)\n```\s*$", response.strip(), re.DOTALL)
        return match.group(2).strip() if match else response.strip()

    @classmethod
    def _is_valid(cls, text: str, lang: str | None) -> bool:
        if not lang:
            return True
        if lang == "html":
            return cls._is_html(text)
        if lang == "csv":
            return cls._is_csv(text)
        if lang == "yaml":
            return cls._is_yaml(text)
        if lang == "xml":
            return cls._is_xml(text)
        if lang == "json":
            return cls._is_json(text)
        if lang == "markdown":
            return True
        return False

    @staticmethod
    def _is_html(text: str) -> bool:
        stack = []
        void_elements = {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }
        tag_regex = re.compile(r"<(/?)(\w+)(?:\s+[^>]*)?>", re.DOTALL)
        for is_closing, tag_name in tag_regex.findall(text):
            tag_name = tag_name.lower()
            if tag_name in void_elements:
                continue
            if not is_closing:
                stack.append(tag_name)
            elif not stack or stack.pop() != tag_name:
                return False
        return not stack

    @staticmethod
    def _is_csv(text: str) -> bool:
        try:
            list(csv.reader(text.splitlines()))
            return True
        except (csv.Error, TypeError):
            return False

    @staticmethod
    def _is_yaml(text: str) -> bool:
        if yaml is None:
            return True
        try:
            yaml.safe_load(text)
            return True
        except Exception:
            lines = text.strip().split("\n")
            if not lines:
                return False
            return any(": " in line for line in lines) or any(line.strip().startswith("- ") for line in lines)

    @staticmethod
    def _is_xml(text: str) -> bool:
        pattern = re.compile(r"^\s*(?:<\?xml[^>]*\?>\s*)?<([^> ]+)[^>]*>.*</\1>\s*$", re.DOTALL)
        return bool(pattern.match(text.strip()))

    @staticmethod
    def _is_json(text: str) -> bool:
        try:
            json.loads(text)
            return True
        except json.JSONDecodeError:
            return False
