from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from data_juicer.core.data import NestedDataset
from data_juicer.ops.base_op import OPERATORS, Pipeline

OP_NAME = "ocr_answer_category_mapper"

TYPE_NAME_MAP = {
    "无": "simple_extract",
    "数值计算与校验": "numerical_calculation",
    "指令式区域定位与KIE": "key_information_extraction",
    "长文档理解": "long_document_understanding",
    "图表语义理解": "chart_understanding",
    "版面结构解析": "layout_analysis",
}


def _robust_match(target: str, candidates: Any) -> str | None:
    if candidates is None:
        return None
    if isinstance(candidates, dict):
        iterable = candidates.keys()
    elif isinstance(candidates, str):
        iterable = [candidates]
    else:
        iterable = candidates
    for key in iterable:
        if target in str(key) and (not isinstance(candidates, dict) or candidates[key]):
            return key
    return None


def _build_messages(match_key: str, qa: Any) -> list[dict[str, str]]:
    if not isinstance(qa, dict):
        return []
    qa_key = _robust_match(match_key, qa)
    if not qa_key or not isinstance(qa.get(qa_key), dict):
        return []
    qa_payload = qa[qa_key]
    messages = []
    for suffix in ["", "1", "2", "3"]:
        question = qa_payload.get(f"question{suffix}")
        answer = qa_payload.get(f"answer{suffix}")
        if question and answer is not None:
            messages.append({"role": "user", "content": str(question)})
            messages.append({"role": "assistant", "content": str(answer)})
    return messages


def parse_answer_categories(answer: Any) -> tuple[list[str], dict[str, list[dict[str, str]]]]:
    if answer is None:
        return [], {}
    if isinstance(answer, bytes):
        answer = answer.decode("utf-8", errors="ignore")
    if isinstance(answer, str):
        try:
            answer = json.loads(answer)
        except json.JSONDecodeError:
            return [], {}
    if not isinstance(answer, dict):
        return [], {}

    labels = answer.get("labels", answer.get("label", []))
    categories = []
    if _robust_match("版面", labels):
        categories.append("版面结构解析")
    if _robust_match("数值", labels):
        categories.append("数值计算与校验")
    if _robust_match("区域", labels):
        categories.append("指令式区域定位与KIE")
    if _robust_match("文档", labels):
        categories.append("长文档理解")
    if _robust_match("图表", labels):
        categories.append("图表语义理解")

    qa = answer.get("qa")
    type2messages = {
        "数值计算与校验": _build_messages("数值", qa),
        "指令式区域定位与KIE": _build_messages("区域", qa),
        "长文档理解": _build_messages("文档", qa),
        "图表语义理解": _build_messages("图表", qa),
        "版面结构解析": _build_messages("版面", qa),
    }
    return categories, type2messages


@OPERATORS.register_module(OP_NAME)
class OcrAnswerCategoryMapper(Pipeline):
    """Expand Seed OCR category answers into one row per category."""

    def __init__(
        self,
        answer_key: str = "ocr_answer",
        type_key: str = "ocr_type",
        type_en_key: str = "ocr_type_en",
        messages_key: str = "messages",
        default_type: str = "无",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.answer_key = answer_key
        self.type_key = type_key
        self.type_en_key = type_en_key
        self.messages_key = messages_key
        self.default_type = default_type

    def run(self, dataset, *, exporter=None, tracer=None):
        if isinstance(dataset, NestedDataset):
            return dataset.map(
                self.process_batched,
                batched=True,
                batch_size=self.batch_size,
                num_proc=self.runtime_np(),
                remove_columns=list(dataset.column_names),
                desc=self._name + "_process",
            )

        return dataset.map_batches(
            self.process_batched,
            batch_format="pyarrow",
            batch_size=self.batch_size,
        )

    def process_batched(self, samples):
        input_schema = samples.schema if isinstance(samples, pa.Table) else None
        rows = samples.to_pylist() if isinstance(samples, pa.Table) else self._dict_batch_to_rows(samples)
        output_rows = []
        for row in rows:
            output_rows.extend(self.process_single(row))
        if isinstance(samples, pa.Table):
            return self._rows_to_arrow_table(output_rows, input_schema)
        if not output_rows:
            return {key: [] for key in self._output_keys(samples.keys() if samples else [])}
        return {key: [row.get(key) for row in output_rows] for key in self._output_keys(samples.keys())}

    def process_single(self, sample: dict[str, Any]) -> list[dict[str, Any]]:
        categories, type2messages = parse_answer_categories(sample.get(self.answer_key))
        if not categories:
            categories = [self.default_type]

        rows = []
        for category in categories:
            row = dict(sample)
            row[self.type_key] = category
            row[self.type_en_key] = TYPE_NAME_MAP.get(category, category)
            row[self.messages_key] = type2messages.get(category, [])
            rows.append(row)
        return rows

    @staticmethod
    def _dict_batch_to_rows(samples) -> list[dict[str, Any]]:
        keys = list(samples.keys())
        if not keys:
            return []
        return [{key: samples[key][idx] for key in keys} for idx in range(len(samples[keys[0]]))]

    def _output_keys(self, input_keys) -> list[str]:
        keys = list(input_keys)
        for key in [self.type_key, self.type_en_key, self.messages_key]:
            if key not in keys:
                keys.append(key)
        return keys

    def _rows_to_arrow_table(self, rows: list[dict[str, Any]], input_schema: pa.Schema | None) -> pa.Table:
        input_names = input_schema.names if input_schema is not None else (list(rows[0].keys()) if rows else [])
        arrays = []
        fields = []
        for key in self._output_keys(input_names):
            values = [row.get(key) for row in rows]
            arrow_type = self._arrow_type_for_key(key, values, input_schema)
            arrays.append(pa.array(values, type=arrow_type))
            fields.append(pa.field(key, arrow_type))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def _arrow_type_for_key(self, key: str, values: list[Any], input_schema: pa.Schema | None) -> pa.DataType:
        if key in {self.type_key, self.type_en_key}:
            return pa.string()
        if key == self.messages_key:
            return pa.list_(pa.struct([pa.field("role", pa.string()), pa.field("content", pa.string())]))
        if input_schema is not None:
            field_index = input_schema.get_field_index(key)
            if field_index >= 0 and not pa.types.is_null(input_schema.field(field_index).type):
                return input_schema.field(field_index).type
        inferred = pa.array(values)
        if pa.types.is_null(inferred.type):
            return pa.string()
        return inferred.type
