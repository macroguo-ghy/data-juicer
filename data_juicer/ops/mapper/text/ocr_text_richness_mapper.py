from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from data_juicer.core.data import NestedDataset
from data_juicer.ops.base_op import OPERATORS, Pipeline

OP_NAME = "ocr_text_richness_mapper"


def _points_to_bbox(points: Any) -> list[float]:
    points = points or []
    xs = []
    ys = []
    for point in points:
        if not isinstance(point, dict):
            continue
        try:
            xs.append(float(point.get("x", 0.0)))
            ys.append(float(point.get("y", 0.0)))
        except (TypeError, ValueError):
            continue
    if not xs or not ys:
        return []
    return [min(xs), min(ys), max(xs), max(ys)]


def _parse_ocr_payload(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, (list, tuple)):
        value = value[0] if value else None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    if isinstance(value, str):
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}
    return value if isinstance(value, dict) else {}


def calculate_text_richness_score(
    ocr_info_dict: dict[str, Any],
    char_max: int = 300,
    area_max_ratio: float = 0.6,
) -> tuple[float, float, float]:
    words = ocr_info_dict.get("words") or []
    if not isinstance(words, list):
        return 0.0, 0.0, 0.0

    char_count = sum(len(str(word.get("text", ""))) for word in words if isinstance(word, dict))
    if char_count == 0:
        return 0.0, 0.0, 0.0

    char_score = min(char_count / char_max, 1.0) if char_max > 0 else 1.0
    try:
        ocr_area_ratio = float(ocr_info_dict.get("ocr_area_ratio", 0.0) or 0.0)
    except (TypeError, ValueError):
        ocr_area_ratio = 0.0
    area_score = min(ocr_area_ratio / area_max_ratio, 1.0) if area_max_ratio > 0 else 1.0
    return 5 * (char_score * area_score) ** 0.5, char_score, area_score


@OPERATORS.register_module(OP_NAME)
class OcrTextRichnessMapper(Pipeline):
    """Parse OCR JSON and add text-richness features."""

    def __init__(
        self,
        ocr_result_key: str = "ocr_result",
        text_richness_score_key: str = "text_richness_score",
        char_score_key: str = "char_score",
        area_score_key: str = "area_score",
        ocr_text_key: str = "ocr_text",
        ocr_bbox_key: str = "ocr_bbox",
        char_max: int = 300,
        area_max_ratio: float = 0.6,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.ocr_result_key = ocr_result_key
        self.text_richness_score_key = text_richness_score_key
        self.char_score_key = char_score_key
        self.area_score_key = area_score_key
        self.ocr_text_key = ocr_text_key
        self.ocr_bbox_key = ocr_bbox_key
        self.char_max = char_max
        self.area_max_ratio = area_max_ratio

    def run(self, dataset, *, exporter=None, tracer=None):
        if isinstance(dataset, NestedDataset):
            return dataset.map(
                self.process_batched,
                batched=True,
                batch_size=self.batch_size,
                num_proc=self.runtime_np(),
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
        output_rows = [self.process_single(row) for row in rows]
        if isinstance(samples, pa.Table):
            return self._rows_to_arrow_table(output_rows, input_schema)
        if not output_rows:
            return {key: [] for key in self._output_keys(samples.keys() if samples else [])}
        return {key: [row.get(key) for row in output_rows] for key in self._output_keys(samples.keys())}

    def process_single(self, sample: dict[str, Any]) -> dict[str, Any]:
        row = dict(sample)
        payload = _parse_ocr_payload(row.get(self.ocr_result_key))
        score, char_score, area_score = calculate_text_richness_score(
            payload,
            char_max=self.char_max,
            area_max_ratio=self.area_max_ratio,
        )
        texts = []
        bboxes = []
        for word in payload.get("words") or []:
            if not isinstance(word, dict):
                continue
            texts.append(str(word.get("text", "")))
            bboxes.append(_points_to_bbox(word.get("det_points_relative") or word.get("det_points_abs")))
        row[self.text_richness_score_key] = score
        row[self.char_score_key] = char_score
        row[self.area_score_key] = area_score
        row[self.ocr_text_key] = texts
        row[self.ocr_bbox_key] = bboxes
        return row

    @staticmethod
    def _dict_batch_to_rows(samples) -> list[dict[str, Any]]:
        keys = list(samples.keys())
        if not keys:
            return []
        return [{key: samples[key][idx] for key in keys} for idx in range(len(samples[keys[0]]))]

    def _output_keys(self, input_keys) -> list[str]:
        keys = list(input_keys)
        for key in [
            self.text_richness_score_key,
            self.char_score_key,
            self.area_score_key,
            self.ocr_text_key,
            self.ocr_bbox_key,
        ]:
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
        if key in {self.text_richness_score_key, self.char_score_key, self.area_score_key}:
            return pa.float64()
        if key == self.ocr_text_key:
            return pa.list_(pa.string())
        if key == self.ocr_bbox_key:
            return pa.list_(pa.list_(pa.float64()))
        if input_schema is not None:
            field_index = input_schema.get_field_index(key)
            if field_index >= 0 and not pa.types.is_null(input_schema.field(field_index).type):
                return input_schema.field(field_index).type
        inferred = pa.array(values)
        if pa.types.is_null(inferred.type):
            return pa.string()
        return inferred.type
