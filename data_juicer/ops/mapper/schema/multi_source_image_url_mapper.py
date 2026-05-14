from __future__ import annotations

import ast
import html
import json
import re
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from data_juicer.core.data import NestedDataset
from data_juicer.ops.base_op import OPERATORS, Pipeline
from data_juicer.ops.mapper.schema.passthrough_type_utils import (
    coerce_value_for_arrow_type,
    parse_arrow_type,
)

OP_NAME = "multi_source_image_url_mapper"

_BASE_OUTPUT_KEYS = [
    "id",
    "source",
    "texts",
    "image_urls",
    "extra",
]
_BASE_ARROW_TYPES = {
    "id": pa.string(),
    "source": pa.string(),
    "texts": pa.list_(pa.string()),
    "image_urls": pa.list_(pa.string()),
    "extra": pa.string(),
}
_ARROW_TYPE_ALIASES = {
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float": pa.float32(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "double": pa.float64(),
    "str": pa.string(),
    "string": pa.string(),
    "binary": pa.binary(),
}
_URL_SPLIT_RE = re.compile(r"[\s,;|]+")
_HTML_IMG_SRC_RE = re.compile(
    r"<img\b[^>]*\bsrc\s*=\s*([\"'])(.*?)\1",
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class ImageSourceSpec:
    name: str
    url_field: str
    source: str
    extra_url_key: str | None = None
    extra_url_mode: str = "list"
    max_urls: int | None = None


def _jsonable(value: Any) -> Any:
    if hasattr(value, "as_py"):
        value = value.as_py()
    if hasattr(value, "tolist"):
        return value.tolist()
    return value


def _extra_to_json(extra: dict[str, Any]) -> str:
    return json.dumps(extra, ensure_ascii=False, default=str)


@OPERATORS.register_module(OP_NAME)
class MultiSourceImageUrlMapper(Pipeline):
    """Expand multiple image URL fields into source-level image URL rows."""

    def __init__(
        self,
        source_specs: list[dict[str, Any]] | None = None,
        id_field: str | None = None,
        id_prefix: str | None = None,
        text_fields: list[str] | None = None,
        extra_keys: list[str] | None = None,
        output_url_key: str = "image_urls",
        passthrough_keys: list[str] | None = None,
        passthrough_types: dict[str, str] | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param source_specs: image source definitions. Each item requires
            `name`, `url_field`, and `source`, and may set `extra_url_key`,
            `extra_url_mode`, and `max_urls`.
        :param id_field: input field used to build output id.
        :param id_prefix: output id prefix. Defaults to `id_field`.
        :param text_fields: input fields to copy into the output `texts` list.
        :param extra_keys: input fields to keep in the output `extra` JSON.
        :param output_url_key: output field for parsed image URLs.
        :param passthrough_keys: input fields to keep as top-level output columns.
        :param passthrough_types: explicit pyarrow types for passthrough fields.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not source_specs:
            raise ValueError("source_specs must be a non-empty list")
        if not id_field:
            raise ValueError("id_field must be provided")
        self.source_specs = [self._parse_source_spec(spec) for spec in source_specs]
        self.id_field = id_field
        self.id_prefix = id_prefix or id_field
        self.text_fields = list(text_fields or [])
        self.extra_keys = list(extra_keys or [])
        self.output_url_key = output_url_key
        self.passthrough_keys = list(passthrough_keys or [])
        self.passthrough_types = {
            key: self._parse_arrow_type(value) for key, value in (passthrough_types or {}).items()
        }

    @staticmethod
    def _parse_source_spec(spec: dict[str, Any]) -> ImageSourceSpec:
        for key in ["name", "url_field", "source"]:
            if key not in spec:
                raise ValueError(f"source_specs item must contain `{key}`")
        extra_url_mode = spec.get("extra_url_mode")
        if extra_url_mode is None:
            extra_url_mode = "single" if spec.get("max_urls") == 1 else "list"
        if extra_url_mode not in {"single", "list"}:
            raise ValueError("extra_url_mode must be either `single` or `list`")
        max_urls = spec.get("max_urls")
        if max_urls is not None:
            max_urls = int(max_urls)
            if max_urls <= 0:
                raise ValueError("max_urls must be positive")
        return ImageSourceSpec(
            name=str(spec["name"]),
            url_field=str(spec["url_field"]),
            source=str(spec["source"]),
            extra_url_key=spec.get("extra_url_key"),
            extra_url_mode=extra_url_mode,
            max_urls=max_urls,
        )

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

    def process_single(self, sample: dict[str, Any]) -> list[dict[str, Any]]:
        output_id = f"{self.id_prefix}-{sample.get(self.id_field)}"
        base_extra = {key: _jsonable(sample[key]) for key in self.extra_keys if key in sample}
        texts = self._texts_from_sample(sample)
        rows = []
        for spec in self.source_specs:
            urls = self._parse_urls(sample.get(spec.url_field))
            if spec.max_urls is not None:
                urls = urls[: spec.max_urls]
            if not urls:
                continue

            extra = dict(base_extra)
            if spec.extra_url_key:
                extra[spec.extra_url_key] = (
                    urls[0] if spec.extra_url_mode == "single" and urls else list(urls)
                )

            row = {
                "id": output_id,
                "source": spec.source,
                "texts": list(texts),
                self.output_url_key: list(urls),
                "extra": _extra_to_json(extra),
            }
            for key in self.passthrough_keys:
                row[key] = self._passthrough_value(key, sample.get(key))
            rows.append(row)
        return rows

    def process_batched(self, samples):
        input_schema = None
        return_arrow = isinstance(samples, pa.Table)
        if return_arrow:
            input_schema = samples.schema
            rows = samples.to_pylist()
        else:
            keys = list(samples.keys())
            if not keys:
                if return_arrow:
                    return self._rows_to_arrow_table([], input_schema)
                return {key: [] for key in self._output_keys()}
            rows = [{key: samples[key][idx] for key in keys} for idx in range(len(samples[keys[0]]))]

        output_rows = []
        for row in rows:
            output_rows.extend(self.process_single(row))

        if return_arrow:
            return self._rows_to_arrow_table(output_rows, input_schema)
        if not output_rows:
            return {key: [] for key in self._output_keys()}
        return {key: [row.get(key) for row in output_rows] for key in self._output_keys()}

    @classmethod
    def _parse_urls(cls, value: Any) -> list[str]:
        urls = []
        for url in cls._urls_from_obj(value):
            if isinstance(url, str):
                cleaned = url.strip().strip("\"'")
                if cleaned:
                    urls.append(cleaned)
        return urls

    @classmethod
    def _urls_from_obj(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
            value = value.tolist()
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="ignore")
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            html_urls = cls._extract_html_image_src_urls(text)
            if html_urls:
                return html_urls
            parsed = cls._parse_structured_string(text)
            if parsed is not None:
                return cls._urls_from_obj(parsed)
            return [token for token in _URL_SPLIT_RE.split(text) if token]
        if isinstance(value, dict):
            for key in ["url", "uri", "src", "image_url", "image", "href"]:
                if key in value:
                    return cls._urls_from_obj(value[key])
            return []
        if isinstance(value, (list, tuple, set)):
            urls = []
            for item in value:
                urls.extend(cls._urls_from_obj(item))
            return urls
        return []

    @staticmethod
    def _extract_html_image_src_urls(text: str) -> list[str]:
        urls = []
        for match in _HTML_IMG_SRC_RE.finditer(text):
            url = html.unescape(match.group(2).strip())
            if url:
                urls.append(url)
        return urls

    def _texts_from_sample(self, sample: dict[str, Any]) -> list[str]:
        texts = []
        for field in self.text_fields:
            if field not in sample:
                continue
            texts.extend(self._text_values_from_obj(_jsonable(sample[field])))
        return texts

    @classmethod
    def _text_values_from_obj(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
            value = value.tolist()
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="ignore")
        if isinstance(value, (list, tuple, set)):
            texts = []
            for item in value:
                texts.extend(cls._text_values_from_obj(item))
            return texts
        text = str(value).strip()
        return [text] if text else []

    @staticmethod
    def _parse_structured_string(text: str) -> Any | None:
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(text)
            except (ValueError, SyntaxError, TypeError, json.JSONDecodeError):
                continue
            if isinstance(parsed, str) and parsed == text:
                return None
            return parsed
        return None

    def _output_keys(self) -> list[str]:
        keys = [self.output_url_key if key == "image_urls" else key for key in _BASE_OUTPUT_KEYS]
        keys.extend(key for key in self.passthrough_keys if key not in keys)
        return keys

    def _rows_to_arrow_table(self, rows: list[dict[str, Any]], input_schema=None) -> pa.Table:
        arrays = []
        fields = []
        for key in self._output_keys():
            values = [self._passthrough_value(key, row.get(key)) for row in rows]
            arrow_type = self._arrow_type_for_key(key, values, input_schema)
            arrays.append(pa.array(values, type=arrow_type))
            fields.append(pa.field(key, arrow_type))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def _passthrough_value(self, key: str, value: Any) -> Any:
        if key in self.passthrough_types:
            return coerce_value_for_arrow_type(value, self.passthrough_types[key])
        return value

    def _arrow_type_for_key(self, key: str, values: list[Any], input_schema=None) -> pa.DataType:
        base_key = "image_urls" if key == self.output_url_key else key
        if base_key in _BASE_ARROW_TYPES:
            return _BASE_ARROW_TYPES[base_key]
        if key in self.passthrough_types:
            return self.passthrough_types[key]
        if input_schema is not None:
            field_index = input_schema.get_field_index(key)
            if field_index >= 0:
                input_type = input_schema.field(field_index).type
                if not pa.types.is_null(input_type):
                    return input_type

        inferred = pa.array(values)
        if pa.types.is_null(inferred.type):
            return pa.string()
        return inferred.type

    @staticmethod
    def _parse_arrow_type(value: str | pa.DataType) -> pa.DataType:
        return parse_arrow_type(value)
