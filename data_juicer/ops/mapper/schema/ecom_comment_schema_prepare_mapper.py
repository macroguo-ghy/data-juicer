from __future__ import annotations

import ast
import hashlib
import json
from typing import Any

import pyarrow as pa

from data_juicer.core.data import NestedDataset
from data_juicer.ops.base_op import OPERATORS, Pipeline

OP_NAME = "ecom_comment_schema_prepare_mapper"

_BASE_OUTPUT_KEYS = [
    "id",
    "source",
    "texts",
    "image_uris",
    "image_urls",
    "image_bytes",
    "valid_image_count",
    "type",
    "extra",
    "md5",
]
_BASE_ARROW_TYPES = {
    "id": pa.string(),
    "source": pa.string(),
    "texts": pa.list_(pa.string()),
    "image_uris": pa.list_(pa.string()),
    "image_urls": pa.list_(pa.string()),
    "image_bytes": pa.list_(pa.binary()),
    "valid_image_count": pa.int64(),
    "type": pa.string(),
    "extra": pa.string(),
    "md5": pa.string(),
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


def _jsonable(value: Any) -> Any:
    if hasattr(value, "as_py"):
        value = value.as_py()
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
        return value.tolist()
    return value


@OPERATORS.register_module(OP_NAME)
class EcomCommentSchemaPrepareMapper(Pipeline):
    """Prepare e-commerce comment rows for the image/text schema pipeline."""

    def __init__(
        self,
        id_field: str = "comment_id",
        id_prefix: str | None = None,
        text_field: str = "content",
        uri_field: str = "cmmt_img_uri",
        image_uri_key: str = "image_uris",
        image_url_key: str = "image_urls",
        image_bytes_key: str = "image_bytes",
        valid_image_count_key: str = "valid_image_count",
        type_key: str = "type",
        extra_key: str = "extra",
        md5_key: str = "md5",
        with_pic_source: str = "ecom_comment_with_pic_raw_data",
        no_pic_source: str = "ecom_comment_no_pic_raw_data",
        extra_keys: list[str] | None = None,
        passthrough_keys: list[str] | None = None,
        passthrough_types: dict[str, str] | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param id_field: source field used to build `id`.
        :param id_prefix: `id` prefix. Defaults to `id_field`.
        :param text_field: comment text field.
        :param uri_field: image URI field; null values become text rows.
        :param image_uri_key: output key for parsed image URIs.
        :param image_url_key: output key initialized for packed URLs.
        :param image_bytes_key: output key initialized for downloaded bytes.
        :param valid_image_count_key: output key initialized for image count.
        :param type_key: output key for image/text row type.
        :param extra_key: output key for JSON extra.
        :param md5_key: output key for text md5 or later image md5.
        :param with_pic_source: source value for non-null image URI rows.
        :param no_pic_source: source value for text-only rows.
        :param extra_keys: source fields serialized into `extra`.
        :param passthrough_keys: fields retained as top-level columns.
        :param passthrough_types: explicit pyarrow types for passthrough fields.
        :param args: extra args.
        :param kwargs: extra args.
        """
        kwargs["image_bytes_key"] = image_bytes_key
        super().__init__(*args, **kwargs)
        if not id_field:
            raise ValueError("id_field must be provided")
        if not text_field:
            raise ValueError("text_field must be provided")
        if not uri_field:
            raise ValueError("uri_field must be provided")

        self.id_field = id_field
        self.id_prefix = id_prefix or id_field
        self.text_field = text_field
        self.uri_field = uri_field
        self.image_uri_key = image_uri_key
        self.image_url_key = image_url_key
        self.image_bytes_key = image_bytes_key
        self.valid_image_count_key = valid_image_count_key
        self.type_key = type_key
        self.extra_key = extra_key
        self.md5_key = md5_key
        self.with_pic_source = with_pic_source
        self.no_pic_source = no_pic_source
        self.extra_keys = list(extra_keys or [])
        self.passthrough_keys = list(passthrough_keys or [])
        self.passthrough_types = {
            key: self._parse_arrow_type(value) for key, value in (passthrough_types or {}).items()
        }

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
        content = self._text_value(sample.get(self.text_field))
        if not content:
            return []

        uri_value = self._unwrap(sample.get(self.uri_field))
        is_image_row = uri_value is not None
        image_uris = self._uri_items(uri_value) if is_image_row else []
        source = self.with_pic_source if is_image_row else self.no_pic_source
        row_type = "image" if is_image_row else "text"
        md5 = None if is_image_row else hashlib.md5(content.encode()).hexdigest()

        row = {
            "id": f"{self.id_prefix}-{sample.get(self.id_field)}",
            "source": source,
            "texts": [content],
            self.image_uri_key: image_uris,
            self.image_url_key: [],
            self.image_bytes_key: [],
            self.valid_image_count_key: 0,
            self.type_key: row_type,
            self.extra_key: json.dumps(self._base_extra(sample), ensure_ascii=False, default=str),
            self.md5_key: md5,
        }
        for key in self.passthrough_keys:
            row[key] = sample.get(key)
        return [row]

    def process_batched(self, samples):
        input_schema = None
        return_arrow = isinstance(samples, pa.Table)
        if return_arrow:
            input_schema = samples.schema
            rows = samples.to_pylist()
        else:
            keys = list(samples.keys())
            if not keys:
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

    def _base_extra(self, sample: dict[str, Any]) -> dict[str, Any]:
        return {key: _jsonable(sample[key]) for key in self.extra_keys if key in sample}

    @classmethod
    def _uri_items(cls, value: Any) -> list[str]:
        value = cls._unwrap(value)
        if value is None:
            return []
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="ignore")
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            parsed = cls._parse_structured_string(text)
            if parsed is not None:
                return cls._uri_items(parsed)
            return [text]
        if isinstance(value, dict):
            for key in ("uri", "url", "src", "image_uri", "image"):
                if key in value:
                    return cls._uri_items(value[key])
            return []
        if isinstance(value, (list, tuple, set)):
            items = []
            for item in value:
                items.extend(cls._uri_items(item))
            return items
        return [str(value)]

    @staticmethod
    def _text_value(value: Any) -> str | None:
        value = EcomCommentSchemaPrepareMapper._unwrap(value)
        if value is None:
            return None
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="ignore")
        text = str(value)
        return text if text.strip() else None

    @staticmethod
    def _unwrap(value: Any) -> Any:
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
            value = value.tolist()
        return value

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
        replacements = {
            "image_uris": self.image_uri_key,
            "image_urls": self.image_url_key,
            "image_bytes": self.image_bytes_key,
            "valid_image_count": self.valid_image_count_key,
            "type": self.type_key,
            "extra": self.extra_key,
            "md5": self.md5_key,
        }
        keys = [replacements.get(key, key) for key in _BASE_OUTPUT_KEYS]
        keys.extend(key for key in self.passthrough_keys if key not in keys)
        return keys

    def _rows_to_arrow_table(self, rows: list[dict[str, Any]], input_schema=None) -> pa.Table:
        arrays = []
        fields = []
        for key in self._output_keys():
            values = [row.get(key) for row in rows]
            arrow_type = self._arrow_type_for_key(key, values, input_schema)
            arrays.append(pa.array(values, type=arrow_type))
            fields.append(pa.field(key, arrow_type))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def _arrow_type_for_key(self, key: str, values: list[Any], input_schema=None) -> pa.DataType:
        base_key = self._base_key_for_output(key)
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

    def _base_key_for_output(self, key: str) -> str:
        output_to_base = {
            self.image_uri_key: "image_uris",
            self.image_url_key: "image_urls",
            self.image_bytes_key: "image_bytes",
            self.valid_image_count_key: "valid_image_count",
            self.type_key: "type",
            self.extra_key: "extra",
            self.md5_key: "md5",
        }
        return output_to_base.get(key, key)

    @staticmethod
    def _parse_arrow_type(value: str | pa.DataType) -> pa.DataType:
        if isinstance(value, pa.DataType):
            return value
        normalized = str(value).strip().lower()
        if normalized in _ARROW_TYPE_ALIASES:
            return _ARROW_TYPE_ALIASES[normalized]
        raise ValueError(f"Unsupported passthrough arrow type: {value}")
