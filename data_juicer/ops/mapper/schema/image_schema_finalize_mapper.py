from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from data_juicer.core.data import NestedDataset
from data_juicer.ops.base_op import OPERATORS, Pipeline
from data_juicer.ops.mapper.schema.passthrough_type_utils import (
    coerce_value_for_arrow_type,
    parse_arrow_type,
)

OP_NAME = "image_schema_finalize_mapper"

_BASE_OUTPUT_KEYS = [
    "id",
    "source",
    "texts",
    "images",
    "audios",
    "videos",
    "has_audio_in_video",
    "type",
    "extra",
    "md5",
]
_BASE_ARROW_TYPES = {
    "id": pa.string(),
    "source": pa.string(),
    "texts": pa.list_(pa.string()),
    "images": pa.list_(pa.binary()),
    "audios": pa.list_(pa.binary()),
    "videos": pa.list_(pa.binary()),
    "has_audio_in_video": pa.bool_(),
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


@OPERATORS.register_module(OP_NAME)
class ImageSchemaFinalizeMapper(Pipeline):
    """Finalize image byte rows into the Data-Juicer multimodal schema."""

    def __init__(
        self,
        id_key: str = "id",
        source_key: str = "source",
        texts_key: str = "texts",
        image_bytes_key: str = "image_bytes",
        extra_key: str = "extra",
        md5_key: str = "md5",
        type_key: str | None = None,
        passthrough_keys: list[str] | None = None,
        passthrough_types: dict[str, str] | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param id_key: input field containing the final id.
        :param source_key: input field containing the image source.
        :param texts_key: input field containing text values.
        :param image_bytes_key: input field containing final image bytes.
        :param extra_key: input field containing JSON-ready extra values.
        :param md5_key: input field containing sample-level md5.
        :param type_key: optional input field containing the output row type.
        :param passthrough_keys: source fields to keep as top-level output columns.
        :param passthrough_types: pyarrow types for passthrough fields.
        :param args: extra args.
        :param kwargs: extra args.
        """
        kwargs["image_bytes_key"] = image_bytes_key
        super().__init__(*args, **kwargs)
        self.id_key = id_key
        self.source_key = source_key
        self.texts_key = texts_key
        self.image_bytes_key = image_bytes_key
        self.extra_key = extra_key
        self.md5_key = md5_key
        self.type_key = type_key
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

    def process_single(self, sample: dict[str, Any]) -> dict[str, Any]:
        row = {
            "id": sample.get(self.id_key),
            "source": sample.get(self.source_key),
            "texts": self._as_string_list(sample.get(self.texts_key)),
            "images": self._as_bytes_list(sample.get(self.image_bytes_key)),
            "audios": [],
            "videos": [],
            "has_audio_in_video": False,
            "type": self._row_type(sample),
            "extra": self._extra_to_json(sample.get(self.extra_key)),
            "md5": sample.get(self.md5_key),
        }
        for key in self.passthrough_keys:
            row[key] = self._passthrough_value(key, sample.get(key))
        return row

    def _row_type(self, sample: dict[str, Any]) -> str:
        if self.type_key:
            value = sample.get(self.type_key)
            if value is not None:
                value = str(value).strip()
                if value:
                    return value
        return "image"

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

        output_rows = [self.process_single(row) for row in rows]

        if return_arrow:
            return self._rows_to_arrow_table(output_rows, input_schema)
        if not output_rows:
            return {key: [] for key in self._output_keys()}
        return {key: [row.get(key) for row in output_rows] for key in self._output_keys()}

    @staticmethod
    def _as_string_list(value: Any) -> list[str]:
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
                texts.extend(ImageSchemaFinalizeMapper._as_string_list(item))
            return texts
        text = str(value).strip()
        return [text] if text else []

    @staticmethod
    def _as_bytes_list(value: Any) -> list[bytes]:
        if value is None:
            return []
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (bytes, bytearray, memoryview, str)):
            value = value.tolist()
        if isinstance(value, (bytes, bytearray, memoryview)):
            return [bytes(value)]
        if isinstance(value, (list, tuple)):
            values = []
            for item in value:
                values.extend(ImageSchemaFinalizeMapper._as_bytes_list(item))
            return values
        return []

    @staticmethod
    def _extra_to_json(value: Any) -> str:
        if value is None or value == "":
            return "{}"
        if hasattr(value, "as_py"):
            value = value.as_py()
        if isinstance(value, str):
            return value
        if hasattr(value, "tolist"):
            value = value.tolist()
        return json.dumps(value, ensure_ascii=False, default=str)

    def _output_keys(self) -> list[str]:
        keys = list(_BASE_OUTPUT_KEYS)
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
        if key in _BASE_ARROW_TYPES:
            return _BASE_ARROW_TYPES[key]
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
