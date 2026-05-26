from __future__ import annotations

import base64
import json
from typing import Any

import pyarrow as pa

from data_juicer.ops.base_op import OPERATORS, Mapper

OP_NAME = "json_object_mapper"


@OPERATORS.register_module(OP_NAME)
class JsonObjectMapper(Mapper):
    """Build a JSON object string from sample fields."""

    _batched_op = True

    def __init__(
        self,
        output_key: str = "extra",
        include_all: bool = False,
        include_keys: list[str] | None = None,
        exclude_keys: list[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if not output_key:
            raise ValueError("output_key must be provided")
        self.output_key = output_key
        self.include_all = include_all
        self.include_keys = list(include_keys or [])
        self.exclude_keys = set(exclude_keys or [])

    def process_single(self, sample):
        keys = list(sample.keys()) if self.include_all else list(self.include_keys)
        payload = {}
        for key in keys:
            if key == self.output_key or key in self.exclude_keys:
                continue
            payload[key] = self._jsonable(sample.get(key))
        sample[self.output_key] = json.dumps(payload, ensure_ascii=False, default=str)
        return sample

    def process_batched(self, samples):
        input_schema = samples.schema if isinstance(samples, pa.Table) else None
        return_arrow = isinstance(samples, pa.Table)
        if return_arrow:
            samples = samples.to_pydict()
        rows = self._dict_to_rows(samples)
        rows = [self.process_single(row) for row in rows]
        if return_arrow:
            return self._rows_to_table(rows, input_schema)
        return self._rows_to_dict(rows, samples.keys())

    @classmethod
    def _jsonable(cls, value: Any) -> Any:
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray, memoryview)):
            value = value.tolist()
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                return base64.b64encode(value).decode("ascii")
        if isinstance(value, (bytearray, memoryview)):
            return cls._jsonable(bytes(value))
        if isinstance(value, dict):
            return {str(key): cls._jsonable(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [cls._jsonable(item) for item in value]
        return value

    @staticmethod
    def _dict_to_rows(samples: dict[str, list[Any]]) -> list[dict[str, Any]]:
        keys = list(samples.keys())
        if not keys:
            return []
        return [{key: samples[key][i] for key in keys} for i in range(len(samples[keys[0]]))]

    def _rows_to_dict(self, rows: list[dict[str, Any]], original_keys) -> dict[str, list[Any]]:
        keys = list(original_keys)
        if self.output_key not in keys:
            keys.append(self.output_key)
        if not rows:
            return {key: [] for key in keys}
        return {key: [row.get(key) for row in rows] for key in keys}

    def _rows_to_table(self, rows: list[dict[str, Any]], input_schema: pa.Schema | None) -> pa.Table:
        keys = list(input_schema.names if input_schema is not None else [])
        if self.output_key not in keys:
            keys.append(self.output_key)
        arrays = []
        fields = []
        for key in keys:
            values = [row.get(key) for row in rows]
            arrow_type = pa.string() if key == self.output_key else self._input_or_inferred_type(key, values, input_schema)
            arrays.append(pa.array(values, type=arrow_type))
            fields.append(pa.field(key, arrow_type))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    @staticmethod
    def _input_or_inferred_type(key: str, values: list[Any], input_schema: pa.Schema | None) -> pa.DataType:
        if input_schema is not None:
            field_index = input_schema.get_field_index(key)
            if field_index >= 0:
                return input_schema.field(field_index).type
        inferred = pa.array(values)
        if pa.types.is_null(inferred.type):
            return pa.string()
        return inferred.type
