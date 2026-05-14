from __future__ import annotations

from typing import Any, Sequence

import pyarrow as pa

from data_juicer.core.data import NestedDataset
from data_juicer.ops.base_op import OPERATORS, Pipeline
from data_juicer.ops.mapper.schema.passthrough_type_utils import (
    coerce_value_for_arrow_type,
    parse_arrow_type,
)

OP_NAME = "aligned_list_field_flatten_mapper"


@OPERATORS.register_module(OP_NAME)
class AlignedListFieldFlattenMapper(Pipeline):
    """Flatten multiple list-like fields by aligned index."""

    def __init__(
        self,
        field_keys: Sequence[str] | None = None,
        output_field_keys: dict[str, str] | None = None,
        wrap_value_keys: Sequence[str] | None = None,
        drop_empty: bool = True,
        drop_mismatch: bool = True,
        index_key: str | None = None,
        id_key: str | None = None,
        id_format: str | None = None,
        id_index_separator: str = "-",
        passthrough_types: dict[str, str] | None = None,
        *args,
        **kwargs,
    ):
        """
        :param field_keys: list-like input fields to flatten together.
        :param output_field_keys: optional source-field to output-field mapping.
        :param wrap_value_keys: fields whose flattened value should remain a
            single-item list, useful for list<binary> image columns.
        :param drop_empty: drop rows where any aligned input field is empty.
        :param drop_mismatch: drop rows when aligned field lengths differ.
            If False, flatten up to the shortest non-empty field length.
        :param index_key: optional output field storing the flattened index.
        :param id_key: optional id field to suffix with the flattened index.
        :param id_format: optional format string with `{id}` and `{index}`.
        :param passthrough_types: pyarrow types used to normalize preserved fields.
        """
        super().__init__(*args, **kwargs)
        if not field_keys:
            raise ValueError("field_keys must be a non-empty list")
        self.field_keys = list(field_keys)
        self.output_field_keys = dict(output_field_keys or {})
        self.wrap_value_keys = set(wrap_value_keys or [])
        self.drop_empty = drop_empty
        self.drop_mismatch = drop_mismatch
        self.index_key = index_key
        self.id_key = id_key
        self.id_format = id_format
        self.id_index_separator = id_index_separator
        self.passthrough_types = {
            key: parse_arrow_type(value) for key, value in (passthrough_types or {}).items()
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

    def process_batched(self, samples):
        input_schema = samples.schema if isinstance(samples, pa.Table) else None
        input_rows = self._batch_to_rows(samples)

        output_rows = []
        for row in input_rows:
            output_rows.extend(self.process_single(row))

        if isinstance(samples, pa.Table):
            return self._rows_to_arrow_table(output_rows, input_schema)
        if not output_rows:
            return {key: [] for key in self._output_keys(samples.keys() if samples else [])}
        return {key: [row.get(key) for row in output_rows] for key in self._output_keys(samples.keys())}

    def process_single(self, sample: dict[str, Any]) -> list[dict[str, Any]]:
        values_by_key = {field_key: self._as_list(sample.get(field_key)) for field_key in self.field_keys}
        lengths = [len(values) for values in values_by_key.values()]
        if any(length == 0 for length in lengths):
            if self.drop_empty:
                return []
            row = dict(sample)
            self._coerce_passthrough_values(row)
            for field_key in self.field_keys:
                row[self._output_key(field_key)] = [] if field_key in self.wrap_value_keys else None
            if self.index_key:
                row[self.index_key] = None
            return [row]

        if len(set(lengths)) != 1 and self.drop_mismatch:
            return []

        rows = []
        for index in range(min(lengths)):
            row = dict(sample)
            self._coerce_passthrough_values(row)
            for field_key, values in values_by_key.items():
                value = values[index]
                row[self._output_key(field_key)] = [value] if field_key in self.wrap_value_keys else value
            if self.index_key:
                row[self.index_key] = index
            if self.id_key and self.id_key in row:
                source_id = row.get(self.id_key)
                if self.id_format is None:
                    row[self.id_key] = f"{source_id}{self.id_index_separator}{index}"
                else:
                    row[self.id_key] = self.id_format.format(id=source_id, index=index)
            rows.append(row)
        return rows

    @staticmethod
    def _batch_to_rows(samples) -> list[dict[str, Any]]:
        if isinstance(samples, pa.Table):
            return samples.to_pylist()
        keys = list(samples.keys())
        if not keys:
            return []
        return [{key: samples[key][idx] for key in keys} for idx in range(len(samples[keys[0]]))]

    @staticmethod
    def _as_list(value: Any) -> list[Any]:
        if value is None:
            return []
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray, memoryview)):
            value = value.tolist()
        if isinstance(value, (list, tuple)):
            return list(value)
        return [value]

    def _coerce_passthrough_values(self, row: dict[str, Any]) -> None:
        for key, arrow_type in self.passthrough_types.items():
            if key in row:
                row[key] = coerce_value_for_arrow_type(row.get(key), arrow_type)

    def _output_key(self, field_key: str) -> str:
        return self.output_field_keys.get(field_key, field_key)

    def _output_keys(self, input_keys) -> list[str]:
        keys = list(input_keys)
        for field_key in self.field_keys:
            output_key = self._output_key(field_key)
            if output_key not in keys:
                keys.append(output_key)
        if self.index_key and self.index_key not in keys:
            keys.append(self.index_key)
        return keys

    def _rows_to_arrow_table(self, rows: list[dict[str, Any]], input_schema: pa.Schema | None) -> pa.Table:
        input_names = input_schema.names if input_schema is not None else (list(rows[0].keys()) if rows else [])
        keys = self._output_keys(input_names)
        arrays = []
        fields = []
        for key in keys:
            values = [row.get(key) for row in rows]
            arrow_type = self._arrow_type_for_key(key, values, input_schema)
            arrays.append(pa.array(values, type=arrow_type))
            fields.append(pa.field(key, arrow_type))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def _arrow_type_for_key(self, key: str, values: list[Any], input_schema: pa.Schema | None) -> pa.DataType:
        if key == self.index_key:
            return pa.int64()
        if key in self.passthrough_types:
            return self.passthrough_types[key]
        if input_schema is not None:
            field_index = input_schema.get_field_index(key)
            if field_index >= 0:
                input_type = input_schema.field(field_index).type
                source_key = next((field_key for field_key in self.field_keys if self._output_key(field_key) == key), None)
                if source_key and source_key not in self.wrap_value_keys:
                    if pa.types.is_list(input_type) or pa.types.is_large_list(input_type):
                        return input_type.value_type
                if not pa.types.is_null(input_type):
                    return input_type
        inferred = pa.array(values)
        if pa.types.is_null(inferred.type):
            return pa.string()
        return inferred.type
