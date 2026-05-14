from __future__ import annotations

from typing import Any

import pyarrow as pa

from data_juicer.ops.base_op import OPERATORS, Pipeline
from data_juicer.ops.mapper.schema.passthrough_type_utils import (
    coerce_value_for_arrow_type,
    parse_arrow_type,
)

OP_NAME = "list_field_flatten_mapper"


@OPERATORS.register_module(OP_NAME)
class ListFieldFlattenMapper(Pipeline):
    """Flatten one list-like field into one output row per element."""

    def __init__(
        self,
        field_key: str = "",
        output_field_key: str | None = None,
        wrap_value: bool = True,
        drop_empty: bool = True,
        index_key: str | None = None,
        id_key: str | None = None,
        id_format: str | None = None,
        id_index_separator: str = "-",
        passthrough_types: dict[str, str] | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param field_key: input field to flatten.
        :param output_field_key: output field for each flattened element.
            Defaults to replacing `field_key`.
        :param wrap_value: whether each flattened element should be wrapped as
            a single-item list. This preserves list-shaped fields for downstream
            image/audio/video operators.
        :param drop_empty: whether rows with empty input values should be
            dropped. If False, they are kept with an empty output value.
        :param index_key: optional output field storing the element index.
        :param id_key: optional field whose value should be rewritten with
            `id_format` for flattened rows.
        :param id_format: optional Python format string with `{id}` and
            `{index}`. If not set, the output id is `{id}{id_index_separator}{index}`.
        :param id_index_separator: separator used when appending `index` to
            `id_key` and `id_format` is not set.
        :param passthrough_types: pyarrow types used to normalize preserved
            fields when input Ray blocks have inconsistent inferred schemas.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not field_key:
            raise ValueError("field_key must be provided")
        self.field_key = field_key
        self.output_field_key = output_field_key or field_key
        self.wrap_value = wrap_value
        self.drop_empty = drop_empty
        self.index_key = index_key
        self.id_key = id_key
        self.id_format = id_format
        self.id_index_separator = id_index_separator
        self.passthrough_types = {
            key: parse_arrow_type(value) for key, value in (passthrough_types or {}).items()
        }

    def run(self, dataset, *, exporter=None, tracer=None):
        from data_juicer.core.data import NestedDataset

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
        input_schema = None
        return_arrow = isinstance(samples, pa.Table)
        if return_arrow:
            input_schema = samples.schema
            input_rows = samples.to_pylist()
        else:
            keys = list(samples.keys())
            if not keys:
                return self._rows_to_arrow_table([], input_schema) if return_arrow else {}
            input_rows = [{key: samples[key][idx] for key in keys} for idx in range(len(samples[keys[0]]))]

        output_rows = []
        for row in input_rows:
            output_rows.extend(self.process_single(row))

        if return_arrow:
            return self._rows_to_arrow_table(output_rows, input_schema)
        if not output_rows:
            return {key: [] for key in self._output_keys(samples.keys())}
        return {key: [row.get(key) for row in output_rows] for key in self._output_keys(samples.keys())}

    def process_single(self, sample: dict[str, Any]) -> list[dict[str, Any]]:
        values = self._as_list(sample.get(self.field_key))
        if not values:
            if self.drop_empty:
                return []
            row = dict(sample)
            self._coerce_passthrough_values(row)
            row[self.output_field_key] = []
            if self.index_key:
                row[self.index_key] = None
            return [row]

        rows = []
        for index, value in enumerate(values):
            row = dict(sample)
            self._coerce_passthrough_values(row)
            row[self.output_field_key] = [value] if self.wrap_value else value
            if self.index_key:
                row[self.index_key] = index
            if self.id_key and self.id_key in row:
                source_id = row.get(self.id_key)
                if self.id_format is None:
                    row[self.id_key] = f"{source_id}{self.id_index_separator}{index}"
                else:
                    row[self.id_key] = self.id_format.format(id=source_id, index=index, value=value)
            rows.append(row)
        return rows

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

    def _output_keys(self, input_keys) -> list[str]:
        keys = list(input_keys)
        if self.output_field_key not in keys:
            keys.append(self.output_field_key)
        if self.index_key and self.index_key not in keys:
            keys.append(self.index_key)
        return keys

    def _rows_to_arrow_table(self, rows: list[dict[str, Any]], input_schema=None) -> pa.Table:
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

    def _arrow_type_for_key(self, key: str, values: list[Any], input_schema=None) -> pa.DataType:
        if key == self.index_key:
            return pa.int64()

        if key in self.passthrough_types:
            return self.passthrough_types[key]

        if input_schema is not None:
            field_index = input_schema.get_field_index(key)
            if field_index >= 0:
                input_type = input_schema.field(field_index).type
                if key == self.output_field_key and self.wrap_value:
                    if pa.types.is_list(input_type) or pa.types.is_large_list(input_type):
                        return input_type
                if not pa.types.is_null(input_type):
                    return input_type

        inferred = pa.array(values)
        if pa.types.is_null(inferred.type):
            return pa.string()
        return inferred.type
