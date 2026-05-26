from __future__ import annotations

from typing import Any

import pyarrow as pa

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.ops.mapper.schema.passthrough_type_utils import (
    coerce_value_for_arrow_type,
    parse_arrow_type,
)

OP_NAME = "field_assign_mapper"


@OPERATORS.register_module(OP_NAME)
class FieldAssignMapper(Mapper):
    """Assign fields from constants, templates, or copied source fields."""

    _batched_op = True

    def __init__(self, assignments: dict[str, dict[str, Any]] | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.assignments = dict(assignments or {})
        self.assignment_types = {
            key: parse_arrow_type(spec["type"]) for key, spec in self.assignments.items() if spec and "type" in spec
        }

    def process_single(self, sample):
        for key, spec in self.assignments.items():
            sample[key] = self._assignment_value(spec or {}, sample)
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

    def _assignment_value(self, spec: dict[str, Any], sample: dict[str, Any]) -> Any:
        if "copy_from" in spec:
            value = sample.get(spec["copy_from"])
        elif "template" in spec:
            value = spec["template"].format_map(_MissingAsEmptyDict(sample))
        else:
            value = spec.get("value")
        if "type" in spec:
            value = coerce_value_for_arrow_type(value, parse_arrow_type(spec["type"]))
        return value

    @staticmethod
    def _dict_to_rows(samples: dict[str, list[Any]]) -> list[dict[str, Any]]:
        keys = list(samples.keys())
        if not keys:
            return []
        return [{key: samples[key][i] for key in keys} for i in range(len(samples[keys[0]]))]

    def _rows_to_dict(self, rows: list[dict[str, Any]], original_keys) -> dict[str, list[Any]]:
        keys = list(original_keys)
        keys.extend(key for key in self.assignments if key not in keys)
        if not rows:
            return {key: [] for key in keys}
        return {key: [row.get(key) for row in rows] for key in keys}

    def _rows_to_table(self, rows: list[dict[str, Any]], input_schema: pa.Schema | None) -> pa.Table:
        keys = list(input_schema.names if input_schema is not None else [])
        keys.extend(key for key in self.assignments if key not in keys)
        arrays = []
        fields = []
        for key in keys:
            values = [row.get(key) for row in rows]
            arrow_type = self._arrow_type_for_key(key, values, input_schema)
            arrays.append(pa.array(values, type=arrow_type))
            fields.append(pa.field(key, arrow_type))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def _arrow_type_for_key(self, key: str, values: list[Any], input_schema: pa.Schema | None) -> pa.DataType:
        if key in self.assignment_types:
            return self.assignment_types[key]
        if input_schema is not None:
            field_index = input_schema.get_field_index(key)
            if field_index >= 0:
                return input_schema.field(field_index).type
        inferred = pa.array(values)
        if pa.types.is_null(inferred.type):
            return pa.string()
        return inferred.type


class _MissingAsEmptyDict(dict):
    def __missing__(self, key):
        return ""
