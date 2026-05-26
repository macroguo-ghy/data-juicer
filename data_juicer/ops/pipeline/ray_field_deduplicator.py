from __future__ import annotations

import hashlib
from typing import Any

import pyarrow as pa

from data_juicer.core.data import NestedDataset
from data_juicer.ops.base_op import OPERATORS, Pipeline
from data_juicer.ops.condition_utils import RowCondition

OP_NAME = "ray_field_deduplicator"


@OPERATORS.register_module(OP_NAME)
class RayFieldDeduplicator(Pipeline):
    """Deduplicate rows by exact value of a field in default and Ray executors."""

    _HASH_KEY = "__dj_field_dedup_hash"

    def __init__(
        self,
        field_key: str = "images",
        condition: str = "",
        id_key: str | None = None,
        duplicate_ids_key: str | None = None,
        duplicate_ids_mode: str = "none",
        representative_policy: str = "first",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if not field_key:
            raise ValueError("field_key must be provided")
        if duplicate_ids_mode not in {"none", "removed"}:
            raise ValueError("duplicate_ids_mode must be one of: none, removed")
        if representative_policy not in {"first", "min_id"}:
            raise ValueError("representative_policy must be one of: first, min_id")
        if representative_policy == "min_id" and not id_key:
            raise ValueError("id_key must be provided when representative_policy is min_id")
        self.field_key = field_key
        self.condition = condition
        self._condition = RowCondition(condition)
        self.id_key = id_key
        self.duplicate_ids_key = duplicate_ids_key
        self.duplicate_ids_mode = duplicate_ids_mode
        self.representative_policy = representative_policy

    def run(self, dataset, *, exporter=None, tracer=None):
        if isinstance(dataset, NestedDataset):
            rows = self._deduplicate_rows(dataset.to_list())
            return NestedDataset.from_list(rows)

        dataset = dataset.map_batches(
            self._append_hash_batch,
            batch_format="pyarrow",
            batch_size=self.batch_size,
            fn_kwargs={
                "field_key": self.field_key,
                "hash_key": self._HASH_KEY,
                "condition": self.condition,
            },
        )
        dataset = dataset.groupby(self._HASH_KEY).map_groups(
            self._take_group_representative,
            batch_format="pyarrow",
            fn_kwargs={
                "hash_key": self._HASH_KEY,
                "id_key": self.id_key,
                "duplicate_ids_key": self.duplicate_ids_key,
                "duplicate_ids_mode": self.duplicate_ids_mode,
                "representative_policy": self.representative_policy,
            },
        )
        return dataset

    @classmethod
    def _append_hash_batch(cls, table: pa.Table, *, field_key: str, hash_key: str, condition: str = "") -> pa.Table:
        rows = table.to_pylist()
        row_condition = RowCondition(condition)
        values = table.column(field_key).to_pylist()
        hashes = [
            cls._hash_value(value)
            if cls._row_is_eligible(row, row_condition, field_key)
            else f"__dj_passthrough__{id(table)}__{index}__{cls._hash_value(row)}"
            for index, (row, value) in enumerate(zip(rows, values))
        ]
        return table.append_column(hash_key, pa.array(hashes, type=pa.string()))

    @staticmethod
    def _take_first_group(table: pa.Table, *, hash_key: str) -> pa.Table:
        output = table.slice(0, 1)
        field_index = output.schema.get_field_index(hash_key)
        if field_index >= 0:
            output = output.remove_column(field_index)
        return output

    @classmethod
    def _take_group_representative(
        cls,
        table: pa.Table,
        *,
        hash_key: str,
        id_key: str | None = None,
        duplicate_ids_key: str | None = None,
        duplicate_ids_mode: str = "none",
        representative_policy: str = "first",
    ) -> pa.Table:
        if representative_policy == "first" and duplicate_ids_mode == "none":
            return cls._take_first_group(table, hash_key=hash_key)

        rows = table.to_pylist()
        if representative_policy == "min_id":
            representative_index = min(range(len(rows)), key=lambda index: str(rows[index].get(id_key)))
        else:
            representative_index = 0
        output = table.slice(representative_index, 1)
        if duplicate_ids_mode == "removed" and duplicate_ids_key:
            removed_ids = [
                str(row.get(id_key))
                for index, row in enumerate(rows)
                if index != representative_index and row.get(id_key) is not None
            ]
            output = cls._set_duplicate_ids(output, duplicate_ids_key, sorted(removed_ids))

        field_index = table.schema.get_field_index(hash_key)
        if field_index >= 0:
            output = output.remove_column(field_index)
        return output

    @staticmethod
    def _set_duplicate_ids(table: pa.Table, duplicate_ids_key: str, duplicate_ids: list[str]) -> pa.Table:
        field_index = table.schema.get_field_index(duplicate_ids_key)
        field = RayFieldDeduplicator._duplicate_ids_field(
            table.schema.field(field_index) if field_index >= 0 else None,
            duplicate_ids_key,
        )
        values = pa.array([duplicate_ids], type=field.type)
        if field_index >= 0:
            return table.set_column(field_index, field, values)
        return table.append_column(field, values)

    @staticmethod
    def _duplicate_ids_field(existing_field: pa.Field | None, duplicate_ids_key: str) -> pa.Field:
        return pa.field(
            duplicate_ids_key,
            pa.list_(pa.string()),
            nullable=existing_field.nullable if existing_field is not None else True,
            metadata=existing_field.metadata if existing_field is not None else None,
        )

    def _deduplicate_rows(self, input_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        passthrough_rows = []
        groups = {}
        group_order = []
        for row in input_rows:
            if not self._row_is_eligible(row, self._condition, self.field_key):
                output = dict(row)
                if self.duplicate_ids_mode == "removed" and self.duplicate_ids_key and self.duplicate_ids_key not in output:
                    output[self.duplicate_ids_key] = []
                passthrough_rows.append(output)
                continue
            digest = self._hash_value(row.get(self.field_key))
            if digest not in groups:
                groups[digest] = []
                group_order.append(digest)
            groups[digest].append(dict(row))

        deduped_rows = [self._representative_row(groups[digest]) for digest in group_order]
        deduped_rows.extend(passthrough_rows)
        if self.representative_policy == "min_id" and self.id_key:
            deduped_rows.sort(key=lambda row: str(row.get(self.id_key)))
        return deduped_rows

    def _representative_row(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        if self.representative_policy == "min_id":
            representative = min(rows, key=lambda row: str(row.get(self.id_key)))
        else:
            representative = rows[0]
        output = dict(representative)
        if self.duplicate_ids_mode == "removed" and self.duplicate_ids_key:
            removed_ids = [
                str(row.get(self.id_key))
                for row in rows
                if row is not representative and row.get(self.id_key) is not None
            ]
            output[self.duplicate_ids_key] = sorted(removed_ids)
        return output

    @staticmethod
    def _row_is_eligible(row: dict[str, Any], condition: RowCondition, field_key: str) -> bool:
        return condition.matches(row) and row.get(field_key) is not None

    @classmethod
    def _hash_value(cls, value: Any) -> str:
        hasher = hashlib.sha256()
        cls._update_hash(hasher, value)
        return hasher.hexdigest()

    @classmethod
    def _update_hash(cls, hasher, value: Any) -> None:
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray, memoryview)):
            value = value.tolist()

        if value is None:
            hasher.update(b"N")
        elif isinstance(value, (bytes, bytearray, memoryview)):
            hasher.update(b"B")
            hasher.update(bytes(value))
        elif isinstance(value, str):
            hasher.update(b"S")
            hasher.update(value.encode("utf-8"))
        elif isinstance(value, dict):
            hasher.update(b"D")
            for key in sorted(value):
                cls._update_hash(hasher, key)
                cls._update_hash(hasher, value[key])
        elif isinstance(value, (list, tuple)):
            hasher.update(b"L")
            for item in value:
                cls._update_hash(hasher, item)
                hasher.update(b";")
        else:
            hasher.update(b"V")
            hasher.update(str(value).encode("utf-8"))
