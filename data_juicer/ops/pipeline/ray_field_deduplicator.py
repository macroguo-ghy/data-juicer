from __future__ import annotations

import hashlib
from typing import Any

import pyarrow as pa

from data_juicer.core.data import NestedDataset
from data_juicer.ops.base_op import OPERATORS, Pipeline

OP_NAME = "ray_field_deduplicator"


@OPERATORS.register_module(OP_NAME)
class RayFieldDeduplicator(Pipeline):
    """Deduplicate rows by exact value of a field in default and Ray executors."""

    _HASH_KEY = "__dj_field_dedup_hash"

    def __init__(
        self,
        field_key: str = "images",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if not field_key:
            raise ValueError("field_key must be provided")
        self.field_key = field_key

    def run(self, dataset, *, exporter=None, tracer=None):
        if isinstance(dataset, NestedDataset):
            seen = set()
            rows = []
            for row in dataset.to_list():
                digest = self._hash_value(row.get(self.field_key))
                if digest in seen:
                    continue
                seen.add(digest)
                rows.append(row)
            return NestedDataset.from_list(rows)

        dataset = dataset.map_batches(
            self._append_hash_batch,
            batch_format="pyarrow",
            batch_size=self.batch_size,
            fn_kwargs={"field_key": self.field_key, "hash_key": self._HASH_KEY},
        )
        dataset = dataset.groupby(self._HASH_KEY).map_groups(
            self._take_first_group,
            batch_format="pyarrow",
            fn_kwargs={"hash_key": self._HASH_KEY},
        )
        return dataset

    @classmethod
    def _append_hash_batch(cls, table: pa.Table, *, field_key: str, hash_key: str) -> pa.Table:
        values = table.column(field_key).to_pylist()
        hashes = [cls._hash_value(value) for value in values]
        return table.append_column(hash_key, pa.array(hashes, type=pa.string()))

    @staticmethod
    def _take_first_group(table: pa.Table, *, hash_key: str) -> pa.Table:
        output = table.slice(0, 1)
        field_index = output.schema.get_field_index(hash_key)
        if field_index >= 0:
            output = output.remove_column(field_index)
        return output

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
