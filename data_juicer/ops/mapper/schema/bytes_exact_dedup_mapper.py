from __future__ import annotations

import hashlib
from typing import Any

import pyarrow as pa

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.ops.condition_utils import RowCondition

OP_NAME = "bytes_exact_dedup_mapper"


@OPERATORS.register_module(OP_NAME)
class BytesExactDedupMapper(Mapper):
    """Deduplicate aligned URL and bytes lists within each sample by exact bytes."""

    _batched_op = True

    def __init__(
        self,
        url_key: str = "urls",
        bytes_key: str = "videos",
        md5_key: str = "md5",
        valid_count_key: str = "valid_video_count",
        condition: str = "",
        null_md5_on_empty: bool = True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.url_key = url_key
        self.bytes_key = bytes_key
        self.md5_key = md5_key
        self.valid_count_key = valid_count_key
        self.condition = condition
        self._condition = RowCondition(condition)
        self.null_md5_on_empty = null_md5_on_empty

    def process_single(self, sample):
        if not self._condition.matches(sample):
            return sample
        urls, binary_values = dedup_aligned_bytes(
            sample.get(self.url_key),
            sample.get(self.bytes_key),
        )
        sample[self.url_key] = urls
        sample[self.bytes_key] = binary_values
        sample[self.valid_count_key] = len(binary_values)
        sample[self.md5_key] = sample_md5(binary_values) if binary_values or not self.null_md5_on_empty else None
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

    @staticmethod
    def _dict_to_rows(samples: dict[str, list[Any]]) -> list[dict[str, Any]]:
        keys = list(samples.keys())
        if not keys:
            return []
        return [{key: samples[key][i] for key in keys} for i in range(len(samples[keys[0]]))]

    def _rows_to_dict(self, rows: list[dict[str, Any]], original_keys) -> dict[str, list[Any]]:
        keys = list(original_keys)
        for key in [self.url_key, self.bytes_key, self.md5_key, self.valid_count_key]:
            if key not in keys:
                keys.append(key)
        if not rows:
            return {key: [] for key in keys}
        return {key: [row.get(key) for row in rows] for key in keys}

    def _rows_to_table(self, rows: list[dict[str, Any]], input_schema: pa.Schema | None) -> pa.Table:
        keys = list(input_schema.names if input_schema is not None else [])
        for key in [self.url_key, self.bytes_key, self.md5_key, self.valid_count_key]:
            if key not in keys:
                keys.append(key)
        arrays = []
        fields = []
        forced_types = {
            self.url_key: pa.list_(pa.string()),
            self.bytes_key: pa.list_(pa.binary()),
            self.md5_key: pa.string(),
            self.valid_count_key: pa.int64(),
        }
        for key in keys:
            values = [row.get(key) for row in rows]
            arrow_type = forced_types.get(key) or self._input_or_inferred_type(key, values, input_schema)
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


def dedup_aligned_bytes(url_value: Any, bytes_value: Any) -> tuple[list[str], list[bytes]]:
    urls = _as_list(url_value)
    binary_items = [_to_bytes(item) for item in _as_list(bytes_value)]
    pairs = [(binary, urls[index] if index < len(urls) else None) for index, binary in enumerate(binary_items) if binary]
    pairs = sorted(pairs, key=lambda item: item[0])

    seen = set()
    deduped_urls = []
    deduped_bytes = []
    for binary, url in pairs:
        digest = hashlib.md5(binary).hexdigest()
        if digest in seen:
            continue
        seen.add(digest)
        deduped_bytes.append(binary)
        deduped_urls.append(url)
    return deduped_urls, deduped_bytes


def sample_md5(binary_items: list[bytes]) -> str:
    hasher = hashlib.md5()
    for binary in binary_items:
        hasher.update(binary)
    return hasher.hexdigest()


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if hasattr(value, "as_py"):
        value = value.as_py()
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray, memoryview)):
        value = value.tolist()
    return value if isinstance(value, list) else [value]


def _to_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, bytes):
        return value
    return None
