from __future__ import annotations

import hashlib
import random
from typing import Any

import pyarrow as pa
from pydantic import PositiveInt

from data_juicer.core.data import NestedDataset
from data_juicer.ops.base_op import OPERATORS, Pipeline

OP_NAME = "ray_group_sample_pipeline"


@OPERATORS.register_module(OP_NAME)
class RayGroupSamplePipeline(Pipeline):
    """Sample up to N rows per group in both default and Ray executors."""

    def __init__(
        self,
        group_field_key: str = "ocr_type_en",
        select_num_per_group: PositiveInt = 150,
        seed: int = 0,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if not group_field_key:
            raise ValueError("group_field_key must be provided")
        self.group_field_key = group_field_key
        self.select_num_per_group = int(select_num_per_group)
        self.seed = seed

    def run(self, dataset):
        if isinstance(dataset, NestedDataset):
            return self._run_nested(dataset)

        return dataset.groupby(self.group_field_key).map_groups(
            self._sample_arrow_group,
            batch_format="pyarrow",
            fn_kwargs={
                "group_field_key": self.group_field_key,
                "select_num_per_group": self.select_num_per_group,
                "seed": self.seed,
            },
        )

    def _run_nested(self, dataset: NestedDataset) -> NestedDataset:
        groups: dict[Any, list[dict[str, Any]]] = {}
        for row in dataset.to_list():
            groups.setdefault(row.get(self.group_field_key), []).append(row)

        output_rows = []
        for group_value, rows in groups.items():
            output_rows.extend(
                self._sample_rows(
                    rows,
                    select_num_per_group=self.select_num_per_group,
                    seed=self.seed,
                    group_value=group_value,
                )
            )
        return NestedDataset.from_list(output_rows)

    @staticmethod
    def _sample_arrow_group(
        table: pa.Table,
        *,
        group_field_key: str,
        select_num_per_group: int,
        seed: int,
    ) -> pa.Table:
        if table.num_rows <= select_num_per_group:
            return table

        group_index = table.schema.get_field_index(group_field_key)
        group_value = table.column(group_index)[0].as_py() if group_index >= 0 and table.num_rows else None
        indices = RayGroupSamplePipeline._sample_indices(
            table.num_rows,
            select_num_per_group=select_num_per_group,
            seed=seed,
            group_value=group_value,
        )
        return table.take(pa.array(indices, type=pa.int64()))

    @staticmethod
    def _sample_rows(
        rows: list[dict[str, Any]],
        *,
        select_num_per_group: int,
        seed: int,
        group_value: Any,
    ) -> list[dict[str, Any]]:
        if len(rows) <= select_num_per_group:
            return rows
        indices = RayGroupSamplePipeline._sample_indices(
            len(rows),
            select_num_per_group=select_num_per_group,
            seed=seed,
            group_value=group_value,
        )
        return [rows[index] for index in indices]

    @staticmethod
    def _sample_indices(
        row_count: int,
        *,
        select_num_per_group: int,
        seed: int,
        group_value: Any,
    ) -> list[int]:
        group_seed = RayGroupSamplePipeline._group_seed(seed, group_value)
        rng = random.Random(group_seed)
        indices = list(range(row_count))
        rng.shuffle(indices)
        return indices[:select_num_per_group]

    @staticmethod
    def _group_seed(seed: int, group_value: Any) -> int:
        digest = hashlib.sha256(f"{seed}:{group_value}".encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big")
