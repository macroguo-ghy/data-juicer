from __future__ import annotations

from typing import Any

import pyarrow as pa

from data_juicer.core.data import NestedDataset

from ..base_op import OPERATORS, Pipeline

OP_NAME = "ray_group_required_field_filter_pipeline"


@OPERATORS.register_module(OP_NAME)
class RayGroupRequiredFieldFilterPipeline(Pipeline):
    """Keep groups that satisfy per-field-value count requirements."""

    def __init__(
        self,
        group_key: str = "id",
        field_key: str = "source",
        count_key: str = "valid_image_count",
        required_values: dict[str, int] | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param group_key: field used to group related rows.
        :param field_key: field whose values must appear in each kept group.
        :param count_key: numeric field used to validate each required value.
        :param required_values: mapping from required field value to minimum count.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not group_key:
            raise ValueError("group_key must be provided")
        if not field_key:
            raise ValueError("field_key must be provided")
        if not required_values:
            raise ValueError("required_values must be a non-empty mapping")
        self.group_key = group_key
        self.field_key = field_key
        self.count_key = count_key
        self.required_values = {str(value): int(min_count) for value, min_count in required_values.items()}

    def run(self, dataset, *, exporter=None, tracer=None):
        if isinstance(dataset, NestedDataset):
            rows = dataset.to_list()
            keep_groups = self._complete_group_keys(rows)
            return dataset.filter(
                lambda sample: sample.get(self.group_key) in keep_groups,
                num_proc=1,
                batch_size=self.batch_size,
                desc=self._name + "_process",
            )

        return dataset.groupby(self.group_key).map_groups(
            self._filter_arrow_group,
            batch_format="pyarrow",
            zero_copy_batch=False,
        )

    def _filter_arrow_group(self, table: pa.Table) -> pa.Table:
        rows = table.to_pylist()
        if self._group_satisfies_requirements(rows):
            return table
        return table.slice(0, 0)

    def _complete_group_keys(self, rows: list[dict[str, Any]]) -> set[Any]:
        rows_by_group: dict[Any, list[dict[str, Any]]] = {}
        for row in rows:
            rows_by_group.setdefault(row.get(self.group_key), []).append(row)
        return {
            group_key
            for group_key, group_rows in rows_by_group.items()
            if self._group_satisfies_requirements(group_rows)
        }

    def _group_satisfies_requirements(self, rows: list[dict[str, Any]]) -> bool:
        value_counts: dict[str, int] = {}
        for row in rows:
            value = row.get(self.field_key)
            if value is None:
                continue
            count = self._as_int(row.get(self.count_key))
            value_counts[str(value)] = max(value_counts.get(str(value), 0), count)
        return all(value_counts.get(value, 0) >= min_count for value, min_count in self.required_values.items())

    @staticmethod
    def _as_int(value: Any) -> int:
        if hasattr(value, "as_py"):
            value = value.as_py()
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
