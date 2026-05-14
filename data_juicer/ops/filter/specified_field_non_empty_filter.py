from __future__ import annotations

from typing import Any

from ..base_op import NON_STATS_FILTERS, OPERATORS, Filter

OP_NAME = "specified_field_non_empty_filter"


@NON_STATS_FILTERS.register_module(OP_NAME)
@OPERATORS.register_module(OP_NAME)
class SpecifiedFieldNonEmptyFilter(Filter):
    """Filter to keep samples whose specified field is non-empty."""

    _batched_op = True

    def __init__(self, field_key: str = "", *args, **kwargs):
        """
        Initialization method.

        :param field_key: Filter based on the specified field. Nested dict
            paths are separated by '.', for example 'meta.suffix'.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not field_key:
            raise ValueError("field_key must be provided")
        self.field_key = field_key
        self._field_path = field_key.split(".")

    def compute_stats_batched(self, samples):
        return samples

    def process_batched(self, samples):
        if not samples:
            return []
        first_key = next(iter(samples))
        keep_bools = [
            not self._is_empty(self._get_field_value(samples, index))
            for index in range(len(samples[first_key]))
        ]
        if self.reversed_range:
            keep_bools = [not keep for keep in keep_bools]
        return keep_bools

    def _get_field_value(self, samples: dict[str, list[Any]], index: int) -> Any:
        key = self._field_path[0]
        if key not in samples:
            raise KeyError(f"`{self.field_key}` not found: missing `{key}`")

        value = samples[key][index]
        path_prefix = key
        for key in self._field_path[1:]:
            if not isinstance(value, dict) or key not in value:
                raise KeyError(f"`{self.field_key}` not found: missing `{path_prefix}.{key}`")
            value = value[key]
            path_prefix = f"{path_prefix}.{key}"
        return value

    @classmethod
    def _is_empty(cls, value: Any) -> bool:
        if hasattr(value, "as_py"):
            value = value.as_py()
        if value is None:
            return True
        if isinstance(value, str):
            return not value.strip()
        if isinstance(value, (bytes, bytearray, memoryview)):
            return len(value) == 0
        if isinstance(value, dict):
            return not value or all(cls._is_empty(item) for item in value.values())
        if isinstance(value, (list, tuple, set, frozenset)):
            return not value or all(cls._is_empty(item) for item in value)
        return False
