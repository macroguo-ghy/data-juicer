from __future__ import annotations

import json
from typing import Any

from data_juicer.ops.base_op import OPERATORS, Mapper

OP_NAME = "json_extra_update_mapper"


@OPERATORS.register_module(OP_NAME)
class JsonExtraUpdateMapper(Mapper):
    """Update a JSON extra field from configured sample fields."""

    def __init__(
        self,
        extra_key: str = "extra",
        field_mappings: dict[str, str] | None = None,
        skip_empty: bool = True,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param extra_key: field containing a JSON object or dict to update.
        :param field_mappings: mapping of sample field name to extra JSON key.
        :param skip_empty: whether to skip empty source values.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not extra_key:
            raise ValueError("extra_key must be provided")
        self.extra_key = extra_key
        self.field_mappings = dict(field_mappings or {})
        self.skip_empty = skip_empty

    def process_single(self, sample):
        extra = self._extra_to_dict(sample.get(self.extra_key))
        for sample_key, extra_json_key in self.field_mappings.items():
            value = self._jsonable(sample.get(sample_key))
            if self.skip_empty and self._is_empty(value):
                continue
            extra[extra_json_key] = value
        sample[self.extra_key] = json.dumps(extra, ensure_ascii=False, default=str)
        return sample

    @classmethod
    def _extra_to_dict(cls, value: Any) -> dict[str, Any]:
        value = cls._jsonable(value)
        if value is None or value == "":
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    @classmethod
    def _jsonable(cls, value: Any) -> Any:
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
            return value.tolist()
        return value

    @classmethod
    def _is_empty(cls, value: Any) -> bool:
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
