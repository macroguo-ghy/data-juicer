import ast
from typing import Any

from data_juicer.ops.filter.general_field_filter import ExpressionTransformer


class RowCondition:
    """Small wrapper for Data-Juicer's general field filter expression syntax."""

    def __init__(self, condition: str | None = None):
        self.condition = (condition or "").strip()
        self.ast_tree = None
        if self.condition:
            try:
                self.ast_tree = ast.parse(self.condition, mode="eval")
            except SyntaxError as exc:
                raise ValueError(f"Invalid condition: {condition}") from exc

    def matches(self, sample: dict[str, Any]) -> bool:
        if not self.ast_tree:
            return True
        return bool(ExpressionTransformer(sample).transform(self.ast_tree))
