from typing import Dict

from ..base_op import NON_STATS_FILTERS, OPERATORS, Filter
from .general_field_filter import compile_filter_condition

OP_NAME = "stateless_field_filter"


@NON_STATS_FILTERS.register_module(OP_NAME)
@OPERATORS.register_module(OP_NAME)
class StatelessFieldFilter(Filter):
    """Filter samples directly by a field expression without writing stats."""

    def __init__(self, filter_condition: str = "", *args, **kwargs):
        """
        Initialization method.
        :param filter_condition: The filter condition as a string. It uses the
            same expression syntax as general_field_filter.
        """
        super().__init__(*args, **kwargs)
        self.filter_condition = (filter_condition or "").strip()
        self.condition = compile_filter_condition(filter_condition)
        self.ast_tree = self.condition.ast_tree

    def compute_stats_single(self, sample, context=False):
        return sample

    def process_single(self, sample: Dict) -> bool:
        return self.condition.matches(sample)
