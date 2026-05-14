from __future__ import annotations

import random
from typing import Any

from pydantic import PositiveInt

from data_juicer.core.data import NestedDataset
from data_juicer.ops.base_op import OPERATORS, Pipeline

OP_NAME = "ray_random_sample_pipeline"


@OPERATORS.register_module(OP_NAME)
class RayRandomSamplePipeline(Pipeline):
    """Randomly sample up to N rows in both default and Ray executors."""

    def __init__(
        self,
        select_num: PositiveInt = 20000,
        seed: int = 0,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.select_num = int(select_num)
        self.seed = seed

    def run(self, dataset, *, exporter=None, tracer=None):
        if isinstance(dataset, NestedDataset):
            rows = dataset.to_list()
            return NestedDataset.from_list(self._sample_rows(rows, self.select_num, self.seed))

        return dataset.random_shuffle(seed=self.seed).limit(self.select_num)

    @staticmethod
    def _sample_rows(rows: list[dict[str, Any]], select_num: int, seed: int) -> list[dict[str, Any]]:
        if len(rows) <= select_num:
            return rows
        indices = list(range(len(rows)))
        random.Random(seed).shuffle(indices)
        return [rows[index] for index in indices[:select_num]]
