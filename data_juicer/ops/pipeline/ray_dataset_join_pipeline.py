from __future__ import annotations

from typing import Any

from data_juicer.core.data import NestedDataset
from data_juicer.core.data.ray_join import (
    apply_ray_dataset_join,
    load_single_ray_dataset_from_config,
    log_join_dry_run_notice,
    normalize_ray_join_config,
)
from data_juicer.ops.base_op import OPERATORS, Pipeline

OP_NAME = "ray_dataset_join_pipeline"


@OPERATORS.register_module(OP_NAME)
class RayDatasetJoinPipeline(Pipeline):
    """Join the current Ray Dataset with an inline right-side Ray loader."""

    def __init__(
        self,
        right: dict[str, Any],
        join_type: str = "inner",
        num_partitions: int = 256,
        on: str | list[str] | None = None,
        left_on: str | list[str] | None = None,
        right_on: str | list[str] | None = None,
        left_suffix: str | None = None,
        right_suffix: str | None = "_right",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if not isinstance(right, dict):
            raise ValueError("`right` must be an inline dataset loader config dictionary.")
        if "configs" in right or "join" in right:
            raise ValueError("`right` must be a single inline dataset loader config, not a dataset block.")
        self.right = right
        join_config = {
            "join_type": join_type,
            "num_partitions": num_partitions,
            "left_suffix": left_suffix,
            "right_suffix": right_suffix,
        }
        if on is not None:
            join_config["on"] = on
        if left_on is not None:
            join_config["left_on"] = left_on
        if right_on is not None:
            join_config["right_on"] = right_on
        self.join_config = normalize_ray_join_config(join_config)
        self.cfg = None

    def set_runtime_context(self, *, cfg=None):
        self.cfg = cfg

    def run(self, dataset):
        if isinstance(dataset, NestedDataset):
            raise ValueError("ray_dataset_join_pipeline is only supported by the Ray executor.")
        if self.cfg is None:
            raise ValueError("ray_dataset_join_pipeline requires Ray executor runtime context.")

        right_dataset = load_single_ray_dataset_from_config(self.right, self.cfg)
        return apply_ray_dataset_join(dataset, right_dataset.data, self.join_config)

    def run_plan_only(self, dataset):
        log_join_dry_run_notice("pipeline")
        return dataset
