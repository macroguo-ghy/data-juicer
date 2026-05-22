from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from jsonargparse import Namespace
from loguru import logger

from data_juicer.core.data.config_validator import ConfigValidationError
from data_juicer.core.data.load_strategy import DataLoadStrategyRegistry
from data_juicer.core.data.ray_dataset import RayDataset


SUPPORTED_JOIN_TYPES = {"inner", "left_outer", "right_outer", "full_outer"}
DEFAULT_JOIN_NUM_PARTITIONS = 256
DEFAULT_RIGHT_SUFFIX = "_right"
CHECKPOINT_JOIN_UNSUPPORTED_MESSAGE = (
    "ray_data_checkpoint is not supported with dataset join or ray_dataset_join_pipeline. "
    "Join introduces a Ray shuffle/all-to-all boundary that is not covered by current "
    "source-to-sink checkpoint metadata."
)


@dataclass(frozen=True)
class RayJoinConfig:
    join_type: str
    num_partitions: int
    left_on: tuple[str, ...]
    right_on: tuple[str, ...] | None
    left_suffix: str | None
    right_suffix: str | None


def _as_key_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    if isinstance(value, str):
        if not value:
            raise ConfigValidationError(f"`{field_name}` must not be empty.")
        return (value,)
    if isinstance(value, list) or isinstance(value, tuple):
        if not value:
            raise ConfigValidationError(f"`{field_name}` must not be empty.")
        if not all(isinstance(item, str) and item for item in value):
            raise ConfigValidationError(f"`{field_name}` entries must be non-empty strings.")
        return tuple(value)
    raise ConfigValidationError(f"`{field_name}` must be a string or a non-empty list of strings.")


def normalize_ray_join_config(join_config: dict[str, Any]) -> RayJoinConfig:
    if not isinstance(join_config, dict):
        raise ConfigValidationError("Ray join config should be a dictionary.")
    if True in join_config and "on" not in join_config:
        join_config = dict(join_config)
        join_config["on"] = join_config[True]

    join_type = join_config.get("join_type", "inner")
    if join_type not in SUPPORTED_JOIN_TYPES:
        raise ConfigValidationError(
            f"`join_type` must be one of {sorted(SUPPORTED_JOIN_TYPES)}, got {join_type!r}."
        )

    num_partitions = join_config.get("num_partitions", DEFAULT_JOIN_NUM_PARTITIONS)
    if not isinstance(num_partitions, int) or num_partitions <= 0:
        raise ConfigValidationError("`num_partitions` must be a positive integer.")

    has_on = "on" in join_config
    has_left_on = "left_on" in join_config
    has_right_on = "right_on" in join_config
    if has_on and (has_left_on or has_right_on):
        raise ConfigValidationError("Configure either `on` or `left_on`/`right_on`, not both.")
    if has_left_on != has_right_on:
        raise ConfigValidationError("`left_on` and `right_on` must be configured together.")
    if has_on:
        left_on = _as_key_tuple(join_config["on"], "on")
        right_on = None
    elif has_left_on:
        left_on = _as_key_tuple(join_config["left_on"], "left_on")
        right_on = _as_key_tuple(join_config["right_on"], "right_on")
        if len(left_on) != len(right_on):
            raise ConfigValidationError("`left_on` and `right_on` must have the same length.")
    else:
        raise ConfigValidationError("Ray join config requires `on` or `left_on`/`right_on`.")

    left_suffix = join_config.get("left_suffix")
    right_suffix = join_config.get("right_suffix", DEFAULT_RIGHT_SUFFIX)
    for field_name, value in (("left_suffix", left_suffix), ("right_suffix", right_suffix)):
        if value is not None and not isinstance(value, str):
            raise ConfigValidationError(f"`{field_name}` must be a string or null.")

    return RayJoinConfig(
        join_type=join_type,
        num_partitions=num_partitions,
        left_on=left_on,
        right_on=right_on,
        left_suffix=left_suffix,
        right_suffix=right_suffix,
    )


def normalize_data_type_and_source(ds_config: dict[str, Any]) -> tuple[str | None, str | None]:
    data_type = ds_config.get("type", None)
    data_source = ds_config.get("source", None)
    if data_source is None and data_type not in {None, "local", "remote"}:
        data_source = data_type
        data_type = "remote"
    return data_type, data_source


def load_single_ray_dataset_from_config(ds_config: dict[str, Any], cfg: Namespace, **kwargs) -> RayDataset:
    data_type, data_source = normalize_data_type_and_source(ds_config)
    strategy_cls = DataLoadStrategyRegistry.get_strategy_class("ray", data_type, data_source)
    if strategy_cls is None:
        raise ValueError(f"No data load strategy found for {data_type} {data_source}")
    dataset = strategy_cls(ds_config, cfg=cfg).load_data(**kwargs)
    if not isinstance(dataset, RayDataset):
        raise TypeError(f"Ray loader for {data_type} {data_source} returned {type(dataset).__name__}.")
    return dataset


def apply_ray_dataset_join(left_dataset, right_dataset, join_config: RayJoinConfig):
    join_kwargs = {
        "join_type": join_config.join_type,
        "num_partitions": join_config.num_partitions,
        "on": join_config.left_on,
        "left_suffix": join_config.left_suffix,
        "right_suffix": join_config.right_suffix,
    }
    if join_config.right_on is not None:
        join_kwargs["right_on"] = join_config.right_on
    return left_dataset.join(right_dataset, **join_kwargs)


def has_dataset_join(cfg) -> bool:
    dataset_cfg = getattr(cfg, "dataset", None)
    return isinstance(dataset_cfg, dict) and bool(dataset_cfg.get("join"))


def has_pipeline_join(cfg) -> bool:
    process_list = getattr(cfg, "process", None) or []
    return any(isinstance(process, dict) and "ray_dataset_join_pipeline" in process for process in process_list)


def has_ray_join(cfg) -> bool:
    return has_dataset_join(cfg) or has_pipeline_join(cfg)


def validate_no_checkpoint_with_ray_join(cfg) -> None:
    checkpoint_cfg = getattr(cfg, "ray_data_checkpoint", None)
    if bool(getattr(checkpoint_cfg, "enabled", False)) and has_ray_join(cfg):
        raise ValueError(CHECKPOINT_JOIN_UNSUPPORTED_MESSAGE)


def log_join_dry_run_notice(kind: str) -> None:
    logger.info(
        "Ray dry-run plan requested; {} join config was validated but the right-side "
        "loader was not expanded or read.",
        kind,
    )
