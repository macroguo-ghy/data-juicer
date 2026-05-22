import os
import shlex
from typing import List, Tuple

import numpy as np
import pyarrow
from datasets import concatenate_datasets
from jsonargparse import Namespace
from loguru import logger

from data_juicer.core.data import DJDataset, NestedDataset
from data_juicer.core.data.config_validator import ConfigValidationError
from data_juicer.core.data.data_validator import DataValidatorRegistry
from data_juicer.core.data.load_strategy import DataLoadStrategyRegistry
from data_juicer.core.data.ray_join import (
    apply_ray_dataset_join,
    log_join_dry_run_notice,
    normalize_data_type_and_source,
    normalize_ray_join_config,
    validate_no_checkpoint_with_ray_join,
)
from data_juicer.utils.file_utils import is_absolute_path
from data_juicer.utils.sample import random_sample


def _align_nested_datasets(datasets_to_merge):
    if len(datasets_to_merge) <= 1:
        return datasets_to_merge

    union_columns = []
    feature_defs = {}
    feature_signatures = {}
    for dataset in datasets_to_merge:
        for column_name, feature in dataset.features.items():
            if column_name not in union_columns:
                union_columns.append(column_name)
            signature = str(feature)
            if column_name in feature_signatures and feature_signatures[column_name] != signature:
                raise ConfigValidationError(
                    f"Conflicting column types detected for `{column_name}`: "
                    f"{feature_signatures[column_name]} vs {signature}"
                )
            feature_signatures.setdefault(column_name, signature)
            feature_defs.setdefault(column_name, feature)

    aligned = []
    for dataset in datasets_to_merge:
        for column_name in union_columns:
            if column_name not in dataset.column_names:
                dataset = dataset.add_column(column_name, [None] * len(dataset), feature=feature_defs[column_name])
        dataset = dataset.select_columns(union_columns)
        aligned.append(dataset)
    return aligned


def _add_null_column_to_ray(dataset, column_name: str, column_type):
    def build_null_column(batch: pyarrow.Table):
        return pyarrow.array([None] * len(batch), type=column_type)

    return dataset.add_column(column_name, build_null_column, batch_format="pyarrow")


def _align_ray_datasets(datasets_to_merge):
    if len(datasets_to_merge) <= 1:
        return datasets_to_merge

    union_columns = []
    column_types = {}
    type_signatures = {}
    for dataset in datasets_to_merge:
        schema = dataset.schema()
        for column_name, column_type in zip(schema.names, schema.types):
            if column_name not in union_columns:
                union_columns.append(column_name)
            signature = str(column_type)
            if column_name in type_signatures and type_signatures[column_name] != signature:
                raise ConfigValidationError(
                    f"Conflicting column types detected for `{column_name}`: "
                    f"{type_signatures[column_name]} vs {signature}"
                )
            type_signatures.setdefault(column_name, signature)
            column_types.setdefault(column_name, column_type)

    aligned = []
    for dataset in datasets_to_merge:
        current_columns = set(dataset.columns())
        for column_name in union_columns:
            if column_name not in current_columns:
                dataset = _add_null_column_to_ray(dataset, column_name, column_types[column_name])
        dataset = dataset.select_columns(union_columns)
        aligned.append(dataset)
    return aligned


class DatasetBuilder(object):
    """
    DatasetBuilder is a class that builds a dataset from a configuration.
    """

    def __init__(self, cfg: Namespace, executor_type: str = "default"):
        self.use_generated_dataset_config = False
        self.cfg = cfg
        self.executor_type = executor_type
        self.require_dataset_arg = False

        # initialize data validators
        self.validators = []
        if hasattr(cfg, "validators"):
            for validator_config in cfg.validators:
                if "type" not in validator_config:
                    raise ValueError('Validator config must have a "type" key')
                validator_type = validator_config["type"]
                validator_cls = DataValidatorRegistry.get_validator(validator_type)
                if validator_cls:
                    self.validators.append(validator_cls(validator_config))
                else:
                    raise ValueError(f"No data validator found for {validator_type}")

        # priority: generated_dataset_config > dataset_path > dataset
        if hasattr(cfg, "generated_dataset_config") and cfg.generated_dataset_config:
            self.use_generated_dataset_config = True
            self.generated_dataset_config = cfg.generated_dataset_config
            return
        elif hasattr(cfg, "dataset_path") and cfg.dataset_path:
            logger.info(f"found dataset_path setting: {cfg.dataset_path}")
            ds_configs = rewrite_cli_datapath(cfg.dataset_path)
        elif hasattr(cfg, "dataset") and cfg.dataset:
            logger.info(f"found dataset setting: {cfg.dataset}")
            ds_configs = cfg.dataset
        else:
            logger.warning(
                "No dataset setting found in configurations. Will " "check the dataset argument before loading dataset."
            )
            self.require_dataset_arg = True
            return

        # validate dataset config for type constraints
        # TODO other constraints; ray dataset only supports local, etc.
        if not isinstance(ds_configs, dict):
            raise ConfigValidationError("Dataset config should be a dictionary")
        if "configs" not in ds_configs:
            raise ConfigValidationError('Dataset config should have a "configs" key')
        if not isinstance(ds_configs["configs"], list) or len(ds_configs["configs"]) == 0:
            raise ConfigValidationError('Dataset config "configs" should be a non-empty list')
        if "max_sample_num" in ds_configs and (
            not isinstance(ds_configs["max_sample_num"], int) or ds_configs["max_sample_num"] <= 0
        ):
            raise ConfigValidationError('Dataset config "max_sample_num" should be a positive integer')
        for ds_config in ds_configs["configs"]:
            if not isinstance(ds_config, dict):
                raise ConfigValidationError("Dataset configs should be dictionaries")
        normalized_sources = [normalize_data_type_and_source(ds_config) for ds_config in ds_configs["configs"]]
        data_types = {data_type for data_type, _ in normalized_sources}
        if self.executor_type == "default" and len(data_types) > 1:
            raise ConfigValidationError("Mixture of diff types is not supported by the default dataset builder")
        if (
            self.executor_type == "default"
            and len(ds_configs["configs"]) > 1
            and data_types == {"remote"}
        ):
            raise ConfigValidationError("Multiple remote datasets are not supported by the default dataset builder")
        self.dataset_join_config = None
        self.dataset_join_strategy_names = None
        if "join" in ds_configs and ds_configs["join"]:
            if self.executor_type != "ray":
                raise ConfigValidationError("`dataset.join` is only supported with executor_type='ray'.")
            self.dataset_join_config = normalize_ray_join_config(ds_configs["join"])
            left_name = ds_configs["join"].get("left")
            right_name = ds_configs["join"].get("right")
            if not isinstance(left_name, str) or not left_name:
                raise ConfigValidationError("`dataset.join.left` must be a non-empty dataset config name.")
            if not isinstance(right_name, str) or not right_name:
                raise ConfigValidationError("`dataset.join.right` must be a non-empty dataset config name.")
            if left_name == right_name:
                raise ConfigValidationError("`dataset.join.left` and `dataset.join.right` must be different.")
            if len(ds_configs["configs"]) != 2:
                raise ConfigValidationError("`dataset.join` requires exactly two dataset configs.")
            if "max_sample_num" in ds_configs:
                raise ConfigValidationError("`dataset.join` cannot be combined with `dataset.max_sample_num`.")
            missing_names = [
                idx for idx, ds_config in enumerate(ds_configs["configs"])
                if not isinstance(ds_config.get("name"), str) or not ds_config.get("name")
            ]
            if missing_names:
                raise ConfigValidationError(
                    "`dataset.join` requires every dataset config to have a non-empty `name`; "
                    f"missing at index(es): {missing_names}."
                )
            if any("weight" in ds_config for ds_config in ds_configs["configs"]):
                raise ConfigValidationError("`dataset.join` cannot be combined with dataset config `weight`.")
            config_names = {ds_config["name"] for ds_config in ds_configs["configs"]}
            expected_names = {left_name, right_name}
            if config_names != expected_names:
                raise ConfigValidationError(
                    "`dataset.join` configs must contain exactly the referenced `left` and `right` names; "
                    f"got {sorted(config_names)}, expected {sorted(expected_names)}."
                )
            self.dataset_join_strategy_names = (left_name, right_name)
        # initialize the data load strategies
        self.load_strategies = []
        for ds_config in ds_configs["configs"]:
            # initialize data loading strategy
            data_type, data_source = normalize_data_type_and_source(ds_config)
            stra = DataLoadStrategyRegistry.get_strategy_class(self.executor_type, data_type, data_source)
            if stra is None:
                raise ValueError(f"No data load strategy found for" f" {data_type} {data_source}")
            stra = stra(ds_config, cfg=self.cfg)
            self.load_strategies.append(stra)

        # failed to initialize any load strategy
        if not self.load_strategies:
            logger.error(f"No data load strategies found for {ds_configs}")
            raise ConfigValidationError("No data load strategies found")

        # initialzie the sample numbers
        self.max_sample_num = ds_configs.get("max_sample_num", None)
        # get weights and sample numbers
        if self.max_sample_num:
            self.weights = [stra.weight for stra in self.load_strategies]
            self.sample_numbers = get_sample_numbers(self.weights, self.max_sample_num)
        else:
            self.weights = [1.0 for stra in self.load_strategies]
            self.sample_numbers = [None for stra in self.load_strategies]

    def validate_ray_data_checkpoint_support(self) -> None:
        if self.executor_type != "ray":
            return
        validate_no_checkpoint_with_ray_join(self.cfg)

        if self.require_dataset_arg:
            raise ValueError(
                "Ray Data checkpointing requires dataset configs with loaders that declare checkpoint support; "
                "runtime dataset arguments are not supported."
            )

        if self.use_generated_dataset_config:
            raise ValueError(
                "Ray Data checkpointing does not support `generated_dataset_config` because it does not "
                "declare a recoverable Ray read source boundary."
            )

        load_strategies = getattr(self, "load_strategies", None)
        if not load_strategies:
            raise ValueError("Ray Data checkpointing requires at least one configured Ray dataset loader.")

        unsupported = []
        for idx, strategy in enumerate(load_strategies):
            support = strategy.get_ray_data_checkpoint_support()
            if support.supported:
                continue
            data_type, data_source = normalize_data_type_and_source(strategy.ds_config)
            reason = support.reason or "loader does not declare Ray Data checkpoint support"
            unsupported.append(
                f"dataset.configs[{idx}] type={data_type!r} source={data_source!r} "
                f"strategy={strategy.__class__.__name__}: {reason}"
            )

        if unsupported:
            raise ValueError(
                "Ray Data checkpointing requires every Ray dataset loader to provide a recoverable "
                "source boundary for checkpoint detail metadata. Unsupported loader(s): "
                + "; ".join(unsupported)
            )

    def load_dataset(self, **kwargs) -> DJDataset:
        if self.require_dataset_arg:
            # should not get into this method
            raise ValueError(
                "Unable to load dataset; should have one of "
                "generated_dataset_config, dataset_path, or dataset "
                "in configurations, or pass the `dataset` object through `run`"
                " method"
            )

        # if generated_dataset_config present, prioritize
        if self.use_generated_dataset_config:
            return DatasetBuilder.load_dataset_by_generated_config(self.generated_dataset_config)

        if self.dataset_join_config is not None:
            if getattr(self.cfg, "ray_dry_run_plan", False):
                log_join_dry_run_notice("dataset")
            strategy_by_name = {strategy.ds_config["name"]: strategy for strategy in self.load_strategies}
            left_name, right_name = self.dataset_join_strategy_names
            left = strategy_by_name[left_name].load_data(**kwargs)
            right = strategy_by_name[right_name].load_data(**kwargs)
            for dataset in (left, right):
                for validator in self.validators:
                    validator.validate(dataset)
            from data_juicer.core.data.ray_dataset import RayDataset

            joined = apply_ray_dataset_join(left.data, right.data, self.dataset_join_config)
            return RayDataset(joined, cfg=self.cfg)

        _datasets = []
        # load datasets with sample numbers
        for stra, weight, sample_num in zip(self.load_strategies, self.weights, self.sample_numbers):
            # load dataset with its load strategy
            dataset = stra.load_data(**kwargs)

            # do data validation
            for validator in self.validators:
                validator.validate(dataset)

            # do data sampling, if necessary
            if self.max_sample_num:
                dataset = random_sample(dataset, weight, sample_num)

            _datasets.append(dataset)

        # handle data mixture
        if self.executor_type == "default":
            aligned = _align_nested_datasets(_datasets)
            return NestedDataset(concatenate_datasets(aligned))
        elif self.executor_type == "ray":
            if len(_datasets) == 1:
                return _datasets[0]
            from data_juicer.core.data.ray_dataset import RayDataset

            aligned = _align_ray_datasets([dataset.data for dataset in _datasets])
            dataset = aligned[0]
            for other in aligned[1:]:
                dataset = dataset.union(other)
            return RayDataset(dataset, cfg=self.cfg)

    @classmethod
    def load_dataset_by_generated_config(cls, generated_dataset_config):
        """
        load dataset by generated config
        """
        assert isinstance(generated_dataset_config, dict) and "type" in generated_dataset_config
        args = generated_dataset_config.copy()

        # TODO finish the auto local dataset part
        obj_name = args.pop("type")
        from data_juicer.format.formatter import FORMATTERS

        dataset = FORMATTERS.modules[obj_name](**args).load_dataset()
        return dataset


def rewrite_cli_datapath(dataset_path, max_sample_num=None) -> List:
    """
    rewrite the dataset_path from CLI into proper dataset config format
    that is compatible with YAML config style; retrofitting CLI input
    of local files and huggingface path

    :param dataset_path: a dataset file or a dataset dir or a list of
        them, e.g. `<w1> ds1.jsonl <w2> ds2_dir <w3> ds3_file.json`
    :param max_sample_num: the maximum number of samples to load
    :return: list of dataset configs
    """
    paths, weights = parse_cli_datapath(dataset_path)
    ret = {"configs": [], "max_sample_num": max_sample_num} if max_sample_num else {"configs": []}
    for p, w in zip(paths, weights):
        if os.path.isdir(p) or os.path.isfile(p):
            # local files
            ret["configs"].append({"type": "local", "path": p, "weight": w})
        elif p.startswith("s3://"):
            ret["configs"].append({"type": "remote", "source": "s3", "path": p, "weight": w})
        elif p.startswith("hdfs://"):
            ret["configs"].append({"type": "remote", "source": "hdfs", "path": p, "weight": w})
        elif not is_absolute_path(p) and not p.startswith(".") and p.count("/") <= 1:
            # remote huggingface
            ret["configs"].append({"type": "remote", "source": "huggingface", "path": p, "split": "train"})
        else:
            #
            raise ValueError(
                f"Unable to load the dataset from [{dataset_path}]. "
                f"Data-Juicer CLI mode only supports local files "
                f"w or w/o weights, huggingface path, s3:// path, or hdfs:// path"
            )
    return ret


def parse_cli_datapath(dataset_path) -> Tuple[List[str], List[float]]:
    """
    Split every dataset path and its weight.

    :param dataset_path: a dataset file or a dataset dir or a list of
        them, e.g. `<w1> ds1.jsonl <w2> ds2_dir <w3> ds3_file.json`
    :return: list of dataset path and list of weights
    """
    # Handle empty input
    if not dataset_path or not dataset_path.strip():
        return [], []

    # Use shlex to properly handle quoted strings
    try:
        tokens = shlex.split(dataset_path)
    except ValueError as e:
        raise ValueError(f"Invalid dataset path format: {e}")

    prefixes = []
    weights = []

    for i in range(len(tokens)):
        try:
            value = max(float(tokens[i]), 0.0)
            weights.append(value)
        except:  # noqa: E722
            value = tokens[i].strip()
            # if not set weight, use 1.0 as default
            if i == 0 or len(weights) == len(prefixes):
                weights.append(1.0)
            prefixes.append(value)

    return prefixes, weights


def get_sample_numbers(weights, max_sample_num):
    sample_numbers = [0] * len(weights)

    # Normalize weights
    weights = np.array(weights, dtype=np.float64)
    sum_weights = np.sum(weights)
    assert sum_weights > 0.0
    weights /= sum_weights
    sample_num_per_dataset = [int(np.ceil(max_sample_num * weight)) for weight in weights]

    # Adjust
    acc_sample_numbers = 0
    for i in range(len(sample_num_per_dataset)):
        sample_numbers[i] = min(sample_num_per_dataset[i], max_sample_num - acc_sample_numbers)
        acc_sample_numbers += sample_numbers[i]

    return sample_numbers
