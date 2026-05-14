from __future__ import annotations

import hashlib
import json
from typing import Any

from data_juicer.utils.constant import Fields

from ..base_op import OPERATORS, Filter

OP_NAME = "numeric_probability_sampling_filter"


@OPERATORS.register_module(OP_NAME)
class NumericProbabilitySamplingFilter(Filter):
    """Keep high scores and deterministically sample middle scores."""

    def __init__(
        self,
        field_key: str = "text_richness_score",
        hash_key: str | None = "id",
        low_threshold: float = 0.2,
        high_threshold: float = 0.5,
        base_sample_prob: float = 0.01,
        sampling_power: float = 2.0,
        seed: str = "data-juicer",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if not 0.0 <= base_sample_prob <= 1.0:
            raise ValueError("base_sample_prob must be in [0, 1]")
        if low_threshold > high_threshold:
            raise ValueError("low_threshold must be <= high_threshold")
        self.field_key = field_key
        self.hash_key = hash_key
        self.low_threshold = low_threshold
        self.high_threshold = high_threshold
        self.base_sample_prob = base_sample_prob
        self.sampling_power = sampling_power
        self.seed = seed

    def compute_stats_single(self, sample):
        score = self._get_nested_value(sample, self.field_key)
        sample[Fields.stats][self.field_key] = score
        sample[Fields.stats][self._hash_stats_key] = self._stable_uniform(sample, score)
        return sample

    def process_single(self, sample):
        score = self._as_float(sample[Fields.stats].get(self.field_key))
        if score is None or score <= self.low_threshold:
            return False
        if score > self.high_threshold:
            return True
        prob_factor = score**self.sampling_power
        keep_prob = self.base_sample_prob + (1.0 - self.base_sample_prob) * prob_factor
        return sample[Fields.stats].get(self._hash_stats_key, 1.0) < keep_prob

    @property
    def _hash_stats_key(self) -> str:
        return f"{self.field_key}__stable_sample"

    @staticmethod
    def _as_float(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _get_nested_value(sample: dict[str, Any], field_key: str) -> Any:
        value = sample
        for key in field_key.split("."):
            if not isinstance(value, dict) or key not in value:
                return None
            value = value[key]
        return value

    def _stable_uniform(self, sample: dict[str, Any], score: Any) -> float:
        if self.hash_key:
            identity = self._get_nested_value(sample, self.hash_key)
        else:
            identity = None
        if identity is None:
            identity = sample
        identity_text = json.dumps(identity, ensure_ascii=False, sort_keys=True, default=str)
        digest = hashlib.sha256(f"{self.seed}:{identity_text}:{score}".encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") / float(1 << 64)
