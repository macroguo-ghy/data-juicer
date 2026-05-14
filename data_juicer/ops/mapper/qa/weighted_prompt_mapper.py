from __future__ import annotations

import hashlib
from bisect import bisect_right
from typing import Any, Mapping, Sequence

from data_juicer.ops.base_op import OPERATORS, Mapper

OP_NAME = "weighted_prompt_mapper"


@OPERATORS.register_module(OP_NAME)
class WeightedPromptMapper(Mapper):
    """Assign a deterministic weighted prompt to each sample."""

    _batched_op = True

    def __init__(
        self,
        prompt_weights: Mapping[str, float] | Sequence[Mapping[str, Any]] | None = None,
        output_key: str = "qwen_prompt",
        hash_key: str = "id",
        seed: int = 0,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if not prompt_weights:
            raise ValueError("prompt_weights must be a non-empty mapping")

        prompts: list[str] = []
        cumulative_weights: list[float] = []
        total = 0.0
        for prompt, weight in self._iter_prompt_weights(prompt_weights):
            value = float(weight)
            if value <= 0:
                raise ValueError("prompt weights must be positive")
            prompts.append(str(prompt))
            total += value
            cumulative_weights.append(total)

        self.prompts = prompts
        self.cumulative_weights = cumulative_weights
        self.total_weight = total
        self.output_key = output_key
        self.hash_key = hash_key
        self.seed = seed

    @staticmethod
    def _iter_prompt_weights(prompt_weights):
        if isinstance(prompt_weights, Mapping):
            return prompt_weights.items()
        pairs = []
        for item in prompt_weights:
            if not isinstance(item, Mapping) or "prompt" not in item or "weight" not in item:
                raise ValueError("prompt_weights list entries must contain prompt and weight")
            pairs.append((item["prompt"], item["weight"]))
        return pairs

    def process_batched(self, samples):
        row_count = self._row_count(samples)
        outputs = []
        for idx in range(row_count):
            row_value = samples.get(self.hash_key, [None] * row_count)[idx]
            outputs.append(self._choose_prompt(row_value, idx))
        samples[self.output_key] = outputs
        return samples

    @staticmethod
    def _row_count(samples) -> int:
        if not samples:
            return 0
        first_key = next(iter(samples))
        return len(samples[first_key])

    def _choose_prompt(self, row_value: Any, row_index: int) -> str:
        token = f"{self.seed}:{row_value if row_value is not None else row_index}".encode("utf-8")
        digest = hashlib.sha256(token).digest()
        fraction = int.from_bytes(digest[:8], "big") / 2**64
        target = fraction * self.total_weight
        index = bisect_right(self.cumulative_weights, target)
        if index >= len(self.prompts):
            index = len(self.prompts) - 1
        return self.prompts[index]
