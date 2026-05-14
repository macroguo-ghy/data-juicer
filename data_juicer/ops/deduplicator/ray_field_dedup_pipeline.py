import hashlib
import json
from typing import Any, Union

import pyarrow as pa

from data_juicer.core.data import NestedDataset

from ..base_op import OPERATORS, Pipeline
from .ray_basic_deduplicator import ActorBackend

OP_NAME = "ray_field_dedup_pipeline"


@OPERATORS.register_module(OP_NAME)
class RayFieldDedupPipeline(Pipeline):
    """Deduplicate rows by one field without adding Data-Juicer stats columns."""

    def __init__(
        self,
        field_key: str,
        dedup_set_num: Union[int, str] = "auto",
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param field_key: field used for exact deduplication. Nested paths use dot separators.
        :param dedup_set_num: number of Ray dedup actors, or "auto".
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not field_key:
            raise ValueError("field_key must be provided")
        self.field_key = field_key
        self._field_path = field_key.split(".")
        self.backend = ActorBackend(dedup_set_num)

    def run(self, dataset, *, exporter=None, tracer=None):
        if isinstance(dataset, NestedDataset):
            seen = set()

            def keep_unique(sample):
                key = self.calculate_hash(sample)
                if key in seen:
                    return False
                seen.add(key)
                return True

            return dataset.filter(
                keep_unique,
                num_proc=1,
                batch_size=self.batch_size,
                desc=self._name + "_process",
            )

        self.backend.prepare_for_ray_tasks()
        return dataset.map_batches(
            self.process_batched,
            batch_format="pyarrow",
            batch_size=self.batch_size,
        )

    def process_batched(self, samples):
        if isinstance(samples, pa.Table):
            rows = samples.to_pylist()
            keep = [self.backend.is_unique(self.calculate_hash(row)) for row in rows]
            return samples.filter(pa.array(keep))

        keys = list(samples.keys())
        if not keys:
            return samples

        rows = [{key: samples[key][idx] for key in keys} for idx in range(len(samples[keys[0]]))]
        keep = [self.backend.is_unique(self.calculate_hash(row)) for row in rows]
        return {
            key: [value for value, should_keep in zip(samples[key], keep) if should_keep]
            for key in keys
        }

    def calculate_hash(self, sample: dict[str, Any]) -> str:
        value = self._get_field_value(sample)
        if value is None:
            return "EMPTY"
        return hashlib.md5(self._normalize_value(value).encode("utf-8")).hexdigest()

    def _get_field_value(self, sample: dict[str, Any]) -> Any:
        value: Any = sample
        for key in self._field_path:
            if hasattr(value, "as_py"):
                value = value.as_py()
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        if hasattr(value, "as_py"):
            value = value.as_py()
        return value

    @classmethod
    def _normalize_value(cls, value: Any) -> str:
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
            value = value.tolist()
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="ignore")
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
