import hashlib
import json
from typing import Any, Union

import pyarrow as pa

from data_juicer.core.data import NestedDataset
from data_juicer.ops.condition_utils import RowCondition

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
        condition: str = "",
        id_key: str | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param field_key: field used for exact deduplication. Nested paths use dot separators.
        :param dedup_set_num: number of Ray dedup actors, or "auto".
        :param condition: row condition for deduplication. Non-matching rows pass through.
        :param id_key: optional stable row id for idempotent task retry decisions.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not field_key:
            raise ValueError("field_key must be provided")
        self.field_key = field_key
        self._field_path = field_key.split(".")
        self.condition = condition
        self._condition = RowCondition(condition)
        self.id_key = id_key
        self._id_path = id_key.split(".") if id_key else None
        self.backend = ActorBackend(dedup_set_num)

    def run(self, dataset, *, exporter=None, tracer=None):
        if isinstance(dataset, NestedDataset):
            seen = set()
            row_decisions = {}

            def keep_unique(sample):
                if not self._condition.matches(sample):
                    return True
                key = self.calculate_hash(sample)
                row_id = self._get_id_value(sample)
                if row_id is not None:
                    row_decision_key = (key, str(row_id))
                    if row_decision_key in row_decisions:
                        return row_decisions[row_decision_key]
                if key in seen:
                    if row_id is not None:
                        row_decisions[(key, str(row_id))] = False
                    return False
                seen.add(key)
                if row_id is not None:
                    row_decisions[(key, str(row_id))] = True
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
            keep = self._keep_rows(rows)
            return samples.filter(pa.array(keep))

        keys = list(samples.keys())
        if not keys:
            return samples

        rows = [{key: samples[key][idx] for key in keys} for idx in range(len(samples[keys[0]]))]
        keep = self._keep_rows(rows)
        return {
            key: [value for value, should_keep in zip(samples[key], keep) if should_keep]
            for key in keys
        }

    def _keep_rows(self, rows: list[dict[str, Any]]) -> list[bool]:
        eligible_indices = []
        eligible_hashes = []
        eligible_row_ids = []
        keep = [True] * len(rows)
        for index, row in enumerate(rows):
            if not self._condition.matches(row):
                continue
            eligible_indices.append(index)
            eligible_hashes.append(self.calculate_hash(row))
            eligible_row_ids.append(self._get_id_value(row))

        if not eligible_indices:
            return keep

        if hasattr(self.backend, "is_unique_many"):
            eligible_keep = self.backend.is_unique_many(eligible_hashes, eligible_row_ids)
        else:
            eligible_keep = [
                self.backend.is_unique(hash_value, row_id)
                for hash_value, row_id in zip(eligible_hashes, eligible_row_ids)
            ]
        for index, should_keep in zip(eligible_indices, eligible_keep):
            keep[index] = should_keep
        return keep

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

    def _get_id_value(self, sample: dict[str, Any]) -> Any:
        if not self._id_path:
            return None
        return self._get_path_value(sample, self._id_path)

    def _get_path_value(self, sample: Any, path: list[str]) -> Any:
        value: Any = sample
        for key in path:
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
