import hashlib
import json
import os
import time
from typing import Any, Union

import pyarrow as pa
from loguru import logger

from data_juicer.core.data import NestedDataset
from data_juicer.core.task_notification import RuntimeStatsCollector
from data_juicer.ops.condition_utils import RowCondition
from data_juicer.utils.metrics_utils import emit_dedup_rows

from ..base_op import OPERATORS, Pipeline
from .ray_basic_deduplicator import (
    DEFAULT_ACTOR_GET_RETRY_TIMES,
    DEFAULT_ACTOR_GET_TIMEOUT,
    ActorBackend,
)

OP_NAME = "ray_field_dedup_pipeline"


@OPERATORS.register_module(OP_NAME)
class RayFieldDedupPipeline(Pipeline):
    """Deduplicate rows by one field without adding Data-Juicer stats columns."""

    def __init__(
        self,
        field_key: str,
        dedup_set_num: Union[int, str] = "auto",
        actor_get_timeout: float | None = DEFAULT_ACTOR_GET_TIMEOUT,
        actor_get_retry_times: int = DEFAULT_ACTOR_GET_RETRY_TIMES,
        condition: str = "",
        id_key: str | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param field_key: field used for exact deduplication. Nested paths use dot separators.
        :param dedup_set_num: number of Ray dedup actors, or "auto".
        :param actor_get_timeout: max seconds to wait for a Ray actor result, or None to wait forever.
        :param actor_get_retry_times: number of times to wait on the same actor result before failing.
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
        self._debug_batch_count = 0
        self.backend = ActorBackend(
            dedup_set_num,
            actor_get_timeout=actor_get_timeout,
            actor_get_retry_times=actor_get_retry_times,
        )

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
        self._debug_batch_count += 1
        batch_index = self._debug_batch_count
        batch_start = time.monotonic()
        should_log = batch_index <= 3 or batch_index % 100 == 0
        if isinstance(samples, pa.Table):
            input_rows = samples.num_rows
            input_bytes = int(getattr(samples, "nbytes", 0) or 0)
            if should_log:
                self._log_process_batch_event(
                    "ray_field_dedup_batch_start",
                    batch_index=batch_index,
                    input_kind="pyarrow",
                    input_rows=input_rows,
                    input_bytes=input_bytes,
                )
            to_pylist_start = time.monotonic()
            rows = samples.to_pylist()
            to_pylist_seconds = time.monotonic() - to_pylist_start
            keep_start = time.monotonic()
            keep = self._keep_rows(rows)
            keep_seconds = time.monotonic() - keep_start
            filter_start = time.monotonic()
            output = samples.filter(pa.array(keep))
            filter_seconds = time.monotonic() - filter_start
            elapsed_seconds = time.monotonic() - batch_start
            if should_log or elapsed_seconds >= 5:
                self._log_process_batch_event(
                    "ray_field_dedup_batch_complete",
                    batch_index=batch_index,
                    input_kind="pyarrow",
                    input_rows=input_rows,
                    input_bytes=input_bytes,
                    output_rows=output.num_rows,
                    to_pylist_seconds=round(to_pylist_seconds, 3),
                    keep_seconds=round(keep_seconds, 3),
                    filter_seconds=round(filter_seconds, 3),
                    elapsed_seconds=round(elapsed_seconds, 3),
                )
            return output

        keys = list(samples.keys())
        if not keys:
            return samples

        input_rows = len(samples[keys[0]])
        if should_log:
            self._log_process_batch_event(
                "ray_field_dedup_batch_start",
                batch_index=batch_index,
                input_kind="dict",
                input_rows=input_rows,
                columns_count=len(keys),
            )
        rows_start = time.monotonic()
        rows = [{key: samples[key][idx] for key in keys} for idx in range(len(samples[keys[0]]))]
        rows_seconds = time.monotonic() - rows_start
        keep_start = time.monotonic()
        keep = self._keep_rows(rows)
        keep_seconds = time.monotonic() - keep_start
        output = {
            key: [value for value, should_keep in zip(samples[key], keep) if should_keep]
            for key in keys
        }
        elapsed_seconds = time.monotonic() - batch_start
        if should_log or elapsed_seconds >= 5:
            self._log_process_batch_event(
                "ray_field_dedup_batch_complete",
                batch_index=batch_index,
                input_kind="dict",
                input_rows=input_rows,
                columns_count=len(keys),
                output_rows=len(output[keys[0]]) if keys else 0,
                rows_build_seconds=round(rows_seconds, 3),
                keep_seconds=round(keep_seconds, 3),
                elapsed_seconds=round(elapsed_seconds, 3),
            )
        return output

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

        actor_start = time.monotonic()
        if hasattr(self.backend, "is_unique_many"):
            eligible_keep = self.backend.is_unique_many(eligible_hashes, eligible_row_ids)
        else:
            eligible_keep = [
                self.backend.is_unique(hash_value, row_id)
                for hash_value, row_id in zip(eligible_hashes, eligible_row_ids)
            ]
        actor_seconds = time.monotonic() - actor_start
        if actor_seconds >= 5:
            self._log_process_batch_event(
                "ray_field_dedup_actor_call_slow",
                eligible_rows=len(eligible_indices),
                unique_rows=sum(bool(should_keep) for should_keep in eligible_keep),
                duplicate_rows=len(eligible_indices) - sum(bool(should_keep) for should_keep in eligible_keep),
                actor_seconds=round(actor_seconds, 3),
                dedup_set_num=getattr(self.backend, "dedup_set_num", None),
            )
        self._emit_dedup_rows(len(eligible_indices), eligible_keep)
        for index, should_keep in zip(eligible_indices, eligible_keep):
            keep[index] = should_keep
        return keep

    def _log_process_batch_event(self, event: str, **payload) -> None:
        body = {
            "event": event,
            "pid": os.getpid(),
            "op_name": self._name,
            "field_key": self.field_key,
            "condition": self.condition,
            **payload,
        }
        logger.info(json.dumps(body, sort_keys=True, default=str))

    def _emit_dedup_rows(self, eligible_count: int, eligible_keep: list[bool]) -> None:
        unique_count = sum(bool(should_keep) for should_keep in eligible_keep)
        duplicate_count = eligible_count - unique_count
        tags = {"backend": "ray_actor"}
        emit_dedup_rows(
            op_name=self._name,
            field_key=self.field_key,
            event="eligible",
            count=eligible_count,
            extra_tags=tags,
        )
        emit_dedup_rows(
            op_name=self._name,
            field_key=self.field_key,
            event="unique",
            count=unique_count,
            extra_tags=tags,
        )
        emit_dedup_rows(
            op_name=self._name,
            field_key=self.field_key,
            event="duplicate",
            count=duplicate_count,
            extra_tags=tags,
        )
        collector = RuntimeStatsCollector()
        collector.increment("dedup.eligible_rows", eligible_count)
        collector.increment("dedup.unique_rows", unique_count)
        collector.increment("dedup.duplicate_rows", duplicate_count)

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
