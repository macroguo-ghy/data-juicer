from __future__ import annotations

import base64
import copy
import hashlib
import json
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.adc_record_context import ADC_LOG_ID_FIELD
from data_juicer.utils.operator_execution_callback_utils import (
    OperatorExecutionCallbackClient,
    RECORD_KEY_FIELD,
    current_time_millis,
)

OP_NAME = "prepare_record_key_mapper"
OP_DISPLAY_NAME = "生成唯一键"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
INTERNAL_FIELDS = {"ctx", RECORD_KEY_FIELD, ADC_LOG_ID_FIELD}
FAILED_RECORD_KEY_PREFIX = "prepare_record_key_failed:"


@OPERATORS.register_module(OP_NAME)
class PrepareRecordKeyMapper(Mapper):
    """Prepare a stable internal record key for downstream ADC status callbacks."""

    _requirements = ["bytedlogid"]

    def __init__(
        self,
        source_field: str | None = None,
        source_fields: list[str] | None = None,
        overwrite: bool = False,
        ctx: dict | None = None,
        *args,
        repartition_num_blocks: int | None = None,
        **kwargs,
    ):
        """
        Initialization method.

        :param source_field: optional sample field whose value is reused as
            the record key. If set, this takes precedence over source_fields.
        :param source_fields: optional fields used to build the record key.
            If empty, all non-internal sample fields are used.
        :param overwrite: whether to overwrite an existing record key.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param repartition_num_blocks: Ray Dataset block count before generating keys.
            None means num_proc * 4 when num_proc is positive.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if (
            repartition_num_blocks is not None
            and (not isinstance(repartition_num_blocks, int) or repartition_num_blocks <= 0)
        ):
            raise ValueError("repartition_num_blocks must be a positive integer")

        self.source_field = source_field
        self.source_fields = list(source_fields or [])
        self.overwrite = overwrite
        self.ctx = ctx
        self.repartition_num_blocks = repartition_num_blocks
        self._operator_execution_callback_client = None

    def run(self, dataset, *, exporter=None, tracer=None):
        return super().run(
            self.prepare_ray_dataset(dataset),
            exporter=exporter,
            tracer=tracer,
        )

    def prepare_ray_dataset(self, dataset):
        repartition_num_blocks = self._effective_repartition_num_blocks()
        if repartition_num_blocks is None:
            return dataset
        repartition = getattr(dataset, "repartition", None)
        if not callable(repartition):
            return dataset
        logger.info(
            "PrepareRecordKeyMapper repartition input to {} blocks before key generation",
            repartition_num_blocks,
        )
        return repartition(num_blocks=repartition_num_blocks, shuffle=False)

    def _effective_repartition_num_blocks(self) -> int | None:
        if self.repartition_num_blocks is not None:
            return self.repartition_num_blocks
        num_proc = getattr(self, "num_proc", None)
        if isinstance(num_proc, int) and num_proc > 0:
            return num_proc * 4
        return None

    def process_single(self, sample: dict[str, Any]):
        record_started_at = current_time_millis()
        try:
            original_sample = copy.deepcopy(sample)
            if not self.overwrite and sample.get(RECORD_KEY_FIELD):
                output_sample = self._put_record_key_first(sample, sample[RECORD_KEY_FIELD])
            else:
                record_key = self._build_record_key(sample)
                output_sample = self._put_record_key_first(sample, record_key)
        except Exception as exc:
            self._report_record_failure(
                locals().get("original_sample", sample),
                sample,
                exc,
                record_started_at,
            )
            raise
        self._report_record_success(
            original_sample,
            output_sample,
            record_started_at,
        )
        return output_sample

    def _report_record_success(self, input_sample, output_sample, started_at):
        try:
            callback_client = self._get_operator_execution_callback_client()
            callback_client.report_record_success(
                record_key=output_sample[RECORD_KEY_FIELD],
                input_data=input_sample,
                output_data=self._safe_deepcopy(output_sample),
                started_at=started_at,
            )
        except Exception as exc:
            logger.warning("Failed to report record success callback: {}", exc)

    def _report_record_failure(self, input_sample, output_sample, exc, started_at):
        try:
            record_key = output_sample.get(RECORD_KEY_FIELD) or self._fallback_record_key(input_sample)
            callback_client = self._get_operator_execution_callback_client()
            callback_client.report_record_failure(
                record_key=record_key,
                input_data=input_sample,
                output_data=self._safe_deepcopy(output_sample),
                error_message=str(exc),
                started_at=started_at,
            )
        except Exception as callback_exc:
            logger.warning("Failed to report record failure callback: {}", callback_exc)

    def before_operator_started(self, dataset=None, context=None):
        try:
            self._get_operator_execution_callback_client()
        except Exception as exc:
            logger.warning("Failed to start operator execution callback: {}", exc)

    def after_operator_finished(self, dataset=None, context=None, error=None):
        try:
            callback_client = self._get_operator_execution_callback_client()
            if error is None:
                callback_client.finalize()
            else:
                callback_client.failed(error_message=str(error))
        except Exception as exc:
            logger.warning("Failed to finish operator execution callback: {}", exc)

    def _get_operator_execution_callback_client(self):
        if self._operator_execution_callback_client is None:
            callback_client = OperatorExecutionCallbackClient(self.ctx)
            callback_client.start(
                operator_config={
                    "source_field": self.source_field,
                    "source_fields": self.source_fields,
                    "overwrite": self.overwrite,
                    "repartition_num_blocks": self.repartition_num_blocks,
                }
            )
            self._operator_execution_callback_client = callback_client
        return self._operator_execution_callback_client

    def _build_record_key(self, sample: dict[str, Any]) -> str:
        if self.source_field:
            value = sample.get(self.source_field)
            if value in (None, ""):
                raise ValueError(f"sample.{self.source_field} must be provided")
            return str(value)
        source = self._build_source(sample)
        return self._stable_hash(source)

    def _build_source(self, sample: dict[str, Any]):
        if self.source_fields:
            return {
                field: sample.get(field)
                for field in self.source_fields
            }
        return {
            key: value
            for key, value in sample.items()
            if key not in INTERNAL_FIELDS
        }

    @staticmethod
    def _put_record_key_first(sample: dict[str, Any], record_key: str) -> dict[str, Any]:
        sample.pop(RECORD_KEY_FIELD, None)
        sample.pop(ADC_LOG_ID_FIELD, None)
        return {
            RECORD_KEY_FIELD: record_key,
            ADC_LOG_ID_FIELD: PrepareRecordKeyMapper._generate_log_id(),
            **sample,
        }

    @staticmethod
    def _generate_log_id() -> str:
        try:
            import logid
        except ImportError as exc:
            raise ImportError(
                "bytedlogid is required to generate ADC log id. "
                "Install it with `pip install bytedlogid -i https://bytedpypi.byted.org/simple/`."
            ) from exc
        return str(logid.generate_v2())

    @staticmethod
    def _stable_hash(value) -> str:
        normalized_value = PrepareRecordKeyMapper._normalize_for_hash(value)
        payload = json.dumps(
            normalized_value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return PrepareRecordKeyMapper._sha256_hex(payload)

    @staticmethod
    def _fallback_record_key(value) -> str:
        try:
            normalized_value = PrepareRecordKeyMapper._normalize_for_hash(value)
            payload = PrepareRecordKeyMapper._stable_json_payload(normalized_value)
        except Exception:
            payload = json.dumps(
                {
                    "__type__": "unserializable_sample",
                    "class": PrepareRecordKeyMapper._class_name(value),
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        return FAILED_RECORD_KEY_PREFIX + PrepareRecordKeyMapper._sha256_hex(payload)

    @staticmethod
    def _normalize_for_hash(value):
        if value is None or isinstance(value, (str, bool, int)):
            return value
        if isinstance(value, float):
            if math.isnan(value):
                return "nan"
            if math.isinf(value):
                return "inf" if value > 0 else "-inf"
            return value
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (bytes, bytearray, memoryview)):
            return base64.b64encode(bytes(value)).decode("ascii")
        if isinstance(value, dict):
            if all(isinstance(key, str) for key in value.keys()):
                return {
                    key: PrepareRecordKeyMapper._normalize_for_hash(item)
                    for key, item in value.items()
                }
            normalized_items = [
                [
                    PrepareRecordKeyMapper._normalize_for_hash(key),
                    PrepareRecordKeyMapper._normalize_for_hash(item),
                ]
                for key, item in value.items()
            ]
            return sorted(normalized_items, key=PrepareRecordKeyMapper._stable_json_payload)
        if isinstance(value, (list, tuple)):
            return [
                PrepareRecordKeyMapper._normalize_for_hash(item)
                for item in value
            ]
        if isinstance(value, (set, frozenset)):
            normalized_items = [
                PrepareRecordKeyMapper._normalize_for_hash(item)
                for item in value
            ]
            return sorted(normalized_items, key=PrepareRecordKeyMapper._stable_json_payload)
        if type(value).__str__ is object.__str__ and type(value).__repr__ is object.__repr__:
            return f"<{PrepareRecordKeyMapper._class_name(value)}>"
        return str(value)

    @staticmethod
    def _stable_json_payload(value) -> str:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _class_name(value) -> str:
        value_type = type(value)
        return f"{value_type.__module__}.{value_type.__qualname__}"

    @staticmethod
    def _sha256_hex(payload: str) -> str:
        return hashlib.sha256(memoryview(payload.encode("utf-8"))).hexdigest()

    @staticmethod
    def _safe_deepcopy(value):
        try:
            return copy.deepcopy(value)
        except Exception:
            return value
