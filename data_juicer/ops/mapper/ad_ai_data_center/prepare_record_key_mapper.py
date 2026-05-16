from __future__ import annotations

import copy
import hashlib
import json
from typing import Any

from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.operator_execution_callback_utils import (
    OperatorExecutionCallbackClient,
    RECORD_KEY_FIELD,
    current_time_millis,
)

OP_NAME = "prepare_record_key_mapper"
NEED_CTX = True
INTERNAL_FIELDS = {"ctx", RECORD_KEY_FIELD}


@OPERATORS.register_module(OP_NAME)
class PrepareRecordKeyMapper(Mapper):
    """Prepare a stable internal record key for downstream ADC status callbacks."""

    def __init__(
        self,
        source_fields: list[str] | None = None,
        overwrite: bool = False,
        ctx: dict | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param source_fields: optional fields used to build the record key.
            If empty, all non-internal sample fields are used.
        :param overwrite: whether to overwrite an existing record key.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        self.source_fields = list(source_fields or [])
        self.overwrite = overwrite
        self.ctx = ctx
        self._operator_execution_callback_client = None

    def process_single(self, sample: dict[str, Any]):
        record_started_at = current_time_millis()
        original_sample = copy.deepcopy(sample)
        try:
            if not self.overwrite and sample.get(RECORD_KEY_FIELD):
                output_sample = sample
            else:
                source = self._build_source(sample)
                sample[RECORD_KEY_FIELD] = self._stable_hash(source)
                output_sample = sample
        except Exception as exc:
            self._report_record_failure(
                original_sample,
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
                output_data=copy.deepcopy(output_sample),
                started_at=started_at,
            )
        except Exception as exc:
            logger.warning("Failed to report record success callback: {}", exc)

    def _report_record_failure(self, input_sample, output_sample, exc, started_at):
        try:
            record_key = output_sample.get(RECORD_KEY_FIELD)
            if not record_key:
                return
            callback_client = self._get_operator_execution_callback_client()
            callback_client.report_record_failure(
                record_key=record_key,
                input_data=input_sample,
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
                    "source_fields": self.source_fields,
                    "overwrite": self.overwrite,
                }
            )
            self._operator_execution_callback_client = callback_client
        return self._operator_execution_callback_client

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
    def _stable_hash(value) -> str:
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
