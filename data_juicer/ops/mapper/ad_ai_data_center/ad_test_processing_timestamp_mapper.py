from __future__ import annotations

import copy
import json
import time
from typing import Any

from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.notification_utils import send_test_card_notification
from data_juicer.utils.operator_execution_callback_utils import (
    OperatorExecutionCallbackClient,
    RECORD_KEY_FIELD,
)

OP_NAME = "ad_test_processing_timestamp_mapper"
NEED_CTX = True
TEST_CARD_NOTIFICATION_TEMPLATE_ID = "AAqt1lQ72dVxK"


@OPERATORS.register_module(OP_NAME)
class AdTestProcessingTimestampMapper(Mapper):
    """Add the current processing timestamp to each sample."""

    _batched_op = True

    def __init__(
        self,
        field_name: str = "processing_timestamp",
        ctx: dict | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param field_name: field name used to store the Unix timestamp.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not field_name:
            raise ValueError("field_name must be provided")
        self.field_name = field_name
        self.ctx = ctx
        self._operator_execution_callback_client = None

    def process_batched(self, samples):
        first_key = next(iter(samples.keys()))
        sample_count = len(samples[first_key])
        input_samples = [
            self._build_sample_from_batch(samples, index)
            for index in range(sample_count)
        ]

        for input_sample in input_samples:
            self._try_send_test_card_notification(
                stage="开始",
                content=input_sample,
                err_msg="",
            )

        samples[self.field_name] = [time.time() for _ in range(sample_count)]

        for index, input_sample in enumerate(input_samples):
            output_sample = self._build_sample_from_batch(samples, index)
            self._try_send_test_card_notification(
                stage="结束",
                content=output_sample,
                err_msg="",
            )
            self._report_record_success(input_sample, output_sample)
        return samples

    def _try_send_test_card_notification(self, stage: str, content: dict[str, Any], err_msg: str):
        if not isinstance(self.ctx, dict):
            return
        try:
            send_test_card_notification(
                template_id=TEST_CARD_NOTIFICATION_TEMPLATE_ID,
                template_variable={
                    "operator": OP_NAME,
                    "stage": stage,
                    "content": self._stringify_result_value(content),
                    "errMsg": err_msg,
                },
                ctx=self.ctx,
            )
        except Exception as exc:
            logger.warning("Failed to send test card notification: {}", exc)

    def _report_record_success(self, input_sample, output_sample):
        if not isinstance(self.ctx, dict) or not output_sample.get(RECORD_KEY_FIELD):
            return
        try:
            self._get_operator_execution_callback_client().report_record_success(
                record_key=self._get_record_key(output_sample),
                input_data=input_sample,
                output_data=copy.deepcopy(output_sample),
            )
        except Exception as exc:
            logger.warning("Failed to report record success callback: {}", exc)

    def before_operator_started(self, dataset=None, context=None):
        if not isinstance(self.ctx, dict):
            return
        try:
            self._get_operator_execution_callback_client()
        except Exception as exc:
            logger.warning("Failed to upsert operator execution callback: {}", exc)

    def _get_operator_execution_callback_client(self):
        if self._operator_execution_callback_client is None:
            callback_client = OperatorExecutionCallbackClient(self.ctx)
            callback_client.upsert(
                operator_config={
                    "field_name": self.field_name,
                }
            )
            self._operator_execution_callback_client = callback_client
        return self._operator_execution_callback_client

    @staticmethod
    def _build_sample_from_batch(samples, index):
        return {
            key: values[index]
            for key, values in samples.items()
        }

    @staticmethod
    def _get_record_key(sample):
        if not sample.get(RECORD_KEY_FIELD):
            raise ValueError(f"sample.{RECORD_KEY_FIELD} must be provided")
        return sample[RECORD_KEY_FIELD]

    @staticmethod
    def _stringify_result_value(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return value
