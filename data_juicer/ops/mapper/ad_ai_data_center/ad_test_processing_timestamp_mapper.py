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
    current_time_millis,
)

OP_NAME = "ad_test_processing_timestamp_mapper"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
TEST_CARD_NOTIFICATION_TEMPLATE_ID = "AAqt1lQ72dVxK"


@OPERATORS.register_module(OP_NAME)
class AdTestProcessingTimestampMapper(Mapper):
    """Add the current processing timestamp to each sample."""

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

    def process_single(self, sample):
        record_started_at = current_time_millis()
        input_sample = copy.deepcopy(sample)
        sample[self.field_name] = time.time()
        self._report_record_success(
            input_sample,
            sample,
            record_started_at,
        )
        return sample

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

    def _report_record_success(self, input_sample, output_sample, started_at):
        if not isinstance(self.ctx, dict) or not output_sample.get(RECORD_KEY_FIELD):
            return
        try:
            self._get_operator_execution_callback_client().report_record_success(
                record_key=self._get_record_key(output_sample),
                input_data=input_sample,
                output_data=copy.deepcopy(output_sample),
                started_at=started_at,
            )
        except Exception as exc:
            logger.warning("Failed to report record success callback: {}", exc)

    def before_operator_started(self, dataset=None, context=None):
        if not isinstance(self.ctx, dict):
            return
        try:
            self._get_operator_execution_callback_client()
        except Exception as exc:
            logger.warning("Failed to start operator execution callback: {}", exc)
        self._try_send_test_card_notification(
            stage="开始",
            content=self._operator_config(),
            err_msg="",
        )

    def after_operator_finished(self, dataset=None, context=None, error=None):
        if not isinstance(self.ctx, dict):
            return
        try:
            callback_client = self._get_operator_execution_callback_client()
            if error is None:
                callback_client.finalize()
            else:
                callback_client.failed(error_message=str(error))
        except Exception as exc:
            logger.warning("Failed to finish operator execution callback: {}", exc)
        self._try_send_test_card_notification(
            stage="结束",
            content={
                "status": "SUCCESS" if error is None else "FAILED",
            },
            err_msg="" if error is None else str(error),
        )

    def _get_operator_execution_callback_client(self):
        if self._operator_execution_callback_client is None:
            callback_client = OperatorExecutionCallbackClient(self.ctx)
            callback_client.start(
                operator_config=self._operator_config()
            )
            self._operator_execution_callback_client = callback_client
        return self._operator_execution_callback_client

    def _operator_config(self):
        return {
            "field_name": self.field_name,
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
