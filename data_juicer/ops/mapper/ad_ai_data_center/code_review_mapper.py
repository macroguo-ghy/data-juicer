from __future__ import annotations

import copy
import json
from typing import Any

from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.notification_utils import send_test_card_notification
from data_juicer.utils.operator_execution_callback_utils import (
    NoOpOperatorExecutionCallbackClient,
    OperatorExecutionCallbackClient,
    RECORD_KEY_FIELD,
    current_time_millis,
    has_operator_execution_callback_ctx,
)
from data_juicer.utils.python_script_utils import PythonScriptRunner

OP_NAME = "code_review_mapper"
OP_DISPLAY_NAME = "代码审核"
CONFIG_PAGE_KEY = "code_review_builder"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
TEST_CARD_NOTIFICATION_TEMPLATE_ID = "AAqt1lQ72dVxK"


@OPERATORS.register_module(OP_NAME)
class CodeReviewMapper(Mapper):
    """Review a configured sample field with trusted Python code."""

    def __init__(
        self,
        input_field: str | None = None,
        status_field: str = "review_status",
        reason_field: str = "review_reason",
        python_code: str | None = None,
        entrypoint: str = "review_row",
        ctx: dict | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param input_field: sample field to review.
        :param status_field: output field for boolean review result.
        :param reason_field: output field for review failure reason.
        :param python_code: trusted Python script defining the review entrypoint.
        :param entrypoint: function name to call. The function receives
            ``(value, row, context)`` and returns ``(passed, reason)`` or a dict.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not input_field:
            raise ValueError("input_field must be provided")
        if not status_field:
            raise ValueError("status_field must be provided")
        if not reason_field:
            raise ValueError("reason_field must be provided")
        if not entrypoint:
            raise ValueError("entrypoint must be provided")

        self.input_field = input_field
        self.status_field = status_field
        self.reason_field = reason_field
        self.entrypoint = entrypoint
        self.ctx = ctx
        self.runner = PythonScriptRunner(
            python_code=python_code or "",
            entrypoint=entrypoint,
            require_dict_result=False,
        )
        self._operator_execution_callback_client = None

    def process_single(self, sample):
        ctx = self._get_ctx()
        record_started_at = current_time_millis()
        input_sample = copy.deepcopy(sample)
        try:
            if self.input_field not in sample:
                raise ValueError(f"sample.{self.input_field} must be provided")

            output_sample = copy.deepcopy(sample)
            result = self.runner.run_with_args(
                copy.deepcopy(sample[self.input_field]),
                copy.deepcopy(sample),
                {
                    "ctx": ctx,
                    "operator": OP_NAME,
                    "input_field": self.input_field,
                },
            )
            passed, reason = self._normalize_review_result(result)
            output_sample[self.status_field] = passed
            output_sample[self.reason_field] = reason
        except Exception as exc:
            self._report_record_failure(
                input_sample,
                sample,
                str(exc),
                record_started_at,
            )
            raise

        self._report_record_success(
            input_sample,
            output_sample,
            record_started_at,
        )
        return output_sample

    def before_operator_started(self, dataset=None, context=None):
        try:
            self._get_operator_execution_callback_client()
        except Exception as exc:
            logger.warning("Failed to start operator execution callback: {}", exc)
        # Test card notifications are temporarily disabled for ADC business operators.

    def after_operator_finished(self, dataset=None, context=None, error=None):
        try:
            callback_client = self._get_operator_execution_callback_client()
            if error is None:
                callback_client.finalize()
            else:
                callback_client.failed(error_message=str(error))
        except Exception as exc:
            logger.warning("Failed to finish operator execution callback: {}", exc)
        # Finish notifications are temporarily disabled for ADC business operators.

    def _get_operator_execution_callback_client(self):
        if self._operator_execution_callback_client is None:
            if not has_operator_execution_callback_ctx(self.ctx):
                callback_client = NoOpOperatorExecutionCallbackClient()
            else:
                callback_client = OperatorExecutionCallbackClient(self.ctx)
            callback_client.start(
                operator_config=self._operator_config()
            )
            self._operator_execution_callback_client = callback_client
        return self._operator_execution_callback_client

    def _operator_config(self):
        return {
            "input_field": self.input_field,
            "status_field": self.status_field,
            "reason_field": self.reason_field,
            "entrypoint": self.entrypoint,
        }

    @staticmethod
    def _normalize_review_result(result):
        if isinstance(result, tuple) and len(result) == 2:
            passed, reason = result
        elif isinstance(result, dict):
            passed = result.get("passed")
            reason = result.get("reason", "")
        else:
            raise ValueError("review result must be (passed, reason) or a dictionary")

        if not isinstance(passed, bool):
            raise ValueError("review passed must be a boolean")
        if not isinstance(reason, str):
            raise ValueError("review reason must be a string")
        return passed, reason

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
        try:
            output_record_key = self._maybe_get_record_key(output_sample)
            callback_kwargs = {
                "record_key": output_record_key,
                "input_data": input_sample,
                "output_data": copy.deepcopy(output_sample),
                "started_at": started_at,
            }
            if output_record_key is None:
                callback_kwargs["fallback_record_key"] = self._get_record_key(input_sample)
            self._get_operator_execution_callback_client().report_record_success(**callback_kwargs)
        except Exception as exc:
            logger.warning("Failed to report record success callback: {}", exc)

    def _report_record_failure(self, input_sample, output_sample, error_message, started_at):
        try:
            self._get_operator_execution_callback_client().report_record_failure(
                record_key=self._get_record_key(input_sample),
                input_data=input_sample,
                output_data=None,
                error_message=error_message,
                started_at=started_at,
            )
        except Exception as exc:
            logger.warning("Failed to report record failure callback: {}", exc)

    def _get_ctx(self):
        return self.ctx if isinstance(self.ctx, dict) else None

    @staticmethod
    def _stringify_result_value(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return value

    @staticmethod
    def _get_record_key(sample: dict[str, Any]):
        if not sample.get(RECORD_KEY_FIELD):
            raise ValueError(f"sample.{RECORD_KEY_FIELD} must be provided")
        return sample[RECORD_KEY_FIELD]

    @staticmethod
    def _maybe_get_record_key(sample: dict[str, Any] | None):
        if not isinstance(sample, dict):
            return None
        value = sample.get(RECORD_KEY_FIELD)
        return value if value not in (None, "") else None
