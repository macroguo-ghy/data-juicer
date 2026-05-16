import copy
import json

from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.http_utils import HttpClient
from data_juicer.utils.notification_utils import send_test_card_notification
from data_juicer.utils.operator_execution_callback_utils import (
    OperatorExecutionCallbackClient,
    RECORD_KEY_FIELD,
)

OP_NAME = "ad_ai_data_center_http_mapper"
CONFIG_PAGE_KEY = "adAiDataCenterHttp"
NEED_CTX = True
TEST_CARD_NOTIFICATION_TEMPLATE_ID = "AAqt1lQ72dVxK"


@OPERATORS.register_module(OP_NAME)
class AdAiDataCenterHttpMapper(Mapper):
    """Call an HTTP endpoint with selected sample fields and write the result."""

    def __init__(
        self,
        endpoint: str | None = None,
        input_fields: list[str] | None = None,
        output_field: str = "http_result",
        error_field: str = "http_error",
        method: str = "POST",
        headers: dict[str, str] | None = None,
        ctx: dict | None = None,
        timeout: float = 30.0,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param endpoint: request URL.
        :param input_fields: sample fields copied into the JSON request body.
        :param output_field: field used to store successful response data or text.
        :param error_field: field used to store failed HTTP result.
        :param method: HTTP method.
        :param headers: HTTP request headers.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param timeout: HTTP timeout in seconds.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not endpoint:
            raise ValueError("endpoint must be provided")
        if not input_fields:
            raise ValueError("input_fields must be provided")
        if not output_field:
            raise ValueError("output_field must be provided")
        if not error_field:
            raise ValueError("error_field must be provided")

        self.input_fields = list(input_fields)
        self.output_field = output_field
        self.error_field = error_field
        self.endpoint = endpoint
        self.method = method
        self.headers = dict(headers or {})
        self.ctx = ctx
        self.timeout = timeout
        self._operator_execution_callback_client = None

    def process_single(self, sample):
        ctx = self._get_notification_ctx()
        original_sample = copy.deepcopy(sample)
        try:
            self._try_send_test_card_notification(
                stage="开始",
                content=copy.deepcopy(sample),
                err_msg="",
                ctx=ctx,
            )

            payload = {
                "inputs": {
                    field: sample.get(field)
                    for field in self.input_fields
                }
            }
            result = self._build_client(ctx).request(json_body=payload)
            if result["ok"]:
                sample[self.output_field] = self._stringify_result_value(
                    result["data"] if result["data"] is not None else result["text"]
                )
                sample.pop(self.error_field, None)
            else:
                sample[self.error_field] = self._stringify_result_value(result)
                sample.pop(self.output_field, None)

            self._try_send_test_card_notification(
                stage="结束",
                content=copy.deepcopy(sample),
                err_msg=self._extract_error_message(result),
                ctx=ctx,
            )
        except Exception as exc:
            self._report_record_failure(original_sample, sample, str(exc))
            raise
        if result["ok"]:
            self._report_record_success(original_sample, sample)
        else:
            error_message = self._extract_error_message(result)
            self._report_record_failure(original_sample, sample, error_message)
            raise ValueError(f"HTTP mapper request failed: {error_message}")
        return sample

    @staticmethod
    def _try_send_test_card_notification(stage, content, err_msg, ctx):
        try:
            send_test_card_notification(
                template_id=TEST_CARD_NOTIFICATION_TEMPLATE_ID,
                template_variable={
                    "operator": OP_NAME,
                    "stage": stage,
                    "content": AdAiDataCenterHttpMapper._stringify_result_value(content),
                    "errMsg": err_msg,
                },
                ctx=ctx,
            )
        except Exception as exc:
            logger.warning("Failed to send test card notification: {}", exc)

    def _build_client(self, ctx):
        headers = dict(self.headers)
        headers["user-account"] = self._get_user_account(ctx)
        return HttpClient(
            endpoint=self.endpoint,
            method=self.method,
            headers=headers,
            timeout=self.timeout,
        )

    @staticmethod
    def _stringify_result_value(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return value

    def _get_notification_ctx(self):
        ctx = self.ctx
        if not isinstance(ctx, dict):
            raise ValueError("ctx must be provided")
        return ctx

    @staticmethod
    def _get_user_account(ctx):
        if not ctx.get("userAccount"):
            raise ValueError("ctx.userAccount must be provided")
        return str(ctx["userAccount"])

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
                    "endpoint": self.endpoint,
                    "input_fields": self.input_fields,
                    "output_field": self.output_field,
                    "error_field": self.error_field,
                    "method": self.method,
                    "headers": self.headers,
                }
            )
            self._operator_execution_callback_client = callback_client
        return self._operator_execution_callback_client

    def _report_record_success(self, input_sample, output_sample):
        try:
            self._get_operator_execution_callback_client().report_record_success(
                record_key=self._get_record_key(output_sample),
                input_data=input_sample,
                output_data=copy.deepcopy(output_sample),
            )
        except Exception as exc:
            logger.warning("Failed to report record success callback: {}", exc)

    def _report_record_failure(self, input_sample, output_sample, error_message):
        try:
            self._get_operator_execution_callback_client().report_record_failure(
                record_key=self._get_record_key(output_sample),
                input_data=input_sample,
                error_message=error_message,
                output_data=copy.deepcopy(output_sample),
            )
        except Exception as exc:
            logger.warning("Failed to report record failure callback: {}", exc)

    @staticmethod
    def _get_record_key(sample):
        if not sample.get(RECORD_KEY_FIELD):
            raise ValueError(f"sample.{RECORD_KEY_FIELD} must be provided")
        return sample[RECORD_KEY_FIELD]

    @staticmethod
    def _extract_error_message(result):
        if result["ok"]:
            return ""
        error = result.get("error")
        if isinstance(error, dict):
            return error.get("message") or AdAiDataCenterHttpMapper._stringify_result_value(error)
        return AdAiDataCenterHttpMapper._stringify_result_value(error or result)
