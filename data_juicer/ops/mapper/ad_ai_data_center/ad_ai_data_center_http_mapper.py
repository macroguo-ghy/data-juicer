import copy
import json

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.http_utils import HttpClient
from data_juicer.utils.notification_utils import send_test_card_notification

OP_NAME = "ad_ai_data_center_http_mapper"
CONFIG_PAGE_KEY = "adAiDataCenterHttp"
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
        self.client = HttpClient(
            endpoint=endpoint,
            method=method,
            headers=headers,
            timeout=timeout,
        )

    def process_single(self, sample):
        user = self._get_user_account(sample)
        self._send_test_card_notification(
            stage="开始",
            content=copy.deepcopy(sample),
            err_msg="",
            user=user,
        )

        payload = {
            "inputs": {
                field: sample.get(field)
                for field in self.input_fields
            }
        }
        result = self.client.request(json_body=payload)
        if result["ok"]:
            sample[self.output_field] = self._stringify_result_value(
                result["data"] if result["data"] is not None else result["text"]
            )
            sample.pop(self.error_field, None)
        else:
            sample[self.error_field] = self._stringify_result_value(result)
            sample.pop(self.output_field, None)

        self._send_test_card_notification(
            stage="结束",
            content=copy.deepcopy(sample),
            err_msg=self._extract_error_message(result),
            user=user,
        )
        return sample

    @staticmethod
    def _send_test_card_notification(stage, content, err_msg, user):
        send_test_card_notification(
            template_id=TEST_CARD_NOTIFICATION_TEMPLATE_ID,
            template_variable={
                "operator": OP_NAME,
                "stage": stage,
                "content": AdAiDataCenterHttpMapper._stringify_result_value(content),
                "errMsg": err_msg,
                "user": user,
            },
            user_email_or_account=user,
        )

    @staticmethod
    def _stringify_result_value(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return value

    @staticmethod
    def _get_user_account(sample):
        ctx = sample.get("ctx")
        if not isinstance(ctx, dict) or not ctx.get("userAccount"):
            raise ValueError("sample.ctx.userAccount must be provided")
        return ctx["userAccount"]

    @staticmethod
    def _extract_error_message(result):
        if result["ok"]:
            return ""
        error = result.get("error")
        if isinstance(error, dict):
            return error.get("message") or AdAiDataCenterHttpMapper._stringify_result_value(error)
        return AdAiDataCenterHttpMapper._stringify_result_value(error or result)
