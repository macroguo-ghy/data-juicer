from __future__ import annotations

import copy
import json
from typing import Any

from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.adc_record_context import add_record_log_id_header
from data_juicer.utils.http_utils import HttpClient
from data_juicer.utils.operator_execution_callback_utils import (
    OperatorExecutionCallbackClient,
    RECORD_KEY_FIELD,
    current_time_millis,
)

OP_NAME = "state_template_mapper"
OP_DISPLAY_NAME = "State 模板生成"
CONFIG_PAGE_KEY = "state_template_builder"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
GENERATE_JSON_PATH = "/openapi/state-meta/generate-json"


@OPERATORS.register_module(OP_NAME)
class StateTemplateMapper(Mapper):
    """Generate a state_template string from selected State metadata IDs."""

    def __init__(
        self,
        state_meta_group_items: dict[str, list[int]] | None = None,
        output_field: str = "state_template",
        ctx: dict | None = None,
        timeout: float = 30.0,
        retry_attempts: int = 3,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param state_meta_group_items: group name to selected attribute/operator IDs.
        :param output_field: field used to store the generated state template string.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param timeout: HTTP timeout in seconds.
        :param retry_attempts: HTTP retry attempts for ADC OpenAPI requests.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not isinstance(state_meta_group_items, dict) or not state_meta_group_items:
            raise ValueError("state_meta_group_items must be a non-empty dictionary")
        if not output_field:
            raise ValueError("output_field must be provided")
        if retry_attempts < 0:
            raise ValueError("retry_attempts must be non-negative")

        self.state_meta_group_items = copy.deepcopy(state_meta_group_items)
        self.output_field = output_field
        self.ctx = ctx
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self._state_template_cache = None
        self._operator_execution_callback_client = None

    def process_single(self, sample):
        ctx = self._get_ctx()
        record_started_at = current_time_millis()
        input_sample = copy.deepcopy(sample)
        try:
            state_template = self._get_state_template(ctx, sample)
            sample[self.output_field] = state_template
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
            sample,
            record_started_at,
        )
        return sample

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

    def _get_state_template(self, ctx: dict[str, Any], sample: dict[str, Any]) -> str:
        if self._state_template_cache is None:
            self._state_template_cache = self._generate_state_template(ctx, sample)
        return self._state_template_cache

    def _generate_state_template(self, ctx: dict[str, Any], sample: dict[str, Any]) -> str:
        client = HttpClient(
            endpoint=self._build_openapi_url(ctx, GENERATE_JSON_PATH),
            method="POST",
            headers=add_record_log_id_header(self._build_headers(ctx), sample),
            timeout=self.timeout,
            retry_attempts=self.retry_attempts,
        )
        result = client.request(
            json_body={
                "groupItems": self.state_meta_group_items,
            }
        )
        if not result["ok"]:
            raise ValueError(f"Failed to generate state template: {result['error']}")
        return self._parse_state_template(self._unwrap_openapi_response(result["data"]))

    @staticmethod
    def _unwrap_openapi_response(data):
        if isinstance(data, dict) and "code" in data:
            code = data.get("code")
            if code != 0:
                message = data.get("message") or data.get("msg") or ""
                raise ValueError(
                    f"Generate state template business failed: code={code}, message={message}"
                )
            return data.get("data")
        return data

    @staticmethod
    def _parse_state_template(data) -> str:
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except ValueError as exc:
                raise ValueError("state template response data must be a JSON object string") from exc
        if not isinstance(data, dict):
            raise ValueError("state template response data must be a dictionary")
        return json.dumps(data, ensure_ascii=False)

    def _get_operator_execution_callback_client(self):
        if self._operator_execution_callback_client is None:
            callback_client = OperatorExecutionCallbackClient(self.ctx)
            callback_client.start(
                operator_config={
                    "state_meta_group_items": self.state_meta_group_items,
                    "output_field": self.output_field,
                }
            )
            self._operator_execution_callback_client = callback_client
        return self._operator_execution_callback_client

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
        if not isinstance(self.ctx, dict):
            raise ValueError("ctx must be provided")
        return self.ctx

    @staticmethod
    def _build_openapi_url(ctx: dict[str, Any], path: str) -> str:
        base_url = StateTemplateMapper._get_ctx_required_value(ctx, "apiBase")
        return f"{base_url.rstrip('/')}/{path.lstrip('/')}"

    @staticmethod
    def _build_headers(ctx: dict[str, Any]) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "user-account": StateTemplateMapper._get_ctx_required_value(ctx, "userAccount"),
            "space-id": StateTemplateMapper._get_ctx_required_value(ctx, "spaceId"),
        }
        for key in ("x-tt-env", "x-use-ppe"):
            value = ctx.get(key)
            if value:
                headers[key] = str(value)
        return headers

    @staticmethod
    def _get_ctx_required_value(ctx: dict[str, Any], key: str) -> str:
        if not isinstance(ctx, dict) or not ctx.get(key):
            raise ValueError(f"ctx.{key} must be provided")
        return str(ctx[key])

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
