from __future__ import annotations

import copy
from typing import Any

from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.adc_record_context import add_record_log_id_header
from data_juicer.utils.http_utils import HttpClient
from data_juicer.utils.operator_execution_callback_utils import (
    NoOpOperatorExecutionCallbackClient,
    OperatorExecutionCallbackClient,
    RECORD_KEY_FIELD,
    current_time_millis,
    has_operator_execution_callback_ctx,
)

OP_NAME = "derived_input_parameter_context_mapper"
OP_DISPLAY_NAME = "派生字段入参上下文"
CONFIG_PAGE_KEY = "derived_input_parameter_context_builder"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
INPUT_KEYS_BATCH_GET_PATH = "/openapi/state-meta/input-keys/batch-get"


@OPERATORS.register_module(OP_NAME)
class DerivedInputParameterContextMapper(Mapper):
    """Add selected derived input parameter descriptions to each sample."""

    def __init__(
        self,
        input_key_ids: list[int] | None = None,
        ctx: dict | None = None,
        timeout: float = 30.0,
        retry_attempts: int = 3,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param input_key_ids: selected input parameter key IDs.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param timeout: HTTP timeout in seconds.
        :param retry_attempts: HTTP retry attempts for ADC OpenAPI requests.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not isinstance(input_key_ids, list) or not input_key_ids:
            raise ValueError("input_key_ids must be a non-empty list")
        if retry_attempts < 0:
            raise ValueError("retry_attempts must be non-negative")

        self.input_key_ids = [int(item) for item in input_key_ids]
        self.ctx = ctx
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self._parameter_columns_cache = None
        self._operator_execution_callback_client = None

    def process_single(self, sample):
        record_started_at = current_time_millis()
        input_sample = copy.deepcopy(sample)
        try:
            sample.update(self._get_parameter_columns(sample))
        except Exception as exc:
            self._report_record_failure(
                input_sample,
                None,
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

    def _get_parameter_columns(self, sample: dict[str, Any]):
        if self._parameter_columns_cache is None:
            self._parameter_columns_cache = self._fetch_parameter_columns(sample)
        return self._parameter_columns_cache

    def _fetch_parameter_columns(self, sample: dict[str, Any]):
        ctx = self._get_ctx()
        client = HttpClient(
            endpoint=self._build_openapi_url(ctx, INPUT_KEYS_BATCH_GET_PATH),
            method="POST",
            headers=add_record_log_id_header(self._build_headers(ctx), sample),
            timeout=self.timeout,
            retry_attempts=self.retry_attempts,
        )
        result = client.request(
            json_body={
                "keyIds": self.input_key_ids,
            }
        )
        if not result["ok"]:
            raise ValueError(f"Failed to fetch input key metadata: {result['error']}")

        data = self._unwrap_openapi_response(result["data"])
        if not isinstance(data, dict) or not isinstance(
            data.get("inputParameterDetails"), list
        ):
            raise ValueError(
                "input key metadata response data must contain "
                "inputParameterDetails list"
            )
        return self._build_parameter_columns(
            data["inputParameterDetails"],
            expected_key_ids=self.input_key_ids,
        )

    @classmethod
    def _build_parameter_columns(
        cls,
        details: list[dict[str, Any]],
        expected_key_ids: list[int] | None = None,
    ):
        by_key_id = {}
        key_name_to_id = {}
        columns = {}
        for detail in details:
            if not isinstance(detail, dict):
                raise ValueError("inputParameterDetails item must be an object")
            key_id = detail.get("keyId")
            key_name = detail.get("keyNameEn")
            if key_id is None:
                raise ValueError("inputParameterDetails item must contain keyId")
            if not key_name:
                raise ValueError("inputParameterDetails item must contain keyNameEn")
            key_id = int(key_id)
            key_name = str(key_name)
            if key_id in by_key_id:
                if by_key_id[key_id] != detail:
                    logger.warning(
                        "Duplicate input key metadata differs; keeping first: keyId={}",
                        key_id,
                    )
                continue
            if key_name in key_name_to_id and key_name_to_id[key_name] != key_id:
                raise ValueError(f"duplicate keyNameEn in input key metadata: {key_name}")
            by_key_id[key_id] = detail
            key_name_to_id[key_name] = key_id
            columns[key_name] = cls._format_parameter_description(detail)

        if expected_key_ids:
            missing_key_ids = sorted(set(expected_key_ids) - set(by_key_id.keys()))
            if missing_key_ids:
                missing = ", ".join(str(key_id) for key_id in missing_key_ids)
                raise ValueError(f"input key metadata missing keyIds: {missing}")
        return columns

    @classmethod
    def _format_parameter_description(cls, detail: dict[str, Any]) -> str:
        key_name = str(detail.get("keyNameEn") or "")
        label = str(detail.get("keyNameCn") or key_name)
        parts = []

        description = cls._clean_optional_text(detail.get("description"))
        if description:
            parts.append(cls._ensure_sentence(description))

        demo_value = cls._clean_optional_text(detail.get("demoValue"))
        if demo_value:
            parts.append(cls._ensure_sentence(f"示例：{demo_value}"))

        if detail.get("multiValue") is True:
            parts.append("支持多值。")

        if not parts:
            return label
        return f"{label}：{''.join(parts)}"

    @staticmethod
    def _clean_optional_text(value: Any) -> str:
        if value in (None, ""):
            return ""
        return str(value).strip()

    @staticmethod
    def _ensure_sentence(value: str) -> str:
        if value.endswith(("。", "！", "？", ".", "!", "?")):
            return value
        return f"{value}。"

    @staticmethod
    def _unwrap_openapi_response(data):
        if isinstance(data, dict) and "code" in data:
            code = data.get("code")
            if code != 0:
                message = data.get("message") or data.get("msg") or ""
                raise ValueError(
                    f"Fetch input key metadata business failed: "
                    f"code={code}, message={message}"
                )
            return data.get("data")
        return data

    def _get_operator_execution_callback_client(self):
        if self._operator_execution_callback_client is None:
            if not has_operator_execution_callback_ctx(self.ctx):
                callback_client = NoOpOperatorExecutionCallbackClient()
            else:
                callback_client = OperatorExecutionCallbackClient(self.ctx)
            callback_client.start(operator_config=self._operator_config())
            self._operator_execution_callback_client = callback_client
        return self._operator_execution_callback_client

    def _operator_config(self):
        return {
            "input_key_ids": self.input_key_ids,
        }

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
            record_key_sample = output_sample if output_sample is not None else input_sample
            self._get_operator_execution_callback_client().report_record_failure(
                record_key=self._get_record_key(record_key_sample),
                input_data=input_sample,
                output_data=copy.deepcopy(output_sample) if output_sample is not None else None,
                error_message=error_message,
                started_at=started_at,
            )
        except Exception as exc:
            logger.warning("Failed to report record failure callback: {}", exc)

    def _get_ctx(self):
        if not isinstance(self.ctx, dict):
            raise ValueError("ctx must be provided")
        return self.ctx

    @classmethod
    def _build_openapi_url(cls, ctx: dict[str, Any], path: str) -> str:
        base_url = cls._get_ctx_required_value(ctx, "apiBase")
        return f"{base_url.rstrip('/')}/{path.lstrip('/')}"

    @classmethod
    def _build_headers(cls, ctx: dict[str, Any]) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "user-account": cls._get_ctx_required_value(ctx, "userAccount"),
            "space-id": cls._get_ctx_required_value(ctx, "spaceId"),
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
