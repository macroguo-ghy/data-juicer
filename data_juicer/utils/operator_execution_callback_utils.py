from __future__ import annotations

import base64
import time
from datetime import date, datetime
from decimal import Decimal
from enum import IntEnum
from typing import Any

from data_juicer.utils.http_utils import HttpClient
from data_juicer.utils.adc_record_context import (
    ADC_LOG_ID_FIELD,
    TT_LOG_ID_HEADER,
)

OPERATOR_EXECUTION_API_PREFIX = "/openapi/synthesis/operator-execution"
RECORD_KEY_FIELD = "__adc_record_key"
JSON_SAFE_MAX_DEPTH = 20
JSON_SAFE_MAX_SEQUENCE_ITEMS = 10


def current_time_millis() -> int:
    return int(time.time() * 1000)


def current_finished_time_millis(started_at: int) -> int:
    return max(current_time_millis(), int(started_at) + 1)


class OperatorExecutionStatus(IntEnum):
    RUNNING = 1
    SUCCESS = 2
    FAILED = 3


def has_operator_execution_callback_ctx(ctx: dict[str, Any] | None) -> bool:
    return (
        isinstance(ctx, dict)
        and ctx.get("apiBase") not in (None, "")
        and ctx.get("userAccount") not in (None, "")
        and ctx.get("spaceId") not in (None, "")
    )


class NoOpOperatorExecutionCallbackClient:
    """No-op callback client used when platform callback context is absent."""

    operator_execution_id = None

    def start(self, **kwargs) -> None:
        return None

    def report_record_success(self, **kwargs) -> dict[str, Any]:
        return {}

    def report_record_failure(self, **kwargs) -> dict[str, Any]:
        return {}

    def report_record(self, **kwargs) -> dict[str, Any]:
        return {}

    def failed(self, **kwargs) -> dict[str, Any]:
        return {}

    def finalize(self, **kwargs) -> dict[str, Any]:
        return {}


class OperatorExecutionCallbackClient:
    """Client for data synthesis operator execution callback APIs.

    Intended usage:
    - Business operators with ``NEED_CTX = True`` call ``start`` before
      processing records and keep the returned ``operatorExecutionId``.
    - Business operators call ``report_record_success`` or
      ``report_record_failure`` after each record. A failed record is only a
      record-level failure and does not imply operator-level FAILED.
    - Execution engines call ``finalize`` only after the current operator output
      is fully consumed. Business operators should not infer that by themselves.
    - Execution engines call ``failed`` when the operator as a whole fails, not
      for ordinary single-record failures.
    """

    def __init__(
        self,
        ctx: dict[str, Any],
        operator_execution_id: int | None = None,
        timeout: float = 30.0,
        retry_attempts: int = 5,
    ):
        if retry_attempts < 0:
            raise ValueError("retry_attempts must be non-negative")
        self.ctx = ctx
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.operator_execution_id = operator_execution_id
        self.enabled = self._has_callback_ctx()
        self.api_base = self._get_api_base() if self.enabled else ""
        self.user_account = (
            self._get_ctx_required_value("userAccount") if self.enabled else ""
        )

    def start(
        self,
        *,
        operator_config: dict[str, Any] | None = None,
        started_at: int | None = None,
        properties: dict[str, Any] | None = None,
    ) -> int | None:
        """Start the operator execution row and cache its ID.

        The backend makes this API idempotent for the same operator execution
        context, so retrying start does not create duplicate execution rows.
        """
        if not self.enabled:
            return None
        if started_at is None:
            started_at = current_time_millis()
        payload = {
            "synthesisInstanceId": self._get_ctx_required_value("synthesisInstanceId"),
            "operatorIndex": self._get_ctx_required_value("operatorIndex"),
            "operatorName": self._get_ctx_required_value("operatorName"),
            "operatorConfig": self._to_json_safe_value(operator_config or {}),
        }
        self._add_optional_ctx_value(payload, "taskId")
        self._add_optional_ctx_value(payload, "taskVersion")
        self._add_optional_ctx_value(payload, "flowInstanceId")
        self._add_optional_ctx_value(payload, "flowNodeId")
        self._add_optional_ctx_value(payload, "operatorType")
        self._add_optional_value(payload, "startedAt", started_at)
        self._add_optional_value(payload, "properties", properties)

        result = self._post("start", payload)
        operator_execution_id = self._extract_operator_execution_id(result)
        self.operator_execution_id = operator_execution_id
        return operator_execution_id

    def report_record_success(
        self,
        *,
        record_key: str,
        input_data: Any | None = None,
        output_data: Any | None = None,
        properties: dict[str, Any] | None = None,
        started_at: int | None = None,
        finished_at: int | None = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {}
        if started_at is not None and finished_at is None:
            finished_at = current_finished_time_millis(started_at)
        return self.report_record(
            record_key=record_key,
            status=OperatorExecutionStatus.SUCCESS,
            input_data=input_data,
            output_data=output_data,
            properties=properties,
            started_at=started_at,
            finished_at=finished_at,
        )

    def report_record_failure(
        self,
        *,
        record_key: str,
        input_data: Any | None = None,
        output_data: Any | None = None,
        error_message: str | None = None,
        properties: dict[str, Any] | None = None,
        started_at: int | None = None,
        finished_at: int | None = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {}
        if started_at is not None and finished_at is None:
            finished_at = current_finished_time_millis(started_at)
        return self.report_record(
            record_key=record_key,
            status=OperatorExecutionStatus.FAILED,
            input_data=input_data,
            output_data=output_data,
            error_message=error_message,
            properties=properties,
            started_at=started_at,
            finished_at=finished_at,
        )

    def report_record(
        self,
        *,
        record_key: str,
        status: OperatorExecutionStatus | int,
        input_data: Any | None = None,
        output_data: Any | None = None,
        error_message: str | None = None,
        properties: dict[str, Any] | None = None,
        started_at: int | None = None,
        finished_at: int | None = None,
    ) -> dict[str, Any]:
        """Report the execution result of one input record.

        This method upserts by ``recordKey`` on the server side, so reruns can
        safely update the same record result.
        """
        if not self.enabled:
            return {}
        payload = {
            "operatorExecutionId": self._get_operator_execution_id(),
            "recordKey": self._get_required_value(record_key, "record_key"),
            "status": int(status),
        }
        self._add_optional_value(payload, "inputData", input_data)
        self._add_optional_value(payload, "outputData", output_data)
        self._add_optional_value(payload, "errorMessage", error_message)
        self._add_optional_value(payload, "properties", properties)
        self._add_optional_value(payload, "startedAt", started_at)
        self._add_optional_value(payload, "finishedAt", finished_at)
        return self._post(
            "record",
            payload,
            record_log_id=self._extract_record_log_id(input_data, output_data),
        )

    def failed(
        self,
        *,
        finished_at: int | None = None,
        error_message: str | None = None,
        properties: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Finalize operator failure after the execution engine observes it."""
        if not self.enabled:
            return {}

        payload = {
            "operatorExecutionId": self._get_operator_execution_id(),
        }
        self._add_optional_value(payload, "finishedAt", finished_at)
        self._add_optional_value(payload, "errorMessage", error_message)
        self._add_optional_value(payload, "properties", properties)
        return self._post("failed", payload)

    def finalize(
        self,
        *,
        finished_at: int | None = None,
        properties: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Finalize operator success after the execution engine consumes output."""
        if not self.enabled:
            return {}
        payload = {
            "operatorExecutionId": self._get_operator_execution_id(),
        }
        self._add_optional_value(payload, "finishedAt", finished_at)
        self._add_optional_value(payload, "properties", properties)
        return self._post("finalize", payload)

    def _post(
        self,
        path: str,
        payload: dict[str, Any],
        record_log_id: Any | None = None,
    ) -> dict[str, Any]:
        client = HttpClient(
            endpoint=self._build_url(path),
            method="POST",
            headers=self._build_headers(record_log_id=record_log_id),
            timeout=self.timeout,
            retry_attempts=self.retry_attempts,
        )
        result = client.request(json_body=payload)
        if not result["ok"]:
            raise ValueError(f"Operator execution callback failed: {result['error']}")
        self._validate_openapi_result(result)
        return result

    def _build_url(self, path: str) -> str:
        return f"{self.api_base.rstrip('/')}/{OPERATOR_EXECUTION_API_PREFIX.strip('/')}/{path.lstrip('/')}"

    def _build_headers(self, record_log_id: Any | None = None) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "user-account": self.user_account,
            "space-id": str(self._get_ctx_required_value("spaceId")),
        }
        for key in ("x-tt-env", "x-use-ppe"):
            value = self.ctx.get(key)
            if value:
                headers[key] = str(value)
        if record_log_id not in (None, ""):
            headers[TT_LOG_ID_HEADER] = str(record_log_id)
        return headers

    @staticmethod
    def _extract_record_log_id(*values: Any) -> Any | None:
        for value in values:
            if isinstance(value, dict) and value.get(ADC_LOG_ID_FIELD) not in (None, ""):
                return value[ADC_LOG_ID_FIELD]
        return None

    def _get_ctx_required_value(self, key: str) -> Any:
        if not isinstance(self.ctx, dict) or self.ctx.get(key) in (None, ""):
            raise ValueError(f"ctx.{key} must be provided")
        return self.ctx[key]

    def _get_api_base(self) -> str:
        if not isinstance(self.ctx, dict):
            raise ValueError("ctx.apiBase must be provided")
        api_base = self.ctx.get("apiBase")
        if not api_base:
            raise ValueError("ctx.apiBase must be provided")
        return str(api_base)

    def _has_callback_ctx(self) -> bool:
        return has_operator_execution_callback_ctx(self.ctx)

    def _add_optional_ctx_value(self, payload: dict[str, Any], key: str) -> None:
        self._add_optional_value(payload, key, self.ctx.get(key))

    @staticmethod
    def _add_optional_value(payload: dict[str, Any], key: str, value: Any) -> None:
        if value is not None:
            payload[key] = OperatorExecutionCallbackClient._to_json_safe_value(value)

    @staticmethod
    def _get_required_value(value: Any, name: str) -> Any:
        if value in (None, ""):
            raise ValueError(f"{name} must be provided")
        return value

    @staticmethod
    def _to_json_safe_value(value: Any, seen: set[int] | None = None, depth: int = 0) -> Any:
        if seen is None:
            seen = set()
        if depth > JSON_SAFE_MAX_DEPTH:
            return {"__type__": "max_depth_exceeded"}
        if value is None or isinstance(value, (str, bool, int, float)):
            if isinstance(value, float):
                if value != value:
                    return {"__type__": "float", "value": "nan"}
                if value == float("inf"):
                    return {"__type__": "float", "value": "inf"}
                if value == float("-inf"):
                    return {"__type__": "float", "value": "-inf"}
            return value

        value_id = id(value)
        if isinstance(value, (dict, list, tuple, set, frozenset)):
            if value_id in seen:
                return {
                    "__type__": "circular_reference",
                    "class": OperatorExecutionCallbackClient._class_name(value),
                }

        if hasattr(value, "as_py") and callable(value.as_py):
            try:
                converted = value.as_py()
            except Exception:
                pass
            else:
                if converted is not value:
                    return OperatorExecutionCallbackClient._to_json_safe_value(converted, seen, depth + 1)
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (bytes, bytearray, memoryview)):
            return {
                "__type__": "bytes",
                "base64": base64.b64encode(bytes(value)).decode("ascii"),
            }
        if isinstance(value, dict):
            seen.add(value_id)
            try:
                return {
                    OperatorExecutionCallbackClient._json_safe_dict_key(key):
                    OperatorExecutionCallbackClient._to_json_safe_value(item, seen, depth + 1)
                    for key, item in value.items()
                }
            finally:
                seen.remove(value_id)
        if isinstance(value, (list, tuple)):
            seen.add(value_id)
            try:
                preview = [
                    OperatorExecutionCallbackClient._to_json_safe_value(item, seen, depth + 1)
                    for item in value[:JSON_SAFE_MAX_SEQUENCE_ITEMS]
                ]
                if len(value) <= JSON_SAFE_MAX_SEQUENCE_ITEMS:
                    return preview
                return {
                    "__type__": type(value).__name__,
                    "length": len(value),
                    "preview": preview,
                    "truncated": True,
                }
            finally:
                seen.remove(value_id)
        if isinstance(value, (set, frozenset)):
            seen.add(value_id)
            try:
                items = sorted(
                    [
                        OperatorExecutionCallbackClient._to_json_safe_value(item, seen, depth + 1)
                        for item in value
                    ],
                    key=lambda item: str(item),
                )
                if len(items) <= JSON_SAFE_MAX_SEQUENCE_ITEMS:
                    return items
                return {
                    "__type__": type(value).__name__,
                    "length": len(items),
                    "preview": items[:JSON_SAFE_MAX_SEQUENCE_ITEMS],
                    "truncated": True,
                }
            finally:
                seen.remove(value_id)
        if hasattr(value, "tolist") and callable(value.tolist):
            try:
                converted = value.tolist()
            except Exception:
                pass
            else:
                if converted is not value:
                    return OperatorExecutionCallbackClient._to_json_safe_value(converted, seen, depth + 1)
        if hasattr(value, "item") and callable(value.item):
            try:
                converted = value.item()
            except Exception:
                pass
            else:
                if converted is not value:
                    return OperatorExecutionCallbackClient._to_json_safe_value(converted, seen, depth + 1)
        return {
            "__type__": "object",
            "class": OperatorExecutionCallbackClient._class_name(value),
            "repr": repr(value),
        }

    @staticmethod
    def _json_safe_dict_key(key: Any) -> str:
        if isinstance(key, str):
            return key
        return str(key)

    @staticmethod
    def _class_name(value: Any) -> str:
        cls = type(value)
        return f"{cls.__module__}.{cls.__qualname__}"

    def _get_operator_execution_id(self) -> int:
        if self.operator_execution_id in (None, ""):
            raise ValueError("operatorExecutionId must be provided")
        return int(self.operator_execution_id)

    @staticmethod
    def _validate_openapi_result(result: dict[str, Any]) -> None:
        data = result.get("data")
        if not isinstance(data, dict):
            return

        code = data.get("code")
        if code not in (None, 0):
            message = data.get("message") or data.get("msg") or ""
            raise ValueError(
                f"Operator execution callback business failed: code={code}, message={message}"
            )

        response_data = data.get("data")
        if isinstance(response_data, dict) and response_data.get("success") is False:
            raise ValueError("Operator execution callback business failed: success=false")

    @staticmethod
    def _extract_operator_execution_id(result: dict[str, Any]) -> int:
        data = result.get("data")
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            data = data["data"]
        if not isinstance(data, dict) or data.get("operatorExecutionId") in (None, ""):
            raise ValueError("start response must contain operatorExecutionId")
        return int(data["operatorExecutionId"])
