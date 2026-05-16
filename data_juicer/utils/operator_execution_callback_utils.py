from __future__ import annotations

from enum import IntEnum
from typing import Any

from data_juicer.utils.http_utils import HttpClient

OPERATOR_EXECUTION_API_PREFIX = "/openapi/synthesis/operator-execution"
RECORD_KEY_FIELD = "__adc_record_key"


class OperatorExecutionStatus(IntEnum):
    RUNNING = 1
    SUCCESS = 2
    FAILED = 3


class OperatorExecutionCallbackClient:
    """Client for data synthesis operator execution callback APIs.

    Intended usage:
    - Business operators with ``NEED_CTX = True`` call ``upsert`` before
      processing records and keep the returned ``operatorExecutionId``.
    - Business operators call ``report_record_success`` or
      ``report_record_failure`` after each record. A failed record is only a
      record-level failure and does not imply operator-level FAILED.
    - Execution engines call ``finalize`` only after the current operator output
      is fully consumed. Business operators should not infer that by themselves.
    - Operator-level FAILED is reported through ``report_status`` when the
      operator as a whole fails, not for ordinary single-record failures.
    """

    def __init__(
        self,
        ctx: dict[str, Any],
        operator_execution_id: int | None = None,
        timeout: float = 30.0,
    ):
        self.ctx = ctx
        self.timeout = timeout
        self.operator_execution_id = operator_execution_id
        self.api_base = self._get_api_base()
        self.user_account = self._get_ctx_required_value("userAccount")

    def upsert(
        self,
        *,
        operator_config: dict[str, Any] | None = None,
        status: OperatorExecutionStatus | int = OperatorExecutionStatus.RUNNING,
        started_at: int | None = None,
        properties: dict[str, Any] | None = None,
    ) -> int:
        """Create or update the operator execution row and cache its ID."""
        payload = {
            "synthesisInstanceId": self._get_ctx_required_value("synthesisInstanceId"),
            "taskId": self._get_ctx_required_value("taskId"),
            "taskVersion": self._get_ctx_required_value("taskVersion"),
            "operatorIndex": self._get_ctx_required_value("operatorIndex"),
            "operatorName": self._get_ctx_required_value("operatorName"),
            "operatorConfig": operator_config or {},
            "status": int(status),
        }
        self._add_optional_ctx_value(payload, "flowInstanceId")
        self._add_optional_ctx_value(payload, "flowNodeId")
        self._add_optional_ctx_value(payload, "operatorType")
        self._add_optional_value(payload, "startedAt", started_at)
        self._add_optional_value(payload, "properties", properties)

        result = self._post("upsert", payload)
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
        return self._post("record", payload)

    def report_status(
        self,
        status: OperatorExecutionStatus | int,
        *,
        finished_at: int | None = None,
        error_message: str | None = None,
        properties: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Report an operator-level status transition.

        Do not call this with FAILED for ordinary single-record failures; use
        ``report_record_failure`` instead.
        """
        payload = {
            "operatorExecutionId": self._get_operator_execution_id(),
            "status": int(status),
        }
        self._add_optional_value(payload, "finishedAt", finished_at)
        self._add_optional_value(payload, "errorMessage", error_message)
        self._add_optional_value(payload, "properties", properties)
        return self._post("status", payload)

    def finalize(
        self,
        *,
        finished_at: int | None = None,
        properties: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Finalize operator success after the execution engine consumes output."""
        payload = {
            "operatorExecutionId": self._get_operator_execution_id(),
        }
        self._add_optional_value(payload, "finishedAt", finished_at)
        self._add_optional_value(payload, "properties", properties)
        return self._post("finalize", payload)

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        client = HttpClient(
            endpoint=self._build_url(path),
            method="POST",
            headers=self._build_headers(),
            timeout=self.timeout,
        )
        result = client.request(json_body=payload)
        if not result["ok"]:
            raise ValueError(f"Operator execution callback failed: {result['error']}")
        self._validate_openapi_result(result)
        return result

    def _build_url(self, path: str) -> str:
        return f"{self.api_base.rstrip('/')}/{OPERATOR_EXECUTION_API_PREFIX.strip('/')}/{path.lstrip('/')}"

    def _build_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "user-account": self.user_account,
        }
        for key in ("x-tt-env", "x-use-ppe"):
            value = self.ctx.get(key)
            if value:
                headers[key] = str(value)
        return headers

    def _get_ctx_required_value(self, key: str) -> Any:
        if not isinstance(self.ctx, dict) or self.ctx.get(key) in (None, ""):
            raise ValueError(f"ctx.{key} must be provided")
        return self.ctx[key]

    def _get_api_base(self) -> str:
        if not isinstance(self.ctx, dict):
            raise ValueError("ctx.apiBase must be provided")
        api_base = self.ctx.get("apiBase") or self.ctx.get("openapiBaseUrl")
        if not api_base:
            raise ValueError("ctx.apiBase must be provided")
        return str(api_base)

    def _add_optional_ctx_value(self, payload: dict[str, Any], key: str) -> None:
        self._add_optional_value(payload, key, self.ctx.get(key))

    @staticmethod
    def _add_optional_value(payload: dict[str, Any], key: str, value: Any) -> None:
        if value is not None:
            payload[key] = value

    @staticmethod
    def _get_required_value(value: Any, name: str) -> Any:
        if value in (None, ""):
            raise ValueError(f"{name} must be provided")
        return value

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
            raise ValueError("upsert response must contain operatorExecutionId")
        return int(data["operatorExecutionId"])
