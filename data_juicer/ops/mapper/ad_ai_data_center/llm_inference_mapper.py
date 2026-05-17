from __future__ import annotations

import copy
import json
import time
from typing import Any

from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.http_utils import HttpClient
from data_juicer.utils.notification_utils import send_test_card_notification
from data_juicer.utils.operator_execution_callback_utils import (
    OperatorExecutionCallbackClient,
    RECORD_KEY_FIELD,
    current_time_millis,
)

OP_NAME = "llm_inference_mapper"
NEED_CTX = True
TEST_CARD_NOTIFICATION_TEMPLATE_ID = "AAqt1lQ72dVxK"
SUBMIT_PATH = "/openapi/synthesis/llm-inference/submit"
RESULT_PATH = "/openapi/synthesis/llm-inference/result"


@OPERATORS.register_module(OP_NAME)
class LLMInferenceMapper(Mapper):
    """Call ADC LLM inference OpenAPI and write the inference output."""

    def __init__(
        self,
        prompt: str | None = None,
        prompt_template: str | None = None,
        prompt_field: str | None = None,
        model: str = "",
        output_field: str = "llm_output",
        metadata_field: str = "llm_metadata",
        poll_interval_seconds: float = 2.0,
        max_poll_attempts: int = 60,
        ctx: dict | None = None,
        timeout: float = 30.0,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param prompt: static prompt for every sample.
        :param prompt_template: Python str.format template rendered by sample fields.
        :param prompt_field: sample field that stores the prompt.
        :param model: model name sent to the server. Empty string means default.
        :param output_field: field used to store the server output.
        :param metadata_field: field used to store task metadata.
        :param poll_interval_seconds: seconds to wait between result polling.
        :param max_poll_attempts: maximum result polling attempts.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param timeout: HTTP timeout in seconds.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not any([prompt, prompt_template, prompt_field]):
            raise ValueError("one of prompt, prompt_template, or prompt_field must be provided")
        if not output_field:
            raise ValueError("output_field must be provided")
        if not metadata_field:
            raise ValueError("metadata_field must be provided")
        if poll_interval_seconds < 0:
            raise ValueError("poll_interval_seconds must be >= 0")
        if max_poll_attempts <= 0:
            raise ValueError("max_poll_attempts must be > 0")

        self.prompt = prompt
        self.prompt_template = prompt_template
        self.prompt_field = prompt_field
        self.model = model or ""
        self.output_field = output_field
        self.metadata_field = metadata_field
        self.poll_interval_seconds = poll_interval_seconds
        self.max_poll_attempts = max_poll_attempts
        self.ctx = ctx
        self.timeout = timeout
        self._operator_execution_callback_client = None

    def process_single(self, sample):
        ctx = self._get_ctx()
        record_started_at = current_time_millis()
        input_sample = copy.deepcopy(sample)
        try:
            prompt = self._build_prompt(sample)
            submit_data = self._submit(prompt, ctx)
            task_id = self._get_required_response_value(submit_data, "taskId")
            result_data = self._poll_result(task_id, ctx)
            output = result_data.get("output")
            self._ensure_json_serializable(output)

            sample[self.output_field] = output
            sample[self.metadata_field] = self._build_metadata(result_data)
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
        self._try_send_test_card_notification(
            stage="开始",
            content=self._operator_config(),
            err_msg="",
        )

    def after_operator_finished(self, dataset=None, context=None, error=None):
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

    def _build_prompt(self, sample: dict[str, Any]) -> str:
        if self.prompt_field:
            prompt = sample.get(self.prompt_field)
        elif self.prompt_template:
            try:
                prompt = self.prompt_template.format(**sample)
            except KeyError as exc:
                missing_field = exc.args[0]
                raise ValueError(f"prompt_template missing field: {missing_field}") from exc
        else:
            prompt = self.prompt

        if not isinstance(prompt, str) or not prompt:
            raise ValueError("prompt must be a non-empty string")
        return prompt

    def _submit(self, prompt: str, ctx: dict[str, Any]) -> dict[str, Any]:
        return self._post_openapi(
            path=SUBMIT_PATH,
            ctx=ctx,
            json_body={
                "prompt": prompt,
                "model": self.model,
            },
        )

    def _poll_result(self, task_id: str, ctx: dict[str, Any]) -> dict[str, Any]:
        for attempt in range(self.max_poll_attempts):
            data = self._post_openapi(
                path=RESULT_PATH,
                ctx=ctx,
                json_body={
                    "taskId": task_id,
                },
            )
            if self._is_running(data):
                if attempt < self.max_poll_attempts - 1 and self.poll_interval_seconds:
                    time.sleep(self.poll_interval_seconds)
                continue
            if self._is_success(data):
                return data
            raise ValueError(self._extract_result_error_message(data))

        raise TimeoutError(
            f"LLM inference task {task_id} did not finish after {self.max_poll_attempts} polls"
        )

    def _post_openapi(self, path: str, ctx: dict[str, Any], json_body: dict[str, Any]) -> dict[str, Any]:
        client = HttpClient(
            endpoint=self._build_openapi_url(ctx, path),
            method="POST",
            headers=self._build_headers(ctx),
            timeout=self.timeout,
        )
        result = client.request(json_body=json_body)
        if not result["ok"]:
            raise ValueError(f"LLM inference request failed: {result['error']}")
        return self._unwrap_openapi_response(result["data"])

    @staticmethod
    def _unwrap_openapi_response(data):
        if not isinstance(data, dict):
            raise ValueError("LLM inference response must be a dictionary")
        code = data.get("code")
        if code != 0:
            message = data.get("message") or data.get("msg") or ""
            raise ValueError(f"LLM inference business failed: code={code}, message={message}")
        response_data = data.get("data")
        if not isinstance(response_data, dict):
            raise ValueError("LLM inference response data must be a dictionary")
        return response_data

    @staticmethod
    def _is_running(data: dict[str, Any]) -> bool:
        return data.get("finished") is False or data.get("resultStatus") == "RUNNING"

    @staticmethod
    def _is_success(data: dict[str, Any]) -> bool:
        if data.get("success") is False:
            return False
        return data.get("finished") is True and (
            data.get("success") is True or data.get("resultStatus") == "SUCCESS"
        )

    @staticmethod
    def _extract_result_error_message(data: dict[str, Any]) -> str:
        return data.get("message") or data.get("resultStatus") or data.get("status") or "LLM inference failed"

    @staticmethod
    def _build_metadata(data: dict[str, Any]) -> dict[str, Any]:
        metadata = {}
        for key in ("taskId", "conversationId", "requestId", "resultStatus", "status"):
            value = data.get(key)
            if value is not None:
                metadata[key] = value
        return metadata

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
            "prompt_source": self._prompt_source(),
            "model": self.model,
            "output_field": self.output_field,
            "metadata_field": self.metadata_field,
            "poll_interval_seconds": self.poll_interval_seconds,
            "max_poll_attempts": self.max_poll_attempts,
        }

    def _prompt_source(self) -> str:
        if self.prompt_field:
            return "prompt_field"
        if self.prompt_template:
            return "prompt_template"
        return "prompt"

    def _report_record_success(self, input_sample, output_sample, started_at):
        try:
            self._get_operator_execution_callback_client().report_record_success(
                record_key=self._get_record_key(output_sample),
                input_data=input_sample,
                output_data=copy.deepcopy(output_sample),
                started_at=started_at,
            )
        except Exception as exc:
            logger.warning("Failed to report record success callback: {}", exc)

    def _report_record_failure(self, input_sample, output_sample, error_message, started_at):
        try:
            self._get_operator_execution_callback_client().report_record_failure(
                record_key=self._get_record_key(output_sample),
                input_data=input_sample,
                output_data=copy.deepcopy(output_sample),
                error_message=error_message,
                started_at=started_at,
            )
        except Exception as exc:
            logger.warning("Failed to report record failure callback: {}", exc)

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

    def _get_ctx(self):
        if not isinstance(self.ctx, dict):
            raise ValueError("ctx must be provided")
        return self.ctx

    @staticmethod
    def _build_openapi_url(ctx: dict[str, Any], path: str) -> str:
        base_url = LLMInferenceMapper._get_ctx_required_value(ctx, "apiBase")
        return f"{base_url.rstrip('/')}/{path.lstrip('/')}"

    @staticmethod
    def _build_headers(ctx: dict[str, Any]) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "user-account": LLMInferenceMapper._get_ctx_required_value(ctx, "userAccount"),
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
    def _get_required_response_value(data: dict[str, Any], key: str) -> str:
        value = data.get(key)
        if not value:
            raise ValueError(f"LLM inference response data.{key} must be provided")
        return str(value)

    @staticmethod
    def _get_record_key(sample: dict[str, Any]):
        if not sample.get(RECORD_KEY_FIELD):
            raise ValueError(f"sample.{RECORD_KEY_FIELD} must be provided")
        return sample[RECORD_KEY_FIELD]

    @staticmethod
    def _stringify_result_value(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return value

    @staticmethod
    def _ensure_json_serializable(value):
        try:
            json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"LLM inference output must be JSON serializable: {exc}") from exc
