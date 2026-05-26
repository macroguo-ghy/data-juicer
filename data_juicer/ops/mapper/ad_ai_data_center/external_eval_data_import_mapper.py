from __future__ import annotations

import copy
from typing import Any

from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.adc_record_context import add_record_log_id_header
from data_juicer.utils.http_utils import HttpClient
from data_juicer.utils.operator_execution_callback_utils import (
    OperatorExecutionCallbackClient,
)
from data_juicer.utils.python_script_utils import PythonScriptRunner

OP_NAME = "external_eval_data_import_mapper"
OP_DISPLAY_NAME = "导入外部评测数据"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
CLOUD_DOC_ALL_PLAIN_VALUES_PATH = "/openapi/cloud-doc/sheets/all-plain-values"
OUTPUT_FIELD = "externalDataSet"
SUPPORTED_DATA_TYPES = {"eval_data"}


@OPERATORS.register_module(OP_NAME)
class ExternalEvalDataImportMapper(Mapper):
    """Import external eval_data sheet content and run custom Python logic."""

    def __init__(
        self,
        sheet_url: str | None = None,
        data_type: str | None = None,
        python_code: str | None = None,
        ctx: dict | None = None,
        timeout: float = 30.0,
        retry_attempts: int = 3,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param sheet_url: external sheet URL, wiki URL, or spreadsheet token.
        :param data_type: parser type. Currently only ``eval_data`` is supported.
        :param python_code: Python script defining ``process(data, context)``.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param timeout: HTTP timeout in seconds.
        :param retry_attempts: HTTP retry attempts for ADC OpenAPI requests.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not sheet_url:
            raise ValueError("sheet_url must be provided")
        if not data_type:
            raise ValueError("data_type must be provided")
        if data_type not in SUPPORTED_DATA_TYPES:
            raise ValueError(f"Unsupported data_type: {data_type}")
        if not python_code:
            raise ValueError("python_code must be provided")
        if retry_attempts < 0:
            raise ValueError("retry_attempts must be non-negative")

        self.sheet_url = sheet_url
        self.data_type = data_type
        self.output_field = OUTPUT_FIELD
        self.ctx = ctx
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.script_runner = PythonScriptRunner(
            python_code,
            entrypoint="process",
            require_dict_result=False,
        )
        self._operator_execution_callback_client = None

    def process_single(self, sample):
        ctx = self._get_ctx()
        user_account = self._get_ctx_required_value(ctx, "userAccount")
        endpoint = self._build_openapi_url(ctx, CLOUD_DOC_ALL_PLAIN_VALUES_PATH)
        sheets = self._load_sheets(endpoint, user_account, sample)
        parsed_data = self._parse_eval_data(sheets)
        context = {
            "data_type": self.data_type,
            "sheet_url": self.sheet_url,
            "raw_sheets": sheets,
        }
        result = self.script_runner.run(copy.deepcopy(parsed_data), copy.deepcopy(context))
        sample[self.output_field] = result
        return sample

    def _load_sheets(self, endpoint: str, user_account: str, sample: dict[str, Any]) -> list[dict[str, Any]]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "user-account": user_account,
            "space-id": self._get_ctx_required_value(self.ctx, "spaceId"),
        }
        add_record_log_id_header(headers, sample)
        client = HttpClient(
            endpoint=endpoint,
            method="POST",
            timeout=self.timeout,
            retry_attempts=self.retry_attempts,
            headers=headers,
        )
        result = client.request(json_body={"docUrl": self.sheet_url})
        if not result["ok"]:
            raise ValueError(f"Failed to load sheet data: {result['error']}")
        data = self._unwrap_sheet_response(result["data"])
        if not isinstance(data, dict) or not isinstance(data.get("sheets"), list):
            raise ValueError("Sheet response must contain a sheets list")
        return data["sheets"]

    def _get_ctx(self):
        ctx = self.ctx
        if not isinstance(ctx, dict):
            raise ValueError("ctx must be provided")
        return ctx

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
                    "sheet_url": self.sheet_url,
                    "data_type": self.data_type,
                    "output_field": self.output_field,
                }
            )
            self._operator_execution_callback_client = callback_client
        return self._operator_execution_callback_client

    @staticmethod
    def _build_openapi_url(ctx: dict[str, Any], path: str) -> str:
        base_url = ExternalEvalDataImportMapper._get_ctx_required_value(
            ctx, "apiBase"
        )
        return f"{base_url.rstrip('/')}/{path.lstrip('/')}"

    @staticmethod
    def _get_ctx_required_value(ctx: dict[str, Any], key: str) -> str:
        if not ctx.get(key):
            raise ValueError(f"ctx.{key} must be provided")
        return str(ctx[key])

    @staticmethod
    def _unwrap_sheet_response(data):
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            return data["data"]
        return data

    def _parse_eval_data(self, sheets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        items = []
        for sheet in sheets:
            values = sheet.get("values", [])
            if not values:
                continue
            header = values[0]
            if not isinstance(header, list) or not all(isinstance(col, str) and col for col in header):
                raise ValueError("eval_data sheet header must be a non-empty string list")
            for row in values[1:]:
                if not isinstance(row, list):
                    raise ValueError("eval_data sheet rows must be lists")
                if not any(cell not in (None, "") for cell in row):
                    continue
                item = {}
                for index, field_name in enumerate(header):
                    item[field_name] = row[index] if index < len(row) else ""
                items.append(item)
        return items
