from __future__ import annotations

import copy
import json
from typing import Any

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.http_utils import HttpClient

OP_NAME = "external_eval_data_import_mapper"
DEFAULT_ENDPOINT = "https://ai-data-center.bytedance.net/api/openapi/cloud-doc/sheets/all-plain-values"
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
        timeout: float = 30.0,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param sheet_url: external sheet URL, wiki URL, or spreadsheet token.
        :param data_type: parser type. Currently only ``eval_data`` is supported.
        :param python_code: Python script defining ``process(data, context)``.
        :param timeout: HTTP timeout in seconds.
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

        self.sheet_url = sheet_url
        self.data_type = data_type
        self.output_field = OUTPUT_FIELD
        self.client = HttpClient(
            endpoint=DEFAULT_ENDPOINT,
            method="POST",
            timeout=timeout,
        )
        self.process_func = self._compile_process_func(python_code)

    def process_single(self, sample):
        sheets = self._load_sheets()
        parsed_data = self._parse_eval_data(sheets)
        context = {
            "data_type": self.data_type,
            "sheet_url": self.sheet_url,
            "raw_sheets": sheets,
        }
        result = self.process_func(copy.deepcopy(parsed_data), copy.deepcopy(context))
        self._ensure_json_serializable(result)
        sample[self.output_field] = result
        return sample

    def _load_sheets(self) -> list[dict[str, Any]]:
        result = self.client.request(json_body={"docUrl": self.sheet_url})
        if not result["ok"]:
            raise ValueError(f"Failed to load sheet data: {result['error']}")
        data = result["data"]
        if not isinstance(data, dict) or not isinstance(data.get("sheets"), list):
            raise ValueError("Sheet response must contain a sheets list")
        return data["sheets"]

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

    @staticmethod
    def _compile_process_func(python_code: str):
        namespace: dict[str, Any] = {}
        try:
            compiled_code = compile(python_code, "<external_eval_data_import_mapper>", "exec")
            exec(compiled_code, {"__builtins__": __builtins__}, namespace)
        except Exception as exc:
            raise ValueError(f"Invalid python_code: {exc}") from exc

        process_func = namespace.get("process")
        if not callable(process_func):
            raise ValueError("python_code must define a callable process(data, context)")
        return process_func

    @staticmethod
    def _ensure_json_serializable(value):
        try:
            json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"python_code result must be JSON serializable: {exc}") from exc
