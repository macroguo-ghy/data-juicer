from __future__ import annotations

import copy
import inspect
import json
from typing import Any

from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.http_utils import HttpClient
from data_juicer.utils.operator_execution_callback_utils import (
    OperatorExecutionCallbackClient,
    RECORD_KEY_FIELD,
    current_time_millis,
)
from data_juicer.utils.python_script_utils import PythonScriptRunner

OP_NAME = "state_metric_calculator"
CONFIG_PAGE_KEY = "state_metric_calculator"
NEED_CTX = True
BATCH_GET_OPERATORS_PATH = "/openapi/state-meta/operators/batch-get"


@OPERATORS.register_module(OP_NAME)
class StateMetricCalculatorMapper(Mapper):
    """Calculate derived State metrics for each sample with trusted Python code."""

    def __init__(
        self,
        state_key: str = "state",
        output_key: str = "query_metric_data_outputs",
        result_mode: str = "object",
        fail_policy: str = "continue",
        operators: list[dict] | None = None,
        ctx: dict | None = None,
        timeout: float = 30.0,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param state_key: sample field containing the State object.
        :param output_key: sample field used to store metric outputs.
        :param result_mode: first version supports ``object`` only.
        :param fail_policy: first version supports ``continue`` only.
        :param operators: selected derived metric operator configs.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param timeout: HTTP timeout in seconds.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not state_key:
            raise ValueError("state_key must be provided")
        if not output_key:
            raise ValueError("output_key must be provided")
        if result_mode != "object":
            raise ValueError("result_mode must be object")
        if fail_policy != "continue":
            raise ValueError("fail_policy must be continue")
        self.operators = self._normalize_operators(operators)

        self.state_key = state_key
        self.output_key = output_key
        self.result_mode = result_mode
        self.fail_policy = fail_policy
        self.ctx = ctx
        self.timeout = timeout
        self._operator_details: dict[int, dict[str, Any]] | None = None
        self._operator_details_error: str | None = None
        self._calculate_runners: dict[int, PythonScriptRunner] = {}
        self._operator_execution_callback_client = None

    def process_single(self, sample):
        record_started_at = current_time_millis()
        input_sample = copy.deepcopy(sample)
        try:
            self._get_ctx()
            output_sample = copy.deepcopy(sample)
            output_sample[self.output_key] = self._calculate_metric_outputs(sample)
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

    def after_operator_finished(self, dataset=None, context=None, error=None):
        try:
            callback_client = self._get_operator_execution_callback_client()
            if error is None:
                callback_client.finalize()
            else:
                callback_client.failed(error_message=str(error))
        except Exception as exc:
            logger.warning("Failed to finish operator execution callback: {}", exc)

    def _calculate_metric_outputs(self, sample: dict[str, Any]) -> dict[str, Any]:
        details = self._get_operator_details()
        self._validate_state_key_when_all_metrics_depend_on_state(sample, details)
        outputs = {}
        for operator_config in self.operators:
            operator_id = operator_config["operator_id"]
            detail = details.get(operator_id)
            result_key = self._result_key(detail, operator_id)
            try:
                if self._operator_details_error:
                    raise ValueError(self._operator_details_error)
                if detail is None:
                    raise ValueError(f"operator detail not found: {operator_id}")
                outputs[result_key] = self._calculate_one_metric(
                    sample,
                    operator_config,
                    detail,
                )
            except Exception as exc:
                outputs[result_key] = self._metric_failure_result(
                    operator_id,
                    detail,
                    str(exc),
                )
        return outputs

    def _validate_state_key_when_all_metrics_depend_on_state(
        self,
        sample: dict[str, Any],
        details: dict[int, dict[str, Any]],
    ) -> None:
        if self.state_key in sample or self._operator_details_error:
            return
        selected_details = [
            details.get(operator_config["operator_id"])
            for operator_config in self.operators
        ]
        if not selected_details or any(detail is None for detail in selected_details):
            return
        all_depend_on_state = all(
            self._detail_depends_on_state(detail)
            for detail in selected_details
        )
        if all_depend_on_state:
            raise ValueError(f"sample.{self.state_key} must be provided")

    def _detail_depends_on_state(self, detail: dict[str, Any]) -> bool:
        try:
            signature = inspect.signature(
                self._get_calculate_runner(int(detail["id"]), detail).process_func
            )
        except Exception:
            return False
        return "state" in signature.parameters

    def _calculate_one_metric(
        self,
        sample: dict[str, Any],
        operator_config: dict[str, Any],
        detail: dict[str, Any],
    ) -> dict[str, Any]:
        operator_id = int(detail["id"])
        parameters = self._parse_input_parameters(detail)
        runner = self._get_calculate_runner(operator_id, detail)
        args = self._resolve_calculate_args(
            sample,
            operator_config,
            parameters,
            inspect.signature(runner.process_func),
        )
        value = runner.run_with_args(*args)
        return {
            "success": True,
            "value": value,
            "error": "",
            "operator_id": operator_id,
            "operator_name_cn": detail.get("operatorNameCn") or "",
        }

    def _resolve_calculate_args(
        self,
        sample: dict[str, Any],
        operator_config: dict[str, Any],
        parameters: list[dict[str, Any]],
        signature: inspect.Signature,
    ) -> list[Any]:
        parameters_by_name = {
            parameter["key_name_en"]: parameter
            for parameter in parameters
        }
        args = []
        for func_parameter in signature.parameters.values():
            if func_parameter.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                raise ValueError("calculate does not support *args or **kwargs")
            if func_parameter.kind == inspect.Parameter.KEYWORD_ONLY:
                raise ValueError("calculate does not support keyword-only parameters")

            name = func_parameter.name
            if name == "state":
                args.append(self._resolve_state_value(sample))
            elif name in parameters_by_name:
                args.append(
                    self._resolve_parameter_value(
                        sample,
                        operator_config,
                        parameters_by_name[name],
                    )
                )
            elif func_parameter.default is not inspect.Parameter.empty:
                args.append(func_parameter.default)
            else:
                raise ValueError(f"inputParameter.params missing parameter: {name}")
        return args

    def _resolve_parameter_value(
        self,
        sample: dict[str, Any],
        operator_config: dict[str, Any],
        parameter: dict[str, Any],
    ):
        name = parameter.get("key_name_en")
        data_type = parameter.get("data_type")
        missing = object()

        if data_type == "placeholder":
            mapping = operator_config.get("parameter_mapping") or {}
            field_name = mapping.get(name)
            value = sample.get(field_name, missing) if field_name else missing
        elif data_type == "defaultValue":
            value = parameter.get("default_or_placeholder_value", missing)
        else:
            raise ValueError(f"unsupported inputParameter data_type: {data_type}")

        if value is missing:
            raise ValueError(f"missing required parameter: {name}")
        return value

    def _resolve_state_value(self, sample: dict[str, Any]):
        missing = object()
        value = sample.get(self.state_key, missing)
        if self._is_missing_state_value(value, missing):
            raise ValueError(f"sample.{self.state_key} must be provided")
        return self._normalize_state_value(value)

    @staticmethod
    def _is_missing_state_value(value, missing) -> bool:
        return value is missing or value is None or value == ""

    @staticmethod
    def _normalize_state_value(value):
        if isinstance(value, str):
            try:
                parsed_value = json.loads(value)
            except ValueError as exc:
                raise ValueError("state must be a valid JSON object string") from exc
            if not isinstance(parsed_value, (dict, list)):
                raise ValueError("state JSON string must decode to an object or array")
            return parsed_value
        return value

    def _get_operator_details(self) -> dict[int, dict[str, Any]]:
        if self._operator_details is not None:
            return self._operator_details
        try:
            self._operator_details = self._fetch_operator_details()
        except Exception as exc:
            self._operator_details_error = f"Failed to fetch state metric operators: {exc}"
            self._operator_details = {}
        return self._operator_details

    def _fetch_operator_details(self) -> dict[int, dict[str, Any]]:
        ctx = self._get_ctx()
        client = HttpClient(
            endpoint=self._build_openapi_url(ctx, BATCH_GET_OPERATORS_PATH),
            method="POST",
            headers=self._build_headers(ctx),
            timeout=self.timeout,
        )
        result = client.request(
            json_body={
                "operatorIds": self._operator_ids(),
            }
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        data = self._unwrap_openapi_response(result["data"])
        operators = data.get("operators") if isinstance(data, dict) else None
        if not isinstance(operators, list):
            raise ValueError("state metric operator response must contain operators list")
        details = {}
        for detail in operators:
            if isinstance(detail, dict) and detail.get("id") is not None:
                details[int(detail["id"])] = detail
        return details

    def _get_calculate_runner(self, operator_id: int, detail: dict[str, Any]) -> PythonScriptRunner:
        if operator_id not in self._calculate_runners:
            self._calculate_runners[operator_id] = PythonScriptRunner(
                python_code=detail.get("operatorCode") or "",
                entrypoint="calculate",
                require_dict_result=False,
            )
        return self._calculate_runners[operator_id]

    @staticmethod
    def _unwrap_openapi_response(data):
        if isinstance(data, dict) and "code" in data:
            code = data.get("code")
            if code != 0:
                message = data.get("message") or data.get("msg") or ""
                raise ValueError(
                    f"Fetch state metric operators business failed: code={code}, message={message}"
                )
            return data.get("data")
        return data

    @staticmethod
    def _parse_input_parameters(detail: dict[str, Any]) -> list[dict[str, Any]]:
        input_parameter = detail.get("inputParameter")
        if isinstance(input_parameter, str):
            try:
                input_parameter = json.loads(input_parameter)
            except ValueError as exc:
                raise ValueError("inputParameter must be a valid JSON object string") from exc
        if not isinstance(input_parameter, dict):
            raise ValueError("inputParameter must be a dictionary")
        parameters = input_parameter.get("params")
        if not isinstance(parameters, list):
            raise ValueError("inputParameter.params must be a list")
        for parameter in parameters:
            if not isinstance(parameter, dict) or not parameter.get("key_name_en"):
                raise ValueError("inputParameter.params item must contain key_name_en")
        return parameters

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
            "state_key": self.state_key,
            "output_key": self.output_key,
            "result_mode": self.result_mode,
            "fail_policy": self.fail_policy,
            "operators": copy.deepcopy(self.operators),
        }

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

    def _operator_ids(self) -> list[int]:
        seen = set()
        operator_ids = []
        for operator_config in self.operators:
            operator_id = operator_config["operator_id"]
            if operator_id not in seen:
                seen.add(operator_id)
                operator_ids.append(operator_id)
        return operator_ids

    @staticmethod
    def _normalize_operators(operators: list[dict] | None) -> list[dict[str, Any]]:
        if not isinstance(operators, list) or not operators:
            raise ValueError("operators must be a non-empty list")
        normalized = []
        for operator_config in operators:
            if not isinstance(operator_config, dict):
                raise ValueError("operators item must be a dictionary")
            if operator_config.get("operator_id") is None:
                raise ValueError("operators item must contain operator_id")
            parameter_mapping = operator_config.get("parameter_mapping") or {}
            if not isinstance(parameter_mapping, dict):
                raise ValueError("parameter_mapping must be a dictionary")
            normalized.append({
                "operator_id": int(operator_config["operator_id"]),
                "parameter_mapping": copy.deepcopy(parameter_mapping),
            })
        return normalized

    @staticmethod
    def _result_key(detail: dict[str, Any] | None, operator_id: int) -> str:
        if isinstance(detail, dict) and detail.get("operatorNameEn"):
            return str(detail["operatorNameEn"])
        return f"operator_{operator_id}"

    @staticmethod
    def _metric_failure_result(
        operator_id: int,
        detail: dict[str, Any] | None,
        error: str,
    ) -> dict[str, Any]:
        return {
            "success": False,
            "value": None,
            "error": error,
            "operator_id": operator_id,
            "operator_name_cn": detail.get("operatorNameCn") if isinstance(detail, dict) else "",
        }

    def _get_ctx(self):
        if not isinstance(self.ctx, dict):
            raise ValueError("ctx must be provided")
        self._get_ctx_required_value(self.ctx, "apiBase")
        self._get_ctx_required_value(self.ctx, "userAccount")
        return self.ctx

    @staticmethod
    def _build_openapi_url(ctx: dict[str, Any], path: str) -> str:
        base_url = StateMetricCalculatorMapper._get_ctx_required_value(ctx, "apiBase")
        return f"{base_url.rstrip('/')}/{path.lstrip('/')}"

    @staticmethod
    def _build_headers(ctx: dict[str, Any]) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "user-account": StateMetricCalculatorMapper._get_ctx_required_value(ctx, "userAccount"),
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
