from __future__ import annotations

import copy
import datetime
import inspect
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
from data_juicer.utils.python_script_utils import PythonScriptRunner
from data_juicer.ops.mapper.ad_ai_data_center.state_metric_runtime import (
    MetricHelpers,
    detect_id_key,
    extract_metric_ids,
)

OP_NAME = "state_metric_calculator"
OP_DISPLAY_NAME = "State 指标计算"
CONFIG_PAGE_KEY = "state_metric_calculator"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
BATCH_GET_OPERATORS_PATH = "/openapi/state-meta/operators/batch-get"


@OPERATORS.register_module(OP_NAME)
class StateMetricCalculatorMapper(Mapper):
    """Calculate derived State metrics for each sample with trusted Python code."""

    def __init__(
        self,
        state_key: str = "state",
        id_source_key: str | None = None,
        output_key: str = "query_metric_data_outputs",
        result_mode: str = "summary",
        fail_policy: str = "continue",
        operators: list[dict] | None = None,
        ctx: dict | None = None,
        start_date_key: str | None = None,
        end_date_key: str | None = None,
        timeout: float = 30.0,
        retry_attempts: int = 3,
        *args,
        repartition_num_blocks: int | None = None,
        **kwargs,
    ):
        """
        Initialization method.

        :param state_key: sample field containing the State object.
        :param id_source_key: optional common sample field containing metric item IDs.
        :param output_key: sample field used to store metric outputs.
        :param result_mode: supports Dataset Factory summary string output only.
        :param fail_policy: first version supports ``continue`` only.
        :param operators: selected derived metric operator configs.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param start_date_key: optional sample field containing metric start date.
        :param end_date_key: optional sample field containing metric end date.
        :param timeout: HTTP timeout in seconds.
        :param retry_attempts: HTTP retry attempts for ADC OpenAPI requests.
        :param repartition_num_blocks: Ray Dataset block count before metric calculation.
            None means num_proc * 4 when num_proc is positive.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not state_key:
            raise ValueError("state_key must be provided")
        if id_source_key is not None and not id_source_key:
            raise ValueError("id_source_key must be a non-empty string")
        if output_key is None or output_key == "":
            output_key = "query_metric_data_outputs"
        if result_mode is None or result_mode == "":
            result_mode = "summary"
        if result_mode != "summary":
            raise ValueError("result_mode must be summary")
        if fail_policy is None or fail_policy == "":
            fail_policy = "continue"
        if fail_policy != "continue":
            raise ValueError("fail_policy must be continue")
        if retry_attempts < 0:
            raise ValueError("retry_attempts must be non-negative")
        if (
            repartition_num_blocks is not None
            and (type(repartition_num_blocks) is not int or repartition_num_blocks <= 0)
        ):
            raise ValueError("repartition_num_blocks must be a positive integer")
        self.operators = self._normalize_operators(operators)

        self.state_key = state_key
        self.id_source_key = id_source_key
        self.output_key = output_key
        self.result_mode = result_mode
        self.fail_policy = fail_policy
        self.ctx = ctx
        self.start_date_key = start_date_key
        self.end_date_key = end_date_key
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.repartition_num_blocks = repartition_num_blocks
        self._operator_details: dict[int, dict[str, Any]] | None = None
        self._operator_details_error: str | None = None
        self._calculate_runners: dict[int, PythonScriptRunner] = {}
        self._operator_execution_callback_client = None

    def run(self, dataset, *, exporter=None, tracer=None):
        return super().run(
            self.prepare_ray_dataset(dataset),
            exporter=exporter,
            tracer=tracer,
        )

    def prepare_ray_dataset(self, dataset):
        repartition_num_blocks = self._effective_repartition_num_blocks()
        if repartition_num_blocks is None:
            return dataset
        repartition = getattr(dataset, "repartition", None)
        if not callable(repartition):
            return dataset
        logger.info(
            "StateMetricCalculatorMapper repartition input to {} blocks before metric calculation",
            repartition_num_blocks,
        )
        return repartition(num_blocks=repartition_num_blocks, shuffle=False)

    def _effective_repartition_num_blocks(self) -> int | None:
        if self.repartition_num_blocks is not None:
            return self.repartition_num_blocks
        num_proc = getattr(self, "num_proc", None)
        if isinstance(num_proc, int) and num_proc > 0:
            return num_proc * 4
        return None

    def process_single(self, sample):
        record_started_at = current_time_millis()
        input_sample = copy.deepcopy(sample)
        try:
            self._get_ctx()
            output_sample = copy.deepcopy(sample)
            output_sample[self.output_key] = self._build_summary_output(
                self._calculate_metric_outputs(sample)
            )
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
        details = self._get_operator_details(sample)
        self._validate_state_key_when_all_metrics_depend_on_state(sample, details)
        state_present = self._has_state_value(sample)
        state_data = self._resolve_state_value(sample) if state_present else {}
        helpers = MetricHelpers()
        output_id, item_ids, id_source_field = self._resolve_output_items(
            sample,
            details,
        )
        items = []
        for item_id in item_ids:
            metrics = []
            current_id_key = detect_id_key(state_data, item_id)
            for operator_config in self.operators:
                operator_id = operator_config["operator_id"]
                detail = details.get(operator_id)
                try:
                    if self._operator_details_error:
                        raise ValueError(self._operator_details_error)
                    if detail is None:
                        raise ValueError(f"operator detail not found: {operator_id}")
                    metrics.append(self._calculate_one_metric(
                        sample,
                        operator_config,
                        detail,
                        current_id=item_id,
                        id_source_field=id_source_field,
                        state_data=state_data,
                        state_present=state_present,
                        id_key=current_id_key,
                        helpers=helpers,
                    ))
                except Exception as exc:
                    metrics.append(self._metric_failure_result(
                        operator_id,
                        detail,
                        str(exc),
                    ))
            items.append({
                "id": item_id,
                "metrics": metrics,
            })
        return {
            "id": output_id,
            "items": items,
        }

    @staticmethod
    def _build_summary_output(metric_outputs: dict[str, Any]) -> str:
        summary = {}
        for item in metric_outputs.get("items", []) or []:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("id") or "")
            metrics = item.get("metrics")
            if not item_id or not isinstance(metrics, list) or not metrics:
                continue
            summary[item_id] = {
                "metrics": [
                    StateMetricCalculatorMapper._normalize_summary_metric(metric)
                    for metric in metrics
                    if isinstance(metric, dict)
                ]
            }
        return json.dumps(summary, ensure_ascii=False) if summary else ""

    @staticmethod
    def _normalize_summary_metric(metric: dict[str, Any]) -> dict[str, str]:
        return {
            "metricCode": str(metric.get("metricCode") or ""),
            "metricName": str(metric.get("metricName") or ""),
            "output": str(
                metric.get("output")
                if metric.get("output") is not None
                else "null"
            ),
            "error": str(metric.get("error") or ""),
        }

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
        current_id: str | None = None,
        id_source_field: str | None = None,
        state_data: Any = None,
        state_present: bool = False,
        id_key: str | None = None,
        helpers: MetricHelpers | None = None,
    ) -> dict[str, Any]:
        operator_id = int(detail["id"])
        parameters = self._parse_input_parameters(detail)
        runner = self._get_calculate_runner(operator_id, detail)
        args = self._resolve_calculate_args(
            sample,
            operator_config,
            parameters,
            inspect.signature(runner.process_func),
            current_id=current_id,
            id_source_field=id_source_field,
            state_data=state_data,
            state_present=state_present,
            id_key=id_key,
            helpers=helpers,
        )
        value = runner.run_with_args(*args)
        return {
            "metricCode": self._result_key(detail, operator_id),
            "metricName": detail.get("operatorNameCn") or "",
            "output": self._stringify_metric_output(value),
            "error": "",
        }

    def _resolve_calculate_args(
        self,
        sample: dict[str, Any],
        operator_config: dict[str, Any],
        parameters: list[dict[str, Any]],
        signature: inspect.Signature,
        current_id: str | None = None,
        id_source_field: str | None = None,
        state_data: Any = None,
        state_present: bool = False,
        id_key: str | None = None,
        helpers: MetricHelpers | None = None,
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
                args.append(
                    state_data
                    if state_present
                    else self._resolve_state_value(sample)
                )
            elif name in parameters_by_name:
                args.append(
                    self._resolve_parameter_value(
                        sample,
                        operator_config,
                        parameters_by_name[name],
                        current_id=current_id,
                        id_source_field=id_source_field,
                    )
                )
            elif name == "id_key":
                if id_key is None:
                    raise ValueError(f"Unknown id: {current_id}")
                args.append(id_key)
            elif name == "id_value":
                args.append(current_id)
            elif name == "start_date":
                args.append(
                    self._resolve_date_value(sample, self.start_date_key)
                )
            elif name == "end_date":
                args.append(
                    self._resolve_date_value(sample, self.end_date_key)
                )
            elif name == "helpers":
                args.append(helpers or MetricHelpers())
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
        current_id: str | None = None,
        id_source_field: str | None = None,
    ):
        name = parameter.get("key_name_en")
        data_type = parameter.get("data_type")
        missing = object()

        if (
            current_id is not None
            and data_type == "placeholder"
            and self._is_current_id_parameter(
                operator_config,
                name,
                id_source_field,
            )
        ):
            value = current_id
        elif data_type == "placeholder":
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

    def _has_state_value(self, sample: dict[str, Any]) -> bool:
        missing = object()
        value = sample.get(self.state_key, missing)
        return not self._is_missing_state_value(value, missing)

    @staticmethod
    def _resolve_date_value(sample: dict[str, Any], key: str | None):
        if not key:
            return None
        value = sample.get(key)
        if value is None or value == "":
            return None
        if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
            return value
        try:
            return datetime.date.fromisoformat(str(value))
        except Exception as exc:
            raise ValueError(f"sample.{key} must be a YYYY-MM-DD date") from exc

    def _resolve_output_items(
        self,
        sample: dict[str, Any],
        details: dict[int, dict[str, Any]],
    ) -> tuple[str, list[str], str | None]:
        candidate = self._resolve_output_id_candidate(sample, details)
        if candidate is None:
            return "unknown", ["unknown"], None

        _, _, _, _, source_field, value = candidate
        output_id = self._stringify_output_id(value)
        item_ids = self._split_output_id_value(value)
        return output_id, item_ids, source_field

    def _resolve_output_id(
        self,
        sample: dict[str, Any],
        details: dict[int, dict[str, Any]],
    ) -> str:
        candidate = self._resolve_output_id_candidate(sample, details)
        if candidate is None:
            return "unknown"
        return self._stringify_output_id(candidate[5])

    def _resolve_output_id_candidate(
        self,
        sample: dict[str, Any],
        details: dict[int, dict[str, Any]],
    ):
        candidates = []
        for operator_index, operator_config in enumerate(self.operators):
            operator_id = operator_config["operator_id"]
            detail = details.get(operator_id)
            if not isinstance(detail, dict):
                continue
            try:
                parameters = self._parse_input_parameters(detail)
            except Exception:
                continue
            for parameter_index, parameter in enumerate(parameters):
                priority = self._id_parameter_priority(parameter.get("key_name_en"))
                if priority is None:
                    continue
                try:
                    value = self._resolve_parameter_value(sample, operator_config, parameter)
                except Exception:
                    continue
                if value is None or value == "":
                    continue
                candidates.append((
                    priority,
                    operator_index,
                    parameter_index,
                    parameter.get("key_name_en"),
                    (operator_config.get("parameter_mapping") or {}).get(
                        parameter.get("key_name_en")
                    ),
                    value,
                ))
        if not candidates:
            if self.id_source_key:
                value = sample.get(self.id_source_key)
                if value is not None and value != "":
                    return 3, -1, -1, None, self.id_source_key, value
            return None
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[0]

    @staticmethod
    def _id_parameter_priority(name: str | None):
        if not name:
            return None
        normalized = name.lower().replace("_", "")
        if normalized == "ids":
            return 0
        if normalized == "id":
            return 1
        if normalized.endswith("id"):
            return 2
        return None

    @staticmethod
    def _is_current_id_parameter(
        operator_config: dict[str, Any],
        parameter_name: str | None,
        id_source_field: str | None,
    ) -> bool:
        if id_source_field is None:
            return False
        if StateMetricCalculatorMapper._id_parameter_priority(parameter_name) is None:
            return False
        mapping = operator_config.get("parameter_mapping") or {}
        mapped_field = mapping.get(parameter_name)
        return mapped_field == id_source_field or (
            mapped_field is None
            and id_source_field is not None
        )

    @staticmethod
    def _stringify_output_id(value) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    @staticmethod
    def _split_output_id_value(value) -> list[str]:
        if isinstance(value, list):
            items = [
                StateMetricCalculatorMapper._stringify_output_id(item).strip()
                for item in value
            ]
        else:
            items = extract_metric_ids(value)
        items = [item for item in items if item]
        return items or ["unknown"]

    @staticmethod
    def _stringify_metric_output(value) -> str:
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False)

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

    def _get_operator_details(self, sample: dict[str, Any] | None = None) -> dict[int, dict[str, Any]]:
        if self._operator_details is not None:
            return self._operator_details
        try:
            self._operator_details = self._fetch_operator_details(sample)
        except Exception as exc:
            self._operator_details_error = f"Failed to fetch state metric operators: {exc}"
            self._operator_details = {}
        return self._operator_details

    def _fetch_operator_details(self, sample: dict[str, Any] | None = None) -> dict[int, dict[str, Any]]:
        ctx = self._get_ctx()
        client = HttpClient(
            endpoint=self._build_openapi_url(ctx, BATCH_GET_OPERATORS_PATH),
            method="POST",
            headers=add_record_log_id_header(self._build_headers(ctx), sample),
            timeout=self.timeout,
            retry_attempts=self.retry_attempts,
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
            "id_source_key": self.id_source_key,
            "output_key": self.output_key,
            "result_mode": self.result_mode,
            "fail_policy": self.fail_policy,
            "start_date_key": self.start_date_key,
            "end_date_key": self.end_date_key,
            "output_format": "dataset_factory_summary",
            "preserve_error": True,
            "runtime": "adc_operator_code",
            "operators": copy.deepcopy(self.operators),
            "repartition_num_blocks": self.repartition_num_blocks,
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
            "metricCode": StateMetricCalculatorMapper._result_key(detail, operator_id),
            "metricName": detail.get("operatorNameCn") if isinstance(detail, dict) else "",
            "output": StateMetricCalculatorMapper._stringify_metric_output(None),
            "error": error,
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
