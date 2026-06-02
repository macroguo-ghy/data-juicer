from __future__ import annotations

import copy
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
        summary_success_only: bool = False,
        *args,
        repartition_num_blocks: int | None = None,
        **kwargs,
    ):
        """
        Initialization method.

        :param state_key: sample field containing the State object.
        :param id_source_key: optional common sample field containing metric item IDs.
        :param output_key: sample field used to store metric outputs.
        :param result_mode: ``summary`` for Dataset Factory summary string output,
            ``object`` for the intermediate object output, or ``metric_list`` for
            operator-grouped output.
        :param fail_policy: first version supports ``continue`` only.
        :param operators: selected derived metric operator configs.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param start_date_key: compatibility field retained in operator config.
        :param end_date_key: compatibility field retained in operator config.
        :param timeout: HTTP timeout in seconds.
        :param retry_attempts: HTTP retry attempts for ADC OpenAPI requests.
        :param summary_success_only: when true, summary output keeps only
            successful DF-compatible fields.
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
        if not output_key:
            raise ValueError("output_key must be provided")
        if result_mode not in ("summary", "object", "metric_list"):
            raise ValueError("result_mode must be summary, object or metric_list")
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
        self.summary_success_only = summary_success_only
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
            if self.result_mode == "metric_list":
                output_sample[self.output_key] = self._calculate_metric_list_outputs(sample)
            else:
                metric_outputs = self._calculate_metric_outputs(sample)
            if self.result_mode == "summary":
                output_sample[self.output_key] = self._build_summary_output(
                    metric_outputs,
                    success_only=self.summary_success_only,
                )
            elif self.result_mode == "object":
                output_sample[self.output_key] = self._build_summary_object(
                    metric_outputs,
                    success_only=self.summary_success_only,
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
        output_id, item_ids = self._resolve_output_items(sample)
        items = []
        for item_id in item_ids:
            metrics = []
            tools = []
            for operator_config in self.operators:
                operator_id = operator_config["operator_id"]
                detail = details.get(operator_id)
                try:
                    if self._operator_details_error:
                        raise ValueError(self._operator_details_error)
                    if detail is None:
                        raise ValueError(f"operator detail not found: {operator_id}")
                    operator_type = self._operator_type(detail)
                    if operator_type == "tool":
                        tools.append(self._calculate_one_tool(
                            sample,
                            operator_config,
                            detail,
                            state_data=state_data,
                            state_present=state_present,
                            helpers=helpers,
                        ))
                    else:
                        metrics.append(self._calculate_one_metric(
                            sample,
                            operator_config,
                            detail,
                            state_data=state_data,
                            state_present=state_present,
                            helpers=helpers,
                        ))
                except Exception as exc:
                    if self._is_tool_detail(detail):
                        tools.append(self._tool_failure_result(
                            operator_id,
                            detail,
                            str(exc),
                        ))
                    else:
                        metrics.append(self._metric_failure_result(
                            operator_id,
                            detail,
                            str(exc),
                        ))
            item = {
                "id": item_id,
            }
            if metrics:
                item["metrics"] = metrics
            if tools:
                item["tools"] = tools
            items.append(item)
        return {
            "id": output_id,
            "items": items,
        }

    def _calculate_metric_list_outputs(self, sample: dict[str, Any]) -> list[dict[str, Any]]:
        details = self._get_operator_details(sample)
        self._validate_state_key_when_all_metrics_depend_on_state(sample, details)
        state_present = self._has_state_value(sample)
        state_data = self._resolve_state_value(sample) if state_present else {}
        helpers = MetricHelpers()
        outputs = []
        for operator_config in self.operators:
            operator_id = operator_config["operator_id"]
            detail = details.get(operator_id)
            outputs.append(
                self._calculate_one_metric_list_operator(
                    sample,
                    operator_config,
                    operator_id,
                    detail,
                    state_data=state_data,
                    state_present=state_present,
                    helpers=helpers,
                )
            )
        return outputs

    def _calculate_one_metric_list_operator(
        self,
        sample: dict[str, Any],
        operator_config: dict[str, Any],
        operator_id: int,
        detail: dict[str, Any] | None,
        state_data: Any = None,
        state_present: bool = False,
        helpers: MetricHelpers | None = None,
    ) -> dict[str, Any]:
        meta = self._build_metric_list_meta(operator_id, detail)
        metric_list = []
        try:
            if self._operator_details_error:
                raise ValueError(self._operator_details_error)
            if detail is None:
                raise ValueError(f"operator detail not found: {operator_id}")
            self._operator_type(detail)
            parameters = self._parse_input_parameters(detail)
            runner = self._get_calculate_runner(operator_id, detail)
            invocations = self._resolve_metric_list_invocations(
                sample,
                operator_config,
                parameters,
                inspect.signature(runner.process_func),
                state_data=state_data,
                state_present=state_present,
                helpers=helpers,
            )
        except Exception as exc:
            return {
                "meta": meta,
                "metric_list": [{
                    "input": self._raw_metric_list_input(sample, operator_config, detail),
                    "output": self._stringify_metric_output(None),
                    "error": str(exc),
                }],
            }

        for args, input_values in invocations:
            try:
                value = runner.run_with_args(*args)
                metric_list.append({
                    "input": input_values,
                    "output": self._stringify_metric_output(value),
                    "error": "",
                })
            except Exception as exc:
                metric_list.append({
                    "input": input_values,
                    "output": self._stringify_metric_output(None),
                    "error": str(exc),
                })

        return {
            "meta": meta,
            "metric_list": metric_list,
        }

    @staticmethod
    def _build_summary_output(
        metric_outputs: dict[str, Any],
        success_only: bool = False,
    ) -> str:
        summary = StateMetricCalculatorMapper._build_summary_object(
            metric_outputs,
            success_only=success_only,
        )
        return json.dumps(summary, ensure_ascii=False) if summary else ""

    @staticmethod
    def _build_summary_object(
        metric_outputs: dict[str, Any],
        success_only: bool = False,
    ) -> dict[str, Any]:
        summary = {}
        for item in metric_outputs.get("items", []) or []:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("id") or "")
            metrics = item.get("metrics")
            tools = item.get("tools")
            has_metrics = isinstance(metrics, list) and bool(metrics)
            has_tools = isinstance(tools, list) and bool(tools)
            if not item_id or (not has_metrics and not has_tools):
                continue
            payload = {}
            if has_metrics:
                metrics_out = [
                    StateMetricCalculatorMapper._normalize_summary_metric(
                        metric,
                        success_only=success_only,
                    )
                    for metric in metrics
                    if (
                        isinstance(metric, dict)
                        and (
                            not success_only
                            or StateMetricCalculatorMapper._is_success_summary_item(metric)
                        )
                    )
                ]
                if metrics_out:
                    payload["metrics"] = metrics_out
            if has_tools:
                tools_out = [
                    StateMetricCalculatorMapper._normalize_summary_tool(
                        tool,
                        success_only=success_only,
                    )
                    for tool in tools
                    if (
                        isinstance(tool, dict)
                        and (
                            not success_only
                            or StateMetricCalculatorMapper._is_success_summary_item(tool)
                        )
                    )
                ]
                if tools_out:
                    payload["tools"] = tools_out
            if payload:
                summary[item_id] = payload
        return summary

    @staticmethod
    def _normalize_summary_metric(
        metric: dict[str, Any],
        success_only: bool = False,
    ) -> dict[str, str]:
        result = {
            "metricCode": str(metric.get("metricCode") or ""),
            "metricName": str(metric.get("metricName") or ""),
            "output": str(
                metric.get("output")
                if metric.get("output") is not None
                else "null"
            ),
        }
        if not success_only:
            result["error"] = str(metric.get("error") or "")
        return result

    @staticmethod
    def _normalize_summary_tool(
        tool: dict[str, Any],
        success_only: bool = False,
    ) -> dict[str, str]:
        result = {
            "tool": str(tool.get("tool") or ""),
            "output": str(
                tool.get("output")
                if tool.get("output") is not None
                else "null"
            ),
        }
        if not success_only:
            result["toolName"] = str(tool.get("toolName") or "")
            result["error"] = str(tool.get("error") or "")
        return result

    @staticmethod
    def _is_success_summary_item(item: dict[str, Any]) -> bool:
        output = item.get("output")
        if output is None:
            return False
        output_text = str(output)
        return (
            bool(output_text.strip())
            and "返回调用失败" not in output_text
            and not item.get("error")
        )

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
        state_data: Any = None,
        state_present: bool = False,
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
            state_data=state_data,
            state_present=state_present,
            helpers=helpers,
        )
        value = runner.run_with_args(*args)
        return {
            "metricCode": self._result_key(detail, operator_id),
            "metricName": detail.get("operatorNameCn") or "",
            "output": self._stringify_metric_output(value),
            "error": "",
        }

    def _calculate_one_tool(
        self,
        sample: dict[str, Any],
        operator_config: dict[str, Any],
        detail: dict[str, Any],
        state_data: Any = None,
        state_present: bool = False,
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
            state_data=state_data,
            state_present=state_present,
            helpers=helpers,
        )
        value = runner.run_with_args(*args)
        return {
            "tool": self._tool_key(detail, operator_id),
            "toolName": detail.get("toolNameCn") or "",
            "output": self._stringify_metric_output(value),
            "error": "",
        }

    def _resolve_calculate_args(
        self,
        sample: dict[str, Any],
        operator_config: dict[str, Any],
        parameters: list[dict[str, Any]],
        signature: inspect.Signature,
        state_data: Any = None,
        state_present: bool = False,
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
            elif name == "helpers":
                args.append(helpers or MetricHelpers())
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

    def _resolve_metric_list_invocations(
        self,
        sample: dict[str, Any],
        operator_config: dict[str, Any],
        parameters: list[dict[str, Any]],
        signature: inspect.Signature,
        state_data: Any = None,
        state_present: bool = False,
        helpers: MetricHelpers | None = None,
    ) -> list[tuple[list[Any], dict[str, Any]]]:
        parameters_by_name = {
            parameter["key_name_en"]: parameter
            for parameter in parameters
        }
        argument_specs = []
        input_values = {}
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
                argument_specs.append((
                    "constant",
                    state_data if state_present else self._resolve_state_value(sample),
                ))
            elif name == "helpers":
                argument_specs.append(("constant", helpers or MetricHelpers()))
            elif name in parameters_by_name:
                parameter = parameters_by_name[name]
                value = self._resolve_parameter_value(
                    sample,
                    operator_config,
                    parameter,
                )
                input_values[name] = {
                    "value": value,
                    "multi_value": bool(parameter.get("multi_value")),
                }
                argument_specs.append(("input", name))
            elif func_parameter.default is not inspect.Parameter.empty:
                argument_specs.append(("constant", func_parameter.default))
            else:
                raise ValueError(f"inputParameter.params missing parameter: {name}")

        expanded_inputs = self._expand_metric_list_inputs(input_values)
        invocations = []
        for expanded_input in expanded_inputs:
            args = []
            for spec in argument_specs:
                if spec[0] == "input":
                    args.append(expanded_input[spec[1]])
                else:
                    args.append(spec[1])
            invocations.append((args, expanded_input))
        return invocations

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

    @classmethod
    def _expand_metric_list_inputs(cls, input_values: dict[str, Any]) -> list[dict[str, Any]]:
        if not input_values:
            return [{}]
        split_values = {
            name: cls._split_metric_list_input_value(
                spec["value"],
                multi_value=spec["multi_value"],
            )
            for name, spec in input_values.items()
        }
        empty_names = [
            name
            for name, values in split_values.items()
            if not values
        ]
        if empty_names:
            raise ValueError(
                "multi-value parameters have no values: "
                + ", ".join(empty_names)
            )
        multi_lengths = {
            name: len(values)
            for name, values in split_values.items()
            if len(values) > 1
        }
        if len(set(multi_lengths.values())) > 1:
            detail = ", ".join(
                f"{name}={length}"
                for name, length in multi_lengths.items()
            )
            raise ValueError(f"multi-value parameters have different lengths: {detail}")
        target_len = next(iter(multi_lengths.values()), 1)
        expanded = []
        for index in range(target_len):
            expanded.append({
                name: values[index] if len(values) > 1 else values[0]
                for name, values in split_values.items()
            })
        return expanded

    @staticmethod
    def _split_metric_list_input_value(value: Any, multi_value: bool = False) -> list[Any]:
        if not multi_value:
            if value is None:
                return []
            return [value]
        if isinstance(value, (list, tuple)):
            return [
                item
                for item in value
                if item is not None and item != ""
            ]
        if isinstance(value, str):
            if "," not in value:
                return [value] if value != "" else []
            return [
                item.strip()
                for item in value.split(",")
                if item.strip()
            ]
        if value is None:
            return []
        return [value]

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

    def _resolve_output_items(
        self,
        sample: dict[str, Any],
    ) -> tuple[str, list[str]]:
        candidate = self._resolve_output_id_candidate(sample)
        if candidate is None:
            return "unknown", ["unknown"]

        value = candidate
        output_id = self._stringify_output_id(value)
        item_ids = self._split_output_id_value(value)
        return output_id, item_ids

    def _resolve_output_id_candidate(
        self,
        sample: dict[str, Any],
    ):
        if self.id_source_key:
            value = sample.get(self.id_source_key)
            if value is not None and value != "":
                return value
        return None

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
        parameter_details = detail.get("inputParameterDetails")
        if parameter_details is not None:
            return StateMetricCalculatorMapper._parse_input_parameter_details(parameter_details)

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

    @staticmethod
    def _parse_input_parameter_details(parameter_details: Any) -> list[dict[str, Any]]:
        if not isinstance(parameter_details, list):
            raise ValueError("inputParameterDetails must be a list")
        parameters = []
        for parameter in parameter_details:
            if not isinstance(parameter, dict) or not parameter.get("keyNameEn"):
                raise ValueError("inputParameterDetails item must contain keyNameEn")
            parameters.append({
                "key_name_en": parameter.get("keyNameEn"),
                "data_type": parameter.get("dataType"),
                "default_or_placeholder_value": parameter.get("defaultOrPlaceholderValue"),
                "multi_value": bool(parameter.get("multiValue")),
            })
        return parameters

    @staticmethod
    def _build_metric_list_params(detail: dict[str, Any] | None) -> dict[str, dict[str, str]]:
        if not isinstance(detail, dict):
            return {}
        parameter_details = detail.get("inputParameterDetails")
        if not isinstance(parameter_details, list):
            return {}
        params = {}
        for parameter in parameter_details:
            if not isinstance(parameter, dict) or not parameter.get("keyNameEn"):
                continue
            params[str(parameter["keyNameEn"])] = {
                "name": str(parameter.get("keyNameCn") or ""),
                "type": str(parameter.get("keyType") or ""),
            }
        return params

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
            "summary_success_only": self.summary_success_only,
            "runtime": "adc_operator_code",
            "operators": copy.deepcopy(self.operators),
            "repartition_num_blocks": self.repartition_num_blocks,
        }

    @classmethod
    def _build_metric_list_meta(
        cls,
        operator_id: int,
        detail: dict[str, Any] | None,
    ) -> dict[str, Any]:
        operator_type = "metric"
        if isinstance(detail, dict):
            operator_type = str(detail.get("operatorType") or "metric").strip().lower()
            if operator_type not in ("metric", "tool"):
                operator_type = "metric"
        if operator_type == "tool":
            metric_code = cls._tool_key(detail, operator_id)
            metric_name = detail.get("toolNameCn") if isinstance(detail, dict) else ""
        else:
            metric_code = cls._result_key(detail, operator_id)
            metric_name = detail.get("operatorNameCn") if isinstance(detail, dict) else ""
        return {
            "metric_code": metric_code,
            "metric_name": str(metric_name or ""),
            "params": cls._build_metric_list_params(detail),
        }

    @classmethod
    def _raw_metric_list_input(
        cls,
        sample: dict[str, Any],
        operator_config: dict[str, Any],
        detail: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not isinstance(detail, dict):
            return {}
        parameter_details = detail.get("inputParameterDetails")
        if not isinstance(parameter_details, list):
            return {}
        mapping = operator_config.get("parameter_mapping") or {}
        raw_input = {}
        for parameter in parameter_details:
            if not isinstance(parameter, dict) or not parameter.get("keyNameEn"):
                continue
            name = str(parameter["keyNameEn"])
            if parameter.get("dataType") == "defaultValue":
                raw_input[name] = parameter.get("defaultOrPlaceholderValue")
                continue
            field_name = mapping.get(name)
            if field_name and field_name in sample:
                raw_input[name] = sample[field_name]
        return raw_input

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
    def _operator_type(detail: dict[str, Any] | None) -> str:
        if not isinstance(detail, dict):
            return "metric"
        operator_type = str(detail.get("operatorType") or "metric").strip().lower()
        if operator_type not in ("metric", "tool"):
            raise ValueError(f"unsupported operatorType: {operator_type}")
        return operator_type

    @staticmethod
    def _is_tool_detail(detail: dict[str, Any] | None) -> bool:
        return (
            isinstance(detail, dict)
            and str(detail.get("operatorType") or "").strip().lower() == "tool"
        )

    @staticmethod
    def _tool_key(detail: dict[str, Any] | None, operator_id: int) -> str:
        if isinstance(detail, dict):
            for key in ("toolName", "handlerName", "operatorNameEn"):
                if detail.get(key):
                    return str(detail[key])
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

    @staticmethod
    def _tool_failure_result(
        operator_id: int,
        detail: dict[str, Any] | None,
        error: str,
    ) -> dict[str, Any]:
        return {
            "tool": StateMetricCalculatorMapper._tool_key(detail, operator_id),
            "toolName": detail.get("toolNameCn") if isinstance(detail, dict) else "",
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
            "space-id": StateMetricCalculatorMapper._get_ctx_required_value(ctx, "spaceId"),
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
