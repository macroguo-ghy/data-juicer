import json
import time
from typing import Any

import requests
from loguru import logger

from data_juicer.ops.base_op import OPERATORS, Mapper

OP_NAME = "zhishang_workflow_mapper"
SUPPORTED_OUTPUT_TYPES = {"string", "long"}


@OPERATORS.register_module(OP_NAME)
class ZhishangWorkflowMapper(Mapper):
    """Submit samples to a Zhishang workflow and return async task IDs."""

    def __init__(
        self,
        workflow_id: str | None = None,
        token: str | None = None,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
        timeout: int = 30,
        max_result_poll_seconds: int = 60,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param workflow_id: Zhishang workflow ID.
        :param token: Zhishang open platform token.
        :param input_schema: Mapping from sample fields to workflow input params.
        :param output_schema: Mapping from workflow output content fields to sample fields.
        :param timeout: Timeout for the workflow request.
        :param max_result_poll_seconds: Max seconds to poll async workflow result.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if workflow_id is None or str(workflow_id).strip() == "":
            raise ValueError("workflow_id is required for zhishang_workflow_mapper.")
        if token is None or str(token).strip() == "":
            raise ValueError("token is required for zhishang_workflow_mapper.")

        self.workflow_id = str(workflow_id)
        self.token = str(token)
        self.input_schema = self._normalize_schema(input_schema, "input_schema")
        self.output_schema = self._normalize_schema(output_schema, "output_schema")
        self._validate_output_schema_types()
        self.timeout = timeout
        self.max_result_poll_seconds = max_result_poll_seconds

    @staticmethod
    def _normalize_schema(schema, schema_name: str) -> list[dict[str, Any]]:
        if schema is None:
            return []
        if not isinstance(schema, dict):
            raise ValueError(f"{schema_name} must be an object containing a '{schema_name}' field.")
        if schema_name not in schema:
            raise ValueError(f"{schema_name} object must contain a '{schema_name}' field.")
        schema = schema[schema_name]
        if not isinstance(schema, list):
            raise ValueError(f"{schema_name}.{schema_name} must be a list.")
        for item in schema:
            if not isinstance(item, dict):
                raise ValueError(f"Each item in {schema_name} must be an object.")
        return schema

    def _validate_output_schema_types(self):
        for item in self.output_schema:
            output_type = item.get("type")
            if output_type is not None and output_type not in SUPPORTED_OUTPUT_TYPES:
                raise ValueError(
                    "output_schema type must be one of "
                    f"{sorted(SUPPORTED_OUTPUT_TYPES)}. Got: {output_type}"
                )

    def process_single(self, sample):
        started_at = time.monotonic()
        mapped_sample = self._build_default_mapped_sample()
        success = True
        try:
            mapped_sample.update(self._run_workflow(sample))
        except Exception as error:
            success = False
            logger.warning("Zhishang workflow mapper process_single failed: {}", error)
        elapsed_seconds = time.monotonic() - started_at
        logger.info(
            "Zhishang workflow mapper process_single finished in {:.3f}s. success={}",
            elapsed_seconds,
            success,
        )
        return mapped_sample

    def _build_default_mapped_sample(self):
        return {item["value"]: None for item in self.output_schema}

    def _run_workflow(self, sample):
        # 1. Submit async zhishang workflow task.
        completion_url = (
            "https://zhishang.bytedance.net/open-exec/api/v1/workflow/"
            f"{self.workflow_id}/async-completions"
        )
        input_params = []
        for item in self.input_schema:
            source_field = item["key"]
            input_params.append(
                {
                    "key": item["value"],
                    "type": item["type"],
                    "value": sample[source_field],
                }
            )

        if not input_params:
            raise ValueError("input_schema must build at least one workflow input param.")

        payload = {
            "input_params": input_params,
        }

        response = requests.post(
            completion_url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()
        task_id = response.json()["data"]["task_id"]

        if not task_id:
            raise ValueError("task_id is missing from zhishang_workflow.")

        # 2. Using loop check the result from zhishang workflow task.
        result_url = (
            "https://zhishang.bytedance.net/open-exec/api/v1/workflow/"
            f"{self.workflow_id}/async-results"
        )
        poll_start_time = time.monotonic()
        while time.monotonic() - poll_start_time <= self.max_result_poll_seconds:
            response = requests.post(
                result_url,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
                json={"task_id": task_id},
                timeout=self.timeout,
            )
            response.raise_for_status()
            result_data = response.json()["data"]
            task_status = result_data["task_status"]
            if task_status == "executing":
                time.sleep(5)
                continue
            if task_status != "success":
                raise ValueError(f"workflow task failed with status: {task_status}")
            break
        else:
            raise TimeoutError(
                f"workflow task result polling timed out after "
                f"{self.max_result_poll_seconds} seconds."
            )

        # 3. Analysis the result and mapping result schema
        mapped_sample = {}
        if self.output_schema:
            content = json.loads(result_data["outputs"][0]["content"])
            for item in self.output_schema:
                output_key = item["key"]
                try:
                    mapped_sample[item["value"]] = self._convert_output_value(
                        content[output_key],
                        item,
                    )
                except KeyError as error:
                    raise ValueError(
                        "Cannot find field in workflow response. "
                        f"key: {output_key}, response: {content}"
                    ) from error
        return mapped_sample

    @staticmethod
    def _convert_output_value(value, schema_item: dict[str, Any]):
        output_type = schema_item.get("type")
        if output_type is None or value is None:
            return value
        if output_type == "string":
            return str(value)
        if output_type == "long":
            return int(value)
        raise ValueError(f"Unsupported output_schema type: {output_type}")
