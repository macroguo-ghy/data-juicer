from __future__ import annotations

import copy
from typing import Any

from loguru import logger

from docs.reference.standard_dataset_assembler import (
    KEEP_KEYS,
    process as assemble_standard_dataset,
)

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.operator_execution_callback_utils import (
    NoOpOperatorExecutionCallbackClient,
    OperatorExecutionCallbackClient,
    RECORD_KEY_FIELD,
    current_time_millis,
    has_operator_execution_callback_ctx,
)

OP_NAME = "standard_dataset_assembler_mapper"
OP_DISPLAY_NAME = "标准数据集组装"
CONFIG_PAGE_KEY = "standard_dataset_assembler_builder"
NEED_CTX = True
OPERATOR_TAG = "business_operator"


@OPERATORS.register_module(OP_NAME)
class StandardDatasetAssemblerMapper(Mapper):
    """Assemble slash-path ADC samples into the standard dataset structure."""

    def __init__(
        self,
        ctx: dict | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        self.ctx = ctx
        self._operator_execution_callback_client = None

    def run(self, dataset, *, exporter=None, tracer=None):
        dataset = super().run(dataset, exporter=exporter, tracer=tracer)
        return self._remove_non_standard_columns(dataset)

    def process_single(self, sample):
        record_started_at = current_time_millis()
        input_sample = copy.deepcopy(sample)
        try:
            output_sample = assemble_standard_dataset(copy.deepcopy(sample), {})
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
            output_sample,
            record_started_at,
        )
        return output_sample

    def process_batched(self, samples, *args, **kwargs):
        keys = list(samples.keys())
        first_key = next(iter(keys), None)
        if first_key is None:
            return samples

        output_samples = []
        output_keys = []
        for index in range(len(samples[first_key])):
            sample = {key: samples[key][index] for key in keys}
            output_sample = self.process_single(sample)
            output_samples.append(output_sample)
            for key in output_sample.keys():
                if key not in output_keys:
                    output_keys.append(key)

        return {
            key: [sample.get(key) for sample in output_samples]
            for key in output_keys
        }

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
            if not has_operator_execution_callback_ctx(self.ctx):
                callback_client = NoOpOperatorExecutionCallbackClient()
            else:
                callback_client = OperatorExecutionCallbackClient(self.ctx)
            callback_client.start(
                operator_config=self._operator_config()
            )
            self._operator_execution_callback_client = callback_client
        return self._operator_execution_callback_client

    def _operator_config(self):
        return {
            "default_script": "docs/reference/standard_dataset_assembler.py",
        }

    def _remove_non_standard_columns(self, dataset):
        columns = self._dataset_columns(dataset)
        if not columns:
            return dataset

        removable = [column for column in columns if column not in KEEP_KEYS]
        if not removable:
            return dataset

        remove_columns = getattr(dataset, "remove_columns", None)
        if callable(remove_columns):
            return remove_columns(removable)

        drop_columns = getattr(dataset, "drop_columns", None)
        if callable(drop_columns):
            return drop_columns(removable)

        logger.warning(
            "StandardDatasetAssemblerMapper cannot remove non-standard columns from dataset type {}",
            type(dataset).__name__,
        )
        return dataset

    @staticmethod
    def _dataset_columns(dataset):
        column_names = getattr(dataset, "column_names", None)
        if column_names is not None:
            return list(column_names)

        return []

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
