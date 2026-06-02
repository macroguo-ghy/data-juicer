---
name: adc-business-operator-development
description: >-
  Use when creating, modifying, reviewing, or testing ADC business operators in
  Data-Juicer under data_juicer/ops/mapper/ad_ai_data_center. Covers operator
  naming and constants, ctx handling, ADC operator/record callbacks, record key
  fallback behavior, whole-sample rebuilding, YAML loading, focused tests, and
  CR checks.
---

# ADC Business Operator Development

Use this skill for ADC business mappers: Data-Juicer `Mapper` operators with
ADC platform metadata, optional `ctx`, ADC OpenAPI access, and operator/record
execution callbacks.

If the user only needs custom user code, prefer `python_script_mapper`. If
existing operators can express the workflow in YAML, compose existing operators
before adding a new business mapper.

## Scope

Business operator code lives in:

```text
data_juicer/ops/mapper/ad_ai_data_center/
```

Tests live in:

```text
tests/ops/mapper/
```

Follow repo development workflow and testing policy when making code changes:

- `.agents/skills/data-juicer-development-workflow/SKILL.md`
- `docs/AgentTesting.md`

## Naming And Constants

Use consistent names:

- File: lowercase snake case ending in `_mapper.py`, for example `standard_dataset_assembler_mapper.py`.
- `OP_NAME`: same as the YAML node name, for example `standard_dataset_assembler_mapper`.
- Class: PascalCase, for example `StandardDatasetAssemblerMapper`.
- Test: `test_<op_name>.py`.

Required constants:

```python
OP_NAME = "standard_dataset_assembler_mapper"
OP_DISPLAY_NAME = "标准数据集组装"
CONFIG_PAGE_KEY = "standard_dataset_assembler_builder"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
```

`NEED_CTX` is metadata. Do not use it as a reason to hard-fail construction
when local business logic can run without `ctx`.

## Minimal Mapper Shape

Register with `OPERATORS.register_module(OP_NAME)` and inherit `Mapper`.
Keep fixed business logic in normal Python functions or modules. Do not expose
`python_code` in YAML unless the business requirement is truly user-defined
script execution.

```python
from __future__ import annotations

from data_juicer.ops.base_op import OPERATORS, Mapper

OP_NAME = "my_business_mapper"
OP_DISPLAY_NAME = "我的业务算子"
CONFIG_PAGE_KEY = "my_business_builder"
NEED_CTX = True
OPERATOR_TAG = "business_operator"


@OPERATORS.register_module(OP_NAME)
class MyBusinessMapper(Mapper):
    def __init__(self, ctx: dict | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ctx = ctx
        self._operator_execution_callback_client = None

    def process_single(self, sample):
        sample["result"] = "ok"
        return sample
```

This is only a runnable skeleton. For an ADC business operator, add the
operator/record callback logic from the sections below before treating the
implementation as production-ready.

## `ctx` Rules

`ctx` is used for callbacks and ADC OpenAPI calls.

- Constructors should allow `ctx=None`.
- If only callbacks need `ctx`, use `NoOpOperatorExecutionCallbackClient` when callback context is absent.
- Only validate `apiBase`, `userAccount`, `spaceId`, or similar fields on the path that actually calls ADC OpenAPI.
- Keep `_operator_config()` JSON-serializable and small. Do not include secrets, huge objects, or non-serializable values.

Callback client pattern:

```python
def _get_operator_execution_callback_client(self):
    if self._operator_execution_callback_client is None:
        if not has_operator_execution_callback_ctx(self.ctx):
            callback_client = NoOpOperatorExecutionCallbackClient()
        else:
            callback_client = OperatorExecutionCallbackClient(self.ctx)
        callback_client.start(operator_config=self._operator_config())
        self._operator_execution_callback_client = callback_client
    return self._operator_execution_callback_client
```

## Record Callback Contract

Business operators should report record success/failure per sample. Callback
failures must be warning-only and must not block the business result.

Success path must support fallback record key when the output sample removed
`__adc_record_key`:

```python
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
```

Failure path:

```python
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
```

Record key helpers:

```python
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
```

Expected behavior:

- Output keeps `__adc_record_key`: report with output key.
- Output deletes `__adc_record_key`, input has key: report with `fallback_record_key`.
- Input and output both lack key: business result continues; warning should mention missing `sample.__adc_record_key`.
- Business processing fails: report record failure if possible, then re-raise the original business exception.

Do not generate `__adc_record_key` in a normal business mapper. It is normally
prepared by `prepare_record_key_mapper`.

## Whole-Sample Rebuild

Most mappers should add or update fields and return `sample`.

If the operator rebuilds the whole sample, local `NestedDataset` or HuggingFace
Dataset mapping can retain old columns. Remove non-standard columns after
`super().run(...)` only for datasets whose columns are directly available:

```python
def run(self, dataset, *, exporter=None, tracer=None):
    dataset = super().run(dataset, exporter=exporter, tracer=tracer)
    return self._remove_non_standard_columns(dataset)

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
    return dataset

@staticmethod
def _dataset_columns(dataset):
    column_names = getattr(dataset, "column_names", None)
    return list(column_names) if column_names is not None else []
```

Do not call Ray Dataset `columns()` merely to remove old columns. It can trigger
schema inference or pre-execution and change upstream execution timing.

Some dataset wrappers may expose `drop_columns(...)` instead of
`remove_columns(...)`. Do not add that fallback blindly. If the current target
wrapper requires it, ask the user whether to support `drop_columns` compatibility
for this operator, then add the fallback with a focused test.

## Fixed Script Encapsulation

For a fixed script-like business flow:

1. Keep the logic as normal Python code.
2. Import and call the function from the operator.
3. Do not expose `python_code` in YAML.
4. Test the operator's real control path.

When the logic only needs the sample, pass an empty dict for script context.
Keep ADC `ctx` for platform callbacks and backend calls; do not mix it with
Data-Juicer runtime context unless the contract is explicit.

## YAML Loading

Minimal YAML shape:

```yaml
process:
  - standard_dataset_assembler_mapper:
      ctx:
        userAccount: "zhangsan"
        apiBase: "https://ai-data-center.bytedance.net/api"
        spaceId: 1
        synthesisInstanceId: 10001
        flowInstanceId: 20001
        flowNodeId: "task_1"
        taskId: 30001
        taskVersion: 1
        operatorIndex: 1
        operatorName: "standard_dataset_assembler_mapper"
        operatorType: "Mapper"
```

Pure local logic should still run without `ctx`, but no real platform callback
will be emitted.

## Tests

At minimum, cover:

- Successful `process_single` or `op.run(Dataset.from_list(...))`.
- Success record callback, including `record_key`, `fallback_record_key`, input/output, and `started_at`.
- Failure record callback and re-raising the original business exception.
- Missing `ctx` does not block local business logic.
- Missing `__adc_record_key` does not block local business result and logs a clear warning.
- YAML `load_ops` can load the operator without `ad_ai_data_center.` prefix.
- Constant assertions for `OP_NAME`, `OP_DISPLAY_NAME`, `CONFIG_PAGE_KEY`, `NEED_CTX`, and `OPERATOR_TAG`.

Patch the callback class in the operator module, not in the utility module:

```python
@patch(
    "data_juicer.ops.mapper.ad_ai_data_center.standard_dataset_assembler_mapper."
    "OperatorExecutionCallbackClient"
)
def test_record_callback(self, mock_callback_cls):
    ...
```

## CR Checklist

Before committing:

- `OP_NAME` matches the YAML node name.
- `OP_DISPLAY_NAME` is the intended product-facing Chinese name.
- `OPERATOR_TAG = "business_operator"`.
- `ctx` is not over-validated; local logic can run without callback context when appropriate.
- ADC OpenAPI paths validate required context fields only where needed.
- Success and failure record callbacks exist.
- Output-deleted `__adc_record_key` uses `fallback_record_key`.
- Callback exceptions are warning-only.
- Whole-sample rebuilds do not leak old columns.
- No Ray schema-preexecution APIs are introduced.
- Tests cover real `load_ops` path where applicable.
- Run focused tests, `py_compile`, and `git diff --check`.

## Reference Implementation

When available in the current checkout, use these as concrete patterns:

- `data_juicer/ops/mapper/ad_ai_data_center/standard_dataset_assembler_mapper.py`
- `docs/reference/standard_dataset_assembler.py`
- `tests/ops/mapper/test_standard_dataset_assembler_mapper.py`
