# Code Review Mapper Usage

## Goal

`code_review_mapper` uses trusted Python code to review one configured field in each sample and writes two review
fields back to the same sample. It is a generic code-based review operator. Business field names such as `state` are
passed through `input_field`; they are not part of the operator name.

## Operator Metadata

```python
OP_NAME = "code_review_mapper"
CONFIG_PAGE_KEY = "code_review_builder"
NEED_CTX = True
```

File:

```text
data_juicer/ops/mapper/ad_ai_data_center/code_review_mapper.py
```

`CONFIG_PAGE_KEY` tells the frontend to render a custom code review configuration page. It is a module-level constant
and is not passed to `@OPERATORS.register_module(...)`.

## Parameters

| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `input_field` | `str` | Yes | `None` | Sample field to review, for example `state`. |
| `status_field` | `str` | No | `"review_status"` | Output field for boolean review result. |
| `reason_field` | `str` | No | `"review_reason"` | Output field for review failure reason. |
| `python_code` | `str` | Yes | `None` | Trusted Python script that defines the review function. |
| `entrypoint` | `str` | No | `"review_row"` | Function name to call from `python_code`. |
| `ctx` | `dict` | Yes | `None` | Backend-injected platform context. |

## Script Contract

Default entrypoint:

```python
def review_row(value, row, context):
    return True, ""
```

Arguments:

- `value`: deep copy of `sample[input_field]`.
- `row`: deep copy of the full sample.
- `context`: platform execution context.

Context shape:

```json
{
  "ctx": {
    "userAccount": "wangjianda.667",
    "apiBase": "https://ai-data-center.bytedance.net/api"
  },
  "operator": "code_review_mapper",
  "input_field": "state"
}
```

Supported return shapes:

```python
return True, ""
```

```python
return {
    "passed": False,
    "reason": "缺少 scene 字段"
}
```

`passed` must be a boolean. `reason` must be a string; return `""` when there is no failure reason.

## Output

The mapper preserves the original sample and adds / overwrites the configured result fields.

Input:

```json
{
  "__adc_record_key": "record-1",
  "state": {
    "scene": "feed"
  }
}
```

Output:

```json
{
  "__adc_record_key": "record-1",
  "state": {
    "scene": "feed"
  },
  "state_review_status": true,
  "state_review_reason": ""
}
```

Business review failure is normal output, not an operator failure:

```json
{
  "__adc_record_key": "record-2",
  "state": {},
  "state_review_status": false,
  "state_review_reason": "缺少 scene 字段"
}
```

## YAML Example

```yaml
process:
  - code_review_mapper:
      ctx:
        userAccount: "wangjianda.667"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        apiBase: "https://ai-data-center.bytedance.net/api"
        synthesisInstanceId: 10001
        taskId: 30001
        taskVersion: 1
        operatorIndex: 0
        operatorName: "code_review_mapper"
        operatorType: "business"
      input_field: "state"
      status_field: "state_review_status"
      reason_field: "state_review_reason"
      python_code: |
        def review_row(value, row, context):
            if not value:
                return False, "state 为空"
            if not isinstance(value, dict):
                return False, "state 必须是对象"
            if "scene" not in value:
                return False, "缺少 scene 字段"
            return True, ""
```

## Error Handling

The mapper fails fast when:

- `input_field`, `status_field`, `reason_field`, or `entrypoint` is empty.
- `python_code` is empty or cannot compile.
- the configured entrypoint is missing or not callable.
- the sample does not contain `input_field`.
- the review function raises an exception.
- the review function returns an unsupported shape.
- `passed` is not a boolean.

Per-record callback failures and test-card notification failures are observational and are logged as warnings; they
do not block successful review output.

## Dataset Split

The operator only produces one output dataset. To get three datasets:

- all records: use the mapper output directly.
- passed records: filter by `status_field == true`.
- failed records: filter by `status_field == false`.

Keeping split logic outside this mapper matches Data-Juicer's one-input-one-output mapper model and avoids coupling a
field review operator to platform export behavior.
