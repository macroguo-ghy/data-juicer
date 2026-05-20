# State Metric Calculator Mapper Usage

## Goal

`state_metric_calculator` reads the State object from each sample, fetches the latest selected derived metric operator
definitions from AD AI Data Center OpenAPI, executes each operator's trusted Python `calculate(...)` function, and writes
metric results back to the sample.

## Operator Metadata

```python
OP_NAME = "state_metric_calculator"
CONFIG_PAGE_KEY = "state_metric_calculator"
NEED_CTX = True
```

File:

```text
data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py
```

## Parameters

| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `state_key` | `str` | No | `"state"` | Sample field containing the State object. The value can be a JSON object/array or a JSON string decoded to an object/array. |
| `output_key` | `str` | No | `"query_metric_data_outputs"` | Sample field used to store metric outputs. |
| `result_mode` | `str` | No | `"object"` | First version supports object output only. |
| `fail_policy` | `str` | No | `"continue"` | Single metric failures are written to that metric result and other metrics continue. |
| `operators` | `list[dict]` | Yes | `None` | Selected derived metric operators. |
| `ctx` | `dict` | Yes | `None` | Backend-injected platform context. |

`operators` item:

```json
{
  "operator_id": 201,
  "parameter_mapping": {
    "start_time": "source_table_start_time"
  }
}
```

`parameter_mapping` maps derived metric parameter keys to source table field names. The mapping key is
`inputParameter.params[].key_name_en`; the mapping value is the selected source table field name. Pass `{}` when no
placeholder parameter needs a source table field.

## OpenAPI

The mapper calls:

```http
POST {ctx.apiBase}/openapi/state-meta/operators/batch-get
```

Payload:

```json
{
  "operatorIds": [201, 202]
}
```

Headers include `Content-Type`, `Accept`, `user-account`, and PPE headers from `ctx` when present.

Operator details are cached in the mapper instance. The same `operator_id` is requested only once per mapper process.

## Parameter Resolution

The State object is an implicit runtime parameter. If `calculate(...)` declares a `state` parameter, the mapper reads
`sample[state_key]`, decodes it when it is a JSON string, and passes the decoded object to `calculate`.

For every `inputParameter.params[]` item used by the `calculate(...)` function signature:

| `data_type` | Resolution |
| --- | --- |
| `placeholder` | Uses `parameter_mapping[key_name_en]` to read a source table field from the sample. |
| `defaultValue` | Uses `default_or_placeholder_value` from `inputParameter.params[]`. |

If a required parameter cannot be resolved, that metric item is written with `output=null` and an `error` message.

`inputParameter.params[]` may contain parameters not declared by the current `calculate(...)` function. The mapper ignores
those unused parameter definitions. Invalid JSON strings in `sample[state_key]` are recorded as metric failures and do
not require every metric script to parse JSON by itself.

Successful metric `output` is always a JSON string produced by `json.dumps(..., ensure_ascii=False)`. For example, a
numeric result `0.82` is written as `"0.82"`, and an array result `[0.14, 0.28]` is written as `"[0.14, 0.28]"`.

The output `id` is resolved from actual input values of ID-like parameters. The priority is `ids`, then `id`, then other
parameter names ending with `id`, such as `task_id`. If multiple operators provide different IDs at the same priority,
the first one in `operators` order wins. If no ID-like parameter can be resolved, `id` is written as `"unknown"`.

## Output

The mapper writes an object to `output_key`:

```json
{
  "query_metric_data_outputs": {
    "id": "1854168911595796",
    "metrics": [
      {
        "metricCode": "bench_roi_score",
        "metricName": "行业基准 ROI 得分",
        "output": "0.82",
        "error": ""
      },
      {
        "metricCode": "quality_score",
        "metricName": "质量得分",
        "output": null,
        "error": "missing required parameter: bench_roi"
      }
    ]
  }
}
```

`metricCode` is `operatorNameEn`; if missing, it falls back to `operator_{operator_id}`.

## YAML Example

```yaml
process:
  - state_metric_calculator:
      state_key: "state"
      output_key: "query_metric_data_outputs"
      result_mode: "object"
      fail_policy: "continue"
      operators:
        - operator_id: 201
          parameter_mapping:
            start_time: "source_table_start_time"
        - operator_id: 202
          parameter_mapping: {}
      ctx:
        userAccount: "wangjianda.667"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        apiBase: "https://ai-data-center.bytedance.net/api"
        synthesisInstanceId: 10001
        taskId: 30001
        taskVersion: 1
        operatorIndex: 2
        operatorName: "state_metric_calculator"
        operatorType: "business"
```

## Error Handling

Single metric failures are written into that metric's result object and do not fail the record.

The record fails when constructor/runtime prerequisites are invalid, including invalid `operators`, missing `ctx.apiBase`,
or missing `state_key` when all selected metrics require State.
