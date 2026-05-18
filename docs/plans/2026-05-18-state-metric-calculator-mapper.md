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
    "bench_roi": "bench_roi"
  }
}
```

`parameter_mapping` maps Python parameter names with `source=field` to sample field names.

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

For every `inputParameter.parameters[]` item:

| `source` | Resolution |
| --- | --- |
| `state` | Reads `sample[state_key]`. |
| `attribute` | Searches the State object by `attributeId`, then by attribute English/name fields. |
| `field` | Uses `parameter_mapping[name]` to read a sample field. |
| `constant` | Not supported in the first version. |

If a required parameter cannot be resolved, that metric output is marked as `success=false`.

If `sample[state_key]` is a string, the mapper parses it with `json.loads(...)` before passing it to `calculate(...)` or
before resolving `source=attribute` parameters. Metric scripts can therefore treat `state` as a decoded object. Invalid
JSON strings are recorded as metric failures and do not require every metric script to parse JSON by itself.

## Output

The mapper writes an object to `output_key`:

```json
{
  "query_metric_data_outputs": {
    "bench_roi_score": {
      "success": true,
      "value": 0.82,
      "error": "",
      "operator_id": 201,
      "operator_name_cn": "行业基准 ROI 得分"
    },
    "quality_score": {
      "success": false,
      "value": null,
      "error": "missing required parameter: bench_roi",
      "operator_id": 202,
      "operator_name_cn": "质量得分"
    }
  }
}
```

The result key is `operatorNameEn`; if missing, it falls back to `operator_{operator_id}`.

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
            bench_roi: "bench_roi"
        - operator_id: 202
          parameter_mapping:
            industry: "industry"
            target_roi: "target_roi"
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
