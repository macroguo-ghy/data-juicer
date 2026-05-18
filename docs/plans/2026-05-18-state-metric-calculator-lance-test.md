# State Metric Calculator Lance Test Guide

This guide shows how to test `state_metric_calculator` with Lance source and output tables. The test focuses on:

- loading selected State derived metric definitions through OpenAPI
- resolving implicit `state`, `placeholder`, and `defaultValue` parameters from each sample
- writing stable object output for both successful and failed metric results

## Test Goal

Verify that `state_metric_calculator` can:

- call `/openapi/state-meta/operators/batch-get` with selected `operator_id` values
- execute each returned `operatorCode.calculate(...)`
- write metric results to `query_metric_data_outputs`
- keep metric result schema stable when one row succeeds and another row fails

## 0. Pick Test Operators

The mapper does not accept `operatorCode` or `inputParameter` snapshots. It fetches the latest operator definitions by
`operator_id`, so use real IDs from the current test environment.

Example IDs below:

```text
201 -> bench_roi_score
202 -> quality_score
```

Replace them with real state-meta derived metric IDs if your environment uses different IDs.

Expected operator shape:

```json
{
  "id": 201,
  "operatorNameEn": "bench_roi_score",
  "operatorNameCn": "行业基准 ROI 得分",
  "inputParameter": "{\"params\":[...]}",
  "operatorCode": "def calculate(...): ..."
}
```

The output table schema stores all metric items in `query_metric_data_outputs.metrics[]`.

## 1. Create Source Table

This source table contains:

- `state`: State object passed to `calculate(state, ...)`. It may also be stored as a JSON string; the mapper decodes
  JSON-string state values before running metric code.
- `bench_roi`: source table field used by `parameter_mapping` for a `data_type=placeholder` parameter.

```python
import pyarrow as pa
from pyiceberg.magnus import MagnusClient

magnus_client = MagnusClient()

source_schema = pa.schema([
    pa.field("__adc_record_key", pa.string()),
    pa.field(
        "state",
        pa.struct([
            pa.field("101", pa.float64()),
            pa.field("102", pa.float64()),
            pa.field("ad_material_click_rate", pa.float64()),
            pa.field("quality", pa.float64()),
        ]),
    ),
    pa.field("bench_roi", pa.float64()),
])

source_table = magnus_client.create_table(
    catalog="zsy_test",
    database="default",
    table="wjd_test_state_metric_source",
    schema=source_schema,
    properties={
        "write.format.default": "lance",
    },
)
```

## 2. Prepare Source Data

The first row should calculate successfully. The second row leaves `bench_roi` empty so the metric depending on that
field can produce a failed metric result while the record itself still succeeds.

```python
import collections
import collections.abc

from pyiceberg.magnus import MagnusClient

# Python 3.11+ compatibility patch for older writer dependencies.
collections.Iterable = collections.abc.Iterable

magnus_client = MagnusClient()
table = magnus_client.load_table("zsy_test.default.wjd_test_state_metric_source")

data = [
    {
        "__adc_record_key": "record-001",
        "state": {
            "101": 0.41,
            "102": 8.0,
            "ad_material_click_rate": 0.41,
            "quality": 8.0,
        },
        "bench_roi": 0.5,
    },
    {
        "__adc_record_key": "record-002",
        "state": {
            "101": 0.8,
            "102": 5.0,
            "ad_material_click_rate": 0.8,
            "quality": 5.0,
        },
        "bench_roi": None,
    },
]

writer = table.get_writer(
    operation="OVERWRITE",
    snapshot_summary={
        "message": "Prepare state_metric_calculator test source data",
    },
)
writer.write(data)
snapshot = writer.close()
print(f"Write Success. Snapshot: {snapshot}")
```

## 3. Create Output Table

For Lance stability, the output object uses the same field shape:

```text
id, metrics
```

Successful metric `output` values are JSON strings, so the schema below stores `output` as `pa.string()`.

```python
import pyarrow as pa
from pyiceberg.magnus import MagnusClient

magnus_client = MagnusClient()

metric_item_schema = pa.struct([
    pa.field("metricCode", pa.string()),
    pa.field("metricName", pa.string()),
    pa.field("output", pa.string()),
    pa.field("error", pa.string()),
])

output_schema = pa.schema([
    pa.field("__adc_record_key", pa.string()),
    pa.field(
        "state",
        pa.struct([
            pa.field("101", pa.float64()),
            pa.field("102", pa.float64()),
            pa.field("ad_material_click_rate", pa.float64()),
            pa.field("quality", pa.float64()),
        ]),
    ),
    pa.field("bench_roi", pa.float64()),
    pa.field(
        "query_metric_data_outputs",
        pa.struct([
            pa.field("id", pa.string()),
            pa.field("metrics", pa.list_(metric_item_schema)),
        ]),
    ),
])

output_table = magnus_client.create_table(
    catalog="zsy_test",
    database="default",
    table="wjd_test_state_metric_output",
    schema=output_schema,
    properties={
        "write.format.default": "lance",
    },
)
```

If a metric item fails, `output` is null and `error` contains the failure message.

## 4. Operator YAML

```yaml
process:
  - state_metric_calculator:
      ctx:
        userAccount: "wangjianda.667"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        apiBase: "https://ai-data-center.bytedance.net/api"
        synthesisInstanceId: 10001
        taskId: 30001
        taskVersion: 1
        operatorIndex: 0
        operatorName: "state_metric_calculator"
        operatorType: "Mapper"
      state_key: "state"
      output_key: "query_metric_data_outputs"
      result_mode: "object"
      fail_policy: "continue"
      operators:
        - operator_id: 201
          parameter_mapping:
            bench_roi: "bench_roi"
        - operator_id: 202
          parameter_mapping: {}
```

Notes:

- Replace `operator_id` values with real derived metric IDs.
- `parameter_mapping` only maps `inputParameter.params[]` items with `data_type=placeholder`.
- `state_key` controls the State field name and does not enter `parameter_mapping`.
- `inputParameter.params[]` items with `data_type=defaultValue` do not enter `parameter_mapping`.

## 5. Expected Output Shape

The first row should contain successful metric results:

```json
{
  "__adc_record_key": "record-001",
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
        "output": "9.0",
        "error": ""
      }
    ]
  }
}
```

The second row can contain a failed result for metrics that require `bench_roi`:

```json
{
  "__adc_record_key": "record-002",
  "query_metric_data_outputs": {
    "id": "1854168911595796",
    "metrics": [
      {
        "metricCode": "bench_roi_score",
        "metricName": "行业基准 ROI 得分",
        "output": null,
        "error": "missing required parameter: bench_roi"
      }
    ]
  }
}
```

The exact `error` text depends on the current operator definition. The important part is that failed metric items still
contain `output: null` and an `error` message.

## 6. Validation Checklist

- The Ray/Data-Juicer task can load `state_metric_calculator`.
- The mapper calls `/openapi/state-meta/operators/batch-get`.
- `query_metric_data_outputs` exists in every output row.
- Every output object contains `id` and `metrics`.
- Every successful metric item contains `metricCode`, `metricName`, and `output`.
- At least one failed metric item contains `output: null` and `error` if the chosen operator can be tested with missing
  or invalid input.
- No Lance/PyArrow struct field-order error appears during write.

## Notes

- The mapper executes trusted Python code returned by the platform. It is not a sandbox.
- The mapper caches operator details inside one mapper instance. Ray may create multiple worker-side instances, so each
  worker can call the batch-get API once.
- Successful `output` values are JSON strings. Consumers should parse them when they need the original scalar, array, or
  object value.
