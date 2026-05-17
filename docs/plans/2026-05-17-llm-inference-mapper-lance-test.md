# LLM Inference Mapper Lance Test Guide

This guide shows how to test `llm_inference_mapper` with Lance source and output tables. The test focuses on
`prompt_template` variable rendering, including nested object fields and array-object expansion.

## Test Goal

Verify that the mapper can render prompts from source samples like:

- object path: `{article.title}`, `{article.body}`
- array-object path: `{metrics[*].name}`, `{metrics[].value}`
- unrelated fields are ignored when they are not referenced by the prompt template

## 1. Create Source Table

The source table stores two input samples. Each sample contains an article object, a metrics array, and an unused
object field.

```python
import pyarrow as pa
from pyiceberg.magnus import MagnusClient

magnus_client = MagnusClient()

source_schema = pa.schema([
    pa.field("__adc_record_key", pa.string()),
    pa.field(
        "article",
        pa.struct([
            pa.field("title", pa.string()),
            pa.field("body", pa.string()),
        ]),
    ),
    pa.field(
        "metrics",
        pa.list_(
            pa.struct([
                pa.field("name", pa.string()),
                pa.field("value", pa.int64()),
            ])
        ),
    ),
    pa.field(
        "unused",
        pa.struct([
            pa.field("raw", pa.string()),
        ]),
    ),
])

source_table = magnus_client.create_table(
    catalog="zsy_test",
    database="default",
    table="wjd_test_llm_test_source",
    schema=source_schema,
    properties={
        "write.format.default": "lance",
    },
)
```

## 2. Prepare Source Data

Use these two rows to test template variable rendering:

```jsonl
{"__adc_record_key":"record-001","article":{"title":"北京周末出行","body":"今天北京天气晴朗，气温适宜，公园和郊区都适合户外活动。"},"metrics":[{"name":"曝光","value":1200},{"name":"点击","value":86}],"unused":{"raw":"这个字段不会被 prompt_template 引用"}}
{"__adc_record_key":"record-002","article":{"title":"上海通勤提醒","body":"明天上海早高峰可能有小雨，建议提前出门并携带雨具。"},"metrics":[{"name":"曝光","value":980},{"name":"点击","value":73}],"unused":{"raw":"这个字段也不会被引用"}}
```

After creating the source table, write the two rows into it:

```python
import collections
import collections.abc

from pyiceberg.magnus import MagnusClient

# Python 3.11+ compatibility patch for older writer dependencies.
collections.Iterable = collections.abc.Iterable

magnus_client = MagnusClient()

table = magnus_client.load_table("zsy_test.default.wjd_test_llm_test_source")

data = [
    {
        "__adc_record_key": "record-001",
        "article": {
            "title": "北京周末出行",
            "body": "今天北京天气晴朗，气温适宜，公园和郊区都适合户外活动。",
        },
        "metrics": [
            {
                "name": "曝光",
                "value": 1200,
            },
            {
                "name": "点击",
                "value": 86,
            },
        ],
        "unused": {
            "raw": "这个字段不会被 prompt_template 引用",
        },
    },
    {
        "__adc_record_key": "record-002",
        "article": {
            "title": "上海通勤提醒",
            "body": "明天上海早高峰可能有小雨，建议提前出门并携带雨具。",
        },
        "metrics": [
            {
                "name": "曝光",
                "value": 980,
            },
            {
                "name": "点击",
                "value": 73,
            },
        ],
        "unused": {
            "raw": "这个字段也不会被引用",
        },
    },
]

writer = table.get_writer(
    operation="OVERWRITE",
    snapshot_summary={
        "message": "Prepare LLM mapper test source data",
    },
)
writer.write(data)
snapshot = writer.close()
print(f"Write Success. Snapshot: {snapshot}")
```

## 3. Create Output Table

For this smoke test, define `llm_output` as `string` because the current LLM service can return plain text output,
for example `"上海通勤提醒指出明天早高峰可能有小雨..."`. `llm_metadata` is still a dictionary, so define it as a
struct.

```python
import pyarrow as pa
from pyiceberg.magnus import MagnusClient

magnus_client = MagnusClient()

output_schema = pa.schema([
    pa.field("__adc_record_key", pa.string()),
    pa.field(
        "article",
        pa.struct([
            pa.field("title", pa.string()),
            pa.field("body", pa.string()),
        ]),
    ),
    pa.field(
        "metrics",
        pa.list_(
            pa.struct([
                pa.field("name", pa.string()),
                pa.field("value", pa.int64()),
            ])
        ),
    ),
    pa.field(
        "unused",
        pa.struct([
            pa.field("raw", pa.string()),
        ]),
    ),
    pa.field("llm_output", pa.string()),
    pa.field(
        "llm_metadata",
        pa.struct([
            pa.field("taskId", pa.string()),
            pa.field("conversationId", pa.string()),
            pa.field("requestId", pa.string()),
            pa.field("resultStatus", pa.string()),
            pa.field("status", pa.string()),
        ]),
    ),
])

output_table = magnus_client.create_table(
    catalog="zsy_test",
    database="default",
    table="wjd_test_llm_test_output",
    schema=output_schema,
    properties={
        "write.format.default": "lance",
    },
)
```

## 4. Operator YAML

Configure `prompt_template` with nested object paths and array-object expansion.

```yaml
process:
  - llm_inference_mapper:
      ctx:
        userAccount: "wangjianda.667"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        apiBase: "https://ai-data-center.bytedance.net/api"
        synthesisInstanceId: 147
        taskId: 6
        taskVersion: 19
        operatorIndex: 0
        operatorName: "llm_inference_mapper"
        operatorType: "Mapper"
      prompt_template: |
        请根据以下内容生成一句话摘要。

        标题：{article.title}
        正文：{article.body}
        指标名称：{metrics[*].name}
        指标数值：{metrics[].value}
      model: "doubao"
      output_field: "llm_output"
      metadata_field: "llm_metadata"
      poll_interval_seconds: 2
      max_poll_attempts: 60
      timeout: 30
```

## 5. Expected Prompt Rendering

For the first row, the rendered prompt should contain:

```text
标题：北京周末出行
正文：今天北京天气晴朗，气温适宜，公园和郊区都适合户外活动。
指标名称：["曝光", "点击"]
指标数值：[1200, 86]
```

For the second row, the rendered prompt should contain:

```text
标题：上海通勤提醒
正文：明天上海早高峰可能有小雨，建议提前出门并携带雨具。
指标名称：["曝光", "点击"]
指标数值：[980, 73]
```

## 6. Expected Output Fields

After the operator succeeds, each output row should keep the original fields and add:

```json
{
  "llm_output": "上海通勤提醒指出明天早高峰可能有小雨，建议提前出门并携带雨具。",
  "llm_metadata": {
    "taskId": "task-xxx",
    "conversationId": "conv-xxx",
    "requestId": "req-xxx",
    "resultStatus": "SUCCESS",
    "status": "success"
  }
}
```

`llm_metadata` normally includes task metadata such as `taskId`, `conversationId`, `requestId`, `resultStatus`, and
`status`. If the LLM service returns object output in another scenario, adjust `llm_output` in the output table schema
to match the actual response, or add a downstream step that stringifies the object before writing to a string column.
