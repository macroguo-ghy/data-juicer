# Python Script Mapper Lance Test Guide

This guide shows how to test `python_script_mapper` with a production-shaped nested sample in Lance source and output
tables. The test script reads nested fields from the current sample and adds several flat output fields.

## Test Goal

Verify that `python_script_mapper` can:

- read nested fields such as `input.user_query`, `meta_info.unique_id`, and `meta_info.scenario.scenario_id`
- preserve the original sample fields
- add JSON/Lance-friendly scalar output fields
- receive backend-injected `ctx` through `context["ctx"]`

## 1. Create Source Table

The source table uses the same nested shape as the sample data.

```python
import pyarrow as pa
from pyiceberg.magnus import MagnusClient

magnus_client = MagnusClient()

source_schema = pa.schema([
    pa.field("__adc_record_key", pa.string()),
    pa.field(
        "context",
        pa.struct([
            pa.field("env_state", pa.string()),
            pa.field(
                "memory",
                pa.struct([
                    pa.field("chat_history", pa.string()),
                    pa.field("extracted_facts", pa.string()),
                    pa.field("user_profile", pa.string()),
                ]),
            ),
        ]),
    ),
    pa.field(
        "extra",
        pa.struct([
            pa.field("chat_id", pa.string()),
            pa.field("message_id", pa.string()),
        ]),
    ),
    pa.field(
        "input",
        pa.struct([
            pa.field("response", pa.string()),
            pa.field("supplementary_info", pa.string()),
            pa.field("task", pa.string()),
            pa.field("trajectory", pa.string()),
            pa.field("user_query", pa.string()),
        ]),
    ),
    pa.field(
        "meta_info",
        pa.struct([
            pa.field(
                "scenario",
                pa.struct([
                    pa.field("business_line", pa.string()),
                    pa.field("event_type", pa.int64()),
                    pa.field("query_type", pa.string()),
                    pa.field("scenario_id", pa.string()),
                    pa.field("task_type", pa.string()),
                ]),
            ),
            pa.field("unique_id", pa.int64()),
            pa.field(
                "自定义",
                pa.struct([
                    pa.field("自定义", pa.string()),
                ]),
            ),
        ]),
    ),
    pa.field(
        "reference",
        pa.struct([
            pa.field("content", pa.string()),
            pa.field("function_call", pa.string()),
            pa.field("routing", pa.string()),
            pa.field("rubric_answers", pa.string()),
        ]),
    ),
    pa.field(
        "rubrics",
        pa.struct([
            pa.field("id1", pa.string()),
            pa.field("id2", pa.string()),
        ]),
    ),
])

source_table = magnus_client.create_table(
    catalog="zsy_test",
    database="default",
    table="wjd_test_python_script_source",
    schema=source_schema,
    properties={
        "write.format.default": "lance",
    },
)
```

## 2. Prepare Source Data

Write one row into the source table.

```python
import collections
import collections.abc

from pyiceberg.magnus import MagnusClient

# Python 3.11+ compatibility patch for older writer dependencies.
collections.Iterable = collections.abc.Iterable

magnus_client = MagnusClient()
table = magnus_client.load_table("zsy_test.default.wjd_test_python_script_source")

data = [
    {
        "__adc_record_key": "record-001",
        "context": {
            "env_state": "",
            "memory": {
                "chat_history": "",
                "extracted_facts": None,
                "user_profile": None,
            },
        },
        "extra": {
            "chat_id": None,
            "message_id": None,
        },
        "input": {
            "response": None,
            "supplementary_info": None,
            "task": None,
            "trajectory": None,
            "user_query": "帮我查一下：7608750000000000000",
        },
        "meta_info": {
            "scenario": {
                "business_line": "非闭环",
                "event_type": 1,
                "query_type": None,
                "scenario_id": "roi_low",
                "task_type": "客户问题反馈",
            },
            "unique_id": 7590091993904394000,
            "自定义": {
                "自定义": None,
            },
        },
        "reference": {
            "content": None,
            "function_call": None,
            "routing": None,
            "rubric_answers": None,
        },
        "rubrics": {
            "id1": None,
            "id2": None,
        },
    },
]

writer = table.get_writer(
    operation="OVERWRITE",
    snapshot_summary={
        "message": "Prepare python_script_mapper test source data",
    },
)
writer.write(data)
snapshot = writer.close()
print(f"Write Success. Snapshot: {snapshot}")
```

## 3. Create Output Table

The output table keeps the original fields and adds scalar fields generated by the script.

```python
import pyarrow as pa
from pyiceberg.magnus import MagnusClient

magnus_client = MagnusClient()

output_schema = pa.schema([
    pa.field("__adc_record_key", pa.string()),
    pa.field(
        "context",
        pa.struct([
            pa.field("env_state", pa.string()),
            pa.field(
                "memory",
                pa.struct([
                    pa.field("chat_history", pa.string()),
                    pa.field("extracted_facts", pa.string()),
                    pa.field("user_profile", pa.string()),
                ]),
            ),
        ]),
    ),
    pa.field(
        "extra",
        pa.struct([
            pa.field("chat_id", pa.string()),
            pa.field("message_id", pa.string()),
        ]),
    ),
    pa.field(
        "input",
        pa.struct([
            pa.field("response", pa.string()),
            pa.field("supplementary_info", pa.string()),
            pa.field("task", pa.string()),
            pa.field("trajectory", pa.string()),
            pa.field("user_query", pa.string()),
        ]),
    ),
    pa.field(
        "meta_info",
        pa.struct([
            pa.field(
                "scenario",
                pa.struct([
                    pa.field("business_line", pa.string()),
                    pa.field("event_type", pa.int64()),
                    pa.field("query_type", pa.string()),
                    pa.field("scenario_id", pa.string()),
                    pa.field("task_type", pa.string()),
                ]),
            ),
            pa.field("unique_id", pa.int64()),
            pa.field(
                "自定义",
                pa.struct([
                    pa.field("自定义", pa.string()),
                ]),
            ),
        ]),
    ),
    pa.field(
        "reference",
        pa.struct([
            pa.field("content", pa.string()),
            pa.field("function_call", pa.string()),
            pa.field("routing", pa.string()),
            pa.field("rubric_answers", pa.string()),
        ]),
    ),
    pa.field(
        "rubrics",
        pa.struct([
            pa.field("id1", pa.string()),
            pa.field("id2", pa.string()),
        ]),
    ),
    pa.field("script_user_query", pa.string()),
    pa.field("script_query_id", pa.string()),
    pa.field("script_unique_id", pa.string()),
    pa.field("script_scenario_id", pa.string()),
    pa.field("script_business_line", pa.string()),
    pa.field("script_has_response", pa.bool_()),
    pa.field("script_user_account", pa.string()),
])

output_table = magnus_client.create_table(
    catalog="zsy_test",
    database="default",
    table="wjd_test_python_script_output",
    schema=output_schema,
    properties={
        "write.format.default": "lance",
    },
)
```

## 4. Operator YAML

The script extracts fields from the nested sample and writes flat output fields. Keep the return value as a dictionary
and only add Lance-friendly scalar fields.

```yaml
process:
  - python_script_mapper:
      ctx:
        userAccount: "wangjianda.667"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        apiBase: "https://ai-data-center.bytedance.net/api"
        synthesisInstanceId: 10001
        taskId: 30001
        taskVersion: 1
        operatorIndex: 0
        operatorName: "python_script_mapper"
        operatorType: "Mapper"
      python_code: |
        import re

        def process(sample, context):
            input_obj = sample.get("input") or {}
            meta_info = sample.get("meta_info") or {}
            scenario = meta_info.get("scenario") or {}

            user_query = input_obj.get("user_query") or ""
            matched = re.search(r"\d{10,}", user_query)

            sample["script_user_query"] = user_query
            sample["script_query_id"] = matched.group(0) if matched else ""
            sample["script_unique_id"] = str(meta_info.get("unique_id") or "")
            sample["script_scenario_id"] = scenario.get("scenario_id") or ""
            sample["script_business_line"] = scenario.get("business_line") or ""
            sample["script_has_response"] = input_obj.get("response") is not None
            sample["script_user_account"] = context["ctx"].get("userAccount", "")
            return sample
```

## 5. Expected Output

The output row should preserve all original fields and add:

```json
{
  "script_user_query": "帮我查一下：7608750000000000000",
  "script_query_id": "7608750000000000000",
  "script_unique_id": "7590091993904394000",
  "script_scenario_id": "roi_low",
  "script_business_line": "非闭环",
  "script_has_response": false,
  "script_user_account": "wangjianda.667"
}
```

## 6. Validation Checklist

- The Ray/Data-Juicer task can load `python_script_mapper`.
- The script receives the full sample as `sample`.
- The script receives platform context as `context["ctx"]`.
- The output Lance table contains the added scalar fields.
- `script_query_id` equals `7608750000000000000`.
- `script_unique_id` is a string, not an integer, to avoid downstream precision or JSON display issues.
- No dynamic object field is added by the script.

## Notes

- `python_script_mapper` is not a sandbox. The script runs in the current Python process and should only contain
  trusted code.
- YAML indentation under `python_code: |` must be preserved. The Python code must be indented under the YAML block.
- The mapper returns exactly the dictionary returned by `process(sample, context)`. Always return `sample` after
  adding fields.
