# LLM Inference Mapper Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an ADC business mapper that submits a prompt to the AD AI Data Center LLM inference OpenAPI, polls until completion, and writes the LLM output back to each sample.

**Architecture:** Put the operator under `data_juicer/ops/mapper/ad_ai_data_center` while registering it as `llm_inference_mapper`, without the `ad_ai_data_center` name prefix. The mapper builds a prompt from the current sample, calls the server-side `/submit` and `/result` APIs through the existing `HttpClient`, and relies on server-side TCC for Workflow app configuration and authorization. The mapper follows the existing ADC lifecycle pattern: `NEED_CTX = True`, `/start` before processing, `/record` for each sample, `/finalize` or `/failed` after the operator finishes, and test-card notification only on operator start/finish.

**Critical Assumptions & Early Checks:** The backend injects `ctx.apiBase` and `ctx.userAccount`; the operator does not accept appId or Authorization. The server response follows the documented `{code, message, data}` envelope. `data.taskId` from `/submit` is required. The result API eventually returns `finished=true`; the mapper must fail clearly on timeout, missing taskId, failed workflow status, or malformed output. Upstream should provide `__adc_record_key` when record callbacks are required.

**Tech Stack:** Data-Juicer `Mapper`, ADC `HttpClient`, operator execution callback utils, notification utils, `time.sleep`, `unittest`.

---

## API Contract

### Submit LLM Inference Task

```http
POST /openapi/synthesis/llm-inference/submit
Content-Type: application/json
```

Request body:

```json
{
  "prompt": "请根据以下内容生成摘要：xxx",
  "model": "doubao"
}
```

Fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `prompt` | `string` | Yes | LLM inference input. |
| `model` | `string` | No | Model name. If not configured, send an empty string to the server. |

Success response:

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "taskId": "task-xxx",
    "conversationId": "conv-xxx",
    "requestId": "req-xxx"
  }
}
```

### Query LLM Inference Result

```http
POST /openapi/synthesis/llm-inference/result
Content-Type: application/json
```

Request body:

```json
{
  "taskId": "task-xxx"
}
```

Running response:

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "finished": false,
    "success": null,
    "resultStatus": "RUNNING",
    "status": "executing",
    "output": null,
    "message": null,
    "taskId": "task-xxx",
    "conversationId": "conv-xxx",
    "requestId": "req-xxx"
  }
}
```

Success response:

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "finished": true,
    "success": true,
    "resultStatus": "SUCCESS",
    "status": "success",
    "output": {
      "summary": "这里是生成结果"
    },
    "message": null,
    "taskId": "task-xxx",
    "conversationId": "conv-xxx",
    "requestId": "req-xxx"
  }
}
```

Failure response:

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "finished": true,
    "success": false,
    "resultStatus": "FAILED",
    "status": "fail",
    "output": null,
    "message": "workflow task fail, request_id=req-xxx",
    "taskId": "task-xxx",
    "conversationId": "conv-xxx",
    "requestId": "req-xxx"
  }
}
```

---

## Operator Contract

### Name And Location

```python
OP_NAME = "llm_inference_mapper"
NEED_CTX = True
```

File:

```text
data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py
```

### Parameters

| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `prompt` | `str` | No | `None` | Static prompt. |
| `prompt_template` | `str` | No | `None` | Prompt template rendered from Jinja-style sample field placeholders, for example `请总结：{{ text }}`. |
| `prompt_field` | `str` | No | `None` | Field name that stores the prompt in each sample. |
| `model` | `str` | No | `""` | Model name. Empty string is sent when not configured. |
| `output_field` | `str` | No | `"llm_output"` | Field used to store `data.output` as a string. Object and array outputs are JSON-stringified. |
| `metadata_field` | `str` | No | `None` | Optional field used to store JSON-stringified task metadata such as `taskId`, `conversationId`, and `requestId`. If omitted or set to `null`, metadata is not written. |
| `poll_interval_seconds` | `float` | No | `2.0` | Sleep time between result polling requests. |
| `max_poll_attempts` | `int` | No | `300` | Maximum result polling attempts before timeout. |
| `timeout` | `float` | No | `30.0` | HTTP request timeout in seconds. |
| `ctx` | `dict` | Yes | `None` | Backend-injected platform context. |

Prompt source precedence:

1. `prompt_field`
2. `prompt_template`
3. `prompt`

At least one prompt source must be configured. Empty prompt after rendering is invalid. `prompt_field` and
`prompt_template` both read values from the current sample. `prompt_field` treats one sample field as the complete
prompt. `prompt_template` reads one or more sample fields and renders them into a prompt.

### Prompt Template Syntax

`prompt_template` only treats Jinja-style `{{ field }}` placeholders as variables. A placeholder name must match a
field path in the current sample. Single-brace text such as `{field}` is kept as ordinary prompt text.

Sample:

```json
{
  "title": "文章标题",
  "text": "这是一段长文本"
}
```

Template:

```yaml
prompt_template: "请总结文章《{{ title }}》：{{ text }}"
```

Rendered prompt:

```text
请总结文章《文章标题》：这是一段长文本
```

For multiline prompts, use a YAML block scalar:

```yaml
prompt_template: |
  请根据以下信息生成摘要：

  标题：{{ title }}

  正文：
  {{ text }}
```

If a placeholder field is missing from the sample, the mapper should raise a clear `ValueError` that includes the missing field name.

Jinja-style placeholders support object paths and array expansion:

```yaml
prompt_template: |
  State 模板：
  {{ state_template }}

  城市：{{ a.b.d }}
  指标：{{ items[*].metric }}
```

If a placeholder value is a `dict` or `list`, the mapper serializes it with `json.dumps(..., ensure_ascii=False)`
before rendering. This keeps object and array fields as standard JSON text instead of Python `dict` / `list` repr.
The mapper only resolves and serializes fields referenced by placeholders, so unrelated complex fields in the sample
do not affect prompt rendering.

Sample:

```json
{
  "content": {
    "city": "北京",
    "weather": "晴朗"
  },
  "tags": ["天气", "户外"]
}
```

Template:

```yaml
prompt_template: "请总结对象：{{ content }}；标签：{{ tags }}"
```

Rendered prompt:

```text
请总结对象：{"city": "北京", "weather": "晴朗"}；标签：["天气", "户外"]
```

Object fields can be addressed with dot paths:

```json
{
  "a": {
    "b": {
      "d": "北京"
    }
  }
}
```

Template:

```yaml
prompt_template: "城市：{{ a.b.d }}"
```

Rendered prompt:

```text
城市：北京
```

Array-object fields support whole-array expansion with `[]` or `[*]`. Specific numeric indexes are not supported.

```json
{
  "items": [
    {
      "metric": 1,
      "name": "曝光"
    },
    {
      "metric": 2,
      "name": "点击"
    }
  ]
}
```

Template:

```yaml
prompt_template: "指标：{{ items[*].metric }}；名称：{{ items[].name }}"
```

Rendered prompt:

```text
指标：[1, 2]；名称：["曝光", "点击"]
```

If the prompt is already prepared in a sample field, use `prompt_field`:

```yaml
prompt_field: "llm_prompt"
```

This reads `sample["llm_prompt"]` as the full prompt and does not render `prompt_template`.

### Polling Status Logic

The mapper polls `/result` until the task reaches a terminal state or exceeds `max_poll_attempts`.

Running condition:

```text
finished = false OR resultStatus = RUNNING
```

Success condition:

```text
success is not false
AND finished = true
AND (success = true OR resultStatus = SUCCESS)
```

Failure condition:

```text
success = false
OR resultStatus = FAILED
OR any non-running, non-success terminal response
```

The error message is selected from `message`, then `resultStatus`, then `status`, then the default
`LLM inference failed`.

With the default configuration:

```yaml
poll_interval_seconds: 2
max_poll_attempts: 300
```

the normal polling window is about ten minutes. The HTTP `timeout` parameter is still applied to each individual
request, so a stuck request can add extra wait time up to the configured per-request timeout.

### Output

For a successful inference, the mapper returns the full sample with:

```json
{
  "llm_output": "这里是生成结果"
}
```

The operator stores the server `output` as a string to avoid Lance/PyArrow dynamic struct schema issues. If `output`
is already a string, it is preserved. If `output` is a `dict`, `list`, number, or boolean, it is serialized with
`json.dumps(..., ensure_ascii=False)`. By default, the mapper does not write metadata. If `metadata_field` is
explicitly configured, task metadata is stored in that field as a JSON string. This avoids PyArrow struct field-order
failures when writing to Lance/Magnus tables.

### YAML Example

```yaml
process:
  - llm_inference_mapper:
      ctx:
        userAccount: "wangjianda.667"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        apiBase: "https://ai-data-center.bytedance.net/api"
        synthesisInstanceId: 148
        taskId: 6
        taskVersion: 20
        operatorIndex: 3
        operatorName: "llm_inference_mapper"
        operatorType: "Mapper"
      prompt_template: "请根据以下内容生成摘要：{{ text }}"
      model: "doubao"
      output_field: "llm_output"
      poll_interval_seconds: 2
      max_poll_attempts: 300
```

---

### Task 1: LLM Inference Mapper Tests

**Files:**
- Create: `tests/ops/mapper/test_llm_inference_mapper.py`

**Step 1: Write failing tests**

Cover:
- config can instantiate `llm_inference_mapper` from YAML
- `NEED_CTX = True`
- prompt can be built from `prompt_field`
- prompt can be built from `prompt_template`
- submit request posts `prompt` and `model`
- result polling retries while `resultStatus = RUNNING`
- success writes `output_field` as a string
- success writes `metadata_field` as a string only when `metadata_field` is explicitly configured
- failed result raises `ValueError` with response `message`
- missing `taskId` raises a clear `ValueError`
- timeout after `max_poll_attempts` raises a clear `TimeoutError`
- record success/failure callbacks include per-record `started_at`
- notification is sent only on operator start/finish
- notification failure does not block lifecycle callback

**Step 2: Run tests to verify RED**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_llm_inference_mapper
```

Expected: fail because `llm_inference_mapper` is not implemented.

### Task 2: Implement LLM Inference Mapper

**Files:**
- Create: `data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py`

**Step 1: Implement constructor validation**

Validate:
- at least one prompt source is configured
- `output_field` is not empty
- `metadata_field` is either omitted / `null`, or a non-empty string
- `poll_interval_seconds >= 0`
- `max_poll_attempts > 0`

**Step 2: Implement prompt rendering**

Rules:
- `prompt_field`: read string from sample
- `prompt_template`: render only Jinja-style `{{ field }}` placeholders from sample fields
- only referenced placeholders are resolved and serialized
- nested object paths use dot syntax, for example `{{ a.b.d }}`
- array-object paths support `[]` and `[*]` as whole-array expansion, for example `{{ items[*].name }}`
- `dict` and `list` template values are serialized as JSON strings before rendering
- `prompt`: use static string
- missing template field should raise `ValueError` with the missing field name

**Step 3: Implement OpenAPI client calls**

Use `ctx.apiBase`:

```text
{apiBase}/openapi/synthesis/llm-inference/submit
{apiBase}/openapi/synthesis/llm-inference/result
```

Headers:

```json
{
  "Content-Type": "application/json",
  "Accept": "application/json",
  "user-account": "<ctx.userAccount>"
}
```

Also pass `x-tt-env` and `x-use-ppe` when present in `ctx`.

**Step 4: Implement polling**

Flow:
1. submit prompt/model
2. require returned `data.taskId`
3. loop result call
4. if `resultStatus = RUNNING` or `finished = false`, sleep and continue
5. if `resultStatus = SUCCESS` and `success is True`, write stringified output and metadata
6. if `resultStatus = FAILED` or `success is False`, raise `ValueError(message)`
7. after `max_poll_attempts`, raise `TimeoutError`

**Step 5: Implement ADC observation**

Use the same conventions as existing ADC mappers:
- `before_operator_started`: create callback client and send test-card start notification
- `after_operator_finished`: finalize/failed callback and send test-card finish notification
- `process_single`: report `/record SUCCESS` or `/record FAILED`
- callback and notification failures are observational and should not mask the main LLM result or main LLM failure

### Task 3: Verification

Run:

```bash
python3 -m py_compile data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py tests/ops/mapper/test_llm_inference_mapper.py
git diff --check
./.venv/bin/python -m unittest tests.ops.mapper.test_llm_inference_mapper
```

If broader regression risk is suspected, also run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_http_mapper tests.ops.mapper.test_python_script_mapper tests.ops.mapper.test_prepare_record_key_mapper
```

Expected: all pass.
