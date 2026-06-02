# Derived Input Parameter Context Mapper Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `derived_input_parameter_context_mapper`, an ADC business mapper that writes selected derived-field input parameter descriptions into each sample for downstream LLM prompts.

**Architecture:** Keep `state_template_mapper` focused on state template generation only. Introduce a separate mapper that calls `POST /openapi/state-meta/input-keys/batch-get`, formats returned `inputParameterDetails` records into prompt-ready strings, caches the formatted columns per mapper instance, and writes them into the sample using `keyNameEn` as the output column name.

**Critical Assumptions & Early Checks:** Backend will add `POST /openapi/state-meta/input-keys/batch-get` and return `inputParameterDetails` with at least `keyId`, `keyNameEn`, `keyNameCn`, `keyType`, `description`, `demoValue`, `multiValue`, `dataType`, and `defaultOrPlaceholderValue`. Confirm the request and response envelope before implementation. Existing `state_template_mapper` behavior must remain unchanged.

**Tech Stack:** Python 3, Data-Juicer `Mapper`, ADC `HttpClient`, ADC OpenAPI, unittest, Ray/HF Dataset mapper behavior.

---

## Requirement Summary

The new mapper enriches the current sample with prompt context columns selected by input parameter `keyId`:

```python
sample["unknown_id"] = "未知ID：可能表示广告、广告主或素材 ID，需要结合上游数据判断。示例：1837647382987362。支持多值。"
sample["startDate"] = "开始日期：查询开始日期。示例：2026-05-01。"
sample["endDate"] = "结束日期：查询结束日期。示例：2026-05-14。"
```

These fields are not metric results, not state template fields, and not derived-field calculation outputs. They are prompt context fields that explain the input parameters required by selected derived fields.

Recommended pipeline:

```yaml
process:
  - state_template_mapper:
      state_meta_group_items:
        ad_state:
          - 101
          - 102
      output_field: state_template
      ctx: ${ctx}

  - derived_input_parameter_context_mapper:
      input_key_ids:
        - 8
        - 12
        - 13
      ctx: ${ctx}

  - llm_state_generator:
      user_prompt: |
        State 模板：{{ state_template }}
        ID 入参说明：{{ unknown_id }}
        开始时间说明：{{ startDate }}
```

## Operator Contract

Create ADC business mapper:

```python
OP_NAME = "derived_input_parameter_context_mapper"
OP_DISPLAY_NAME = "生成派生字段入参元信息"
CONFIG_PAGE_KEY = "derived_input_parameter_context_builder"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
```

Constructor/YAML parameters:

```yaml
process:
  - derived_input_parameter_context_mapper:
      input_key_ids:
        - 8
        - 12
        - 13
      ctx:
        apiBase: "https://ai-data-center.bytedance.net/api"
        userAccount: "zhangsan"
        spaceId: 1
```

Parameter semantics:

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `input_key_ids` | `list[int]` | required | Input parameter key IDs selected by the user. Each key ID maps to one backend input key metadata record. |
| `ctx` | `dict` or `None` | `None` | ADC context. Required for this mapper because it must call the metadata API. |

Rules:

- `input_key_ids` must be a non-empty list of IDs.
- Frontend should pass the selected input key IDs from the user's checkbox selection.
- Do not infer selected parameters from state template IDs or derived operator IDs.
- Do not modify `state_template_mapper` for this capability.

## Metadata API Contract

Use the new input-key metadata endpoint:

```http
POST {ctx.apiBase}/openapi/state-meta/input-keys/batch-get
Content-Type: application/json
space-id: {ctx.spaceId}
user-account: {ctx.userAccount}
```

Expected request body:

```json
{
  "keyIds": [8, 12, 13]
}
```

Expected response envelope:

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "inputParameterDetails": [
      {
        "keyId": 8,
        "keyNameEn": "unknown_id",
        "keyNameCn": "未知ID",
        "keyType": "AMBIGUOUS",
        "description": "可能表示广告、广告主或素材 ID，需要结合上游数据判断",
        "demoValue": "1837647382987362",
        "multiValue": true,
        "dataType": "placeholder",
        "defaultOrPlaceholderValue": "unknown_id"
      },
      {
        "keyId": 12,
        "keyNameEn": "startDate",
        "keyNameCn": "开始日期",
        "keyType": "CONCRETE",
        "description": "查询开始日期",
        "demoValue": "2026-05-01",
        "multiValue": false,
        "dataType": "placeholder",
        "defaultOrPlaceholderValue": "startDate"
      }
    ]
  }
}
```

Implementation should validate this endpoint shape early. If the backend envelope changes before implementation, update only the metadata request/parser task and its tests.

## Output Formatting

Each `inputParameterDetails[]` item becomes one candidate column:

```python
column_name = detail["keyNameEn"]
column_value = format_parameter_description(detail)
sample[column_name] = column_value
```

Recommended format:

```text
{keyNameCn}：{description}。示例：{demoValue}。支持多值。
```

Formatting rules:

- `keyId` is required for request selection but does not appear in output.
- `keyNameEn` is required and is used as the output column name.
- `keyNameCn` is optional; if missing, use `keyNameEn`.
- Append `description` only when non-empty.
- Append `demoValue` only when non-empty.
- Append `支持多值。` only when `multiValue` is true.
- Output must be a plain string, not JSON, because the target consumer is LLM prompt text.
- The output string should not include `keyId`, `keyType`, `dataType`, or `defaultOrPlaceholderValue`; those are backend/UI metadata.

Duplicate and collision rules:

- Deduplicate by `keyId` before formatting.
- If duplicated `keyId` records have identical content, keep one.
- If duplicated `keyId` records differ, keep the first record and log a warning.
- If two different `keyId` records resolve to the same `keyNameEn`, raise `ValueError`, because they would write the same sample column with ambiguous meaning.

## Caching

Cache formatted parameter columns per mapper instance:

```python
self._parameter_columns_cache = None
```

Reasoning:

- Metadata depends on static key ID configuration, not per-record sample values.
- In Ray execution, each worker may fetch once, which is acceptable for mapper-local caches.
- Record-level `x-tt-logid` can still be added to the first sample that triggers the fetch.

## Record Callback

This mapper is an ADC business operator and should support Record success/failure callbacks consistently with sibling mappers.

Behavior:

- On success, report record success with the updated sample.
- On metadata fetch or formatting failure, report record failure and re-raise.
- Callback failures should be logged as warnings and must not mask the main business result/failure.
- Missing `__adc_record_key` should follow the same behavior as other ADC business mappers that require record callbacks.

## Error Handling

Fail fast when:

- `input_key_ids` is empty or invalid.
- `ctx` is missing or does not contain enough information to call the API.
- metadata HTTP request fails.
- backend business response has non-zero `code`.
- response data is not a dictionary containing an `inputParameterDetails` list.
- any selected metadata item misses `keyId` or `keyNameEn`.
- two different selected key IDs map to the same `keyNameEn`.

## Task 1: Operator Skeleton and Config Contract

**Files:**
- Create: `data_juicer/ops/mapper/ad_ai_data_center/derived_input_parameter_context_mapper.py`
- Modify: `tests/ops/mapper/test_derived_input_parameter_context_mapper.py`

**Step 1: Write failing constructor and config tests**

Create focused tests:

```python
def test_accepts_input_key_ids_config(self):
    op = DerivedInputParameterContextMapper(
        input_key_ids=[8, 12, 13],
        ctx=self._ctx(),
    )
    self.assertEqual(op.input_key_ids, [8, 12, 13])


def test_rejects_empty_input_key_ids(self):
    with self.assertRaisesRegex(ValueError, "input_key_ids"):
        DerivedInputParameterContextMapper(input_key_ids=[], ctx=self._ctx())
```

Add an operator loading test with YAML:

```yaml
process:
  - derived_input_parameter_context_mapper:
      input_key_ids:
        - 8
      ctx:
        apiBase: "https://ai-data-center.bytedance.net/api"
        userAccount: "zhangsan"
        spaceId: 1
```

Assert the operator loads and preserves config.

**Step 2: Run RED**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_derived_input_parameter_context_mapper
```

Expected: new mapper module does not exist.

**Step 3: Implement minimal skeleton**

Create mapper with constants:

```python
OP_NAME = "derived_input_parameter_context_mapper"
OP_DISPLAY_NAME = "生成派生字段入参元信息"
CONFIG_PAGE_KEY = "derived_input_parameter_context_builder"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
INPUT_KEYS_BATCH_GET_PATH = "/openapi/state-meta/input-keys/batch-get"
```

Constructor:

```python
def __init__(self, input_key_ids: list[int], ctx: dict | None = None, *args, **kwargs):
    super().__init__(*args, **kwargs)
    if not isinstance(input_key_ids, list) or not input_key_ids:
        raise ValueError("input_key_ids must be a non-empty list")
    self.input_key_ids = [int(item) for item in input_key_ids]
    self.ctx = ctx
    self._parameter_columns_cache = None
```

**Step 4: Run GREEN**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_derived_input_parameter_context_mapper
```

Expected: tests pass.

**Step 5: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/derived_input_parameter_context_mapper.py tests/ops/mapper/test_derived_input_parameter_context_mapper.py
git commit -m "feat: add derived input parameter context mapper"
```

## Task 2: Input Key Metadata Fetcher

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/derived_input_parameter_context_mapper.py`
- Modify: `tests/ops/mapper/test_derived_input_parameter_context_mapper.py`

**Step 1: Write failing metadata fetch test**

Add test:

```python
@patch("data_juicer.ops.mapper.ad_ai_data_center.derived_input_parameter_context_mapper.HttpClient")
def test_fetches_selected_input_key_metadata(self, mock_client_cls):
    metadata_client = FakeHttpClient(success_envelope({
        "inputParameterDetails": [
            {
                "keyId": 8,
                "keyNameEn": "unknown_id",
                "keyNameCn": "未知ID",
                "description": "可能表示广告、广告主或素材 ID",
                "demoValue": "1837647382987362",
                "multiValue": True,
            }
        ]
    }))
    mock_client_cls.return_value = metadata_client
    op = DerivedInputParameterContextMapper(
        input_key_ids=[8],
        ctx=self._ctx(),
    )

    result = op.process_single({RECORD_KEY_FIELD: "record-1"})

    self.assertEqual(
        result["unknown_id"],
        "未知ID：可能表示广告、广告主或素材 ID。示例：1837647382987362。支持多值。",
    )
```

Also assert:

- endpoint is `{ctx.apiBase}/openapi/state-meta/input-keys/batch-get`
- body is `{"keyIds": [8]}`
- headers include `space-id` when `ctx.spaceId` exists

**Step 2: Run RED**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_derived_input_parameter_context_mapper
```

Expected: no metadata fetch; `unknown_id` missing.

**Step 3: Implement metadata fetch**

Add `_get_parameter_columns`:

```python
def _get_parameter_columns(self, sample):
    if self._parameter_columns_cache is None:
        self._parameter_columns_cache = self._fetch_parameter_columns(sample)
    return self._parameter_columns_cache
```

Add `_fetch_parameter_columns` using `HttpClient`, `_build_openapi_url`, `_build_headers`, and `add_record_log_id_header`.

Response parser:

```python
data = self._unwrap_openapi_response(result["data"])
if not isinstance(data, dict) or not isinstance(
    data.get("inputParameterDetails"), list
):
    raise ValueError(
        "input key metadata response data must contain inputParameterDetails list"
    )
return self._build_parameter_columns(data["inputParameterDetails"])
```

**Step 4: Write sample columns in `process_single`**

```python
def process_single(self, sample):
    sample.update(self._get_parameter_columns(sample))
    return sample
```

Wrap this with the same record success/failure pattern used by other ADC business mappers.

**Step 5: Run GREEN**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_derived_input_parameter_context_mapper
```

Expected: all tests pass.

**Step 6: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/derived_input_parameter_context_mapper.py tests/ops/mapper/test_derived_input_parameter_context_mapper.py
git commit -m "feat: fetch derived input key metadata"
```

## Task 3: Parameter Description Formatting and Collision Handling

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/derived_input_parameter_context_mapper.py`
- Modify: `tests/ops/mapper/test_derived_input_parameter_context_mapper.py`

**Step 1: Write failing formatter tests**

Add direct helper tests:

```python
def test_formats_parameter_description_with_optional_parts(self):
    self.assertEqual(
        DerivedInputParameterContextMapper._format_parameter_description(
            {
                "keyId": 8,
                "keyNameEn": "unknown_id",
                "keyNameCn": "未知ID",
                "description": "可能表示广告、广告主或素材 ID",
                "demoValue": "1837647382987362",
                "multiValue": True,
            }
        ),
        "未知ID：可能表示广告、广告主或素材 ID。示例：1837647382987362。支持多值。",
    )


def test_rejects_different_key_ids_with_same_key_name_en(self):
    with self.assertRaisesRegex(ValueError, "duplicate keyNameEn"):
        DerivedInputParameterContextMapper._build_parameter_columns([
            {
                "keyId": 8,
                "keyNameEn": "unknown_id",
                "keyNameCn": "未知ID",
                "description": "描述A",
            },
            {
                "keyId": 9,
                "keyNameEn": "unknown_id",
                "keyNameCn": "未知ID",
                "description": "描述B",
            },
        ])
```

**Step 2: Run RED**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_derived_input_parameter_context_mapper
```

Expected: formatter helpers missing.

**Step 3: Implement formatting helpers**

Add:

```python
@classmethod
def _build_parameter_columns(cls, details):
    by_key_id = {}
    key_name_to_id = {}
    columns = {}
    for detail in details:
        if not isinstance(detail, dict):
            continue
        key_id = detail.get("keyId")
        key_name = detail.get("keyNameEn")
        if key_id is None:
            raise ValueError("inputParameterDetails item must contain keyId")
        if not key_name:
            raise ValueError("inputParameterDetails item must contain keyNameEn")
        key_id = int(key_id)
        key_name = str(key_name)
        if key_id in by_key_id:
            if by_key_id[key_id] != detail:
                logger.warning(
                    "Duplicate input key metadata differs; keeping first: keyId=%s",
                    key_id,
                )
            continue
        if key_name in key_name_to_id and key_name_to_id[key_name] != key_id:
            raise ValueError(f"duplicate keyNameEn in input key metadata: {key_name}")
        by_key_id[key_id] = detail
        key_name_to_id[key_name] = key_id
        columns[key_name] = cls._format_parameter_description(detail)
    return columns
```

**Step 4: Run GREEN**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_derived_input_parameter_context_mapper
```

**Step 5: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/derived_input_parameter_context_mapper.py tests/ops/mapper/test_derived_input_parameter_context_mapper.py
git commit -m "feat: format derived input parameter context columns"
```

## Task 4: Cache and Callback Tests

**Files:**
- Modify: `tests/ops/mapper/test_derived_input_parameter_context_mapper.py`
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/derived_input_parameter_context_mapper.py`

**Step 1: Write cache test**

Add a dataset with two rows and assert:

- `input-keys/batch-get` client called once.
- both rows receive identical `unknown_id` value.

Use `auto_op_parallelism=False`.

**Step 2: Write record callback tests**

Cover:

- Success callback receives updated sample.
- Failure callback is sent when metadata fetch fails.
- Callback failure is logged as warning and does not mask the business result.
- Missing `__adc_record_key` behavior matches existing ADC business mappers.

**Step 3: Run RED if behavior not already implemented**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_derived_input_parameter_context_mapper
```

Expected: cache or callback assertions fail if previous tasks missed them.

**Step 4: Implement missing minimal code**

If needed, add mapper-local cache and callback integration.

**Step 5: Run GREEN**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_derived_input_parameter_context_mapper
```

**Step 6: Commit**

```bash
git add tests/ops/mapper/test_derived_input_parameter_context_mapper.py data_juicer/ops/mapper/ad_ai_data_center/derived_input_parameter_context_mapper.py
git commit -m "test: cover derived input parameter context callbacks"
```

## Task 5: Documentation

**Files:**
- Create or update: `docs/tutorial/DerivedInputParameterContextMapper_ZH.md`

**Step 1: Add usage documentation**

Document:

- Operator purpose and recommended placement before LLM prompt construction.
- Parameter `input_key_ids`.
- Metadata API contract for `/openapi/state-meta/input-keys/batch-get`.
- Output sample showing generated parameter description columns.
- Error handling and caching notes.
- Frontend behavior: users select desired input parameters, and the frontend writes selected `keyId` values into `input_key_ids`.

**Step 2: Run markdown smoke checks**

```bash
git diff --check
```

Expected: no whitespace errors.

**Step 3: Commit**

```bash
git add docs/tutorial/DerivedInputParameterContextMapper_ZH.md
git commit -m "docs: document derived input parameter context mapper"
```

## Final Verification

Run focused tests:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_derived_input_parameter_context_mapper
```

Run adjacent ADC mapper regression:

```bash
./.venv/bin/python -m unittest \
  tests.ops.mapper.test_derived_input_parameter_context_mapper \
  tests.ops.mapper.test_state_template_mapper \
  tests.ops.mapper.test_state_metric_calculator_mapper \
  tests.ops.mapper.test_llm_inference_mapper
```

Run diff check:

```bash
git diff --check
```
