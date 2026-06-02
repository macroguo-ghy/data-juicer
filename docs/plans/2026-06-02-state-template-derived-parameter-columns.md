# State Template Selected Parameter Columns Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extend `state_template_mapper` so it can optionally add selected derived-field input parameter description columns into each sample for downstream LLM prompts.

**Architecture:** Keep the existing state template generation path unchanged: `/openapi/state-meta/generate-json` still produces the `state_template` string and remains cached per mapper instance. Add an optional second metadata fetch controlled by selected input parameter key IDs; when configured, the mapper calls `/openapi/state-meta/input-keys/batch-get`, formats each returned `inputParameterDetails` item into prompt-ready text, and writes those texts directly into `sample` using `keyNameEn` as the column name.

**Critical Assumptions & Early Checks:** Backend will add `POST /openapi/state-meta/input-keys/batch-get` and return `inputParameterDetails` with at least `keyId`, `keyNameEn`, `keyNameCn`, `keyType`, `description`, `demoValue`, `multiValue`, `dataType`, and `defaultOrPlaceholderValue`. Confirm the request and response envelope before implementation. Existing `state_template_mapper` behavior must remain unchanged when `emit_derived_parameter_columns` is false or omitted.

**Tech Stack:** Python 3, Data-Juicer `Mapper`, ADC `HttpClient`, ADC OpenAPI, unittest, Ray/HF Dataset mapper behavior.

---

## Requirement Summary

The State template node currently writes one field:

```python
sample[output_field] = state_template
```

The new optional capability writes additional top-level sample fields selected by input parameter `keyId`:

```python
sample["unknown_id"] = "未知ID：可能表示广告、广告主或素材 ID，需要结合上游数据判断。示例：1837647382987362。支持多值。"
sample["startDate"] = "开始日期：查询开始日期。示例：2026-05-01。"
sample["endDate"] = "结束日期：查询结束日期。示例：2026-05-14。"
```

These fields are not metric results and are not state template fields. They are prompt context fields that explain the input parameters required by selected derived fields.

## Public Configuration

Add constructor/YAML parameters:

```yaml
process:
  - state_template_mapper:
      state_meta_group_items:
        ad_state:
          - 101
          - 102
      output_field: state_template
      emit_derived_parameter_columns: true
      derived_parameter_key_ids:
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
| `emit_derived_parameter_columns` | `bool` | `False` | Whether to write selected input-parameter description columns into `sample`. |
| `derived_parameter_key_ids` | `list[int]` or `None` | `None` | Input parameter key IDs selected by the user. Each key ID maps to one backend input key metadata record. |

Rules:

- If `emit_derived_parameter_columns=false`, ignore `derived_parameter_key_ids` and preserve old behavior.
- If `emit_derived_parameter_columns=true`, require `derived_parameter_key_ids` to be a non-empty list of IDs.
- `state_meta_group_items` remains dedicated to state template generation.
- `derived_parameter_key_ids` is the only selector for generated parameter columns. Do not infer selected parameters from derived operator IDs.
- Frontend should pass the selected input key IDs from the user's checkbox selection.

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

Duplicate and collision rules:

- Deduplicate by `keyId` before formatting.
- If duplicated `keyId` records have identical content, keep one.
- If duplicated `keyId` records differ, keep the first record and log a warning.
- If two different `keyId` records resolve to the same `keyNameEn`, raise `ValueError`, because they would write the same sample column with ambiguous meaning.

## Caching

Cache derived parameter columns per mapper instance, just like `_state_template_cache`.

Add:

```python
self._derived_parameter_columns_cache = None
```

Reasoning:

- Metadata depends on static key ID configuration, not per-record sample values.
- In Ray execution, each worker may fetch once, which is acceptable and consistent with the existing state template cache behavior.
- Record-level `x-tt-logid` can still be added to the first sample that triggers the fetch, same as current `generate-json` behavior.

## Error Handling

Fail fast when:

- `emit_derived_parameter_columns=true` and `derived_parameter_key_ids` is empty or invalid.
- metadata HTTP request fails.
- backend business response has non-zero `code`.
- response data is not a dictionary containing an `inputParameterDetails` list.
- any selected metadata item misses `keyNameEn`.
- two different selected key IDs map to the same `keyNameEn`.

Per-record callback behavior is unchanged:

- If metadata fetch fails, `process_single` should report record failure and re-raise.
- If callback fails, log warning and do not mask the main business result/failure.

## Task 1: Constructor and Config Contract

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/state_template_mapper.py`
- Modify: `tests/ops/mapper/test_state_template_mapper.py`

**Step 1: Write failing constructor tests**

Add tests:

```python
def test_accepts_derived_parameter_column_config(self):
    op = StateTemplateMapper(
        state_meta_group_items=self._state_meta_group_items(),
        derived_parameter_key_ids=[8, 12, 13],
        emit_derived_parameter_columns=True,
        ctx=self._ctx(),
    )
    self.assertEqual(op.derived_parameter_key_ids, [8, 12, 13])
    self.assertEqual(op.emit_derived_parameter_columns, True)


def test_rejects_emit_parameter_columns_without_key_ids(self):
    with self.assertRaisesRegex(ValueError, "derived_parameter_key_ids"):
        StateTemplateMapper(
            state_meta_group_items=self._state_meta_group_items(),
            emit_derived_parameter_columns=True,
            ctx=self._ctx(),
        )
```

Also update `test_config_loads_operator_name_without_ad_ai_data_center_prefix` YAML with:

```yaml
derived_parameter_key_ids:
  - 8
emit_derived_parameter_columns: true
```

Assert loaded values.

**Step 2: Run RED**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper
```

Expected: constructor does not accept the new arguments.

**Step 3: Implement minimal constructor changes**

Add parameters:

```python
derived_parameter_key_ids: list[int] | None = None,
emit_derived_parameter_columns: bool = False,
```

Validation:

```python
self.emit_derived_parameter_columns = bool(emit_derived_parameter_columns)
if derived_parameter_key_ids is None:
    normalized_key_ids = []
elif isinstance(derived_parameter_key_ids, list):
    normalized_key_ids = [int(item) for item in derived_parameter_key_ids]
else:
    raise ValueError("derived_parameter_key_ids must be a list")
if self.emit_derived_parameter_columns and not normalized_key_ids:
    raise ValueError(
        "derived_parameter_key_ids must be provided when "
        "emit_derived_parameter_columns is true"
    )
self.derived_parameter_key_ids = normalized_key_ids
self._derived_parameter_columns_cache = None
```

Update `_operator_config()` callback payload to include:

```python
"derived_parameter_key_ids": self.derived_parameter_key_ids,
"emit_derived_parameter_columns": self.emit_derived_parameter_columns,
```

**Step 4: Run GREEN**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper
```

Expected: tests pass.

**Step 5: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_template_mapper.py tests/ops/mapper/test_state_template_mapper.py
git commit -m "feat: add state template parameter column config"
```

## Task 2: Input Key Metadata Fetcher

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/state_template_mapper.py`
- Modify: `tests/ops/mapper/test_state_template_mapper.py`

**Step 1: Write failing metadata fetch tests**

Add constant:

```python
INPUT_KEYS_BATCH_GET_PATH = "/openapi/state-meta/input-keys/batch-get"
```

Add test:

```python
@patch("data_juicer.ops.mapper.ad_ai_data_center.state_template_mapper.HttpClient")
def test_fetches_selected_input_key_metadata_when_enabled(self, mock_client_cls):
    template_client = FakeHttpClient(success_envelope(self._state_template()))
    metadata_client = FakeHttpClient(success_envelope({
        "inputParameterDetails": [
            {
                "keyId": 8,
                "keyNameEn": "unknown_id",
                "keyNameCn": "未知ID",
                "keyType": "AMBIGUOUS",
                "description": "可能表示广告、广告主或素材 ID",
                "demoValue": "1837647382987362",
                "multiValue": True,
            }
        ]
    }))
    mock_client_cls.side_effect = [template_client, metadata_client]
    op = StateTemplateMapper(
        state_meta_group_items=self._state_meta_group_items(),
        derived_parameter_key_ids=[8],
        emit_derived_parameter_columns=True,
        ctx=self._ctx(),
    )

    result = op.process_single({RECORD_KEY_FIELD: "record-1"})

    self.assertIn("state_template", result)
    self.assertEqual(
        result["unknown_id"],
        "未知ID：可能表示广告、广告主或素材 ID。示例：1837647382987362。支持多值。",
    )
```

Also assert the second `HttpClient` endpoint is:

```text
{ctx.apiBase}/openapi/state-meta/input-keys/batch-get
```

and body is:

```python
{"keyIds": [8]}
```

**Step 2: Run RED**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper
```

Expected: no metadata fetch; `unknown_id` missing.

**Step 3: Implement metadata fetch**

Add:

```python
INPUT_KEYS_BATCH_GET_PATH = "/openapi/state-meta/input-keys/batch-get"
```

Add method:

```python
def _get_derived_parameter_columns(self, ctx, sample):
    if not self.emit_derived_parameter_columns:
        return {}
    if self._derived_parameter_columns_cache is None:
        self._derived_parameter_columns_cache = (
            self._fetch_derived_parameter_columns(ctx, sample)
        )
    return self._derived_parameter_columns_cache
```

Add `_fetch_derived_parameter_columns` using `HttpClient`, `_build_openapi_url`, `_build_headers`, and `add_record_log_id_header`, mirroring `_generate_state_template`.

Response parser:

```python
data = self._unwrap_openapi_response(result["data"])
if not isinstance(data, dict) or not isinstance(
    data.get("inputParameterDetails"), list
):
    raise ValueError(
        "input key metadata response data must contain inputParameterDetails list"
    )
return self._build_derived_parameter_columns(data["inputParameterDetails"])
```

**Step 4: Write sample columns in process_single**

After state template generation:

```python
sample[self.output_field] = state_template
sample.update(self._get_derived_parameter_columns(ctx, sample))
```

Keep `state_template` generation first, so existing failures and output behavior remain unchanged.

**Step 5: Run GREEN**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper
```

Expected: all tests pass.

**Step 6: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_template_mapper.py tests/ops/mapper/test_state_template_mapper.py
git commit -m "feat: fetch state template input key metadata"
```

## Task 3: Parameter Description Formatting and Collision Handling

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/state_template_mapper.py`
- Modify: `tests/ops/mapper/test_state_template_mapper.py`

**Step 1: Write failing formatter tests**

Add direct unit tests for static/class helpers:

```python
def test_formats_parameter_description_with_optional_parts(self):
    self.assertEqual(
        StateTemplateMapper._format_parameter_description(
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
        StateTemplateMapper._build_derived_parameter_columns([
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
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper
```

Expected: formatter helpers missing.

**Step 3: Implement formatting helpers**

Add:

```python
@classmethod
def _build_derived_parameter_columns(cls, details):
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

Formatter detail:

- Single detail returns a plain prompt string.
- Missing optional fields are omitted cleanly.
- The returned string should not include `keyId`, `keyType`, `dataType`, or `defaultOrPlaceholderValue`; those are backend/UI metadata, not prompt text.

**Step 4: Run GREEN**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper
```

**Step 5: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_template_mapper.py tests/ops/mapper/test_state_template_mapper.py
git commit -m "feat: format selected state template parameter columns"
```

## Task 4: Cache and Backward Compatibility Tests

**Files:**
- Modify: `tests/ops/mapper/test_state_template_mapper.py`

**Step 1: Write cache test**

Add a dataset with two rows and assert:

- `generate-json` client called once.
- `input-keys/batch-get` client called once.
- both rows receive identical `unknown_id` value.

Use `mock_client_cls.side_effect = [template_client, metadata_client]` and `auto_op_parallelism=False`.

**Step 2: Write backward compatibility tests**

Add a test where `derived_parameter_key_ids=[8]` but `emit_derived_parameter_columns=False`.

Expected:

- only `/generate-json` client is constructed.
- no `unknown_id` column is written.
- existing `state_template` behavior remains unchanged.

Add a test where both new parameters are omitted.

Expected:

- old constructor/config path works unchanged.
- no second metadata request is made.

**Step 3: Run RED if behavior not already implemented**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper
```

Expected: cache or backward compatibility assertions fail if previous tasks missed them.

**Step 4: Implement missing minimal code**

If needed, adjust `_get_derived_parameter_columns` to return `{}` when disabled and to cache enabled results.

**Step 5: Run GREEN**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper
```

**Step 6: Commit**

```bash
git add tests/ops/mapper/test_state_template_mapper.py data_juicer/ops/mapper/ad_ai_data_center/state_template_mapper.py
git commit -m "test: cover state template input key metadata cache"
```

## Task 5: Documentation

**Files:**
- Create or update: `docs/tutorial/StateTemplateMapper_ZH.md`

**Step 1: Add usage documentation**

Document:

- New parameters `emit_derived_parameter_columns` and `derived_parameter_key_ids`.
- Metadata API contract for `/openapi/state-meta/input-keys/batch-get`.
- Output sample showing generated parameter description columns.
- Error handling and caching notes.
- Frontend behavior: users select desired input parameters, and the frontend writes selected `keyId` values into `derived_parameter_key_ids`.

**Step 2: Run markdown smoke checks**

```bash
git diff --check
```

Expected: no whitespace errors.

**Step 3: Commit**

```bash
git add docs/tutorial/StateTemplateMapper_ZH.md
git commit -m "docs: document state template selected parameter columns"
```

## Final Verification

Run focused tests:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper
```

Run adjacent ADC mapper regression:

```bash
./.venv/bin/python -m unittest \
  tests.ops.mapper.test_state_template_mapper \
  tests.ops.mapper.test_state_metric_calculator_mapper \
  tests.ops.mapper.test_llm_inference_mapper
```
