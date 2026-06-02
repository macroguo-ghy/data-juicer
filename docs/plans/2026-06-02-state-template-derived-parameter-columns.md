# State Template Derived Parameter Columns Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extend `state_template_mapper` so it can optionally add sample columns that describe derived operator input parameters for downstream LLM prompts.

**Architecture:** Keep the existing state template generation path unchanged: `/openapi/state-meta/generate-json` still produces the `state_template` string and remains cached per mapper instance. Add an optional second metadata fetch controlled by `derived_operator_ids`; when configured, the mapper calls `/openapi/state-meta/operators/batch-get`, extracts `inputParameterDetails`, formats each parameter into prompt-ready text, and writes those texts directly into `sample` using `keyNameEn` as the column name.

**Critical Assumptions & Early Checks:** Backend must expose derived operator metadata through `/openapi/state-meta/operators/batch-get` and return `inputParameterDetails` with `keyNameEn`, `keyNameCn`, `keyType`, `description`, `demoValue`, and `multiValue`. Confirm the expected request/response shape before implementation; if the interface differs, only the metadata fetch/parser task should change. Existing `state_template_mapper` behavior must remain byte-compatible when `derived_operator_ids` is omitted.

**Tech Stack:** Python 3, Data-Juicer `Mapper`, ADC `HttpClient`, ADC OpenAPI, unittest, Ray/HF Dataset mapper behavior.

---

## Requirement Summary

The State template node currently writes one field:

```python
sample[output_field] = state_template
```

The new optional capability writes additional top-level sample fields:

```python
sample["unknown_id"] = "未知ID：可能表示广告、广告主或素材 ID，需要结合上游数据判断。示例：1837647382987362。支持多值。"
sample["startDate"] = "开始日期：查询开始日期。示例：2026-05-01。"
sample["endDate"] = "结束日期：查询结束日期。示例：2026-05-14。"
```

These fields are not metric results. They are prompt context fields that explain the input parameters required by selected derived fields.

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
      derived_operator_ids:
        - 201
        - 202
      emit_derived_parameter_columns: true
      ctx:
        apiBase: "https://ai-data-center.bytedance.net/api"
        userAccount: "zhangsan"
        spaceId: 1
```

Parameter semantics:

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `derived_operator_ids` | `list[int] \| None` | `None` | Derived metric/tool operator IDs whose input parameter metadata should be fetched. |
| `emit_derived_parameter_columns` | `bool` | `False` | Whether to write input-parameter description columns into `sample`. |

Rules:

- If `emit_derived_parameter_columns=false`, ignore `derived_operator_ids` and preserve old behavior.
- If `emit_derived_parameter_columns=true`, require `derived_operator_ids` to be a non-empty list of IDs.
- `state_meta_group_items` remains dedicated to state template generation.
- `derived_operator_ids` is separate so the mapper does not need to infer which IDs are state attributes and which IDs are derived operators.

## Metadata API Contract

Use the same operator metadata source as the metric calculator:

```http
POST {ctx.apiBase}/openapi/state-meta/operators/batch-get
Content-Type: application/json
space-id: {ctx.spaceId}
user-account: {ctx.userAccount}
```

Expected request body:

```json
{
  "operatorIds": [201, 202]
}
```

Expected response envelope:

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "operators": [
      {
        "id": 201,
        "operatorNameEn": "EcpCost",
        "operatorNameCn": "千川消耗环比",
        "operatorType": "metric",
        "inputParameterDetails": [
          {
            "keyNameEn": "unknown_id",
            "keyNameCn": "未知ID",
            "keyType": "AMBIGUOUS",
            "description": "可能表示广告、广告主或素材 ID，需要结合上游数据判断",
            "demoValue": "1837647382987362",
            "multiValue": true,
            "dataType": "placeholder",
            "defaultOrPlaceholderValue": "unknown_id"
          }
        ]
      }
    ]
  }
}
```

If the backend currently expects a different request key, such as `ids`, update only the request builder and tests for that API shape.

## Output Formatting

Each `inputParameterDetails[]` item becomes a candidate column:

```python
column_name = detail["keyNameEn"]
column_value = format_parameter_description(detail, operator_detail)
sample[column_name] = column_value
```

Recommended format:

```text
{keyNameCn}：{description}。示例：{demoValue}。支持多值。
```

Formatting rules:

- `keyNameEn` is required; skip items without it.
- `keyNameCn` is optional; if missing, use `keyNameEn`.
- Append `description` only when non-empty.
- Append `demoValue` only when non-empty.
- Append `支持多值。` only when `multiValue` is true.
- Output must be a plain string, not JSON, because the target consumer is LLM prompt text.

Duplicate parameter rules:

- Merge by `keyNameEn`.
- If duplicate formatted descriptions are identical, keep one.
- If duplicate descriptions differ, join them with newline bullets and prefix the operator display name:

```text
未知ID：
- 千川消耗环比：可能表示广告、广告主或素材 ID，需要结合上游数据判断。示例：1837647382987362。支持多值。
- ROI同行数据：广告或广告主 ID。示例：1834567890123456。
```

The first implementation can preserve input order by iterating operator metadata in the order returned by the backend and parameter details in their list order.

## Caching

Cache derived parameter columns per mapper instance, just like `_state_template_cache`.

Add:

```python
self._derived_parameter_columns_cache = None
```

Reasoning:

- Metadata depends on operator configuration, not per-record sample values.
- In Ray execution, each worker may fetch once, which is acceptable and consistent with the existing state template cache behavior.
- Record-level `x-tt-logid` can still be added to the first sample that triggers the fetch, same as current `generate-json` behavior.

## Error Handling

Fail fast when:

- `emit_derived_parameter_columns=true` and `derived_operator_ids` is empty or invalid.
- metadata HTTP request fails.
- backend business response has non-zero `code`.
- response data is not a dictionary containing an `operators` list.

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
def test_accepts_derived_operator_column_config(self):
    op = StateTemplateMapper(
        state_meta_group_items=self._state_meta_group_items(),
        derived_operator_ids=[301, 302],
        emit_derived_parameter_columns=True,
        ctx=self._ctx(),
    )
    self.assertEqual(op.derived_operator_ids, [301, 302])
    self.assertEqual(op.emit_derived_parameter_columns, True)


def test_rejects_emit_parameter_columns_without_operator_ids(self):
    with self.assertRaisesRegex(ValueError, "derived_operator_ids"):
        StateTemplateMapper(
            state_meta_group_items=self._state_meta_group_items(),
            emit_derived_parameter_columns=True,
            ctx=self._ctx(),
        )
```

Also update `test_config_loads_operator_name_without_ad_ai_data_center_prefix` YAML with:

```yaml
derived_operator_ids:
  - 301
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
derived_operator_ids: list[int] | None = None,
emit_derived_parameter_columns: bool = False,
```

Validation:

```python
self.emit_derived_parameter_columns = bool(emit_derived_parameter_columns)
if derived_operator_ids is None:
    normalized_operator_ids = []
elif isinstance(derived_operator_ids, list):
    normalized_operator_ids = [int(item) for item in derived_operator_ids]
else:
    raise ValueError("derived_operator_ids must be a list")
if self.emit_derived_parameter_columns and not normalized_operator_ids:
    raise ValueError("derived_operator_ids must be provided when emit_derived_parameter_columns is true")
self.derived_operator_ids = normalized_operator_ids
self._derived_parameter_columns_cache = None
```

Update `_operator_config()` callback payload to include:

```python
"derived_operator_ids": self.derived_operator_ids,
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
git commit -m "feat: add state template derived parameter config"
```

## Task 2: Metadata Fetcher

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/state_template_mapper.py`
- Modify: `tests/ops/mapper/test_state_template_mapper.py`

**Step 1: Write failing metadata fetch tests**

Add constants:

```python
OPERATORS_BATCH_GET_PATH = "/openapi/state-meta/operators/batch-get"
```

Add test:

```python
@patch("data_juicer.ops.mapper.ad_ai_data_center.state_template_mapper.HttpClient")
def test_fetches_derived_operator_metadata_when_enabled(self, mock_client_cls):
    template_client = FakeHttpClient(success_envelope(self._state_template()))
    metadata_client = FakeHttpClient(success_envelope({
        "operators": [
            {
                "id": 301,
                "operatorNameCn": "千川消耗环比",
                "inputParameterDetails": [
                    {
                        "keyNameEn": "unknown_id",
                        "keyNameCn": "未知ID",
                        "keyType": "AMBIGUOUS",
                        "description": "可能表示广告、广告主或素材 ID",
                        "demoValue": "1837647382987362",
                        "multiValue": True,
                    }
                ],
            }
        ]
    }))
    mock_client_cls.side_effect = [template_client, metadata_client]
    op = StateTemplateMapper(
        state_meta_group_items=self._state_meta_group_items(),
        derived_operator_ids=[301],
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
{ctx.apiBase}/openapi/state-meta/operators/batch-get
```

and body is:

```python
{"operatorIds": [301]}
```

**Step 2: Run RED**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper
```

Expected: no metadata fetch; `unknown_id` missing.

**Step 3: Implement metadata fetch**

Add:

```python
OPERATORS_BATCH_GET_PATH = "/openapi/state-meta/operators/batch-get"
```

Add method:

```python
def _get_derived_parameter_columns(self, ctx, sample):
    if not self.emit_derived_parameter_columns:
        return {}
    if self._derived_parameter_columns_cache is None:
        self._derived_parameter_columns_cache = self._fetch_derived_parameter_columns(ctx, sample)
    return self._derived_parameter_columns_cache
```

Add `_fetch_derived_parameter_columns` using `HttpClient`, `_build_openapi_url`, `_build_headers`, and `add_record_log_id_header`, mirroring `_generate_state_template`.

Response parser:

```python
data = self._unwrap_openapi_response(result["data"])
if not isinstance(data, dict) or not isinstance(data.get("operators"), list):
    raise ValueError("derived operator metadata response data must contain operators list")
return self._build_derived_parameter_columns(data["operators"])
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
git commit -m "feat: fetch state template derived parameter metadata"
```

## Task 3: Parameter Description Formatting and Duplicate Merge

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
                "keyNameEn": "unknown_id",
                "keyNameCn": "未知ID",
                "description": "可能表示广告、广告主或素材 ID",
                "demoValue": "1837647382987362",
                "multiValue": True,
            },
            {"operatorNameCn": "千川消耗环比"},
        ),
        "未知ID：可能表示广告、广告主或素材 ID。示例：1837647382987362。支持多值。",
    )


def test_merges_duplicate_parameter_descriptions_with_operator_names(self):
    columns = StateTemplateMapper._build_derived_parameter_columns([
        {
            "operatorNameCn": "指标A",
            "inputParameterDetails": [
                {"keyNameEn": "unknown_id", "keyNameCn": "未知ID", "description": "描述A"}
            ],
        },
        {
            "operatorNameCn": "指标B",
            "inputParameterDetails": [
                {"keyNameEn": "unknown_id", "keyNameCn": "未知ID", "description": "描述B"}
            ],
        },
    ])
    self.assertEqual(columns["unknown_id"], "未知ID：\n- 指标A：描述A。\n- 指标B：描述B。")
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
def _build_derived_parameter_columns(cls, operators):
    grouped = {}
    labels = {}
    for operator in operators:
        if not isinstance(operator, dict):
            continue
        operator_name = cls._operator_display_name(operator)
        details = operator.get("inputParameterDetails")
        if not isinstance(details, list):
            continue
        for detail in details:
            if not isinstance(detail, dict) or not detail.get("keyNameEn"):
                continue
            key = str(detail["keyNameEn"])
            labels.setdefault(key, str(detail.get("keyNameCn") or key))
            text = cls._format_parameter_description(detail, operator)
            grouped.setdefault(key, [])
            if text not in grouped[key]:
                grouped[key].append((operator_name, text))
    return {
        key: cls._merge_parameter_descriptions(labels[key], values)
        for key, values in grouped.items()
    }
```

Formatter detail:

- Single unique value returns just the formatted description.
- Multiple different values returns:

```text
{label}：
- {operatorName}：{description}
- {operatorName}：{description}
```

**Step 4: Run GREEN**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper
```

**Step 5: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_template_mapper.py tests/ops/mapper/test_state_template_mapper.py
git commit -m "feat: format derived parameter prompt columns"
```

## Task 4: Cache and Backward Compatibility Tests

**Files:**
- Modify: `tests/ops/mapper/test_state_template_mapper.py`

**Step 1: Write cache test**

Add a dataset with two rows and assert:

- `generate-json` client called once.
- `operators/batch-get` client called once.
- both rows receive identical `unknown_id` value.

Use `mock_client_cls.side_effect = [template_client, metadata_client]` and `auto_op_parallelism=False`.

**Step 2: Write backward compatibility test**

Add a test where `derived_operator_ids=[301]` but `emit_derived_parameter_columns=False`.

Expected:

- only `/generate-json` client is constructed.
- no `unknown_id` column is written.
- existing `state_template` behavior remains unchanged.

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
git commit -m "test: cover state template derived parameter cache"
```

## Task 5: Documentation

**Files:**
- Modify: `docs/plans/2026-05-17-state-template-mapper.md`
- Optionally create: `docs/tutorial/StateTemplateMapper_ZH.md` if a tutorial page is desired.

**Step 1: Update existing plan/usage doc**

Add:

- New parameters `derived_operator_ids` and `emit_derived_parameter_columns`.
- Metadata API contract for `/openapi/state-meta/operators/batch-get`.
- Output sample showing generated parameter columns.
- Error handling and caching notes.

**Step 2: Run markdown smoke checks**

```bash
git diff --check
```

Expected: no whitespace errors.

**Step 3: Commit**

```bash
git add docs/plans/2026-05-17-state-template-mapper.md
git commit -m "docs: update state template derived parameter columns"
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

Run compile check:

```bash
python3 -m py_compile \
  data_juicer/ops/mapper/ad_ai_data_center/state_template_mapper.py \
  tests/ops/mapper/test_state_template_mapper.py
```

Run diff check:

```bash
git diff --check
```

Review checklist:

- Existing configs without `derived_operator_ids` produce exactly the same `state_template` output.
- `emit_derived_parameter_columns=false` never calls the metadata endpoint.
- `emit_derived_parameter_columns=true` fails clearly when `derived_operator_ids` is missing.
- Duplicate parameter keys are merged into one column.
- Output parameter columns are strings, not nested objects.
- Callback payload includes the new config fields.
- No Ray Dataset `columns()` call is introduced.

## Rollout Notes

Frontend/backend changes needed:

- Frontend should pass `derived_operator_ids` separately from `state_meta_group_items`.
- Frontend should set `emit_derived_parameter_columns=true` only when it wants prompt parameter columns.
- Backend metadata API must include `inputParameterDetails` for each derived operator.

Backward compatibility:

- Default behavior is unchanged because `emit_derived_parameter_columns=false`.
- Existing `state_template_mapper` YAML continues to work.
- New fields are additive and written directly into sample only when enabled.
