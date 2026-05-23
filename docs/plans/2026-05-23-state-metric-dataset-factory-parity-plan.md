# State Metric Dataset Factory Parity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extend `StateMetricCalculatorMapper` so it can support the same metric-calculation flow shape as Dataset Factory while preserving the current ADC operator-metadata API model.

**Architecture:** Keep `StateMetricCalculatorMapper` as the Data-Juicer operator entrypoint and keep fetching metric metadata from `/openapi/state-meta/operators/batch-get`. Add a local shared metric runtime layer under `data_juicer/ops/mapper/ad_ai_data_center/` that provides id extraction, `id_key` detection, common math helpers, code execution helpers, and output aggregation. Existing per-operator `calculate(...)` scripts remain supported; new scripts can opt into shared helpers and richer context without requiring Dataset Factory's registry/import model.

**Critical Assumptions & Early Checks:** The backend operator detail payload can carry enough metadata to identify `metricCode`, `metricName`, `operatorCode`, and `inputParameter.params`; verify this before adding a registry-like path. The backend will continue to be the source of metric definitions, so do not hard-code metric definitions in Data-Juicer except shared math/helper utilities. Ray output schema must remain stable: metric `output` and `error` stay strings, and all new nested fields must have deterministic types across success and failure rows.

**Tech Stack:** Python 3, Data-Juicer `Mapper`, Ray Dataset, PyArrow schema stability, trusted dynamic Python via `PythonScriptRunner`, ADC OpenAPI `HttpClient`.

---

## Current Gap Summary

Dataset Factory stage 4 has these capabilities:

- Sample-level preprocessing: parse `state`, read issue/id field, optional date range fields.
- ID extraction: `extract_numeric_ids(issue_id)` extracts all numeric fragments, de-duplicates by first appearance, then falls back to the stripped original id.
- Entity detection: shared `detect_id_keys(state_data, id)` resolves whether the current id is `ad_id` or `adv_id`.
- Metric routing: `metricCode` routes through a local registry to a handler.
- Shared helper library: common date, math, ratio, sequential, percent, and formatting helpers live in `utils/math.py`.
- Per-call execution context: concrete handlers receive `state_data`, `tool_input`, `id_key`, `id_value`, `start_date`, and `end_date`.
- Result aggregation: per-id metrics/tools are summarized into a JSON string-like map `{id: {metrics: [...], tools: [...]}}`.

Current `StateMetricCalculatorMapper` already has:

- Operator metadata fetched from ADC API, not local registry.
- Per-operator `operatorCode` compiled as `calculate`.
- Dynamic argument injection from function signature and `inputParameter.params`.
- Multi-id splitting for comma/list ids and per-id metric execution.
- Stable output shape:

```json
{
  "id": "id1,id2",
  "items": [
    {
      "id": "id1",
      "metrics": [
        {
          "metricCode": "bench_roi_score",
          "metricName": "行业基准 ROI 得分",
          "output": "0.82",
          "error": ""
        }
      ]
    }
  ]
}
```

Main gaps:

- No Dataset Factory-compatible numeric id extraction from mixed strings.
- No shared `id_key` detection layer.
- No common helper module injected into metric scripts.
- No standard rich context object passed to metric scripts.
- No optional Dataset Factory-style summary output.
- No support for auxiliary tools; this should remain out of scope unless explicitly requested.

## Target Behavior

The mapper should support two compatible script styles:

### Existing Style

```python
def calculate(state, ids, bench_roi):
    return 0.82
```

This must keep working unchanged.

### New Context-Aware Style

```python
def calculate(state, ids, id_key, start_date=None, end_date=None, helpers=None):
    if id_key == "ad_id":
        ...
    return helpers.fmt4(1.23456)
```

The new style should be enabled by parameter names and default values, not by changing the required function name.

## Out Of Scope

- Do not copy Dataset Factory's metric registry into Data-Juicer.
- Do not implement Dataset Factory auxiliary tools such as `get_industry_creative_tips`.
- Do not change ADC API ownership of metric metadata.
- Do not change existing default output shape unless a config flag asks for a summary-compatible field.

---

### Task 1: Add Shared Metric Runtime Helpers

**Files:**
- Create: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py`
- Test: `tests/ops/mapper/test_state_metric_runtime.py`

**Step 1: Write failing tests**

Cover these contracts:

```python
def test_extract_metric_ids_deduplicates_numeric_fragments():
    assert extract_metric_ids("ad:123, adv:456, again:123") == ["123", "456"]


def test_extract_metric_ids_falls_back_to_stripped_original():
    assert extract_metric_ids("abc_def") == ["abc_def"]


def test_detect_id_key_prefers_ad_when_id_matches_both_ad_and_adv():
    state = {
        "ad_state": [{"ad_id": "123"}],
        "adv_state": [{"adv_id": "123"}],
    }
    assert detect_id_key(state, "123") == "ad_id"


def test_detect_id_key_supports_adv_meta_data_fallback():
    state = {"adv_state": [{"meta_data": {"adv_id": "456"}}]}
    assert detect_id_key(state, "456") == "adv_id"
```

**Step 2: Run tests to verify they fail**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_runtime
```

Expected: import/function failures.

**Step 3: Implement minimal helper module**

Implement:

```python
def extract_numeric_ids(value): ...
def extract_metric_ids(value): ...
def detect_id_keys(state_data, id_value): ...
def detect_id_key(state_data, id_value): ...
```

Rules:

- Match Dataset Factory `extract_numeric_ids` behavior.
- `extract_metric_ids` adds fallback `[str(value or "").strip()]` when numeric extraction returns empty.
- `detect_id_keys` checks `ad_state[].ad_id`, `adv_state[].adv_id`, and `adv_state[].meta_data.adv_id`.
- `detect_id_key` returns `"adv_id"` only when matched keys contain `adv_id` and not `ad_id`; otherwise prefer `"ad_id"` when present.
- Unknown id should return `None`; callers decide whether to fail or continue.

**Step 4: Run tests to verify they pass**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_runtime
```

Expected: all tests pass.

**Step 5: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py tests/ops/mapper/test_state_metric_runtime.py
git commit -m "add state metric runtime id helpers"
```

---

### Task 2: Add Shared Math Helper Surface

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py`
- Test: `tests/ops/mapper/test_state_metric_runtime.py`

**Step 1: Write failing tests**

Cover representative Dataset Factory math behavior:

```python
def test_helpers_safe_divide_and_parse_percent():
    helpers = MetricHelpers()
    assert helpers.safe_divide(1, 0) == 0.0
    assert helpers.parse_percent_to_ratio("75%") == 0.75


def test_helpers_calc_sequential_stats_integer():
    helpers = MetricHelpers()
    series = {
        "2024-01-01": 10,
        "2024-01-02": 20,
        "2024-01-03": 30,
        "2024-01-04": 60,
    }
    assert helpers.calc_sequential_stats_integer(
        series,
        date(2024, 1, 3),
        date(2024, 1, 4),
    ) == (45, 15, 2.0)
```

**Step 2: Run tests to verify they fail**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_runtime
```

Expected: missing `MetricHelpers`.

**Step 3: Implement minimal helper object**

Add `MetricHelpers` with stable methods copied or adapted from Dataset Factory:

- `extract_numeric_values_in_range`
- `sum_numeric_values_in_range`
- `safe_divide`
- `calc_ratio_from_series`
- `calc_sequential_stats`
- `calc_sequential_stats_integer`
- `calc_sequential_stats_for_fraction`
- `calc_bench_compare`
- `fmt4`
- `average`
- `calc_sequential_ratio`
- `parse_percent_to_ratio`
- `resolve_date_range_from_series`
- `parse_duration_seconds`

Keep behavior pure and dependency-free. Avoid importing Dataset Factory at runtime.

**Step 4: Run tests to verify they pass**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_runtime
```

Expected: all tests pass.

**Step 5: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py tests/ops/mapper/test_state_metric_runtime.py
git commit -m "add state metric math helpers"
```

---

### Task 3: Parse Date Fields And Add Calculator Context

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py`
- Test: `tests/ops/mapper/test_state_metric_calculator_mapper.py`

**Step 1: Write failing tests**

Add constructor config:

- `id_key: str | None = None` or reuse existing id candidate logic first.
- `start_date_key: str | None = None`
- `end_date_key: str | None = None`

Test that scripts can request new context parameters by name:

```python
def test_calculate_can_receive_id_key_and_dates(...):
    operatorCode = '''
def calculate(state, ids, id_key, start_date=None, end_date=None):
    return {"id": ids, "id_key": id_key, "start": str(start_date), "end": str(end_date)}
'''
```

Expected output for an id found in `ad_state`:

```json
{"id": "123", "id_key": "ad_id", "start": "2024-01-01", "end": "2024-01-07"}
```

**Step 2: Run test to verify it fails**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_calculate_can_receive_id_key_and_dates
```

Expected: `inputParameter.params missing parameter: id_key` or missing date parameter.

**Step 3: Implement minimal context resolution**

In `StateMetricCalculatorMapper`:

- Normalize `state` once per sample.
- For each current item id, call `detect_id_key(state_data, item_id)`.
- Parse optional date fields with `datetime.date.fromisoformat`.
- Extend `_resolve_calculate_args` reserved names:
  - `id_key`
  - `id_value`
  - `start_date`
  - `end_date`
  - `helpers`

Keep existing `state` and `inputParameter.params` behavior unchanged.

**Step 4: Run test to verify it passes**

Run the focused test.

**Step 5: Run existing state metric suite**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper
```

Expected: all tests pass.

**Step 6: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "add state metric calculation context"
```

---

### Task 4: Align Multi-ID Extraction With Dataset Factory

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py`
- Test: `tests/ops/mapper/test_state_metric_calculator_mapper.py`

**Step 1: Write failing tests**

Current code splits non-list id values by comma. Add a test for mixed strings:

```python
def test_output_items_extract_numeric_ids_from_mixed_issue_id(...):
    sample["issue_id"] = "ad:1854751525764108, adv:1853671159428096, again:1854751525764108"
    ...
    assert [item["id"] for item in output["items"]] == [
        "1854751525764108",
        "1853671159428096",
    ]
```

Also test fallback:

```python
def test_output_items_fallback_to_original_when_no_numeric_id(...):
    sample["issue_id"] = "abc_def"
    assert output["items"][0]["id"] == "abc_def"
```

**Step 2: Run tests to verify they fail**

Expected: current comma split keeps `ad:...` fragments or does not de-duplicate Dataset Factory-style.

**Step 3: Replace `_split_output_id_value` implementation**

Use `extract_metric_ids(value)` from `state_metric_runtime.py`.

List inputs should preserve current list semantics, but each list item should stringify and strip. String inputs should use Dataset Factory numeric extraction plus fallback.

**Step 4: Run tests to verify they pass**

Run focused tests.

**Step 5: Run Arrow schema stability test**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_metric_failure_output_keeps_arrow_schema_as_string
```

Expected: pass.

**Step 6: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "align state metric id extraction"
```

---

### Task 5: Add Optional Dataset Factory Summary Output Mode

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py`
- Test: `tests/ops/mapper/test_state_metric_calculator_mapper.py`

**Step 1: Write failing tests**

Add a config option:

```python
summary_output_key: str | None = None
```

Test that when configured, the mapper writes both the current object output and a Dataset Factory-style summary JSON string:

```python
op = StateMetricCalculatorMapper(
    output_key="query_metric_data_outputs",
    summary_output_key="metric_summary",
    ...
)
```

Expected:

```python
json.loads(result["metric_summary"]) == {
    "123": {
        "metrics": [
            {"metricCode": "...", "metricName": "...", "output": "..."}
        ]
    }
}
```

Failure metrics with non-empty `error` should be excluded from summary.

**Step 2: Run test to verify it fails**

Expected: constructor does not accept `summary_output_key` or output key missing.

**Step 3: Implement summary builder**

Add a helper:

```python
def _build_summary_output(metric_outputs: dict[str, Any]) -> str:
    ...
```

Rules:

- Iterate `items`.
- Keep metrics with non-empty `output`, empty `error`, and output text not containing `返回调用失败`.
- Omit `tools` for now.
- If no valid outputs exist, return `""`.
- `summary_output_key` is opt-in to preserve current output shape.

**Step 4: Run focused test**

Expected: pass.

**Step 5: Run full state metric suite**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper
```

Expected: all tests pass.

**Step 6: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "add state metric summary output"
```

---

### Task 6: Expose Operator Metadata And Compatibility In Callback Config

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py`
- Test: `tests/ops/mapper/test_state_metric_calculator_mapper.py`

**Step 1: Write failing test**

Extend existing callback config test to include:

```python
"start_date_key": None,
"end_date_key": None,
"summary_output_key": None,
"runtime": "adc_operator_code",
```

**Step 2: Run test to verify it fails**

Expected: missing keys.

**Step 3: Update `_operator_config`**

Add new config keys. Do not include fetched operator details or full operator code in callbacks; keep callback payload small and avoid leaking code.

**Step 4: Run test to verify it passes**

Run focused callback test.

**Step 5: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "report state metric runtime config"
```

---

### Task 7: End-To-End Schema And Ray Verification

**Files:**
- Test: `tests/ops/mapper/test_state_metric_calculator_mapper.py`
- Test: `tests/core/data/test_ray_dataset.py` only if Ray mapper hook behavior changes.

**Step 1: Add mixed success/failure block-level schema test**

Build a PyArrow table or Ray-style block with rows where:

- One metric succeeds with string output.
- One metric fails and returns `"null"`.
- One sample has two ids.
- One sample has empty summary output.

Assert `query_metric_data_outputs.items.metrics.output` remains `string`.

**Step 2: Run focused schema test**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_dataset_factory_summary_keeps_arrow_schema_stable
```

Expected: pass.

**Step 3: Run full focused suites**

Run:

```bash
python3 -m py_compile \
  data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py \
  data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py \
  tests/ops/mapper/test_state_metric_calculator_mapper.py \
  tests/ops/mapper/test_state_metric_runtime.py

./.venv/bin/python -m unittest \
  tests.ops.mapper.test_state_metric_runtime \
  tests.ops.mapper.test_state_metric_calculator_mapper
```

Expected: all pass.

**Step 4: Commit**

```bash
git add tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "verify state metric summary schema"
```

---

## Compatibility Notes

- Existing `calculate(...)` functions remain valid.
- Existing `output_key` object shape remains default.
- Existing ADC metadata fetch remains the only source of metric definitions.
- New helpers are provided as optional reserved parameters, so scripts must explicitly request them.
- Auxiliary tools are intentionally excluded from this plan. Add them later as a separate operator or a separate opt-in mode because their handler contracts differ from metric code.

## Verification Matrix

Run before final handoff:

```bash
python3 -m py_compile \
  data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py \
  data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py \
  tests/ops/mapper/test_state_metric_calculator_mapper.py \
  tests/ops/mapper/test_state_metric_runtime.py

./.venv/bin/python -m unittest \
  tests.ops.mapper.test_state_metric_runtime \
  tests.ops.mapper.test_state_metric_calculator_mapper

git diff --check
```

If local Ray or ADC API credentials are unavailable, do not claim online parity. State that local unit/schema verification passed and that final online validation still needs a Ray job with backend operator metadata from `/openapi/state-meta/operators/batch-get`.
