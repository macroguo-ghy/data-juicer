# Code Review Mapper Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an ADC business mapper that reviews one configured sample field with trusted Python code and writes review status and reason fields back to each sample.

**Architecture:** Implement a focused mapper under `data_juicer/ops/mapper/ad_ai_data_center` and register it as `code_review_mapper`, without the `ad_ai_data_center` name prefix. Reuse `PythonScriptRunner` for trusted `compile` / `exec` behavior, but wrap it with a review-specific contract so users write `review(value, row, context)` instead of a generic `process(sample, context)`. The mapper remains one-input-one-output; downstream pipeline/export logic can split all/pass/fail datasets by the configured status field.

**Critical Assumptions & Early Checks:** The review script is trusted and runs in the current Python process, not a sandbox. The backend injects `ctx` because this operator declares `NEED_CTX = True`. The frontend custom page can send the configured field names and `python_code` as plain YAML parameters. The earliest implementation check is config loading through `load_ops(cfg.process)`, because this validates that the frontend-shaped YAML maps to the actual operator constructor.

**Tech Stack:** Python `compile`/`exec` through `PythonScriptRunner`, Data-Juicer `Mapper`, ADC operator execution callback utils, test card notification utils, `unittest`.

---

## Operator Contract

Metadata:

```python
OP_NAME = "code_review_mapper"
CONFIG_PAGE_KEY = "code_review_builder"
NEED_CTX = True
```

File:

```text
data_juicer/ops/mapper/ad_ai_data_center/code_review_mapper.py
```

Parameters:

| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `input_field` | `str` | Yes | `None` | Field in each sample to review, for example `state`. |
| `status_field` | `str` | No | `"review_status"` | Output field for pass/fail result. |
| `reason_field` | `str` | No | `"review_reason"` | Output field for failure reason or empty string. |
| `python_code` | `str` | Yes | `None` | Trusted Python script that defines the review entrypoint. |
| `entrypoint` | `str` | No | `"review"` | Function name to call from `python_code`. |
| `ctx` | `dict` | Yes | `None` | Backend-injected platform context. |

Script contract:

```python
def review(value, row, context):
    return True, ""
```

Arguments:

- `value`: `copy.deepcopy(sample[input_field])`
- `row`: `copy.deepcopy(sample)`
- `context`: `{"ctx": ctx, "operator": "code_review_mapper", "input_field": input_field}`

Supported return shapes:

```python
return True, ""
```

```python
return {
    "passed": True,
    "reason": ""
}
```

Normalize return values as:

- `passed`: `bool`
- `reason`: `str`, return `""` when there is no failure reason

The mapper output keeps the original sample fields and adds / overwrites `status_field` and `reason_field`.

Example output:

```json
{
  "state": {"scene": "feed"},
  "state_review_status": true,
  "state_review_reason": ""
}
```

Business review failure is not an operator exception:

```json
{
  "state": {},
  "state_review_status": false,
  "state_review_reason": "缺少 scene 字段"
}
```

Script compile errors, missing entrypoint, missing input field, invalid return shape, and runtime exceptions are operator failures and should be re-raised after `/record FAILED` reporting.

## YAML Example

```yaml
process:
  - code_review_mapper:
      ctx:
        userAccount: "wangjianda.667"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        apiBase: "https://ai-data-center.bytedance.net/api"
        synthesisInstanceId: 10001
        taskId: 30001
        taskVersion: 1
        operatorIndex: 0
        operatorName: "code_review_mapper"
        operatorType: "business"
      input_field: "state"
      status_field: "state_review_status"
      reason_field: "state_review_reason"
      python_code: |
        def review(value, row, context):
            if not value:
                return False, "state 为空"
            if not isinstance(value, dict):
                return False, "state 必须是对象"
            return True, ""
```

---

### Task 1: Review Script Runner Tests

**Files:**
- Modify: `data_juicer/utils/python_script_utils.py`
- Test: `tests/utils/test_python_script_utils.py`

**Step 1: Write failing tests**

Add tests for review-style functions while preserving existing `process(sample, context)` behavior:

```python
def test_run_with_args_supports_review_entrypoint(self):
    runner = PythonScriptRunner(
        python_code=(
            "def review(value, row, context):\n"
            "    return value == row['state'], ''\n"
        ),
        entrypoint="review",
        require_dict_result=False,
    )

    result = runner.run_with_args(
        {"scene": "feed"},
        {"state": {"scene": "feed"}},
        {"operator": "code_review_mapper"},
    )

    self.assertEqual(result, (True, ""))
```

Also cover:
- runtime exception bubbles with the original message
- missing `review` entrypoint raises `ValueError`

**Step 2: Run tests to verify RED**

Run:

```bash
./.venv/bin/python -m unittest tests.utils.test_python_script_utils
```

Expected: fail because `PythonScriptRunner.run_with_args` does not exist.

**Step 3: Implement minimal runner extension**

Add:

```python
def run_with_args(self, *args):
    result = self.process_func(*args)
    if self.require_dict_result and not isinstance(result, dict):
        raise ValueError(...)
    self._ensure_json_serializable(result)
    return result
```

Keep the existing `run(data, context)` method unchanged by delegating to `run_with_args(data, context or {})`.

**Step 4: Run tests to verify GREEN**

Run:

```bash
./.venv/bin/python -m unittest tests.utils.test_python_script_utils
```

Expected: pass.

**Step 5: Commit**

```bash
git add data_juicer/utils/python_script_utils.py tests/utils/test_python_script_utils.py
git commit -m "Extend Python script runner arguments"
```

### Task 2: Code Review Mapper

**Files:**
- Create: `data_juicer/ops/mapper/ad_ai_data_center/code_review_mapper.py`
- Test: `tests/ops/mapper/test_code_review_mapper.py`

**Step 1: Write failing tests**

Cover:
- metadata constants: `OP_NAME`, `CONFIG_PAGE_KEY`, `NEED_CTX`
- config loading can instantiate `code_review_mapper`
- valid tuple return writes `status_field=True` and `reason_field=""`
- valid dict return writes `status_field=False` and `reason_field`
- missing `input_field` in sample reports record failure and raises
- invalid return shape reports record failure and raises
- script runtime exception reports record failure and raises
- callback failures are observational and do not block successful review output
- lifecycle callbacks call `start`, `finalize`, `failed`
- test card notifications send at operator start and finish

**Step 2: Run tests to verify RED**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_code_review_mapper
```

Expected: fail because `code_review_mapper` is not implemented.

**Step 3: Implement minimal mapper**

Implement:

```python
OP_NAME = "code_review_mapper"
CONFIG_PAGE_KEY = "code_review_builder"
NEED_CTX = True
TEST_CARD_NOTIFICATION_TEMPLATE_ID = "AAqt1lQ72dVxK"
```

Constructor validation:

- `input_field` must be a non-empty string
- `status_field` must be a non-empty string
- `reason_field` must be a non-empty string
- `python_code` must be provided
- `entrypoint` must be a non-empty string

Processing:

```python
def process_single(self, sample):
    ctx = self._get_ctx()
    record_started_at = current_time_millis()
    input_sample = copy.deepcopy(sample)
    try:
        if self.input_field not in sample:
            raise ValueError(f"sample.{self.input_field} must be provided")
        output_sample = copy.deepcopy(sample)
        value = copy.deepcopy(sample[self.input_field])
        result = self.runner.run_with_args(
            value,
            copy.deepcopy(sample),
            {
                "ctx": ctx,
                "operator": OP_NAME,
                "input_field": self.input_field,
            },
        )
        passed, reason = self._normalize_review_result(result)
        output_sample[self.status_field] = passed
        output_sample[self.reason_field] = reason
    except Exception as exc:
        self._report_record_failure(input_sample, sample, str(exc), record_started_at)
        raise
    self._report_record_success(input_sample, output_sample, record_started_at)
    return output_sample
```

Review result normalization:

```python
def _normalize_review_result(self, result):
    if isinstance(result, tuple) and len(result) == 2:
        passed, reason = result
    elif isinstance(result, dict):
        passed = result.get("passed")
        reason = result.get("reason", "")
    else:
        raise ValueError("review result must be (passed, reason) or a dictionary")
    if not isinstance(passed, bool):
        raise ValueError("review passed must be a boolean")
    if not isinstance(reason, str):
        raise ValueError("review reason must be a string")
    return passed, reason
```

Lifecycle, notification, record callbacks should follow `PythonScriptMapper` patterns.

**Step 4: Run tests to verify GREEN**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_code_review_mapper
```

Expected: pass.

**Step 5: Commit**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/code_review_mapper.py tests/ops/mapper/test_code_review_mapper.py
git commit -m "Add code review mapper"
```

### Task 3: Documentation

**Files:**
- Create: `docs/plans/2026-05-17-code-review-mapper-usage.md`

**Step 1: Write operator usage doc**

Document:
- frontend-facing parameters
- script contract
- tuple and dict return examples
- output fields
- YAML example
- error handling rules
- why pass/fail dataset splitting should be done downstream

**Step 2: Verify docs references**

Run:

```bash
rg -n "code_review_mapper|code_review_builder|review_status" docs/plans data_juicer/ops/mapper/ad_ai_data_center tests/ops/mapper
```

Expected: references are consistent.

**Step 3: Commit**

```bash
git add docs/plans/2026-05-17-code-review-mapper-usage.md
git commit -m "Document code review mapper"
```

### Task 4: Verification

Run:

```bash
python3 -m py_compile data_juicer/utils/python_script_utils.py data_juicer/ops/mapper/ad_ai_data_center/code_review_mapper.py tests/utils/test_python_script_utils.py tests/ops/mapper/test_code_review_mapper.py
git diff --check
./.venv/bin/python -m unittest tests.utils.test_python_script_utils tests.ops.mapper.test_code_review_mapper tests.ops.mapper.test_python_script_mapper
```

Expected: all pass.

Run ADC mapper regression if time allows:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_code_review_mapper tests.ops.mapper.test_python_script_mapper tests.ops.mapper.test_llm_inference_mapper tests.ops.mapper.test_state_template_mapper tests.ops.mapper.test_prepare_record_key_mapper
```

Expected: all pass.
