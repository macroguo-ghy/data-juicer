# Python Script Mapper Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an ADC business mapper that executes a trusted full Python script against each sample.

**Architecture:** Put the operator under `data_juicer/ops/mapper/ad_ai_data_center` while registering it as `python_script_mapper`, without the `ad_ai_data_center` name prefix. Extract script compile/entrypoint/return validation into a shared utility so this mapper and existing script-driven ADC operators do not duplicate `exec` handling.

**Critical Assumptions & Early Checks:** The script is trusted and executes in the current Python process, not a sandbox. The script must define a callable `process(sample, context)` entrypoint and return a JSON-serializable `dict`. The mapper requires backend-injected `ctx` and expects upstream `prepare_record_key_mapper` to have already produced `__adc_record_key` when record callbacks are needed.

**Tech Stack:** Python `compile`/`exec`, Data-Juicer `Mapper`, ADC operator execution callback utils, `unittest`.

---

### Task 1: Shared Python Script Runner

**Files:**
- Create: `data_juicer/utils/python_script_utils.py`
- Test: `tests/utils/test_python_script_utils.py`

**Step 1: Write failing tests**

Cover:
- valid script with `process(sample, context)` mutates and returns a dict
- missing entrypoint raises a clear `ValueError`
- non-dict return raises a clear `ValueError`
- non-JSON-serializable return raises a clear `ValueError`

**Step 2: Run tests to verify RED**

Run:

```bash
./.venv/bin/python -m unittest tests.utils.test_python_script_utils
```

Expected: fail because `data_juicer.utils.python_script_utils` does not exist.

**Step 3: Implement minimal runner**

Create `PythonScriptRunner`:
- compile script with mode `exec`
- execute with builtins available
- read configured `entrypoint`
- call entrypoint with `sample` and `context`
- validate the result is a `dict`
- validate JSON serialization

**Step 4: Run tests to verify GREEN**

Run:

```bash
./.venv/bin/python -m unittest tests.utils.test_python_script_utils
```

Expected: pass.

### Task 2: ADC Python Script Mapper

**Files:**
- Create: `data_juicer/ops/mapper/ad_ai_data_center/python_script_mapper.py`
- Test: `tests/ops/mapper/test_python_script_mapper.py`

**Step 1: Write failing tests**

Cover:
- registered name is `python_script_mapper`
- `NEED_CTX = True`
- config loading can instantiate the operator by `python_script_mapper`
- processing a sample calls the script with `ctx` in context and returns the script result
- success reports `/record SUCCESS` with per-record `started_at`
- script failure reports `/record FAILED` and re-raises
- callback failures are logged and do not block the script result
- before/after lifecycle calls start/finalize/failed

**Step 2: Run tests to verify RED**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_python_script_mapper
```

Expected: fail because `python_script_mapper` is not implemented.

**Step 3: Implement mapper**

Implement `PythonScriptMapper` under ADC directory:
- `OP_NAME = "python_script_mapper"`
- `NEED_CTX = True`
- parameters: `python_code`, `entrypoint="process"`, `ctx=None`
- context passed to script: `{"ctx": ctx, "operator": OP_NAME}`
- use `current_time_millis()` for per-record `started_at`
- report record success/failure through `OperatorExecutionCallbackClient`
- keep callback failures observational
- use `before_operator_started` and `after_operator_finished` like other ADC mappers

**Step 4: Run tests to verify GREEN**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_python_script_mapper
```

Expected: pass.

### Task 3: Reuse Runner In External Eval Import

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/external_eval_data_import_mapper.py`
- Test: `tests/ops/mapper/test_external_eval_data_import_mapper.py`

**Step 1: Add a focused test if needed**

Ensure existing external eval tests still verify script result validation.

**Step 2: Replace local compile/exec helper**

Use `PythonScriptRunner` for `python_code`, keeping the public behavior unchanged.

**Step 3: Run focused tests**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_external_eval_data_import_mapper
```

Expected: pass.

### Task 4: Verification

Run:

```bash
python3 -m py_compile data_juicer/utils/python_script_utils.py data_juicer/ops/mapper/ad_ai_data_center/python_script_mapper.py data_juicer/ops/mapper/ad_ai_data_center/external_eval_data_import_mapper.py tests/utils/test_python_script_utils.py tests/ops/mapper/test_python_script_mapper.py tests/ops/mapper/test_external_eval_data_import_mapper.py
git diff --check
./.venv/bin/python -m unittest tests.utils.test_python_script_utils tests.ops.mapper.test_python_script_mapper tests.ops.mapper.test_external_eval_data_import_mapper
```

Expected: all pass.
