# LLM Inference System User Prompt Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `system_prompt` and `user_prompt` to `llm_inference_mapper`, send them to the backend LLM OpenAPI as separate fields, and make these the preferred prompt configuration path.

**Architecture:** Keep the current LLM submit/result polling flow and record callback behavior unchanged. Add a new prompt-building path that renders `system_prompt` and `user_prompt` with the same Jinja2 renderer planned for `prompt_template`, then submit `{systemPrompt, userPrompt, model}` to `/openapi/synthesis/llm-inference/submit`. Keep legacy `prompt`, `prompt_template`, and `prompt_field` temporarily for backward compatibility, but reject mixed new/legacy prompt configs and stop exposing legacy fields in user-facing docs.

**Critical Assumptions & Early Checks:** The backend submit API accepts `systemPrompt` and `userPrompt` in addition to or instead of `prompt`; confirm exact field names before implementation (`systemPrompt/userPrompt` vs `system_prompt/user_prompt`). `user_prompt` should be required for the new mode, while `system_prompt` is optional. Existing YAMLs that use `prompt_template`, `prompt_field`, or `prompt` must keep working unless they are mixed with the new parameters.

**Tech Stack:** Data-Juicer `Mapper`, ADC `HttpClient`, existing LLM mapper tests, Jinja2 prompt renderer from `docs/plans/2026-05-23-llm-inference-jinja2-prompt-template.md`, `unittest`.

---

## Target Operator Contract

Preferred new YAML:

```yaml
process:
  - llm_inference_mapper:
      ctx:
        userAccount: "wangjianda.667"
        apiBase: "https://ai-data-center.bytedance.net/api"
        operatorName: "llm_inference_mapper"
        operatorType: "Mapper"
      model: "doubao-seed-1.6-flash"
      output_field: "state"
      system_prompt: |
        你是一个广告诊断专家。
        只输出 JSON，不要输出 Markdown 或解释。
      user_prompt: |
        请根据下面的 State 模板生成一份真实 State 数据：

        {{ state_template }}

        用户问题：
        {{ input.user_query }}
```

New parameter rules:

| Parameter | Required | Description |
| --- | --- | --- |
| `user_prompt` | Yes in new mode | Per-sample task prompt. Supports Jinja2. |
| `system_prompt` | No | Role, constraints, output format, and reusable instruction prompt. Supports Jinja2. |

Legacy parameters:

| Parameter | Status | Behavior |
| --- | --- | --- |
| `prompt_template` | Legacy compatible | Keep working when `user_prompt` is not configured. |
| `prompt_field` | Legacy compatible | Keep working when `user_prompt` is not configured. |
| `prompt` | Legacy compatible | Keep working when `user_prompt` is not configured. |

Validation:

- At least one prompt source must be configured.
- If `user_prompt` is configured, do not allow `prompt`, `prompt_template`, or `prompt_field`.
- `system_prompt` without `user_prompt` is invalid.
- Rendered `user_prompt` must be a non-empty string.
- Rendered `system_prompt` may be empty or omitted.

Submit payload in new mode:

```json
{
  "systemPrompt": "你是一个广告诊断专家。",
  "userPrompt": "请根据下面的 State 模板生成...",
  "model": "doubao-seed-1.6-flash"
}
```

Submit payload in legacy mode remains unchanged:

```json
{
  "prompt": "请根据下面的 State 模板生成...",
  "model": "doubao-seed-1.6-flash"
}
```

---

### Task 1: Confirm Backend Submit Field Names

**Files:**
- Read: backend API docs or the backend DTO/controller that owns `/openapi/synthesis/llm-inference/submit`
- Modify later: `data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py`

**Step 1: Verify backend request contract**

Confirm whether backend expects:

```json
{
  "systemPrompt": "...",
  "userPrompt": "...",
  "model": "..."
}
```

or:

```json
{
  "system_prompt": "...",
  "user_prompt": "...",
  "model": "..."
}
```

**Step 2: Record the chosen field names**

Update this plan before implementation if the backend contract is not `systemPrompt/userPrompt`.

Expected:

- The code implementation uses the exact backend field names.
- Tests assert the exact request JSON body.

---

### Task 2: Add Failing Tests For New Prompt Parameters

**Files:**
- Modify: `tests/ops/mapper/test_llm_inference_mapper.py`

**Step 1: Write test for new submit payload**

Add a test near `test_submits_prompt_from_template_polls_result_and_writes_output`:

```python
@patch("data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper.HttpClient")
def test_submits_system_and_user_prompt_payload(self, mock_client_cls):
    submit_client = FakeHttpClient(success_envelope(self._submit_data()))
    success_client = FakeHttpClient(success_envelope(self._success_result_data()))
    mock_client_cls.side_effect = [submit_client, success_client]
    dataset = Dataset.from_list([{
        "state_template": {"ad_state": [{"ad_roi": "ROI"}]},
        "input": {"user_query": "怎么提升 ROI"},
        RECORD_KEY_FIELD: "record-1",
    }])
    op = LLMInferenceMapper(
        system_prompt="你是广告诊断专家：{{ input.user_query }}",
        user_prompt="模板：{{ state_template }}",
        model="doubao",
        ctx=self._ctx(),
        poll_interval_seconds=0,
        auto_op_parallelism=False,
    )

    op.run(dataset).to_list()

    self.assertEqual(submit_client.requests, [{
        "json_body": {
            "systemPrompt": "你是广告诊断专家：怎么提升 ROI",
            "userPrompt": '模板：{"ad_state": [{"ad_roi": "ROI"}]}',
            "model": "doubao",
        }
    }])
```

**Step 2: Write validation tests**

```python
def test_rejects_mixed_new_and_legacy_prompt_configs(self):
    with self.assertRaisesRegex(ValueError, "cannot be configured together"):
        LLMInferenceMapper(
            user_prompt="hello",
            prompt_template="{{ text }}",
            ctx=self._ctx(),
        )
```

```python
def test_rejects_system_prompt_without_user_prompt(self):
    with self.assertRaisesRegex(ValueError, "user_prompt must be provided"):
        LLMInferenceMapper(
            system_prompt="system only",
            ctx=self._ctx(),
        )
```

```python
def test_rejects_empty_rendered_user_prompt(self):
    op = LLMInferenceMapper(
        user_prompt="{{ missing_or_empty }}",
        ctx=self._ctx(),
    )

    with self.assertRaisesRegex(ValueError, "user_prompt must be a non-empty string"):
        op._build_prompt_payload({"missing_or_empty": "", RECORD_KEY_FIELD: "record-1"})
```

**Step 3: Run tests to verify they fail**

Run:

```bash
./.venv/bin/python -m unittest \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_submits_system_and_user_prompt_payload \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_rejects_mixed_new_and_legacy_prompt_configs \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_rejects_system_prompt_without_user_prompt \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_rejects_empty_rendered_user_prompt
```

Expected:

- Fail because constructor does not accept `system_prompt/user_prompt` yet.

---

### Task 3: Implement New Prompt Config Path

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py`

**Step 1: Add constructor parameters**

Add parameters before legacy prompt parameters or immediately after them:

```python
system_prompt: str | None = None,
user_prompt: str | None = None,
```

Store them:

```python
self.system_prompt = system_prompt
self.user_prompt = user_prompt
```

**Step 2: Add validation helpers**

Implement constructor validation:

```python
legacy_prompt_sources = [prompt, prompt_template, prompt_field]
has_new_prompt = user_prompt is not None or system_prompt is not None
has_legacy_prompt = any(legacy_prompt_sources)

if has_new_prompt:
    if user_prompt is None:
        raise ValueError("user_prompt must be provided when system_prompt is configured")
    if has_legacy_prompt:
        raise ValueError(
            "user_prompt/system_prompt cannot be configured together with "
            "prompt, prompt_template, or prompt_field"
        )
elif not has_legacy_prompt:
    raise ValueError(
        "one of user_prompt, prompt, prompt_template, or prompt_field must be provided"
    )
```

**Step 3: Add prompt payload builder**

Replace single `_build_prompt()` usage in `process_single()` with a payload builder:

```python
prompt_payload = self._build_prompt_payload(sample)
submit_data = self._submit(prompt_payload, ctx, sample)
```

Implement:

```python
def _build_prompt_payload(self, sample: dict[str, Any]) -> dict[str, str]:
    if self.user_prompt is not None:
        user_prompt = self._render_prompt_text(self.user_prompt, sample, "user_prompt")
        system_prompt = ""
        if self.system_prompt is not None:
            system_prompt = self._render_prompt_text(self.system_prompt, sample, "system_prompt")
        if not user_prompt:
            raise ValueError("user_prompt must be a non-empty string")
        payload = {
            "userPrompt": user_prompt,
            "model": self.model,
        }
        if system_prompt:
            payload["systemPrompt"] = system_prompt
        return payload

    prompt = self._build_prompt(sample)
    return {
        "prompt": prompt,
        "model": self.model,
    }
```

**Step 4: Add shared render helper**

```python
def _render_prompt_text(self, template: str, sample: dict[str, Any], field_name: str) -> str:
    try:
        value = _SamplePromptRenderer(sample).render(template)
    except KeyError as exc:
        missing_field = exc.args[0]
        raise ValueError(f"{field_name} missing field: {missing_field}") from exc
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must render to a string")
    return value
```

Keep `_build_prompt()` for legacy paths.

**Step 5: Change `_submit()` signature**

From:

```python
def _submit(self, prompt: str, ctx: dict[str, Any], sample: dict[str, Any]) -> dict[str, Any]:
```

To:

```python
def _submit(self, prompt_payload: dict[str, Any], ctx: dict[str, Any], sample: dict[str, Any]) -> dict[str, Any]:
```

and send `json_body=prompt_payload`.

---

### Task 4: Update Operator Config Metadata

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py`
- Modify: `tests/ops/mapper/test_llm_inference_mapper.py`

**Step 1: Update `_prompt_source()`**

Return new source when applicable:

```python
def _prompt_source(self) -> str:
    if self.user_prompt is not None:
        return "system_user_prompt" if self.system_prompt is not None else "user_prompt"
    if self.prompt_field:
        return "prompt_field"
    if self.prompt_template:
        return "prompt_template"
    return "prompt"
```

**Step 2: Update `_operator_config()`**

Include only metadata, not full prompt content:

```python
return {
    "prompt_source": self._prompt_source(),
    "has_system_prompt": self.system_prompt is not None,
    "model": self.model,
    ...
}
```

Do not send raw prompt text to operator config unless product explicitly needs it, because prompts may contain long content and sample placeholders.

**Step 3: Add metadata test**

Update the existing `operator_config` assertion to include:

```python
"prompt_source": "system_user_prompt",
"has_system_prompt": True,
```

Expected:

- `/start` still works.
- Operator lifecycle hooks stay unchanged.

---

### Task 5: Preserve Legacy Behavior

**Files:**
- Test: `tests/ops/mapper/test_llm_inference_mapper.py`
- Modify if needed: `data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py`

**Step 1: Run legacy submit test**

Run:

```bash
./.venv/bin/python -m unittest \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_submits_prompt_from_template_polls_result_and_writes_output \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_prompt_field_takes_precedence_and_sends_empty_model_by_default
```

Expected:

- Existing tests pass without changing expected legacy payloads.
- Legacy mode still sends `{"prompt": ..., "model": ...}`.

**Step 2: Keep old methods where useful**

Do not delete `_build_prompt()` in this change. It remains the compatibility implementation for old YAMLs and tests.

---

### Task 6: Update Docs And Config Page Guide

**Files:**
- Modify: `docs/plans/2026-05-17-llm-inference-mapper.md`
- Modify: `docs/plans/2026-05-22-llm-inference-mapper-config-page-guide.md`
- Modify: `docs/plans/2026-05-23-llm-inference-jinja2-prompt-template.md` if it should reference this follow-up plan

**Step 1: Update parameter table**

Add:

```markdown
| `system_prompt` | `str` | No | `None` | System instruction prompt. Supports Jinja2. |
| `user_prompt` | `str` | Yes in new mode | `None` | User task prompt. Supports Jinja2. |
```

Mark legacy fields:

```markdown
`prompt`, `prompt_template`, and `prompt_field` are legacy-compatible fields.
New custom configuration pages should use `system_prompt` and `user_prompt`.
```

**Step 2: Add front-end guidance**

For the custom config page:

```markdown
前端建议只暴露 `system_prompt` 和 `user_prompt` 两个 Prompt 编辑器。
`system_prompt` 可选，适合角色、边界、输出格式约束。
`user_prompt` 必填，适合放每条数据相关的任务输入，支持 Jinja2 变量。
```

**Step 3: Add YAML example**

Use the target YAML from the top of this plan.

---

### Task 7: Verification

**Files:**
- `data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py`
- `tests/ops/mapper/test_llm_inference_mapper.py`

**Step 1: Syntax check**

Run:

```bash
python3 -m py_compile \
  data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py \
  tests/ops/mapper/test_llm_inference_mapper.py
```

Expected: exit code `0`.

**Step 2: Full unit test**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_llm_inference_mapper
```

Expected: all LLM mapper tests pass.

**Step 3: Coverage check**

Run:

```bash
./.venv/bin/python -m pytest tests/ops/mapper/test_llm_inference_mapper.py \
  --cov=data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper \
  --cov-report=term-missing \
  --cov-fail-under=90
```

Expected: coverage is at least `90%`.

---

### Task 8: Commit

**Files:**
- `data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py`
- `tests/ops/mapper/test_llm_inference_mapper.py`
- `docs/plans/2026-05-17-llm-inference-mapper.md`
- `docs/plans/2026-05-22-llm-inference-mapper-config-page-guide.md`
- `docs/plans/2026-05-23-llm-inference-jinja2-prompt-template.md` if updated

**Step 1: Review scoped diff**

Run:

```bash
git status --short
git diff -- data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py tests/ops/mapper/test_llm_inference_mapper.py
git diff -- docs/plans/2026-05-17-llm-inference-mapper.md docs/plans/2026-05-22-llm-inference-mapper-config-page-guide.md docs/plans/2026-05-23-llm-inference-jinja2-prompt-template.md
```

Expected:

- Only scoped files are changed.
- No generated files or unrelated local edits are staged.

**Step 2: Commit**

Run:

```bash
git add \
  data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py \
  tests/ops/mapper/test_llm_inference_mapper.py \
  docs/plans/2026-05-17-llm-inference-mapper.md \
  docs/plans/2026-05-22-llm-inference-mapper-config-page-guide.md \
  docs/plans/2026-05-23-llm-inference-jinja2-prompt-template.md
git commit -m "feat: support system and user prompts for llm mapper"
```

