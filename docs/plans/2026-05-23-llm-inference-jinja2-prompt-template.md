# LLM Inference Jinja2 Prompt Template Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the LLM inference mapper's custom `prompt_template` placeholder renderer with real Jinja2 rendering while preserving the current operator API and output behavior.

**Architecture:** Keep `llm_inference_mapper` parameters unchanged for the first version: users still configure `prompt`, `prompt_field`, or `prompt_template`, and only `prompt_template` gains full Jinja2 syntax. Introduce a small Jinja2 rendering helper inside `data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py`, configure strict missing-variable handling, and add filters for stable JSON stringification. The submit/result OpenAPI request body remains unchanged: rendered prompt is still sent as the existing `prompt` field.

**Critical Assumptions & Early Checks:** Jinja2 must be available in local tests and online Ray workers; if it is not guaranteed by the base image, add it to operator/runtime dependencies. Existing templates using `{{ text }}`, `{{ a.b.d }}`, and single-brace literals like `{state}` must keep working. Existing custom wildcard syntax `items[*].metric` is not valid native Jinja2, so either keep a compatibility preprocessor or migrate tests/docs to Jinja2-native `map`/`for` usage; this plan chooses compatibility for `[*]` to avoid breaking current configured prompts.

**Tech Stack:** Data-Juicer `Mapper`, `jinja2.Environment`, `jinja2.StrictUndefined`, `unittest`, existing ADC LLM mapper tests.

---

## Target User Contract

The YAML surface stays the same:

```yaml
process:
  - llm_inference_mapper:
      ctx:
        userAccount: "wangjianda.667"
        apiBase: "https://ai-data-center.bytedance.net/api"
        operatorName: "llm_inference_mapper"
        operatorType: "Mapper"
      output_field: "state"
      model: "doubao-seed-1.6-flash"
      prompt_template: |
        你是一个广告诊断场景的 State 数据模拟器。

        State 模板：
        {{ state_template }}

        用户问题：
        {{ input.user_query }}

        {% if review_reason %}
        上轮审核失败原因：
        {{ review_reason }}
        {% endif %}
```

Supported Jinja2 capabilities:

- Variable replacement: `{{ state_template }}`
- Nested dict access: `{{ input.user_query }}`
- Conditional blocks: `{% if review_reason %}...{% endif %}`
- Loops: `{% for item in adv_state %}...{% endfor %}`
- JSON filter for object/list values: `{{ state | tojson_cn }}`

Compatibility behavior to preserve:

- Single-brace text such as `{state_template}` remains literal text.
- Missing fields fail clearly with `ValueError("prompt_template missing field: ...")`.
- `None` renders as an empty string for simple variable output.
- Object/list values render as JSON with `ensure_ascii=False` when inserted directly.
- Existing `{{ items[*].metric }}` and `{{ items[].metric }}` templates keep working by preconverting those paths into a renderable value.

`system_prompt` and `user_prompt` are handled by the follow-up plan
`docs/plans/2026-05-23-llm-inference-system-user-prompt.md`; they should reuse the same Jinja2 renderer.

---

### Task 1: Add Failing Tests For Jinja2 Syntax

**Files:**
- Modify: `tests/ops/mapper/test_llm_inference_mapper.py`

**Step 1: Write failing tests**

Add tests near the existing prompt-template tests:

```python
def test_prompt_template_supports_jinja2_if_and_for_blocks(self):
    op = LLMInferenceMapper(
        prompt_template=(
            "用户：{{ input.user_query }}\n"
            "{% if review_reason %}失败原因：{{ review_reason }}{% endif %}\n"
            "{% for item in adv_state %}计划：{{ item.adv_id }}={{ item.adv_roi }};{% endfor %}"
        ),
        ctx=self._ctx(),
    )

    prompt = op._build_prompt({
        "input": {"user_query": "怎么提升 ROI"},
        "review_reason": "ROI 为空",
        "adv_state": [
            {"adv_id": "1", "adv_roi": 1.2},
            {"adv_id": "2", "adv_roi": 1.5},
        ],
        RECORD_KEY_FIELD: "record-1",
    })

    self.assertIn("用户：怎么提升 ROI", prompt)
    self.assertIn("失败原因：ROI 为空", prompt)
    self.assertIn("计划：1=1.2;计划：2=1.5;", prompt)
```

```python
def test_prompt_template_supports_tojson_cn_filter(self):
    op = LLMInferenceMapper(
        prompt_template="State：{{ state | tojson_cn }}",
        ctx=self._ctx(),
    )

    prompt = op._build_prompt({
        "state": {"广告": [{"roi": 1.2}]},
        RECORD_KEY_FIELD: "record-1",
    })

    self.assertEqual(prompt, 'State：{"广告": [{"roi": 1.2}]}')
```

```python
def test_prompt_template_missing_nested_field_raises_clear_error(self):
    op = LLMInferenceMapper(
        prompt_template="用户：{{ input.user_query }}",
        ctx=self._ctx(),
    )

    with self.assertRaisesRegex(ValueError, "prompt_template missing field: input.user_query"):
        op._build_prompt({
            "input": {},
            RECORD_KEY_FIELD: "record-1",
        })
```

**Step 2: Run tests to verify they fail**

Run:

```bash
./.venv/bin/python -m unittest \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_prompt_template_supports_jinja2_if_and_for_blocks \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_prompt_template_supports_tojson_cn_filter \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_prompt_template_missing_nested_field_raises_clear_error
```

Expected:

- `if`/`for` test fails because the current renderer only replaces simple `{{ }}` placeholders.
- `tojson_cn` test fails because the filter does not exist.
- Missing nested field message may fail because current behavior comes from the custom path resolver.

---

### Task 2: Add Jinja2 Rendering Helper

**Files:**
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py`
- Modify if needed: `pyproject.toml`

**Step 1: Add dependency handling**

If `Jinja2` is not already a guaranteed runtime dependency, add it to `pyproject.toml` core dependencies:

```toml
"Jinja2",
```

Also consider explicit operator dependency if the project expects operator-scoped runtime env installation:

```python
class LLMInferenceMapper(Mapper):
    _requirements = ["Jinja2"]
```

Use only one dependency path if the repository convention makes that clear during implementation.

**Step 2: Replace `_SamplePromptRenderer` internals**

Keep the class name to limit call-site churn, but change it to use Jinja2:

```python
from jinja2 import Environment, StrictUndefined, UndefinedError
```

Implementation shape:

```python
class _SamplePromptRenderer:
    """Render prompt templates with Jinja2 and ADC compatibility filters."""

    ARRAY_PATH_PATTERN = re.compile(
        r"{{\s*([A-Za-z_][A-Za-z0-9_]*(?:\[\*\]|\[\])?"
        r"(?:\.[A-Za-z_][A-Za-z0-9_]*(?:\[\*\]|\[\])?)*)\s*}}"
    )

    def __init__(self, sample: dict[str, Any]):
        self.sample = sample

    def render(self, template: str) -> str:
        context = self._build_context(template)
        env = Environment(
            autoescape=False,
            undefined=StrictUndefined,
        )
        env.filters["tojson_cn"] = self._tojson_cn
        env.finalize = self._finalize_template_value
        try:
            return env.from_string(template).render(**context)
        except UndefinedError as exc:
            missing_field = self._extract_missing_field(str(exc))
            raise KeyError(missing_field) from exc
```

The helper must keep `_build_prompt()`'s current error conversion:

```python
except KeyError as exc:
    missing_field = exc.args[0]
    raise ValueError(f"prompt_template missing field: {missing_field}") from exc
```

**Step 3: Preserve direct object/list rendering**

Use `Environment.finalize` to keep current behavior:

```python
@classmethod
def _finalize_template_value(cls, value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return cls._tojson_cn(value)
    return value
```

Filter:

```python
@staticmethod
def _tojson_cn(value):
    return json.dumps(value, ensure_ascii=False)
```

**Step 4: Preserve `items[*].metric` compatibility**

Jinja2 cannot parse `items[*].metric`. Before rendering, detect current wildcard placeholders and rewrite only those placeholders to generated variable names:

```python
{{ items[*].metric }} -> {{ __adc_jinja_path_0 }}
```

Build `context["__adc_jinja_path_0"]` by reusing the current path resolution logic for wildcard paths. This keeps existing templates working while enabling new Jinja2 syntax.

Important boundary:

- Only rewrite simple placeholder expressions matching the old syntax.
- Do not rewrite expressions containing filters, function calls, arithmetic, or Jinja blocks.
- Native Jinja2 syntax should pass through untouched.

---

### Task 3: Preserve Existing Prompt Template Contracts

**Files:**
- Modify: `tests/ops/mapper/test_llm_inference_mapper.py`
- Modify: `data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py`

**Step 1: Run existing prompt-template tests**

Run:

```bash
./.venv/bin/python -m unittest \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_prompt_template_supports_jinja_style_placeholders \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_prompt_template_keeps_single_brace_text_literal \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_prompt_template_stringifies_scalar_values \
  tests.ops.mapper.test_llm_inference_mapper.LLMInferenceMapperTest.test_prompt_template_only_serializes_referenced_fields
```

Expected:

- All pass after implementation.

**Step 2: Add or update regression tests if needed**

If direct Jinja2 rendering changes boolean output or missing-field message, add explicit tests documenting the chosen contract.

Current desired scalar contract:

```text
True -> True
None -> ""
```

Current desired missing-field contract:

```text
ValueError: prompt_template missing field: <path>
```

---

### Task 4: Verify Full LLM Mapper Behavior

**Files:**
- Test: `tests/ops/mapper/test_llm_inference_mapper.py`

**Step 1: Run focused syntax check**

Run:

```bash
python3 -m py_compile \
  data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py \
  tests/ops/mapper/test_llm_inference_mapper.py
```

Expected: no output and exit code `0`.

**Step 2: Run full LLM mapper tests**

Run:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_llm_inference_mapper
```

Expected: all tests pass.

**Step 3: Run coverage for changed behavior**

Run:

```bash
./.venv/bin/python -m pytest tests/ops/mapper/test_llm_inference_mapper.py \
  --cov=data_juicer.ops.mapper.ad_ai_data_center.llm_inference_mapper \
  --cov-report=term-missing \
  --cov-fail-under=90
```

Expected: coverage is at least `90%`.

---

### Task 5: Update User-Facing Docs

**Files:**
- Modify: `docs/plans/2026-05-17-llm-inference-mapper.md`
- Modify: `docs/plans/2026-05-22-llm-inference-mapper-config-page-guide.md`

**Step 1: Update prompt-template description**

Replace the old custom placeholder wording with:

```markdown
`prompt_template` supports Jinja2 syntax. Variables come from the current sample.
Use `{{ field }}` for simple fields, `{{ a.b.c }}` for nested objects,
`{% if ... %}` for conditional text, and `{% for item in items %}` for loops.
```

**Step 2: Add object/list rendering guidance**

Document:

```markdown
When object/list values need to be embedded as JSON, prefer:

{{ state | tojson_cn }}

Direct `{{ state }}` is also JSON-stringified for compatibility, but the filter is clearer for users.
```

**Step 3: Keep single-brace warning**

Document:

```markdown
Only `{{ }}` is treated as template syntax. Single braces such as `{state}` are ordinary text.
```

---

### Task 6: Commit

**Files:**
- `data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py`
- `tests/ops/mapper/test_llm_inference_mapper.py`
- `docs/plans/2026-05-17-llm-inference-mapper.md`
- `docs/plans/2026-05-22-llm-inference-mapper-config-page-guide.md`
- `pyproject.toml` if dependency is added

**Step 1: Review worktree**

Run:

```bash
git status --short
git diff -- data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py tests/ops/mapper/test_llm_inference_mapper.py
git diff -- docs/plans/2026-05-17-llm-inference-mapper.md docs/plans/2026-05-22-llm-inference-mapper-config-page-guide.md
```

Expected:

- Only scoped files are changed.
- No generated files, output tables, caches, or local artifacts are staged.

**Step 2: Commit**

Run:

```bash
git add \
  data_juicer/ops/mapper/ad_ai_data_center/llm_inference_mapper.py \
  tests/ops/mapper/test_llm_inference_mapper.py \
  docs/plans/2026-05-17-llm-inference-mapper.md \
  docs/plans/2026-05-22-llm-inference-mapper-config-page-guide.md \
  pyproject.toml
git commit -m "feat: support jinja2 prompt templates for llm mapper"
```

If `pyproject.toml` is not changed, omit it from `git add`.
