# State Template Mapper Implementation

## Goal

Add an ADC business mapper that generates `state_template` from selected State metadata. Users configure selected
State groups and IDs in the frontend; the operator calls AD AI Data Center OpenAPI at runtime and writes the generated
template JSON string into each sample.

## Operator Metadata

```python
OP_NAME = "state_template_mapper"
CONFIG_PAGE_KEY = "state_template_builder"
NEED_CTX = True
```

File:

```text
data_juicer/ops/mapper/ad_ai_data_center/state_template_mapper.py
```

`CONFIG_PAGE_KEY` tells the frontend to render the custom State template builder page. It is a module-level constant
and is not passed to `@OPERATORS.register_module(...)`.

## Parameters

| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `state_meta_group_items` | `dict[str, list[int]]` | Yes | `None` | State group English name to selected attribute/operator ID list. |
| `output_field` | `str` | No | `"state_template"` | Field used to store the generated template JSON string. |
| `ctx` | `dict` | Yes | `None` | Backend-injected platform context. |
| `timeout` | `float` | No | `30.0` | HTTP request timeout in seconds. |

Example `state_meta_group_items`:

```json
{
  "ad_state": [101, 102],
  "world_state": [201]
}
```

The operator does not distinguish attribute IDs from operator IDs. It passes the selected IDs through to the backend.

## API Contract

The operator calls:

```http
POST {ctx.apiBase}/openapi/state-meta/generate-json
Content-Type: application/json
```

Request body:

```json
{
  "groupItems": {
    "ad_state": [101, 102],
    "world_state": [201]
  }
}
```

Headers:

```json
{
  "Content-Type": "application/json",
  "Accept": "application/json",
  "user-account": "<ctx.userAccount>",
  "x-tt-env": "<ctx.x-tt-env>",
  "x-use-ppe": "<ctx.x-use-ppe>"
}
```

`x-tt-env` and `x-use-ppe` are included only when present in `ctx`.

## Output

The backend may return either a JSON object or a JSON object string. The mapper validates the value as a JSON object
and writes a normalized JSON string to `output_field`.

Example output sample:

```json
{
  "state_template": "{\"ad_state\": {\"ad_material_clicks\": {\"cn_name\": \"素材点击数\", \"description\": \"素材每日点击数 14日序列\", \"format_requirement\": \"14个元素的整数数组，非负\"}}}"
}
```

If the backend returns a non-object JSON value or a malformed JSON string, the mapper raises `ValueError`.

The generated template only depends on operator configuration, not on each sample. The mapper caches it inside the
operator instance after the first successful API call. In Ray execution, each worker can have its own operator
instance, so the API may be called once per worker, but not once per record in the same worker.

The string output is intentional. Downstream LLM prompt templates can use `{state_template}` directly, and Lance/Magnus
output tables can use a simple string column instead of a dynamic struct schema.

## YAML Example

```yaml
process:
  - state_template_mapper:
      ctx:
        userAccount: "wangjianda.667"
        x-tt-env: "ppe_sirius2"
        x-use-ppe: "1"
        apiBase: "https://ai-data-center.bytedance.net/api"
        synthesisInstanceId: 10001
        taskId: 30001
        taskVersion: 1
        operatorIndex: 0
        operatorName: "state_template_mapper"
        operatorType: "business"
      state_meta_group_items:
        ad_state:
          - 101
          - 102
        world_state:
          - 201
      output_field: "state_template"
      timeout: 30
```

## Error Handling

The operator fails fast when:

- `state_meta_group_items` is empty or not a dictionary.
- `output_field` is empty.
- required `ctx` fields such as `apiBase` or `userAccount` are missing.
- HTTP request fails.
- backend business response has a non-zero `code`.
- backend response cannot be normalized to a JSON object.

Per-record callback failures are observational and are logged as warnings; they do not mask the main State template
generation result or failure.

## Verification

Focused tests:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper
```

Broader ADC mapper regression:

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_template_mapper tests.ops.mapper.test_llm_inference_mapper tests.ops.mapper.test_http_mapper tests.ops.mapper.test_python_script_mapper tests.ops.mapper.test_prepare_record_key_mapper tests.ops.mapper.test_external_eval_data_import_mapper
```
