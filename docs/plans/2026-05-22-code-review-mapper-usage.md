# 代码审核算子使用说明

`code_review_mapper` 是字段审核算子，用于对样本中的某个字段执行可信 Python 校验逻辑，并输出审核结果字段和失败原因字段。

## 适用场景

- 校验 `state` 字段是否符合业务格式。
- 校验 LLM 生成结果是否满足约束。
- 对某个字段做布尔审核，并记录失败原因。

如果目标是任意字段加工或整行转换，优先使用 `python_script_mapper`。

## 算子元数据

```python
OP_NAME = "code_review_mapper"
OP_DISPLAY_NAME = "代码审核"
CONFIG_PAGE_KEY = "code_review_builder"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
```

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `ctx` | object | 是 | 无 | 平台上下文，由后端注入。 |
| `input_field` | string | 是 | 无 | 需要审核的样本字段名。 |
| `status_field` | string | 否 | `review_status` | 审核结果输出字段，值为 `bool`。 |
| `reason_field` | string | 否 | `review_reason` | 审核原因输出字段，值为 `string`。 |
| `python_code` | string | 是 | `""` | 完整 Python 审核脚本。 |
| `entrypoint` | string | 否 | `review_row` | 脚本入口函数名。 |

`ctx` 至少需要包含平台回调所需字段，例如：

```yaml
ctx:
  userAccount: "wangjianda.667"
  apiBase: "https://ai-data-center.bytedance.net/api"
  synthesisInstanceId: 10001
  operatorIndex: 2
  operatorName: "code_review_mapper"
  operatorType: "Mapper"
```

## 脚本入口规范

默认入口函数：

```python
def review_row(value, row, context):
    return True, ""
```

入参说明：

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `value` | any | `input_field` 对应的字段值。 |
| `row` | dict | 当前完整样本的副本。 |
| `context` | dict | 算子上下文。 |

`context` 结构：

```python
{
    "ctx": ctx,
    "operator": "code_review_mapper",
    "input_field": input_field
}
```

返回值支持两种格式。

格式一：二元组。

```python
return True, ""
return False, "失败原因"
```

格式二：字典。

```python
return {
    "passed": True,
    "reason": ""
}
```

返回值要求：

- `passed` 必须是 `bool`。
- `reason` 必须是 `str`。
- 不符合格式会导致当前记录执行失败。

## 示例：审核 State 字段

```yaml
process:
  - code_review_mapper:
      ctx:
        userAccount: "wangjianda.667"
        apiBase: "https://ai-data-center.bytedance.net/api"
        synthesisInstanceId: 10001
        operatorIndex: 2
        operatorName: "code_review_mapper"
        operatorType: "Mapper"
      input_field: "state"
      status_field: "state_review_status"
      reason_field: "state_review_reason"
      python_code: |
        import json

        def review_row(value, row, context):
            try:
                state = json.loads(value) if isinstance(value, str) else value
            except Exception as exc:
                return False, f"state 不是合法 JSON: {exc}"

            if not isinstance(state, dict):
                return False, "state 必须是对象"

            ad_state = state.get("ad_state")
            if not isinstance(ad_state, list):
                return False, "ad_state 必须是数组"

            return True, ""
      entrypoint: "review_row"
```

输入：

```json
{
  "__adc_record_key": "record-1",
  "state": "{\"ad_state\": [{\"ad_roi\": [1.2, 1.3]}]}"
}
```

输出：

```json
{
  "__adc_record_key": "record-1",
  "state": "{\"ad_state\": [{\"ad_roi\": [1.2, 1.3]}]}",
  "state_review_status": true,
  "state_review_reason": ""
}
```

## 示例：审核失败

脚本：

```python
def review_row(value, row, context):
    if not value:
        return False, "待审核字段不能为空"
    return True, ""
```

输出：

```json
{
  "review_status": false,
  "review_reason": "待审核字段不能为空"
}
```

## 异常与回调

- 审核通过或审核不通过都属于脚本正常执行，会上报 `/record` 成功。
- 只有脚本异常、入口函数不存在、返回值格式错误等情况，才会上报 `/record` 失败。
- 算子开始时会上报算子执行开始。
- 算子结束时会上报成功或失败状态。
- 当前暂时关闭算子完成阶段的测试卡片通知。

## 注意事项

- 该算子不提供沙箱隔离，脚本会在当前 Python 环境中执行，只适合可信代码。
- `input_field` 必须存在，否则当前记录失败。
- `reason` 必须返回字符串；如果没有失败原因，返回空字符串。
- 审核不通过时不要抛异常，应返回 `False, "原因"`，这样可以保留样本并写出审核结果。
- 如果样本中有 `__adc_log_id`，后续 HTTP/OpenAPI 调用和 `/record` 回调会按需透传 `x-tt-logid`。
