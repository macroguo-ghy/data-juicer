# Python 脚本处理算子使用说明

`python_script_mapper` 是通用 Python 脚本处理算子，用于对每条样本执行可信 Python 代码，并把脚本返回的 `dict` 作为当前样本的输出。

## 适用场景

- 给样本新增字段。
- 根据多个字段生成一个新字段。
- 对整条样本做结构转换。
- 用少量 Python 逻辑实现临时业务处理。

如果目标是“审核某个字段并输出通过/失败原因”，优先使用 `code_review_mapper`。

## 算子元数据

```python
OP_NAME = "python_script_mapper"
OP_DISPLAY_NAME = "Python 脚本处理"
CONFIG_PAGE_KEY = "python_script_builder"
NEED_CTX = True
OPERATOR_TAG = "business_operator"
```

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `ctx` | object | 是 | 无 | 平台上下文，由后端注入。 |
| `python_code` | string | 是 | `""` | 完整 Python 脚本内容。 |
| `entrypoint` | string | 否 | `process` | 脚本入口函数名。 |

`ctx` 至少需要包含平台回调所需字段，例如：

```yaml
ctx:
  userAccount: "wangjianda.667"
  apiBase: "https://ai-data-center.bytedance.net/api"
  synthesisInstanceId: 10001
  operatorIndex: 1
  operatorName: "python_script_mapper"
  operatorType: "Mapper"
```

## 脚本入口规范

默认入口函数：

```python
def process(sample, context):
    return sample
```

入参说明：

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `sample` | dict | 当前样本的副本。脚本修改它不会在异常时污染原始样本。 |
| `context` | dict | 算子上下文。 |

`context` 结构：

```python
{
    "ctx": ctx,
    "operator": "python_script_mapper"
}
```

返回值要求：

- 必须返回 `dict`。
- 返回的 `dict` 会作为当前样本的最终输出。
- 如果返回非 `dict`，算子会报错。

## 示例：新增字段

```yaml
process:
  - python_script_mapper:
      ctx:
        userAccount: "wangjianda.667"
        apiBase: "https://ai-data-center.bytedance.net/api"
        synthesisInstanceId: 10001
        operatorIndex: 1
        operatorName: "python_script_mapper"
        operatorType: "Mapper"
      python_code: |
        def process(sample, context):
            sample["test_field"] = "hello"
            return sample
      entrypoint: "process"
```

输入：

```json
{
  "__adc_record_key": "record-1",
  "text": "hello world"
}
```

输出：

```json
{
  "__adc_record_key": "record-1",
  "text": "hello world",
  "test_field": "hello"
}
```

## 示例：根据多个字段生成字段

```yaml
process:
  - python_script_mapper:
      ctx:
        userAccount: "wangjianda.667"
        apiBase: "https://ai-data-center.bytedance.net/api"
        synthesisInstanceId: 10001
        operatorIndex: 1
        operatorName: "python_script_mapper"
        operatorType: "Mapper"
      python_code: |
        def process(sample, context):
            title = sample.get("title") or ""
            body = sample.get("body") or ""
            sample["summary_input"] = title + "\n" + body
            return sample
```

## 异常与回调

- 单条样本脚本执行成功后，上报 `/record` 成功。
- 单条样本脚本执行失败后，上报 `/record` 失败，并抛出异常。
- 算子开始时会上报算子执行开始。
- 算子结束时会上报成功或失败状态。
- 当前暂时关闭算子完成阶段的测试卡片通知。

## 注意事项

- 该算子不提供沙箱隔离，脚本会在当前 Python 环境中执行，只适合可信代码。
- 脚本应避免访问不必要的本地文件、网络或系统资源。
- 输出数据需要兼容下游存储类型。写入 Lance/Magnus 时，复杂对象字段建议转成 JSON 字符串，避免结构字段顺序或类型不一致。
- 如果样本中有 `__adc_log_id`，后续 HTTP/OpenAPI 调用和 `/record` 回调会按需透传 `x-tt-logid`。
