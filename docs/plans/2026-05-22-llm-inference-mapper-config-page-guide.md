# LLM 推理节点配置页使用说明

LLM 推理节点用于按每条样本生成 Prompt，调用模型服务，并把模型返回结果写入指定字段。

## 字段说明

| 配置项 | 说明 |
| --- | --- |
| 新增字段 | 模型输出写入的字段名，例如 `state`、`llm_output`。 |
| 模型 | 调用的模型名称，例如 `doubao-15-pro-32k`。 |
| 并行进程数 | 算子执行并发数，`-1` 表示自动计算。 |
| System Prompt | 可选，放角色设定、输出格式、约束规则。 |
| User Prompt | 必填，放具体任务描述和样本变量。 |

## Prompt 模板变量

System Prompt 和 User Prompt 都支持 Jinja2 语法，使用 `{{字段名}}` 读取当前样本字段。

示例：

```markdown
你是一个广告诊断专家，只输出 JSON。
```

```markdown
请根据用户问题生成摘要：

用户问题：{{input.user_query}}
历史信息：{{context.memory.chat_history}}

{% if review_reason %}
上轮审核失败原因：{{ review_reason }}
{% endif %}
```

变量说明：

- `{{state_template}}`：读取样本中的 `state_template` 字段。
- `{{input.user_query}}`：读取对象内部字段。
- `{% for item in items %}{{ item.name }}{% endfor %}`：遍历数组。
- `{{state | tojson_cn}}`：把对象或数组转成 JSON 字符串。
- `{state_template}` 这种单大括号不会被识别为变量，只会作为普通文本保留。

如果变量对应的值是对象或数组，会自动转成 JSON 字符串放入 Prompt。

## 输出结果

模型返回的 `output` 会写入“新增字段”。

如果模型输出是对象或数组，算子会转成 JSON 字符串写入，便于写表和后续节点继续使用。

## 使用示例

```markdown
System Prompt：

你是一个广告诊断场景的 State 数据模拟器。
只输出最终 JSON，不要输出解释、Markdown 或代码块。
```

```markdown
User Prompt：

请根据下面提供的 State 模板，生成一份符合字段口径的模拟 State 数据。

要求：
1. 字段名必须和模板一致。
2. 数组长度、百分比、枚举值等格式要求必须遵守模板说明。

State 模板：
{{state_template}}
```

## 注意事项

- `{{}}` 用于输出变量，`{% %}` 用于 `if` / `for` 等控制语句，单大括号 `{}` 会作为普通文本保留。
- `User Prompt` 必填，`System Prompt` 可为空。
- 新配置页不需要提交旧的 `prompt`、`prompt_template`、`prompt_field` 字段。
- 变量字段不存在时，当前记录会执行失败。
- 模型调用是异步任务，算子会自动提交任务并轮询结果。
- 轮询超时或模型返回失败时，当前记录会执行失败。
- 建议要求模型“只输出最终结果”，避免输出解释文本影响后续解析。
