# Python 脚本处理节点配置页使用说明

Python 脚本处理节点用于对每条样本执行一段 Python 代码，并把脚本返回的结果作为当前样本的新输出。

## 字段说明

| 配置项 | 说明 |
| --- | --- |
| 入口函数 | Python 代码中的函数名，默认建议使用 `process`。 |
| 并行进程数 | 算子执行并发数，`-1` 表示自动计算。 |
| Python 处理代码 | 用户编写的数据处理逻辑。 |

## 入口函数规范

函数签名固定为：

```python
def process(sample, context):
    return sample
```

参数含义：

- `sample`：当前完整样本，可以读取、修改或新增字段。
- `context`：平台上下文，一般不需要使用。

返回值：

- 必须返回一个 `dict`。
- 返回的 `dict` 会作为当前样本的最终输出。

## 使用示例：新增字段

```python
def process(sample, context):
    sample["new_field"] = "hello"
    return sample
```

## 使用示例：根据已有字段生成新字段

```python
def process(sample, context):
    title = sample.get("title") or ""
    body = sample.get("body") or ""
    sample["summary_input"] = title + "\n" + body
    return sample
```

## 使用示例：解析 JSON 字符串

```python
import json

def process(sample, context):
    state = sample.get("state")
    state_obj = json.loads(state) if isinstance(state, str) else state
    sample["state_obj"] = state_obj
    return sample
```

## 注意事项

- 必须返回完整的 `sample` 字典，不能只返回新增字段的值。
- 如果只想新增字段，建议在原 `sample` 上赋值后返回。
- 脚本抛异常表示当前记录处理失败。
- 复杂对象写入表时可能遇到字段类型或顺序问题，必要时可以转成 JSON 字符串。
- 该节点不做沙箱隔离，只适合执行可信代码。
