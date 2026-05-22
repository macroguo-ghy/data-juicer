# 代码审核节点配置页使用说明

代码审核节点用于对每条样本执行一段 Python 审核代码，并输出“是否通过”和“失败原因”两个字段。

## 字段说明

| 配置项 | 说明 |
| --- | --- |
| 审核字段 | 主审核字段。该字段的值会作为 `value` 传给入口函数。 |
| 入口函数 | Python 代码中的函数名，默认建议使用 `review_row`。 |
| 结果字段 | 审核结果输出字段，值为 `True` 或 `False`。 |
| 原因字段 | 审核失败原因输出字段，通过时一般为空字符串。 |
| 并行进程数 | 算子执行并发数，`-1` 表示自动计算。 |
| Python 审核代码 | 用户编写的审核逻辑。 |

## 入口函数规范

函数签名固定为：

```python
def review_row(value, row, context):
    return True, ""
```

参数含义：

- `value`：审核字段对应的值，是主审核对象。
- `row`：当前完整样本，可以读取其他字段。
- `context`：平台上下文，一般不需要使用。

返回值：

- 通过：`return True, ""`
- 不通过：`return False, "失败原因"`

## 使用示例

```python
import json

def review_row(value, row, context):
    state = json.loads(value) if isinstance(value, str) else value

    if not isinstance(state, dict):
        return False, "state 必须是对象"

    if "ad_state" not in state:
        return False, "缺少 ad_state"

    return True, ""
```

## 注意事项

- 审核不通过时不要抛异常，应返回 `False, "原因"`。
- 抛异常表示脚本执行失败，不等同于审核不通过。
- 如果需要读取其他字段，可以使用 `row.get("字段名")`。
