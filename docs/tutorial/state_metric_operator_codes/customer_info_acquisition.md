# customer_info_acquisition 计算代码

- `operatorType`: `tool`
- `toolName`: `customer_info_acquisition`
- `toolNameCn`: `客户信息获取`
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `customer_info_acquisition`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/customer_info_acquisition.py:7`
- DF handler：`render_customer_info_list`
- DF tool_input 构造：`{}`
- Data-Juicer 目标入口：`operatorCode.calculate(...)`

## Data-Juicer operatorCode 要求

同步到 ADC 元数据平台时，`operatorCode` 必须定义 `calculate(...)`。推荐签名：

```python
def calculate(state, id_value, helpers=None):
    ...
```

Data-Juicer 当前不会根据 `handlerType` / `handlerName` 自动调用 Dataset Factory handler；这两个字段只作为元信息保留。真正执行逻辑必须写入 `operatorCode.calculate(...)`。

## DF 模块依赖上下文

```python
import json

from . import register_tool_handler
```

## DF 原始计算代码

```python
def render_customer_info_list(state_data, tool_input):
    """
    从 state_data 中提取客户（广告主）信息列表。

    state 结构：
    - customer_state.json：adv_state[*].adv_id / adv_state[*].adv_name
    """
    result = []

    for i in state_data.get("adv_state", []):
        adv_id = i.get("adv_id")
        adv_name = i.get("adv_name")

        result.append(
            f"{{'adv_name':'{adv_name}','account_type':80,'adv_id':'{adv_id}'}}"
        )

    return "; ".join(result)
```
