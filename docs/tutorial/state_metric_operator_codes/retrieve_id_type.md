# retrieve_id_type 计算代码

- `operatorType`: `tool`
- `toolName`: `retrieve_id_type`
- `toolNameCn`: `识别ID类型`
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `retrieve_id_type`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/retrieve_id_type.py:6`
- DF handler：`retrieve_id_type`
- DF tool_input 构造：`{"unknownTypeIDs": [id_value]}`
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
from . import register_tool_handler

import json
```

## DF 原始计算代码

```python
def retrieve_id_type(state_data, tool_input):
    result = []
    for ID in tool_input.get("unknownTypeIDs", []):
        id = ID
        id_type = ""
        for i in state_data.get("ad_state", []):
            if i.get("ad_id") == id:
                id_type = "ad_id"
        for i in state_data.get("adv_state", []):
            if i.get("adv_id") == id:
                id_type = "adv_id"
        for i in state_data.get("material_state", []):
            if i.get("material_id") == id:
                id_type = "material_id"
        if id_type == "ad_id":
            for i in state_data.get("ad_state", []):
                if i.get("ad_id") == id:
                    retrieve_id_type = i.get("retrieve_id_type", "计划ID")
                    result.append(f"ID[{id}]是{retrieve_id_type}")
        elif id_type == "adv_id":
            result.append(f"ID[{id}]是账户ID")
        elif id_type == "material_id":
            result.append(f"ID[{id}]是素材ID")
        else:
            return {"BaseResp": {"StatusCode": "1", "StatusMessage": "id_type错误"}}
    Result = {"BaseResp": {"StatusCode": "0", "StatusMessage": ""}, "result": result}
    return json.dumps(Result, ensure_ascii=False)
```
