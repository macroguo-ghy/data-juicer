# review_exception_diagnosis_and_promption 计算代码

- `operatorType`: `tool`
- `toolName`: `review_exception_diagnosis_and_promption`
- `toolNameCn`: `审核异常诊断与申诉`
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `review_exception_diagnosis_and_promption`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/review_exception_diagnosis_and_promption.py:5`
- DF handler：`review_exception_diagnosis_and_promption`
- DF tool_input 构造：`{"properties": {"objectId": {"type": id_value}, "advId": {}}}`
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
```

## DF 原始计算代码

```python
def review_exception_diagnosis_and_promption(state_data, tool_input):
    tool_input = tool_input.get("properties", {})
    objectid = tool_input.get("objectId", [])
    advid = tool_input.get("advId", [])
    if not advid:
        for ad_id in state_data.get("ad_state", []):
            if ad_id.get("ad_id") == objectid.get("type", []):
                return ad_id.get("review_exception_diagnosis_and_promotion")
        return "查询失败"
    else:
        for ad_id in state_data.get("ad_state", []):
            if ad_id.get("ad_id") == objectid.get("type", []) and ad_id.get(
                "related_adv_id"
            ) == advid.get("type", []):
                return ad_id.get("review_exception_diagnosis_and_promotion")
        return "查询失败"
```
