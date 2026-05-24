# ad_plan_diagnosis_sec 计算代码

- `operatorType`: `tool`
- `toolName`: `ad_plan_diagnosis_sec`
- `toolNameCn`: `广告计划诊断`
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `ad_plan_diagnosis_sec`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/ad_plan_diagnosis_sec.py:7`
- DF handler：`ad_plan_diagnosis_sec`
- DF tool_input 构造：`{"objectId": id_value}`
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
def ad_plan_diagnosis_sec(state_data, tool_input):
    """
    广告计划诊断工具，根据计划ID(objectId)查询对应计划的诊断数据。

    入参:
        state_data: 包含 ad_state 列表的全局状态数据
        tool_input: 工具输入参数，包含 diagnoseTime 和 objectId
            - diagnoseTime: 诊断时间（暂不影响逻辑）
            - objectId: 计划ID，用于在 ad_state 中匹配 ad_id

    出参:
        str: 匹配到的计划的 ad_plan_diagnosis_sec 字段数据（JSON字符串），
             若未匹配到计划ID则抛出 ValueError

    依赖:
        state_data["ad_state"] 中每个元素需包含 ad_id 字段
    """
    id = tool_input.get("objectId")
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
    if id_type == "adv_id":
        return "返回调用失败"
    elif id_type == "ad_id":
        # 在 ad_state 中查找匹配的计划ID
        for ad in state_data.get("ad_state", []) or []:
            cur_ad_id = ad.get("ad_id")
            if str(cur_ad_id or "").strip() != id:
                continue

            # 找到匹配的计划，返回 ad_plan_diagnosis_sec 字段
            diagnosis = ad.get("ad_plan_diagnosis_sec")
            if diagnosis is None:
                return ""
            if isinstance(diagnosis, str):
                return diagnosis
            return json.dumps(diagnosis, ensure_ascii=False)

    elif id_type == "material_id":
        return "返回调用失败"
    else:
        return "返回调用失败，错误类型"
```
