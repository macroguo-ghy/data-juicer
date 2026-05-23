# get_industry_creative_tips 计算代码

- `operatorType`: `tool`
- `toolName`: `get_industry_creative_tips`
- `toolNameCn`: `行业创意建议`
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `get_industry_creative_tips`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/get_industry_creative_tips.py:7`
- DF handler：`get_industry_creative_tips`
- DF tool_input 构造：`{"advId": id_value}`
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
def get_industry_creative_tips(state_data, tool_input):
    id = tool_input.get("advId")
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
        return '{"BaseResp":{"StatusCode":0,"StatusMessage":"false"},"extra":{"has_action":"","replace_text":"","deleted_str":"","action_list":""},"result":"返回调用失败，仅查询到相关ad_id，无法发送报告"}'
    elif id_type == "adv_id":
        for adv in state_data.get("adv_state", []) or []:
            cur_adv_id = adv.get("adv_id")
            if cur_adv_id is None:
                meta = adv.get("meta_data", {}) or {}
                cur_adv_id = meta.get("adv_id")
            if str(cur_adv_id or "").strip() != id:
                continue

            tips = adv.get("get_industry_creative_tips")
            if tips is None:
                return ""
            if isinstance(tips, str):
                return tips
            result = json.dumps(tips, ensure_ascii=False)
            return f'{{"BaseResp":{{"StatusCode":0,"StatusMessage":"true"}},"extra":{{"has_action":"","replace_text":"","deleted_str":"","action_list":""}},"result":"{result}"}}'
    elif id_type == "material_id":
        return '{"BaseResp":{"StatusCode":0,"StatusMessage":"false"},"extra":{"has_action":"","replace_text":"","deleted_str":"","action_list":""},"result":"返回调用失败，仅查询到相关material_id，无法发送报告"}'
    else:
        return '{"BaseResp":{"StatusCode":0,"StatusMessage":"false"},"extra":{"has_action":"","replace_text":"","deleted_str":"","action_list":""},"result":"返回调用失败，错误类型"}'
```
