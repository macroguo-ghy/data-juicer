# send_insight_report 计算代码

- `operatorType`: `tool`
- `toolName`: `send_insight_report`
- `toolNameCn`: `发送洞察报告`
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `send_insight_report`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/send_insight_report.py:5`
- DF handler：`send_insight_report`
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
from . import register_tool_handler
```

## DF 原始计算代码

```python
def send_insight_report(state_data, tool_input):
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
        return '{"BaseResp":{"StatusCode":0,"StatusMessage":"false"},"extra":{"has_action":"","replace_text":"","deleted_str":"","action_list":""},"result":"返回调用失败，仅查询到相关material_id，无法发送报告"}'
    elif id_type == "adv_id":
        return '{"BaseResp":{"StatusCode":0,"StatusMessage":"true"},"extra":{"has_action":"","replace_text":"","deleted_str":"","action_list":""},"result":"工具调用成功！《店铺体验分》报告已发送到客户对话框, 回复客户的文案为：【为您量身定制专属成长报告，点击查收 \\n 老板你好，商家体验分会直接影响到我们的投流跟整体流量，体验分越高流量就越好，咱们就越有机会会出爆品赚钱呢~ 具体内容可以看看为您制作的这份专属指南。 \\n 】"}'
    elif id_type == "material_id":
        return '{"BaseResp":{"StatusCode":0,"StatusMessage":"false"},"extra":{"has_action":"","replace_text":"","deleted_str":"","action_list":""},"result":"返回调用失败，仅查询到相关material_id，无法发送报告"}'
    else:
        return '{"BaseResp":{"StatusCode":0,"StatusMessage":"false"},"extra":{"has_action":"","replace_text":"","deleted_str":"","action_list":""},"result":"返回调用失败，仅查询到相关material_id，无法发送报告"}'
```
