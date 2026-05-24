# get_douplus_order_detail 计算代码

- `operatorType`: `tool`
- `toolName`: `get_douplus_order_detail`
- `toolNameCn`: `订单详情查询`
- `groupName`: `aux_tools`
- `handlerType`: `builtin`
- `handlerName`: `get_douplus_order_detail`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/get_douplus_order_detail.py:5`
- DF handler：`get_douplus_order_detail`
- DF tool_input 构造：`{"order_id": id_value}`
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
def get_douplus_order_detail(state_data, tool_input):
    id = tool_input.get("objectId")
    for ad in state_data.get("ad_state", []):
        if str(ad.get("ad_id")) == str(id):
            ad_status = ad.get("ad_status")
            ad_created_time = ad.get("ad_created_time")
            ad_budget = ad.get("ad_budget")
            PrimaryTargetName = ad.get("PrimaryTargetName")
            SecondaryTargetName = ad.get("SecondaryTargetName")
            is_can_cancel = ad.get("is_can_cancel")
            return f"""订单ID：{id}，订单信息查询结果如下：
订单状态={ad_status}，订单创建时间={ad_created_time}，订单预算={ad_budget}元，订单一级投放目标={PrimaryTargetName}，订单二级投放目标={SecondaryTargetName}，{is_can_cancel}。"""
    return "返回调用失败"
```
