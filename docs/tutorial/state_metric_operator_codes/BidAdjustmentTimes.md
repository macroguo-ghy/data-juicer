# BidAdjustmentTimes 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `BidAdjustmentTimes`
- `operatorNameCn`: `是否频繁调整出价`
- `groupName`: `ad_flags`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py:14`
- DF handler：`metric_bid_adjustment_times`
- Data-Juicer 目标入口：`operatorCode.calculate(...)`

## Data-Juicer operatorCode 要求

同步到 ADC 元数据平台时，`operatorCode` 必须定义 `calculate(...)`。推荐写法：

```python
def calculate(state, id_value, start_date=None, end_date=None, helpers=None):
    id_key = helpers.get_id_key(state, id_value)
    ...
```

其中 `id_value`、`start_date`、`end_date` 需要在 `inputParameter.params` 和 YAML `parameter_mapping` 中显式配置；`id_key` 通过 `helpers.get_id_key(state, id_value)` 获取，不再作为形参注入。

下面保留的是 Dataset Factory 真实计算代码，迁移时需要把 `state_data` 改为 `state`，把公共数学函数改为 `helpers.xxx(...)`，并移除对 Dataset Factory 包路径的运行时依赖。

## DF 模块依赖上下文

```python
from ..query_metric_data import get_main_ad_ids, register_metric

def _to_yes_no(value):
    s = str(value).strip()
    if s in ("是", "1", "true", "True", "yes", "YES"):
        return "是"
    if s in ("否", "0", "false", "False", "no", "NO"):
        return "否"
    return "否"
```

## DF 原始计算代码

```python
def metric_bid_adjustment_times(
    state_data, tool_input, id_key, id_value, start_date, end_date
):
    """
    BidAdjustmentTimes（是否频繁调整出价）：
    - 计划ID：取 ad_state[*].is_bid_frequently_adjusted
    - 账户ID：遍历主投计划 ad_id，逐个取值拼接
    返回：消息内容字符串
    """

    def get_value(ad_id):
        for ad in state_data.get("ad_state", []) or []:
            if str(ad.get("ad_id", "")).strip() == ad_id:
                return _to_yes_no(ad.get("is_bid_frequently_adjusted"))
        return "否"

    def item(ad_id, v):
        return f"计划ID:{ad_id}：{v}"

    if id_key == "ad_id":
        v = get_value(id_value)
        return f"指标名称:是否频繁调整出价, 指标值：{item(id_value, v)}"

    if id_key == "adv_id":
        items = [item(ad_id, get_value(ad_id)) for ad_id in get_main_ad_ids(state_data)]
        return f"指标名称:是否频繁调整出价, 指标值：{'；'.join(items)}"

    raise ValueError(f"Unsupported id for metricCode BidAdjustmentTimes: {id_value}")
```
