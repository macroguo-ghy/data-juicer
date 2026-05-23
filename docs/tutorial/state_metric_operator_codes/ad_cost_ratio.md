# ad_cost_ratio 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `ad_cost_ratio`
- `operatorNameCn`: `计划消耗环比`
- `groupName`: `cost`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_cost_ratio.py:6`
- DF handler：`ad_cost_ratio`
- Data-Juicer 目标入口：`operatorCode.calculate(...)`

## Data-Juicer operatorCode 要求

同步到 ADC 元数据平台时，`operatorCode` 必须定义 `calculate(...)`。推荐签名：

```python
def calculate(state, id_value, id_key, start_date=None, end_date=None, helpers=None):
    ...
```

下面保留的是 Dataset Factory 真实计算代码，迁移时需要把 `state_data` 改为 `state`，把公共数学函数改为 `helpers.xxx(...)`，并移除对 Dataset Factory 包路径的运行时依赖。

## DF 模块依赖上下文

```python
from ..query_metric_data import get_main_ad_ids, register_metric

from utils.math import calc_sequential_ratio
```

## DF 原始计算代码

```python
def ad_cost_ratio(state_data, tool_input, id_key, id_value, start_date, end_date):
    if id_key == "adv_id":
        return f"指标名称:计划消耗环比, 指标值：计划ID：{id_value}：调用失败"

    if id_key == "ad_id":
        series_map = None
        for ad_id in state_data.get("ad_state", []):
            if ad_id.get("ad_id", []) == id_value:
                series_map = ad_id.get("ad_cost", [])
                ratio = calc_sequential_ratio(series_map, start_date, end_date)
                if ratio[2] < 0:
                    return f"指标名称:计划消耗环比, 指标值：计划ID：{id_value}：{ratio[1]}元 环比下降{-ratio[2]*100:.2f}%（上周期{ratio[0]}元）"
                else:
                    return f"指标名称:计划消耗环比, 指标值：计划ID：{id_value}：{ratio[1]}元 环比上升{ratio[2]*100:.2f}%（上周期{ratio[0]}元）"
        return f"指标名称:计划消耗环比, 指标值：计划ID：{id_value}：调用失败"

    raise ValueError(f"Unsupported id for metricCode EcomUserGroupLabel: {id_value}")
```
