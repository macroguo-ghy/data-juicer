# adv_cost_ratio 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `adv_cost_ratio`
- `operatorNameCn`: `账户消耗环比`
- `groupName`: `cost`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/adv_cost_ratio.py:6`
- DF handler：`adv_cost_ratio`
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

from utils.math import calc_sequential_ratio
```

## DF 原始计算代码

```python
def adv_cost_ratio(state_data, tool_input, id_key, id_value, start_date, end_date):
    if id_key == "ad_id":
        return f"指标名称:账号消耗环比, 指标值：广告主ID：{id_value}：调用失败"

    if id_key == "adv_id":
        series_map = None
        for adv_id in state_data.get("adv_state", []):
            if adv_id.get("adv_id", []) == id_value:
                series_map = adv_id.get("adv_cost", [])
                ratio = calc_sequential_ratio(series_map, start_date, end_date)
                if ratio[2] < 0:
                    return f"指标名称:账号消耗环比, 指标值：广告主ID：{id_value}：{ratio[1]}元 环比下降{-ratio[2]*100:.2f}%（上周期{ratio[0]}元）"
                else:
                    return f"指标名称:账号消耗环比, 指标值：广告主ID：{id_value}：{ratio[1]}元 环比上升{ratio[2]*100:.2f}%（上周期{ratio[0]}元）"
        return f"指标名称:账号消耗环比, 指标值：广告主ID：{id_value}：调用失败"

    raise ValueError(f"Unsupported id for metricCode EcomUserGroupLabel: {id_value}")
```
