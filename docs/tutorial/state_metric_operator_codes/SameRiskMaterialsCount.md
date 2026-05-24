# SameRiskMaterialsCount 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `SameRiskMaterialsCount`
- `operatorNameCn`: `同质化素材数`
- `groupName`: `materials`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:197`
- DF handler：`metric_same_risk_materials_count`
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
from utils.math import (
    calc_bench_compare,
    calc_ratio_from_series,
    calc_sequential_stats,
    calc_sequential_stats_integer,
    calc_sequential_stats_for_fraction,
    parse_percent_to_ratio,
)

from ..query_metric_data import get_main_ad_ids, register_metric

def _format_sequential_message(
    metric_name, id_label, ad_id, cur, ratio, prev, as_percent=False
):
    """
    通用环比格式化。
    - as_percent=True 时，cur/prev 以百分比展示（乘100加%），用于率类指标。
    - as_percent=False 时，cur/prev 以小数展示，用于计数类指标。
    """
    direction = "上升" if (ratio or 0) >= 0 else "下降"
    diff_pct = abs((ratio or 0) * 100.0)
    prev_val = 0.0 if prev is None else float(prev)
    cur_val = 0.0 if cur is None else float(cur)
    if as_percent:
        return (
            f"指标名称:{metric_name}, 指标值：{id_label}:{ad_id}：{cur_val * 100:.4f}% "
            f"环比{direction}{diff_pct:.2f}%（上周期{prev_val * 100:.4f}%）"
        )
    return (
        f"指标名称:{metric_name}, 指标值：{id_label}:{ad_id}：{cur_val:.4f} "
        f"环比{direction}{diff_pct:.2f}%（上周期{prev_val:.4f}）"
    )
```

## DF 原始计算代码

```python
def metric_same_risk_materials_count(
    state_data, tool_input, id_key, id_value, start_date, end_date
):
    """
    SameRiskMaterialsCount（同质化素材数）：
    - 计划ID：ad_homogeneous_materials_count
    - 账户ID：adv_homogeneous_materials_count
    返回：消息内容字符串
    """
    label = "计划ID" if id_key == "ad_id" else "账户ID"
    value = 0.0
    if id_key == "ad_id":
        for ad in state_data.get("ad_state", []) or []:
            if str(ad.get("ad_id", "")).strip() == id_value:
                value = float(ad.get("ad_homogeneous_materials_count") or 0.0)
                break
    elif id_key == "adv_id":
        for adv in state_data.get("adv_state", []) or []:
            if str(adv.get("adv_id", "")).strip() == id_value:
                value = float(adv.get("adv_homogeneous_materials_count") or 0.0)
                break
    else:
        raise ValueError(
            f"Unsupported id for metricCode SameRiskMaterialsCount: {id_value}"
        )
    return f"指标名称:同质素材数, 指标值：{label}:{id_value}：{value:.4f}"
```
