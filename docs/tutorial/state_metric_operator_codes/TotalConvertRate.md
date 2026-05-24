# TotalConvertRate 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `TotalConvertRate`
- `operatorNameCn`: `直播整体转化率同行数据`
- `groupName`: `live`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/live_metrics.py:382`
- DF handler：`metric_total_convert_rate_bench`
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
    calc_sequential_stats_for_fraction,
    parse_duration_seconds,
    parse_percent_to_ratio,
    resolve_date_range_from_series,
    sum_numeric_values_in_range,
)

from ..query_metric_data import get_main_ad_ids, register_metric

def _format_sequential_rate_item(ad_id, cur, ratio, prev):
    """环比格式化（率类指标），cur/prev 以百分比展示。"""
    direction = "上升" if (ratio or 0) >= 0 else "下降"
    diff_pct = abs((ratio or 0) * 100.0)
    prev_val = 0.0 if prev is None else float(prev)
    cur_val = 0.0 if cur is None else float(cur)
    return f"计划ID:{ad_id}：{cur_val * 100:.4f}% 环比{direction}{diff_pct:.2f}%（上周期{prev_val * 100:.4f}%）"

def _get_ad_item(state_data, ad_id):
    for ad in state_data.get("ad_state", []) or []:
        if str(ad.get("ad_id", "")).strip() == ad_id:
            return ad
    return None
```

## DF 原始计算代码

```python
def metric_total_convert_rate_bench(
    state_data, tool_input, id_key, id_value, start_date, end_date
):
    """
    TotalConvertRate（直播整体转化率同行数据）：
    - 计划ID：ad_live_product_orders / ad_live_impressions 对比 bench_live_conversion_rate
    - 账户ID：遍历主投计划 ad_id 逐个计算并拼接
    返回：消息内容字符串
    """
    world = state_data.get("world_state", {}) or {}
    bench = parse_percent_to_ratio(world.get("bench_live_conversion_rate")) or 0.0

    def calc_rate(ad_id):
        ad = _get_ad_item(state_data, ad_id) or {}
        return calc_ratio_from_series(
            ad.get("ad_live_product_orders") or {},
            ad.get("ad_live_impressions") or {},
            start_date,
            end_date,
        )

    def item(ad_id, cur):
        word, pct = calc_bench_compare(cur, bench)
        return f"计划ID:{ad_id}：{cur * 100:.4f}% {word}{pct:.2f}%（同行均值{bench * 100:.4f}%）"

    if id_key == "ad_id":
        return f"指标名称:整体转化率（对比同行）, 指标值：{item(id_value, calc_rate(id_value))}"
    if id_key == "adv_id":
        items = [item(ad_id, calc_rate(ad_id)) for ad_id in get_main_ad_ids(state_data)]
        return f"指标名称:整体转化率（对比同行）, 指标值：{'；'.join(items)}"
    raise ValueError(f"Unsupported id for metricCode TotalConvertRate: {id_value}")
```
