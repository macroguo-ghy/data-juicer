# merchant_score 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `merchant_score`
- `operatorNameCn`: `店铺体验分`
- `groupName`: `materials`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:37`
- DF handler：`metric_merchant_score`
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
def metric_merchant_score(
    state_data, tool_input, id_key, id_value, start_date, end_date
):
    """
    merchant_score（店铺体验分）：
    - 计划ID：取计划下 shop_experience_score 并拼消息
    - 账户ID：遍历主投计划 ad_id，对每个计划取 shop_experience_score 并拼多条消息
    返回：消息内容字符串
    """

    def get_score_by_ad_id(ad_id):
        score = None
        for ad in state_data.get("ad_state", []) or []:
            if str(ad.get("ad_id", "")).strip() != ad_id:
                continue
            score = ad.get("shop_experience_score")
            break
        return score

    def format_item(ad_id, score_value):
        score_str = "0.0000" if score_value is None else str(score_value)
        return f"计划ID:{ad_id}：{score_str}"

    if id_key == "ad_id":
        score_value = get_score_by_ad_id(id_value)
        return f"指标名称:店铺体验分, 指标值：{format_item(id_value, score_value)}"

    if id_key == "adv_id":
        items = []
        for ad_id in get_main_ad_ids(state_data):
            score_value = get_score_by_ad_id(ad_id)
            items.append(format_item(ad_id, score_value))
        return f"指标名称:店铺体验分, 指标值：{'；'.join(items)}"

    raise ValueError(f"Unsupported id for metricCode merchant_score: {id_value}")
```
