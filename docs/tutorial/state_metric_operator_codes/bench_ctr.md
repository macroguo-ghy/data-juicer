# bench_ctr 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `bench_ctr`
- `operatorNameCn`: `素材 CTR（同行）`
- `groupName`: `materials`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/materials_metrics.py:285`
- DF handler：`metric_bench_ctr`
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
def metric_bench_ctr(state_data, tool_input, id_key, id_value, start_date, end_date):
    """
    bench_ctr（素材 CTR 同行）：
    - 计划ID：ad_material_clicks / ad_material_impressions 对比 bench_material_ctr
    - 账户ID：adv_material_clicks / adv_material_impressions 对比 bench_material_ctr
    返回：消息内容字符串
    """
    world = state_data.get("world_state", {}) or {}
    bench = parse_percent_to_ratio(world.get("bench_material_ctr")) or 0.0

    if id_key == "ad_id":
        ad = next(
            (
                x
                for x in state_data.get("ad_state", []) or []
                if str(x.get("ad_id", "")).strip() == id_value
            ),
            None,
        )
        cur = calc_ratio_from_series(
            (ad or {}).get("ad_material_clicks") or {},
            (ad or {}).get("ad_material_impressions") or {},
            start_date,
            end_date,
        )
        word, pct = calc_bench_compare(cur, bench)
        return f"指标名称:素材点击率（同行）, 指标值：计划ID:{id_value}：{cur * 100:.4f}% {word}{pct:.2f}%（同行均值{bench * 100:.4f}%）"
    if id_key == "adv_id":
        adv = next(
            (
                x
                for x in state_data.get("adv_state", []) or []
                if str(x.get("adv_id", "")).strip() == id_value
            ),
            None,
        )
        cur = calc_ratio_from_series(
            (adv or {}).get("adv_material_clicks") or {},
            (adv or {}).get("adv_material_impressions") or {},
            start_date,
            end_date,
        )
        word, pct = calc_bench_compare(cur, bench)
        return f"指标名称:素材点击率（同行）, 指标值：广告主ID:{id_value}：{cur * 100:.4f}% {word}{pct:.2f}%（同行均值{bench * 100:.4f}%）"
    raise ValueError(f"Unsupported id for metricCode bench_ctr: {id_value}")
```
