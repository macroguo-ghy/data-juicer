# sequential_roi 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `sequential_roi`
- `operatorNameCn`: `ROI数据`
- `groupName`: `roi`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/roi_compare.py:58`
- DF handler：`metric_sequential_roi`
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
import bisect

from utils.math import average, calc_bench_compare, calc_sequential_ratio, extract_numeric_values_in_range, safe_divide

from ..query_metric_data import register_metric
```

## DF 原始计算代码

```python
def metric_sequential_roi(
    state_data, tool_input, id_key, id_value, start_date, end_date
):
    """
    sequential_roi（当前ROI环比）：
    - 计划ID：取 ad_state[*].ad_roi 的时间序列，按入参日期计算环比
    - 账户ID：取 adv_state[*].adv_roi 的时间序列，按入参日期计算环比
    返回：消息内容字符串
    """

    series_map = None
    if id_key == "ad_id":

        for ad in state_data.get("ad_state", []) or []:
            if str(ad.get("ad_id", "")).strip() != id_value:
                continue
            series_map = ad.get("ad_roi") or {}
            break
        series_map = series_map or {}
        ratio = calc_sequential_ratio(series_map, start_date, end_date)
        ratio[2] = float(ratio[2]) if isinstance(ratio[2], (int, float)) else 0.0
        if ratio[2] < 0:
            return f"指标名称：当前ROI（环比） , 指标值：计划ID:{id_value}；上周ROI平均值：{ratio[0]}；这周ROI平均值：{ratio[1]}；这周相比于上周环比下降：{-ratio[2]*100:.4f}%"
        if ratio[2] >= 0:
            return f"指标名称：当前ROI（环比） , 指标值：计划ID:{id_value}；上周ROI平均值：{ratio[0]}；这周ROI平均值：{ratio[1]}；这周相比于上周环比上升：{ratio[2]*100:.4f}%"

    if id_key == "adv_id":

        for adv in state_data.get("adv_state", []) or []:
            adv_id = adv.get("adv_id")
            if adv_id is None:
                meta = adv.get("meta_data", {}) or {}
                adv_id = meta.get("adv_id")
            if str(adv_id or "").strip() != id_value:
                continue
            series_map = adv.get("adv_roi") or {}
            break
        series_map = series_map or {}
        ratio = calc_sequential_ratio(series_map, start_date, end_date)
        ratio[2] = float(ratio[2]) if isinstance(ratio[2], (int, float)) else 0.0
        if ratio[2] < 0:
            return f"指标名称：当前ROI（环比） , 指标值：计划ID:{id_value}；上周ROI平均值：{ratio[0]}；这周ROI平均值：{ratio[1]}；这周相比于上周环比下降：{-ratio[2]*100:.4f}%"
        if ratio[2] >= 0:
            return f"指标名称：当前ROI（环比） , 指标值：计划ID:{id_value}；上周ROI平均值：{ratio[0]}；这周ROI平均值：{ratio[1]}；这周相比于上周环比上升：{ratio[2]*100:.4f}%"

    raise ValueError(f"Unsupported id for metricCode sequential_roi: {id_value}")
```
