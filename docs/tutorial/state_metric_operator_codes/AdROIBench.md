# AdROIBench 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `AdROIBench`
- `operatorNameCn`: `ROI同行数据`
- `groupName`: `roi`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/roi_compare.py:107`
- DF handler：`metric_ad_roi_bench`
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
def metric_ad_roi_bench(state_data, tool_input, id_key, id_value, start_date, end_date):
    """
    AdROIBench（ROI同行数据）：
    - 计划ID：取 ad_state[*].ad_roi（按日期区间均值）与 world_state.bench_roi 对比
    - 账户ID：取 adv_state[*].adv_roi（按日期区间均值）与 world_state.bench_roi 对比
    返回：消息内容字符串
    """
    world_state = state_data.get("world_state", {}) or {}
    bench = world_state.get("bench_roi")
    bench_val = float(bench) if isinstance(bench, (int, float)) else 0.0
    bench_list = None
    if isinstance(bench, (list, tuple)):
        values = [float(x) for x in bench if isinstance(x, (int, float))]
        bench_list = sorted(values) if values else None

    def compare(cur_value):
        if bench_list:
            p = bisect.bisect_right(bench_list, cur_value) / float(len(bench_list))
            if p >= 0.5:
                return "高于", p * 100.0
            return "低于", (1.0 - p) * 100.0

        if bench_val == 0:
            return "高于", 0.0
        if cur_value >= bench_val:
            return "高于", (1 - safe_divide(bench_val, cur_value, default=0.0)) * 100.0
        return "低于", (1 - safe_divide(cur_value, bench_val, default=0.0)) * 100.0

    if id_key == "ad_id":
        cur = 0.0
        for ad in state_data.get("ad_state", []) or []:
            if str(ad.get("ad_id", "")).strip() == id_value:
                roi_series = ad.get("ad_roi") or {}
                roi_values = extract_numeric_values_in_range(roi_series, start_date, end_date)
                cur_avg = average(roi_values)
                cur = round(float(cur_avg), 4) if cur_avg is not None else 0.0
                break
        word, pct = compare(cur)
        return f"指标名称:当前ROI及其在同行中的占比, 指标值：计划ID:{id_value}：{cur:.4f} {word}{pct:.2f}%同行"

    if id_key == "adv_id":
        cur = 0.0
        for adv in state_data.get("adv_state", []) or []:
            adv_id = adv.get("adv_id")
            if str(adv_id or "").strip() != id_value:
                continue
            roi_series = adv.get("adv_roi") or {}
            roi_values = extract_numeric_values_in_range(roi_series, start_date, end_date)
            cur_avg = average(roi_values)
            cur = round(float(cur_avg), 4) if cur_avg is not None else 0.0
            break
        word, pct = compare(cur)
        return f"指标名称:当前ROI及其在同行中的占比, 指标值：广告主ID:{id_value}：{cur:.4f} {word}{pct:.2f}%同行"

    raise ValueError(f"Unsupported id for metricCode AdROIBench: {id_value}")
```
