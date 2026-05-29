# GetAdROIAndRecommendedValues 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `GetAdROIAndRecommendedValues`
- `operatorNameCn`: `当前ROI对比推荐ROI`
- `groupName`: `roi`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/roi_compare.py:8`
- DF handler：`metric_get_ad_roi_and_recommended_values`
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
import bisect

from utils.math import average, calc_bench_compare, calc_sequential_ratio, extract_numeric_values_in_range, safe_divide

from ..query_metric_data import register_metric
```

## DF 原始计算代码

```python
def metric_get_ad_roi_and_recommended_values(
    state_data, tool_input, id_key, id_value, start_date, end_date
):
    """
    GetAdROIAndRecommendedValues（当前ROI对比推荐ROI）：
    - 计划ID：取 ad_state[*].ad_roi（按日期区间均值）、sys_recommended_roi、customer_set_roi
    - 账户ID：遍历主投计划 ad_id，逐个计算并拼接
    返回：消息内容字符串
    """

    def build_for_ad(ad):
        cust_roi = ad.get("customer_set_roi")
        cur = float(cust_roi) if isinstance(cust_roi, (int, float)) else 0.0
        rec = ad.get("sys_recommended_roi")
        cust = ad.get("customer_set_roi")
        rec_val = float(rec) if isinstance(rec, (int, float)) else 0.0
        pct = 0.0 if rec_val == 0 else (cur - rec_val) / rec_val * 100.0
        cmp_word = "高于推荐" if pct >= 0 else "低于推荐"
        diff_abs = abs(pct)
        cust_val = float(cust) if isinstance(cust, (int, float)) else 0.0
        if cust is None:
            return cur, rec_val, cust_val, cmp_word, diff_abs, False
        return cur, rec_val, cust_val, cmp_word, diff_abs, True

    def item(ad_id, cur, rec, cust, cmp_word, diff_abs, has_cust):
        return f"计划ID:{ad_id}：{cur:.4f} {cmp_word}{diff_abs:.2f}%（系统推荐值{rec:.4f}）"

    if id_key == "ad_id":
        for ad in state_data.get("ad_state", []) or []:
            if str(ad.get("ad_id", "")).strip() == id_value:
                cur, rec, cust, cmp_word, diff_abs, has_cust = build_for_ad(ad)
                return f"指标名称:计划ROI和推荐ROI, 指标值：{item(id_value, cur, rec, cust, cmp_word, diff_abs, has_cust)}"
        return f"指标名称:计划ROI和推荐ROI, 指标值：{item(id_value, 0.0, 0.0, 0.0, '低于推荐', 0.0, False)}"

    if id_key == "adv_id":
        items = []
        for ad in state_data.get("ad_state", []) or []:
            if str(ad.get("is_main_ad", "")).strip() != "是":
                continue
            ad_id = str(ad.get("ad_id", "")).strip()
            cur, rec, cust, cmp_word, diff_abs, has_cust = build_for_ad(ad)
            items.append(item(ad_id, cur, rec, cust, cmp_word, diff_abs, has_cust))
        return f"指标名称:计划ROI和推荐ROI, 指标值：{'；'.join(items)}"

    raise ValueError(
        f"Unsupported id for metricCode GetAdROIAndRecommendedValues: {id_value}"
    )
```
