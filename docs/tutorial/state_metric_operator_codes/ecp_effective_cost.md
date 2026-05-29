# ecp_effective_cost 计算代码

- `operatorType`: `metric`
- `operatorNameEn`: `ecp_effective_cost`
- `operatorNameCn`: `千川有效消耗门槛`
- `groupName`: `ecp`
- DF 来源：`/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ecp.py:75`
- DF handler：`ecp_effective_cost`
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

import datetime

def extract_numeric_values_in_range(series_map, start_date, end_date):
    """从 series_map 中提取指定日期区间内的数值列表（忽略非数字/非法日期）。"""
    if not series_map:
        return []
    values = []
    for k, v in series_map.items():
        try:
            d = datetime.date.fromisoformat(str(k))
        except Exception:
            continue
        if start_date and d < start_date:
            continue
        if end_date and d > end_date:
            continue
        if isinstance(v, (int, float)):
            values.append(float(v))
    return values

def calc_sequential_ratio(series_map, start_date, end_date):
    date_keys = []
    for k in (series_map or {}).keys():
        try:
            date_keys.append(datetime.date.fromisoformat(str(k)))
        except Exception:
            continue
    date_keys = sorted(set(date_keys))
    if not date_keys:
        return None

    current_end = end_date or date_keys[-1]
    current_start = start_date or current_end

    days = (current_end - current_start).days + 1
    if days <= 0:
        return None

    prev_end = current_start - datetime.timedelta(days=1)
    prev_start = current_start - datetime.timedelta(days=days)

    cur_values = extract_numeric_values_in_range(series_map, current_start, current_end)
    prev_values = extract_numeric_values_in_range(series_map, prev_start, prev_end)

    cur_sum = sum(cur_values) if cur_values else None
    prev_sum = sum(prev_values) if prev_values else None

    cur_sum = round(cur_sum, 4) if cur_sum is not None else None
    prev_sum = round(prev_sum, 4) if prev_sum is not None else None

    if cur_sum is None or prev_sum is None or prev_sum == 0:
        return 0.0

    return [prev_sum, cur_sum]

def ecp_pc_cost_ratio(state_data, tool_input, id_key, id_value, start_date, end_date):
    if id_key == "ad_id":
        return f"指标名称:千川PC消耗占比, 指标值：广告主ID：{id_value}：调用失败"

    if id_key == "adv_id":
        for adv_id in state_data.get("adv_state", []):
            if adv_id["adv_id"] == id_value:
                return f"指标名称:千川PC消耗占比, 指标值：广告主ID：{id_value}：{adv_id['ecp_pc_cost_ratio']}"
        return f"指标名称:千川PC消耗占比, 指标值：广告主ID：{id_value}：调用失败"

    raise ValueError(f"Unsupported id for metricCode EcomUserGroupLabel: {id_value}")

def ecp_balance_enough(state_data, tool_input, id_key, id_value, start_date, end_date):
    id_search = id_value
    if id_key == "ad_id":
        for ad_id in state_data.get("ad_state", []):
            if ad_id["ad_id"] == id_value:
                id_search = ad_id.get("related_adv_id", [])

    for adv_id in state_data.get("adv_state", []):
        if adv_id.get("adv_id", []) == id_search:
            return f"指标名称:账户余额是否充足, 指标值：广告主ID：{id_search}：{adv_id['is_low_adv_balance']}"
    return f"指标名称:账户余额是否充足, 指标值：广告主ID：{id_value}：调用失败"
```

## DF 原始计算代码

```python
def ecp_effective_cost(state_data, tool_input, id_key, id_value, start_date, end_date):
    if id_key == "ad_id":
        for ad_id in state_data.get("ad_state", []):
            if ad_id.get("ad_id", []) == id_value:
                ad_gmv_series_map = ad_id.get("ad_gmv", [])
                ad_pay_counts_series_map = ad_id.get("ad_pay_counts", [])
                ad_gmv_result = calc_sequential_ratio(
                    ad_gmv_series_map, start_date, end_date
                )[1]
                ad_pay_counts_result = calc_sequential_ratio(
                    ad_pay_counts_series_map, start_date, end_date
                )[1]

                result = 3 * ad_gmv_result / ad_pay_counts_result
                return (
                    f"指标名称:千川有效消耗, 指标值：计划ID：{id_value}：{result:.2f}元"
                )
        return f"指标名称:千川有效消耗, 指标值：计划ID：{id_value}：调用失败"

    if id_key == "adv_id":
        for adv_id in state_data.get("adv_state", []):
            if adv_id.get("adv_id", []) == id_value:
                adv_gmv_series_map = adv_id.get("adv_gmv", [])
                adv_pay_counts_series_map = adv_id.get("adv_pay_counts", [])
                adv_gmv_result = calc_sequential_ratio(
                    adv_gmv_series_map, start_date, end_date
                )[1]
                adv_pay_counts_result = calc_sequential_ratio(
                    adv_pay_counts_series_map, start_date, end_date
                )[1]

                result = 3 * adv_gmv_result / adv_pay_counts_result
                return f"指标名称:千川有效消耗, 指标值：广告主ID：{id_value}：{result:.2f}元"
        return f"指标名称:千川有效消耗, 指标值：广告主ID：{id_value}：调用失败"

    raise ValueError(f"Unsupported id for metricCode EcomUserGroupLabel: {id_value}")
```
