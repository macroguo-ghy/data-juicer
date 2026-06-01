# State Metric Helpers 使用说明

本文档说明 `state_metric_calculator` 指标代码中可通过 `helpers` 调用的公共方法。指标代码入口通常写成：

```python
def calculate(state, unknown_id=None, startDate=None, endDate=None, helpers=None):
    ...
```

其中 `state` 和 `helpers` 由算子运行时注入；其他业务参数需要由元数据参数和 YAML `parameter_mapping` 显式配置。

## 基础约定

### 日期参数

涉及时间范围的方法期望 `start_date` / `end_date` 是 `datetime.date` 类型，或者为 `None`。如果指标代码需要做日期加减，建议先把字符串转换成日期：

```python
import datetime

if isinstance(startDate, str):
    startDate = datetime.date.fromisoformat(startDate)
if isinstance(endDate, str):
    endDate = datetime.date.fromisoformat(endDate)
```

### 序列数据格式

helper 支持两类序列：

- 日期字典：`{"2026-05-01": 10, "2026-05-02": 12}`
- 数组或元组：`[10, 12, 14, 16]`

日期字典会按 `start_date` / `end_date` 过滤日期 key。数组没有日期信息，环比类方法会按前半段作为上周期、后半段作为本周期。

### 返回值风格

部分方法在无法计算时会返回 `None`、`0.0` 或 `(None, None, None)`。指标代码需要显式判断，避免直接解包异常。

```python
result = helpers.calc_sequential_ratio(series_map, startDate, endDate)
if not result:
    return "无法计算"
prev_val, cur_val, ratio = result
```

## ID 识别

### `helpers.get_id_keys(state_data, id_value)`

返回输入 ID 在 `state` 中命中的所有 ID 类型集合。

```python
id_keys = helpers.get_id_keys(state, unknown_id)
```

查找范围：

- `state["ad_state"][*]["ad_id"]`
- `state["adv_state"][*]["adv_id"]`
- `state["adv_state"][*]["meta_data"]["adv_id"]`
- `state["material_state"][*]["material_id"]`

返回示例：

```python
{"ad_id"}
{"adv_id", "ad_id"}
set()
```

### `helpers.get_id_key(state_data, id_value)`

返回输入 ID 的单一 ID 类型。

```python
id_key = helpers.get_id_key(state, unknown_id)
```

优先级：

1. `ad_id`
2. `adv_id`
3. `material_id`

未命中时返回 `None`。

典型用法：

```python
id_key = helpers.get_id_key(state, unknown_id)
if id_key == "adv_id":
    ...
elif id_key == "ad_id":
    ...
else:
    raise ValueError(f"Unsupported id: {unknown_id}")
```

## 时间序列取数

### `helpers.extract_numeric_values_in_range(series_map, start_date, end_date)`

从序列中提取数值。

```python
values = helpers.extract_numeric_values_in_range(series_map, startDate, endDate)
```

行为：

- `series_map` 为空时返回 `[]`
- 数组或元组：返回其中的数字元素，忽略非数字
- 日期字典：只保留日期 key 在范围内、value 为数字的元素
- 无法解析成日期的 key 会被忽略

返回示例：

```python
[10.0, 12.0, 14.0]
```

### `helpers.sum_numeric_values_in_range(series_map, start_date, end_date)`

对指定范围内的数值求和。

```python
total = helpers.sum_numeric_values_in_range(series_map, startDate, endDate)
```

等价于：

```python
sum(helpers.extract_numeric_values_in_range(series_map, startDate, endDate))
```

## 基础数学方法

### `helpers.safe_divide(numerator, denominator, default=0.0)`

安全除法。

```python
ratio = helpers.safe_divide(clicks, impressions)
```

行为：

- 分子或分母无法转成数字时，返回 `default`
- 分母为 0 时，返回 `default`
- 否则返回 `float(numerator) / float(denominator)`

### `helpers.average(values)`

计算平均值。

```python
avg = helpers.average([1, 2, 3])
```

行为：

- 空列表返回 `None`
- 非空列表返回平均值

### `helpers.fmt4(value)`

格式化数字，最多保留 4 位小数，并去掉末尾多余的 0 和小数点。

```python
helpers.fmt4(1.2300)  # "1.23"
helpers.fmt4(1.0)     # "1"
```

## 比例计算

### `helpers.calc_ratio_from_series(numerator_series, denominator_series, start_date, end_date)`

计算两个序列的平均日比例。

```python
ctr = helpers.calc_ratio_from_series(click_series, impression_series, startDate, endDate)
```

行为：

- 数组输入：按位置 `zip`，逐项计算 `numerator / denominator`，再求平均
- 日期字典输入：按分子序列的日期 key 遍历，并用同日期的分母值计算比例
- 分母为 0、非数字或缺失的数据点会被忽略
- 没有有效数据时返回 `0.0`
- 返回值保留 6 位小数

适用场景：

- CTR：`clicks / impressions`
- CVR：`conversions / clicks`
- 其他分子/分母类指标

## 环比计算

### `helpers.calc_sequential_stats(series_map, start_date, end_date)`

计算普通数值序列的本周期均值、上周期均值和环比。

```python
cur_avg, prev_avg, ratio = helpers.calc_sequential_stats(series_map, startDate, endDate)
```

返回：

```python
(cur_avg, prev_avg, ratio)
```

行为：

- 日期字典：当前周期为 `[start_date, end_date]`；上周期为同样天数的前一段时间
- 数组：前半段作为上周期，后半段作为本周期
- `cur_avg` / `prev_avg` 保留 4 位小数
- `ratio = (cur_avg - prev_avg) / prev_avg`，保留 6 位小数
- 无数据时返回 `(None, None, None)`
- 上周期为 0 或缺失时，`ratio` 返回 `0.0`

### `helpers.calc_sequential_stats_integer(series_map, start_date, end_date)`

和 `calc_sequential_stats` 类似，但本周期均值和上周期均值会转成整数。

```python
cur_count, prev_count, ratio = helpers.calc_sequential_stats_integer(
    count_series,
    startDate,
    endDate,
)
```

适用场景：

- 在投素材数
- 调价次数
- 直播场次
- 其他计数类指标

### `helpers.calc_sequential_stats_for_fraction(numerator_series, denominator_series, start_date, end_date)`

计算分子/分母类比例指标的本周期比例、上周期比例和环比。

```python
cur_rate, prev_rate, ratio = helpers.calc_sequential_stats_for_fraction(
    click_series,
    impression_series,
    startDate,
    endDate,
)
```

返回：

```python
(cur_rate, prev_rate, ratio)
```

行为：

- 先分别计算本周期和上周期的平均日比例
- 再计算比例本身的环比
- 上周期比例为 0 时，`ratio` 返回 `0.0`
- 无数据时返回 `(None, None, None)`

适用场景：

- CTR 环比
- 转化率环比
- 观看率环比

### `helpers.calc_sequential_ratio(series_map, start_date, end_date)`

计算单序列环比，并返回更适合直接拼文案的顺序。

```python
result = helpers.calc_sequential_ratio(series_map, startDate, endDate)
if not result:
    return "无法计算"
prev_val, cur_val, change_ratio = result
```

返回：

```python
[prev_value, cur_value, ratio]
```

注意：

- 这个方法返回列表，不是元组。
- 无日期范围或无有效数据时可能返回 `None` 或 `0.0`，调用方应先判断。
- 数组输入时，前半段作为上周期，后半段作为本周期。
- 日期字典输入时，当前周期和上周期的计算规则与 `calc_sequential_stats` 一致。

适用场景：

- 消耗环比
- GMV 环比
- ROI 环比
- 需要输出“本周期值、上周期值、环比变化”的指标

## 同行或基准对比

### `helpers.calc_bench_compare(current_value, bench_value)`

将当前值和同行或基准值比较。

```python
desc, diff_pct = helpers.calc_bench_compare(current_roi, bench_roi)
```

返回：

```python
("高于同行", 12.34)
("低于同行", 8.5)
```

行为：

- `diff_pct = abs((current_value - bench_value) / bench_value * 100)`
- `bench_value` 为 0 时返回 `("高于同行", 0.0)`
- 非数字输入会按 0 处理

## 字符串解析

### `helpers.parse_percent_to_ratio(value)`

把百分比或数字转换成比例。

```python
helpers.parse_percent_to_ratio("2.9%")  # 0.029
helpers.parse_percent_to_ratio(2.9)     # 0.029
helpers.parse_percent_to_ratio(0.029)   # 0.029
```

行为：

- `None` 或空字符串返回 `None`
- 字符串以 `%` 结尾时，去掉 `%` 后除以 100
- 数字或普通数字字符串大于 1 时，按百分数处理并除以 100
- 数字或普通数字字符串小于等于 1 时，认为已经是比例
- 无法解析时返回 `None`

### `helpers.parse_duration_seconds(value)`

解析秒数。

```python
helpers.parse_duration_seconds("12秒")  # 12.0
helpers.parse_duration_seconds(3)       # 3.0
```

行为：

- `None` 或空字符串返回 `None`
- 数字返回 `float(value)`
- 支持 `"12秒"` 这类字符串
- 无法解析时返回 `None`

## 日期范围推断

### `helpers.resolve_date_range_from_series(series_maps, start_date, end_date)`

从一个或多个日期字典中推断日期范围。

```python
start, end = helpers.resolve_date_range_from_series(
    [cost_series, roi_series],
    startDate,
    endDate,
)
```

行为：

- 如果 `start_date` 和 `end_date` 都有值，直接返回原值
- 如果只传 `start_date`，返回 `(start_date, start_date)`
- 如果只传 `end_date`，返回 `(end_date, end_date)`
- 如果两个都没传，会从日期字典 key 中取最大日期，返回 `(max_date, max_date)`
- 数组或元组没有日期 key，会被忽略
- 找不到任何日期时返回 `(None, None)`

适用场景：

- 指标代码没有显式时间范围，但需要从 state 序列里找一个默认查询日
- 多个序列需要共享同一个默认日期

## 不建议直接调用的内部方法

`MetricHelpers` 内部还有以下下划线方法：

- `_sequential_ranges(...)`
- `_sequential_ranges_for_series_maps(...)`
- `_calc_sequential_stats_from_list(...)`
- `_calc_sequential_stats_for_fraction_from_lists(...)`

这些方法是公开 helper 的实现细节，指标 `operatorCode` 不建议直接调用。优先使用上文列出的非下划线方法。

## 完整示例

```python
def calculate(state, unknown_id, startDate=None, endDate=None, helpers=None):
    id_key = helpers.get_id_key(state, unknown_id)

    if id_key == "adv_id":
        for adv in state.get("adv_state", []) or []:
            if str(adv.get("adv_id", "")).strip() != str(unknown_id):
                continue
            result = helpers.calc_sequential_ratio(
                adv.get("adv_cost", {}),
                startDate,
                endDate,
            )
            if not result:
                return f"指标名称:账号消耗环比, 指标值：广告主ID：{unknown_id}：无法计算"

            prev_val, cur_val, ratio = result
            direction = "上升" if ratio >= 0 else "下降"
            return (
                f"指标名称:账号消耗环比, 指标值：广告主ID：{unknown_id}："
                f"{helpers.fmt4(cur_val)}元 环比{direction}{abs(ratio) * 100:.2f}%"
                f"（上周期{helpers.fmt4(prev_val)}元）"
            )

    if id_key == "ad_id":
        for ad in state.get("ad_state", []) or []:
            if str(ad.get("ad_id", "")).strip() != str(unknown_id):
                continue
            cur_avg, prev_avg, ratio = helpers.calc_sequential_stats(
                ad.get("ad_roi", {}),
                startDate,
                endDate,
            )
            if cur_avg is None or prev_avg is None:
                return f"指标名称:计划ROI环比, 指标值：计划ID：{unknown_id}：无法计算"
            direction = "上升" if ratio >= 0 else "下降"
            return (
                f"指标名称:计划ROI环比, 指标值：计划ID：{unknown_id}："
                f"{helpers.fmt4(cur_avg)} 环比{direction}{abs(ratio) * 100:.2f}%"
                f"（上周期{helpers.fmt4(prev_avg)}）"
            )

    raise ValueError(f"Unsupported id: {unknown_id}")
```
