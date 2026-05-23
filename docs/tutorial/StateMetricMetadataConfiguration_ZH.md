# State 指标计算元数据配置教程

本文说明 `state_metric_calculator` 算子的元数据应该怎么配置、一个指标最少需要哪些字段、以及当前如何区分 metric 和 tool。

## 1. 配置分层

`state_metric_calculator` 的配置分为两层：

1. Data-Juicer YAML：选择本次要执行哪些指标，并配置样本字段映射。
2. ADC 后端指标元数据：由 `/openapi/state-meta/operators/batch-get` 按 `operator_id` 拉取，提供指标名称、入参声明和 `calculate(...)` 计算代码。

算子不会从 YAML 里读取 `operatorCode` 或 `inputParameter` 快照。YAML 里只放选择结果和字段映射，真实指标口径以接口返回的元数据为准。

## 2. Data-Juicer YAML 配置

最小配置示例：

```yaml
process:
  - state_metric_calculator:
      state_key: state
      id_source_key: issue_id
      output_key: query_metric_data_outputs
      result_mode: summary
      summary_success_only: false
      start_date_key: "客户反馈的问题周期开始时间"
      end_date_key: "客户反馈的问题周期结束时间"
      operators:
        - operator_id: 201
          parameter_mapping:
            bench_roi: bench_roi
      ctx:
        apiBase: "https://example.bytedance.net/api"
        userAccount: "zhangsan"
```

字段说明：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `state_key` | 否 | State 所在样本字段，默认 `state`。 |
| `id_source_key` | 否 | 样本里的公共 ID 字段，支持逗号分隔多个 ID。 |
| `output_key` | 否 | 输出字段，默认 `query_metric_data_outputs`。 |
| `result_mode` | 否 | 输出模式，支持 `summary` 和 `object`，默认 `summary`。 |
| `summary_success_only` | 否 | 仅 `summary` 模式生效；默认 `false` 保留成功和失败结果，`true` 时只输出 DF 成功字段。 |
| `start_date_key` | 否 | 样本里的起始日期字段，供 `calculate(..., start_date, ...)` 使用。 |
| `end_date_key` | 否 | 样本里的结束日期字段，供 `calculate(..., end_date, ...)` 使用。 |
| `operators` | 是 | 本次要执行的指标列表。 |
| `operators[].operator_id` | 是 | 后端指标元数据 ID。 |
| `operators[].parameter_mapping` | 否 | `inputParameter.params` 中 `placeholder` 参数到样本字段的映射。 |
| `ctx.apiBase` | 是 | ADC OpenAPI base URL。 |
| `ctx.userAccount` | 是 | 请求后端接口和回调时使用的用户。 |

公共上下文字段建议收敛为 `state_key`、`id_source_key`、`start_date_key`、`end_date_key`。`parameter_mapping` 只配置指标自己的业务参数；它的 key 必须是后端 `inputParameter.params[].key_name_en`，value 是输入样本里的字段名。

## 3. 后端指标元数据

接口返回的单个 metric 元数据建议保持下面的形态：

```json
{
  "id": 201,
  "operatorType": "metric",
  "operatorNameEn": "bench_roi_score",
  "operatorNameCn": "行业基准 ROI 得分",
  "inputParameter": "{\"params\":[{\"key_name_en\":\"bench_roi\",\"data_type\":\"placeholder\"},{\"key_name_en\":\"threshold\",\"data_type\":\"defaultValue\",\"default_or_placeholder_value\":0.8}]}",
  "operatorCode": "def calculate(state, bench_roi, threshold, id_value, id_key, start_date=None, end_date=None, helpers=None):\n    return helpers.fmt4(float(threshold))"
}
```

接口返回的单个 tool 元数据建议保持下面的形态：

```json
{
  "id": 301,
  "operatorType": "tool",
  "toolName": "get_industry_creative_tips",
  "toolNameCn": "行业创意建议",
  "inputParameter": "{\"params\":[]}",
  "handlerType": "builtin",
  "handlerName": "get_industry_creative_tips",
  "operatorCode": "def calculate(state, id_value, helpers=None):\n    return f\"建议优化 {id_value} 的前三秒卖点\""
}
```

通用必需字段：

| 字段 | 用途 |
| --- | --- |
| `id` | 与 YAML 中的 `operator_id` 对应。 |
| `operatorType` | `metric` 或 `tool`；缺失时按 `metric` 兼容。 |
| `inputParameter` | JSON object 或 JSON 字符串，必须包含 `params` 数组。 |
| `operatorCode` | 可信 Python 代码，必须提供入口函数 `calculate(...)`。 |

metric 专用字段：

| 字段 | 用途 |
| --- | --- |
| `operatorNameEn` | 输出里的 `metricCode`。缺失时会退化为 `operator_{operator_id}`。 |
| `operatorNameCn` | 输出里的 `metricName`。 |

tool 专用字段：

| 字段 | 用途 |
| --- | --- |
| `toolName` | 输出里的 `tool`；缺失时依次退化为 `handlerName`、`operatorNameEn`、`operator_{operator_id}`。 |
| `toolNameCn` | 输出里的 `toolName`。 |
| `handlerType` / `handlerName` | 当前只作为元信息保留；执行仍以 `operatorCode.calculate(...)` 为准。 |

`inputParameter.params` 中每一项至少要有：

| 字段 | 说明 |
| --- | --- |
| `key_name_en` | 参数英文名，必须和 `calculate(...)` 形参名一致，或用于识别 ID 字段。 |
| `data_type` | 当前支持 `placeholder` 和 `defaultValue`。 |
| `default_or_placeholder_value` | 当 `data_type=defaultValue` 时使用。 |

参数取值规则：

| `data_type` | 取值方式 |
| --- | --- |
| `placeholder` | 通过 YAML 的 `parameter_mapping[key_name_en]` 找到样本字段，再从 sample 取值。 |
| `defaultValue` | 使用元数据里的 `default_or_placeholder_value`。 |

如果 `calculate(...)` 声明了普通业务参数，但 `inputParameter.params` 没有对应项，指标会失败并把错误写入该指标的 `error` 字段。

## 4. `calculate(...)` 函数写法

`operatorCode` 里必须定义 `calculate(...)`。函数可以返回字符串、数字、对象或数组；算子最终都会把输出保存成字符串。字符串会原样保存，数字、对象和数组会通过 JSON 序列化保存。

推荐写法：

```python
def calculate(state, id_value, id_key, start_date=None, end_date=None, helpers=None):
    series = {}
    for adv in state.get("adv_state", []):
        if str(adv.get("adv_id")) == str(id_value):
            series = adv.get("roi_by_day", {})
            break

    values = helpers.extract_numeric_values_in_range(series, start_date, end_date)
    avg = helpers.average(values)
    return helpers.fmt4(avg or 0.0)
```

不支持的函数签名：

- 不支持 `*args`。
- 不支持 `**kwargs`。
- 不支持 keyword-only 参数。

### 4.1 runtime 注入参数

下面这些参数不需要写进 `inputParameter.params`，算子会按函数签名自动注入：

| 参数 | 说明 |
| --- | --- |
| `state` | 从 `state_key` 读取并解析后的 State。 |
| `id_key` | 当前 ID 在 State 中命中的字段，目前识别 `ad_id` 和 `adv_id`。 |
| `id_value` | 当前正在计算的单个 ID。 |
| `start_date` | 从 `start_date_key` 样本字段解析出的 `datetime.date`。 |
| `end_date` | 从 `end_date_key` 样本字段解析出的 `datetime.date`。 |
| `helpers` | 公共数学和日期辅助方法集合。 |

注意：如果 `inputParameter.params` 里显式声明了同名参数，例如 `start_date` 或 `helpers`，则声明参数优先，会走 `parameter_mapping` 或 `defaultValue`，不会走 runtime 注入。一般不要把这些保留参数写进 `inputParameter.params`。

### 4.2 公共函数兼容策略

指标计算代码不要直接 import Dataset Factory 里的模块，例如不要写：

```python
from utils.math import calc_sequential_stats
```

Data-Juicer 不会加载 Dataset Factory 的运行环境，也不会把 `utils.math` 这类路径注入到 `operatorCode` 的执行环境中。公共数学、日期和格式化能力统一通过 `helpers` 参数调用：

```python
def calculate(state, id_value, start_date=None, end_date=None, helpers=None):
    series = {}
    cur, prev, ratio = helpers.calc_sequential_stats(series, start_date, end_date)
    return helpers.fmt4(cur or 0.0)
```

当前 `helpers` 支持的常用方法包括：

| 方法 | 说明 |
| --- | --- |
| `extract_numeric_values_in_range` | 从日期序列中取指定周期内的数值。 |
| `sum_numeric_values_in_range` | 对指定周期内的数值求和。 |
| `safe_divide` | 安全除法，分母为 0 或类型错误时返回默认值。 |
| `calc_ratio_from_series` | 基于两个日期序列计算平均比例。 |
| `calc_sequential_stats` | 计算普通数值类本周期均值、上周期均值和环比。 |
| `calc_sequential_stats_integer` | 计算计数类本周期均值、上周期均值和环比，均值取整数。 |
| `calc_sequential_stats_for_fraction` | 计算率类指标本周期比例、上周期比例和环比。 |
| `calc_bench_compare` | 计算当前值与同行基准的高低关系和差异百分比。 |
| `calc_sequential_ratio` | 返回 `[上周期均值, 本周期均值, 环比]`。 |
| `parse_percent_to_ratio` | 把百分数字符串或数值转成比例。 |
| `resolve_date_range_from_series` | 从序列中推导默认日期范围。 |
| `parse_duration_seconds` | 解析秒数。 |
| `average` | 求均值。 |
| `fmt4` | 小数格式化，最多保留 4 位并去掉尾随 0。 |

如果从 DF 迁移某个指标时缺少公共方法，优先把该方法补到 `MetricHelpers`，再在 `operatorCode` 中通过 `helpers.xxx(...)` 调用。不要在每个指标代码里重复粘贴公共函数，也不要依赖 DF 的 import 路径。

### 4.3 ID 识别和多 ID 计算

推荐通过算子级 `id_source_key` 配置公共 ID 字段。这样所有指标默认共用同一个 ID 来源，不需要在每个指标的 `parameter_mapping` 里重复配置 `ids`。

兼容老配置时，算子仍会从所选指标的参数中找 ID 候选字段。优先级如下：

1. `ids`
2. `id`
3. 其他以 `id` 结尾的参数，例如 `adv_id`

如果找到了指标级 ID 映射，它优先于算子级 `id_source_key`。如果没有找到指标级 ID 映射，才会使用 `id_source_key`。

如果样本字段是字符串：

- `"1854751525764108"` 会按一个 ID 计算一次。
- `"1854751525764108, 1853671159428096"` 会拆成两个 ID，分别计算一次。
- 混合文本中出现的数字片段会按出现顺序去重。

如果样本字段是数组，则数组元素会按列表语义逐个作为 ID。

每个 ID 都会执行一遍 `operators` 中的所有指标。若指标函数声明了 `id_key`，但当前 ID 无法在 `state.ad_state[].ad_id` 或 `state.adv_state[].adv_id` 中命中，该指标会失败并输出 `Unknown id: ...`。

如果某个老指标仍声明了 `ids`、`id`、`adv_id` 这类 ID 形参，但没有在 `parameter_mapping` 里配置对应字段，且算子配置了 `id_source_key`，算子会把当前拆分后的 ID 注入给该形参。

## 5. 输出格式

`result_mode=summary` 时，`output_key` 字段写入的是 JSON 字符串。下游需要先 `json.loads(...)`。这是推荐模式，适合写 Lance/Magnus 表，schema 更稳定。

默认 `summary_success_only=false`，summary 会保留成功和失败结果，并保留 `error`、`toolName` 等扩展字段，方便排查。

示例输出：

```json
{
  "1854751525764108": {
    "metrics": [
      {
        "metricCode": "bench_roi_score",
        "metricName": "行业基准 ROI 得分",
        "output": "0.82",
        "error": ""
      },
      {
        "metricCode": "ad_ctr_score",
        "metricName": "广告 CTR 得分",
        "output": "null",
        "error": "Unknown id: 1854751525764108"
      }
    ],
    "tools": [
      {
        "tool": "get_industry_creative_tips",
        "toolName": "行业创意建议",
        "output": "建议优化计划前三秒卖点",
        "error": ""
      }
    ]
  }
}
```

输出约束：

- 最外层 key 是当前 ID。
- 每个 ID 下可以有 `metrics` 和 `tools` 数组。
- `metricCode`、`metricName`、`output`、`error` 都会稳定输出为字符串。
- `tool`、`toolName`、`output`、`error` 也会稳定输出为字符串。
- 单个 metric/tool 失败不会中断整条样本，失败原因写入对应结果的 `error`。
- 如果没有可输出的 metric/tool 结果，`output_key` 会是空字符串。

如果配置：

```yaml
summary_success_only: true
```

summary 会按 DF 最终输入格式只保留成功字段：

- 过滤 `output` 为空的结果。
- 过滤 `output` 包含 `返回调用失败` 的结果。
- 过滤带 `error` 的结果。
- metric 只输出 `metricCode`、`metricName`、`output`。
- tool 只输出 `tool`、`output`。

示例：

```json
{
  "1812218125331659": {
    "metrics": [
      {
        "metricCode": "BidAdjustmentTimes",
        "metricName": "是否频繁调整出价",
        "output": "指标名称:是否频繁调整出价, 指标值：计划ID:1834567890123456：否"
      }
    ],
    "tools": [
      {
        "tool": "customer_info_acquisition",
        "output": "{'adv_name':'焱焱香文化','account_type':80,'adv_id':'1812218125331659'}"
      }
    ]
  }
}
```

## 6. metric 和 tool 怎么区分

当前 `state_metric_calculator` 支持 `metric` 和 `tool` 两类元数据，通过 `operatorType` 区分：

| `operatorType` | 执行方式 | 输出位置 |
| --- | --- | --- |
| `metric` 或缺失 | 执行 `operatorCode.calculate(...)` | `summary[id].metrics[]` |
| `tool` | 执行 `operatorCode.calculate(...)` | `summary[id].tools[]` |

metric 和 tool 的共同要求：

- 后端能按 `operator_id` 返回元数据。
- 元数据里有 `operatorCode`，并且能通过 `calculate(...)` 直接得到输出。
- 入参能通过公共上下文字段、`inputParameter.params`、`parameter_mapping`、State 或 runtime 注入参数解决。

注意：Data-Juicer 当前不会加载 Dataset Factory 的 `run_aux_tools` / `get_tool_handler` 注册表，也不会因为 `handlerType=builtin` 自动调用 DF builtin handler。`handlerType` 和 `handlerName` 只是元信息；真正执行逻辑必须写在 `operatorCode.calculate(...)` 里。

如果某个 tool 必须调用外部服务，也应该在 `operatorCode.calculate(...)` 中完成，或后续再新增明确的后端 tool 执行接口；不要依赖 DF 工程里的 import 路径。

## 7. 配置检查清单

上线或联调前按下面顺序检查：

1. 后端指标元数据存在，`id` 和 YAML 的 `operator_id` 一致。
2. `operatorType` 配置为 `metric` 或 `tool`；老 metric 元数据可以暂时不填，缺失时按 `metric` 兼容。
3. metric 配置 `operatorNameEn` 和 `operatorNameCn`，便于下游识别 `metricCode` 和 `metricName`。
4. tool 配置 `toolName` 和 `toolNameCn`，便于下游识别工具结果。
5. `inputParameter` 是合法 JSON object 或 JSON 字符串，且 `params` 是数组。
6. 每个 `calculate(...)` 普通业务参数都能在 `inputParameter.params` 找到。
7. YAML 配置了公共上下文字段：`state_key`、`id_source_key`、`start_date_key`、`end_date_key`。
8. 每个业务 `placeholder` 参数都在 YAML `parameter_mapping` 中映射到了真实样本字段。
9. 不把 `state`、`id_key`、`id_value`、`start_date`、`end_date`、`helpers` 这些 runtime 注入参数写进 `inputParameter.params`，除非明确要覆盖注入行为。
10. 如果 metric/tool 依赖 `id_key`，确认 State 里有对应 ID：`ad_state[].ad_id` 或 `adv_state[].adv_id`。
11. 多 ID 样本确认 `id_source_key` 字段能用逗号或数组表达，并确认下游按多个 summary key 消费。
12. 推荐使用 `result_mode=summary`；下游读取 `query_metric_data_outputs` 时先 `json.loads`。如果需要对象形态，可以使用 `result_mode=object`。

## 8. 常见问题

### `result_mode` 应该配什么？

默认推荐 `summary`，会输出 Dataset Factory summary JSON 字符串。`object` 也支持，会输出同一套 summary 的对象形态。两者结构一致，只差一次 JSON 序列化。

`summary` 模式输出字符串：

```json
"{\"123\":{\"metrics\":[],\"tools\":[]}}"
```

`object` 模式输出对象：

```json
{
  "123": {
    "metrics": [],
    "tools": []
  }
}
```

生产写表场景仍优先用 `summary`，因为字符串列的 schema 最稳定；`object` 更适合本地调试或不需要写复杂嵌套表结构的场景。

### 为什么指标代码里拿不到 `start_date`？

需要同时满足两个条件：

1. YAML 配置了 `start_date_key`。
2. `calculate(...)` 声明了 `start_date`，并且 `inputParameter.params` 没有显式声明同名参数。

`end_date` 同理。

### 字符串输出会不会额外加 JSON 引号？

不会。`calculate(...)` 返回 Python 字符串时，最终 `output` 会直接保存原字符串，适合在计算口径里维护 DF 风格 summary 文案。例如返回 `"指标名称:在投素材数（环比）, 指标值：..."`，summary 里也是这段文案。

非字符串仍会 JSON 序列化：返回数字 `0.82`，最终 `output` 是 `"0.82"`；返回对象或数组时，最终 `output` 是对应 JSON 字符串。

### 一个样本多个 ID 时会怎么执行？

例如样本字段是 `"1854751525764108, 1853671159428096"`，算子会拆成两个 ID。每个 ID 都会执行一遍本次选中的所有指标，最终 summary 中会有两个顶层 key。
