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
| `result_mode` | 否 | 输出模式，支持 `summary`、`object` 和 `metric_list`，默认 `summary`。 |
| `summary_success_only` | 否 | 仅 `summary` 模式生效；默认 `false` 保留成功和失败结果，`true` 时只输出 DF 成功字段。 |
| `start_date_key` | 否 | 兼容保留字段；不会再自动注入 `calculate(...)`。日期参数请通过 `inputParameter.params` 和 `parameter_mapping` 配置。 |
| `end_date_key` | 否 | 兼容保留字段；不会再自动注入 `calculate(...)`。日期参数请通过 `inputParameter.params` 和 `parameter_mapping` 配置。 |
| `operators` | 是 | 本次要执行的指标列表。 |
| `operators[].operator_id` | 是 | 后端指标元数据 ID。 |
| `operators[].parameter_mapping` | 否 | `inputParameter.params` 中 `placeholder` 参数到样本字段的映射。 |
| `ctx.apiBase` | 是 | ADC OpenAPI base URL。 |
| `ctx.userAccount` | 是 | 请求后端接口和回调时使用的用户。 |

公共上下文字段建议收敛为 `state_key`、`id_source_key`。`parameter_mapping` 配置指标自己的业务参数；它的 key 必须是后端 `inputParameter.params[].key_name_en`，value 是输入样本里的字段名。

## 3. 后端指标元数据

接口返回的单个 metric 元数据建议保持下面的形态：

```json
{
  "id": 201,
  "operatorType": "metric",
  "operatorNameEn": "bench_roi_score",
  "operatorNameCn": "行业基准 ROI 得分",
  "inputParameter": "{\"params\":[{\"key_name_en\":\"bench_roi\",\"data_type\":\"placeholder\"},{\"key_name_en\":\"id_value\",\"data_type\":\"placeholder\"},{\"key_name_en\":\"threshold\",\"data_type\":\"defaultValue\",\"default_or_placeholder_value\":0.8}]}",
  "operatorCode": "def calculate(state, bench_roi, id_value, threshold, helpers=None):\n    id_key = helpers.get_id_key(state, id_value)\n    return helpers.fmt4(float(threshold))"
}
```

接口返回的单个 tool 元数据建议保持下面的形态：

```json
{
  "id": 301,
  "operatorType": "tool",
  "toolName": "get_industry_creative_tips",
  "toolNameCn": "行业创意建议",
  "inputParameter": "{\"params\":[{\"key_name_en\":\"id_value\",\"data_type\":\"placeholder\"}]}",
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
def calculate(state, id_value, start_date=None, end_date=None, helpers=None):
    id_key = helpers.get_id_key(state, id_value)
    if id_key is None:
        raise ValueError(f"Unknown id: {id_value}")
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
| `helpers` | 公共数学和日期辅助方法集合。 |

`id_value`、`id_key`、`start_date`、`end_date` 不再是公共 runtime 注入参数。如果指标需要这些值，必须在 `inputParameter.params` 中声明，并通过 YAML `parameter_mapping` 映射到样本字段，或使用 `defaultValue`。`id_key` 不再作为形参注入，指标代码应通过 `helpers.get_id_key(state, id_value)` 主动识别。

### 4.2 公共函数兼容策略

指标计算代码不要直接 import Dataset Factory 里的模块，例如不要写：

```python
from utils.math import calc_sequential_stats
```

Data-Juicer 不会加载 Dataset Factory 的运行环境，也不会把 `utils.math` 这类路径注入到 `operatorCode` 的执行环境中。公共数学、日期和格式化能力统一通过 `helpers` 参数调用：

```python
def calculate(state, start_date=None, end_date=None, helpers=None):
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
| `get_id_key` | 根据 `state` 和业务传入的 ID 识别 `ad_id`、`adv_id` 或 `material_id`。 |
| `get_id_keys` | 返回当前 ID 在 State 中命中的所有 ID 类型集合。 |

对于模板中“生成含问题发生时间及之前的 14 天数据”的数组字段，例如 `ad_active_materials_count: [5,5,6,6,6,7,7,7,6,6,5,5,6,6]`，`calc_sequential_stats`、`calc_sequential_stats_integer`、`calc_sequential_stats_for_fraction` 和 `calc_sequential_ratio` 会按数组前半段作为上周期、后半段作为本周期计算环比。若字段是 `{YYYY-MM-DD: value}` 字典，则仍按日期范围计算。

如果从 DF 迁移某个指标时缺少公共方法，优先把该方法补到 `MetricHelpers`，再在 `operatorCode` 中通过 `helpers.xxx(...)` 调用。不要在每个指标代码里重复粘贴公共函数，也不要依赖 DF 的 import 路径。

### 4.3 ID 识别和多 ID 计算

推荐通过算子级 `id_source_key` 配置公共 ID 字段。这样所有指标默认共用同一个 ID 来源，不需要在每个指标的 `parameter_mapping` 里重复配置 `ids`。

如果配置了 `id_source_key` 且样本中该字段有值，算子会使用它作为当前样本的 ID 来源。算子不会再从 `inputParameter.params` 或 `parameter_mapping` 中按参数名推断 ID 来源；`ids`、`id`、`adv_id` 这类参数名都只是普通业务参数。

因此，推荐配置是：公共 ID 统一放在算子级 `id_source_key`；指标级 `parameter_mapping` 只维护该指标自己的业务参数。需要在指标代码中使用 ID 时，显式声明普通参数，例如 `id_value`，并在该指标的 `parameter_mapping` 中映射到样本字段。

注意：外部传入的 ID 是当前样本的主输入来源。也就是说，题目或样本字段里的 `adv_id` / `ad_id` 应优先通过 `id_source_key` 传入；summary 顶层 key 使用这个外部 ID。State 里的 ID 只用于 `helpers.get_id_key(state, id_value)` 识别当前 ID 类型。

当前 `id_key` 识别只检查：

- `state.ad_state[].ad_id`
- `state.adv_state[].adv_id`
- `state.adv_state[].meta_data.adv_id`
- `state.material_state[].material_id`

当前不会通过 `state.ad_state[].related_adv_id` 推断 `adv_id`。因此如果模型生成的 State 没有把外部账户 ID 写到 `adv_state[].adv_id`，但只写在 `ad_state[].related_adv_id`，`helpers.get_id_key(state, id_value)` 会返回 `None`。如果指标依赖 ID 类型，应在指标代码里显式抛出类似 `Unknown id: ...` 的错误。生成 State 的 prompt 可以包含题目 ID，但仍需要尽量让生成结果中的 `ad_id` / `adv_id` / `material_id` 与外部样本 ID 对齐。

如果样本字段是字符串：

- `"1854751525764108"` 会按一个 ID 计算一次。
- `"1854751525764108, 1853671159428096"` 会拆成两个 ID，分别计算一次。
- 混合文本中出现的数字片段会按出现顺序去重。

如果样本字段是数组，则数组元素会按列表语义逐个作为 ID。

每个 ID 都会执行一遍 `operators` 中的所有指标。`id_source_key` 负责拆分 summary 的外层 ID key，但不会把当前拆分后的 ID 自动注入到 `calculate(...)`。如果指标把 `id_value` 配置为 placeholder，它拿到的是 `parameter_mapping` 指向的样本字段原始值。

如果某个指标声明了 `ids`、`id`、`adv_id` 这类形参，它们会按普通 placeholder 处理：必须在 `parameter_mapping` 中映射到样本字段，且传入值是该字段原始值，不会被替换成当前拆分后的 ID。

## 5. 输出格式

`result_mode=summary` 时，`output_key` 字段写入的是 JSON 字符串。下游需要先 `json.loads(...)`。这是兼容 Dataset Factory summary 的模式，适合写 Lance/Magnus 表，schema 更稳定。

`result_mode=metric_list` 时，`output_key` 字段写入的是对象数组。数组中每个对象对应一个被选择的 metric 或 tool operator，tool 也统一放入 `metric_list`。这个模式适合前端按派生字段展示每次计算的 `input`、`output` 和 `error`。

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
- 入参能通过公共上下文字段、`inputParameter.params`、`parameter_mapping`、State 或 runtime 注入参数 `state` / `helpers` 解决。

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
7. YAML 配置了公共上下文字段：`state_key`、`id_source_key`。`start_date_key`、`end_date_key` 不再自动注入到指标代码，日期参数需要按普通业务参数配置。
8. 每个业务 `placeholder` 参数都在 YAML `parameter_mapping` 中映射到了真实样本字段。
9. 不把 `state`、`helpers` 写进 `inputParameter.params`；它们是当前仅有的 runtime 注入参数。
10. 如果 metric/tool 依赖 ID 类型，使用 `helpers.get_id_key(state, id_value)`，并确认 State 里有对应 ID：`ad_state[].ad_id`、`adv_state[].adv_id` 或 `material_state[].material_id`。
11. 多 ID 样本确认 `id_source_key` 字段能用逗号或数组表达，并确认下游按多个 summary key 消费。
12. 兼容 Dataset Factory summary 时使用 `result_mode=summary`；需要按派生字段展示入参和多值展开结果时使用 `result_mode=metric_list`。如果只需要旧 summary 对象形态，可以使用 `result_mode=object`。

## 8. 常见问题

### `result_mode` 应该配什么？

默认推荐 `summary`，会输出 Dataset Factory summary JSON 字符串。`metric_list` 会输出面向前端展示的新结构，最外层是对象数组，每个对象包含 `meta` 和 `metric_list`。`object` 也支持，会输出旧 summary 的对象形态。

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

`metric_list` 模式输出对象数组：

```json
[
  {
    "meta": {
      "operator_id": 47,
      "operator_type": "metric",
      "metric_code": "EcpCost",
      "metric_name": "计划消耗环比",
      "params": {}
    },
    "metric_list": [
      {
        "input": {
          "unknown_id": "123"
        },
        "output": "...",
        "error": ""
      }
    ]
  }
]
```

生产写表场景仍优先用 `summary`，因为字符串列的 schema 最稳定；`metric_list` 更适合前端展示，`object` 更适合本地调试或不需要写复杂嵌套表结构的场景。

### 为什么指标代码里拿不到 `start_date`？

`start_date` 不再由算子自动注入。需要在指标元数据和 YAML 中把它按普通参数配置：

1. `inputParameter.params` 声明 `{"key_name_en": "start_date", "data_type": "placeholder"}`。
2. YAML 的 `operators[].parameter_mapping.start_date` 映射到真实样本字段。

`end_date` 同理。

### 字符串输出会不会额外加 JSON 引号？

不会。`calculate(...)` 返回 Python 字符串时，最终 `output` 会直接保存原字符串，适合在计算口径里维护 DF 风格 summary 文案。例如返回 `"指标名称:在投素材数（环比）, 指标值：..."`，summary 里也是这段文案。

非字符串仍会 JSON 序列化：返回数字 `0.82`，最终 `output` 是 `"0.82"`；返回对象或数组时，最终 `output` 是对应 JSON 字符串。

### 一个样本多个 ID 时会怎么执行？

例如样本字段是 `"1854751525764108, 1853671159428096"`，算子会拆成两个 ID。每个 ID 都会执行一遍本次选中的所有指标，最终 summary 中会有两个顶层 key。
