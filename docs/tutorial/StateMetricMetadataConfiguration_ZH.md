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
| `result_mode` | 否 | 当前只支持 `summary`，可省略；不要配置 `object`。 |
| `start_date_key` | 否 | 样本里的起始日期字段，供 `calculate(..., start_date, ...)` 使用。 |
| `end_date_key` | 否 | 样本里的结束日期字段，供 `calculate(..., end_date, ...)` 使用。 |
| `operators` | 是 | 本次要执行的指标列表。 |
| `operators[].operator_id` | 是 | 后端指标元数据 ID。 |
| `operators[].parameter_mapping` | 否 | `inputParameter.params` 中 `placeholder` 参数到样本字段的映射。 |
| `ctx.apiBase` | 是 | ADC OpenAPI base URL。 |
| `ctx.userAccount` | 是 | 请求后端接口和回调时使用的用户。 |

公共上下文字段建议收敛为 `state_key`、`id_source_key`、`start_date_key`、`end_date_key`。`parameter_mapping` 只配置指标自己的业务参数；它的 key 必须是后端 `inputParameter.params[].key_name_en`，value 是输入样本里的字段名。

## 3. 后端指标元数据

接口返回的单个指标元数据建议保持下面的形态：

```json
{
  "id": 201,
  "operatorNameEn": "bench_roi_score",
  "operatorNameCn": "行业基准 ROI 得分",
  "inputParameter": "{\"params\":[{\"key_name_en\":\"bench_roi\",\"data_type\":\"placeholder\"},{\"key_name_en\":\"threshold\",\"data_type\":\"defaultValue\",\"default_or_placeholder_value\":0.8}]}",
  "operatorCode": "def calculate(state, bench_roi, threshold, id_value, id_key, start_date=None, end_date=None, helpers=None):\n    return helpers.fmt4(float(threshold))"
}
```

必需字段：

| 字段 | 用途 |
| --- | --- |
| `id` | 与 YAML 中的 `operator_id` 对应。 |
| `operatorNameEn` | 输出里的 `metricCode`。缺失时会退化为 `operator_{operator_id}`。 |
| `operatorNameCn` | 输出里的 `metricName`。 |
| `inputParameter` | JSON object 或 JSON 字符串，必须包含 `params` 数组。 |
| `operatorCode` | 可信 Python 代码，必须提供入口函数 `calculate(...)`。 |

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

### 4.2 ID 识别和多 ID 计算

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

`output_key` 字段写入的是 JSON 字符串，不是对象。下游需要先 `json.loads(...)`。

示例输出：

```json
{
  "1854751525764108": {
    "metrics": [
      {
        "metricCode": "bench_roi_score",
        "metricName": "行业基准 ROI 得分",
        "output": "\"0.82\"",
        "error": ""
      },
      {
        "metricCode": "ad_ctr_score",
        "metricName": "广告 CTR 得分",
        "output": "null",
        "error": "Unknown id: 1854751525764108"
      }
    ]
  }
}
```

输出约束：

- 最外层 key 是当前 ID。
- 每个 ID 下只有 `metrics` 数组。
- `metricCode`、`metricName`、`output`、`error` 都会稳定输出为字符串。
- 单个指标失败不会中断整条样本，失败原因写入该指标的 `error`。
- 如果没有可输出的指标结果，`output_key` 会是空字符串。

## 6. metric 和 tool 怎么区分

当前 `state_metric_calculator` 只支持 metric，不支持 tool。

可以放进 `operators` 的，是满足下面条件的 metric：

- 后端能按 `operator_id` 返回指标元数据。
- 元数据里有 `operatorCode`，并且能通过 `calculate(...)` 直接得到指标值。
- 入参能通过公共上下文字段、`inputParameter.params`、`parameter_mapping`、State 或 runtime 注入参数解决。
- 输出应该进入 Dataset Factory summary 风格的 `metrics` 数组。

不要放进 `operators` 的，是 tool 或外部能力：

- 需要走单独工具 handler，而不是 `calculate(...)`。
- 需要构造自定义 HTTP 请求、RPC 请求或额外认证。
- 有副作用，例如写外部系统、触发任务、生成文件。
- 输出不应该进入 `metrics`，而应该进入类似 `tools` 或其他独立结构。
- 依赖 Dataset Factory 中 `run_aux_tools` / `get_tool_handler` 这类 tool 路径。

Dataset Factory 里 metric 和 tool 是两条路径：metric 走指标注册和 `query_metric_data`，tool 走 tool handler。Data-Juicer 当前只对齐 metric summary 输出和公共计算辅助能力，没有把 Dataset Factory 的 tool handler 迁移进来。

如果后续要支持 tool，建议由后端元数据显式区分 `operatorType: metric|tool`，再新增独立 tool 算子或显式 opt-in 模式。不要在当前 `state_metric_calculator.operators` 中混配 tool。

## 7. 配置检查清单

上线或联调前按下面顺序检查：

1. 后端指标元数据存在，`id` 和 YAML 的 `operator_id` 一致。
2. `operatorNameEn` 和 `operatorNameCn` 已配置，便于下游识别 `metricCode` 和 `metricName`。
3. `inputParameter` 是合法 JSON object 或 JSON 字符串，且 `params` 是数组。
4. 每个 `calculate(...)` 普通业务参数都能在 `inputParameter.params` 找到。
5. YAML 配置了公共上下文字段：`state_key`、`id_source_key`、`start_date_key`、`end_date_key`。
6. 每个业务 `placeholder` 参数都在 YAML `parameter_mapping` 中映射到了真实样本字段。
7. 不把 `state`、`id_key`、`id_value`、`start_date`、`end_date`、`helpers` 这些 runtime 注入参数写进 `inputParameter.params`，除非明确要覆盖注入行为。
8. 如果指标依赖 `id_key`，确认 State 里有对应 ID：`ad_state[].ad_id` 或 `adv_state[].adv_id`。
9. 多 ID 样本确认 `id_source_key` 字段能用逗号或数组表达，并确认下游按多个 summary key 消费。
10. 下游读取 `query_metric_data_outputs` 时先 `json.loads`，不要按对象列读取。
11. tool 不配置到 `state_metric_calculator.operators`。

## 8. 常见问题

### 为什么我配置了 `result_mode: object` 会失败？

当前算子只支持 Dataset Factory summary 字符串输出，所以 `result_mode` 必须是 `summary`。这个字段可以不配，默认就是 `summary`。

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
