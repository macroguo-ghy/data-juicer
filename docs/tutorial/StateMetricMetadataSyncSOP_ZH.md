# State 指标和 Tool 元数据同步 SOP

本文给其他 agent 使用，目标是把 Dataset Factory 中已经维护的指标和 tool，同步到 ADC 后端元数据平台，供 Data-Juicer `state_metric_calculator` 算子通过 `/openapi/state-meta/operators/batch-get` 拉取并执行。

本 SOP 只约定同步流程和元数据契约，不设计新的 CLI。实际执行时使用后端已经提供的元数据维护 CLI，把本文中的 JSON payload 或 manifest 转成 CLI 所需输入即可。

## 1. 适用范围

适用于这两类同步：

1. Dataset Factory 中的指标同步为 ADC `operatorType=metric` 元数据。
2. Dataset Factory 中的辅助 tool 同步为 ADC `operatorType=tool` 元数据。

不适用：

- 不要把 Dataset Factory 整个 registry 或 tool handler 运行时迁移到 Data-Juicer。
- 不要让 Data-Juicer 在运行时 import Dataset Factory 包。
- 不要在本 SOP 中新增 Data-Juicer 算子参数或改执行逻辑。

Data-Juicer 侧消费规则见 `docs/tutorial/StateMetricMetadataConfiguration_ZH.md`。

## 2. 必须先确认的代码来源

同步前必须打开 Dataset Factory 真实代码，不要只按口头说明整理。

| 类型 | 必看路径 | 用途 |
| --- | --- | --- |
| metric 清单 | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/registry.py` | 读取 `METRIC_DEFINITIONS`，确认指标英文 code、中文名和 handler 函数。 |
| metric 实现 | `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/` | 读取具体指标函数，确认计算口径、公共函数依赖和输出文案。 |
| tool 清单 | `/Users/bytedance/develop/dataset_factory/core/metric_runner.py` | 读取 `_AUX_TOOL_DEFINITIONS`，确认 tool 英文名和中文名。 |
| tool handler | `/Users/bytedance/develop/dataset_factory/tool_handlers/` | 读取具体 tool handler，确认输入、输出和失败语义。 |
| 公共函数 | `/Users/bytedance/develop/dataset_factory/utils/math.py` 等 | 判断 Data-Juicer `helpers` 是否已有等价方法。 |

## 3. 最终元数据契约

### 3.1 Metric 元数据

metric 必须同步成下面的结构：

```json
{
  "operatorType": "metric",
  "operatorNameEn": "BidAdjustmentTimes",
  "operatorNameCn": "是否频繁调整出价",
  "groupName": "ad_flags",
  "inputParameter": "{\"params\":[]}",
  "operatorCode": "def calculate(state, id_value, id_key, start_date=None, end_date=None, helpers=None):\n    return \"指标名称:是否频繁调整出价, 指标值：计划ID:%s：否\" % id_value"
}
```

字段要求：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `operatorType` | 是 | 固定为 `metric`。 |
| `operatorNameEn` | 是 | 对应 DF `METRIC_DEFINITIONS` 的英文 code，会输出为 `metricCode`。 |
| `operatorNameCn` | 是 | 对应 DF 中文名，会输出为 `metricName`。 |
| `groupName` | 建议 | 所属分组，来自同步 manifest 或人工维护规则。 |
| `inputParameter` | 是 | JSON 字符串或对象，必须包含 `params` 数组。 |
| `operatorCode` | 是 | Data-Juicer 可直接执行的 Python 代码，必须定义 `calculate(...)`。 |

### 3.2 Tool 元数据

tool 必须同步成下面的结构：

```json
{
  "operatorType": "tool",
  "toolName": "customer_info_acquisition",
  "toolNameCn": "客户信息获取",
  "groupName": "aux_tools",
  "handlerType": "builtin",
  "handlerName": "customer_info_acquisition",
  "inputParameter": "{\"params\":[]}",
  "operatorCode": "def calculate(state, id_value, helpers=None):\n    return \"{'adv_id':'%s'}\" % id_value"
}
```

字段要求：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `operatorType` | 是 | 固定为 `tool`。 |
| `toolName` | 是 | 对应 DF tool 英文名，会输出为 `tools[].tool`。 |
| `toolNameCn` | 是 | 对应 DF tool 中文名，会输出为 `tools[].toolName`。 |
| `groupName` | 建议 | 建议统一为 `aux_tools`，或按业务再细分。 |
| `handlerType` | 建议 | 当前只作为元信息保留，建议填 `builtin`。 |
| `handlerName` | 建议 | 当前只作为元信息保留，建议和 DF handler 名称一致。 |
| `inputParameter` | 是 | JSON 字符串或对象，必须包含 `params` 数组。 |
| `operatorCode` | 是 | Data-Juicer 可直接执行的 Python 代码，必须定义 `calculate(...)`。 |

注意：`handlerType` 和 `handlerName` 不会触发 Data-Juicer 自动调用 DF handler。真正执行的是 `operatorCode.calculate(...)`。

## 4. operatorCode 改写规则

从 DF 同步时，不能原样复制依赖 DF import 的函数。每个指标或 tool 都要改写为 Data-Juicer 运行时可执行的独立 `calculate(...)`。

推荐函数签名：

```python
def calculate(state, id_value, id_key, start_date=None, end_date=None, helpers=None):
    return ""
```

允许按需省略没用到的参数，例如 tool 可以写：

```python
def calculate(state, id_value, helpers=None):
    return ""
```

禁止写法：

```python
from utils.math import calc_sequential_stats

def calculate(...):
    ...
```

改写要求：

1. 公共数学、日期、格式化能力通过 `helpers.xxx(...)` 调用。
2. 如果 DF 代码依赖的公共函数在 Data-Juicer `MetricHelpers` 中不存在，先记录为待补齐项，不要把公共函数复制到每个 `operatorCode`。
3. 输出文案先维护在 `operatorCode` 中，返回字符串即可，不需要手动 `json.dumps`。
4. 返回 DF summary 风格文案时，直接返回字符串，例如 `指标名称:xxx, 指标值：计划ID:123：12.0000`。
5. 单个指标失败时可以抛异常，Data-Juicer 会把错误写入对应结果的 `error` 字段。

## 5. inputParameter 配置规则

`inputParameter` 只声明业务参数，不声明 runtime 注入参数。

不要放入 `inputParameter.params` 的参数：

- `state`
- `id_value`
- `id_key`
- `start_date`
- `end_date`
- `helpers`

业务参数示例：

```json
{
  "params": [
    {
      "key_name_en": "bench_roi",
      "data_type": "placeholder"
    },
    {
      "key_name_en": "threshold",
      "data_type": "defaultValue",
      "default_or_placeholder_value": 0.8
    }
  ]
}
```

规则：

| `data_type` | 含义 |
| --- | --- |
| `placeholder` | 运行时从 Data-Juicer YAML 的 `parameter_mapping` 映射到样本字段。 |
| `defaultValue` | 运行时直接使用元数据里的 `default_or_placeholder_value`。 |

`calculate(...)` 中每个普通业务形参，都必须能从 `inputParameter.params` 解析，或者属于 runtime 注入参数。

## 6. 建议维护一个同步 manifest

为了让 CLI 同步可复现，建议维护一份本地 manifest。manifest 不要求由 Data-Juicer 读取，只给同步 agent 和元数据 CLI 使用。

示例：

```yaml
metrics:
  - operatorType: metric
    operatorNameEn: BidAdjustmentTimes
    operatorNameCn: 是否频繁调整出价
    groupName: ad_flags
    sourcePath: /Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/ad_flags.py
    sourceFunction: metric_bid_adjustment_times
    operatorCodeFile: metadata_codes/metrics/BidAdjustmentTimes.py
    inputParameter:
      params: []

tools:
  - operatorType: tool
    toolName: customer_info_acquisition
    toolNameCn: 客户信息获取
    groupName: aux_tools
    handlerType: builtin
    handlerName: customer_info_acquisition
    sourcePath: /Users/bytedance/develop/dataset_factory/tool_handlers/customer_info_acquisition.py
    operatorCodeFile: metadata_codes/tools/customer_info_acquisition.py
    inputParameter:
      params: []
```

manifest 至少要能回答：

1. 这条元数据是 metric 还是 tool。
2. 英文名和中文名是什么。
3. 属于哪个分组。
4. DF 来源文件和函数是什么。
5. 最终同步的 `operatorCode` 从哪个文件读取。
6. `inputParameter` 需要哪些业务参数。

## 7. 同步执行步骤

### Step 1: 从 DF 抽取候选清单

metric：

1. 打开 `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/registry.py`。
2. 读取 `METRIC_DEFINITIONS`。
3. 对每个 tuple 记录：`operatorNameEn`、`operatorNameCn`、handler 函数名和来源模块。

tool：

1. 打开 `/Users/bytedance/develop/dataset_factory/core/metric_runner.py`。
2. 读取 `_AUX_TOOL_DEFINITIONS`。
3. 对每个 tuple 记录：`toolName`、`toolNameCn`。
4. 到 `/Users/bytedance/develop/dataset_factory/tool_handlers/` 下找到真实 handler 实现。

### Step 2: 分配或确认分组

优先使用业务已有分组。如果没有现成分组，按来源模块或业务域临时归类，例如：

| 来源 | 建议分组 |
| --- | --- |
| `ad_flags.py` | `ad_flags` |
| `materials_metrics.py` | `materials` |
| `live_metrics.py` | `live` |
| `roi_compare.py` | `roi` |
| `user_group.py` | `user_group` |
| aux tool | `aux_tools` |

分组名称要同步给前端或后端元数据平台，保证页面可以按组展示。

### Step 3: 改写 operatorCode

对每个候选项：

1. 阅读 DF handler 的真实实现。
2. 找出依赖的 State 字段、公共函数、日期范围、ID 语义和输出文案。
3. 改写为 `def calculate(...):`。
4. 把公共函数调用替换为 `helpers.xxx(...)`。
5. 保持输出文案和 DF 对齐，返回字符串。
6. 如果缺少 helper，记录待补齐，不要在多个 `operatorCode` 中重复复制。

### Step 4: 生成元数据 payload

metric payload：

```json
{
  "operatorType": "metric",
  "operatorNameEn": "<DF metric code>",
  "operatorNameCn": "<DF metric name>",
  "groupName": "<group>",
  "inputParameter": "{\"params\":[]}",
  "operatorCode": "<Data-Juicer calculate code>"
}
```

tool payload：

```json
{
  "operatorType": "tool",
  "toolName": "<DF tool name>",
  "toolNameCn": "<DF tool cn name>",
  "groupName": "aux_tools",
  "handlerType": "builtin",
  "handlerName": "<DF handler name>",
  "inputParameter": "{\"params\":[]}",
  "operatorCode": "<Data-Juicer calculate code>"
}
```

如果元数据接口需要 `id`、`version`、`enabled`、`description` 等额外字段，按后端 CLI 规范补齐；不要改变上述核心字段含义。

### Step 5: 用后端 CLI upsert

使用后端提供的元数据维护 CLI 执行 upsert。SOP 不规定 CLI 名称，但同步命令必须满足：

1. 支持按唯一键更新，避免重复创建同一 metric/tool。
2. metric 唯一键建议为 `operatorType + operatorNameEn`。
3. tool 唯一键建议为 `operatorType + toolName`。
4. 同步后能返回元数据 `id`，用于 Data-Juicer YAML 的 `operators[].operator_id`。
5. 同步失败时保留 CLI 原始错误，便于回滚或重试。

执行记录至少保留：

```text
operatorType
operatorNameEn/toolName
operatorNameCn/toolNameCn
groupName
metadata id
sourcePath
operatorCodeFile
CLI request id or log id
```

### Step 6: 拉取校验

同步后必须通过后端读取接口或 CLI get 命令验证一次，确认平台实际保存的内容。

检查项：

1. `operatorType` 是否为 `metric` 或 `tool`。
2. metric 是否有 `operatorNameEn`、`operatorNameCn`。
3. tool 是否有 `toolName`、`toolNameCn`、`handlerType`、`handlerName`。
4. `inputParameter` 是否是合法 JSON，且包含 `params` 数组。
5. `operatorCode` 是否完整，没有被 CLI 转义或截断。
6. 返回的 `id` 是否写入后续 Data-Juicer YAML。

### Step 7: Data-Juicer 联调

准备最小 YAML：

```yaml
process:
  - state_metric_calculator:
      state_key: state
      id_source_key: issue_id
      output_key: query_metric_data_outputs
      result_mode: summary
      summary_success_only: false
      operators:
        - operator_id: 201
          parameter_mapping: {}
        - operator_id: 301
          parameter_mapping: {}
      ctx:
        apiBase: "https://ai-data-center.bytedance.net/api"
        userAccount: "wangjianda.667"
```

联调检查：

1. 算子能通过 `operator_id` 拉到元数据。
2. metric 输出进入 `summary[id].metrics[]`。
3. tool 输出进入 `summary[id].tools[]`。
4. `output` 是字符串，且 DF 风格文案没有被二次 JSON 编码。
5. 失败项保留 `error` 字段，整条样本不中断。
6. 多 ID 样本会拆成多个 summary 顶层 key。

## 8. 同步前本地校验

每条 `operatorCode` 入库前至少做这些校验：

1. Python 语法能通过 `compile(code, "<operatorCode>", "exec")`。
2. 执行后存在 `calculate` 函数。
3. `calculate` 不使用 `*args`、`**kwargs`、keyword-only 参数。
4. 代码中没有 Dataset Factory import，例如 `from utils.`、`from tool_handlers.`。
5. 代码中没有访问本地绝对路径。
6. 字符串输出不再手动 `json.dumps`。
7. 所有普通业务参数都能被 `inputParameter.params` 解释。
8. sample state 能跑通一次，并输出字符串或可序列化对象。

## 9. 输出对齐规则

Data-Juicer 当前支持两种 `result_mode`：

| 模式 | `output_key` 内容 | 适用场景 |
| --- | --- | --- |
| `summary` | DF summary 结构的 JSON 字符串 | 生产写表，推荐。 |
| `object` | 同一套 DF summary 结构的 Python 对象 | 本地调试或不写复杂表结构。 |

两种模式的结构一致，只差 JSON 序列化。

默认 `summary_success_only=false`，输出会保留成功和失败结果，并保留 `error`。如果配置 `summary_success_only=true`，才会过滤失败项并只保留 DF 成功字段。

目标 summary 结构：

```json
{
  "1812218125331659": {
    "metrics": [
      {
        "metricCode": "BidAdjustmentTimes",
        "metricName": "是否频繁调整出价",
        "output": "指标名称:是否频繁调整出价, 指标值：计划ID:1834567890123456：否",
        "error": ""
      }
    ],
    "tools": [
      {
        "tool": "customer_info_acquisition",
        "toolName": "客户信息获取",
        "output": "{'adv_name':'焱焱香文化','account_type':80,'adv_id':'1812218125331659'}",
        "error": ""
      }
    ]
  }
}
```

## 10. 常见错误和处理

| 问题 | 原因 | 处理 |
| --- | --- | --- |
| Data-Juicer 报 `operator detail not found` | YAML 使用的 `operator_id` 不存在或同步环境不一致 | 用 CLI get 确认元数据 ID，并检查 `apiBase` 环境。 |
| 输出进入 `metrics[]` 而不是 `tools[]` | `operatorType` 缺失或不是 `tool` | 修正元数据 `operatorType=tool`。 |
| `output` 里有多余 JSON 引号 | `calculate` 返回前手动 `json.dumps` 了字符串 | 字符串直接 return，不做 `json.dumps`。 |
| 报 DF import 找不到 | operatorCode 原样复制了 DF import | 改成 `helpers.xxx(...)`，缺 helper 时先补 Data-Juicer helper。 |
| 参数缺失 | `calculate` 形参没有 runtime 注入，也没有写入 `inputParameter.params` | 补 `inputParameter` 或调整函数签名。 |
| 前端无法区分 metric/tool | 元数据缺 `operatorType` | metric 填 `metric`，tool 填 `tool`。 |
| 分组展示不对 | `groupName` 缺失或命名不统一 | 按 manifest 统一维护分组。 |

## 11. 交付清单

每批同步完成后，agent 需要交付：

1. 同步 manifest 或等价记录。
2. 每条 metric/tool 的元数据 ID。
3. 每条元数据的 DF 来源路径和函数名。
4. 每条元数据的 `operatorCode` 文件或代码快照。
5. CLI upsert 结果和拉取校验结果。
6. 一份 Data-Juicer 最小 YAML 示例。
7. 一条包含 metric 和 tool 的样本联调输出。
8. 未完成项列表，例如缺少的 `helpers`、暂未迁移的 tool、口径待确认的指标。

## 12. Agent 执行原则

1. 先读 DF 真实代码，再整理元数据。
2. 先同步少量 metric/tool 做闭环，再批量同步。
3. 不在 Data-Juicer 里引入 DF runtime 依赖。
4. 不把公共函数重复粘贴到多个 `operatorCode`。
5. 不改变 `state_metric_calculator` 的输出契约。
6. 同步后必须通过平台读取结果确认，不只看 CLI upsert 成功。
7. 联调时优先使用 `result_mode=summary` 和 `summary_success_only=false`。
