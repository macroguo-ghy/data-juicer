# State 指标计算输出契约设计

本文说明 `state_metric_calculator` 的 `result_mode=metric_list` 输出展示结构。目标是适配“每个指标或工具单独声明入参 key”的模型，不再依赖全局 `id_source_key` 决定所有结果的展示分组。

## 1. 输出结构

输出最外层是对象数组：

```json
[
  {
    "meta": {
      "metric_code": "EcpCost",
      "metric_name": "计划消耗环比",
      "params": {
        "unknown_id": {
          "name": "未知ID",
          "type": "AMBIGUOUS"
        }
      }
    },
    "metric_list": [
      {
        "input": {
          "unknown_id": "123444"
        },
        "output": "指标名称:计划消耗环比, 指标值：计划ID：123444：345.5元 环比下降34.57%（上周期528.0714元）",
        "error": ""
      },
      {
        "input": {
          "unknown_id": "123445"
        },
        "output": "null",
        "error": "name 'state_data' is not defined"
      }
    ]
  }
]
```

最外层数组中每个对象对应一个被选择的派生 operator。这里的 operator 包括 metric 和 tool。

## 2. 为什么最外层是数组

一个样本可以配置多个派生指标或工具，例如：

```yaml
operators:
  - operator_id: 47
    parameter_mapping:
      unknown_id: "id"
      startDate: "startDate"
      endDate: "endDate"
  - operator_id: 48
    parameter_mapping:
      adv_id: "advertiser_id"
      startDate: "startDate"
      endDate: "endDate"
```

这些 operator 的入参 key、展示名称、错误信息和执行次数可能都不一样。用数组可以自然表达：

- 第一个对象：operator 47 的元信息和多次计算结果。
- 第二个对象：operator 48 的元信息和多次计算结果。

因此不建议继续把最外层设计成以 ID 为 key 的对象。ID 只是某个 operator 的业务入参，不再是全局输出分组依据。

## 3. metric 和 tool 的统一展示

tool 的结果也放入 `metric_list`，不再单独输出 `tool_list`。

原因：

- 前端展示上都属于“派生字段计算结果”。
- tool 也有入参、输出和错误，和 metric 的展示结构一致。
- 字段名统一后，下游不需要为 metric/tool 写两套列表逻辑。

tool 和 metric 统一输出为派生指标结果，不在结果里保留 `operator_type`：

```json
{
  "meta": {
    "metric_code": "customer_info_acquisition",
    "metric_name": "客户信息获取工具",
    "params": {
      "adv_id": {
        "name": "广告主ID",
        "type": "CONCRETE"
      }
    }
  },
  "metric_list": [
    {
      "input": {
        "adv_id": "1812218125331659"
      },
      "output": "{'adv_name':'焱焱香文化','account_type':80,'adv_id':'1812218125331659'}",
      "error": ""
    }
  ]
}
```

## 4. 字段说明

### 4.1 顶层对象

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `meta` | object | 当前 operator 的展示元信息和入参定义摘要。 |
| `metric_list` | array | 当前 operator 的一次或多次计算结果。metric 和 tool 都使用这个字段。 |

### 4.2 `meta`

`meta` 和实际计算数据拆开，便于 LLM Prompt 或前端先读取指标说明，再遍历结果列表。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `metric_code` | string | 展示和排查使用的英文标识。metric 可取 `operatorNameEn`，tool 可取 `toolName`。 |
| `metric_name` | string | 展示名称。metric 可取 `operatorNameCn`，tool 可取 `toolNameCn`。 |
| `params` | object | 当前 operator 的入参定义摘要。key 是参数英文名。 |

### 4.3 `meta.params`

`params` 描述参数定义，不放运行时取值。

```json
{
  "unknown_id": {
    "name": "未知ID",
    "type": "AMBIGUOUS"
  },
  "startDate": {
    "name": "开始时间",
    "type": "CONCRETE"
  }
}
```

建议来源：

| 输出字段 | 元数据来源 |
| --- | --- |
| `name` | `inputParameterDetails[].keyNameCn`。 |
| `type` | `inputParameterDetails[].keyType`，例如 `CONCRETE`、`AMBIGUOUS`。 |

`operator_id`、`operator_type`、`multiValue` 属于运行和配置字段，不输出给 LLM Prompt。`multiValue` 只参与运行时多值展开。

`params` 只从 `inputParameterDetails` 构造。如果后端没有提供 `inputParameterDetails`，`params` 输出为空对象：

```json
{}
```

### 4.4 `metric_list[]`

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `input` | object | 本次计算实际传给 `calculate(...)` 的业务入参值。 |
| `output` | string | 指标或工具输出。成功时通常是文案字符串；失败时使用 `"null"`。 |
| `error` | string | 错误信息。成功时为空字符串。 |

`output` 和 `error` 必须始终是字符串，避免 Ray/PyArrow 对嵌套 schema 推断不稳定。

## 5. 多值参数展开规则

只有 `inputParameterDetails[].multiValue=true` 的参数会被识别为多值参数。当多值参数映射到的样本字段包含多个值，例如 `"123,456"`，算子会拆成多次执行，并在 `metric_list` 里生成多条结果。

`multiValue=false` 或缺省的参数永远按单值处理。即使字符串里包含英文逗号，也会原样传给 `calculate(...)`，并广播到每一次多值调用。

### 5.1 单个多值参数

输入：

```json
{
  "id": "123,456"
}
```

配置：

```yaml
parameter_mapping:
  unknown_id: "id"
```

执行和输出：

```json
{
  "metric_list": [
    {
      "input": {
        "unknown_id": "123"
      },
      "output": "...",
      "error": ""
    },
    {
      "input": {
        "unknown_id": "456"
      },
      "output": "...",
      "error": ""
    }
  ]
}
```

含义：`calculate(...)` 会被调用两次，每次收到一个单值 ID。

### 5.2 多值参数和单值参数混合

输入：

```json
{
  "id": "123,456",
  "startDate": "2026-05-01",
  "endDate": "2026-05-14"
}
```

配置：

```yaml
parameter_mapping:
  unknown_id: "id"
  startDate: "startDate"
  endDate: "endDate"
```

执行时，单值参数广播到每一次调用：

```json
{
  "metric_list": [
    {
      "input": {
        "unknown_id": "123",
        "startDate": "2026-05-01",
        "endDate": "2026-05-14"
      },
      "output": "...",
      "error": ""
    },
    {
      "input": {
        "unknown_id": "456",
        "startDate": "2026-05-01",
        "endDate": "2026-05-14"
      },
      "output": "...",
      "error": ""
    }
  ]
}
```

### 5.3 多列都是多值且长度相同

输入：

```json
{
  "ad_id": "123,456",
  "material_id": "m1,m2"
}
```

配置：

```yaml
parameter_mapping:
  ad_id: "ad_id"
  material_id: "material_id"
```

多列多值按位置 zip 对齐，不做笛卡尔积：

```json
{
  "metric_list": [
    {
      "input": {
        "ad_id": "123",
        "material_id": "m1"
      },
      "output": "...",
      "error": ""
    },
    {
      "input": {
        "ad_id": "456",
        "material_id": "m2"
      },
      "output": "...",
      "error": ""
    }
  ]
}
```

不默认做笛卡尔积的原因是：业务输入通常表示同一行中的实体组合，而不是所有可能组合。默认笛卡尔积会把 2 条输入扩成 4 条结果，容易产生用户没有预期的计算。

### 5.4 多列都是多值但长度不同

输入：

```json
{
  "ad_id": "123,456",
  "material_id": "m1"
}
```

配置：

```yaml
parameter_mapping:
  ad_id: "ad_id"
  material_id: "material_id"
```

如果 `material_id` 被识别为单值，则它会广播：

```json
{
  "metric_list": [
    {
      "input": {
        "ad_id": "123",
        "material_id": "m1"
      },
      "output": "...",
      "error": ""
    },
    {
      "input": {
        "ad_id": "456",
        "material_id": "m1"
      },
      "output": "...",
      "error": ""
    }
  ]
}
```

如果存在两个及以上长度大于 1 的多值参数，且这些多值参数长度不一致，则当前 operator 不执行 `calculate(...)`，而是输出一条错误结果：

```json
{
  "metric_list": [
    {
      "input": {
        "ad_id": "123,456,789",
        "material_id": "m1,m2"
      },
      "output": "null",
      "error": "multi-value parameters have different lengths: ad_id=3, material_id=2"
    }
  ]
}
```

## 6. 多值拆分建议

仅当参数元数据 `multiValue=true` 时，多值字段按下面方式识别：

- 字符串中用英文逗号 `,` 分隔。
- 分隔后去掉首尾空白。
- 空值丢弃。
- 如果值本身是数组，也可以直接按数组元素展开。

`multiValue=false` 的参数不适用这些拆分规则，始终作为单值传入。

示例：

| 原始值 | 展开结果 |
| --- | --- |
| `"123,456"` | `["123", "456"]` |
| `"123, 456"` | `["123", "456"]` |
| `["123", "456"]` | `["123", "456"]` |
| `"123"` | `["123"]` |
| `""` | `[]` |

不建议在通用拆分逻辑中从任意文本里用正则抽取数字。派生指标的入参已经由 `parameter_mapping` 明确指定，拆分逻辑应尽量保留用户输入语义。

## 7. 与旧输出的关系

旧输出接近：

```json
{
  "123": {
    "metrics": [
      {
        "metricCode": "EcpCost",
        "metricName": "计划消耗环比",
        "output": "...",
        "error": ""
      }
    ],
    "tools": []
  }
}
```

新输出改为：

```json
[
  {
    "meta": {
      "metric_code": "EcpCost",
      "metric_name": "计划消耗环比",
      "params": {
        "unknown_id": {
          "name": "未知ID",
          "type": "AMBIGUOUS"
        }
      }
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

核心变化：

- 不再以 ID 作为最外层 key。
- 不再区分 `metrics` 和 `tools` 列表，统一放入 `metric_list`。
- 每条结果显式保留本次计算的 `input`。
- 一个 operator 可以因为多值参数展开出多条 `metric_list` 结果。
- 多个 operator 通过最外层数组表达。

## 8. 前端展示建议

前端可以按两级结构展示：

1. 第一层：按最外层数组展示 operator 卡片或表格分组，标题使用 `meta.metric_name`，副标题可显示 `meta.metric_code`。
2. 第二层：每个 operator 下展示 `metric_list`，每行展示 `input`、`output`、`error`。

如果 `error` 非空：

- 优先展示错误信息。
- `output` 通常为 `"null"`，可以弱化展示或隐藏。

如果 `metric_list` 有多条：

- 表示同一个 operator 被多组入参执行了多次。
- 多值拆分后的每个 ID 或实体组合都可以在 `input` 中看到。
