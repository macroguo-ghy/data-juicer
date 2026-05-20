# 导入外部数据集算子定义

## 背景

数据合成流程需要支持从外部电子表格导入 eval_data 模板数据，并允许业务方在导入阶段执行自定义处理逻辑。

该能力通过一个业务复合算子承载：算子负责加载外部表格原始数据、按数据类型解析模板内容，并执行用户提供的 Python 代码，最终输出一个新的结构化 object，供后续工作流节点或下游算子继续使用。

## 算子名称

建议名称：

```text
ExternalEvalDataImportOperator
```

中文名称：

```text
导入外部数据集算子
```

## 算子定位

该算子用于在数据合成编排中完成以下事情：

1. 从外部电子表格加载原始 eval_data 模板数据。
2. 根据数据类型字段选择对应的解析、校验或转换逻辑。
3. 执行业务方配置的自定义 Python 处理代码。
4. 输出处理后的新 object。

该算子不是单纯的数据读取算子，而是一个业务复合算子：`外部数据加载 + eval_data 模板解析 + 自定义 Python 处理` 在同一个算子内完成。

## 输入参数

| 参数名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `sheet_url` | `str` | 是 | 外部电子表格链接，表格内容遵循 eval_data 模板约定。 |
| `data_type` | `str` | 是 | 数据类型字段，用于标识当前导入数据的业务类型或解析方式。算子根据该字段选择对应的数据解析、校验或转换逻辑。 |
| `python_code` | `str` | 是 | 用户自定义 Python 处理逻辑。代码接收解析后的原始数据对象，并返回处理后的新 object。 |

对外配置只暴露以上业务参数。云文档读取接口的 `endpoint`、`headers` 等 HTTP 细节由算子内部固定封装，不作为 YAML 参数透出。

## 输出

| 字段名 | 类型 | 说明 |
| --- | --- | --- |
| `externalDataSet` | `object` | 经过自定义 Python 逻辑处理后的结构化对象，可作为后续算子输入。 |

## 执行流程

1. 调用 AI Data Center 后端封装接口，读取 `sheet_url` 指向云文档下所有 Sheets 的纯文本数据。
2. 按 eval_data 模板约定解析表格原始数据。
3. 根据 `data_type` 识别数据类型，并执行对应的数据解析、校验或转换逻辑。
4. 将解析后的原始数据对象传入 `python_code`。
5. 执行自定义 Python 逻辑。
6. 将 Python 逻辑返回值封装为 `externalDataSet` 输出。

## 依赖的项目内 HTTP 接口

算子不直接调用飞书 / Lark OpenAPI，不在算子代码中管理 `app_id`、`app_secret`、`user_access_token` 等鉴权信息。云文档读取统一通过 AI Data Center 后端封装接口完成。

当前约定：执行算子时已经具备读取云文档的权限，授权引导链路暂不放在算子内处理。

### 读取所有 Sheets 纯文本数据

```http
POST /api/openapi/cloud-doc/sheets/all-plain-values
Content-Type: application/json
```

请求体：

```json
{
  "docUrl": "https://bytedance.feishu.cn/sheets/xxxx"
}
```

字段说明：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `docUrl` | `string` | 是 | 云文档表格链接，也可以兼容后端已支持的 wiki URL 或裸 spreadsheet token。 |

响应数据：

```json
{
  "sheets": [
    {
      "sheetId": "sheet_id",
      "title": "Sheet1",
      "values": [
        ["query", "answer"],
        ["example question", "example answer"]
      ]
    }
  ]
}
```

字段说明：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `sheets` | `list[object]` | 云文档下所有 Sheet 的纯文本数据列表。 |
| `sheets[].sheetId` | `string` | Sheet 内部 ID。 |
| `sheets[].title` | `string` | Sheet 名称。 |
| `sheets[].values` | `list[list[string]]` | Sheet 单元格纯文本二维数组。第一行是否作为表头由算子按 `data_type` 决定。 |

算子侧建议将接口返回值作为原始输入对象，例如：

```json
{
  "sheet_url": "https://bytedance.feishu.cn/sheets/xxxx",
  "data_type": "eval_data",
  "sheets": [
    {
      "sheetId": "sheet_id",
      "title": "Sheet1",
      "values": [
        ["query", "answer"],
        ["example question", "example answer"]
      ]
    }
  ]
}
```

## Python 代码约定

`python_code` 需要表达一个可执行的处理逻辑。建议约定为定义固定函数，例如：

```python
def process(data, context):
    # data: 按 eval_data 模板解析后的原始数据对象
    # raw_sheets: 可通过 context["raw_sheets"] 获取所有 Sheet 的原始二维文本数据
    # context: 算子执行上下文，可包含 data_type、sheet_url 等信息
    return {
        "items": data,
        "data_type": context["data_type"]
    }
```

算子执行时调用：

```python
context = {
    "data_type": data_type,
    "sheet_url": sheet_url,
    "raw_sheets": sheets
}
result = process(parsed_data, context)
```

## 开发要求

1. `sheet_url` 为空时应直接失败，并返回明确错误信息。
2. `data_type` 为空或不支持时应直接失败，并返回明确错误信息。
3. `python_code` 为空、无法编译或执行失败时应直接失败，并保留错误原因。
4. Python 代码执行结果必须是可序列化 object。
5. 算子输出统一封装在 `externalDataSet` 字段中。

## 示例配置

```yaml
process:
  - external_eval_data_import_mapper:
      sheet_url: "https://bytedance.feishu.cn/sheets/xxxx"
      data_type: "eval_data"
      python_code: "def process(data, context):\n    return {\"items\": data, \"data_type\": context[\"data_type\"]}"
```

## 示例输出

```json
{
  "externalDataSet": {
    "items": [
      {
        "query": "example question",
        "answer": "example answer"
      }
    ],
    "data_type": "eval_data"
  }
}
