# Ad AI Data Center HttpClient 设计

## 1. 背景

后续计划建设一系列 `ad_ai_data_center` 算子，包括通用 HTTP 算子、LLM 算子和 Code 算子。这些算子的共同能力是：根据样本字段构造请求，调用外部 HTTP 服务，再把响应或错误写回样本。

为了避免每个算子各自实现 `requests.post(...)` 或 `httpx.request(...)`，先建设一个通用 HTTP 工具类：

```text
data_juicer/utils/http_utils.py
```

工具类命名为：

```python
HttpClient
```

名称中不包含 `Json`，因为它应该表达通用 HTTP 能力，而不是只绑定 JSON 场景。第一版会重点支持 JSON 请求和 JSON 响应，同时保留非 JSON 响应的文本回退。

## 2. 目标

`HttpClient` 作为稳定复用层，供后续算子使用：

```text
AdAiDataCenterHttpMapper
        |
        v
HttpClient

AdAiDataCenterLlmMapper
        |
        v
HttpClient

AdAiDataCenterCodeMapper
        |
        v
HttpClient
```

它负责：

- 支持常见 HTTP method。
- 支持 headers、query params、JSON body。
- 支持请求超时。
- 统一成功响应结构。
- 统一异常和 HTTP 错误结构。
- 对 JSON 响应优先解析，非 JSON 响应保留文本。

## 3. 非目标

第一版不做：

- retry / backoff。
- 鉴权模板或 token 自动刷新。
- QPS 限流。
- multipart/form-data。
- streaming response。
- 复杂 response path 提取。
- 与 Data-Juicer sample 结构绑定。

这些能力后续可以在 `HttpClient` 上扩展，或由上层算子组合实现。

## 4. 接口设计

### 4.1 类定义

```python
class HttpClient:
    SUPPORTED_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}

    def __init__(
        self,
        endpoint: str,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
    ):
        ...

    def request(
        self,
        *,
        params: dict | None = None,
        json_body: dict | list | None = None,
    ) -> dict:
        ...
```

### 4.2 参数含义

| 参数 | 作用 |
| --- | --- |
| `endpoint` | 请求 URL |
| `method` | HTTP method，支持 `GET`、`POST`、`PUT`、`PATCH`、`DELETE` |
| `headers` | 请求头 |
| `timeout` | 请求超时时间，单位秒 |
| `params` | query string 参数 |
| `json_body` | JSON request body |

### 4.3 method 规则

`HttpClient` 不强行限制 method 和 body 的组合，只做 method 白名单校验。

例如：

```python
# GET with query params
client = HttpClient(endpoint="http://localhost:8000/items", method="GET")
client.request(params={"id": 1})

# POST with JSON body
client = HttpClient(endpoint="http://localhost:8000/invoke", method="POST")
client.request(json_body={"inputs": {"prompt": "hello"}})

# DELETE with query params
client = HttpClient(endpoint="http://localhost:8000/items", method="DELETE")
client.request(params={"id": 1})
```

底层统一调用：

```python
httpx.request(
    method=self.method,
    url=self.endpoint,
    headers=self.headers,
    timeout=self.timeout,
    params=params,
    json=json_body,
)
```

## 5. 返回结构

`HttpClient.request(...)` 始终返回一个 dict。上层算子不用捕获 HTTP 库异常，也不用关心 response 是否 JSON。

### 5.1 成功且响应为 JSON

```python
{
    "ok": True,
    "status_code": 200,
    "data": {"answer": "hello"},
    "text": None,
    "error": None,
}
```

### 5.2 成功但响应不是 JSON

```python
{
    "ok": True,
    "status_code": 200,
    "data": None,
    "text": "plain text response",
    "error": None,
}
```

### 5.3 HTTP 错误

```python
{
    "ok": False,
    "status_code": 500,
    "data": None,
    "text": "server error body",
    "error": {
        "type": "HTTPStatusError",
        "message": "...",
    },
}
```

### 5.4 请求异常

```python
{
    "ok": False,
    "status_code": None,
    "data": None,
    "text": None,
    "error": {
        "type": "TimeoutException",
        "message": "...",
    },
}
```

## 6. 错误处理原则

工具类内部捕获 `httpx.HTTPError` 及其子类，并返回统一错误结构。

上层算子负责决定失败样本如何处理：

- 通用 HTTP Mapper 可以把完整返回写入 `error_field`。
- LLM Mapper 可以把错误写入 `llm_error`。
- Code Mapper 可以把错误写入 `code_error`。
- 如果上层算子需要失败即中断，可以根据 `result["ok"]` 自行抛异常。

`HttpClient` 本身不退出进程、不修改样本、不记录业务字段。

## 7. 平台上下文

需要调用平台 OpenAPI 或透传执行用户/PPE 环境的算子声明：

```python
NEED_CTX = True
```

后端生成 YAML 时只给这类算子注入 `ctx` 参数。第一版 `ctx` 契约如下：

```json
{
  "userAccount": "wangjianda.667",
  "x-tt-env": "ppe_sirius2",
  "x-use-ppe": "1",
  "synthesisInstanceId": 10001,
  "flowInstanceId": 20001,
  "flowNodeId": "node_load_data",
  "taskId": 30001,
  "taskVersion": 1,
  "operatorIndex": 0,
  "operatorName": "load_external_dataset",
  "operatorType": "business",
  "openapiBaseUrl": "https://ai-data-center.bytedance.net/api"
}
```

算子侧按需读取字段：

- `userAccount`：调用平台 OpenAPI 或外部服务时透传为执行用户。
- `x-tt-env` / `x-use-ppe`：调用 PPE 环境时透传到请求头。
- `openapiBaseUrl`：拼接当前项目 OpenAPI 地址，避免算子写死环境 URL。
- 任务、工作流、算子元信息先保留在 `ctx` 中，供状态上报和审计类算子使用。

例如测试卡片通知使用：

```python
endpoint = ctx["openapiBaseUrl"].rstrip("/") + "/openapi/lark/message/template-card/send-to-user"
```

外部数据导入使用：

```python
endpoint = ctx["openapiBaseUrl"].rstrip("/") + "/openapi/cloud-doc/sheets/all-plain-values"
```

## 8. 与后续算子的关系

### 8.1 通用 HTTP 算子

`ad_ai_data_center_http_mapper` 只负责 sample 字段映射：

```python
payload = {
    "inputs": {
        field: sample.get(field)
        for field in self.input_fields
    }
}

result = self.client.request(json_body=payload)
```

成功时写入：

```python
sample[self.output_field] = result["data"] if result["data"] is not None else result["text"]
```

失败时写入：

```python
sample[self.error_field] = result
```

### 8.2 LLM 算子

LLM 算子只负责 LLM 语义字段：

```python
payload = {
    "inputs": {
        "prompt": sample[self.prompt_key],
    }
}
```

HTTP 请求、超时、状态码、响应解析都复用 `HttpClient`。

### 8.3 Code 算子

Code 算子只负责 Code 语义字段：

```python
payload = {
    "inputs": {
        "code": sample[self.code_key],
        "language": sample.get(self.language_key),
    }
}
```

HTTP 请求逻辑同样复用 `HttpClient`。

## 9. 测试方案

### 9.1 工具类单测

新增：

```text
tests/utils/test_http_utils.py
```

覆盖：

- `GET` 带 `params`。
- `POST` 带 `json_body`。
- JSON response 解析到 `data`。
- 非 JSON response 回退到 `text`。
- HTTP 500 返回 `ok=False` 和 `status_code=500`。
- timeout / connect error 返回 `ok=False` 和错误类型。
- 不支持的 method 抛 `ValueError`。

测试不请求真实外网，使用 `httpx.MockTransport` 或 patch `httpx.request`。

### 9.2 后续算子单测

后续 `ad_ai_data_center_http_mapper` 的测试只验证字段映射：

- `input_fields` 是否进入 payload。
- 成功响应是否写入 `output_field`。
- 失败响应是否写入 `error_field`。

不重复测试 HTTP status、timeout、JSON 解析细节，这些属于 `HttpClient` 的职责。

真实外部接口调用用环境变量显式开启，避免默认单测依赖网络和内部服务稳定性：

```bash
RUN_REAL_AD_AI_DATA_CENTER_HTTP_TEST=1 ./.venv/bin/python -m unittest \
  tests.ops.mapper.test_ad_ai_data_center_http_mapper.AdAiDataCenterHttpMapperTest.test_dimension_and_metric_curl_sends_real_http_request_without_cookie

RUN_REAL_EXTERNAL_EVAL_DATA_IMPORT_TEST=1 ./.venv/bin/python -m unittest \
  tests.ops.mapper.test_external_eval_data_import_mapper.ExternalEvalDataImportMapperTest.test_lark_wiki_doc_url_sends_real_request_and_processes_result
```

## 10. 文件范围

第一阶段只需要新增：

```text
data_juicer/utils/http_utils.py
tests/utils/test_http_utils.py
```

后续再新增：

```text
data_juicer/ops/mapper/ad_ai_data_center/ad_ai_data_center_http_mapper.py
tests/ops/mapper/test_ad_ai_data_center_http_mapper.py
```

LLM 和 Code 专用算子等协议稳定后再建设。

## 11. 设计结论

先建设通用 `HttpClient`，不把 HTTP 请求逻辑散落在各个算子中。

`HttpClient` 保持通用 HTTP 能力：

- 名称不带 `Json`。
- 支持多种 HTTP method。
- 支持 JSON body 和 query params。
- 对 JSON response 优先解析，对非 JSON response 文本回退。
- 输出统一结果结构。

上层算子只处理 Data-Juicer sample 和业务语义。这样后续 LLM 算子、Code 算子、通用 HTTP 算子都可以复用同一套请求能力。
