# State 指标计算对齐 Dataset Factory 实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**目标:** 扩展 `StateMetricCalculatorMapper`，让 Data-Juicer 的指标计算算子具备 Dataset Factory 阶段 4 指标计算流程里的关键能力，同时保留当前“从 ADC 接口获取指标元信息和计算口径”的架构。

**架构:** `StateMetricCalculatorMapper` 仍然是唯一算子入口，指标元信息仍然从 `/openapi/state-meta/operators/batch-get` 获取，不迁移 Dataset Factory 的本地 registry。新增一层本地共享 runtime/helper，负责 id 提取、`id_key` 识别、公共数学函数、计算上下文注入和 Dataset Factory summary 风格输出；具体指标计算口径仍由接口返回的 `operatorCode` 中的 `calculate(...)` 执行。公共方法抽取必须以 Dataset Factory 真实代码为依据，文档只作为导航。

**关键假设与前置确认:** 后端 operator detail payload 必须继续提供 `id`、`operatorNameEn`、`operatorNameCn`、`operatorCode`、`inputParameter.params` 等字段；实现前先用现有 mock 或真实接口样例确认字段含义。所有新增输出必须保持 Ray/PyArrow schema 稳定，尤其是嵌套 `metrics.output` 和 `metrics.error` 必须始终是字符串。不能把 Dataset Factory 的指标实现硬复制进 Data-Juicer，Data-Juicer 只维护公共运行时能力和公共数学方法。

**兼容性决策:** 本次会把 `output_key` 的默认写出值从旧对象结构改为 Dataset Factory summary JSON 字符串，这是有意的输出契约变更。`result_mode` 必须同步改为 `summary` 语义，不能继续上报或校验为 `object`；旧 `{"id": ..., "items": ...}` 结构只作为内部中间结果存在，并通过内部 helper 测试覆盖。

**技术栈:** Python 3、Data-Juicer `Mapper`、Ray Dataset、PyArrow schema 稳定性、`PythonScriptRunner` 动态执行可信 Python、ADC OpenAPI `HttpClient`。

---

## 背景和差异

Dataset Factory 阶段 4 的完整流程是：

```text
对每条 sample：
  解析 state
  -> 从 issue_id 提取一个或多个待计算 id
  -> 对每个 id 基于 state_data 识别 id_key（ad_id / adv_id）
  -> 根据 metricCode 路由到具体指标函数
  -> 指标函数使用公共数学函数完成计算并构造 output
  -> 汇总成按 id 分组的 summary JSON 字符串
```

当前 `StateMetricCalculatorMapper` 已具备：

- 从 ADC 接口按 `operator_id` 批量拉取 operator detail。
- 编译并执行每个 operator detail 中的 `operatorCode`。
- 计算入口固定为 `calculate(...)`。
- 根据 `inputParameter.params` 和 `parameter_mapping` 做参数注入。
- 支持一个 sample 中多个 id，输出结构为 `{"id": "...", "items": [...]}`。
- 指标失败不会中断整条样本，会写入 `output: "null"` 和 `error`。

当前主要缺口：

- id 拆分逻辑还没有完全对齐 Dataset Factory 的 `extract_numeric_ids(...)`，尤其是混合字符串中的数字提取和去重。
- 没有公共 `id_key` 识别层，指标代码无法统一知道当前 id 是 `ad_id` 还是 `adv_id`。
- 没有一套 Data-Juicer 内部维护的公共数学函数和公共工具函数。
- `calculate(...)` 只能靠普通参数映射拿数据，缺少 `id_key`、`id_value`、`start_date`、`end_date`、`helpers` 这类公共上下文。
- 输出结构还没有对齐 Dataset Factory 的按 id 分组 summary；同时我们还需要在失败项中保留 `error` 字段，不能像 Dataset Factory summary 那样只收集成功输出。
- 当前构造参数和 callback 仍声明 `result_mode="object"`，但新输出契约需要 `result_mode="summary"`，否则平台侧看到的配置和真实输出会冲突。
- 当前既有测试和潜在下游消费方仍按 `query_metric_data_outputs["items"]` 读取旧对象结构，需要在实现中同步迁移测试、示例配置和消费方说明。
- Dataset Factory 的其他工具执行链路和指标计算不同，本计划暂不纳入。

## 真实源码参考边界

本计划抽取公共方法时必须参考 Dataset Factory summary 工程里的真实代码，而不是只按说明文档重写。实施前先打开并核对这些路径：

- `/Users/bytedance/develop/dataset_factory/core/metric_runner.py`
  - 参考 `extract_numeric_ids(...)`、`run_metrics(...)`、`run_aux_tools(...)` 和 `serialize_run_results(...)`。
  - 这里体现了 sample 级入口、数字 id 抽取、metric/tool 两条执行路径，以及 summary 序列化方式。
- `/Users/bytedance/develop/dataset_factory/app.py`
  - 参考阶段 4 中 `metrics_by_id`、`tools_by_id`、`summary_map` 的构造逻辑。
  - 这里体现了最终写入 Dataset Factory summary 字段的是按 id 聚合后的 JSON 字符串；Dataset Factory 默认只收集成功结果，而 Data-Juicer 本次需要保留失败 metric 的 `error`。
- `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metric_data.py`
  - 参考 `parse_id_list(...)`、`detect_id_keys(...)`、`query_metric_data(...)`。
  - 这里体现了 metric 入口如何识别 `id_key`、校验日期、按 `metricCode` 路由具体 handler。
- `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metrics/`
  - 只参考具体指标 handler 的函数签名、返回值风格、公共 helper 使用方式。
  - 不复制 `registry.py` 的指标清单，也不把 Dataset Factory 的具体业务指标实现迁移到 Data-Juicer。
- `/Users/bytedance/develop/dataset_factory/utils/math.py`
  - 参考公共数学函数的语义和边界条件，迁移为 Data-Juicer 自己维护的 helper。

Dataset Factory 里 metric 和 tool 是两条路径：

- metric 路径：`run_metrics -> query_metric_data -> detect_id_keys -> metric registry -> metric handler`。
- tool 路径：`run_aux_tools -> get_tool_handler(tool_name) -> _build_aux_tool_input -> tool handler`。

Data-Juicer 本次只参考这两条路径的职责拆分，不照搬 Dataset Factory 的 registry/tool handler 机制。我们的指标计算仍以 ADC 元数据为准：

- 指标列表来自 `operators` / `operator_id`。
- 指标名称来自 `operatorNameEn` / `operatorNameCn`。
- 指标计算口径来自 `operatorCode` 中的 `calculate(...)`。
- 参数声明来自 `inputParameter.params`，并结合 `parameter_mapping` 取样本字段。

辅助 tool 不要混进本次 metric 执行链路。后续如果要支持 tool，需要后端元数据显式区分 metric/tool，或新建独立 tool 算子；不能仅因为 Dataset Factory 有 `run_aux_tools(...)` 就把 tool 逻辑塞进 `StateMetricCalculatorMapper`。

## 目标行为

保留现有写法：

```python
def calculate(state, ids, bench_roi):
    return 0.82
```

同时支持新的上下文增强写法：

```python
def calculate(state, ids, id_key, start_date=None, end_date=None, helpers=None):
    if id_key == "ad_id":
        return helpers.fmt4(1.23456)
    return "账户口径"
```

兼容原则：

- 函数名仍然只能是 `calculate`。
- 现有 `state`、`inputParameter.params`、`parameter_mapping` 行为不变。
- 新能力通过“保留参数名”按需注入，不要求所有指标都改代码。
- 输出要对齐 Dataset Factory summary：`output_key` 写入按 id 分组的 summary JSON 字符串。
- 与 Dataset Factory 的差异是：失败指标也要进入对应 id 的 `metrics` 列表，并保留 `error` 字段，便于下游排查。

## 不做的事情

- 不复制 Dataset Factory 的 `query_metrics` registry 或 tool handler 机制到 Data-Juicer。
- 不在 Data-Juicer 中维护具体业务指标列表。
- 不实现 `get_industry_creative_tips` 等辅助工具；tool 支持需要单独元数据模型或单独算子设计。
- 不改变指标元信息从 ADC 接口获取的事实。
- 不保留当前 `{"id": "...", "items": [...]}` 作为最终默认输出结构；如需调试，可在内部 helper 或临时变量中保留中间结构。
- 不继续把 `result_mode="object"` 作为默认或 callback 语义；如需要历史对象输出，应另起显式 legacy 模式，本计划不实现 legacy 写出。

---

### Task 1: 新增指标运行时公共 id helper

**文件:**
- 新增: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py`
- 新增测试: `tests/ops/mapper/test_state_metric_runtime.py`

**Step 1: 写失败测试**

覆盖 Dataset Factory 中 id 提取和 `id_key` 判断的核心行为。测试预期要先对照 `/Users/bytedance/develop/dataset_factory/core/metric_runner.py` 的 `extract_numeric_ids(...)` 和 `/Users/bytedance/develop/dataset_factory/tool_handlers/query_metric_data.py` 的 `detect_id_keys(...)`：

```python
def test_extract_metric_ids_deduplicates_numeric_fragments():
    assert extract_metric_ids("ad:123, adv:456, again:123") == ["123", "456"]


def test_extract_metric_ids_falls_back_to_stripped_original():
    assert extract_metric_ids("abc_def") == ["abc_def"]


def test_detect_id_key_prefers_ad_when_id_matches_both_ad_and_adv():
    state = {
        "ad_state": [{"ad_id": "123"}],
        "adv_state": [{"adv_id": "123"}],
    }
    assert detect_id_key(state, "123") == "ad_id"


def test_detect_id_key_supports_adv_meta_data_fallback():
    state = {"adv_state": [{"meta_data": {"adv_id": "456"}}]}
    assert detect_id_key(state, "456") == "adv_id"
```

**Step 2: 跑测试确认失败**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_runtime
```

预期：模块或函数不存在，测试失败。

**Step 3: 实现最小 helper**

实现前先在本地打开并核对：

```bash
sed -n '1,140p' /Users/bytedance/develop/dataset_factory/core/metric_runner.py
sed -n '1,180p' /Users/bytedance/develop/dataset_factory/tool_handlers/query_metric_data.py
```

在 `state_metric_runtime.py` 中实现：

```python
def extract_numeric_ids(value): ...
def extract_metric_ids(value): ...
def detect_id_keys(state_data, id_value): ...
def detect_id_key(state_data, id_value): ...
```

规则：

- `extract_numeric_ids` 对齐 Dataset Factory：纯数字直接返回；混合字符串用 `re.findall(r"\d+")`；按首次出现顺序去重。
- `extract_metric_ids` 在提取不到数字时 fallback 到 `[str(value or "").strip()]`。
- `detect_id_keys` 检查 `ad_state[].ad_id`、`adv_state[].adv_id`、`adv_state[].meta_data.adv_id`。
- `detect_id_key` 在同时命中时优先返回 `ad_id`；只有命中 `adv_id` 且未命中 `ad_id` 时返回 `adv_id`。
- 未命中时返回 `None`，由调用方决定是否写 error。

**Step 4: 跑测试确认通过**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_runtime
```

**Step 5: 提交**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py tests/ops/mapper/test_state_metric_runtime.py
git commit -m "add state metric runtime id helpers"
```

---

### Task 2: 新增公共数学函数和 helpers 对象

**文件:**
- 修改: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py`
- 修改测试: `tests/ops/mapper/test_state_metric_runtime.py`

**Step 1: 写失败测试**

覆盖一小组关键数学能力，不需要一开始把所有函数都测成大矩阵：

```python
def test_helpers_safe_divide_and_parse_percent():
    helpers = MetricHelpers()
    assert helpers.safe_divide(1, 0) == 0.0
    assert helpers.parse_percent_to_ratio("75%") == 0.75


def test_helpers_calc_sequential_stats_integer():
    helpers = MetricHelpers()
    series = {
        "2024-01-01": 10,
        "2024-01-02": 20,
        "2024-01-03": 30,
        "2024-01-04": 60,
    }
    assert helpers.calc_sequential_stats_integer(
        series,
        date(2024, 1, 3),
        date(2024, 1, 4),
    ) == (45, 15, 2.0)
```

**Step 2: 跑测试确认失败**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_runtime
```

预期：`MetricHelpers` 不存在。

**Step 3: 实现 `MetricHelpers`**

从 Dataset Factory `/Users/bytedance/develop/dataset_factory/utils/math.py` 迁移纯函数语义，作为 Data-Juicer 自己维护的 helper，不运行时 import Dataset Factory。

首批方法：

- `extract_numeric_values_in_range`
- `sum_numeric_values_in_range`
- `safe_divide`
- `calc_ratio_from_series`
- `calc_sequential_stats`
- `calc_sequential_stats_integer`
- `calc_sequential_stats_for_fraction`
- `calc_bench_compare`
- `fmt4`
- `average`
- `calc_sequential_ratio`
- `parse_percent_to_ratio`
- `resolve_date_range_from_series`
- `parse_duration_seconds`

要求：

- 纯 Python 标准库实现。
- 不依赖外部服务。
- 不改变 Dataset Factory 的口径，除非发现明确 bug 并单独记录。
- 每个 helper 的边界行为至少要有一条测试能对应到 Dataset Factory 真实函数语义，不能只覆盖 happy path。

**Step 4: 跑测试确认通过**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_runtime
```

**Step 5: 提交**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py tests/ops/mapper/test_state_metric_runtime.py
git commit -m "add state metric math helpers"
```

---

### Task 3: 给 `calculate(...)` 注入公共计算上下文

**文件:**
- 修改: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py`
- 修改测试: `tests/ops/mapper/test_state_metric_calculator_mapper.py`

**Step 1: 写失败测试**

新增配置：

```python
start_date_key: str | None = None
end_date_key: str | None = None
```

新增测试：指标代码可以声明并收到 `id_key`、`id_value`、`start_date`、`end_date`、`helpers`。

```python
def calculate(state, ids, id_key, id_value, start_date=None, end_date=None, helpers=None):
    return {
        "ids": ids,
        "id_key": id_key,
        "id_value": id_value,
        "start": str(start_date),
        "end": str(end_date),
        "fmt": helpers.fmt4(1.2300),
    }
```

样本中放：

```python
{
    "state": {"ad_state": [{"ad_id": "123"}]},
    "issue_id": "123",
    "start": "2024-01-01",
    "end": "2024-01-07",
}
```

预期输出中 `id_key == "ad_id"`，日期被解析为 `datetime.date` 后再被 `str(...)` 成 `2024-01-01` / `2024-01-07`。

**Step 2: 跑测试确认失败**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_calculate_can_receive_metric_context
```

预期：当前会报 `inputParameter.params missing parameter: id_key` 或类似错误。

**Step 3: 实现上下文注入**

在 `StateMetricCalculatorMapper` 中：

- 每条 sample 解析 `state` 得到 `state_data`，避免重复解析。
- 每个当前 id 调用 `detect_id_key(state_data, item_id)`。
- 新增 `_resolve_date_value(sample, key)`，支持空值和 `YYYY-MM-DD`。
- 扩展 `_resolve_calculate_args` 的保留参数名：
  - `id_key`
  - `id_value`
  - `start_date`
  - `end_date`
  - `helpers`
- `helpers` 注入 `MetricHelpers()` 实例。

设计约束：

- `id_key`、`id_value`、`start_date`、`end_date`、`helpers` 是 Data-Juicer runtime 注入的保留参数，不要求后端 `inputParameter.params` 显式声明。
- 其他业务参数仍由后端 `inputParameter.params` 和 `parameter_mapping` 驱动。
- 不引入 Dataset Factory 的 metric registry。路由关系仍然是 ADC 返回的 `operators` 顺序和每个 operator detail 自带的 `operatorCode`。

保持现有规则：

- `state` 参数仍从 `state_key` 取。
- 普通参数仍从 `inputParameter.params` 和 `parameter_mapping` 取。
- 默认值参数仍可省略。
- `*args`、`**kwargs`、keyword-only 参数仍不支持。

**Step 4: 跑测试确认通过**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_calculate_can_receive_metric_context
```

**Step 5: 跑完整 state metric 测试**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper
```

**Step 6: 提交**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "add state metric calculation context"
```

---

### Task 4: 对齐 Dataset Factory 的多 id 提取逻辑

**文件:**
- 修改: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py`
- 修改测试: `tests/ops/mapper/test_state_metric_calculator_mapper.py`

**Step 1: 写失败测试**

当前算子主要按逗号拆分字符串。新增 mixed string 用例；预期行为以 `/Users/bytedance/develop/dataset_factory/core/metric_runner.py` 的 `extract_numeric_ids(...)` 为准。

在 Task 4 阶段，如果 `process_single` 仍临时写旧对象结构，可以先对内部 `_calculate_metric_outputs(...)` 或等价中间 helper 断言 `items`。但最终到 Task 5 后，所有面向 `output_key` 的测试都必须改为 `json.loads(result[output_key])` 后断言 summary 顶层 id，不再读取 `result[output_key]["items"]`。

```python
def test_intermediate_items_extract_numeric_ids_from_mixed_issue_id(...):
    sample["issue_id"] = "ad:1854751525764108, adv:1853671159428096, again:1854751525764108"
    output = op._calculate_metric_outputs(sample)
    assert [item["id"] for item in output["items"]] == [
        "1854751525764108",
        "1853671159428096",
    ]
```

新增 fallback 用例：

```python
def test_intermediate_items_fallback_to_original_when_no_numeric_id(...):
    sample["issue_id"] = "abc_def"
    output = op._calculate_metric_outputs(sample)
    assert output["items"][0]["id"] == "abc_def"
```

**Step 2: 跑测试确认失败**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_intermediate_items_extract_numeric_ids_from_mixed_issue_id
```

预期：当前输出仍带 `ad:` / `adv:` 片段或没有 Dataset Factory 风格去重。

**Step 3: 替换 id 拆分实现**

将 `_split_output_id_value` 改为调用 `state_metric_runtime.extract_metric_ids(value)`。

兼容要求：

- `list` 输入继续保留当前列表语义：每个元素 stringify、strip，过滤空值。
- `str` 输入使用 Dataset Factory 数字提取和 fallback。
- 输出为空时保底 `["unknown"]` 或按当前行为明确保留；如果引入 fallback，则优先与 Dataset Factory 一致。

**Step 4: 跑测试确认通过**

跑新增测试。

**Step 5: 跑 Arrow schema 稳定性测试**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_metric_failure_output_keeps_arrow_schema_as_string
```

**Step 6: 提交**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "align state metric id extraction"
```

---

### Task 5: 将默认输出对齐 Dataset Factory summary 并保留 error

**文件:**
- 修改: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py`
- 修改测试: `tests/ops/mapper/test_state_metric_calculator_mapper.py`

**Step 1: 写失败测试**

实现前先核对 Dataset Factory 真正写 summary 的 app 层代码，不只看 `serialize_run_results(...)`：

```bash
sed -n '470,555p' /Users/bytedance/develop/dataset_factory/app.py
```

需要确认的真实行为：

- `metrics_by_id` 和 `tools_by_id` 是按 `one_id` 聚合。
- Dataset Factory 默认只把成功结果放进 summary：`output` 非空、不含 `返回调用失败`、没有 `error`。
- Data-Juicer 本次保留 Dataset Factory 的按 id 聚合和 JSON 字符串写出，但有意改变失败处理：失败 metric 也进入 `metrics`，并保留 `error`。

`output_key` 对应字段应直接写入 Dataset Factory summary 风格的 JSON 字符串。成功和失败指标都要保留，其中失败指标保留 `error` 字段：

```python
json.loads(result["query_metric_data_outputs"]) == {
    "123": {
        "metrics": [
            {
                "metricCode": "metric_ok",
                "metricName": "成功指标",
                "output": "成功输出",
                "error": "",
            },
            {
                "metricCode": "metric_failed",
                "metricName": "失败指标",
                "output": "null",
                "error": "missing required parameter: bench_roi",
            },
        ]
    }
}
```

对齐规则：

- 顶层 key 是每个 item id。
- 每个 id 下保留 `metrics` 列表。
- 每个 metric 至少保留 `metricCode`、`metricName`、`output`、`error`。
- 成功 metric 的 `error` 是空字符串。
- 失败 metric 的 `output` 是 `"null"`，`error` 是具体错误信息。
- 没有任何 metric 时返回空字符串 `""`。
- 当前不写 `tools`，因为辅助工具不在本次范围内。
- 如果后端未来返回 tool 类型元数据，需要先新增显式 `operator_type` / `tool_type` 这类区分字段，不能复用当前 metric summary 的隐式结构。

**Step 2: 跑测试确认失败**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_output_key_writes_dataset_factory_summary_with_errors
```

预期：当前 `output_key` 仍然写对象结构，不是 summary JSON 字符串。

**Step 3: 实现 summary builder**

新增：

```python
def _build_summary_output(metric_outputs: dict[str, Any]) -> str:
    ...
```

规则：

- 遍历 `metric_outputs["items"]`。
- 每个 item 的 `id` 作为 summary map key。
- 成功和失败 metric 都保留。
- 失败 metric 必须保留 `error`。
- 当前不写 `tools`。
- 没有任何 metric 时返回空字符串 `""`。
- `process_single` 最终写入 `output_sample[self.output_key] = summary_string`。
- 中间对象结构可以保留在局部变量中，但不要作为默认输出写出。

同时迁移旧对象输出相关测试和断言：

- 所有读取 `result["query_metric_data_outputs"]["items"]` 或 `["id"]` 的面向 `process_single` 测试，改为 `json.loads(result["query_metric_data_outputs"])` 后断言 summary。
- 多 id 测试从断言 `items[*].id` 改为断言 `set(summary.keys())` 或保持顺序时断言 `list(summary.keys())`。
- 如果仍需要覆盖旧中间对象结构，只能直接测试 `_calculate_metric_outputs(...)` 或拆出的中间 helper，不再通过 `output_key` 暴露。
- 搜索命令：

```bash
rg -n 'query_metric_data_outputs.*\\[\"items\"\\]|query_metric_data_outputs.*\\[\"id\"\\]|\\[\"items\"\\]' tests/ops/mapper/test_state_metric_calculator_mapper.py
```

**Step 4: 跑测试确认通过**

跑新增测试。

**Step 5: 跑完整 state metric 测试**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper
```

**Step 6: 提交**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "align state metric summary output"
```

---

### Task 6: callback config 同步新运行时配置

**文件:**
- 修改: `data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py`
- 修改测试: `tests/ops/mapper/test_state_metric_calculator_mapper.py`

**Step 1: 写失败测试**

先把构造参数契约迁移到 `summary`：

```python
op = StateMetricCalculatorMapper(operators=self._operators(), ctx=self._ctx())
assert op.result_mode == "summary"

with self.assertRaisesRegex(ValueError, "result_mode"):
    StateMetricCalculatorMapper(
        operators=self._operators(),
        result_mode="object",
        ctx=self._ctx(),
    )
```

扩展已有 callback config 测试，期望包含：

```python
{
    "start_date_key": None,
    "end_date_key": None,
    "result_mode": "summary",
    "output_format": "dataset_factory_summary",
    "preserve_error": True,
    "runtime": "adc_operator_code",
}
```

**Step 2: 跑测试确认失败**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_before_operator_started_starts_running_once
```

预期：缺少新字段。

**Step 3: 更新 `_operator_config`**

新增配置字段。

注意：

- 不要把接口拉到的完整 operator detail 放进 callback。
- 不要把 `operatorCode` 放进 callback，避免泄露代码和增大 payload。
- `result_mode` 默认值和唯一支持值改为 `summary`；不要在 callback 里继续上报 `result_mode: object`。
- `output_format` 明确标记默认输出已经是 Dataset Factory summary 风格。
- `preserve_error` 明确标记失败 metric 会保留 `error` 字段。
- callback 只记录 Data-Juicer 当前采用的 metric runtime 行为，不声明 tool 支持。

同步修改构造函数 docstring 和配置示例，避免用户继续以为 `object` 是有效模式。旧对象中间结构如果保留，只能作为内部 helper 返回值，不作为 `result_mode` 暴露。

**Step 4: 跑测试确认通过**

跑 callback focused test。

**Step 5: 提交**

```bash
git add data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py tests/ops/mapper/test_state_metric_calculator_mapper.py
git commit -m "report state metric runtime config"
```

---

### Task 7: 迁移旧文档、示例配置和消费方说明

**文件:**
- 修改: `docs/plans/2026-05-18-state-metric-calculator-mapper.md` 或标记为 historical
- 修改: `docs/plans/2026-05-18-state-metric-calculator-lance-test.md` 或标记为 historical
- 按搜索结果修改: `docs/`、`demos/`、`tools/` 下其他引用旧输出结构或 `result_mode: object` 的文件

**Step 1: 搜索旧契约引用**

```bash
rg -n 'query_metric_data_outputs|result_mode.*object|result_mode: object|result_mode: "object"|\\["items"\\]|\\["id"\\]' docs demos tools tests data_juicer
```

预期至少能看到旧 plan 文档和 mapper 测试里的引用。

**Step 2: 分类处理搜索结果**

- 当前实现代码和测试：按 Task 5/6 的新契约迁移。
- 当前可运行 demo / tools / 用户文档：更新为 `result_mode: summary` 和 Dataset Factory summary JSON 字符串读取方式。
- 历史 plan 文档：如果不应改写历史内容，在文件顶部追加明确 historical note，说明该文档描述的是旧对象输出契约，新实现以 `2026-05-23-state-metric-dataset-factory-parity-plan.md` 为准。
- 不要无差别重写所有历史记录；只处理会误导后续执行者或用户的旧契约描述。

**Step 3: 验证旧契约引用已收敛**

```bash
rg -n 'result_mode.*object|result_mode: object|result_mode: "object"' docs demos tools data_juicer
rg -n 'query_metric_data_outputs.*\\[\"items\"\\]|query_metric_data_outputs.*\\[\"id\"\\]' docs demos tools data_juicer
```

预期：没有当前文档/示例继续把 `query_metric_data_outputs` 当旧对象结构消费；历史文件如果保留旧内容，必须有同文件 historical note。

**Step 4: 提交**

```bash
git add docs demos tools data_juicer
git commit -m "docs: migrate state metric summary contract references"
```

---

### Task 8: schema 和 Ray 相关验证

**文件:**
- 修改测试: `tests/ops/mapper/test_state_metric_calculator_mapper.py`
- 修改测试: `tests/core/data/test_ray_dataset.py` 或新增最近的 Ray/Arrow mapper 测试文件

**Step 1: 增加 mixed success/failure schema 测试**

构造生产形状：

- 一个 sample 有两个 id。
- 一个 metric 成功，`output` 是字符串。
- 一个 metric 失败，`output` 是 `"null"`。
- 一个 sample 没有任何 metric 时，`output_key` 是空字符串。

断言：

- `query_metric_data_outputs` 始终是字符串。
- `json.loads(query_metric_data_outputs)` 后，每个 metric 的 `output` 和 `error` 都是字符串。
- 失败 metric 保留非空 `error`，不会被 summary builder 丢弃。

**Step 2: 跑 focused schema test**

```bash
./.venv/bin/python -m unittest tests.ops.mapper.test_state_metric_calculator_mapper.StateMetricCalculatorMapperTest.test_dataset_factory_summary_serializes_outputs_as_strings
```

**Step 3: 增加 Ray/PyArrow block 级测试**

需要覆盖真实 Ray Dataset / PyArrow block concat 风险，不只做 Python dict 断言。测试可以放在 `tests/core/data/test_ray_dataset.py`，或如果 `test_state_metric_calculator_mapper.py` 已有 Ray skip 机制，也可以放在 mapper 测试文件中。

测试形状：

- 使用 `ray.data.from_items(...)` 构造至少 3 条样本，并通过 `repartition(2)` 或等价方式制造多个 block。
- 第一条输出非空 summary，包含成功 metric。
- 第二条输出非空 summary，包含失败 metric，`output` 为 `"null"`，`error` 为字符串。
- 第三条输出空 summary，即 `output_key == ""`。
- 通过 Ray/Data-Juicer mapper 路径执行后，断言 `query_metric_data_outputs` 这一列在 Arrow schema 中是 `string`，不是 struct/list/null。
- `take_all()` 或 `iter_batches(batch_format="pyarrow")` 后断言所有行该字段都是字符串。

示例断言：

```python
schema = result_dataset.schema()
assert schema.field("query_metric_data_outputs").type == pa.string()
```

如果本地 Ray 版本的 `schema()` 不稳定，可以用 `iter_batches(batch_format="pyarrow")` 检查每个 block 的列类型。

**Step 4: 跑 Ray/PyArrow focused test**

```bash
./.venv/bin/python -m unittest tests.core.data.test_ray_dataset.RayDatasetImportTest.test_state_metric_summary_output_stays_string_across_blocks
```

如果本地没有安装 Ray，这个测试应使用现有 `skipUnless(importlib.util.find_spec("ray"), "ray is not installed")` 风格跳过，并在最终说明中明确。

**Step 5: 跑最终验证命令**

```bash
python3 -m py_compile \
  data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py \
  data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py \
  tests/ops/mapper/test_state_metric_calculator_mapper.py \
  tests/ops/mapper/test_state_metric_runtime.py

./.venv/bin/python -m unittest \
  tests.ops.mapper.test_state_metric_runtime \
  tests.ops.mapper.test_state_metric_calculator_mapper

./.venv/bin/python -m unittest \
  tests.core.data.test_ray_dataset.RayDatasetImportTest.test_state_metric_summary_output_stays_string_across_blocks

git diff --check
```

**Step 6: 提交**

```bash
git add tests/ops/mapper/test_state_metric_calculator_mapper.py tests/core/data/test_ray_dataset.py
git commit -m "verify state metric summary schema"
```

---

## 兼容性说明

- 现有 `calculate(...)` 函数继续有效。
- `output_key` 默认输出 Dataset Factory summary 风格 JSON 字符串，这是对旧对象结构的输出契约变更。
- `result_mode` 默认值和唯一支持值应改为 `summary`；不要继续使用或上报 `object`，避免配置语义和真实输出冲突。
- 所有既有读取 `query_metric_data_outputs["items"]` / `["id"]` 的单测、示例配置和下游消费说明都必须迁移到 `json.loads(query_metric_data_outputs)` 后按 summary 结构读取。
- 旧 `{"id": "...", "items": [...]}` 结构只能作为内部中间结构存在；如果需要继续对它做测试，应直接测试内部 helper，不通过 `output_key` 暴露。
- 与 Dataset Factory 不同，失败 metric 也会保留在 summary 中，并带 `error` 字段。
- ADC 接口仍然是指标元信息来源。
- 公共 helper 通过可选保留参数注入，指标代码不声明就不会收到。
- Dataset Factory 的 metric/tool 双路径只作为职责拆分参考；Data-Juicer 当前算子保持 metric-only。
- 辅助工具不纳入本次改造；后续如果需要，建议作为单独算子或基于后端显式 tool 元数据的 opt-in 模式设计。

## 最终验证矩阵

实施完成后至少运行：

```bash
python3 -m py_compile \
  data_juicer/ops/mapper/ad_ai_data_center/state_metric_calculator_mapper.py \
  data_juicer/ops/mapper/ad_ai_data_center/state_metric_runtime.py \
  tests/ops/mapper/test_state_metric_calculator_mapper.py \
  tests/ops/mapper/test_state_metric_runtime.py

./.venv/bin/python -m unittest \
  tests.ops.mapper.test_state_metric_runtime \
  tests.ops.mapper.test_state_metric_calculator_mapper

./.venv/bin/python -m unittest \
  tests.core.data.test_ray_dataset.RayDatasetImportTest.test_state_metric_summary_output_stays_string_across_blocks

git diff --check
```

如果本地无法访问 ADC 接口或线上 Ray 环境，不要声称线上完全对齐。只能说明本地 unit/schema 验证通过，最终还需要用真实 `/openapi/state-meta/operators/batch-get` 元信息跑一次 Ray 任务确认。
